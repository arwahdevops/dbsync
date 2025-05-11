//go:build integration
package integration

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arwahdevops/dbsync/internal/config"
	dbsync_db "github.com/arwahdevops/dbsync/internal/db"
	dbsync_logger "github.com/arwahdevops/dbsync/internal/logger"
	dbsync_metrics "github.com/arwahdevops/dbsync/internal/metrics"
	dbsync_sync "github.com/arwahdevops/dbsync/internal/sync"
)

// mustPortInt (asumsikan sudah ada di db_helpers.go atau didefinisikan di sini jika belum)

func TestMySQLToPostgres_CreateStrategy_WithFKVerification(t *testing.T) { // Nama tes diperjelas
	if os.Getenv("SKIP_INTEGRATION_TESTS") != "" || testing.Short() {
		t.Skip("Skipping integration test.")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	errLogger := dbsync_logger.Init(true, false)
	require.NoError(t, errLogger, "Failed to initialize logger for test")

	sourceDB := startMySQLContainer(ctx, t)
	targetDB := startPostgresContainer(ctx, t)
	defer stopContainer(ctx, t, sourceDB)
	defer stopContainer(ctx, t, targetDB)

	// Setup Skema Awal di Source DB
	sourceDB.DB.Exec("DROP TABLE IF EXISTS comments;") // Drop tabel anak dulu
	sourceDB.DB.Exec("DROP TABLE IF EXISTS post_categories;")
	sourceDB.DB.Exec("DROP TABLE IF EXISTS posts;")
	sourceDB.DB.Exec("DROP TABLE IF EXISTS categories;")
	sourceDB.DB.Exec("DROP TABLE IF EXISTS users;")
	executeSQLFile(t, sourceDB.DB, filepath.Join("testdata", "mysql_to_pg_create", "source_schema.sql"))

	// Pastikan target DB kosong
	targetDB.DB.Exec("DROP TABLE IF EXISTS comments CASCADE;")
	targetDB.DB.Exec("DROP TABLE IF EXISTS post_categories CASCADE;")
	targetDB.DB.Exec("DROP TABLE IF EXISTS posts CASCADE;") // Urutan drop disesuaikan
	targetDB.DB.Exec("DROP TABLE IF EXISTS categories CASCADE;")
	targetDB.DB.Exec("DROP TABLE IF EXISTS users CASCADE;")

	cfg := &config.Config{
		SyncDirection:          "mysql-to-postgres",
		SchemaSyncStrategy:     config.SchemaSyncDropCreate,
		BatchSize:              100,
		Workers:                2, // Untuk tes, 2 worker cukup untuk melihat potensi masalah konkurensi
		TableTimeout:           2 * time.Minute,
		SkipFailedTables:       false, // PENTING: Set false agar tes gagal jika ada error
		DisableFKDuringSync:    true,  // PENTING: Aktifkan fitur ini
		SrcDB: config.DatabaseConfig{
			Dialect:  sourceDB.Dialect, Host:     sourceDB.Host, Port:     mustPortInt(t, sourceDB.Port),
			User:     sourceDB.Username, Password: sourceDB.Password, DBName:   sourceDB.DBName, SSLMode:  "disable",
		},
		DstDB: config.DatabaseConfig{
			Dialect:  targetDB.Dialect, Host:     targetDB.Host, Port:     mustPortInt(t, targetDB.Port),
			User:     targetDB.Username, Password: targetDB.Password, DBName:   targetDB.DBName, SSLMode:  "disable",
		},
		MaxRetries:    2,
		RetryInterval: 2 * time.Second,
		DebugMode:     true,
	}

	// Pemetaan tipe internal akan dimuat oleh NewOrchestrator atau dari main.go
	// Jika ada keraguan, bisa panggil config.LoadTypeMappings("", dbsync_logger.Log) di sini.

	srcConnInternal := &dbsync_db.Connector{DB: sourceDB.DB, Dialect: sourceDB.Dialect}
	dstConnInternal := &dbsync_db.Connector{DB: targetDB.DB, Dialect: targetDB.Dialect}

	metricsStore := dbsync_metrics.NewMetricsStore()
	orchestrator := dbsync_sync.NewOrchestrator(srcConnInternal, dstConnInternal, cfg, dbsync_logger.Log, metricsStore)
	results := orchestrator.Run(ctx)

	require.NotEmpty(t, results, "Orchestrator Run returned no results")
	// Karena SkipFailedTables=false, kita harapkan semua tabel diproses tanpa error SKIP.
	// Jika ada error DDL/Data, results[tableName].SchemaExecutionError atau .DataError akan diisi.

	// Verifikasi hasil untuk setiap tabel
	expectedTableNames := []string{"users", "categories", "posts", "comments", "post_categories"}
	require.Len(t, results, len(expectedTableNames), "Expected results for %d tables", len(expectedTableNames))

	for _, tableName := range expectedTableNames {
		t.Run("VerifyOverallSuccess_"+tableName, func(t *testing.T) {
			res, ok := results[tableName]
			require.True(t, ok, "Result for table '%s' not found", tableName)
			// Dengan SkipFailedTables=false, error ini seharusnya tidak terjadi atau tes akan gagal lebih awal
			// karena error di dalam orchestrator.Run akan menyebabkan panic atau return error.
			// Namun, jika orchestrator.Run mengembalikan hasil parsial, kita cek di sini.
			assert.NoError(t, res.SchemaAnalysisError, "Schema analysis error for %s: %v", tableName, res.SchemaAnalysisError)
			assert.NoError(t, res.SchemaExecutionError, "Schema execution error for %s: %v", tableName, res.SchemaExecutionError)
			assert.NoError(t, res.DataError, "Data error for %s: %v", tableName, res.DataError)
			// ConstraintExecutionError adalah satu-satunya yang mungkin terjadi dan masih dianggap "sukses parsial" oleh processSyncResults.
			// Jika ConstraintExecutionError ada, tes ini mungkin akan gagal pada verifikasi FK di bawah.
			// Kita bisa assert.NoError(t, res.ConstraintExecutionError) jika ingin FK selalu berhasil.
			// Atau biarkan verifikasi FK spesifik menangkapnya.
			if res.ConstraintExecutionError != nil {
				t.Logf("Warning: Constraint execution error for %s: %v. FK verification might fail.", tableName, res.ConstraintExecutionError)
			}
			assert.False(t, res.Skipped, "Table '%s' was skipped, reason: %s", tableName, res.SkipReason)
		})
	}


	// Verifikasi tabel 'users' (skema dan data)
	t.Run("VerifyTableSchemaAndData_users", func(t *testing.T) {
		resUsers, _ := results["users"]
		assert.EqualValues(t, 3, resUsers.RowsSynced, "Row count mismatch for users")

		var userColumns []struct {
			ColumnName    string         `gorm:"column:column_name"`
			DataType      string         `gorm:"column:data_type"`
			UdtName       string         `gorm:"column:udt_name"`
			IsNullable    string         `gorm:"column:is_nullable"`
			ColumnDefault sql.NullString `gorm:"column:column_default"`
		}
		queryCtx, queryCancel := context.WithTimeout(ctx, 10*time.Second)
		defer queryCancel()
		errQuery := targetDB.DB.WithContext(queryCtx).Raw(`
			SELECT column_name, data_type, udt_name, is_nullable, column_default
			FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = 'users'
			ORDER BY ordinal_position
		`).Scan(&userColumns).Error
		require.NoError(t, errQuery, "Failed to query target schema for users")

		expectedUserColumns := map[string]struct{ TargetType, IsNullable string }{
			"id":         {TargetType: "integer", IsNullable: "NO"}, "username":   {TargetType: "character varying", IsNullable: "NO"},
			"email":      {TargetType: "character varying", IsNullable: "NO"}, "full_name":  {TargetType: "character varying", IsNullable: "YES"},
			"bio":        {TargetType: "text", IsNullable: "YES"}, "age":        {TargetType: "smallint", IsNullable: "YES"},
			"salary":     {TargetType: "numeric", IsNullable: "YES"}, "is_active":  {TargetType: "boolean", IsNullable: "YES"},
			"created_at": {TargetType: "timestamp with time zone", IsNullable: "YES"}, "updated_at": {TargetType: "timestamp with time zone", IsNullable: "YES"},
		}
		require.Len(t, userColumns, len(expectedUserColumns), "Incorrect number of columns in target users table")
		for _, col := range userColumns {
			expected, ok := expectedUserColumns[col.ColumnName]
			require.True(t, ok, "Unexpected column '%s' in target users table", col.ColumnName)
			actualDataType := strings.ToLower(col.DataType)
			if col.DataType == "USER-DEFINED" || col.DataType == "ARRAY" { actualDataType = strings.ToLower(strings.TrimPrefix(col.UdtName, "_")) }
			assert.Equal(t, expected.TargetType, actualDataType, "Data type mismatch for column '%s'", col.ColumnName)
			assert.Equal(t, expected.IsNullable, col.IsNullable, "Nullability mismatch for column '%s'", col.ColumnName)
		}
	})

	// Verifikasi tabel 'posts' (data)
	t.Run("VerifyTableData_posts", func(t *testing.T) {
		resPosts, _ := results["posts"]
		assert.EqualValues(t, 4, resPosts.RowsSynced)
	})

	// Verifikasi tabel 'comments' (data)
	t.Run("VerifyTableData_comments", func(t *testing.T) {
		resComments, _ := results["comments"]
		assert.EqualValues(t, 5, resComments.RowsSynced) // Sesuai data di source_schema.sql
	})


	// --- Verifikasi Foreign Key Constraints ---
	// Query ini lebih umum dan mencari berdasarkan nama constraint yang DIHARAPKAN ada.
	// Nama constraint mungkin perlu disesuaikan jika `dbsync` menamakannya secara berbeda
	// dari yang didefinisikan di source_schema.sql saat membuat ulang di target.
	// Jika nama constraint tidak penting, Anda bisa query berdasarkan tabel & kolom yang terlibat.

	t.Run("VerifyForeignKey_posts_to_users", func(t *testing.T) {
		var constraintName string
		// Cari nama constraint FK dari posts(author_id) ke users(id)
		// Ini mungkin sedikit rumit karena nama constraint bisa berbeda jika auto-generate
		query := `
            SELECT kcu.constraint_name
            FROM information_schema.referential_constraints rc
            JOIN information_schema.key_column_usage kcu
                 ON kcu.constraint_name = rc.constraint_name AND kcu.constraint_schema = rc.constraint_schema
            JOIN information_schema.constraint_column_usage ccu
                 ON ccu.constraint_name = rc.constraint_name AND ccu.constraint_schema = rc.constraint_schema
            WHERE kcu.table_schema = current_schema() AND kcu.table_name = 'posts' AND kcu.column_name = 'author_id'
              AND ccu.table_schema = current_schema() AND ccu.table_name = 'users' AND ccu.column_name = 'id';
        `
		errQuery := targetDB.DB.WithContext(ctx).Raw(query).Scan(&constraintName).Error
		if errQuery == sql.ErrNoRows { // gorm.ErrRecordNotFound jika pakai GORM langsung
			t.Fatalf("Foreign key from posts(author_id) to users(id) NOT FOUND")
		}
		require.NoError(t, errQuery, "Error querying for FK posts to users")
		assert.NotEmpty(t, constraintName, "Foreign key from posts(author_id) to users(id) should exist and have a name")
		t.Logf("Found FK from posts to users with name: %s", constraintName)
	})

	t.Run("VerifyForeignKey_comments_to_posts", func(t *testing.T) {
		// Jika error SQLSTATE 23503 terjadi pada constraint fk_comment_post, tes ini akan gagal.
		var constraintName string
		// Ekspektasi nama constraint adalah 'fk_comment_post' seperti di source_schema.sql
		// Jika dbsync membuat ulang dengan nama lain, query ini perlu disesuaikan.
		// Lebih baik mencari berdasarkan kolom yang terlibat.
		query := `
            SELECT kcu.constraint_name
            FROM information_schema.referential_constraints rc
            JOIN information_schema.key_column_usage kcu
                 ON kcu.constraint_name = rc.constraint_name AND kcu.constraint_schema = rc.constraint_schema
            JOIN information_schema.constraint_column_usage ccu
                 ON ccu.constraint_name = rc.constraint_name AND ccu.constraint_schema = rc.constraint_schema
            WHERE kcu.table_schema = current_schema() AND kcu.table_name = 'comments' AND kcu.column_name = 'post_id'
              AND ccu.table_schema = current_schema() AND ccu.table_name = 'posts' AND ccu.column_name = 'post_id';
        `
		errQuery := targetDB.DB.WithContext(ctx).Raw(query).Scan(&constraintName).Error
        // Periksa apakah ada error pada hasil sinkronisasi untuk tabel 'comments'
        resComments, ok := results["comments"]
        if ok && resComments.ConstraintExecutionError != nil {
             t.Errorf("Constraint execution for 'comments' table failed: %v. This FK check might fail or reflect an absent FK.", resComments.ConstraintExecutionError)
             // Jika kita tahu FK gagal dibuat, kita bisa assert bahwa query di atas *tidak* menemukan baris.
             if errQuery == sql.ErrNoRows || errQuery == gorm.ErrRecordNotFound { // gorm.ErrRecordNotFound jika menggunakan GORM
                 t.Logf("As expected, FK 'fk_comment_post' was not created due to error: %v", resComments.ConstraintExecutionError)
                 return // Tes untuk FK ini selesai, karena kita tahu itu gagal.
             }
        }

		if errQuery == sql.ErrNoRows || errQuery == gorm.ErrRecordNotFound {
			t.Fatalf("Foreign key from comments(post_id) to posts(post_id) (expected 'fk_comment_post' or similar) NOT FOUND")
		}
		require.NoError(t, errQuery, "Error querying for FK comments to posts")
		assert.NotEmpty(t, constraintName, "Foreign key from comments(post_id) to posts(post_id) should exist and have a name")
		t.Logf("Found FK from comments to posts with name: %s", constraintName)
	})

	// Tambahkan verifikasi FK untuk post_categories jika perlu
}
