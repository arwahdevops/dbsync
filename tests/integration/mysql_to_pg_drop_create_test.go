//go:build nanti
// Pastikan build tag sudah benar, atau sesuai dengan yang Anda gunakan.
// Jika tes ini dan _create_test.go bisa berjalan bersamaan, tagnya bisa sama.
// Jika tidak, Anda bisa menggunakan tag yang berbeda, misal: //go:build integration_dropcreate

package integration

import (
	"context"
	"database/sql" // Untuk sql.NullString dll.
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/arwahdevops/dbsync/internal/config"
	dbsync_db "github.com/arwahdevops/dbsync/internal/db"
	dbsync_logger "github.com/arwahdevops/dbsync/internal/logger"
	dbsync_metrics "github.com/arwahdevops/dbsync/internal/metrics"
	dbsync_sync "github.com/arwahdevops/dbsync/internal/sync"
)

func TestMySQLToPostgres_DropCreateStrategy(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION_TESTS") != "" || testing.Short() {
		t.Skip("Skipping integration test (DROP_CREATE strategy).")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute) // Durasi mungkin perlu penyesuaian
	defer cancel()

	errLoggerInit := dbsync_logger.Init(true, false)
	require.NoError(t, errLoggerInit, "Failed to initialize logger for test")
	testLogger := dbsync_logger.Log.Named("mysql_to_pg_drop_create_test")

	sourceDB := startMySQLContainer(ctx, t)
	targetDB := startPostgresContainer(ctx, t)
	defer stopContainer(ctx, t, sourceDB)
	defer stopContainer(ctx, t, targetDB)

	// 1. Setup Skema dan Data Sumber
	sourceTablesToDrop := []string{"comments", "post_categories", "posts", "categories", "users", "products", "orders"} // Tambahkan tabel baru jika ada
	for _, tableName := range sourceTablesToDrop {
		if err := sourceDB.DB.Exec("DROP TABLE IF EXISTS " + tableName + ";").Error; err != nil {
			t.Logf("Warning: Failed to drop source table %s (may not exist): %v", tableName, err)
		}
	}
	// Gunakan source_schema.sql yang sama dengan _create_test jika relevan, atau buat yang baru
	executeSQLFile(t, sourceDB.DB, filepath.Join("testdata", "mysql_to_pg_drop_create", "source_schema.sql"))

	// 2. Setup Skema Awal yang Berbeda di Target (untuk di-drop dan recreate)
	targetTablesToDrop := []string{"comments", "post_categories", "posts", "categories", "users", "products", "orders"}
	for _, tableName := range targetTablesToDrop {
		if err := targetDB.DB.Exec("DROP TABLE IF EXISTS " + tableName + " CASCADE;").Error; err != nil {
			t.Logf("Warning: Failed to drop target table %s (may not exist): %v", tableName, err)
		}
	}

	// Buat tabel 'users' di target dengan skema yang berbeda dari sumber
	errCreateTargetUsers := targetDB.DB.Exec(`
		CREATE TABLE users (
			user_id_old SERIAL PRIMARY KEY,
			legacy_username VARCHAR(50) NOT NULL,
			email_old VARCHAR(70) UNIQUE,
			account_status VARCHAR(10) DEFAULT 'pending'
		);
		INSERT INTO users (legacy_username, email_old) VALUES ('old_user1', 'old1@example.com');
	`).Error
	require.NoError(t, errCreateTargetUsers, "Failed to create initial different target schema for users")

	// Buat tabel 'posts' di target dengan skema yang berbeda
	errCreateTargetPosts := targetDB.DB.Exec(`
		CREATE TABLE posts (
			post_identifier INT PRIMARY KEY,
			old_title TEXT,
			created_by INT -- Tanpa FK untuk menunjukkan perbedaan
		);
		INSERT INTO posts (post_identifier, old_title) VALUES (999, 'Legacy Post Title');
	`).Error
	require.NoError(t, errCreateTargetPosts, "Failed to create initial different target schema for posts")

	// 3. Konfigurasi dbsync
	cfg := &config.Config{
		SyncDirection:          "mysql-to-postgres",
		SchemaSyncStrategy:     config.SchemaSyncDropCreate, // Eksplisit drop_create
		BatchSize:              50,                           // Ukuran batch lebih kecil untuk pengujian
		Workers:                2,
		TableTimeout:           3 * time.Minute,
		SkipFailedTables:       false,
		DisableFKDuringSync:    true, // Penting untuk drop_create
		SrcDB: config.DatabaseConfig{
			Dialect:  sourceDB.Dialect, Host: sourceDB.Host, Port: mustPortInt(t, sourceDB.Port),
			User:     sourceDB.Username, Password: sourceDB.Password, DBName: sourceDB.DBName, SSLMode:  "disable",
		},
		DstDB: config.DatabaseConfig{
			Dialect:  targetDB.Dialect, Host: targetDB.Host, Port: mustPortInt(t, targetDB.Port),
			User:     targetDB.Username, Password: targetDB.Password, DBName: targetDB.DBName, SSLMode:  "disable",
		},
		MaxRetries:    1, // Kurangi retry untuk tes agar lebih cepat gagal jika ada masalah
		RetryInterval: 1 * time.Second,
		DebugMode:     true,
	}

	// Inisialisasi konektor dbsync internal
	srcConnInternal := &dbsync_db.Connector{DB: sourceDB.DB, Dialect: sourceDB.Dialect}
	dstConnInternal := &dbsync_db.Connector{DB: targetDB.DB, Dialect: targetDB.Dialect}

	// Muat pemetaan tipe internal
	errLoadMappings := config.LoadTypeMappings(testLogger)
	require.NoError(t, errLoadMappings, "Failed to load internal type mappings for test")

	// 4. Jalankan Sinkronisasi
	metricsStore := dbsync_metrics.NewMetricsStore()
	orchestrator := dbsync_sync.NewOrchestrator(srcConnInternal, dstConnInternal, cfg, testLogger, metricsStore)
	results := orchestrator.Run(ctx)

	// 5. Verifikasi Hasil
	require.NotEmpty(t, results, "Orchestrator Run returned no results")
	// Sesuaikan dengan jumlah tabel di source_schema.sql yang digunakan untuk tes ini
	expectedTableNames := []string{"users", "categories", "posts", "comments", "post_categories"}
	require.Len(t, results, len(expectedTableNames), "Expected results for %d tables, got %d. Results map: %+v", len(expectedTableNames), len(results), results)

	for _, tableName := range expectedTableNames {
		t.Run("VerifyOverallSuccess_"+tableName, func(t *testing.T) {
			res, ok := results[tableName]
			require.True(t, ok, "Result for table '%s' not found", tableName)
			assert.NoError(t, res.SchemaAnalysisError, "Schema analysis error for %s: %v", tableName, res.SchemaAnalysisError)
			assert.NoError(t, res.SchemaExecutionError, "Schema execution error for %s: %v", tableName, res.SchemaExecutionError)
			assert.NoError(t, res.DataError, "Data error for %s: %v", tableName, res.DataError)
			assert.NoError(t, res.ConstraintExecutionError, "Constraint execution error for %s: %v", tableName, res.ConstraintExecutionError)
			assert.False(t, res.Skipped, "Table '%s' was skipped, reason: %s", tableName, res.SkipReason)
		})
	}

	// Verifikasi bahwa skema tabel 'users' telah di-drop dan di-recreate sesuai sumber
	t.Run("VerifyTable_users_RecreatedToSourceSchema", func(t *testing.T) {
		resUsers, _ := results["users"]
		assert.EqualValues(t, 3, resUsers.RowsSynced, "Row count mismatch for users table after recreate") // Sesuai data sumber

		var userColumnsInfo []struct {
			ColumnName             string         `gorm:"column:column_name"`
			DataType               string         `gorm:"column:data_type"`
			UdtName                string         `gorm:"column:udt_name"`
			IsNullable             string         `gorm:"column:is_nullable"`
			ColumnDefault          sql.NullString `gorm:"column:column_default"`
			CharacterMaximumLength sql.NullInt64  `gorm:"column:character_maximum_length"`
		}
		queryCtx, queryCancel := context.WithTimeout(ctx, 15*time.Second)
		defer queryCancel()
		errQuerySchema := targetDB.DB.WithContext(queryCtx).Raw(`
			SELECT column_name, data_type, udt_name, is_nullable, column_default, character_maximum_length
			FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = 'users'
			ORDER BY ordinal_position
		`).Scan(&userColumnsInfo).Error
		require.NoError(t, errQuerySchema, "Failed to query target schema for users table after recreate")

		// Kolom yang diharapkan sesuai dengan skema SUMBER (setelah mapping ke PG)
		expectedSourceUserColNames := []string{"id", "username", "email", "full_name", "bio", "age", "salary", "is_active", "created_at", "updated_at"}
		actualUserColNames := make([]string, len(userColumnsInfo))
		foundOldColumn := false
		for i, col := range userColumnsInfo {
			actualUserColNames[i] = col.ColumnName
			if col.ColumnName == "user_id_old" || col.ColumnName == "legacy_username" {
				foundOldColumn = true
			}
		}
		assert.False(t, foundOldColumn, "Old columns (user_id_old, legacy_username) from initial target schema should not exist in 'users' table")
		assert.ElementsMatch(t, expectedSourceUserColNames, actualUserColNames, "users table columns mismatch after recreate, should match source schema")

		// Verifikasi salah satu tipe kolom yang krusial
		var emailColInfo struct { DataType string; UdtName string; CharMaxLen sql.NullInt64 }
		for _, col := range userColumnsInfo { if col.ColumnName == "email" {
			emailColInfo.DataType = col.DataType
			emailColInfo.UdtName = col.UdtName
			emailColInfo.CharMaxLen = col.CharacterMaximumLength
			break
		}}
		assert.Equal(t, "character varying", strings.ToLower(emailColInfo.DataType), "Data type for 'email' column is incorrect")
		assert.True(t, emailColInfo.CharMaxLen.Valid && emailColInfo.CharMaxLen.Int64 == 100, "Length for 'email' column is incorrect, expected 100")


		// Verifikasi bahwa data lama hilang dan data baru dari sumber ada
		var targetUsersData []struct{ Username string }
		queryDataCtx, queryDataCancel := context.WithTimeout(ctx, 10*time.Second)
		defer queryDataCancel()
		errQueryData := targetDB.DB.WithContext(queryDataCtx).Table("users").Select("username").Order("id").Find(&targetUsersData).Error
		require.NoError(t, errQueryData)
		require.Len(t, targetUsersData, 3, "Data count for users table mismatch after recreate")
		assert.Equal(t, "john_doe", targetUsersData[0].Username, "First user's username incorrect, expected data from source")
	})

	// Verifikasi bahwa skema tabel 'posts' telah di-drop dan di-recreate
	t.Run("VerifyTable_posts_RecreatedToSourceSchema", func(t *testing.T) {
		resPosts, _ := results["posts"]
		assert.EqualValues(t, 4, resPosts.RowsSynced, "Row count mismatch for posts table after recreate")

		var postColumnsInfo []struct { ColumnName string `gorm:"column:column_name"` }
		queryCtx, queryCancel := context.WithTimeout(ctx, 10*time.Second)
		defer queryCancel()
		errQuerySchema := targetDB.DB.WithContext(queryCtx).Raw(
			"SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'posts' ORDER BY ordinal_position",
		).Scan(&postColumnsInfo).Error
		require.NoError(t, errQuerySchema, "Failed to query target schema for posts table after recreate")

		expectedSourcePostColNames := []string{"post_id", "author_id", "title", "content", "slug", "views", "published_at"} // Sesuai sumber
		actualPostColNames := make([]string, len(postColumnsInfo))
		foundOldColumnPosts := false
		for i, col := range postColumnsInfo {
			actualPostColNames[i] = col.ColumnName
			if col.ColumnName == "post_identifier" || col.ColumnName == "old_title" {
				foundOldColumnPosts = true
			}
		}
		assert.False(t, foundOldColumnPosts, "Old columns (post_identifier, old_title) from initial target schema should not exist in 'posts' table")
		assert.ElementsMatch(t, expectedSourcePostColNames, actualPostColNames, "posts table columns mismatch after recreate, should match source schema")

		// Verifikasi data
		var targetPostsData []struct{ Title string }
		queryDataCtx, queryDataCancel := context.WithTimeout(ctx, 10*time.Second)
		defer queryDataCancel()
		errQueryData := targetDB.DB.WithContext(queryDataCtx).Table("posts").Select("title").Order("post_id").Limit(1).Find(&targetPostsData).Error
		require.NoError(t, errQueryData)
		require.GreaterOrEqual(t, len(targetPostsData), 1, "Should have at least one post from source")
		assert.Equal(t, "First Post by John", targetPostsData[0].Title, "First post title incorrect, expected data from source")
	})


	// Verifikasi FK (sama seperti di _create_test.go, karena skema akhir harusnya identik)
	verifyForeignKeyExists := func(t *testing.T, db *gorm.DB, fromTable, fromColumn, toTable, toColumn string) {
		t.Helper()
		var constraintName string
		query := `
            SELECT kcu.constraint_name
            FROM information_schema.referential_constraints rc
            JOIN information_schema.key_column_usage kcu
                 ON kcu.constraint_name = rc.constraint_name AND kcu.constraint_schema = rc.constraint_schema
            JOIN information_schema.constraint_column_usage ccu
                 ON ccu.constraint_name = rc.constraint_name AND ccu.constraint_schema = rc.constraint_schema
            WHERE kcu.table_schema = current_schema() AND kcu.table_name = ? AND kcu.column_name = ?
              AND ccu.table_schema = current_schema() AND ccu.table_name = ? AND ccu.column_name = ?;
        `
		queryCtx, queryCancel := context.WithTimeout(ctx, 10*time.Second)
		defer queryCancel()
		errQuery := db.WithContext(queryCtx).Raw(query, fromTable, fromColumn, toTable, toColumn).Scan(&constraintName).Error

		if errQuery == gorm.ErrRecordNotFound {
			t.Errorf("Foreign key from %s(%s) to %s(%s) NOT FOUND", fromTable, fromColumn, toTable, toColumn)
			return
		}
		require.NoError(t, errQuery, "Error querying for FK %s(%s) -> %s(%s)", fromTable, fromColumn, toTable, toColumn)
		assert.NotEmpty(t, constraintName, "Foreign key from %s(%s) to %s(%s) should exist and have a name", fromTable, fromColumn, toTable, toColumn)
		t.Logf("Found FK from %s(%s) to %s(%s) with name: %s", fromTable, fromColumn, toTable, toColumn, constraintName)
	}

	t.Run("VerifyForeignKey_posts_to_users_AfterDropCreate", func(t *testing.T) {
		verifyForeignKeyExists(t, targetDB.DB, "posts", "author_id", "users", "id")
	})
	t.Run("VerifyForeignKey_comments_to_posts_AfterDropCreate", func(t *testing.T) {
		verifyForeignKeyExists(t, targetDB.DB, "comments", "post_id", "posts", "post_id")
	})
}
