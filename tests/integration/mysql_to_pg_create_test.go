//go:build integration
// Pastikan build tag sudah benar

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
	"gorm.io/gorm"

	"github.com/arwahdevops/dbsync/internal/config"
	dbsync_db "github.com/arwahdevops/dbsync/internal/db"
	dbsync_logger "github.com/arwahdevops/dbsync/internal/logger"
	dbsync_metrics "github.com/arwahdevops/dbsync/internal/metrics"
	dbsync_sync "github.com/arwahdevops/dbsync/internal/sync"
)

func TestMySQLToPostgres_CreateStrategy_WithFKVerification(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION_TESTS") != "" || testing.Short() {
		t.Skip("Skipping integration test.")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute) // Durasi bisa disesuaikan
	defer cancel()

	// Inisialisasi logger untuk pengujian
	errLoggerInit := dbsync_logger.Init(true, false) // Debug mode true, JSON output false
	require.NoError(t, errLoggerInit, "Failed to initialize logger for test")
	testLogger := dbsync_logger.Log.Named("mysql_to_pg_create_test")

	// Memulai kontainer database
	sourceDB := startMySQLContainer(ctx, t)
	targetDB := startPostgresContainer(ctx, t)
	defer stopContainer(ctx, t, sourceDB)
	defer stopContainer(ctx, t, targetDB)

	// Bersihkan dan siapkan skema sumber
	sourceTablesToDrop := []string{"comments", "post_categories", "posts", "categories", "users"}
	for _, tableName := range sourceTablesToDrop {
		if err := sourceDB.DB.Exec("DROP TABLE IF EXISTS " + tableName + ";").Error; err != nil {
			t.Logf("Warning: Failed to drop source table %s (may not exist): %v", tableName, err)
		}
	}
	executeSQLFile(t, sourceDB.DB, filepath.Join("testdata", "mysql_to_pg_create", "source_schema.sql"))

	// Bersihkan skema target (jika ada sisa dari run sebelumnya)
	targetTablesToDrop := []string{"comments", "post_categories", "posts", "categories", "users"}
	for _, tableName := range targetTablesToDrop {
		if err := targetDB.DB.Exec("DROP TABLE IF EXISTS " + tableName + " CASCADE;").Error; err != nil {
			t.Logf("Warning: Failed to drop target table %s (may not exist): %v", tableName, err)
		}
	}

	// Konfigurasi dbsync
	cfg := &config.Config{
		SyncDirection:          "mysql-to-postgres",
		SchemaSyncStrategy:     config.SchemaSyncDropCreate, // Atau SchemaSyncCreate jika itu yang dimaksud
		BatchSize:              100,
		Workers:                2,
		TableTimeout:           3 * time.Minute, // Tingkatkan sedikit untuk CI yang lambat
		SkipFailedTables:       false,
		DisableFKDuringSync:    true, // Penting untuk alur ini
		SrcDB: config.DatabaseConfig{
			Dialect:  sourceDB.Dialect, Host: sourceDB.Host, Port: mustPortInt(t, sourceDB.Port),
			User:     sourceDB.Username, Password: sourceDB.Password, DBName: sourceDB.DBName, SSLMode:  "disable",
		},
		DstDB: config.DatabaseConfig{
			Dialect:  targetDB.Dialect, Host: targetDB.Host, Port: mustPortInt(t, targetDB.Port),
			User:     targetDB.Username, Password: targetDB.Password, DBName: targetDB.DBName, SSLMode:  "disable",
		},
		MaxRetries:    2,
		RetryInterval: 2 * time.Second,
		DebugMode:     true, // Aktifkan debug mode untuk log yang lebih detail
	}

	// Inisialisasi konektor dbsync internal
	srcConnInternal := &dbsync_db.Connector{DB: sourceDB.DB, Dialect: sourceDB.Dialect}
	dstConnInternal := &dbsync_db.Connector{DB: targetDB.DB, Dialect: targetDB.Dialect}

	// Muat pemetaan tipe internal (sekarang otomatis oleh Orchestrator jika belum, atau oleh main)
	// Tidak perlu panggilan eksplisit ke config.LoadTypeMappings(testLogger) di sini jika main sudah melakukannya.
	// Jika ini adalah tes unit murni untuk Orchestrator, maka perlu dipanggil.
	// Untuk integration test yang menjalankan flow mirip main, kita asumsikan sudah termuat.
	// Jika ragu, tambahkan:
	errLoadMappings := config.LoadTypeMappings(testLogger)
	require.NoError(t, errLoadMappings, "Failed to load internal type mappings for test")


	// Jalankan sinkronisasi
	metricsStore := dbsync_metrics.NewMetricsStore()
	orchestrator := dbsync_sync.NewOrchestrator(srcConnInternal, dstConnInternal, cfg, testLogger, metricsStore)
	results := orchestrator.Run(ctx)

	// Verifikasi hasil umum
	require.NotEmpty(t, results, "Orchestrator Run returned no results")
	// Urutan dari source_schema.sql: users, categories, posts, post_categories, comments
	// Orchestrator akan mengurutkan berdasarkan dependensi FK
	expectedTableNames := []string{"users", "categories", "posts", "comments", "post_categories"}
	require.Len(t, results, len(expectedTableNames), "Expected results for %d tables, got %d. Results map: %+v", len(expectedTableNames), len(results), results)

	for _, tableName := range expectedTableNames {
		t.Run("VerifyOverallSuccess_"+tableName, func(t *testing.T) {
			res, ok := results[tableName]
			require.True(t, ok, "Result for table '%s' not found", tableName)
			assert.NoError(t, res.SchemaAnalysisError, "Schema analysis error for %s: %v", tableName, res.SchemaAnalysisError)
			assert.NoError(t, res.SchemaExecutionError, "Schema execution error for %s: %v", tableName, res.SchemaExecutionError)
			assert.NoError(t, res.DataError, "Data error for %s: %v", tableName, res.DataError)
			// ConstraintExecutionError BISA terjadi jika ada masalah data yang melanggar FK setelah data disinkronkan.
			// Untuk drop_create dengan data bersih dari sumber, ini seharusnya tidak terjadi jika urutan benar.
			assert.NoError(t, res.ConstraintExecutionError, "Constraint execution error for %s: %v", tableName, res.ConstraintExecutionError)
			assert.False(t, res.Skipped, "Table '%s' was skipped, reason: %s", tableName, res.SkipReason)
		})
	}

	// Verifikasi Skema dan Data untuk tabel 'users'
	t.Run("VerifyTableSchemaAndData_users", func(t *testing.T) {
		resUsers, _ := results["users"]
		assert.EqualValues(t, 3, resUsers.RowsSynced, "Row count mismatch for users table")

		var userColumnsInfo []struct {
			ColumnName             string         `gorm:"column:column_name"`
			DataType               string         `gorm:"column:data_type"`
			UdtName                string         `gorm:"column:udt_name"` // Penting untuk PG
			IsNullable             string         `gorm:"column:is_nullable"`
			ColumnDefault          sql.NullString `gorm:"column:column_default"`
			CharacterMaximumLength sql.NullInt64  `gorm:"column:character_maximum_length"`
			NumericPrecision       sql.NullInt64  `gorm:"column:numeric_precision"`
			NumericScale           sql.NullInt64  `gorm:"column:numeric_scale"`
		}
		queryCtx, queryCancel := context.WithTimeout(ctx, 15*time.Second)
		defer queryCancel()
		errQuerySchema := targetDB.DB.WithContext(queryCtx).Raw(`
			SELECT column_name, data_type, udt_name, is_nullable, column_default,
			       character_maximum_length, numeric_precision, numeric_scale
			FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = 'users'
			ORDER BY ordinal_position
		`).Scan(&userColumnsInfo).Error
		require.NoError(t, errQuerySchema, "Failed to query target schema for users table")

		// Ekspektasi setelah mapping dari MySQL ke PostgreSQL
		expectedUserSchema := map[string]struct {
			TargetType     string
			IsNullable     string
			CharMaxLen     sql.NullInt64
			NumPrecision   sql.NullInt64
			NumScale       sql.NullInt64
			ExpectedDefault sql.NullString
		}{
			"id":          {TargetType: "integer", IsNullable: "NO"}, // Default SERIAL/IDENTITY akan ditangani oleh PG
			"username":    {TargetType: "character varying", IsNullable: "NO", CharMaxLen: sql.NullInt64{Int64: 50, Valid: true}},
			"email":       {TargetType: "character varying", IsNullable: "NO", CharMaxLen: sql.NullInt64{Int64: 100, Valid: true}},
			"full_name":   {TargetType: "character varying", IsNullable: "YES", CharMaxLen: sql.NullInt64{Int64: 100, Valid: true}},
			"bio":         {TargetType: "text", IsNullable: "YES"},
			"age":         {TargetType: "smallint", IsNullable: "YES"}, // MySQL TINYINT(3) unsigned -> PG SMALLINT
			"salary":      {TargetType: "numeric", IsNullable: "YES", NumPrecision: sql.NullInt64{Int64: 10, Valid: true}, NumScale: sql.NullInt64{Int64: 2, Valid: true}},
			"is_active":   {TargetType: "boolean", IsNullable: "YES", ExpectedDefault: sql.NullString{String: "true", Valid: true}}, // MySQL TINYINT(1) DEFAULT 1 -> PG BOOLEAN DEFAULT TRUE
			"created_at":  {TargetType: "timestamp with time zone", IsNullable: "YES"}, // MySQL TIMESTAMP DEFAULT CURRENT_TIMESTAMP -> PG TIMESTAMPTZ (default akan beda)
			"updated_at":  {TargetType: "timestamp with time zone", IsNullable: "YES"},
		}
		require.Len(t, userColumnsInfo, len(expectedUserSchema), "Incorrect number of columns in target users table")

		for _, col := range userColumnsInfo {
			expected, ok := expectedUserSchema[col.ColumnName]
			require.True(t, ok, "Unexpected column '%s' in target users table", col.ColumnName)

			actualDataType := strings.ToLower(col.DataType)
			if col.DataType == "USER-DEFINED" || col.DataType == "ARRAY" || col.DataType == "timestamp with time zone" || col.DataType == "timestamp without time zone" {
				actualDataType = strings.ToLower(col.UdtName)
				if strings.HasPrefix(actualDataType, "_") { // Untuk array, udt_name diawali '_'
					actualDataType = strings.TrimPrefix(actualDataType, "_") + "[]"
				}
			} else if col.DataType == "character varying" && col.UdtName == "varchar" {
				actualDataType = "character varying" // Konsistensi
			} else if col.DataType == "numeric" && col.UdtName == "numeric" {
				actualDataType = "numeric"
			} else if col.DataType == "text" && col.UdtName == "text" {
				actualDataType = "text"
			}


			assert.Equal(t, expected.TargetType, actualDataType, "Data type mismatch for column '%s'", col.ColumnName)
			assert.Equal(t, expected.IsNullable, col.IsNullable, "Nullability mismatch for column '%s'", col.ColumnName)

			if expected.CharMaxLen.Valid {
				assert.Equal(t, expected.CharMaxLen.Int64, col.CharacterMaximumLength.Int64, "CharacterMaximumLength mismatch for column '%s'", col.ColumnName)
			}
			if expected.NumPrecision.Valid {
				assert.Equal(t, expected.NumPrecision.Int64, col.NumericPrecision.Int64, "NumericPrecision mismatch for column '%s'", col.ColumnName)
			}
			if expected.NumScale.Valid {
				assert.Equal(t, expected.NumScale.Int64, col.NumericScale.Int64, "NumericScale mismatch for column '%s'", col.ColumnName)
			}
			// Verifikasi default value bisa rumit karena normalisasi. Untuk 'is_active', PG akan menampilkannya sebagai 'true'.
			// Default CURRENT_TIMESTAMP akan berbeda antara MySQL dan PG (now() vs CURRENT_TIMESTAMP).
			if col.ColumnName == "is_active" {
				require.True(t, col.ColumnDefault.Valid, "Default value for 'is_active' should be set")
				assert.Equal(t, "true", col.ColumnDefault.String, "Default value mismatch for 'is_active'")
			}
		}
	})

	// Verifikasi Jumlah Data
	t.Run("VerifyDataCount_categories", func(t *testing.T) { res, _ := results["categories"]; assert.EqualValues(t, 2, res.RowsSynced) })
	t.Run("VerifyDataCount_posts", func(t *testing.T) { res, _ := results["posts"]; assert.EqualValues(t, 4, res.RowsSynced) })
	t.Run("VerifyDataCount_comments", func(t *testing.T) { res, _ := results["comments"]; assert.EqualValues(t, 5, res.RowsSynced) }) // Seharusnya 5 jika semua masuk
	t.Run("VerifyDataCount_post_categories", func(t *testing.T) { res, _ := results["post_categories"]; assert.EqualValues(t, 4, res.RowsSynced) })


	// Verifikasi Foreign Keys (setelah perbaikan urutan tabel)
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

	t.Run("VerifyForeignKey_posts_to_users", func(t *testing.T) {
		verifyForeignKeyExists(t, targetDB.DB, "posts", "author_id", "users", "id")
	})
	t.Run("VerifyForeignKey_comments_to_posts", func(t *testing.T) {
		verifyForeignKeyExists(t, targetDB.DB, "comments", "post_id", "posts", "post_id")
	})
	t.Run("VerifyForeignKey_comments_to_users", func(t *testing.T) {
		verifyForeignKeyExists(t, targetDB.DB, "comments", "user_id", "users", "id")
	})
	t.Run("VerifyForeignKey_post_categories_to_posts", func(t *testing.T) {
		verifyForeignKeyExists(t, targetDB.DB, "post_categories", "post_id", "posts", "post_id")
	})
	t.Run("VerifyForeignKey_post_categories_to_categories", func(t *testing.T) {
		verifyForeignKeyExists(t, targetDB.DB, "post_categories", "category_id", "categories", "category_id")
	})

	// Verifikasi Indeks Unik
	verifyUniqueIndexExists := func(t *testing.T, db *gorm.DB, tableName, indexNamePrefix, columnName string) {
		t.Helper()
		var indexDef string
		// Nama indeks di PG bisa berbeda dari MySQL, jadi kita cari berdasarkan kolom
		// Ini lebih rapuh, idealnya kita tahu nama indeks yang diharapkan atau mencari indeks UNIK pada kolom tertentu
		// Untuk `users_email_key` atau `users_username_key` (nama default constraint UNIQUE PG)
		// Atau nama indeks yang dibuat `dbsync` jika berbeda
		query := `
			SELECT indexdef FROM pg_indexes
			WHERE schemaname = current_schema() AND tablename = ? AND indexdef LIKE ? AND indexdef LIKE '%UNIQUE%';
		`
		// Cari indeks unik yang mengandung nama kolom
		likePattern := "%(" + columnName + ")%" // atau variasi yang lebih spesifik
		if indexNamePrefix != "" {
			likePattern = indexNamePrefix + "%" // Jika kita tahu prefix nama constraint/indeksnya
		}

		queryCtx, queryCancel := context.WithTimeout(ctx, 10*time.Second)
		defer queryCancel()
		errQuery := db.WithContext(queryCtx).Raw(query, tableName, likePattern).Scan(&indexDef).Error

		if errQuery == gorm.ErrRecordNotFound {
			t.Errorf("UNIQUE constraint/index on %s(%s) (expected prefix: '%s') NOT FOUND", tableName, columnName, indexNamePrefix)
			return
		}
		require.NoError(t, errQuery, "Error querying for UNIQUE index on %s(%s)", tableName, columnName)
		assert.NotEmpty(t, indexDef, "UNIQUE constraint/index on %s(%s) should exist", tableName, columnName)
		t.Logf("Found UNIQUE index/constraint on %s(%s): %s", tableName, columnName, indexDef)
	}

	t.Run("VerifyUniqueConstraint_users_username", func(t *testing.T) {
		// Nama constraint UNIQUE di PG biasanya `users_username_key` jika dibuat otomatis oleh `UNIQUE`
		// atau nama yang diberikan jika `ADD CONSTRAINT ... UNIQUE`
		verifyUniqueIndexExists(t, targetDB.DB, "users", "users_username_key", "username")
	})
	t.Run("VerifyUniqueConstraint_users_email", func(t *testing.T) {
		verifyUniqueIndexExists(t, targetDB.DB, "users", "users_email_key", "email")
	})
	t.Run("VerifyUniqueConstraint_categories_name", func(t *testing.T) {
		verifyUniqueIndexExists(t, targetDB.DB, "categories", "categories_name_key", "name")
	})
	t.Run("VerifyUniqueConstraint_posts_slug", func(t *testing.T) {
		verifyUniqueIndexExists(t, targetDB.DB, "posts", "posts_slug_key", "slug")
	})
}
