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

// mustPortInt (bisa tetap di sini atau di db_helpers.go jika belum dipindah)
// Jika sudah ada di db_helpers.go, baris ini tidak perlu lagi.
// func mustPortInt(t *testing.T, port nat.Port) int {
// 	t.Helper()
// 	p, err := strconv.Atoi(port.Port())
// 	if err != nil {
// 		t.Fatalf("Failed to convert port %s to int: %v", port.Port(), err)
// 	}
// 	return p
// }

func TestMySQLToPostgres_CreateStrategy(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION_TESTS") != "" || testing.Short() {
		t.Skip("Skipping integration test.")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	err := dbsync_logger.Init(true, false)
	require.NoError(t, err, "Failed to initialize logger")

	sourceDB := startMySQLContainer(ctx, t)
	targetDB := startPostgresContainer(ctx, t)
	defer stopContainer(ctx, t, sourceDB)
	defer stopContainer(ctx, t, targetDB)

	// Setup Skema Awal di Source DB
	sourceDB.DB.Exec("DROP TABLE IF EXISTS posts;")
	sourceDB.DB.Exec("DROP TABLE IF EXISTS comments;")
	sourceDB.DB.Exec("DROP TABLE IF EXISTS post_categories;")
	sourceDB.DB.Exec("DROP TABLE IF EXISTS categories;")
	sourceDB.DB.Exec("DROP TABLE IF EXISTS users;") // Drop users terakhir karena FK
	executeSQLFile(t, sourceDB.DB, filepath.Join("testdata", "mysql_to_pg_create", "source_schema.sql"))

	// Pastikan target DB kosong
	targetDB.DB.Exec("DROP TABLE IF EXISTS comments CASCADE;")
	targetDB.DB.Exec("DROP TABLE IF EXISTS post_categories CASCADE;")
	targetDB.DB.Exec("DROP TABLE IF EXISTS categories CASCADE;")
	targetDB.DB.Exec("DROP TABLE IF EXISTS posts CASCADE;")
	targetDB.DB.Exec("DROP TABLE IF EXISTS users CASCADE;")

	cfg := &config.Config{
		SyncDirection:      "mysql-to-postgres",
		SchemaSyncStrategy: config.SchemaSyncDropCreate, // Menggunakan drop_create karena target kosong, efeknya sama dengan create
		BatchSize:          100,
		Workers:            2,
		TableTimeout:       2 * time.Minute, // Sedikit lebih lama untuk skema lebih kompleks
		SkipFailedTables:   false,
		SrcDB: config.DatabaseConfig{
			Dialect:  sourceDB.Dialect,
			Host:     sourceDB.Host,
			Port:     mustPortInt(t, sourceDB.Port),
			User:     sourceDB.Username,
			Password: sourceDB.Password,
			DBName:   sourceDB.DBName,
			SSLMode:  "disable",
		},
		DstDB: config.DatabaseConfig{
			Dialect:  targetDB.Dialect,
			Host:     targetDB.Host,
			Port:     mustPortInt(t, targetDB.Port),
			User:     targetDB.Username,
			Password: targetDB.Password,
			DBName:   targetDB.DBName,
			SSLMode:  "disable",
		},
		MaxRetries:    2, // Naikkan sedikit untuk CI yang mungkin lebih lambat
		RetryInterval: 2 * time.Second,
		DebugMode:     true,
		TypeMappingFilePath: filepath.Join("..", "..", "typemap.json"), // Path relatif dari file test ke typemap.json di root
	}
	err = config.LoadTypeMappings(cfg.TypeMappingFilePath, dbsync_logger.Log)
	require.NoError(t, err, "Failed to load type mappings from: %s", cfg.TypeMappingFilePath)


	srcConnInternal := &dbsync_db.Connector{DB: sourceDB.DB, Dialect: sourceDB.Dialect}
	dstConnInternal := &dbsync_db.Connector{DB: targetDB.DB, Dialect: targetDB.Dialect}

	metricsStore := dbsync_metrics.NewMetricsStore()
	orchestrator := dbsync_sync.NewOrchestrator(srcConnInternal, dstConnInternal, cfg, dbsync_logger.Log, metricsStore)
	results := orchestrator.Run(ctx)

	require.NotEmpty(t, results, "Orchestrator Run returned no results")
	require.Len(t, results, 5, "Expected results for 5 tables") // users, posts, comments, categories, post_categories

	// Verifikasi tabel 'users'
	t.Run("VerifyTable_users", func(t *testing.T) {
		resUsers, ok := results["users"]
		require.True(t, ok, "Result for table 'users' not found")
		require.NoError(t, resUsers.SchemaAnalysisError, "Schema analysis error for users: %v", resUsers.SchemaAnalysisError)
		require.NoError(t, resUsers.SchemaExecutionError, "Schema execution error for users: %v", resUsers.SchemaExecutionError)
		require.NoError(t, resUsers.DataError, "Data error for users: %v", resUsers.DataError)
		require.False(t, resUsers.Skipped, "Table 'users' was skipped, reason: %s", resUsers.SkipReason)
		assert.EqualValues(t, 3, resUsers.RowsSynced, "Row count mismatch for users") // Sesuai source_schema.sql

		var userColumns []struct {
			ColumnName    string         `gorm:"column:column_name"`
			DataType      string         `gorm:"column:data_type"`
			UdtName       string         `gorm:"column:udt_name"` // Untuk PG, tipe dasar ada di udt_name jika data_type 'USER-DEFINED' atau 'ARRAY'
			IsNullable    string         `gorm:"column:is_nullable"`
			ColumnDefault sql.NullString `gorm:"column:column_default"`
		}
		queryCtx, queryCancel := context.WithTimeout(ctx, 10*time.Second)
		defer queryCancel()
		err = targetDB.DB.WithContext(queryCtx).Raw(`
			SELECT column_name, data_type, udt_name, is_nullable, column_default
			FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = 'users'
			ORDER BY ordinal_position
		`).Scan(&userColumns).Error
		require.NoError(t, err, "Failed to query target schema for users")

		expectedUserColumns := map[string]struct{ TargetType, IsNullable string }{
			"id":         {TargetType: "integer", IsNullable: "NO"},    // MySQL INT -> PG INTEGER (default)
			"username":   {TargetType: "character varying", IsNullable: "NO"},
			"email":      {TargetType: "character varying", IsNullable: "NO"},
			"full_name":  {TargetType: "character varying", IsNullable: "YES"},
			"bio":        {TargetType: "text", IsNullable: "YES"},
			"age":        {TargetType: "smallint", IsNullable: "YES"},  // MySQL TINYINT UNSIGNED -> PG SMALLINT (dari typemap)
			"salary":     {TargetType: "numeric", IsNullable: "YES"},   // MySQL DECIMAL -> PG NUMERIC
			"is_active":  {TargetType: "boolean", IsNullable: "YES"},   // MySQL BOOLEAN (TINYINT(1)) -> PG BOOLEAN (dari special_mappings)
			"created_at": {TargetType: "timestamp with time zone", IsNullable: "YES"},
			"updated_at": {TargetType: "timestamp with time zone", IsNullable: "YES"},
		}
		require.Len(t, userColumns, len(expectedUserColumns), "Incorrect number of columns in target users table")

		for _, col := range userColumns {
			expected, ok := expectedUserColumns[col.ColumnName]
			require.True(t, ok, "Unexpected column '%s' in target users table", col.ColumnName)
			actualDataType := strings.ToLower(col.DataType)
			if col.DataType == "USER-DEFINED" || col.DataType == "ARRAY" { // Handle UDTs and arrays in PG
				actualDataType = strings.ToLower(strings.TrimPrefix(col.UdtName, "_")) // _int4 -> int4
			}
			assert.Equal(t, expected.TargetType, actualDataType, "Data type mismatch for column '%s'", col.ColumnName)
			assert.Equal(t, expected.IsNullable, col.IsNullable, "Nullability mismatch for column '%s'", col.ColumnName)
		}
		// Verifikasi default dan PK secara terpisah jika perlu lebih detail
	})

	// Verifikasi tabel 'posts'
	t.Run("VerifyTable_posts", func(t *testing.T) {
		resPosts, ok := results["posts"]
		require.True(t, ok, "Result for table 'posts' not found")
		require.NoError(t, resPosts.DataError, "Data error for posts: %v", resPosts.DataError)
		assert.EqualValues(t, 4, resPosts.RowsSynced) // Sesuai source_schema.sql

        // Verifikasi FK dari posts ke users
        var fkPostsToUsersExists int
        err = targetDB.DB.Raw(`
            SELECT COUNT(*) FROM information_schema.referential_constraints
            WHERE constraint_schema = current_schema() AND
                  unique_constraint_schema = current_schema() AND
                  constraint_name = 'fk_post_author' AND
                  unique_constraint_name = (SELECT con.conname FROM pg_catalog.pg_constraint con JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid WHERE rel.relname = 'users' AND con.contype = 'p')
        `).Scan(&fkPostsToUsersExists).Error // Query ini perlu disesuaikan
        require.NoError(t, err)
        // Query yang lebih sederhana untuk FK (mungkin perlu disesuaikan nama constraint jika auto-generate)
        // assert.Equal(t, 1, fkPostsToUsersExists, "FK from posts to users not found or incorrect")
        // Anda mungkin perlu query table_constraints dan key_column_usage untuk verifikasi FK yang lebih detail
	})

	// Tambahkan verifikasi untuk tabel comments, categories, post_categories
    // ...
}
