//go:build integration
package integration

import (
	"context"
	"database/sql"
	"os"
	"path/filepath" // Masih digunakan untuk filepath.Join ke testdata
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

func TestMySQLToPostgres_CreateStrategy(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION_TESTS") != "" || testing.Short() {
		t.Skip("Skipping integration test.")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Inisialisasi logger sekali di awal jika belum (atau jika tes ini adalah entry point)
	// Jika logger sudah diinisialisasi oleh test suite yang lebih tinggi, ini mungkin tidak perlu.
	// Untuk keamanan, kita bisa panggil di sini.
	errLogger := dbsync_logger.Init(true, false) // true untuk debug mode di tes, false untuk json output
	require.NoError(t, errLogger, "Failed to initialize logger for test")

	sourceDB := startMySQLContainer(ctx, t)
	targetDB := startPostgresContainer(ctx, t)
	defer stopContainer(ctx, t, sourceDB)
	defer stopContainer(ctx, t, targetDB)

	sourceDB.DB.Exec("DROP TABLE IF EXISTS posts;")
	sourceDB.DB.Exec("DROP TABLE IF EXISTS comments;")
	sourceDB.DB.Exec("DROP TABLE IF EXISTS post_categories;")
	sourceDB.DB.Exec("DROP TABLE IF EXISTS categories;")
	sourceDB.DB.Exec("DROP TABLE IF EXISTS users;")
	executeSQLFile(t, sourceDB.DB, filepath.Join("testdata", "mysql_to_pg_create", "source_schema.sql"))

	targetDB.DB.Exec("DROP TABLE IF EXISTS comments CASCADE;")
	targetDB.DB.Exec("DROP TABLE IF EXISTS post_categories CASCADE;")
	targetDB.DB.Exec("DROP TABLE IF EXISTS categories CASCADE;")
	targetDB.DB.Exec("DROP TABLE IF EXISTS posts CASCADE;")
	targetDB.DB.Exec("DROP TABLE IF EXISTS users CASCADE;")

	cfg := &config.Config{
		SyncDirection:      "mysql-to-postgres",
		SchemaSyncStrategy: config.SchemaSyncDropCreate,
		BatchSize:          100,
		Workers:            2,
		TableTimeout:       2 * time.Minute,
		SkipFailedTables:   false,
		SrcDB: config.DatabaseConfig{
			Dialect:  sourceDB.Dialect,
			Host:     sourceDB.Host,
			Port:     mustPortInt(t, sourceDB.Port), // Pastikan mustPortInt tersedia
			User:     sourceDB.Username,
			Password: sourceDB.Password,
			DBName:   sourceDB.DBName,
			SSLMode:  "disable",
		},
		DstDB: config.DatabaseConfig{
			Dialect:  targetDB.Dialect,
			Host:     targetDB.Host,
			Port:     mustPortInt(t, targetDB.Port), // Pastikan mustPortInt tersedia
			User:     targetDB.Username,
			Password: targetDB.Password,
			DBName:   targetDB.DBName,
			SSLMode:  "disable",
		},
		MaxRetries:    2,
		RetryInterval: 2 * time.Second,
		DebugMode:     true,
		// TypeMappingFilePath: filepath.Join("..", "..", "typemap.json"), // DIHAPUS
	}

	// Pemetaan tipe sekarang dimuat secara internal oleh config.LoadTypeMappings saat aplikasi mulai
	// atau saat dipanggil secara eksplisit. Di sini, karena kita membuat cfg secara manual,
	// kita perlu memastikan mapping internal dimuat jika belum.
	// Namun, NewOrchestrator atau logika di main.go seharusnya sudah memanggil config.LoadTypeMappings("")
	// sebelum ini. Jika tes ini dijalankan terisolasi, kita mungkin perlu memanggilnya.
	// Untuk sekarang, asumsikan sudah dimuat atau akan dimuat oleh komponen yang lebih tinggi.
	// Jika ragu, panggil:
	// errLoadMappings := config.LoadTypeMappings("", dbsync_logger.Log)
	// require.NoError(t, errLoadMappings, "Failed to load internal type mappings")

	srcConnInternal := &dbsync_db.Connector{DB: sourceDB.DB, Dialect: sourceDB.Dialect}
	dstConnInternal := &dbsync_db.Connector{DB: targetDB.DB, Dialect: targetDB.Dialect}

	metricsStore := dbsync_metrics.NewMetricsStore()
	orchestrator := dbsync_sync.NewOrchestrator(srcConnInternal, dstConnInternal, cfg, dbsync_logger.Log, metricsStore)
	results := orchestrator.Run(ctx)

	require.NotEmpty(t, results, "Orchestrator Run returned no results")
	require.Len(t, results, 5, "Expected results for 5 tables")

	t.Run("VerifyTable_users", func(t *testing.T) {
		resUsers, ok := results["users"]
		require.True(t, ok, "Result for table 'users' not found")
		require.NoError(t, resUsers.SchemaAnalysisError, "Schema analysis error for users: %v", resUsers.SchemaAnalysisError)
		require.NoError(t, resUsers.SchemaExecutionError, "Schema execution error for users: %v", resUsers.SchemaExecutionError)
		require.NoError(t, resUsers.DataError, "Data error for users: %v", resUsers.DataError)
		require.False(t, resUsers.Skipped, "Table 'users' was skipped, reason: %s", resUsers.SkipReason)
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
			"id":         {TargetType: "integer", IsNullable: "NO"},
			"username":   {TargetType: "character varying", IsNullable: "NO"},
			"email":      {TargetType: "character varying", IsNullable: "NO"},
			"full_name":  {TargetType: "character varying", IsNullable: "YES"},
			"bio":        {TargetType: "text", IsNullable: "YES"},
			"age":        {TargetType: "smallint", IsNullable: "YES"},
			"salary":     {TargetType: "numeric", IsNullable: "YES"},
			"is_active":  {TargetType: "boolean", IsNullable: "YES"},
			"created_at": {TargetType: "timestamp with time zone", IsNullable: "YES"},
			"updated_at": {TargetType: "timestamp with time zone", IsNullable: "YES"},
		}
		require.Len(t, userColumns, len(expectedUserColumns), "Incorrect number of columns in target users table")

		for _, col := range userColumns {
			expected, ok := expectedUserColumns[col.ColumnName]
			require.True(t, ok, "Unexpected column '%s' in target users table", col.ColumnName)
			actualDataType := strings.ToLower(col.DataType)
			if col.DataType == "USER-DEFINED" || col.DataType == "ARRAY" {
				actualDataType = strings.ToLower(strings.TrimPrefix(col.UdtName, "_"))
			}
			assert.Equal(t, expected.TargetType, actualDataType, "Data type mismatch for column '%s'", col.ColumnName)
			assert.Equal(t, expected.IsNullable, col.IsNullable, "Nullability mismatch for column '%s'", col.ColumnName)
		}
	})

	t.Run("VerifyTable_posts", func(t *testing.T) {
		resPosts, ok := results["posts"]
		require.True(t, ok, "Result for table 'posts' not found")
		require.NoError(t, resPosts.DataError, "Data error for posts: %v", resPosts.DataError)
		assert.EqualValues(t, 4, resPosts.RowsSynced)
	})
}
