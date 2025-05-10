//go:build integration
package integration

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arwahdevops/dbsync/internal/config"
	dbsync_db "github.com/arwahdevops/dbsync/internal/db"
	dbsync_logger "github.com/arwahdevops/dbsync/internal/logger"
	dbsync_metrics "github.com/arwahdevops/dbsync/internal/metrics"
	dbsync_sync "github.com/arwahdevops/dbsync/internal/sync"
)

// mustPortInt (jika belum ada di db_helpers.go)
// ...

func TestMySQLToPostgres_DropCreateStrategy(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION_TESTS") != "" || testing.Short() {
		t.Skip("Skipping integration test.")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute) // Beri waktu lebih
	defer cancel()

	err := dbsync_logger.Init(true, false)
	require.NoError(t, err)

	sourceDB := startMySQLContainer(ctx, t)
	targetDB := startPostgresContainer(ctx, t)
	defer stopContainer(ctx, t, sourceDB)
	defer stopContainer(ctx, t, targetDB)

	// 1. Setup Skema Awal di Source DB (gunakan skema yang sama dengan CREATE)
	sourceDB.DB.Exec("DROP TABLE IF EXISTS posts;")
	sourceDB.DB.Exec("DROP TABLE IF EXISTS comments;")
	sourceDB.DB.Exec("DROP TABLE IF EXISTS post_categories;")
	sourceDB.DB.Exec("DROP TABLE IF EXISTS categories;")
	sourceDB.DB.Exec("DROP TABLE IF EXISTS users;")
	executeSQLFile(t, sourceDB.DB, filepath.Join("testdata", "mysql_to_pg_drop_create", "source_schema.sql"))

	// 2. Setup Skema AWAL yang BERBEDA di Target DB (untuk di-drop)
	targetDB.DB.Exec("DROP TABLE IF EXISTS comments CASCADE;")
	targetDB.DB.Exec("DROP TABLE IF EXISTS post_categories CASCADE;")
	targetDB.DB.Exec("DROP TABLE IF EXISTS categories CASCADE;")
	targetDB.DB.Exec("DROP TABLE IF EXISTS posts CASCADE;")
	targetDB.DB.Exec("DROP TABLE IF EXISTS users CASCADE;")

	// Buat tabel 'users' dengan skema yang berbeda
	err = targetDB.DB.Exec(`
		CREATE TABLE users (
			user_id SERIAL PRIMARY KEY,
			old_username VARCHAR(30) UNIQUE,
			status TEXT DEFAULT 'pending',
			last_login TIMESTAMP
		);
		INSERT INTO users (old_username, status) VALUES ('old_john', 'active');
	`).Error
	require.NoError(t, err, "Failed to create initial different target schema for users")

	// Buat tabel 'posts' juga dengan skema berbeda
	err = targetDB.DB.Exec(`
		CREATE TABLE posts (
			pid INT PRIMARY KEY,
			legacy_title VARCHAR(100)
		);
	`).Error
	require.NoError(t, err, "Failed to create initial different target schema for posts")

	// 3. Konfigurasi dbsync
	cfg := &config.Config{
		SyncDirection:      "mysql-to-postgres",
		SchemaSyncStrategy: config.SchemaSyncDropCreate, // Strategi kunci
		BatchSize:          50,
		Workers:            2, // Bisa 1 atau 2 untuk skema kompleks agar mudah di-debug
		TableTimeout:       2 * time.Minute,
		SrcDB: config.DatabaseConfig{
			Dialect: sourceDB.Dialect, Host: sourceDB.Host, Port: mustPortInt(t, sourceDB.Port),
			User: sourceDB.Username, Password: sourceDB.Password, DBName: sourceDB.DBName, SSLMode: "disable",
		},
		DstDB: config.DatabaseConfig{
			Dialect: targetDB.Dialect, Host: targetDB.Host, Port: mustPortInt(t, targetDB.Port),
			User: targetDB.Username, Password: targetDB.Password, DBName: targetDB.DBName, SSLMode: "disable",
		},
		DebugMode: true,
		TypeMappingFilePath: filepath.Join("..", "..", "typemap.json"),
	}
	err = config.LoadTypeMappings(cfg.TypeMappingFilePath, dbsync_logger.Log)
	require.NoError(t, err)

	srcConnInternal := &dbsync_db.Connector{DB: sourceDB.DB, Dialect: sourceDB.Dialect}
	dstConnInternal := &dbsync_db.Connector{DB: targetDB.DB, Dialect: targetDB.Dialect}

	metricsStore := dbsync_metrics.NewMetricsStore()
	orchestrator := dbsync_sync.NewOrchestrator(srcConnInternal, dstConnInternal, cfg, dbsync_logger.Log, metricsStore)
	results := orchestrator.Run(ctx)

	// 4. Verifikasi Hasil
	require.NotEmpty(t, results)
	require.Len(t, results, 5, "Expected results for 5 tables")

	t.Run("VerifyTable_users_Recreated", func(t *testing.T) {
		resUsers, ok := results["users"]
		require.True(t, ok, "Result for table 'users' not found")
		require.NoError(t, resUsers.SchemaAnalysisError, "Schema analysis error for users: %v", resUsers.SchemaAnalysisError)
		require.NoError(t, resUsers.SchemaExecutionError, "Schema execution error for users: %v", resUsers.SchemaExecutionError)
		require.NoError(t, resUsers.DataError, "Data error for users: %v", resUsers.DataError)
		assert.EqualValues(t, 3, resUsers.RowsSynced) // Data dari source_schema.sql

		// Verifikasi skema BARU di target (harus cocok dengan source setelah drop-create)
		var userColumns []struct {
			ColumnName string `gorm:"column:column_name"`
			DataType   string `gorm:"column:data_type"`
			UdtName    string `gorm:"column:udt_name"`
		}
		queryCtx, queryCancel := context.WithTimeout(ctx, 10*time.Second)
		defer queryCancel()
		err = targetDB.DB.WithContext(queryCtx).Raw(`
			SELECT column_name, data_type, udt_name
			FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = 'users'
			ORDER BY ordinal_position
		`).Scan(&userColumns).Error
		require.NoError(t, err)

		// Verifikasi bahwa kolom lama (user_id, old_username, status, last_login) tidak ada lagi
		// dan kolom baru (id, username, email, ...) ada
		expectedNewUserColNames := []string{"id", "username", "email", "full_name", "bio", "age", "salary", "is_active", "created_at", "updated_at"}
		require.Len(t, userColumns, len(expectedNewUserColNames), "users table should have %d columns after recreate", len(expectedNewUserColNames))
		for i, col := range userColumns {
			assert.Equal(t, expectedNewUserColNames[i], col.ColumnName, "Column name mismatch at position %d", i)
		}

		// Verifikasi data BARU di target (data lama 'old_john' seharusnya hilang)
		var targetUsers []struct{ ID int; Username string }
		queryDataCtx, queryDataCancel := context.WithTimeout(ctx, 10*time.Second)
		defer queryDataCancel()
		err = targetDB.DB.WithContext(queryDataCtx).Table("users").Order("id").Find(&targetUsers).Error
		require.NoError(t, err)
		require.Len(t, targetUsers, 3, "Data lama users seharusnya hilang, data baru terisi")
		assert.Equal(t, "john_doe", targetUsers[0].Username)
	})

	t.Run("VerifyTable_posts_Recreated", func(t *testing.T) {
		resPosts, ok := results["posts"]
		require.True(t, ok, "Result for table 'posts' not found")
		require.NoError(t, resPosts.DataError, "Data error for posts: %v", resPosts.DataError)
		assert.EqualValues(t, 4, resPosts.RowsSynced)

		var postColumns []struct { ColumnName string `gorm:"column:column_name"`	}
		queryCtx, queryCancel := context.WithTimeout(ctx, 10*time.Second)
		defer queryCancel()
		err = targetDB.DB.WithContext(queryCtx).Raw(
			"SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'posts' ORDER BY ordinal_position",
		).Scan(&postColumns).Error
		require.NoError(t, err)

		expectedNewPostColNames := []string{"post_id", "author_id", "title", "content", "slug", "views", "published_at"}
		require.Len(t, postColumns, len(expectedNewPostColNames), "posts table should have %d columns after recreate", len(expectedNewPostColNames))
		for i, col := range postColumns {
			assert.Equal(t, expectedNewPostColNames[i], col.ColumnName, "Post column name mismatch at position %d", i)
		}
		// ... (verifikasi data dan FK posts seperti di test CREATE) ...
	})
	// Tambahkan verifikasi untuk tabel comments, categories, post_categories bahwa mereka dibuat
}
