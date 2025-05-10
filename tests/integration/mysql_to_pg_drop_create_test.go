//go:build integration
package integration

import (
	"context"
	"database/sql"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	// "go.uber.org/zap"

	"github.com/arwahdevops/dbsync/internal/config"
	dbsync_db "github.com/arwahdevops/dbsync/internal/db"
	dbsync_logger "github.com/arwahdevops/dbsync/internal/logger"
	dbsync_metrics "github.com/arwahdevops/dbsync/internal/metrics"
	dbsync_sync "github.com/arwahdevops/dbsync/internal/sync"
	"os"
)

// mustPortInt (bisa di-refactor ke db_helpers.go jika belum)
// ... (sama seperti sebelumnya) ...

func TestMySQLToPostgres_DropCreateStrategy(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION_TESTS") != "" || testing.Short() {
		t.Skip("Skipping integration test.")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	err := dbsync_logger.Init(true, false)
	require.NoError(t, err)

	sourceDB := startMySQLContainer(ctx, t)
	targetDB := startPostgresContainer(ctx, t)
	defer stopContainer(ctx, t, sourceDB)
	defer stopContainer(ctx, t, targetDB)

	// 1. Setup Skema Awal di Source DB (sama seperti skenario CREATE)
	_, _ = sourceDB.DB.Exec("DROP TABLE IF EXISTS posts;").Error
	_, _ = sourceDB.DB.Exec("DROP TABLE IF EXISTS users;").Error
	executeSQLFile(t, sourceDB.DB, filepath.Join("testdata", "mysql_to_pg_drop_create", "source_schema.sql")) // Perlu file SQL ini

	// 2. Setup Skema AWAL yang BERBEDA di Target DB (untuk di-drop)
	_, _ = targetDB.DB.Exec("DROP TABLE IF EXISTS posts CASCADE;").Error
	_, _ = targetDB.DB.Exec("DROP TABLE IF EXISTS users CASCADE;").Error
	// Buat tabel 'users' dengan skema yang sedikit berbeda
	err = targetDB.DB.Exec(`
		CREATE TABLE users (
			user_id SERIAL PRIMARY KEY, -- Nama PK beda, tipe beda
			username VARCHAR(50),      -- Nama kolom beda
			registration_date DATE     -- Kolom ekstra
		);
		INSERT INTO users (username, registration_date) VALUES ('olduser', '2022-01-01');
	`).Error
	require.NoError(t, err, "Failed to create initial different target schema")

	// 3. Konfigurasi dbsync
	cfg := &config.Config{
		SyncDirection:      "mysql-to-postgres",
		SchemaSyncStrategy: config.SchemaSyncDropCreate, // Strategi kunci
		BatchSize:          50,
		Workers:            1,
		TableTimeout:       1 * time.Minute,
		SrcDB: config.DatabaseConfig{
			Dialect: sourceDB.Dialect, Host: sourceDB.Host, Port: mustPortInt(t, sourceDB.Port),
			User: sourceDB.Username, Password: sourceDB.Password, DBName: sourceDB.DBName, SSLMode: "disable",
		},
		DstDB: config.DatabaseConfig{
			Dialect: targetDB.Dialect, Host: targetDB.Host, Port: mustPortInt(t, targetDB.Port),
			User: targetDB.Username, Password: targetDB.Password, DBName: targetDB.DBName, SSLMode: "disable",
		},
		DebugMode: true,
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

	t.Run("VerifyTable_users_Recreated", func(t *testing.T) {
		resUsers, ok := results["users"]
		require.True(t, ok)
		require.NoError(t, resUsers.SchemaAnalysisError, "Schema analysis error for users: %v", resUsers.SchemaAnalysisError)
		require.NoError(t, resUsers.SchemaExecutionError, "Schema execution error for users: %v", resUsers.SchemaExecutionError)
		require.NoError(t, resUsers.DataError, "Data error for users: %v", resUsers.DataError)
		assert.EqualValues(t, 2, resUsers.RowsSynced) // Data dari source_schema.sql

		// Verifikasi skema BARU di target (harus cocok dengan source setelah drop-create)
		var userColumns []struct {
			ColumnName    string         `gorm:"column:column_name"`
			DataType      string         `gorm:"column:data_type"`
			IsNullable    string         `gorm:"column:is_nullable"`
			ColumnDefault sql.NullString `gorm:"column:column_default"`
		}
		err = targetDB.DB.Raw(`
			SELECT column_name, data_type, is_nullable, column_default
			FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = 'users'
			ORDER BY ordinal_position
		`).Scan(&userColumns).Error
		require.NoError(t, err)

		require.Len(t, userColumns, 4, "users table should have 4 columns after recreate")
		assert.Equal(t, "id", userColumns[0].ColumnName) // Bukan user_id lagi
		assert.Equal(t, "integer", strings.ToLower(userColumns[0].DataType))
		assert.Equal(t, "name", userColumns[1].ColumnName) // Bukan username lagi
		// ... (assertion lain untuk kolom users seperti di test CREATE) ...

		// Verifikasi data BARU di target (data lama 'olduser' seharusnya hilang)
		var targetUsers []struct{ ID int; Name string }
		err = targetDB.DB.Table("users").Order("id").Find(&targetUsers).Error
		require.NoError(t, err)
		require.Len(t, targetUsers, 2, "Data lama seharusnya hilang, data baru terisi")
		assert.Equal(t, "Alice", targetUsers[0].Name)
	})

	// Verifikasi tabel 'posts' juga dibuat (karena di source ada, di target awalnya tidak)
	t.Run("VerifyTable_posts_Created", func(t *testing.T) {
		resPosts, ok := results["posts"]
		require.True(t, ok)
		require.NoError(t, resPosts.DataError, "Data error for posts: %v", resPosts.DataError)
		assert.EqualValues(t, 3, resPosts.RowsSynced)
		// ... (verifikasi skema dan FK posts seperti di test CREATE)
	})
}
