//go:build integration
package integration

import (
	"context"
	"database/sql" // Diperlukan untuk sql.NullString
	"fmt"
	"os"
	"path/filepath"
	"strconv" // Diperlukan untuk mustPortInt
	"strings"
	"testing"
	"time"

	"github.com/docker/go-connections/nat" // Diperlukan untuk mustPortInt dan TestDBInstance
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	// "go.uber.org/zap" // Orchestrator menerimanya, jadi tidak perlu di sini jika hanya memanggilnya

	// Impor GORM dan driver
	// Driver akan digunakan secara internal oleh fungsi startXXXContainer atau Orchestrator
	// "gorm.io/driver/mysql"
	// "gorm.io/driver/postgres"
	// "gorm.io/gorm" // GORM akan digunakan melalui instance TestDBInstance.DB

	// Impor internal package dbsync Anda
	"github.com/arwahdevops/dbsync/internal/config"
	dbsync_db "github.com/arwahdevops/dbsync/internal/db" // Alias
	dbsync_logger "github.com/arwahdevops/dbsync/internal/logger"
	dbsync_metrics "github.com/arwahdevops/dbsync/internal/metrics"
	dbsync_sync "github.com/arwahdevops/dbsync/internal/sync"
)

// mustPortInt adalah helper untuk mengkonversi nat.Port ke int.
// Bisa juga diletakkan di db_helpers.go jika lebih disukai.
func mustPortInt(t *testing.T, port nat.Port) int {
	t.Helper()
	p, err := strconv.Atoi(port.Port())
	if err != nil {
		t.Fatalf("Failed to convert port %s to int: %v", port.Port(), err)
	}
	return p
}

func TestMySQLToPostgres_CreateStrategy(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION_TESTS") != "" || testing.Short() {
		t.Skip("Skipping integration test.")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute) // Timeout keseluruhan test
	defer cancel()

	// 1. Setup Logger (sesuaikan dengan main.go Anda)
	err := dbsync_logger.Init(true, false) // Debug true, JSON false untuk test
	require.NoError(t, err, "Failed to initialize logger")

	// 2. Start Database Containers
	// Anda perlu mengimplementasikan startMySQLContainer di db_helpers.go
	// yang serupa dengan startPostgresContainer
	sourceDB := startMySQLContainer(ctx, t)    // Implementasi ini ada di db_helpers.go (atau akan Anda buat)
	targetDB := startPostgresContainer(ctx, t) // Implementasi ini ada di db_helpers.go
	defer stopContainer(ctx, t, sourceDB)      // stopContainer juga dari db_helpers.go
	defer stopContainer(ctx, t, targetDB)

	// 3. Setup Skema Awal di Source DB
	// Bersihkan dulu (jika menjalankan ulang test)
	// Gunakan sourceDB.DB yang merupakan *gorm.DB
	_, err = sourceDB.DB.Exec("DROP TABLE IF EXISTS posts;").Error
    // Tidak perlu require.NoError untuk DROP IF EXISTS karena bisa jadi tabel belum ada
	_, err = sourceDB.DB.Exec("DROP TABLE IF EXISTS users;").Error

	executeSQLFile(t, sourceDB.DB, filepath.Join("testdata", "mysql_to_pg_create", "source_schema.sql"))

	// Pastikan target DB kosong (atau drop tabel yang mungkin ada dari run sebelumnya)
	_, err = targetDB.DB.Exec("DROP TABLE IF EXISTS posts CASCADE;").Error // CASCADE untuk FK
	_, err = targetDB.DB.Exec("DROP TABLE IF EXISTS users CASCADE;").Error


	// 4. Konfigurasi dbsync
	cfg := &config.Config{
		SyncDirection:      "mysql-to-postgres",
		SchemaSyncStrategy: config.SchemaSyncDropCreate,
		BatchSize:          100,
		Workers:            2,
		TableTimeout:       1 * time.Minute,
		SkipFailedTables:   false,
		SrcDB: config.DatabaseConfig{
			Dialect:  sourceDB.Dialect,
			Host:     sourceDB.Host,
			Port:     mustPortInt(t, sourceDB.Port), // Menggunakan helper dan port dari TestContainer
			User:     sourceDB.Username,
			Password: sourceDB.Password,
			DBName:   sourceDB.DBName,
			SSLMode:  "disable", // Sesuaikan jika TestContainer Anda menggunakan SSL
		},
		DstDB: config.DatabaseConfig{
			Dialect:  targetDB.Dialect,
			Host:     targetDB.Host,
			Port:     mustPortInt(t, targetDB.Port), // Menggunakan helper dan port dari TestContainer
			User:     targetDB.Username,
			Password: targetDB.Password,
			DBName:   targetDB.DBName,
			SSLMode:  "disable", // Sesuaikan
		},
		// TypeMappingFilePath: biarkan kosong untuk menggunakan default internal atau typemap.json di root
		MaxRetries:    1,
		RetryInterval: 1 * time.Second,
		DebugMode:     true, // Aktifkan debug mode untuk logging lebih detail selama test
	}
	// Muat type mappings jika diperlukan (menggunakan path default atau yang dikonfigurasi)
	// Jika cfg.TypeMappingFilePath kosong, LoadTypeMappings akan mencari ./typemap.json atau menggunakan internal.
	err = config.LoadTypeMappings(cfg.TypeMappingFilePath, dbsync_logger.Log)
	require.NoError(t, err, "Failed to load type mappings")

	// 5. Buat Konektor DB Internal dbsync
	srcConnInternal := &dbsync_db.Connector{DB: sourceDB.DB, Dialect: sourceDB.Dialect}
	dstConnInternal := &dbsync_db.Connector{DB: targetDB.DB, Dialect: targetDB.Dialect}

	// 6. Eksekusi dbsync Orchestrator
	metricsStore := dbsync_metrics.NewMetricsStore()
	orchestrator := dbsync_sync.NewOrchestrator(srcConnInternal, dstConnInternal, cfg, dbsync_logger.Log, metricsStore)
	results := orchestrator.Run(ctx) // results adalah map[string]dbsync_sync.SyncResult

	// 7. Verifikasi Hasil
	require.NotEmpty(t, results, "Orchestrator Run returned no results")

	// Verifikasi tabel 'users'
	t.Run("VerifyTable_users", func(t *testing.T) {
		resUsers, ok := results["users"]
		require.True(t, ok, "Result for table 'users' not found")
		require.NoError(t, resUsers.SchemaAnalysisError, "Schema analysis error for users: %v", resUsers.SchemaAnalysisError)
		require.NoError(t, resUsers.SchemaExecutionError, "Schema execution error for users: %v", resUsers.SchemaExecutionError)
		require.NoError(t, resUsers.DataError, "Data error for users: %v", resUsers.DataError)
		require.False(t, resUsers.Skipped, "Table 'users' was skipped, reason: %s", resUsers.SkipReason)
		assert.EqualValues(t, 2, resUsers.RowsSynced, "Row count mismatch for users")

		// Verifikasi skema di target (PostgreSQL)
		var userColumns []struct {
			ColumnName    string         `gorm:"column:column_name"`
			DataType      string         `gorm:"column:data_type"`
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

		require.Len(t, userColumns, 4, "Incorrect number of columns in target users table")

		// Kolom id
		assert.Equal(t, "id", userColumns[0].ColumnName)
		// Tipe data di PostgreSQL bisa INTEGER atau BIGINT tergantung pemetaan tinyint/int MySQL di typemap.json
		// Jika int MySQL -> INTEGER PG, maka ini benar. Jika int MySQL -> BIGINT PG, sesuaikan.
		// Asumsi default dari typemap.json: int -> INTEGER
		assert.Equal(t, "integer", strings.ToLower(userColumns[0].DataType), "id column type mismatch")
		assert.Equal(t, "NO", userColumns[0].IsNullable)
		require.True(t, userColumns[0].ColumnDefault.Valid, "id column default should be set for SERIAL type behavior")
		assert.True(t, strings.HasPrefix(userColumns[0].ColumnDefault.String, "nextval("), "id column default is not a sequence")

		// Kolom name
		assert.Equal(t, "name", userColumns[1].ColumnName)
		assert.Equal(t, "character varying", userColumns[1].DataType) // VARCHAR MySQL -> VARCHAR PG
		assert.Equal(t, "NO", userColumns[1].IsNullable)

		// Kolom email
		assert.Equal(t, "email", userColumns[2].ColumnName)
		assert.Equal(t, "character varying", userColumns[2].DataType)
		assert.Equal(t, "YES", userColumns[2].IsNullable) // UNIQUE constraint membuat nullable di PG secara default jika tidak ada NOT NULL

		// Kolom created_at
		assert.Equal(t, "created_at", userColumns[3].ColumnName)
		assert.Equal(t, "timestamp with time zone", userColumns[3].DataType) // MySQL TIMESTAMP -> PG TIMESTAMPTZ (dari typemap.json)
		require.True(t, userColumns[3].ColumnDefault.Valid, "created_at default should be set")
		assert.Contains(t, strings.ToLower(userColumns[3].ColumnDefault.String), "current_timestamp", "created_at default mismatch")

		// Verifikasi data di target
		var targetUsers []struct {
			ID         int
			Name       string
			Email      string
			CreatedAt  time.Time `gorm:"column:created_at"`
		}
		queryCtxData, queryCancelData := context.WithTimeout(ctx, 10*time.Second)
		defer queryCancelData()
		err = targetDB.DB.WithContext(queryCtxData).Table("users").Order("id").Find(&targetUsers).Error
		require.NoError(t, err)
		require.Len(t, targetUsers, 2)
		assert.Equal(t, "Alice", targetUsers[0].Name)
		assert.Equal(t, "bob@example.com", targetUsers[1].Email)

		// Verifikasi constraint UNIQUE email
		var constraintName string
		queryCtxConstraint, queryCancelConstraint := context.WithTimeout(ctx, 10*time.Second)
		defer queryCancelConstraint()
		err = targetDB.DB.WithContext(queryCtxConstraint).Raw(
			"SELECT constraint_name FROM information_schema.table_constraints WHERE table_name = 'users' AND constraint_type = 'UNIQUE' AND constraint_schema = current_schema()",
		).Scan(&constraintName).Error
		require.NoError(t, err, "Failed to find UNIQUE constraint for email")
		assert.NotEmpty(t, constraintName, "UNIQUE constraint for email not found")
	})

	// Verifikasi tabel 'posts'
	t.Run("VerifyTable_posts", func(t *testing.T) {
		resPosts, ok := results["posts"]
		require.True(t, ok, "Result for table 'posts' not found")
		require.NoError(t, resPosts.SchemaAnalysisError, "Schema analysis error for posts: %v", resPosts.SchemaAnalysisError)
		require.NoError(t, resPosts.SchemaExecutionError, "Schema execution error for posts: %v", resPosts.SchemaExecutionError)
		require.NoError(t, resPosts.DataError, "Data error for posts: %v", resPosts.DataError)
		require.False(t, resPosts.Skipped, "Table 'posts' was skipped, reason: %s", resPosts.SkipReason)
		assert.EqualValues(t, 3, resPosts.RowsSynced, "Row count mismatch for posts")

		// Verifikasi kolom 'content' di target adalah TEXT (dari MySQL TEXT -> PG TEXT)
		var contentColumnType string
		queryCtxCol, queryCancelCol := context.WithTimeout(ctx, 10*time.Second)
		defer queryCancelCol()
		err = targetDB.DB.WithContext(queryCtxCol).Raw(
			"SELECT data_type FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'posts' AND column_name = 'content'",
		).Scan(&contentColumnType).Error
		require.NoError(t, err)
		assert.Equal(t, "text", contentColumnType, "posts.content column type mismatch")

		// Verifikasi FK
		var fkExists int
		queryCtxFK, queryCancelFK := context.WithTimeout(ctx, 10*time.Second)
		defer queryCancelFK()
		err = targetDB.DB.WithContext(queryCtxFK).Raw(`
			SELECT COUNT(*)
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.constraint_schema = kcu.constraint_schema
			JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name AND ccu.constraint_schema = tc.constraint_schema
			WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name='posts' AND kcu.column_name='user_id'
			AND ccu.table_name='users' AND ccu.column_name='id' AND tc.table_schema = current_schema()
		`).Scan(&fkExists).Error
		require.NoError(t, err, "Error querying FK constraint")
		assert.Equal(t, 1, fkExists, "Foreign key from posts.user_id to users.id not found or incorrect")

		// Verifikasi data di target posts
		var targetPosts []struct {
			PostID      int `gorm:"column:post_id"`
			UserID      sql.NullInt64 `gorm:"column:user_id"` // Karena ON DELETE SET NULL
			Title       string
			Content     string
			PublishedAt sql.NullTime `gorm:"column:published_at"` // MySQL DATETIME bisa NULL
		}
		queryCtxDataP, queryCancelDataP := context.WithTimeout(ctx, 10*time.Second)
		defer queryCancelDataP()
		err = targetDB.DB.WithContext(queryCtxDataP).Table("posts").Order("post_id").Find(&targetPosts).Error
		require.NoError(t, err)
		require.Len(t, targetPosts, 3)
		assert.Equal(t, "Alice Post 1", targetPosts[0].Title)
		assert.True(t, targetPosts[0].UserID.Valid && targetPosts[0].UserID.Int64 == 1)
		assert.True(t, targetPosts[2].PublishedAt.Valid == false, "Bob Post 1 published_at should be NULL")
	})
}
