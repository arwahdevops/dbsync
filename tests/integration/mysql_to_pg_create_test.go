//go:build integration
// Pastikan build tag sudah benar

package integration

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"go.uber.org/zap"

	"github.com/arwahdevops/dbsync/internal/config"
	dbsync_db "github.com/arwahdevops/dbsync/internal/db"
	dbsync_logger "github.com/arwahdevops/dbsync/internal/logger"
	dbsync_metrics "github.com/arwahdevops/dbsync/internal/metrics"
	dbsync_sync "github.com/arwahdevops/dbsync/internal/sync"
	"github.com/arwahdevops/dbsync/internal/utils"
)

// getPgActualDataTypeTestHelper (tetap sama)
func getPgActualDataTypeTestHelper(colDataType, colUdtName string) string {
	actual := strings.ToLower(colUdtName)
	if actual == "" ||
		(colDataType != "USER-DEFINED" && colDataType != "ARRAY" && !strings.Contains(colDataType, "time zone") && actual == strings.ToLower(colDataType)) {
		actual = strings.ToLower(colDataType)
	}
	if strings.HasPrefix(actual, "_") {
		actual = strings.TrimPrefix(actual, "_") + "[]"
	}
	switch actual {
	case "int4": return "integer"
	case "int8": return "bigint"
	case "int2": return "smallint"
	case "bool": return "boolean"
	case "bpchar": return "character"
	case "varchar": return "character varying"
	}
	return actual
}


func TestMySQLToPostgres_CreateStrategy_WithFKVerification(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION_TESTS") != "" || testing.Short() {
		t.Skip("Skipping integration test (MySQL to PG Create Strategy).")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
	defer cancel()

	errLoggerInit := dbsync_logger.Init(true, false)
	require.NoError(t, errLoggerInit, "Failed to initialize logger for test")
	testLogger := dbsync_logger.Log.Named("mysql_to_pg_create_test")

	testLogger.Info("Starting MySQL and PostgreSQL test containers...")
	sourceDB := startMySQLContainer(ctx, t)
	targetDB := startPostgresContainer(ctx, t)
	defer stopContainer(ctx, t, sourceDB)
	defer stopContainer(ctx, t, targetDB)
	testLogger.Info("Test containers started.")

	testLogger.Info("Setting up source database schema and data...")
	sourceTablesToDrop := []string{"comments", "post_categories", "posts", "categories", "users"}
	for _, tableName := range sourceTablesToDrop {
		quotedTableName := utils.QuoteIdentifier(tableName, sourceDB.Dialect)
		if err := sourceDB.DB.Exec("DROP TABLE IF EXISTS " + quotedTableName + ";").Error; err != nil {
			testLogger.Warn("Failed to drop source table (may not exist)",
				zap.String("table", tableName),
				zap.Error(err))
		}
	}
	// === PERUBAHAN PATH DI SINI ===
	// Mengasumsikan source_schema.sql ada di direktori yang sama dengan file tes ini (tests/integration/)
	sourceSchemaFilePath := "source_schema.sql" // Atau filepath.Join(".", "source_schema.sql")
	// Jika Anda ingin lebih eksplisit dengan path relatif dari root proyek:
	// cwd, _ := os.Getwd() // Dapat membantu untuk debugging path
	// testLogger.Info("Current working directory for test", zap.String("cwd", cwd))
	// sourceSchemaFilePath := filepath.Join("tests", "integration", "source_schema.sql") // Jika dijalankan dari root proyek
	// Namun, karena `go test` biasanya dijalankan dari direktori package, path relatif terhadap file tes lebih aman.

	executeSQLFile(t, sourceDB.DB, sourceSchemaFilePath)
	testLogger.Info("Source database setup complete.", zap.String("schema_file", sourceSchemaFilePath))


	testLogger.Info("Cleaning up target database...")
	targetTablesToDrop := []string{"comments", "post_categories", "posts", "categories", "users"}
	for _, tableName := range targetTablesToDrop {
		quotedTableName := utils.QuoteIdentifier(tableName, targetDB.Dialect)
		if err := targetDB.DB.Exec("DROP TABLE IF EXISTS " + quotedTableName + " CASCADE;").Error; err != nil {
			testLogger.Warn("Failed to drop target table (may not exist)",
				zap.String("table", tableName),
				zap.Error(err))
		}
	}
	testLogger.Info("Target database cleanup complete.")

	cfg := &config.Config{
		SyncDirection:          "mysql-to-postgres",
		SchemaSyncStrategy:     config.SchemaSyncDropCreate,
		BatchSize:              100,
		Workers:                2,
		TableTimeout:           3 * time.Minute,
		SkipFailedTables:       false,
		DisableFKDuringSync:    true,
		SrcDB: config.DatabaseConfig{
			Dialect:  sourceDB.Dialect, Host: sourceDB.Host, Port: mustPortInt(t, sourceDB.Port),
			User:     sourceDB.Username, Password: sourceDB.Password, DBName: sourceDB.DBName, SSLMode:  "disable",
		},
		DstDB: config.DatabaseConfig{
			Dialect:  targetDB.Dialect, Host: targetDB.Host, Port: mustPortInt(t, targetDB.Port),
			User:     targetDB.Username, Password: targetDB.Password, DBName: targetDB.DBName, SSLMode:  "disable",
		},
		MaxRetries:    1,
		RetryInterval: 1 * time.Second,
		DebugMode:     true,
	}

	srcConnInternal := &dbsync_db.Connector{DB: sourceDB.DB, Dialect: sourceDB.Dialect}
	dstConnInternal := &dbsync_db.Connector{DB: targetDB.DB, Dialect: targetDB.Dialect}

	testLogger.Info("Loading internal type mappings...")
	errLoadMappings := config.LoadTypeMappings(testLogger)
	require.NoError(t, errLoadMappings, "Failed to load internal type mappings for test")

	testLogger.Info("Starting dbsync orchestrator...")
	metricsStore := dbsync_metrics.NewMetricsStore()
	orchestrator := dbsync_sync.NewOrchestrator(srcConnInternal, dstConnInternal, cfg, testLogger, metricsStore)
	results := orchestrator.Run(ctx)
	testLogger.Info("Dbsync orchestrator run finished.")


	require.NotEmpty(t, results, "Orchestrator Run returned no results")
	expectedTableNames := []string{"users", "categories", "posts", "comments", "post_categories"}
	require.Len(t, results, len(expectedTableNames), "Expected results for %d tables, got %d. Results map: %+v", len(expectedTableNames), len(results), results)

	for _, tableName := range expectedTableNames {
		t.Run("VerifyOverallSuccess_"+tableName, func(t *testing.T) {
			res, ok := results[tableName]
			require.True(t, ok, "Result for table '%s' not found", tableName)
			if res.SchemaAnalysisError != nil { t.Logf("SchemaAnalysisError for %s: %v", tableName, res.SchemaAnalysisError) }
			if res.SchemaExecutionError != nil { t.Logf("SchemaExecutionError for %s: %v", tableName, res.SchemaExecutionError) }
			if res.DataError != nil { t.Logf("DataError for %s: %v", tableName, res.DataError) }
			if res.ConstraintExecutionError != nil { t.Logf("ConstraintExecutionError for %s: %v", tableName, res.ConstraintExecutionError) }

			assert.NoError(t, res.SchemaAnalysisError, "Schema analysis error for %s", tableName)
			assert.NoError(t, res.SchemaExecutionError, "Schema execution error for %s", tableName)
			assert.NoError(t, res.DataError, "Data error for %s", tableName)
			assert.NoError(t, res.ConstraintExecutionError, "Constraint execution error for %s", tableName)
			assert.False(t, res.Skipped, "Table '%s' was skipped, reason: %s", tableName, res.SkipReason)
		})
	}

	t.Run("VerifyTableSchemaAndData_users", func(t *testing.T) {
		resUsers, ok := results["users"]
		require.True(t, ok, "Result for 'users' table not found")
		assert.EqualValues(t, 3, resUsers.RowsSynced, "Row count mismatch for users table")

		var userColumnsInfo []struct {
			ColumnName             string         `gorm:"column:column_name"`
			DataType               string         `gorm:"column:data_type"`
			UdtName                string         `gorm:"column:udt_name"`
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

		expectedUserSchema := map[string]struct {
			TargetType      string
			IsNullable      string
			CharMaxLen      sql.NullInt64
			NumPrecision    sql.NullInt64
			NumScale        sql.NullInt64
			ExpectedDefault sql.NullString
		}{
			"id":          {TargetType: "integer", IsNullable: "NO"},
			"username":    {TargetType: "character varying", IsNullable: "NO", CharMaxLen: sql.NullInt64{Int64: 50, Valid: true}},
			"email":       {TargetType: "character varying", IsNullable: "NO", CharMaxLen: sql.NullInt64{Int64: 100, Valid: true}},
			"full_name":   {TargetType: "character varying", IsNullable: "YES", CharMaxLen: sql.NullInt64{Int64: 100, Valid: true}},
			"bio":         {TargetType: "text", IsNullable: "YES"},
			"age":         {TargetType: "smallint", IsNullable: "YES"},
			"salary":      {TargetType: "numeric", IsNullable: "YES", NumPrecision: sql.NullInt64{Int64: 10, Valid: true}, NumScale: sql.NullInt64{Int64: 2, Valid: true}},
			"is_active":   {TargetType: "boolean", IsNullable: "YES", ExpectedDefault: sql.NullString{String: "true", Valid: true}},
			"created_at":  {TargetType: "timestamptz", IsNullable: "YES"},
			"updated_at":  {TargetType: "timestamptz", IsNullable: "YES"},
		}
		require.Len(t, userColumnsInfo, len(expectedUserSchema), "Incorrect number of columns in target users table")

		for _, col := range userColumnsInfo {
			expected, ok := expectedUserSchema[col.ColumnName]
			require.True(t, ok, "Unexpected column '%s' in target users table", col.ColumnName)

			actualDataType := getPgActualDataTypeTestHelper(col.DataType, col.UdtName)

			assert.Equal(t, expected.TargetType, actualDataType, "Data type mismatch for column '%s'. Expected '%s', got '%s' (Raw DataType: '%s', Raw UdtName: '%s')", col.ColumnName, expected.TargetType, actualDataType, col.DataType, col.UdtName)
			assert.Equal(t, expected.IsNullable, col.IsNullable, "Nullability mismatch for column '%s'", col.ColumnName)

			if expected.CharMaxLen.Valid {
				if col.CharacterMaximumLength.Valid {
					assert.Equal(t, expected.CharMaxLen.Int64, col.CharacterMaximumLength.Int64, "CharacterMaximumLength mismatch for column '%s'", col.ColumnName)
				} else if expected.TargetType == "character varying" && !strings.Contains(expected.TargetType, "(") {
				} else if expected.TargetType != "text" {
					assert.True(t, col.CharacterMaximumLength.Valid, "Expected CharacterMaximumLength to be valid for column '%s', but was NULL", col.ColumnName)
				}
			} else {
				assert.False(t, col.CharacterMaximumLength.Valid, "Expected CharacterMaximumLength to be NULL for column '%s', but had value", col.ColumnName)
			}

			if expected.NumPrecision.Valid {
				assert.True(t, col.NumericPrecision.Valid, "Expected NumericPrecision to be valid for column '%s'", col.ColumnName)
				assert.Equal(t, expected.NumPrecision.Int64, col.NumericPrecision.Int64, "NumericPrecision mismatch for column '%s'", col.ColumnName)
			}
			if expected.NumScale.Valid {
				assert.True(t, col.NumericScale.Valid, "Expected NumericScale to be valid for column '%s'", col.ColumnName)
				assert.Equal(t, expected.NumScale.Int64, col.NumericScale.Int64, "NumericScale mismatch for column '%s'", col.ColumnName)
			}
			if col.ColumnName == "is_active" {
				require.True(t, col.ColumnDefault.Valid, "Default value for 'is_active' should be set")
				assert.Equal(t, "true", col.ColumnDefault.String, "Default value mismatch for 'is_active'")
			}
		}
	})

	t.Run("VerifyDataCount_users", func(t *testing.T) { res, _ := results["users"]; assert.EqualValues(t, 3, res.RowsSynced, "users row count") })
	t.Run("VerifyDataCount_categories", func(t *testing.T) { res, _ := results["categories"]; assert.EqualValues(t, 3, res.RowsSynced, "categories row count") })
	t.Run("VerifyDataCount_posts", func(t *testing.T) { res, _ := results["posts"]; assert.EqualValues(t, 4, res.RowsSynced, "posts row count") })
	t.Run("VerifyDataCount_comments", func(t *testing.T) { res, _ := results["comments"]; assert.EqualValues(t, 3, res.RowsSynced, "comments row count") })
	t.Run("VerifyDataCount_post_categories", func(t *testing.T) { res, _ := results["post_categories"]; assert.EqualValues(t, 5, res.RowsSynced, "post_categories row count") })


	verifyForeignKeyExists := func(t *testing.T, db *gorm.DB, fromTable, fromColumn, toTable, toColumn, fkNameHint string) {
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
			t.Fatalf("Foreign key from %s(%s) to %s(%s) (expected name like '%s') NOT FOUND", fromTable, fromColumn, toTable, toColumn, fkNameHint)
		}
		require.NoError(t, errQuery, "Error querying for FK %s(%s) -> %s(%s) (hint: %s)", fromTable, fromColumn, toTable, toColumn, fkNameHint)
		assert.NotEmpty(t, constraintName, "Foreign key from %s(%s) to %s(%s) (hint: %s) should exist and have a name", fromTable, fromColumn, toTable, toColumn, fkNameHint)
		t.Logf("Found FK from %s(%s) to %s(%s) with name: %s (Hint was: '%s')", fromTable, fromColumn, toTable, toColumn, constraintName, fkNameHint)
	}

	t.Run("VerifyForeignKey_posts_to_users", func(t *testing.T) {
		verifyForeignKeyExists(t, targetDB.DB, "posts", "author_id", "users", "id", "fk_post_author")
	})
	t.Run("VerifyForeignKey_comments_to_posts", func(t *testing.T) {
		verifyForeignKeyExists(t, targetDB.DB, "comments", "post_id", "posts", "post_id", "fk_comment_post")
	})
	t.Run("VerifyForeignKey_comments_to_users", func(t *testing.T) {
		verifyForeignKeyExists(t, targetDB.DB, "comments", "user_id", "users", "id", "fk_comment_user")
	})
	t.Run("VerifyForeignKey_post_categories_to_posts", func(t *testing.T) {
		verifyForeignKeyExists(t, targetDB.DB, "post_categories", "post_id", "posts", "post_id", "fk_pc_post")
	})
	t.Run("VerifyForeignKey_post_categories_to_categories", func(t *testing.T) {
		verifyForeignKeyExists(t, targetDB.DB, "post_categories", "category_id", "categories", "category_id", "fk_pc_category")
	})
    t.Run("VerifyForeignKey_comments_to_self_comments", func(t *testing.T) {
		verifyForeignKeyExists(t, targetDB.DB, "comments", "parent_comment_id", "comments", "comment_id", "fk_comment_parent")
	})


	verifyUniqueConstraintExistsOnColumn := func(t *testing.T, db *gorm.DB, tableName, columnName, constraintNameHint string) {
		t.Helper()
		var count int
		query := `
			SELECT COUNT(DISTINCT tc.constraint_name)
			FROM information_schema.table_constraints tc
			JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name AND tc.constraint_schema = ccu.constraint_schema
			WHERE tc.table_schema = current_schema()
			  AND tc.table_name = ?
			  AND ccu.column_name = ?
			  AND tc.constraint_type IN ('UNIQUE', 'PRIMARY KEY');
		`
		queryCtx, queryCancel := context.WithTimeout(ctx, 10*time.Second)
		defer queryCancel()
		errQuery := db.WithContext(queryCtx).Raw(query, tableName, columnName).Scan(&count).Error
		require.NoError(t, errQuery, "Error querying for UNIQUE/PK constraint on %s(%s) (hint: %s)", tableName, columnName, constraintNameHint)
		assert.GreaterOrEqual(t, count, 1, "UNIQUE or PRIMARY KEY constraint on %s(%s) (hint: %s) should exist (found %d)", tableName, columnName, constraintNameHint, count)
		t.Logf("Found %d UNIQUE or PK constraint(s) involving column %s on table %s (Hint was: '%s')", count, columnName, tableName, constraintNameHint)
	}

	t.Run("VerifyUniqueConstraint_users_username", func(t *testing.T) {
		verifyUniqueConstraintExistsOnColumn(t, targetDB.DB, "users", "username", "uq_username")
	})
	t.Run("VerifyUniqueConstraint_users_email", func(t *testing.T) {
		verifyUniqueConstraintExistsOnColumn(t, targetDB.DB, "users", "email", "uq_email")
	})
	t.Run("VerifyUniqueConstraint_categories_name", func(t *testing.T) {
		verifyUniqueConstraintExistsOnColumn(t, targetDB.DB, "categories", "name", "uq_category_name")
	})
	t.Run("VerifyUniqueConstraint_posts_slug", func(t *testing.T) {
		verifyUniqueConstraintExistsOnColumn(t, targetDB.DB, "posts", "slug", "uq_post_slug")
	})
}
