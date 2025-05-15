//go:build integration

package integration

import (
	"context"
	"database/sql"
	"errors" // <--- THIS IMPORT WAS MISSING
	"fmt"
	"os"
	"path/filepath" 
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

// pgColumnInfo holds schema details for a PostgreSQL column for verification.
type pgColumnInfo struct {
	ColumnName             string         `gorm:"column:column_name"`
	DataType               string         `gorm:"column:data_type"`                 // e.g., integer, character varying, text, numeric, boolean, timestamp with time zone
	UdtName                string         `gorm:"column:udt_name"`                  // underlying type name, e.g., int4, varchar, text, numeric, bool, timestamptz
	IsNullable             string         `gorm:"column:is_nullable"`               // "YES" or "NO"
	ColumnDefault          sql.NullString `gorm:"column:column_default"`            // e.g., nextval('users_id_seq'::regclass), true, 'active'::character varying
	CharacterMaximumLength sql.NullInt64  `gorm:"column:character_maximum_length"`
	NumericPrecision       sql.NullInt64  `gorm:"column:numeric_precision"`
	NumericScale           sql.NullInt64  `gorm:"column:numeric_scale"`
	IsIdentity             string         `gorm:"column:is_identity"`             // "YES" or "NO" (for PG IDENTITY columns)
	IdentityGeneration     sql.NullString `gorm:"column:identity_generation"`     // "BY DEFAULT" or "ALWAYS"
	IsUpdatable            string         `gorm:"column:is_updatable"`            // "YES" or "NO" (can indicate generated columns if "NO")
	OrdinalPosition        int            `gorm:"column:ordinal_position"`
}

// normalizePgActualDataType normalizes PostgreSQL data types for easier comparison.
// e.g., "int4" becomes "integer", "_varchar" becomes "character varying[]".
func normalizePgActualDataType(rawDataType, rawUdtName string) string {
	dataType := strings.ToLower(rawDataType)
	udtName := strings.ToLower(rawUdtName)

	if dataType == "array" {
		// For arrays, udt_name is like _int4, _varchar. We want int4[], varchar[].
		if strings.HasPrefix(udtName, "_") {
			baseType := normalizePgActualDataType(strings.TrimPrefix(udtName, "_"), strings.TrimPrefix(udtName, "_")) // Recursive call for base type
			return baseType + "[]"
		}
		return udtName + "[]" // Fallback if udt_name is not prefixed with _
	}

	// Prefer udt_name as it's often more specific, then map common aliases
	switch udtName {
	case "int2":
		return "smallint"
	case "int4":
		return "integer"
	case "int8":
		return "bigint"
	case "bool":
		return "boolean"
	case "bpchar": // "character" blank-padded
		return "character" // Can also be mapped to "char" for simplicity if preferred
	case "varchar":
		return "character varying"
	case "float4":
		return "real"
	case "float8":
		return "double precision"
	case "timestamptz":
		return "timestamp with time zone"
	case "timestamp": // This is timestamp without time zone
		return "timestamp without time zone"
	case "timetz":
		return "time with time zone"
	case "time": // This is time without time zone
		return "time without time zone"
	default:
		// If udt_name isn't a known alias, and data_type is user-defined, use udt_name.
		// Otherwise, use the (normalized) data_type.
		if dataType == "user-defined" {
			return udtName
		}
		return dataType // Fallback to raw (normalized) data_type
	}
}


// expectedPgColumnSchema defines the expected schema for a PostgreSQL column after migration.
type expectedPgColumnSchema struct {
	TargetType             string         // Normalized PostgreSQL type (e.g., "integer", "character varying", "boolean")
	IsNullable             string         // "YES" or "NO"
	IsIdentity             string         // "YES" or "NO" (can be "NO" if using sequences instead of IDENTITY)
	IdentityGeneration     sql.NullString // "BY DEFAULT" or "ALWAYS"
	CharMaxLen             sql.NullInt64
	NumPrecision           sql.NullInt64
	NumScale               sql.NullInt64
	ExpectedDefaultSnippet sql.NullString // A snippet to check within the actual default (e.g., "nextval", "true", "active")
}

// verifyTableSchema is a helper to verify the schema of a target table.
func verifyTableSchema(t *testing.T, ctx context.Context, targetDB *gorm.DB, tableName string, expectedSchema map[string]expectedPgColumnSchema) {
	t.Helper()

	var actualColumnsInfo []pgColumnInfo
	queryCtx, queryCancel := context.WithTimeout(ctx, 20*time.Second)
	defer queryCancel()

	// Query to get detailed column information including identity status
	query := `
        SELECT
            c.column_name,
            c.data_type,
            c.udt_name,
            c.is_nullable,
            c.column_default,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale,
            c.is_identity,
            c.identity_generation,
            c.is_updatable,
			c.ordinal_position
        FROM information_schema.columns c
        WHERE c.table_schema = current_schema() AND c.table_name = $1
        ORDER BY c.ordinal_position;
    `
	errQuerySchema := targetDB.WithContext(queryCtx).Raw(query, tableName).Scan(&actualColumnsInfo).Error
	require.NoError(t, errQuerySchema, "Failed to query target schema for table '%s'", tableName)
	require.Len(t, actualColumnsInfo, len(expectedSchema), "Column count mismatch for table '%s'. Expected %d, got %d. Fetched columns: %+v", tableName, len(expectedSchema), len(actualColumnsInfo), actualColumnsInfo)

	for _, actualCol := range actualColumnsInfo {
		expectedCol, ok := expectedSchema[actualCol.ColumnName]
		require.True(t, ok, "Unexpected column '%s' found in target table '%s'", actualCol.ColumnName, tableName)

		actualNormalizedType := normalizePgActualDataType(actualCol.DataType, actualCol.UdtName)
		assert.Equal(t, expectedCol.TargetType, actualNormalizedType, "Data type mismatch for column '%s.%s'. Expected '%s', got '%s' (Raw DB: DataType='%s', UdtName='%s')", tableName, actualCol.ColumnName, expectedCol.TargetType, actualNormalizedType, actualCol.DataType, actualCol.UdtName)
		assert.Equal(t, expectedCol.IsNullable, actualCol.IsNullable, "Nullability mismatch for column '%s.%s'", tableName, actualCol.ColumnName)

		// Verify identity properties
		if expectedCol.IsIdentity != "" { // Only check if an expectation for IsIdentity is set
			assert.Equal(t, expectedCol.IsIdentity, actualCol.IsIdentity, "IsIdentity mismatch for column '%s.%s'", tableName, actualCol.ColumnName)
			if expectedCol.IsIdentity == "YES" {
				assert.Equal(t, expectedCol.IdentityGeneration.String, actualCol.IdentityGeneration.String, "IdentityGeneration mismatch for column '%s.%s'", tableName, actualCol.ColumnName)
				assert.True(t, actualCol.IdentityGeneration.Valid, "IdentityGeneration should be valid for identity column '%s.%s'", tableName, actualCol.ColumnName)
			}
		}

		// Verify default value (snippet check)
		if expectedCol.ExpectedDefaultSnippet.Valid {
			require.True(t, actualCol.ColumnDefault.Valid, "Expected a default value for column '%s.%s', but it was NULL", tableName, actualCol.ColumnName)
			assert.Contains(t, actualCol.ColumnDefault.String, expectedCol.ExpectedDefaultSnippet.String, "Default value snippet mismatch for column '%s.%s'. Expected snippet '%s' in actual default '%s'", tableName, actualCol.ColumnName, expectedCol.ExpectedDefaultSnippet.String, actualCol.ColumnDefault.String)
		} else {
			// If no default is expected, ensure it's NULL, unless it's an identity/serial column (which will have a nextval default)
			if actualCol.IsIdentity != "YES" && !strings.Contains(actualNormalizedType, "serial") { // serial types have implicit defaults
				assert.False(t, actualCol.ColumnDefault.Valid, "Expected no default value for column '%s.%s', but got '%s'", tableName, actualCol.ColumnName, actualCol.ColumnDefault.String)
			}
		}

		// Verify character length
		if expectedCol.CharMaxLen.Valid {
			assert.True(t, actualCol.CharacterMaximumLength.Valid, "Expected CharacterMaximumLength to be valid for column '%s.%s'", tableName, actualCol.ColumnName)
			assert.Equal(t, expectedCol.CharMaxLen.Int64, actualCol.CharacterMaximumLength.Int64, "CharacterMaximumLength mismatch for column '%s.%s'", tableName, actualCol.ColumnName)
		} else if actualCol.CharacterMaximumLength.Valid && actualNormalizedType != "text" && actualNormalizedType != "json" && actualNormalizedType != "jsonb" && actualNormalizedType != "bytea" {
			// If no specific length is expected, but one is found, it might be an issue unless it's a text-like type without explicit length.
			assert.False(t, actualCol.CharacterMaximumLength.Valid, "Expected CharacterMaximumLength to be NULL for column '%s.%s' (type: %s), but had value %d", tableName, actualCol.ColumnName, actualNormalizedType, actualCol.CharacterMaximumLength.Int64)
		}


		// Verify numeric precision and scale
		if expectedCol.NumPrecision.Valid {
			assert.True(t, actualCol.NumericPrecision.Valid, "Expected NumericPrecision to be valid for column '%s.%s'", tableName, actualCol.ColumnName)
			assert.Equal(t, expectedCol.NumPrecision.Int64, actualCol.NumericPrecision.Int64, "NumericPrecision mismatch for column '%s.%s'", tableName, actualCol.ColumnName)
		}
		if expectedCol.NumScale.Valid {
			assert.True(t, actualCol.NumericScale.Valid, "Expected NumericScale to be valid for column '%s.%s'", tableName, actualCol.ColumnName)
			assert.Equal(t, expectedCol.NumScale.Int64, actualCol.NumericScale.Int64, "NumericScale mismatch for column '%s.%s'", tableName, actualCol.ColumnName)
		}
	}
}

func TestMySQLToPostgres_CreateStrategy_WithFKVerification(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION_TESTS") != "" || testing.Short() {
		t.Skip("Skipping integration test (MySQL to PG Create Strategy).")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute) // Increased timeout for CI
	defer cancel()

	// Initialize logger for the test
	errLoggerInit := dbsync_logger.Init(true, false) // Debug mode, console output for tests
	require.NoError(t, errLoggerInit, "Failed to initialize logger for test")
	testLogger := dbsync_logger.Log.Named("mysql_to_pg_create_test")

	testLogger.Info("Starting MySQL and PostgreSQL test containers...")
	sourceDB := startMySQLContainer(ctx, t)
	targetDB := startPostgresContainer(ctx, t)
	defer stopContainer(ctx, t, sourceDB)
	defer stopContainer(ctx, t, targetDB)
	testLogger.Info("Test containers started successfully.")

	// Prepare source database schema and data
	testLogger.Info("Setting up source database schema and data...")
	sourceTablesToDrop := []string{"comments", "post_categories", "posts", "categories", "users", "product_variants", "products"} // Add new tables
	for _, tableName := range sourceTablesToDrop {
		quotedTableName := utils.QuoteIdentifier(tableName, sourceDB.Dialect)
		if err := sourceDB.DB.Exec("DROP TABLE IF EXISTS " + quotedTableName + ";").Error; err != nil {
			testLogger.Warn("Failed to drop source table (might not exist)", zap.String("table", tableName), zap.Error(err))
		}
	}

	// Use filepath.Join for constructing paths robustly
	// Assumes test is run from the package directory 'tests/integration/'
	sourceSchemaFilePath := filepath.Join(".", "source_schema.sql")
	executeSQLFile(t, sourceDB.DB, sourceSchemaFilePath)
	testLogger.Info("Source database setup complete.", zap.String("schema_file_used", sourceSchemaFilePath))


	// Clean up target database (idempotent)
	testLogger.Info("Cleaning up target database before sync...")
	targetTablesToDrop := []string{"comments", "post_categories", "posts", "categories", "users", "product_variants", "products"} // Add new tables
	for _, tableName := range targetTablesToDrop {
		quotedTableName := utils.QuoteIdentifier(tableName, targetDB.Dialect)
		if err := targetDB.DB.Exec("DROP TABLE IF EXISTS " + quotedTableName + " CASCADE;").Error; err != nil { // CASCADE for PG
			testLogger.Warn("Failed to drop target table (might not exist)", zap.String("table", tableName), zap.Error(err))
		}
	}
	testLogger.Info("Target database cleanup complete.")

	// Configure dbsync
	cfg := &config.Config{
		SyncDirection:          "mysql-to-postgres",
		SchemaSyncStrategy:     config.SchemaSyncDropCreate,
		BatchSize:              100,
		Workers:                2,
		TableTimeout:           3 * time.Minute,
		SkipFailedTables:       false,
		DisableFKDuringSync:    true, // Important for create strategy with FKs
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
		DebugMode:     true, // Enable debug for more logs during test
	}

	// Create internal DB connectors for dbsync
	srcConnInternal := &dbsync_db.Connector{DB: sourceDB.DB, Dialect: sourceDB.Dialect}
	dstConnInternal := &dbsync_db.Connector{DB: targetDB.DB, Dialect: targetDB.Dialect}

	testLogger.Info("Loading internal type mappings for dbsync...")
	errLoadMappings := config.LoadTypeMappings(testLogger) // Pass logger
	require.NoError(t, errLoadMappings, "Failed to load internal type mappings for test")

	// Run dbsync
	testLogger.Info("Starting dbsync orchestrator run...")
	metricsStore := dbsync_metrics.NewMetricsStore()
	orchestrator := dbsync_sync.NewOrchestrator(srcConnInternal, dstConnInternal, cfg, testLogger, metricsStore)
	results := orchestrator.Run(ctx)
	testLogger.Info("Dbsync orchestrator run finished.")

	// --- Overall Verification ---
	require.NotEmpty(t, results, "Orchestrator Run returned no results, something is wrong with the setup or execution.")
	expectedTableNames := []string{"users", "categories", "posts", "comments", "post_categories", "products", "product_variants"}
	require.Len(t, results, len(expectedTableNames), "Expected results for %d tables, got %d. Results map: %+v", len(expectedTableNames), len(results), results)

	for _, tableName := range expectedTableNames {
		t.Run("VerifyOverallSuccess_"+tableName, func(t *testing.T) {
			res, ok := results[tableName]
			require.True(t, ok, "Result for table '%s' not found in orchestrator results", tableName)
			// Log errors if any, for easier debugging
			if res.SchemaAnalysisError != nil { t.Logf("SchemaAnalysisError for %s: %v", tableName, res.SchemaAnalysisError) }
			if res.SchemaExecutionError != nil { t.Logf("SchemaExecutionError for %s: %v", tableName, res.SchemaExecutionError) }
			if res.DataError != nil { t.Logf("DataError for %s: %v", tableName, res.DataError) }
			if res.ConstraintExecutionError != nil { t.Logf("ConstraintExecutionError for %s: %v", tableName, res.ConstraintExecutionError) }

			assert.NoError(t, res.SchemaAnalysisError, "Schema analysis should not error for table '%s'", tableName)
			assert.NoError(t, res.SchemaExecutionError, "Schema execution should not error for table '%s'", tableName)
			assert.NoError(t, res.DataError, "Data synchronization should not error for table '%s'", tableName)
			assert.NoError(t, res.ConstraintExecutionError, "Constraint application should not error for table '%s'", tableName)
			assert.False(t, res.Skipped, "Table '%s' should not have been skipped; reason: %s", tableName, res.SkipReason)
		})
	}

	// --- Detailed Schema and Data Verification for each table ---

	// Expected schema for 'users' table in PostgreSQL
	expectedUserSchema := map[string]expectedPgColumnSchema{
		"id":          {TargetType: "integer", IsNullable: "NO", IsIdentity: "YES", IdentityGeneration: sql.NullString{String: "BY DEFAULT", Valid: true}, ExpectedDefaultSnippet: sql.NullString{String: "nextval", Valid: true}}, // MySQL INT AUTO_INCREMENT becomes PG INTEGER + sequence/identity
		"username":    {TargetType: "character varying", IsNullable: "NO", CharMaxLen: sql.NullInt64{Int64: 50, Valid: true}},
		"email":       {TargetType: "character varying", IsNullable: "NO", CharMaxLen: sql.NullInt64{Int64: 100, Valid: true}},
		"full_name":   {TargetType: "character varying", IsNullable: "YES", CharMaxLen: sql.NullInt64{Int64: 100, Valid: true}},
		"bio":         {TargetType: "text", IsNullable: "YES"},
		"age":         {TargetType: "smallint", IsNullable: "YES"}, // MySQL TINYINT (not (1)) maps to SMALLINT
		"salary":      {TargetType: "numeric", IsNullable: "YES", NumPrecision: sql.NullInt64{Int64: 10, Valid: true}, NumScale: sql.NullInt64{Int64: 2, Valid: true}},
		"is_active":   {TargetType: "boolean", IsNullable: "YES", ExpectedDefaultSnippet: sql.NullString{String: "true", Valid: true}}, // MySQL TINYINT(1) DEFAULT 1 maps to BOOLEAN DEFAULT TRUE
		"created_at":  {TargetType: "timestamp with time zone", IsNullable: "YES", ExpectedDefaultSnippet: sql.NullString{String: "now()", Valid: true}}, // MySQL TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		"updated_at":  {TargetType: "timestamp with time zone", IsNullable: "YES", ExpectedDefaultSnippet: sql.NullString{String: "now()", Valid: true}}, // MySQL TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP (ON UPDATE part is lost in create, default remains)
		"json_data":   {TargetType: "jsonb", IsNullable: "YES"}, // MySQL JSON maps to jsonb
		"binary_data": {TargetType: "bytea", IsNullable: "YES"}, // MySQL BLOB maps to bytea
		"user_status": {TargetType: "character varying", IsNullable: "YES", CharMaxLen: sql.NullInt64{Int64: 255, Valid: true}, ExpectedDefaultSnippet: sql.NullString{String: "active", Valid: true}}, // ENUM -> VARCHAR(255)
	}
	t.Run("VerifyTableSchema_users", func(t *testing.T) {
		verifyTableSchema(t, ctx, targetDB.DB, "users", expectedUserSchema)
	})
	t.Run("VerifyDataCount_users", func(t *testing.T) { res, _ := results["users"]; assert.EqualValues(t, 3, res.RowsSynced, "users row count") })


	expectedCategoriesSchema := map[string]expectedPgColumnSchema{
		"category_id":   {TargetType: "integer", IsNullable: "NO", IsIdentity: "YES", IdentityGeneration: sql.NullString{String: "BY DEFAULT", Valid: true}, ExpectedDefaultSnippet: sql.NullString{String: "nextval", Valid: true}},
		"name":          {TargetType: "character varying", IsNullable: "NO", CharMaxLen: sql.NullInt64{Int64: 50, Valid: true}},
		"description":   {TargetType: "text", IsNullable: "YES"},
		"parent_cat_id": {TargetType: "integer", IsNullable: "YES"},
	}
	t.Run("VerifyTableSchema_categories", func(t *testing.T) {
		verifyTableSchema(t, ctx, targetDB.DB, "categories", expectedCategoriesSchema)
	})
	t.Run("VerifyDataCount_categories", func(t *testing.T) { res, _ := results["categories"]; assert.EqualValues(t, 3, res.RowsSynced, "categories row count") })


	expectedPostsSchema := map[string]expectedPgColumnSchema{
		"post_id":     {TargetType: "integer", IsNullable: "NO", IsIdentity: "YES", IdentityGeneration: sql.NullString{String: "BY DEFAULT", Valid: true}, ExpectedDefaultSnippet: sql.NullString{String: "nextval", Valid: true}},
		"author_id":   {TargetType: "integer", IsNullable: "NO"},
		"title":       {TargetType: "character varying", IsNullable: "NO", CharMaxLen: sql.NullInt64{Int64: 200, Valid: true}},
		"content":     {TargetType: "text", IsNullable: "YES"},
		"slug":        {TargetType: "character varying", IsNullable: "NO", CharMaxLen: sql.NullInt64{Int64: 100, Valid: true}},
		"published_at":{TargetType: "timestamp without time zone", IsNullable: "YES"}, // MySQL DATETIME maps to timestamp without time zone
		"views":       {TargetType: "integer", IsNullable: "YES", ExpectedDefaultSnippet: sql.NullString{String: "0", Valid: true}},
	}
	t.Run("VerifyTableSchema_posts", func(t *testing.T) {
		verifyTableSchema(t, ctx, targetDB.DB, "posts", expectedPostsSchema)
	})
	t.Run("VerifyDataCount_posts", func(t *testing.T) { res, _ := results["posts"]; assert.EqualValues(t, 4, res.RowsSynced, "posts row count") })


	expectedCommentsSchema := map[string]expectedPgColumnSchema{
		"comment_id":        {TargetType: "integer", IsNullable: "NO", IsIdentity: "YES", IdentityGeneration: sql.NullString{String: "BY DEFAULT", Valid: true}, ExpectedDefaultSnippet: sql.NullString{String: "nextval", Valid: true}},
		"post_id":           {TargetType: "integer", IsNullable: "NO"},
		"user_id":           {TargetType: "integer", IsNullable: "NO"},
		"parent_comment_id": {TargetType: "integer", IsNullable: "YES"},
		"comment_text":      {TargetType: "text", IsNullable: "NO"},
		"commented_at":      {TargetType: "timestamp with time zone", IsNullable: "YES", ExpectedDefaultSnippet: sql.NullString{String: "now()", Valid: true}},
	}
	t.Run("VerifyTableSchema_comments", func(t *testing.T) {
		verifyTableSchema(t, ctx, targetDB.DB, "comments", expectedCommentsSchema)
	})
	t.Run("VerifyDataCount_comments", func(t *testing.T) { res, _ := results["comments"]; assert.EqualValues(t, 3, res.RowsSynced, "comments row count") })


	expectedPostCategoriesSchema := map[string]expectedPgColumnSchema{
		"post_id":     {TargetType: "integer", IsNullable: "NO"},
		"category_id": {TargetType: "integer", IsNullable: "NO"},
	}
	t.Run("VerifyTableSchema_post_categories", func(t *testing.T) {
		verifyTableSchema(t, ctx, targetDB.DB, "post_categories", expectedPostCategoriesSchema)
	})
	t.Run("VerifyDataCount_post_categories", func(t *testing.T) { res, _ := results["post_categories"]; assert.EqualValues(t, 5, res.RowsSynced, "post_categories row count") })


    // Verification for new tables 'products' and 'product_variants'
    expectedProductsSchema := map[string]expectedPgColumnSchema{
        "product_id":   {TargetType: "integer", IsNullable: "NO", IsIdentity: "YES", IdentityGeneration: sql.NullString{String: "BY DEFAULT", Valid: true}, ExpectedDefaultSnippet: sql.NullString{String: "nextval", Valid: true}},
        "product_name": {TargetType: "character varying", IsNullable: "NO", CharMaxLen: sql.NullInt64{Int64: 255, Valid: true}},
        "description":  {TargetType: "text", IsNullable: "YES"},
        "price":        {TargetType: "numeric", IsNullable: "NO", NumPrecision: sql.NullInt64{Int64: 10, Valid: true}, NumScale: sql.NullInt64{Int64: 2, Valid: true}},
        "created_date": {TargetType: "date", IsNullable: "YES", ExpectedDefaultSnippet: sql.NullString{String: "CURRENT_DATE", Valid: true}}, // MySQL DATE with DEFAULT (CURDATE())
    }
    t.Run("VerifyTableSchema_products", func(t *testing.T) {
        verifyTableSchema(t, ctx, targetDB.DB, "products", expectedProductsSchema)
    })
	t.Run("VerifyDataCount_products", func(t *testing.T) { res, _ := results["products"]; assert.EqualValues(t, 2, res.RowsSynced, "products row count") })


    expectedProductVariantsSchema := map[string]expectedPgColumnSchema{
        "variant_id": {TargetType: "integer", IsNullable: "NO", IsIdentity: "YES", IdentityGeneration: sql.NullString{String: "BY DEFAULT", Valid: true}, ExpectedDefaultSnippet: sql.NullString{String: "nextval", Valid: true}},
        "product_id": {TargetType: "integer", IsNullable: "NO"},
        "sku":        {TargetType: "character varying", IsNullable: "NO", CharMaxLen: sql.NullInt64{Int64: 100, Valid: true}},
        "attributes": {TargetType: "jsonb", IsNullable: "YES"},
        "stock_qty":  {TargetType: "integer", IsNullable: "NO", ExpectedDefaultSnippet: sql.NullString{String: "0", Valid: true}},
    }
    t.Run("VerifyTableSchema_product_variants", func(t *testing.T) {
        verifyTableSchema(t, ctx, targetDB.DB, "product_variants", expectedProductVariantsSchema)
    })
	t.Run("VerifyDataCount_product_variants", func(t *testing.T) { res, _ := results["product_variants"]; assert.EqualValues(t, 3, res.RowsSynced, "product_variants row count") })


	// --- Foreign Key Verification ---
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
            WHERE kcu.table_schema = current_schema() AND kcu.table_name = $1 AND kcu.column_name = $2
              AND ccu.table_schema = current_schema() AND ccu.table_name = $3 AND ccu.column_name = $4;
        `
		queryCtx, queryCancel := context.WithTimeout(ctx, 10*time.Second)
		defer queryCancel()
		errQuery := db.WithContext(queryCtx).Raw(query, fromTable, fromColumn, toTable, toColumn).Scan(&constraintName).Error

		if errors.Is(errQuery, gorm.ErrRecordNotFound) { // Use errors.Is for GORM v2
			t.Fatalf("Foreign key from %s(%s) to %s(%s) (expected name like '%s') NOT FOUND", fromTable, fromColumn, toTable, toColumn, fkNameHint)
		}
		require.NoError(t, errQuery, "Error querying for FK %s(%s) -> %s(%s) (hint: %s)", fromTable, fromColumn, toTable, toColumn, fkNameHint)
		assert.NotEmpty(t, constraintName, "Foreign key from %s(%s) to %s(%s) (hint: %s) should exist and have a name", fromTable, fromColumn, toTable, toColumn, fkNameHint)
		t.Logf("Found FK from %s(%s) to %s(%s) with name: %s (Hint was: '%s')", fromTable, fromColumn, toTable, toColumn, constraintName, fkNameHint)
	}

	t.Run("VerifyForeignKey_posts_to_users", func(t *testing.T) {
		verifyForeignKeyExists(t, targetDB.DB, "posts", "author_id", "users", "id", "posts_author_id_fkey") // PG default FK name
	})
	t.Run("VerifyForeignKey_comments_to_posts", func(t *testing.T) {
		verifyForeignKeyExists(t, targetDB.DB, "comments", "post_id", "posts", "post_id", "comments_post_id_fkey")
	})
	t.Run("VerifyForeignKey_comments_to_users", func(t *testing.T) {
		verifyForeignKeyExists(t, targetDB.DB, "comments", "user_id", "users", "id", "comments_user_id_fkey")
	})
	t.Run("VerifyForeignKey_comments_to_self_comments", func(t *testing.T) {
		verifyForeignKeyExists(t, targetDB.DB, "comments", "parent_comment_id", "comments", "comment_id", "comments_parent_comment_id_fkey")
	})
	t.Run("VerifyForeignKey_post_categories_to_posts", func(t *testing.T) {
		verifyForeignKeyExists(t, targetDB.DB, "post_categories", "post_id", "posts", "post_id", "post_categories_post_id_fkey")
	})
	t.Run("VerifyForeignKey_post_categories_to_categories", func(t *testing.T) {
		verifyForeignKeyExists(t, targetDB.DB, "post_categories", "category_id", "categories", "category_id", "post_categories_category_id_fkey")
	})
	t.Run("VerifyForeignKey_categories_to_self_categories", func(t *testing.T) {
		verifyForeignKeyExists(t, targetDB.DB, "categories", "parent_cat_id", "categories", "category_id", "categories_parent_cat_id_fkey")
	})
    t.Run("VerifyForeignKey_product_variants_to_products", func(t *testing.T) {
		verifyForeignKeyExists(t, targetDB.DB, "product_variants", "product_id", "products", "product_id", "product_variants_product_id_fkey")
	})


	// --- Unique Constraint Verification ---
	verifyUniqueConstraintExists := func(t *testing.T, db *gorm.DB, tableName string, columnNames []string, constraintNameHint string) {
		t.Helper()
		var count int
		// This query checks if a unique or primary key constraint exists that *exactly* covers the given set of columns
		// It's a bit complex due to how constraints and columns are stored in information_schema.
		// We count constraints that have the same number of columns as specified AND all specified columns are present in that constraint.
		colsFormatted := "ARRAY['" + strings.Join(columnNames, "','") + "']::TEXT[]"
		query := fmt.Sprintf(`
			SELECT COUNT(DISTINCT tc.constraint_name)
			FROM information_schema.table_constraints tc
			JOIN (
				SELECT constraint_name, constraint_schema, table_name, array_agg(column_name::TEXT ORDER BY ordinal_position) as columns_in_constraint
				FROM information_schema.key_column_usage
				WHERE table_schema = current_schema() AND table_name = $1
				GROUP BY constraint_name, constraint_schema, table_name
			) kcu ON tc.constraint_name = kcu.constraint_name AND tc.constraint_schema = kcu.constraint_schema AND tc.table_name = kcu.table_name
			WHERE tc.table_schema = current_schema()
			  AND tc.table_name = $1
			  AND tc.constraint_type IN ('UNIQUE', 'PRIMARY KEY')
			  AND kcu.columns_in_constraint @> %s AND kcu.columns_in_constraint <@ %s;
		`, colsFormatted, colsFormatted) // Checks for array equality (both ways for exact match)

		queryCtx, queryCancel := context.WithTimeout(ctx, 10*time.Second)
		defer queryCancel()
		errQuery := db.WithContext(queryCtx).Raw(query, tableName).Scan(&count).Error
		require.NoError(t, errQuery, "Error querying for UNIQUE/PK constraint on %s (columns: %v, hint: %s)", tableName, columnNames, constraintNameHint)
		assert.GreaterOrEqual(t, count, 1, "UNIQUE or PRIMARY KEY constraint on %s (columns: %v, hint: %s) should exist (found %d)", tableName, columnNames, constraintNameHint, count)
		t.Logf("Found %d UNIQUE or PK constraint(s) for columns %v on table %s (Hint was: '%s')", count, columnNames, tableName, constraintNameHint)
	}


	t.Run("VerifyUniqueConstraint_users_username", func(t *testing.T) {
		verifyUniqueConstraintExists(t, targetDB.DB, "users", []string{"username"}, "users_username_key") // PG default name
	})
	t.Run("VerifyUniqueConstraint_users_email", func(t *testing.T) {
		verifyUniqueConstraintExists(t, targetDB.DB, "users", []string{"email"}, "users_email_key")
	})
	t.Run("VerifyUniqueConstraint_categories_name", func(t *testing.T) {
		verifyUniqueConstraintExists(t, targetDB.DB, "categories", []string{"name"}, "categories_name_key")
	})
	t.Run("VerifyUniqueConstraint_posts_slug", func(t *testing.T) {
		verifyUniqueConstraintExists(t, targetDB.DB, "posts", []string{"slug"}, "posts_slug_key")
	})
    t.Run("VerifyUniqueConstraint_product_variants_sku", func(t *testing.T) {
		verifyUniqueConstraintExists(t, targetDB.DB, "product_variants", []string{"sku"}, "product_variants_sku_key")
	})


	// --- Primary Key Verification (Combined with Unique check for tables with simple PKs) ---
	t.Run("VerifyPrimaryKey_users", func(t *testing.T) {
		verifyUniqueConstraintExists(t, targetDB.DB, "users", []string{"id"}, "users_pkey")
	})
	t.Run("VerifyPrimaryKey_categories", func(t *testing.T) {
		verifyUniqueConstraintExists(t, targetDB.DB, "categories", []string{"category_id"}, "categories_pkey")
	})
	t.Run("VerifyPrimaryKey_posts", func(t *testing.T) {
		verifyUniqueConstraintExists(t, targetDB.DB, "posts", []string{"post_id"}, "posts_pkey")
	})
	t.Run("VerifyPrimaryKey_comments", func(t *testing.T) {
		verifyUniqueConstraintExists(t, targetDB.DB, "comments", []string{"comment_id"}, "comments_pkey")
	})
	t.Run("VerifyPrimaryKey_post_categories", func(t *testing.T) { // Composite PK
		verifyUniqueConstraintExists(t, targetDB.DB, "post_categories", []string{"post_id", "category_id"}, "post_categories_pkey")
	})
    t.Run("VerifyPrimaryKey_products", func(t *testing.T) {
		verifyUniqueConstraintExists(t, targetDB.DB, "products", []string{"product_id"}, "products_pkey")
	})
    t.Run("VerifyPrimaryKey_product_variants", func(t *testing.T) {
		verifyUniqueConstraintExists(t, targetDB.DB, "product_variants", []string{"variant_id"}, "product_variants_pkey")
	})
}
