package sync

import (
	"context"
	"database/sql"
	"fmt"
	"regexp" // Pastikan ini ada
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"
	// "gorm.io/gorm/schema" // Impor ini sudah dihapus

	"github.com/arwahdevops/dbsync/internal/config"
	"github.com/arwahdevops/dbsync/internal/utils"
)

// --- Structs ---

type SchemaSyncer struct {
	srcDB      *gorm.DB
	dstDB      *gorm.DB
	srcDialect string
	dstDialect string
	logger     *zap.Logger
}

type ColumnInfo struct {
	Name            string
	Type            string // Source database type (raw)
	MappedType      string // Destination database type
	IsNullable      bool
	IsPrimary       bool
	IsGenerated     bool           // For generated columns
	DefaultValue    sql.NullString // Use sql.NullString for nullable default values
	AutoIncrement   bool
	OrdinalPosition int
	// Tambahan untuk perbandingan yang lebih baik
	Length    sql.NullInt64 // CharacterMaximumLength or similar
	Precision sql.NullInt64 // NumericPrecision
	Scale     sql.NullInt64 // NumericScale
	Collation sql.NullString
	Comment   sql.NullString
}

type IndexInfo struct {
	Name      string
	Columns   []string // Sorted column names
	IsUnique  bool
	IsPrimary bool   // Flag if it's the PK index
	RawDef    string // Raw definition if available (Postgres)
}

type ConstraintInfo struct {
	Name           string
	Type           string // "PRIMARY KEY", "UNIQUE", "FOREIGN KEY", "CHECK"
	Columns        []string // Sorted column names
	Definition     string   // Check clause or FK details
	ForeignTable   string
	ForeignColumns []string // Sorted foreign column names
	OnDelete       string
	OnUpdate       string
}

// Ensure SchemaSyncer implements the interface
var _ SchemaSyncerInterface = (*SchemaSyncer)(nil)

// --- Constructor ---

func NewSchemaSyncer(srcDB, dstDB *gorm.DB, srcDialect, dstDialect string, logger *zap.Logger) *SchemaSyncer {
	return &SchemaSyncer{
		srcDB:      srcDB,
		dstDB:      dstDB,
		srcDialect: srcDialect,
		dstDialect: dstDialect,
		logger:     logger.Named("schema-syncer"),
	}
}

// --- Interface Implementation ---

// SyncTableSchema analyzes source/destination and generates DDLs based on the strategy.
func (s *SchemaSyncer) SyncTableSchema(ctx context.Context, table string, strategy config.SchemaSyncStrategy) (*SchemaExecutionResult, error) {
	start := time.Now()
	log := s.logger.With(zap.String("table", table), zap.String("strategy", string(strategy)))
	log.Info("Starting schema analysis")

	result := &SchemaExecutionResult{}

	if s.isSystemTable(table, s.srcDialect) {
		log.Debug("Skipping system table")
		return result, nil // Return empty result for skipped system tables
	}

	// --- Get Source Schema ---
	srcColumns, err := s.getColumns(ctx, s.srcDB, s.srcDialect, table)
	if err != nil { return nil, fmt.Errorf("failed to get source columns for %s: %w", table, err) }
	if len(srcColumns) == 0 { log.Warn("Source table %s has no columns or does not exist.", zap.String("table", table)); return result, nil }
	result.PrimaryKeys = getPKColumnNames(srcColumns) // Store unquoted PK names

	srcIndexes, err := s.getIndexes(ctx, s.srcDB, s.srcDialect, table) // Use receiver s.
	if err != nil { return nil, fmt.Errorf("failed to get source indexes for %s: %w", table, err) }

	srcConstraints, err := s.getConstraints(ctx, s.srcDB, s.srcDialect, table) // Use receiver s.
	if err != nil { return nil, fmt.Errorf("failed to get source constraints for %s: %w", table, err) }
	// --- End Get Source Schema ---


	// Generate DDL based on strategy
	switch strategy {
	case config.SchemaSyncDropCreate:
		log.Warn("Using 'drop_create' strategy - ALL DATA in destination table will be LOST!")
		tableDDL, _, err := s.generateCreateTableDDL(table, srcColumns) // PKs included in create DDL; Use receiver s.
		if err != nil { return nil, fmt.Errorf("failed to generate CREATE TABLE DDL: %w", err) }
		result.TableDDL = tableDDL
		// Generate DDLs for indexes and constraints based on source schema
		result.IndexDDLs = s.generateCreateIndexDDLs(table, srcIndexes)     // Use receiver s.
		result.ConstraintDDLs = s.generateAddConstraintDDLs(table, srcConstraints) // Use receiver s.

	case config.SchemaSyncAlter:
		log.Info("Attempting 'alter' schema synchronization strategy")
		dstColumns, dstIndexes, dstConstraints, dstExists, err := s.getDestinationSchema(ctx, table)
		if err != nil { return nil, fmt.Errorf("failed to get destination schema for %s: %w", table, err) }

		if !dstExists {
			log.Info("Destination table does not exist, performing CREATE TABLE instead of ALTER")
			tableDDL, _, err := s.generateCreateTableDDL(table, srcColumns) // Use receiver s.
			if err != nil { return nil, fmt.Errorf("failed to generate CREATE TABLE DDL for non-existent dest table: %w", err) }
			result.TableDDL = tableDDL
			result.IndexDDLs = s.generateCreateIndexDDLs(table, srcIndexes)     // Use receiver s.
			result.ConstraintDDLs = s.generateAddConstraintDDLs(table, srcConstraints) // Use receiver s.
		} else {
			log.Debug("Destination table exists, generating ALTER statements")
			alterDDLs, idxDDLs, consDDLs, err := s.generateAlterDDLs(table,
				srcColumns, srcIndexes, srcConstraints,
				dstColumns, dstIndexes, dstConstraints) // Use receiver s.
			if err != nil { return nil, fmt.Errorf("failed to generate ALTER DDLs for %s: %w", table, err) }
			result.TableDDL = strings.Join(alterDDLs, ";\n") // Join column alters
			result.IndexDDLs = idxDDLs
			result.ConstraintDDLs = consDDLs
		}

	case config.SchemaSyncNone:
		log.Info("Schema sync strategy is 'none', skipping DDL generation.")
		// PKs sudah di-set, tidak perlu DDL
	default:
		return nil, fmt.Errorf("unknown schema sync strategy: %s", strategy)
	}

	log.Info("Schema analysis completed", zap.Duration("duration", time.Since(start)))
	return result, nil
}


// getDestinationSchema fetches the schema details for the destination table.
func (s *SchemaSyncer) getDestinationSchema(ctx context.Context, table string) (
	columns []ColumnInfo, indexes []IndexInfo, constraints []ConstraintInfo, exists bool, err error) {

	migrator := s.dstDB.Migrator()
	if !migrator.HasTable(table) {
		s.logger.Debug("Destination table does not exist", zap.String("table", table))
		return nil, nil, nil, false, nil
	}

	columns, err = s.getColumns(ctx, s.dstDB, s.dstDialect, table)
	if err != nil { return nil, nil, nil, true, fmt.Errorf("dest columns: %w", err) }

	indexes, err = s.getIndexes(ctx, s.dstDB, s.dstDialect, table) // Use receiver s.
	if err != nil { return nil, nil, nil, true, fmt.Errorf("dest indexes: %w", err) }

	constraints, err = s.getConstraints(ctx, s.dstDB, s.dstDialect, table) // Use receiver s.
	if err != nil { return nil, nil, nil, true, fmt.Errorf("dest constraints: %w", err) }

	return columns, indexes, constraints, true, nil
}


// ExecuteDDLs applies the generated DDLs to the destination database in phases.
func (s *SchemaSyncer) ExecuteDDLs(ctx context.Context, table string, ddls *SchemaExecutionResult) error {
	log := s.logger.With(zap.String("table", table))

	if ddls == nil || (ddls.TableDDL == "" && len(ddls.IndexDDLs) == 0 && len(ddls.ConstraintDDLs) == 0) {
		log.Debug("No DDLs to execute")
		return nil
	}

	// Separate DDLs by type for phased execution
	alterColumnDDLs := make([]string, 0)
	createTableDDL := ""
	indexDDLs := ddls.IndexDDLs         // Includes potential DROP and ADD
	constraintDDLs := ddls.ConstraintDDLs // Includes potential DROP and ADD

	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(ddls.TableDDL)), "CREATE TABLE") {
		createTableDDL = ddls.TableDDL
	} else if ddls.TableDDL != "" {
		// Assume ALTER statements, split them if joined by semicolon
		for _, ddl := range strings.Split(ddls.TableDDL, ";\n") {
			trimmed := strings.TrimSpace(ddl)
			if trimmed != "" {
				alterColumnDDLs = append(alterColumnDDLs, trimmed)
			}
		}
	}

	// Filter DROP constraints/indexes to execute them first
	finalConstraintDDLs := make([]string, 0, len(constraintDDLs))
	finalIndexDDLs := make([]string, 0, len(indexDDLs))
	dropConstraintDDLs := make([]string, 0)
	dropIndexDDLs := make([]string, 0)

	for _, ddl := range constraintDDLs {
		if strings.Contains(strings.ToUpper(ddl), "DROP CONSTRAINT") {
			dropConstraintDDLs = append(dropConstraintDDLs, ddl)
		} else {
			finalConstraintDDLs = append(finalConstraintDDLs, ddl)
		}
	}
	for _, ddl := range indexDDLs {
		if strings.Contains(strings.ToUpper(ddl), "DROP INDEX") {
			dropIndexDDLs = append(dropIndexDDLs, ddl)
		} else {
			finalIndexDDLs = append(finalIndexDDLs, ddl)
		}
	}

	// Execute in a transaction (best effort, DDL transactional behavior varies)
	return s.dstDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var revertFK func() error // Function to re-enable FKs

		// --- Phase 0: Disable FKs (MySQL for DROP/CREATE) ---
		if createTableDDL != "" && s.dstDialect == "mysql" {
			log.Debug("Disabling MySQL FK checks for DROP/CREATE")
			if err := tx.Exec("SET FOREIGN_KEY_CHECKS=0;").Error; err != nil {
				return fmt.Errorf("failed to disable mysql fks: %w", err)
			}
			revertFK = func() error {
				log.Debug("Re-enabling MySQL FK checks")
				// Use background context for revert? No, stay in transaction context.
				return tx.Exec("SET FOREIGN_KEY_CHECKS=1;").Error // Return error for logging
			}
			// Defer the re-enable within the transaction scope
			defer func() {
				if rErr := revertFK(); rErr != nil {
					log.Error("Failed to re-enable mysql fks", zap.Error(rErr))
					// Should we panic or just log? Logging is safer.
				}
			}()
		}

		// --- Phase 1: Drop Constraints & Indexes (for ALTER mode primarily) ---
		if len(dropConstraintDDLs) > 0 || len(dropIndexDDLs) > 0 {
			log.Info("Executing Phase 1: Drop Constraints/Indexes")
			// Drop FKs first if possible (to avoid dependency issues)
			sortedDropConstraints := sortConstraintsForDrop(dropConstraintDDLs)
			for _, ddl := range sortedDropConstraints {
				log.Debug("Drop Constraint DDL", zap.String("ddl", ddl))
				if err := tx.Exec(ddl).Error; err != nil {
					log.Error("Failed to drop constraint, continuing...", zap.Error(err), zap.String("ddl", ddl))
					// Optionally return err here to make it fatal
				}
			}
			for _, ddl := range dropIndexDDLs {
				log.Debug("Drop Index DDL", zap.String("ddl", ddl))
				if err := tx.Exec(ddl).Error; err != nil {
					log.Error("Failed to drop index, continuing...", zap.Error(err), zap.String("ddl", ddl))
					// Optionally return err here
				}
			}
		}


		// --- Phase 2: Handle Table Structure (CREATE or ALTER Columns) ---
		if createTableDDL != "" {
			// Drop existing table first for drop_create strategy
			log.Warn("Executing DROP TABLE IF EXISTS before CREATE")
			dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", utils.QuoteIdentifier(table, s.dstDialect))
			if s.dstDialect == "postgres" {
				dropSQL += " CASCADE"
				log.Warn("Using DROP TABLE CASCADE for PostgreSQL!")
			}
			if err := tx.Exec(dropSQL).Error; err != nil { return fmt.Errorf("failed to drop table %s: %w", table, err) }

			log.Info("Executing Phase 2: Create Table")
			log.Debug("Create Table DDL", zap.String("ddl", createTableDDL))
			if err := tx.Exec(createTableDDL).Error; err != nil { return fmt.Errorf("failed to create table %s: %w", table, err) }
		} else if len(alterColumnDDLs) > 0 {
			log.Info("Executing Phase 2: Alter Columns")
			// Execute ALTER column statements. Order might matter.
			// Try to execute DROPs first?
			sortedAlters := sortAlterColumns(alterColumnDDLs)
			for _, ddl := range sortedAlters {
				log.Debug("Alter Column DDL", zap.String("ddl", ddl))
				if err := tx.Exec(ddl).Error; err != nil {
					log.Error("Failed to execute alter column DDL, attempting to continue...", zap.Error(err), zap.String("ddl", ddl))
					// Return error to make ALTER failures fatal? Recommended for safety.
					// return fmt.Errorf("failed to alter column for %s: %w, ddl: %s", table, err, ddl)
				}
			}
		} else {
			log.Info("Phase 2: No table structure changes needed.")
		}

		// --- Phase 3: Add Indexes ---
		if len(finalIndexDDLs) > 0 {
			log.Info("Executing Phase 3: Create Indexes")
			for _, ddl := range finalIndexDDLs {
				log.Debug("Create Index DDL", zap.String("ddl", ddl))
				if err := tx.Exec(ddl).Error; err != nil {
					log.Error("Failed to create index, continuing...", zap.Error(err), zap.String("ddl", ddl))
					// Optionally return err
				}
			}
		}

		// --- Phase 4: Add Constraints ---
		if len(finalConstraintDDLs) > 0 {
			log.Info("Executing Phase 4: Add Constraints")
			// Apply FKs last if possible
			sortedAddConstraints := sortConstraintsForAdd(finalConstraintDDLs)
			for _, ddl := range sortedAddConstraints {
				log.Debug("Add Constraint DDL", zap.String("ddl", ddl))
				if err := tx.Exec(ddl).Error; err != nil {
					log.Error("Failed to add constraint, continuing...", zap.Error(err), zap.String("ddl", ddl))
					// Optionally return err
				}
			}
		}

		log.Info("DDL execution transaction finished successfully")
		return nil // Commit transaction
	}) // End Transaction
}

// GetPrimaryKeys fetches primary key column names.
func (s *SchemaSyncer) GetPrimaryKeys(ctx context.Context, table string) ([]string, error) {
	columns, err := s.getColumns(ctx, s.srcDB, s.srcDialect, table)
	if err != nil { return nil, fmt.Errorf("failed get columns for PKs table %s: %w", table, err) }
	pks := getPKColumnNames(columns)
	if len(pks) == 0 { s.logger.Warn("No primary key detected", zap.String("table", table), zap.String("dialect", s.srcDialect)) }
	return pks, nil
}


// --- Helper Functions ---

func getPKColumnNames(columns []ColumnInfo) []string {
	var primaryKeys []string
	for _, col := range columns { if col.IsPrimary { primaryKeys = append(primaryKeys, col.Name) } }
	sort.Strings(primaryKeys)
	return primaryKeys
}

func (s *SchemaSyncer) isSystemTable(table, dialect string) bool {
	tableLower := strings.ToLower(table)
	dialectLower := strings.ToLower(dialect)
	switch dialectLower {
	case "mysql": return tableLower == "information_schema" || tableLower == "performance_schema" || tableLower == "mysql" || tableLower == "sys"
	case "postgres": return strings.HasPrefix(tableLower, "pg_") || tableLower == "information_schema"
	case "sqlite": return strings.HasPrefix(tableLower, "sqlite_")
	default: return false
	}
}

func (s *SchemaSyncer) getColumns(ctx context.Context, db *gorm.DB, dialect string, table string) ([]ColumnInfo, error) {
	switch dialect {
	case "mysql": return s.getMySQLColumns(ctx, db, table)
	case "postgres": return s.getPostgresColumns(ctx, db, table)
	case "sqlite": return s.getSQLiteColumns(ctx, db, table)
	default: return nil, fmt.Errorf("getColumns: unsupported dialect: %s", dialect)
	}
}


// --- Dialect Specific Column Fetching (dengan lebih banyak detail) ---

func (s *SchemaSyncer) getMySQLColumns(ctx context.Context, db *gorm.DB, table string) ([]ColumnInfo, error) {
	var columnsData []struct {
		Field           string         `gorm:"column:COLUMN_NAME"`
		OrdinalPosition int            `gorm:"column:ORDINAL_POSITION"`
		Default         sql.NullString `gorm:"column:COLUMN_DEFAULT"`
		IsNullable      string         `gorm:"column:IS_NULLABLE"` // YES / NO
		Type            string         `gorm:"column:DATA_TYPE"`   // Mis. varchar, int
		FullType        string         `gorm:"column:COLUMN_TYPE"` // Mis. varchar(255), int(11)
		Length          sql.NullInt64  `gorm:"column:CHARACTER_MAXIMUM_LENGTH"`
		Precision       sql.NullInt64  `gorm:"column:NUMERIC_PRECISION"`
		Scale           sql.NullInt64  `gorm:"column:NUMERIC_SCALE"`
		Key             string         `gorm:"column:COLUMN_KEY"` // PRI, UNI, MUL
		Extra           string         `gorm:"column:EXTRA"`      // auto_increment, DEFAULT_GENERATED, VIRTUAL GENERATED, STORED GENERATED
		Collation       sql.NullString `gorm:"column:COLLATION_NAME"`
		Comment         sql.NullString `gorm:"column:COLUMN_COMMENT"`
		GenerationExpr  sql.NullString `gorm:"column:GENERATION_EXPRESSION"` // MySQL 5.7+
	}
	// Query information_schema, termasuk GENERATION_EXPRESSION jika ada
	query := `
		SELECT COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE,
		       COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
		       COLUMN_KEY, EXTRA, COLLATION_NAME, COLUMN_COMMENT,
		       CASE WHEN @@version NOT LIKE '5.6%' THEN GENERATION_EXPRESSION ELSE NULL END AS GENERATION_EXPRESSION
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`
	err := db.WithContext(ctx).Raw(query, table).Scan(&columnsData).Error
	if err != nil {
		// Cek apakah error karena kolom GENERATION_EXPRESSION tidak ada (MySQL < 5.7)
		if strings.Contains(err.Error(), "Unknown column 'GENERATION_EXPRESSION'") {
			s.logger.Debug("GENERATION_EXPRESSION column not found, likely MySQL < 5.7. Retrying without it.")
			queryLegacy := `
				SELECT COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE,
					   COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
					   COLUMN_KEY, EXTRA, COLLATION_NAME, COLUMN_COMMENT,
					   NULL AS GENERATION_EXPRESSION
				FROM information_schema.COLUMNS
				WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
				ORDER BY ORDINAL_POSITION
			`
			err = db.WithContext(ctx).Raw(queryLegacy, table).Scan(&columnsData).Error
			if err != nil {
				return nil, fmt.Errorf("mysql columns legacy query failed for %s: %w", table, err)
			}
		} else {
			return nil, fmt.Errorf("mysql columns query failed for %s: %w", table, err)
		}
	}

	result := make([]ColumnInfo, 0, len(columnsData))
	for _, c := range columnsData {
		isGenerated := strings.Contains(c.Extra, "GENERATED") || c.GenerationExpr.Valid
		colInfo := ColumnInfo{
			Name:            c.Field, Type: c.FullType, IsNullable: strings.ToUpper(c.IsNullable) == "YES",
			IsPrimary: c.Key == "PRI", DefaultValue: c.Default, AutoIncrement: strings.Contains(c.Extra, "auto_increment"),
			IsGenerated: isGenerated, OrdinalPosition: c.OrdinalPosition,
			Length: c.Length, Precision: c.Precision, Scale: c.Scale, Collation: c.Collation, Comment: c.Comment,
		}
		// Jangan petakan tipe jika generated (biarkan DB tujuan yang handle jika memungkinkan)
		var mappedType string
		var mapErr error
		if !isGenerated {
			mappedType, mapErr = s.mapDataType(colInfo.Type) // Use receiver s.
			if mapErr != nil { s.logger.Warn("Mapping failed", zap.String("col", c.Field), zap.Error(mapErr)); mappedType = colInfo.Type }
		} else {
			mappedType = colInfo.Type // Gunakan tipe asli untuk generated column
			s.logger.Warn("Mapping skipped for generated column", zap.String("col", c.Field), zap.String("type", colInfo.Type))
		}
		colInfo.MappedType = mappedType
		result = append(result, colInfo)
	}
	return result, nil
}

func (s *SchemaSyncer) getPostgresColumns(ctx context.Context, db *gorm.DB, table string) ([]ColumnInfo, error) {
	query := `
	SELECT c.column_name, c.ordinal_position, c.column_default, c.is_nullable, c.data_type, c.udt_name,
	       c.character_maximum_length, c.numeric_precision, c.numeric_scale, c.collation_name,
	       pgd.description AS column_comment, -- Get comment from pg_description
	       c.is_identity, c.identity_generation, -- Identity columns (PG 10+)
		   c.is_generated, c.generation_expression, -- Generated columns (PG 12+)
	       CASE WHEN tc.constraint_type = 'PRIMARY KEY' THEN TRUE ELSE FALSE END AS is_primary_key
	FROM information_schema.columns c
	LEFT JOIN information_schema.key_column_usage kcu ON c.table_schema = kcu.table_schema AND c.table_name = kcu.table_name AND c.column_name = kcu.column_name
	LEFT JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name AND kcu.table_schema = tc.table_schema AND kcu.table_name = tc.table_name AND tc.constraint_type = 'PRIMARY KEY'
	LEFT JOIN pg_catalog.pg_statio_all_tables AS st ON c.table_schema = st.schemaname AND c.table_name = st.relname
	LEFT JOIN pg_catalog.pg_description pgd ON pgd.objoid = st.relid AND pgd.objsubid = c.ordinal_position -- Join for comments
	WHERE c.table_schema = current_schema() AND c.table_name = $1 ORDER BY c.ordinal_position;
	`
	var columnsData []struct {
		ColumnName             string         `gorm:"column:column_name"`
		OrdinalPosition        int            `gorm:"column:ordinal_position"`
		ColumnDefault          sql.NullString `gorm:"column:column_default"`
		IsNullable             string         `gorm:"column:is_nullable"`
		DataType               string         `gorm:"column:data_type"`
		UdtName                string         `gorm:"column:udt_name"`
		CharacterMaximumLength sql.NullInt64  `gorm:"column:character_maximum_length"`
		NumericPrecision       sql.NullInt64  `gorm:"column:numeric_precision"`
		NumericScale           sql.NullInt64  `gorm:"column:numeric_scale"`
		CollationName          sql.NullString `gorm:"column:collation_name"`
		ColumnComment          sql.NullString `gorm:"column:column_comment"`
		IsIdentity             string         `gorm:"column:is_identity"`
		IdentityGeneration     sql.NullString `gorm:"column:identity_generation"`
		IsGenerated            sql.NullString `gorm:"column:is_generated"` // ALWAYS / NEVER
		GenerationExpression   sql.NullString `gorm:"column:generation_expression"`
		IsPrimaryKey           bool           `gorm:"column:is_primary_key"`
	}
	err := db.WithContext(ctx).Raw(query, table).Scan(&columnsData).Error
	if err != nil {
		// Cek error spesifik jika kolom generation_expression tidak ada (PG < 12)
		if strings.Contains(err.Error(), "column c.is_generated does not exist") || strings.Contains(err.Error(), "column c.generation_expression does not exist") {
			s.logger.Debug("is_generated/generation_expression columns not found, likely PG < 12. Retrying without them.")
			queryLegacy := `
			SELECT c.column_name, c.ordinal_position, c.column_default, c.is_nullable, c.data_type, c.udt_name,
				   c.character_maximum_length, c.numeric_precision, c.numeric_scale, c.collation_name,
				   pgd.description AS column_comment, c.is_identity, c.identity_generation,
				   NULL AS is_generated, NULL AS generation_expression, -- Placeholder NULLs
				   CASE WHEN tc.constraint_type = 'PRIMARY KEY' THEN TRUE ELSE FALSE END AS is_primary_key
			FROM information_schema.columns c
			LEFT JOIN information_schema.key_column_usage kcu ON c.table_schema = kcu.table_schema AND c.table_name = kcu.table_name AND c.column_name = kcu.column_name
			LEFT JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name AND kcu.table_schema = tc.table_schema AND kcu.table_name = tc.table_name AND tc.constraint_type = 'PRIMARY KEY'
			LEFT JOIN pg_catalog.pg_statio_all_tables AS st ON c.table_schema = st.schemaname AND c.table_name = st.relname
			LEFT JOIN pg_catalog.pg_description pgd ON pgd.objoid = st.relid AND pgd.objsubid = c.ordinal_position
			WHERE c.table_schema = current_schema() AND c.table_name = $1 ORDER BY c.ordinal_position;
			`
			err = db.WithContext(ctx).Raw(queryLegacy, table).Scan(&columnsData).Error
			if err != nil {
				return nil, fmt.Errorf("postgres legacy columns query failed for %s: %w", table, err)
			}
		} else {
			return nil, fmt.Errorf("postgres columns query failed for %s: %w", table, err)
		}
	}

	result := make([]ColumnInfo, 0, len(columnsData))
	for _, c := range columnsData {
		srcType := c.DataType
		if c.UdtName != "" && (c.DataType == "ARRAY" || strings.HasPrefix(c.UdtName, "_")) { // Handle array types like _text or data_type=ARRAY
			cleanUdtName := strings.TrimPrefix(c.UdtName, "_")
			srcType = cleanUdtName + "[]"
		} else if c.UdtName != "" && c.DataType == "USER-DEFINED" {
			srcType = c.UdtName // Gunakan UDT name untuk tipe custom
		}
		// TODO: Add modifiers back more reliably based on data_type and other fields
		isAutoIncrement := c.IsIdentity == "YES" || (c.ColumnDefault.Valid && strings.HasPrefix(c.ColumnDefault.String, "nextval("))
		isGenerated := c.IsGenerated.Valid && c.IsGenerated.String == "ALWAYS" // Consider NEVER?

		colInfo := ColumnInfo{
			Name: c.ColumnName, Type: srcType, IsNullable: c.IsNullable == "YES", IsPrimary: c.IsPrimaryKey,
			DefaultValue: c.ColumnDefault, AutoIncrement: isAutoIncrement, IsGenerated: isGenerated,
			OrdinalPosition: c.OrdinalPosition, Length: c.CharacterMaximumLength, Precision: c.NumericPrecision,
			Scale: c.NumericScale, Collation: c.CollationName, Comment: c.ColumnComment,
		}

		var mappedType string
		var mapErr error
		if !isGenerated {
			mappedType, mapErr = s.mapDataType(colInfo.Type) // Use receiver s.
			if mapErr != nil { s.logger.Warn("Mapping failed", zap.String("col", c.ColumnName), zap.Error(mapErr)); mappedType = colInfo.Type }
		} else {
			mappedType = colInfo.Type // Gunakan tipe asli
			s.logger.Warn("Mapping skipped for generated column", zap.String("col", c.ColumnName), zap.String("type", colInfo.Type))
		}
		colInfo.MappedType = mappedType
		result = append(result, colInfo)
	}
	return result, nil
}

func (s *SchemaSyncer) getSQLiteColumns(ctx context.Context, db *gorm.DB, table string) ([]ColumnInfo, error) {
	// Implementasi sama seperti sebelumnya, detail kolom terbatas di SQLite
	var columnsData []struct {
		Cid       int            `gorm:"column:cid"`
		Name      string         `gorm:"column:name"`
		Type      string         `gorm:"column:type"`
		NotNull   int            `gorm:"column:notnull"`
		DfltValue sql.NullString `gorm:"column:dflt_value"`
		Pk        int            `gorm:"column:pk"`
	}
	if err := db.WithContext(ctx).Raw("PRAGMA table_info(?)", table).Scan(&columnsData).Error; err != nil {
		return nil, fmt.Errorf("sqlite columns query failed for %s: %w", table, err)
	}
	result := make([]ColumnInfo, 0, len(columnsData))
	for i, c := range columnsData {
		isAutoIncrement := c.Pk > 0 && strings.ToUpper(c.Type) == "INTEGER"
		colInfo := ColumnInfo{
			Name: c.Name, Type: c.Type, IsNullable: c.NotNull == 0, IsPrimary: c.Pk > 0,
			DefaultValue: c.DfltValue, AutoIncrement: isAutoIncrement, OrdinalPosition: i,
			// Detail lain tidak tersedia
		}
		mappedType, err := s.mapDataType(colInfo.Type) // Use receiver s.
		if err != nil { s.logger.Warn("Mapping failed", zap.String("col", c.Name), zap.Error(err)); mappedType = colInfo.Type }
		colInfo.MappedType = mappedType
		result = append(result, colInfo)
	}
	return result, nil
}

// --- Index & Constraint Fetching ---

// getIndexes fetches index information for a table.
func (s *SchemaSyncer) getIndexes(ctx context.Context, db *gorm.DB, dialect, table string) ([]IndexInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", dialect))
	log.Debug("Fetching indexes")
	indexes := make([]IndexInfo, 0)
	idxMap := make(map[string]*IndexInfo) // Group columns by index name

	switch dialect {
	case "mysql":
		var results []struct {
			Table      string `gorm:"column:Table"`
			NonUnique  int    `gorm:"column:Non_unique"`
			KeyName    string `gorm:"column:Key_name"`
			SeqInIndex int    `gorm:"column:Seq_in_index"`
			ColumnName string `gorm:"column:Column_name"`
			IndexType  string `gorm:"column:Index_type"`
		}
		query := fmt.Sprintf("SHOW INDEX FROM %s", utils.QuoteIdentifier(table, dialect))
		if err := db.WithContext(ctx).Raw(query).Scan(&results).Error; err != nil {
			if strings.Contains(err.Error(), "doesn't exist") { return indexes, nil }
			return nil, fmt.Errorf("mysql show index failed for %s: %w", table, err)
		}
		for _, r := range results {
			if _, ok := idxMap[r.KeyName]; !ok { idxMap[r.KeyName] = &IndexInfo{ Name: r.KeyName, Columns: make([]string, 0), IsUnique: r.NonUnique == 0, IsPrimary: r.KeyName == "PRIMARY", }}
			currentLen := len(idxMap[r.KeyName].Columns); neededLen := r.SeqInIndex
			if neededLen > currentLen { idxMap[r.KeyName].Columns = append(idxMap[r.KeyName].Columns, make([]string, neededLen-currentLen)...)}
			idxMap[r.KeyName].Columns[r.SeqInIndex-1] = r.ColumnName
		}

	case "postgres":
		query := `
		SELECT i.relname as index_name, idx.indisunique as is_unique, idx.indisprimary as is_primary,
		       a.attname as column_name,
		       array_position(idx.indkey::int[], a.attnum::int) as column_seq,
		       pg_get_indexdef(idx.indexrelid) as raw_def
		FROM pg_class t JOIN pg_index idx ON t.oid = idx.indrelid JOIN pg_class i ON i.oid = idx.indexrelid
		     JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(idx.indkey)
		WHERE t.relkind = 'r' AND t.relname = $1
		  AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())
		ORDER BY index_name, column_seq;
		`
		var results []struct { IndexName string; IsUnique bool; IsPrimary bool; ColumnName string; ColumnSeq sql.NullInt64; RawDef string; }
		if err := db.WithContext(ctx).Raw(query, table).Scan(&results).Error; err != nil {
			if strings.Contains(err.Error(), "does not exist") { return indexes, nil }
			return nil, fmt.Errorf("postgres index query failed for %s: %w", table, err)
		}
		tempColMap := make(map[string][]struct{ Name string; Seq int64 }); idxDetails := make(map[string]*IndexInfo)
		for _, r := range results {
			if _, ok := idxDetails[r.IndexName]; !ok { idxDetails[r.IndexName] = &IndexInfo{Name: r.IndexName, IsUnique: r.IsUnique, IsPrimary: r.IsPrimary, RawDef: r.RawDef}; tempColMap[r.IndexName] = make([]struct{ Name string; Seq int64 }, 0) }
			seq := int64(1); if r.ColumnSeq.Valid { seq = r.ColumnSeq.Int64 }; tempColMap[r.IndexName] = append(tempColMap[r.IndexName], struct{ Name string; Seq int64 }{r.ColumnName, seq})
		}
		for name, idx := range idxDetails {
			cols := tempColMap[name]; sort.Slice(cols, func(i, j int) bool { return cols[i].Seq < cols[j].Seq }); idx.Columns = make([]string, len(cols)); for i, c := range cols { idx.Columns[i] = c.Name }; idxMap[name] = idx
		}


	case "sqlite":
		var indexList []struct { Seq int; Name string; Unique int; Origin string; Partial int; }
		if err := db.WithContext(ctx).Raw("PRAGMA index_list(?)", table).Scan(&indexList).Error; err != nil {
			if strings.Contains(err.Error(), "no such table") { return indexes, nil } // Tabel tidak ada
			return nil, fmt.Errorf("sqlite pragma index_list failed for %s: %w", table, err)
		}
		for _, idxItem := range indexList {
			if strings.HasPrefix(idxItem.Name, "sqlite_autoindex_") { continue }
			var indexInfoList []struct { SeqNo int; Cid int; Name string; }
			queryInfo := fmt.Sprintf("PRAGMA index_info(%s)", utils.QuoteIdentifier(idxItem.Name, dialect))
			if err := db.WithContext(ctx).Raw(queryInfo).Scan(&indexInfoList).Error; err != nil { log.Warn("sqlite pragma index_info failed", zap.String("idx", idxItem.Name), zap.Error(err)); continue }
			if len(indexInfoList) > 0 {
				idx := &IndexInfo{ Name: idxItem.Name, Columns: make([]string, len(indexInfoList)), IsUnique: idxItem.Unique == 1, IsPrimary: idxItem.Origin == "pk", }
				validCols := 0
				for _, colInfo := range indexInfoList {
					if colInfo.SeqNo >= 0 && colInfo.SeqNo < len(idx.Columns) && colInfo.Name != "" {
						idx.Columns[colInfo.SeqNo] = colInfo.Name; validCols++
					} else { log.Warn("Invalid index_info data", zap.Any("info", colInfo)) }
				}
				if validCols < len(idx.Columns) { cleaned := make([]string, 0, validCols); for _, c := range idx.Columns { if c != "" { cleaned = append(cleaned, c) } }; idx.Columns = cleaned }
				if len(idx.Columns) > 0 { idxMap[idx.Name] = idx }
			}
		}

	default:
		log.Warn("Index fetching not implemented for dialect")
	}


	for _, idx := range idxMap {
		sort.Strings(idx.Columns); indexes = append(indexes, *idx)
	}
	sort.Slice(indexes, func(i, j int) bool { return indexes[i].Name < indexes[j].Name })
	return indexes, nil
}

// getConstraints fetches constraint information.
func (s *SchemaSyncer) getConstraints(ctx context.Context, db *gorm.DB, dialect, table string) ([]ConstraintInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", dialect))
	log.Debug("Fetching constraints")
	constraints := make([]ConstraintInfo, 0)
	consMap := make(map[string]*ConstraintInfo)

	switch dialect {
	case "mysql", "postgres":
		var results []struct {
			ConstraintName    string         `gorm:"column:constraint_name"`
			ConstraintType    string         `gorm:"column:constraint_type"`
			ColumnName        string         `gorm:"column:column_name"`
			OrdinalPosition   sql.NullInt64  `gorm:"column:ordinal_position"`
			ForeignTableName  sql.NullString `gorm:"column:foreign_table_name"`
			ForeignColumnName sql.NullString `gorm:"column:foreign_column_name"`
			UpdateRule        sql.NullString `gorm:"column:update_rule"`
			DeleteRule        sql.NullString `gorm:"column:delete_rule"`
			CheckClause       sql.NullString `gorm:"column:check_clause"`
		}
		query := `
		SELECT DISTINCT tc.constraint_name, tc.constraint_type, kcu.column_name, kcu.ordinal_position,
			   ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name,
			   rc.update_rule, rc.delete_rule, cons.check_clause
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema AND tc.table_name = kcu.table_name
		LEFT JOIN information_schema.referential_constraints rc ON tc.constraint_name = rc.constraint_name AND tc.table_schema = rc.constraint_schema
		LEFT JOIN information_schema.constraint_column_usage ccu ON rc.unique_constraint_name = ccu.constraint_name AND rc.unique_constraint_schema = ccu.constraint_schema AND kcu.position_in_unique_constraint = ccu.ordinal_position
		LEFT JOIN information_schema.check_constraints cons ON tc.constraint_name = cons.constraint_name AND tc.constraint_schema = cons.constraint_schema
		WHERE tc.table_name = ? AND tc.table_schema = ? ORDER BY tc.constraint_name, kcu.ordinal_position;
		`
		schemaName := ""
		var schemaErr error
		if dialect == "mysql" {
			sqlDB, dbErr := db.DB()
			if dbErr == nil { var currentDB string; err := sqlDB.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&currentDB); if err == nil && currentDB != "" { schemaName = currentDB } }
			if schemaName == "" { schemaErr = fmt.Errorf("could not determine mysql schema name for constraint query")}
		} else {
			err := db.WithContext(ctx).Raw("SELECT current_schema()").Scan(&schemaName).Error
			if err != nil { log.Warn("Could not determine current_schema, using public", zap.Error(err)); schemaName = "public" }
		}
		if schemaErr != nil { return nil, schemaErr }

		err := db.WithContext(ctx).Raw(query, table, schemaName).Scan(&results).Error
		if err != nil {
			if strings.Contains(err.Error(), "doesn't exist") || strings.Contains(err.Error(), "does not exist"){ return constraints, nil }
			if dialect == "postgres" && strings.Contains(err.Error(), "relation \"information_schema.check_constraints\" does not exist") {
				log.Warn("check_constraints not found, retrying without CHECK.")
				queryNoCheck := strings.Replace(query, "cons.check_clause", "NULL AS check_clause", 1); queryNoCheck = strings.Replace(queryNoCheck, "LEFT JOIN information_schema.check_constraints cons", "-- LEFT JOIN", 1); queryNoCheck = strings.Replace(queryNoCheck, "AND tc.constraint_schema = cons.constraint_schema", "", 1)
				err = db.WithContext(ctx).Raw(queryNoCheck, table, schemaName).Scan(&results).Error
				if err != nil { return nil, fmt.Errorf("%s legacy constraint query failed: %w", dialect, err) }
			} else { return nil, fmt.Errorf("%s constraint query failed: %w", dialect, err) }
		}

		tempFKCols := make(map[string][]struct{ Local, Foreign string; Seq int64 })
		for _, r := range results {
			if _, ok := consMap[r.ConstraintName]; !ok {
				consMap[r.ConstraintName] = &ConstraintInfo{ Name: r.ConstraintName, Type: r.ConstraintType, Columns: make([]string, 0), ForeignTable: r.ForeignTableName.String, ForeignColumns: make([]string, 0), OnDelete: r.DeleteRule.String, OnUpdate: r.UpdateRule.String, Definition: r.CheckClause.String, }
				if r.ConstraintType == "FOREIGN KEY" { tempFKCols[r.ConstraintName] = make([]struct{ Local, Foreign string; Seq int64 }, 0) }
			}
			currentLen := len(consMap[r.ConstraintName].Columns); neededLen := 1; if r.OrdinalPosition.Valid { neededLen = int(r.OrdinalPosition.Int64) }
			if neededLen > currentLen { consMap[r.ConstraintName].Columns = append(consMap[r.ConstraintName].Columns, make([]string, neededLen-currentLen)...) }
			if neededLen > 0 { consMap[r.ConstraintName].Columns[neededLen-1] = r.ColumnName } else { consMap[r.ConstraintName].Columns = append(consMap[r.ConstraintName].Columns, r.ColumnName) }
			if r.ConstraintType == "FOREIGN KEY" && r.ForeignColumnName.Valid { seq := int64(1); if r.OrdinalPosition.Valid { seq = r.OrdinalPosition.Int64 }; tempFKCols[r.ConstraintName] = append(tempFKCols[r.ConstraintName], struct{ Local, Foreign string; Seq int64 }{r.ColumnName, r.ForeignColumnName.String, seq}) }
		}
		for name, fkCons := range consMap {
			if fkCons.Type == "FOREIGN KEY" {
				fkCols := tempFKCols[name]; sort.Slice(fkCols, func(i, j int) bool { return fkCols[i].Seq < fkCols[j].Seq }); fkCons.ForeignColumns = make([]string, len(fkCols)); fkCons.Columns = make([]string, len(fkCols))
				for i, c := range fkCols { fkCons.Columns[i] = c.Local; fkCons.ForeignColumns[i] = c.Foreign }
			} else { sort.Strings(fkCons.Columns) }
			cleanedCols := make([]string, 0, len(fkCons.Columns)); for _, c := range fkCons.Columns { if c != "" { cleanedCols = append(cleanedCols, c) } }; fkCons.Columns = cleanedCols
		}

	case "sqlite":
		// Implementasi SQLite dari respons sebelumnya
		var fkList []struct { ID int; Seq int; Table string; From string; To string; OnUpdate string; OnDelete string; Match string; }
		if err := db.WithContext(ctx).Raw("PRAGMA foreign_key_list(?)", table).Scan(&fkList).Error; err != nil {
			if !strings.Contains(err.Error(), "no such module") && !strings.Contains(err.Error(), "no such table") { log.Warn("sqlite pragma foreign_key_list failed", zap.Error(err)) }
		} else {
			fkGroup := make(map[int]*ConstraintInfo)
			for _, fk := range fkList {
				if _, ok := fkGroup[fk.ID]; !ok { fkName := fmt.Sprintf("fk_%s_foreign_%d", table, fk.ID); fkGroup[fk.ID] = &ConstraintInfo{ Name: fkName, Type: "FOREIGN KEY", Columns: make([]string, 0), ForeignTable: fk.Table, ForeignColumns: make([]string, 0), OnUpdate: fk.OnUpdate, OnDelete: fk.OnDelete } }
				fkGroup[fk.ID].Columns = append(fkGroup[fk.ID].Columns, fk.From); fkGroup[fk.ID].ForeignColumns = append(fkGroup[fk.ID].ForeignColumns, fk.To)
			}
			for _, fkCons := range fkGroup { consMap[fkCons.Name] = fkCons }
		}
		indexes, _ := s.getIndexes(ctx, db, dialect, table) // Use receiver s.
		for _, idx := range indexes {
			if idx.IsUnique && !idx.IsPrimary { if _, ok := consMap[idx.Name]; !ok { consMap[idx.Name] = &ConstraintInfo{ Name: idx.Name, Type: "UNIQUE", Columns: idx.Columns, } } }
		}
		log.Warn("CHECK constraint detection for SQLite not implemented.")

	default:
		log.Warn("Constraint fetching not implemented for dialect")
	}


	for _, cons := range consMap {
		sort.Strings(cons.Columns); if cons.Type == "FOREIGN KEY" { sort.Strings(cons.ForeignColumns) }; constraints = append(constraints, *cons)
	}
	sort.Slice(constraints, func(i, j int) bool { return constraints[i].Name < constraints[j].Name })
	return constraints, nil
}

// --- DDL Generation ---

// generateCreateTableDDL generates the CREATE TABLE statement.
func (s *SchemaSyncer) generateCreateTableDDL(table string, columns []ColumnInfo) (string, []string, error) {
	var builder strings.Builder
	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)
	builder.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", quotedTable))
	primaryKeys := make([]string, 0)
	columnDefs := make([]string, len(columns))
	for i, col := range columns {
		columnDef, err := s.mapColumnDefinition(col) // Use receiver s.
		if err != nil { return "", nil, fmt.Errorf("map col def %s: %w", col.Name, err) }
		columnDefs[i] = "  " + columnDef
		if col.IsPrimary { primaryKeys = append(primaryKeys, utils.QuoteIdentifier(col.Name, s.dstDialect)) }
	}
	builder.WriteString(strings.Join(columnDefs, ",\n"))
	if len(primaryKeys) > 0 {
		sort.Strings(primaryKeys)
		builder.WriteString(",\n  PRIMARY KEY (")
		builder.WriteString(strings.Join(primaryKeys, ", "))
		builder.WriteString(")")
	}
	builder.WriteString("\n);")
	return builder.String(), primaryKeys, nil // Return sorted quoted PK names
}

// mapColumnDefinition generates the column definition part of CREATE TABLE.
func (s *SchemaSyncer) mapColumnDefinition(col ColumnInfo) (string, error) {
	var definition strings.Builder
	definition.WriteString(utils.QuoteIdentifier(col.Name, s.dstDialect))
	definition.WriteString(" ")
	if col.MappedType == "" { return "", fmt.Errorf("mapped type missing: %s", col.Name) }
	definition.WriteString(col.MappedType)
	if col.AutoIncrement && col.IsPrimary {
		if autoIncSyntax := s.getAutoIncrementSyntax(col.MappedType); autoIncSyntax != "" { // Use receiver s.
			definition.WriteString(" ")
			definition.WriteString(autoIncSyntax)
		}
	}
	if col.DefaultValue.Valid && !col.AutoIncrement && !col.IsGenerated {
		defaultValue := col.DefaultValue.String
		if !s.isDefaultNullOrFunction(defaultValue) { // Use receiver s.
			definition.WriteString(" DEFAULT ")
			definition.WriteString(s.formatDefaultValue(defaultValue, col.MappedType)) // Use receiver s.
		}
	}
	if !col.IsNullable { definition.WriteString(" NOT NULL") }
	// TODO: Add Collation, Comment?
	return definition.String(), nil
}


// --- Sisa Helper Functions (Type Mapping, DDL Generation untuk Alter/Add/Drop Index/Constraint) ---

// generateAlterDDLs (Implementasi dari respons sebelumnya)
func (s *SchemaSyncer) generateAlterDDLs(table string,
	srcCols []ColumnInfo, srcIdxs []IndexInfo, srcCons []ConstraintInfo,
	dstCols []ColumnInfo, dstIdxs []IndexInfo, dstCons []ConstraintInfo) (
	alterColumnDDLs []string, indexDDLs []string, constraintDDLs []string, err error) {

	log := s.logger.With(zap.String("table", table), zap.String("dialect", s.dstDialect))
	alterColumnDDLs = make([]string, 0)
	indexDDLs = make([]string, 0)      // Holds DROP INDEX and CREATE INDEX
	constraintDDLs = make([]string, 0) // Holds DROP CONSTRAINT and ADD CONSTRAINT

	srcColMap := make(map[string]ColumnInfo); for _, c := range srcCols { srcColMap[c.Name] = c }
	dstColMap := make(map[string]ColumnInfo); for _, c := range dstCols { dstColMap[c.Name] = c }
	srcIdxMap := make(map[string]IndexInfo); for _, i := range srcIdxs { srcIdxMap[i.Name] = i }
	dstIdxMap := make(map[string]IndexInfo); for _, i := range dstIdxs { dstIdxMap[i.Name] = i }
	srcConsMap := make(map[string]ConstraintInfo); for _, c := range srcCons { srcConsMap[c.Name] = c }
	dstConsMap := make(map[string]ConstraintInfo); for _, c := range dstCons { dstConsMap[c.Name] = c }

	// --- Phase 1: Generate DROP statements for constraints/indexes not in source ---
	for name, dc := range dstConsMap {
		if _, exists := srcConsMap[name]; !exists && dc.Type != "PRIMARY KEY" {
			ddl, dropErr := s.generateDropConstraintDDL(table, dc) // Use receiver s.
			if dropErr != nil { log.Error("Failed generate DROP CONSTRAINT", zap.String("cons", name), zap.Error(dropErr)); continue }
			constraintDDLs = append(constraintDDLs, ddl)
		}
	}
	for name, di := range dstIdxMap {
		if _, exists := srcIdxMap[name]; !exists && !di.IsPrimary {
			ddl, dropErr := s.generateDropIndexDDL(table, di) // Use receiver s.
			if dropErr != nil { log.Error("Failed generate DROP INDEX", zap.String("idx", name), zap.Error(dropErr)); continue }
			indexDDLs = append(indexDDLs, ddl)
		}
	}

	// --- Phase 2: Generate Column ADD/DROP/MODIFY ---
	columnsToDrop := make([]ColumnInfo, 0)
	columnsToAdd := make([]ColumnInfo, 0)
	columnsToModify := make([]struct{ Src, Dst ColumnInfo }, 0)
	for name, sc := range srcColMap {
		if dc, exists := dstColMap[name]; !exists {
			columnsToAdd = append(columnsToAdd, sc)
		} else {
			if s.needsColumnModification(sc, dc, log) { // Use receiver s.
				columnsToModify = append(columnsToModify, struct{ Src, Dst ColumnInfo }{sc, dc})
			}
		}
	}
	for name, dc := range dstColMap {
		if _, exists := srcColMap[name]; !exists {
			columnsToDrop = append(columnsToDrop, dc)
		}
	}
	// Generate column DDLs (order might matter)
	for _, col := range columnsToAdd { // Add dulu
		ddl, addErr := s.generateAddColumnDDL(table, col) // Use receiver s.
		if addErr != nil { log.Error("Failed generate ADD COLUMN", zap.String("col", col.Name), zap.Error(addErr)); continue }
		alterColumnDDLs = append(alterColumnDDLs, ddl)
	}
	for _, mod := range columnsToModify { // Modify setelahnya
		modifyDDLs := s.generateModifyColumnDDLs(table, mod.Src, mod.Dst, log) // Use receiver s.
		alterColumnDDLs = append(alterColumnDDLs, modifyDDLs...)
	}
	for _, col := range columnsToDrop { // Drop terakhir (setelah constraint/index?)
		ddl, dropErr := s.generateDropColumnDDL(table, col) // Use receiver s.
		if dropErr != nil { log.Error("Failed generate DROP COLUMN", zap.String("col", col.Name), zap.Error(dropErr)); continue }
		alterColumnDDLs = append(alterColumnDDLs, ddl)
	}


	// --- Phase 3: Generate ADD statements for constraints/indexes not in destination ---
	indexesToAdd := make([]IndexInfo, 0)
	constraintsToAdd := make([]ConstraintInfo, 0)
    for name, si := range srcIdxMap {
        if _, exists := dstIdxMap[name]; !exists && !si.IsPrimary {
             indexesToAdd = append(indexesToAdd, si)
        }
        // TODO: Handle index modification? Requires deeper comparison. Drop+Add is safest.
    }
    for name, sc := range srcConsMap {
         if _, exists := dstConsMap[name]; !exists && sc.Type != "PRIMARY KEY" {
             constraintsToAdd = append(constraintsToAdd, sc)
         }
         // TODO: Handle constraint modification? Drop+Add is safest.
    }
	// Append ADD DDLs to the existing lists
	indexDDLs = append(indexDDLs, s.generateCreateIndexDDLs(table, indexesToAdd)...) // Use receiver s.
	constraintDDLs = append(constraintDDLs, s.generateAddConstraintDDLs(table, constraintsToAdd)...) // Use receiver s.


	if len(alterColumnDDLs) == 0 && len(indexDDLs) == 0 && len(constraintDDLs) == 0 {
		log.Info("No schema differences found requiring ALTER.")
	}

	return alterColumnDDLs, indexDDLs, constraintDDLs, nil
}

// needsColumnModification (Implementasi dari respons sebelumnya)
func (s *SchemaSyncer) needsColumnModification(src, dst ColumnInfo, log *zap.Logger) bool {
	if src.MappedType == "" { var mapErr error; src.MappedType, mapErr = s.mapDataType(src.Type); if mapErr != nil { log.Warn("Cannot map src type", zap.Error(mapErr)); return false } }
	if !s.areTypesCompatible(src.MappedType, dst.Type, log) { log.Debug("Type mismatch", zap.String("col", src.Name), zap.String("src", src.MappedType), zap.String("dst", dst.Type)); return true }
	if src.IsNullable != dst.IsNullable { log.Debug("Nullability mismatch", zap.String("col", src.Name), zap.Bool("src", src.IsNullable), zap.Bool("dst", dst.IsNullable)); return true }
	srcDefault := ""; if src.DefaultValue.Valid { srcDefault = src.DefaultValue.String }
	dstDefault := ""; if dst.DefaultValue.Valid { dstDefault = dst.DefaultValue.String }
	if !s.areDefaultsEquivalent(srcDefault, dstDefault, src.MappedType, log) { log.Debug("Default mismatch", zap.String("col", src.Name), zap.String("src", srcDefault), zap.String("dst", dstDefault)); return true }
	// TODO: Compare Length, Precision, Scale, Collation, Comment more accurately
	return false
}

// areTypesCompatible (Implementasi dari respons sebelumnya)
func (s *SchemaSyncer) areTypesCompatible(mappedSrcType, dstType string, log *zap.Logger) bool {
	normSrc := strings.ToLower(strings.TrimSpace(mappedSrcType)); normSrc = regexp.MustCompile(`\(\d+(,\d+)?\)`).ReplaceAllString(normSrc, "")
	normDst := strings.ToLower(strings.TrimSpace(dstType)); normDst = regexp.MustCompile(`\(\d+(,\d+)?\)`).ReplaceAllString(normDst, "")
	if normSrc == normDst { return true }
	aliases := map[string]string{"integer": "int", "character varying": "varchar", "double precision": "double", "boolean": "bool"}
	if mapped, ok := aliases[normSrc]; ok { normSrc = mapped }
	if mapped, ok := aliases[normDst]; ok { normDst = mapped }
	if normSrc == normDst { return true }
	if (strings.Contains(normSrc, "tinyint") && strings.Contains(normDst, "bool")) || (strings.Contains(normSrc, "bool") && strings.Contains(normDst, "tinyint")) { return true }
	if (strings.Contains(normSrc, "decimal") && strings.Contains(normDst, "numeric")) || (strings.Contains(normSrc, "numeric") && strings.Contains(normDst, "decimal")) { return true }
	log.Debug("Type comparison mismatch after normalization", zap.String("norm_src", normSrc), zap.String("norm_dst", normDst))
	return false
}

// areDefaultsEquivalent (Implementasi dari respons sebelumnya)
func (s *SchemaSyncer) areDefaultsEquivalent(srcDef, dstDef, mappedType string, log *zap.Logger) bool {
	srcIsNull := (srcDef == "" || strings.ToLower(srcDef) == "null"); dstIsNull := (dstDef == "" || strings.ToLower(dstDef) == "null")
	if srcIsNull && dstIsNull { return true }; if srcIsNull != dstIsNull { return false }
	// TODO: Better comparison (functions, numeric vs string, quoting)
	normSrc := strings.Trim(srcDef, "'\"`"); normDst := strings.Trim(dstDef, "'\"`")
	if normSrc == normDst { return true }
	log.Debug("Default value mismatch", zap.String("src", srcDef), zap.String("dst", dstDef))
	return false
}

// generateAddColumnDDL (Implementasi dari respons sebelumnya)
func (s *SchemaSyncer) generateAddColumnDDL(table string, col ColumnInfo) (string, error) {
	colDef, err := s.mapColumnDefinition(col); if err != nil { return "", fmt.Errorf("map def add %s: %w", col.Name, err) }
	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)
	switch s.dstDialect {
	case "mysql", "postgres", "sqlite": return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", quotedTable, colDef), nil
	default: return "", fmt.Errorf("unsupported ADD COLUMN: %s", s.dstDialect)
	}
}

// generateDropColumnDDL (Implementasi dari respons sebelumnya)
func (s *SchemaSyncer) generateDropColumnDDL(table string, col ColumnInfo) (string, error) {
	quotedTable := utils.QuoteIdentifier(table, s.dstDialect); quotedCol := utils.QuoteIdentifier(col.Name, s.dstDialect)
	switch s.dstDialect {
	case "mysql", "postgres", "sqlite": return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", quotedTable, quotedCol), nil
	default: return "", fmt.Errorf("unsupported DROP COLUMN: %s", s.dstDialect)
	}
}

// generateModifyColumnDDLs (Implementasi dari respons sebelumnya dengan receiver check)
func (s *SchemaSyncer) generateModifyColumnDDLs(table string, src, dst ColumnInfo, log *zap.Logger) []string {
	ddls := make([]string, 0)
	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)
	quotedCol := utils.QuoteIdentifier(src.Name, s.dstDialect)
	mysqlRequiresModify := false

	// 1. Type Change
	if !s.areTypesCompatible(src.MappedType, dst.Type, log) { // Use receiver s.
		log.Warn("Attempting ALTER COLUMN TYPE (may require manual intervention)", zap.String("col", src.Name), zap.String("from", dst.Type), zap.String("to", src.MappedType))
		var ddl string
		switch s.dstDialect {
		case "mysql": mysqlRequiresModify = true
		case "postgres": ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s /* USING ??? */ ;", quotedTable, quotedCol, src.MappedType); log.Warn("Postgres ALTER TYPE often needs USING clause manually added")
		case "sqlite": log.Error("ALTER COLUMN TYPE not supported in SQLite", zap.String("col", src.Name))
		default: log.Error("Unsupported dialect for ALTER COLUMN TYPE", zap.String("dialect", s.dstDialect))
		}
		if ddl != "" { ddls = append(ddls, ddl) }
	}

	// 2. Nullability Change
	if src.IsNullable != dst.IsNullable {
		var ddl string
		action := "DROP NOT NULL"; if !src.IsNullable { action = "SET NOT NULL" }
		switch s.dstDialect {
		case "mysql": mysqlRequiresModify = true
		case "postgres": ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s %s;", quotedTable, quotedCol, action)
		case "sqlite": log.Warn("Modifying NULL constraint not directly supported in SQLite", zap.String("col", src.Name))
		default: log.Error("Unsupported dialect for ALTER COLUMN NULL", zap.String("dialect", s.dstDialect))
		}
		if ddl != "" { ddls = append(ddls, ddl) }
	}

	// 3. Default Change
	srcDefault := ""; if src.DefaultValue.Valid { srcDefault = src.DefaultValue.String }
	dstDefault := ""; if dst.DefaultValue.Valid { dstDefault = dst.DefaultValue.String }
	if !s.areDefaultsEquivalent(srcDefault, dstDefault, src.MappedType, log) { // Use receiver s.
		var ddl string
		action := "DROP DEFAULT"
		if src.DefaultValue.Valid && !s.isDefaultNullOrFunction(srcDefault) { // Use receiver s.
			action = fmt.Sprintf("SET DEFAULT %s", s.formatDefaultValue(srcDefault, src.MappedType)) // Use receiver s.
		}
		switch s.dstDialect {
		case "mysql": mysqlRequiresModify = true
		case "postgres", "sqlite": ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s %s;", quotedTable, quotedCol, action)
		default: log.Error("Unsupported dialect for ALTER COLUMN DEFAULT", zap.String("dialect", s.dstDialect))
		}
		if ddl != "" { ddls = append(ddls, ddl) }
	}

	// Generate single MODIFY statement for MySQL if needed
	if mysqlRequiresModify && s.dstDialect == "mysql" {
		colDef, err := s.mapColumnDefinition(src) // Use receiver s.
		if err == nil {
			ddls = []string{fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s;", quotedTable, colDef)} // Override other potential alters
		} else {
			log.Error("Failed to map definition for MySQL MODIFY COLUMN", zap.Error(err))
			ddls = []string{} // Clear DDLs if definition fails
		}
	}
	return ddls
}

// generateCreateIndexDDLs (Implementasi dari respons sebelumnya)
func (s *SchemaSyncer) generateCreateIndexDDLs(table string, indexes []IndexInfo) []string {
    ddls := make([]string, 0, len(indexes)); quotedTable := utils.QuoteIdentifier(table, s.dstDialect)
    for _, idx := range indexes {
        if idx.IsPrimary { continue }
        quotedIndexName := utils.QuoteIdentifier(idx.Name, s.dstDialect); quotedCols := make([]string, len(idx.Columns))
        for i, col := range idx.Columns { quotedCols[i] = utils.QuoteIdentifier(col, s.dstDialect) }
        uniqueKeyword := ""; if idx.IsUnique { uniqueKeyword = "UNIQUE " }
        // Gunakan IF NOT EXISTS untuk keamanan tambahan jika memungkinkan
        ifExistsKeyword := "IF NOT EXISTS "; if s.dstDialect == "mysql" { ifExistsKeyword = "" } // MySQL tidak support IF NOT EXISTS untuk CREATE INDEX
        ddl := fmt.Sprintf("CREATE %sINDEX %s%s ON %s (%s);", uniqueKeyword, ifExistsKeyword, quotedIndexName, quotedTable, strings.Join(quotedCols, ", "))
        ddls = append(ddls, ddl)
    }
    return ddls
}

// generateAddConstraintDDLs (Implementasi dari respons sebelumnya)
func (s *SchemaSyncer) generateAddConstraintDDLs(table string, constraints []ConstraintInfo) []string {
    ddls := make([]string, 0, len(constraints)); quotedTable := utils.QuoteIdentifier(table, s.dstDialect)
    for _, c := range constraints {
        if c.Type == "PRIMARY KEY" { continue }
        quotedConstraintName := utils.QuoteIdentifier(c.Name, s.dstDialect); quotedCols := make([]string, len(c.Columns))
        for i, col := range c.Columns { quotedCols[i] = utils.QuoteIdentifier(col, s.dstDialect) }
        var ddl string
        switch c.Type {
        case "UNIQUE": ddl = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s);", quotedTable, quotedConstraintName, strings.Join(quotedCols, ", "))
        case "FOREIGN KEY":
			quotedForeignTable := utils.QuoteIdentifier(c.ForeignTable, s.dstDialect); quotedForeignCols := make([]string, len(c.ForeignColumns))
			for i, fcol := range c.ForeignColumns { quotedForeignCols[i] = utils.QuoteIdentifier(fcol, s.dstDialect) }
			fkActions := ""; if c.OnDelete != "" && c.OnDelete != "NO ACTION" { fkActions += " ON DELETE " + c.OnDelete }; if c.OnUpdate != "" && c.OnUpdate != "NO ACTION" { fkActions += " ON UPDATE " + c.OnUpdate }
			ddl = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)%s;", quotedTable, quotedConstraintName, strings.Join(quotedCols, ", "), quotedForeignTable, strings.Join(quotedForeignCols,", "), fkActions)
		case "CHECK":
			if c.Definition != "" { ddl = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s);", quotedTable, quotedConstraintName, c.Definition) } else { s.logger.Error("Cannot ADD CHECK without definition", zap.String("cons", c.Name)) }
        default: s.logger.Warn("Unsupported constraint type for ADD DDL", zap.String("type", c.Type), zap.String("cons", c.Name))
        }
		if ddl != "" { ddls = append(ddls, ddl) }
    }
    return ddls
}

// generateDropIndexDDL (Implementasi dari respons sebelumnya)
func (s *SchemaSyncer) generateDropIndexDDL(table string, idx IndexInfo) (string, error) {
	quotedIndexName := utils.QuoteIdentifier(idx.Name, s.dstDialect)
	switch s.dstDialect {
	case "mysql": rawTableName := utils.QuoteIdentifier(table, s.dstDialect); return fmt.Sprintf("DROP INDEX %s ON %s;", quotedIndexName, rawTableName), nil // MySQL perlu ON table
	case "postgres", "sqlite": return fmt.Sprintf("DROP INDEX IF EXISTS %s;", quotedIndexName), nil // IF EXISTS
	default: return "", fmt.Errorf("unsupported DROP INDEX: %s", s.dstDialect)
	}
}

// generateDropConstraintDDL (Implementasi dari respons sebelumnya)
func (s *SchemaSyncer) generateDropConstraintDDL(table string, constraint ConstraintInfo) (string, error) {
	quotedTable := utils.QuoteIdentifier(table, s.dstDialect); quotedConstraintName := utils.QuoteIdentifier(constraint.Name, s.dstDialect)
	switch s.dstDialect {
	case "mysql":
		if constraint.Type == "FOREIGN KEY" { return fmt.Sprintf("ALTER TABLE %s DROP FOREIGN KEY %s;", quotedTable, quotedConstraintName), nil }
		if constraint.Type == "CHECK" { return fmt.Sprintf("ALTER TABLE %s DROP CHECK %s;", quotedTable, quotedConstraintName), nil } // Syntax MySQL 8.0.16+
		if constraint.Type == "UNIQUE" { return fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s;", quotedTable, quotedConstraintName), nil } // Coba drop constraint
		return "", fmt.Errorf("unsupported drop constraint type MySQL: %s (%s)", constraint.Type, constraint.Name)
	case "postgres", "sqlite": return fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s;", quotedTable, quotedConstraintName), nil // IF EXISTS
	default: return "", fmt.Errorf("unsupported DROP CONSTRAINT: %s", s.dstDialect)
	}
}

// mapDataType (Implementasi dari respons sebelumnya)
func (s *SchemaSyncer) mapDataType(srcType string) (string, error) { normSrcType := strings.ToLower(strings.TrimSpace(srcType)); normSrcType = regexp.MustCompile(`\(\d+(,\d+)?\)`).ReplaceAllString(normSrcType, ""); typeMap := s.getTypeMapping(); if mapped, ok := typeMap[normSrcType]; ok { return s.applyTypeModifiers(srcType, mapped), nil }; baseType := regexp.MustCompile(`^([\w\s]+)`).FindString(normSrcType); /* Handle 'double precision' etc */ if baseType != "" && baseType != normSrcType { if mapped, ok := typeMap[baseType]; ok { return s.applyTypeModifiers(srcType, mapped), nil } }; if s.srcDialect == "mysql" && s.dstDialect == "postgres" { if normSrcType == "tinyint" { if strings.Contains(strings.ToLower(srcType), "tinyint(1)") { return "BOOLEAN", nil } else { return "SMALLINT", nil } } }; if s.srcDialect == "postgres" && s.dstDialect == "mysql" { if normSrcType == "boolean" { return "TINYINT(1)", nil }; if normSrcType == "uuid" { return "CHAR(36)", nil } }; s.logger.Warn("Using fallback type mapping", zap.String("src_type", srcType), zap.String("fallback", s.getGenericFallbackType())); return s.getGenericFallbackType(), nil }

// getTypeMapping (Implementasi dari respons sebelumnya)
func (s *SchemaSyncer) getTypeMapping() map[string]string { key := s.srcDialect + "-to-" + s.dstDialect; switch key { case "mysql-to-postgres": return map[string]string{ "tinyint": "SMALLINT", "smallint": "SMALLINT", "mediumint": "INTEGER", "int": "INTEGER", "integer": "INTEGER", "bigint": "BIGINT", "float": "REAL", "double": "DOUBLE PRECISION", "decimal": "NUMERIC", "numeric": "NUMERIC", "char": "CHAR", "varchar": "VARCHAR", "tinytext": "TEXT", "text": "TEXT", "mediumtext": "TEXT", "longtext": "TEXT", "binary": "BYTEA", "varbinary": "BYTEA", "tinyblob": "BYTEA", "blob": "BYTEA", "mediumblob": "BYTEA", "longblob": "BYTEA", "json": "JSONB", "enum": "VARCHAR(255)", "set": "TEXT", "date": "DATE", "time": "TIME", "datetime": "TIMESTAMP", "timestamp": "TIMESTAMP WITH TIME ZONE", "year": "SMALLINT", }; case "postgres-to-mysql": return map[string]string{ "smallint": "SMALLINT", "integer": "INT", "bigint": "BIGINT", "real": "FLOAT", "double precision": "DOUBLE", "numeric": "DECIMAL", "decimal": "DECIMAL", "smallserial": "SMALLINT", "serial": "INT", "bigserial": "BIGINT", "character varying": "VARCHAR", "varchar": "VARCHAR", "character": "CHAR", "char": "CHAR", "text": "LONGTEXT", "bytea": "LONGBLOB", "json": "JSON", "jsonb": "JSON", "uuid": "CHAR(36)", "date": "DATE", "time": "TIME", "time with time zone": "TIME", "timestamp": "DATETIME(6)", "timestamp with time zone": "TIMESTAMP(6)", "boolean": "TINYINT(1)", "inet": "VARCHAR(43)", "cidr": "VARCHAR(43)", "macaddr": "VARCHAR(17)", "text[]": "JSON", "integer[]": "JSON", /* Add more array types */ }; case "sqlite-to-postgres": return map[string]string{ "integer": "BIGINT", "real": "DOUBLE PRECISION", "text": "TEXT", "blob": "BYTEA", "numeric": "NUMERIC", "datetime": "TIMESTAMP", "date": "DATE", "timestamp": "TIMESTAMP", "boolean": "BOOLEAN", }; case "mysql-to-mysql", "postgres-to-postgres": return map[string]string{}; default: s.logger.Warn("No type map", zap.String("dir", key)); return make(map[string]string) } }

// applyTypeModifiers (Implementasi dari respons sebelumnya)
func (s *SchemaSyncer) applyTypeModifiers(srcType, mappedType string) string { re := regexp.MustCompile(`\((.+?)\)`); matches := re.FindStringSubmatch(srcType); if len(matches) > 1 { baseMappedType := strings.Split(mappedType, "(")[0]; switch strings.ToUpper(baseMappedType) { case "VARCHAR", "CHAR", "DECIMAL", "NUMERIC", "VARBINARY", "BINARY": return fmt.Sprintf("%s(%s)", baseMappedType, matches[1]); default: return mappedType } }; return mappedType }

// getGenericFallbackType (Implementasi dari respons sebelumnya)
func (s *SchemaSyncer) getGenericFallbackType() string { switch s.dstDialect { case "mysql": return "LONGTEXT"; case "postgres": return "TEXT"; case "sqlite": return "TEXT"; default: return "TEXT" } }

// getAutoIncrementSyntax (Implementasi dari respons sebelumnya)
func (s *SchemaSyncer) getAutoIncrementSyntax(mappedDestType string) string { mappedDestTypeLower := strings.ToLower(mappedDestType); switch s.dstDialect { case "mysql": if strings.Contains(mappedDestTypeLower, "int") && !strings.Contains(mappedDestTypeLower, "auto_increment") { return "AUTO_INCREMENT" }; case "postgres": if strings.Contains(mappedDestTypeLower, "int") { return "GENERATED BY DEFAULT AS IDENTITY" }; case "sqlite": if mappedDestTypeLower == "integer" { return "" } }; return "" }

// formatDefaultValue (Implementasi dari respons sebelumnya)
func (s *SchemaSyncer) formatDefaultValue(value, mappedDataType string) string { normType := strings.ToLower(mappedDataType); normValue := strings.ToLower(value); if strings.Contains(normValue, "nextval(") || normValue == "current_timestamp" || normValue == "now()" || normValue == "current_date" || normValue == "current_time" || strings.HasPrefix(normValue, "gen_random_uuid()") || normValue == "uuid()" { return value }; if strings.Contains(normType, "bool") || strings.Contains(normType, "tinyint(1)") { if normValue == "true" || normValue == "1" || normValue == "'t'" || normValue == "'y'" || normValue == "'1'" { return "'1'" }; if normValue == "false" || normValue == "0" || normValue == "'f'" || normValue == "'n'" || normValue == "'0'" { return "'0'" } }; if strings.Contains(normType, "int") || strings.Contains(normType, "serial") || strings.Contains(normType, "numeric") || strings.Contains(normType, "decimal") || strings.Contains(normType, "float") || strings.Contains(normType, "double") || strings.Contains(normType, "real") { return value }; return fmt.Sprintf("'%s'", strings.ReplaceAll(value, "'", "''")) }

// isDefaultNullOrFunction (Implementasi dari respons sebelumnya)
func (s *SchemaSyncer) isDefaultNullOrFunction(defaultValue string) bool { normValue := strings.ToLower(strings.TrimSpace(defaultValue)); if normValue == "null" || normValue == "" { return true }; return strings.Contains(normValue, "nextval(") || strings.Contains(normValue, "current_") || strings.Contains(normValue, "now()") || strings.Contains(normValue, "uuid(") }

// --- DDL Execution Helpers ---

// sortConstraintsForDrop ensures FKs are attempted to be dropped first.
func sortConstraintsForDrop(ddls []string) []string {
	sorted := make([]string, len(ddls)); copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		isFkI := strings.Contains(strings.ToUpper(sorted[i]), "DROP FOREIGN KEY") || (strings.Contains(strings.ToUpper(sorted[i]), "DROP CONSTRAINT") && strings.Contains(strings.ToUpper(sorted[i]), "FK")) // Basic check
		isFkJ := strings.Contains(strings.ToUpper(sorted[j]), "DROP FOREIGN KEY") || (strings.Contains(strings.ToUpper(sorted[j]), "DROP CONSTRAINT") && strings.Contains(strings.ToUpper(sorted[j]), "FK"))
		if isFkI && !isFkJ { return true }; if !isFkI && isFkJ { return false }; return false
	})
	return sorted
}

// sortConstraintsForAdd ensures FKs are attempted to be added last.
func sortConstraintsForAdd(ddls []string) []string {
	sorted := make([]string, len(ddls)); copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		isFkI := strings.Contains(strings.ToUpper(sorted[i]), "ADD CONSTRAINT") && strings.Contains(strings.ToUpper(sorted[i]), "FOREIGN KEY")
		isFkJ := strings.Contains(strings.ToUpper(sorted[j]), "ADD CONSTRAINT") && strings.Contains(strings.ToUpper(sorted[j]), "FOREIGN KEY")
		if !isFkI && isFkJ { return true }; if isFkI && !isFkJ { return false }; return false
	})
	return sorted
}

// sortAlterColumns tries to order ALTER COLUMN statements (DROP first).
func sortAlterColumns(ddls []string) []string {
	sorted := make([]string, len(ddls)); copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		isDropI := strings.Contains(strings.ToUpper(sorted[i]), "DROP COLUMN"); isDropJ := strings.Contains(strings.ToUpper(sorted[j]), "DROP COLUMN")
		if isDropI && !isDropJ { return true }; if !isDropI && isDropJ { return false }
		isAddI := strings.Contains(strings.ToUpper(sorted[i]), "ADD COLUMN"); isAddJ := strings.Contains(strings.ToUpper(sorted[j]), "ADD COLUMN")
        if !isAddI && isAddJ { return true }; if isAddI && !isAddJ { return false}
		return false
	})
	return sorted
}