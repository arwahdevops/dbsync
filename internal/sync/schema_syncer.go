package sync

import (
	"context"
	"database/sql"
	"fmt"
	"regexp" // Diperlukan untuk helper type mapping
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

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
	Name            string; Type            string; MappedType      string; IsNullable      bool; IsPrimary       bool; IsGenerated     bool; DefaultValue    sql.NullString; AutoIncrement   bool; OrdinalPosition int; Length    sql.NullInt64; Precision       sql.NullInt64; Scale     sql.NullInt64; Collation       sql.NullString; Comment   sql.NullString
}
type IndexInfo struct {
	Name string; Columns []string; IsUnique bool; IsPrimary bool; RawDef string
}
type ConstraintInfo struct {
	Name string; Type string; Columns []string; Definition string; ForeignTable string; ForeignColumns []string; OnDelete string; OnUpdate string
}

// Ensure SchemaSyncer implements the interface
var _ SchemaSyncerInterface = (*SchemaSyncer)(nil)

// --- Constructor ---
func NewSchemaSyncer(srcDB, dstDB *gorm.DB, srcDialect, dstDialect string, logger *zap.Logger) *SchemaSyncer {
	return &SchemaSyncer{
		srcDB:      srcDB, dstDB:      dstDB,
		srcDialect: strings.ToLower(srcDialect), dstDialect: strings.ToLower(dstDialect),
		logger:     logger.Named("schema-syncer"),
	}
}

// --- Interface Implementation (Dispatcher Methods) ---

func (s *SchemaSyncer) SyncTableSchema(ctx context.Context, table string, strategy config.SchemaSyncStrategy) (*SchemaExecutionResult, error) {
	start := time.Now()
	log := s.logger.With(zap.String("table", table), zap.String("strategy", string(strategy)))
	log.Info("Starting schema analysis")
	result := &SchemaExecutionResult{}
	if s.isSystemTable(table, s.srcDialect) { log.Debug("Skipping system table"); return result, nil }

	// Get Source Schema
	srcColumns, err := s.getColumns(ctx, s.srcDB, s.srcDialect, table); if err != nil { return nil, fmt.Errorf("get source cols: %w", err) }
	if len(srcColumns) == 0 { log.Warn("Source table empty/not found"); return result, nil }
	result.PrimaryKeys = getPKColumnNames(srcColumns)
	srcIndexes, err := s.getIndexes(ctx, s.srcDB, s.srcDialect, table); if err != nil { return nil, fmt.Errorf("get source indexes: %w", err) }
	srcConstraints, err := s.getConstraints(ctx, s.srcDB, s.srcDialect, table); if err != nil { return nil, fmt.Errorf("get source constraints: %w", err) }

	// Generate DDL
	switch strategy {
	case config.SchemaSyncDropCreate:
		log.Warn("Using 'drop_create' strategy - DESTRUCTIVE!")
		tableDDL, _, err := s.generateCreateTableDDL(table, srcColumns); if err != nil { return nil, err }
		result.TableDDL = tableDDL
		result.IndexDDLs = s.generateCreateIndexDDLs(table, srcIndexes)
		result.ConstraintDDLs = s.generateAddConstraintDDLs(table, srcConstraints)
	case config.SchemaSyncAlter:
		log.Info("Attempting 'alter' strategy")
		dstColumns, dstIndexes, dstConstraints, dstExists, err := s.getDestinationSchema(ctx, table)
		if err != nil { return nil, err }
		if !dstExists {
			log.Info("Destination table not found, performing CREATE")
			tableDDL, _, err := s.generateCreateTableDDL(table, srcColumns); if err != nil { return nil, err }
			result.TableDDL = tableDDL
			result.IndexDDLs = s.generateCreateIndexDDLs(table, srcIndexes)
			result.ConstraintDDLs = s.generateAddConstraintDDLs(table, srcConstraints)
		} else {
			log.Debug("Destination table exists, generating ALTERs")
			alterDDLs, idxDDLs, consDDLs, err := s.generateAlterDDLs(table, srcColumns, srcIndexes, srcConstraints, dstColumns, dstIndexes, dstConstraints)
			if err != nil { return nil, err }
			result.TableDDL = strings.Join(alterDDLs, ";\n")
			result.IndexDDLs = idxDDLs
			result.ConstraintDDLs = consDDLs
		}
	case config.SchemaSyncNone:
		log.Info("Schema sync strategy is 'none'.")
	default:
		return nil, fmt.Errorf("unknown strategy: %s", strategy)
	}

	log.Info("Schema analysis completed", zap.Duration("duration", time.Since(start)))
	return result, nil
}

func (s *SchemaSyncer) getDestinationSchema(ctx context.Context, table string) (columns []ColumnInfo, indexes []IndexInfo, constraints []ConstraintInfo, exists bool, err error) {
	migrator := s.dstDB.Migrator(); if !migrator.HasTable(table) { exists=false; return }
	columns, errC := s.getColumns(ctx, s.dstDB, s.dstDialect, table); if errC != nil { err = errC; exists=true; return } // Pass error up
	indexes, errI := s.getIndexes(ctx, s.dstDB, s.dstDialect, table); if errI != nil { err = errI; exists=true; return }
	constraints, errCo := s.getConstraints(ctx, s.dstDB, s.dstDialect, table); if errCo != nil { err = errCo; exists=true; return }
	exists = true; return
}


func (s *SchemaSyncer) ExecuteDDLs(ctx context.Context, table string, ddls *SchemaExecutionResult) error {
	log := s.logger.With(zap.String("table", table))
	if ddls == nil || (ddls.TableDDL == "" && len(ddls.IndexDDLs) == 0 && len(ddls.ConstraintDDLs) == 0) { log.Debug("No DDLs to execute"); return nil }

	// Pisahkan DDLs
	createTableDDL := ""; alterColumnDDLs := []string{}
	addIndexDDLs := []string{}; dropIndexDDLs := []string{}
	addConstraintDDLs := []string{}; dropConstraintDDLs := []string{}
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(ddls.TableDDL)), "CREATE TABLE") { createTableDDL = ddls.TableDDL } else if ddls.TableDDL != "" { for _, ddl := range strings.Split(ddls.TableDDL, ";\n") { trimmed := strings.TrimSpace(ddl); if trimmed != "" { alterColumnDDLs = append(alterColumnDDLs, trimmed) } } }
	for _, ddl := range ddls.IndexDDLs { if strings.Contains(strings.ToUpper(ddl), "DROP INDEX") { dropIndexDDLs = append(dropIndexDDLs, ddl) } else { addIndexDDLs = append(addIndexDDLs, ddl) } }
	for _, ddl := range ddls.ConstraintDDLs { if strings.Contains(strings.ToUpper(ddl), "DROP CONSTRAINT") { dropConstraintDDLs = append(dropConstraintDDLs, ddl) } else { addConstraintDDLs = append(addConstraintDDLs, ddl) } }

	// Eksekusi dalam transaksi
	return s.dstDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var revertFK func() error
		isDropCreate := createTableDDL != ""
		if isDropCreate && s.dstDialect == "mysql" {
			if err := tx.Exec("SET FOREIGN_KEY_CHECKS=0;").Error; err != nil { return fmt.Errorf("disable fks: %w", err) }
			revertFK = func() error { return tx.Exec("SET FOREIGN_KEY_CHECKS=1;").Error }
			defer func() { if rErr := revertFK(); rErr != nil { log.Error("Failed re-enable fks", zap.Error(rErr)) } }()
		}
		// Fase 1: Drops
		if !isDropCreate && (len(dropConstraintDDLs) > 0 || len(dropIndexDDLs) > 0) {
			log.Info("Executing Phase 1: Drop Constraints/Indexes")
			sortedDropConstraints := sortConstraintsForDrop(dropConstraintDDLs)
			for _, ddl := range sortedDropConstraints { log.Debug("Drop Cons", zap.String("ddl", ddl)); if err := tx.Exec(ddl).Error; err != nil { log.Error("Failed drop cons", zap.Error(err), zap.String("ddl", ddl)) }}
			for _, ddl := range dropIndexDDLs { log.Debug("Drop Idx", zap.String("ddl", ddl)); if err := tx.Exec(ddl).Error; err != nil { log.Error("Failed drop idx", zap.Error(err), zap.String("ddl", ddl)) }}
		}
		// Fase 2: Table Structure
		if isDropCreate {
			log.Warn("Executing DROP TABLE IF EXISTS")
			dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", utils.QuoteIdentifier(table, s.dstDialect)); if s.dstDialect == "postgres" { dropSQL += " CASCADE" }
			if err := tx.Exec(dropSQL).Error; err != nil { return fmt.Errorf("drop table: %w", err) }
			log.Info("Executing Phase 2: Create Table"); log.Debug("Create DDL", zap.String("ddl", createTableDDL))
			if err := tx.Exec(createTableDDL).Error; err != nil { return fmt.Errorf("create table: %w", err) }
		} else if len(alterColumnDDLs) > 0 {
			log.Info("Executing Phase 2: Alter Columns")
			sortedAlters := sortAlterColumns(alterColumnDDLs)
			for _, ddl := range sortedAlters { log.Debug("Alter Col", zap.String("ddl", ddl)); if err := tx.Exec(ddl).Error; err != nil { log.Error("Failed alter col", zap.Error(err), zap.String("ddl", ddl)) } }
		} else { log.Info("Phase 2: No table structure changes.") }
		// Fase 3: Add Indexes
		if len(addIndexDDLs) > 0 {
			log.Info("Executing Phase 3: Create Indexes")
			for _, ddl := range addIndexDDLs { log.Debug("Create Idx", zap.String("ddl", ddl)); if err := tx.Exec(ddl).Error; err != nil { log.Error("Failed create idx", zap.Error(err), zap.String("ddl", ddl)) } }
		}
		// Fase 4: Add Constraints
		if len(addConstraintDDLs) > 0 {
			log.Info("Executing Phase 4: Add Constraints")
			sortedAddConstraints := sortConstraintsForAdd(addConstraintDDLs)
			for _, ddl := range sortedAddConstraints { log.Debug("Add Cons", zap.String("ddl", ddl)); if err := tx.Exec(ddl).Error; err != nil { log.Error("Failed add cons", zap.Error(err), zap.String("ddl", ddl)) } }
		}
		log.Info("DDL execution transaction finished")
		return nil
	})
}


func (s *SchemaSyncer) GetPrimaryKeys(ctx context.Context, table string) ([]string, error) {
	columns, err := s.getColumns(ctx, s.srcDB, s.srcDialect, table)
	if err != nil { return nil, fmt.Errorf("failed get columns for PKs table %s: %w", table, err) }
	pks := getPKColumnNames(columns); if len(pks) == 0 { s.logger.Warn("No primary key detected", zap.String("table", table)) }; return pks, nil
}

// --- Dispatcher Methods ---
func (s *SchemaSyncer) getColumns(ctx context.Context, db *gorm.DB, dialect string, table string) ([]ColumnInfo, error) {
	switch dialect {
	case "mysql": return s.getMySQLColumns(ctx, db, table)
	case "postgres": return s.getPostgresColumns(ctx, db, table)
	case "sqlite": return s.getSQLiteColumns(ctx, db, table)
	default: return nil, fmt.Errorf("getColumns: unsupported dialect: %s", dialect)
	}
}
func (s *SchemaSyncer) getIndexes(ctx context.Context, db *gorm.DB, dialect, table string) ([]IndexInfo, error) {
	switch dialect {
	case "mysql": return s.getMySQLIndexes(ctx, db, table)
	case "postgres": return s.getPostgresIndexes(ctx, db, table)
	case "sqlite": return s.getSQLiteIndexes(ctx, db, table)
	default: s.logger.Warn("Index fetching not impl.", zap.String("dialect", dialect)); return []IndexInfo{}, nil
	}
}
func (s *SchemaSyncer) getConstraints(ctx context.Context, db *gorm.DB, dialect, table string) ([]ConstraintInfo, error) {
	switch dialect {
	case "mysql": return s.getMySQLConstraints(ctx, db, table)
	case "postgres": return s.getPostgresConstraints(ctx, db, table)
	case "sqlite": return s.getSQLiteConstraints(ctx, db, table)
	default: s.logger.Warn("Constraint fetching not impl.", zap.String("dialect", dialect)); return []ConstraintInfo{}, nil
	}
}

// --- General Helper Functions (Common across dialects) ---
func getPKColumnNames(columns []ColumnInfo) []string { var pks []string; for _, c:=range columns{if c.IsPrimary{pks=append(pks,c.Name)}}; sort.Strings(pks); return pks }
func (s *SchemaSyncer) isSystemTable(table, dialect string) bool { lT:=strings.ToLower(table); lD:=strings.ToLower(dialect); switch lD{case "mysql": return lT=="information_schema"||lT=="performance_schema"||lT=="mysql"||lT=="sys"; case "postgres": return strings.HasPrefix(lT, "pg_")||lT=="information_schema"; case "sqlite": return strings.HasPrefix(lT,"sqlite_"); default: return false}}

// --- Type Mapping Helpers ---
func (s *SchemaSyncer) mapDataType(srcType string) (string, error) { normSrcType := strings.ToLower(strings.TrimSpace(srcType)); normSrcType = regexp.MustCompile(`\(\d+(,\d+)?\)`).ReplaceAllString(normSrcType, ""); typeMap := s.getTypeMapping(); if mapped, ok := typeMap[normSrcType]; ok { return s.applyTypeModifiers(srcType, mapped), nil }; baseType := regexp.MustCompile(`^([\w\s]+)`).FindString(normSrcType); if baseType != "" && baseType != normSrcType { if mapped, ok := typeMap[baseType]; ok { return s.applyTypeModifiers(srcType, mapped), nil } }; if s.srcDialect == "mysql" && s.dstDialect == "postgres" { if normSrcType == "tinyint" { if strings.Contains(strings.ToLower(srcType), "tinyint(1)") { return "BOOLEAN", nil } else { return "SMALLINT", nil } } }; if s.srcDialect == "postgres" && s.dstDialect == "mysql" { if normSrcType == "boolean" { return "TINYINT(1)", nil }; if normSrcType == "uuid" { return "CHAR(36)", nil } }; s.logger.Warn("Using fallback type mapping", zap.String("src_type", srcType), zap.String("fallback", s.getGenericFallbackType())); return s.getGenericFallbackType(), nil }
func (s *SchemaSyncer) getTypeMapping() map[string]string { key := s.srcDialect + "-to-" + s.dstDialect; switch key { case "mysql-to-postgres": return map[string]string{ "tinyint": "SMALLINT", "smallint": "SMALLINT", "mediumint": "INTEGER", "int": "INTEGER", "integer": "INTEGER", "bigint": "BIGINT", "float": "REAL", "double": "DOUBLE PRECISION", "decimal": "NUMERIC", "numeric": "NUMERIC", "char": "CHAR", "varchar": "VARCHAR", "tinytext": "TEXT", "text": "TEXT", "mediumtext": "TEXT", "longtext": "TEXT", "binary": "BYTEA", "varbinary": "BYTEA", "tinyblob": "BYTEA", "blob": "BYTEA", "mediumblob": "BYTEA", "longblob": "BYTEA", "json": "JSONB", "enum": "VARCHAR(255)", "set": "TEXT", "date": "DATE", "time": "TIME", "datetime": "TIMESTAMP", "timestamp": "TIMESTAMP WITH TIME ZONE", "year": "SMALLINT", }; case "postgres-to-mysql": return map[string]string{ "smallint": "SMALLINT", "integer": "INT", "bigint": "BIGINT", "real": "FLOAT", "double precision": "DOUBLE", "numeric": "DECIMAL", "decimal": "DECIMAL", "smallserial": "SMALLINT", "serial": "INT", "bigserial": "BIGINT", "character varying": "VARCHAR", "varchar": "VARCHAR", "character": "CHAR", "char": "CHAR", "text": "LONGTEXT", "bytea": "LONGBLOB", "json": "JSON", "jsonb": "JSON", "uuid": "CHAR(36)", "date": "DATE", "time": "TIME", "time with time zone": "TIME", "timestamp": "DATETIME(6)", "timestamp with time zone": "TIMESTAMP(6)", "boolean": "TINYINT(1)", "inet": "VARCHAR(43)", "cidr": "VARCHAR(43)", "macaddr": "VARCHAR(17)", "text[]": "JSON", "integer[]": "JSON", }; case "sqlite-to-postgres": return map[string]string{ "integer": "BIGINT", "real": "DOUBLE PRECISION", "text": "TEXT", "blob": "BYTEA", "numeric": "NUMERIC", "datetime": "TIMESTAMP", "date": "DATE", "timestamp": "TIMESTAMP", "boolean": "BOOLEAN", }; case "mysql-to-mysql", "postgres-to-postgres": return map[string]string{}; default: s.logger.Warn("No type map", zap.String("dir", key)); return make(map[string]string) } }
func (s *SchemaSyncer) applyTypeModifiers(srcType, mappedType string) string { re := regexp.MustCompile(`\((.+?)\)`); matches := re.FindStringSubmatch(srcType); if len(matches) > 1 { baseMappedType := strings.Split(mappedType, "(")[0]; switch strings.ToUpper(baseMappedType) { case "VARCHAR", "CHAR", "DECIMAL", "NUMERIC", "VARBINARY", "BINARY": return fmt.Sprintf("%s(%s)", baseMappedType, matches[1]); default: return mappedType } }; return mappedType }
func (s *SchemaSyncer) getGenericFallbackType() string { switch s.dstDialect { case "mysql": return "LONGTEXT"; case "postgres": return "TEXT"; case "sqlite": return "TEXT"; default: return "TEXT" } }

// --- DDL Execution Helpers (Sorting) ---
func sortConstraintsForDrop(ddls []string) []string { sorted:=make([]string,len(ddls)); copy(sorted,ddls); sort.SliceStable(sorted,func(i,j int)bool{isFkI:=strings.Contains(strings.ToUpper(sorted[i]),"DROP FOREIGN KEY")||(strings.Contains(strings.ToUpper(sorted[i]),"DROP CONSTRAINT")&&strings.Contains(strings.ToUpper(sorted[i]),"FK")); isFkJ:=strings.Contains(strings.ToUpper(sorted[j]),"DROP FOREIGN KEY")||(strings.Contains(strings.ToUpper(sorted[j]),"DROP CONSTRAINT")&&strings.Contains(strings.ToUpper(sorted[j]),"FK")); if isFkI&&!isFkJ{return true}; if !isFkI&&isFkJ{return false}; return false}); return sorted }
func sortConstraintsForAdd(ddls []string) []string { sorted:=make([]string,len(ddls)); copy(sorted,ddls); sort.SliceStable(sorted,func(i,j int)bool{isFkI:=strings.Contains(strings.ToUpper(sorted[i]),"ADD CONSTRAINT")&&strings.Contains(strings.ToUpper(sorted[i]),"FOREIGN KEY"); isFkJ:=strings.Contains(strings.ToUpper(sorted[j]),"ADD CONSTRAINT")&&strings.Contains(strings.ToUpper(sorted[j]),"FOREIGN KEY"); if !isFkI&&isFkJ{return true}; if isFkI&&!isFkJ{return false}; return false}); return sorted }
func sortAlterColumns(ddls []string) []string { sorted:=make([]string,len(ddls)); copy(sorted,ddls); sort.SliceStable(sorted,func(i,j int)bool{isDropI:=strings.Contains(strings.ToUpper(sorted[i]),"DROP COLUMN"); isDropJ:=strings.Contains(strings.ToUpper(sorted[j]),"DROP COLUMN"); if isDropI&&!isDropJ{return true}; if !isDropI&&isDropJ{return false}; isAddI:=strings.Contains(strings.ToUpper(sorted[i]),"ADD COLUMN"); isAddJ:=strings.Contains(strings.ToUpper(sorted[j]),"ADD COLUMN"); if !isAddI&&isAddJ{return true}; if isAddI&&!isAddJ{return false}; return false}); return sorted }