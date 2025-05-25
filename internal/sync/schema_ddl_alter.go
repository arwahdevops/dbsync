// internal/sync/schema_ddl_alter.go
package sync

import (
	"fmt"
	"strings"

	"github.com/arwahdevops/dbsync/internal/utils"
	"go.uber.org/zap"
)

// generateModifyColumnDDLs (tetap sama, hanya memanggil dispatcher)
func (s *SchemaSyncer) generateModifyColumnDDLs(table string, src, dst ColumnInfo, log *zap.Logger) []string {
	diffs := s.getColumnModifications(src, dst, log)
	if len(diffs) == 0 {
		return []string{}
	}
	log.Info("Generating ALTER COLUMN DDL(s) due to detected differences.",
		zap.String("dst_dialect", s.dstDialect),
		zap.Strings("modifications_detected", diffs),
	)
	switch s.dstDialect {
	case "mysql":
		return s.generateMySQLModifyColumnDDLs(table, src, dst, diffs, log)
	case "postgres":
		return s.generatePostgresModifyColumnDDLs(table, src, dst, diffs, log)
	case "sqlite":
		return s.generateSQLiteModifyColumnDDLs(table, src, dst, diffs, log)
	default:
		log.Error("Unsupported destination dialect for ALTER COLUMN modifications", zap.String("dialect", s.dstDialect))
		return []string{}
	}
}

// generateAddColumnDDL (disesuaikan dengan signature mapColumnDefinition baru)
func (s *SchemaSyncer) generateAddColumnDDL(table string, col ColumnInfo) (string, error) {
	log := s.logger.With(zap.String("table", table), zap.String("column", col.Name), zap.String("action", "ADD COLUMN"))

	if col.MappedType == "" && !col.IsGenerated {
		log.Error("Cannot generate ADD COLUMN DDL: MappedType is missing for non-generated column", zap.String("column_name", col.Name))
		return "", fmt.Errorf("cannot generate ADD COLUMN DDL for non-generated column '%s': MappedType is missing", col.Name)
	}
	if col.IsGenerated {
		log.Warn("Generating ADD COLUMN DDL for a generated column. Dialect-specific syntax for GENERATED AS expression is complex and might require manual adjustment or is not fully supported by dbsync.",
			zap.String("target_type", col.MappedType))
	}

	// *** PERUBAHAN DI SINI ***
	quotedColName, typeAndAttrs, err := s.mapColumnDefinition(col)
	if err != nil {
		log.Error("Failed to map column definition for ADD COLUMN", zap.String("column_name", col.Name), zap.Error(err))
		return "", fmt.Errorf("failed to map column definition for ADD COLUMN '%s': %w", col.Name, err)
	}

	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)
	fullColDef := fmt.Sprintf("%s %s", quotedColName, typeAndAttrs) // Gabungkan untuk DDL

	switch s.dstDialect {
	case "mysql", "postgres", "sqlite":
		ddl := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", quotedTable, fullColDef)
		log.Debug("Generated ADD COLUMN DDL.", zap.String("ddl", ddl))
		return ddl, nil
	default:
		log.Error("Unsupported destination dialect for ADD COLUMN", zap.String("dialect", s.dstDialect))
		return "", fmt.Errorf("unsupported destination dialect '%s' for ADD COLUMN", s.dstDialect)
	}
}

// generateDropColumnDDL (tetap sama)
func (s *SchemaSyncer) generateDropColumnDDL(table string, col ColumnInfo) (string, error) {
	log := s.logger.With(zap.String("table", table), zap.String("column", col.Name), zap.String("action", "DROP COLUMN"))
	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)
	quotedColName := utils.QuoteIdentifier(col.Name, s.dstDialect)
	var ddl string
	switch s.dstDialect {
	case "mysql":
		ddl = fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", quotedTable, quotedColName)
	case "postgres":
		ddl = fmt.Sprintf("ALTER TABLE %s DROP COLUMN IF EXISTS %s;", quotedTable, quotedColName)
	case "sqlite":
		log.Warn("DROP COLUMN support in SQLite depends on version (>= 3.35.0). DDL will be generated without IF EXISTS.", zap.String("table", table), zap.String("column", col.Name))
		ddl = fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", quotedTable, quotedColName)
	default:
		log.Error("Unsupported destination dialect for DROP COLUMN", zap.String("dialect", s.dstDialect))
		return "", fmt.Errorf("unsupported destination dialect '%s' for DROP COLUMN", s.dstDialect)
	}
	log.Debug("Generated DROP COLUMN DDL.", zap.String("ddl", ddl))
	return ddl, nil
}

// generateDropIndexDDL (tetap sama)
func (s *SchemaSyncer) generateDropIndexDDL(table string, idx IndexInfo) (string, error) {
	log := s.logger.With(zap.String("table", table), zap.String("index", idx.Name), zap.String("action", "DROP INDEX"))
	quotedIndexName := utils.QuoteIdentifier(idx.Name, s.dstDialect)
	var ddl string
	switch s.dstDialect {
	case "mysql":
		quotedTableName := utils.QuoteIdentifier(table, s.dstDialect)
		ddl = fmt.Sprintf("DROP INDEX %s ON %s;", quotedIndexName, quotedTableName)
	case "postgres", "sqlite":
		ddl = fmt.Sprintf("DROP INDEX IF EXISTS %s;", quotedIndexName)
	default:
		log.Error("Unsupported destination dialect for DROP INDEX", zap.String("dialect", s.dstDialect))
		return "", fmt.Errorf("unsupported destination dialect '%s' for DROP INDEX", s.dstDialect)
	}
	log.Debug("Generated DROP INDEX DDL.", zap.String("ddl", ddl))
	return ddl, nil
}

// generateDropConstraintDDL (tetap sama)
func (s *SchemaSyncer) generateDropConstraintDDL(table string, constraint ConstraintInfo) (string, error) {
	log := s.logger.With(zap.String("table", table), zap.String("constraint", constraint.Name), zap.String("type", constraint.Type), zap.String("action", "DROP CONSTRAINT"))
	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)
	quotedConstraintName := utils.QuoteIdentifier(constraint.Name, s.dstDialect)
	var ddl string
	switch s.dstDialect {
	case "mysql":
		switch strings.ToUpper(constraint.Type) {
		case "FOREIGN KEY":
			ddl = fmt.Sprintf("ALTER TABLE %s DROP FOREIGN KEY %s;", quotedTable, quotedConstraintName)
		case "UNIQUE":
			ddl = fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s;", quotedTable, quotedConstraintName)
		case "CHECK":
			ddl = fmt.Sprintf("ALTER TABLE %s DROP CHECK %s;", quotedTable, quotedConstraintName)
		case "PRIMARY KEY":
			ddl = fmt.Sprintf("ALTER TABLE %s DROP PRIMARY KEY;", quotedTable)
		default:
			log.Error("Unsupported constraint type for DROP CONSTRAINT in MySQL", zap.String("constraint_type", constraint.Type), zap.String("constraint_name", constraint.Name))
			return "", fmt.Errorf("unsupported constraint type '%s' for DROP CONSTRAINT in MySQL for constraint '%s'", constraint.Type, constraint.Name)
		}
	case "postgres", "sqlite":
		ddl = fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s;", quotedTable, quotedConstraintName)
	default:
		log.Error("Unsupported destination dialect for DROP CONSTRAINT", zap.String("dialect", s.dstDialect))
		return "", fmt.Errorf("unsupported destination dialect '%s' for DROP CONSTRAINT", s.dstDialect)
	}
	log.Debug("Generated DROP CONSTRAINT DDL.", zap.String("ddl", ddl))
	return ddl, nil
}
