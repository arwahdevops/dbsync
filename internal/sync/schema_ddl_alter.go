// internal/sync/schema_ddl_alter.go
package sync

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
	"github.com/arwahdevops/dbsync/internal/utils"
)

// generateModifyColumnDDLs generates one or more ALTER statements for column modification.
// Ini sekarang menjadi dispatcher ke fungsi spesifik dialek.
func (s *SchemaSyncer) generateModifyColumnDDLs(table string, src, dst ColumnInfo, log *zap.Logger) []string {
	// Dapatkan perbedaan dulu (menggunakan fungsi dari compare_columns.go)
	// getColumnModifications adalah method dari *SchemaSyncer
	diffs := s.getColumnModifications(src, dst, log) // `s` sudah menjadi receiver di sini

	if len(diffs) == 0 {
		// Tidak perlu log di sini karena pemanggil (generateAlterDDLs) akan log jika tidak ada DDL
		return []string{}
	}

	// Gunakan logger dari parameter fungsi, yang sudah di-scope dengan benar oleh pemanggil
	log.Info("Generating ALTER COLUMN DDL(s) due to detected differences.",
		// table dan column sudah ada di scope logger dari pemanggil (generateAlterDDLs)
		// zap.String("table", table),
		// zap.String("column", src.Name),
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

// --- Fungsi DDL ALTER lainnya (ADD COLUMN, DROP COLUMN, DROP INDEX, DROP CONSTRAINT) ---

// generateAddColumnDDL generates ALTER TABLE ... ADD COLUMN ...
func (s *SchemaSyncer) generateAddColumnDDL(table string, col ColumnInfo) (string, error) {
	log := s.logger.With(zap.String("table", table), zap.String("column", col.Name), zap.String("action", "ADD COLUMN")) // Gunakan s.logger

	if col.IsGenerated {
		log.Warn("Generating ADD COLUMN DDL for a generated column. Ensure expression is part of dialect-specific definition.")
	}
	if col.MappedType == "" && !col.IsGenerated {
		return "", fmt.Errorf("cannot generate ADD COLUMN DDL for column '%s': MappedType is missing", col.Name)
	}

	colDef, err := s.mapColumnDefinition(col)
	if err != nil {
		return "", fmt.Errorf("failed to map column definition for ADD COLUMN '%s': %w", col.Name, err)
	}

	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)

	switch s.dstDialect {
	case "mysql", "postgres", "sqlite":
		return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", quotedTable, colDef), nil
	default:
		return "", fmt.Errorf("unsupported destination dialect '%s' for ADD COLUMN", s.dstDialect)
	}
}

// generateDropColumnDDL generates ALTER TABLE ... DROP COLUMN ...
func (s *SchemaSyncer) generateDropColumnDDL(table string, col ColumnInfo) (string, error) {
	// Tidak perlu logger spesifik di sini jika tidak ada keputusan atau logging khusus
	// Pemanggil (generateAlterDDLs) akan melakukan logging.
	// log := s.logger.With(...)
	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)
	quotedColName := utils.QuoteIdentifier(col.Name, s.dstDialect)

	switch s.dstDialect {
	case "mysql":
		return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", quotedTable, quotedColName), nil
	case "postgres":
		return fmt.Sprintf("ALTER TABLE %s DROP COLUMN IF EXISTS %s;", quotedTable, quotedColName), nil
	case "sqlite":
		s.logger.Warn("DROP COLUMN support in SQLite depends on version (>= 3.35.0). DDL will be generated.", // Gunakan s.logger
			zap.String("table", table), zap.String("column", col.Name))
		return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", quotedTable, quotedColName), nil
	default:
		return "", fmt.Errorf("unsupported destination dialect '%s' for DROP COLUMN", s.dstDialect)
	}
}

// generateDropIndexDDL generates DROP INDEX statement.
func (s *SchemaSyncer) generateDropIndexDDL(table string, idx IndexInfo) (string, error) {
	// log := s.logger.With(...)
	quotedIndexName := utils.QuoteIdentifier(idx.Name, s.dstDialect)
	switch s.dstDialect {
	case "mysql":
		quotedTableName := utils.QuoteIdentifier(table, s.dstDialect)
		return fmt.Sprintf("DROP INDEX %s ON %s;", quotedIndexName, quotedTableName), nil
	case "postgres", "sqlite":
		return fmt.Sprintf("DROP INDEX IF EXISTS %s;", quotedIndexName), nil
	default:
		return "", fmt.Errorf("unsupported destination dialect '%s' for DROP INDEX", s.dstDialect)
	}
}

// generateDropConstraintDDL generates DROP CONSTRAINT statement.
func (s *SchemaSyncer) generateDropConstraintDDL(table string, constraint ConstraintInfo) (string, error) {
	// log := s.logger.With(...)
	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)
	quotedConstraintName := utils.QuoteIdentifier(constraint.Name, s.dstDialect)

	switch s.dstDialect {
	case "mysql":
		switch strings.ToUpper(constraint.Type) {
		case "FOREIGN KEY":
			return fmt.Sprintf("ALTER TABLE %s DROP FOREIGN KEY %s;", quotedTable, quotedConstraintName), nil
		case "UNIQUE":
			return fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s;", quotedTable, quotedConstraintName), nil
		case "CHECK":
			return fmt.Sprintf("ALTER TABLE %s DROP CHECK %s;", quotedTable, quotedConstraintName), nil
		case "PRIMARY KEY":
			return fmt.Sprintf("ALTER TABLE %s DROP PRIMARY KEY;", quotedTable), nil
		default:
			return "", fmt.Errorf("unsupported constraint type '%s' for DROP CONSTRAINT in MySQL for constraint '%s'", constraint.Type, constraint.Name)
		}
	case "postgres", "sqlite":
		return fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s;", quotedTable, quotedConstraintName), nil
	default:
		return "", fmt.Errorf("unsupported destination dialect '%s' for DROP CONSTRAINT", s.dstDialect)
	}
}
