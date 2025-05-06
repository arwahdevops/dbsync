package sync

import (
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/arwahdevops/dbsync/internal/utils"
)

// --- DDL Generation Helpers for ALTER Strategy ---

// generateAddColumnDDL generates ALTER TABLE ... ADD COLUMN ...
func (s *SchemaSyncer) generateAddColumnDDL(table string, col ColumnInfo) (string, error) {
	colDef, err := s.mapColumnDefinition(col) // Use receiver s.
	if err != nil { return "", fmt.Errorf("map def for add col %s: %w", col.Name, err) }
	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)
	switch s.dstDialect {
	case "mysql", "postgres", "sqlite": // SQLite ADD COLUMN is supported
		return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", quotedTable, colDef), nil
	default:
		return "", fmt.Errorf("unsupported dialect for ADD COLUMN: %s", s.dstDialect)
	}
}

// generateDropColumnDDL generates ALTER TABLE ... DROP COLUMN ...
func (s *SchemaSyncer) generateDropColumnDDL(table string, col ColumnInfo) (string, error) {
	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)
	quotedCol := utils.QuoteIdentifier(col.Name, s.dstDialect)
	switch s.dstDialect {
	case "mysql", "postgres":
		// Add IF EXISTS for safety if supported (Postgres >= 9.0)
		ifExists := ""; if s.dstDialect == "postgres" { ifExists = "IF EXISTS "}
		return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s%s;", quotedTable, ifExists, quotedCol), nil
	case "sqlite":
		// SQLite >= 3.35.0 supports DROP COLUMN
		s.logger.Warn("DROP COLUMN support in SQLite depends on version (>= 3.35.0).", zap.String("column", col.Name))
		// Generate the command anyway, it will fail on older versions.
		return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", quotedTable, quotedCol), nil
	default:
		return "", fmt.Errorf("unsupported dialect for DROP COLUMN: %s", s.dstDialect)
	}
}

// generateModifyColumnDDLs generates one or more ALTER statements for column modification.
func (s *SchemaSyncer) generateModifyColumnDDLs(table string, src, dst ColumnInfo, log *zap.Logger) []string {
	ddls := make([]string, 0)
	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)
	quotedCol := utils.QuoteIdentifier(src.Name, s.dstDialect)
	// mysqlRequiresModify := false // Dihapus

	// --- Determine differences using helper ---
	diffTypes := s.getColumnModifications(src, dst, log) // Use receiver s.
	if len(diffTypes) == 0 {
		return ddls // No changes needed
	}

	// --- Generate Dialect-Specific DDLs ---
	switch s.dstDialect {
	case "mysql":
		// MySQL usually requires a single MODIFY COLUMN with the full new definition
		// for any type, nullability, or default change.
		log.Debug("Generating MySQL MODIFY COLUMN statement due to detected changes", zap.String("column", src.Name), zap.Strings("changes", diffTypes))
		colDef, err := s.mapColumnDefinition(src) // Generate full definition from source state
		if err == nil {
			ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s;", quotedTable, colDef))
		} else {
			log.Error("Failed to map definition for MySQL MODIFY COLUMN", zap.Error(err))
			// Return empty DDL on error? Or try partial alters? Stick to safe empty for now.
		}

	case "postgres":
		// PostgreSQL allows altering parts separately
		for _, diff := range diffTypes { // Iterate over specific differences found
			if strings.HasPrefix(diff, "type") {
				log.Warn("Generating ALTER COLUMN TYPE for PostgreSQL (may need manual USING clause)", zap.String("column", src.Name), zap.String("to_type", src.MappedType))
				ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s /* USING ??? */;", quotedTable, quotedCol, src.MappedType))
			} else if strings.HasPrefix(diff, "nullability") {
				action := "DROP NOT NULL"; if !src.IsNullable { action = "SET NOT NULL" }
				ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s %s;", quotedTable, quotedCol, action))
			} else if strings.HasPrefix(diff, "default") {
				action := "DROP DEFAULT"
				if src.DefaultValue.Valid && !s.isDefaultNullOrFunction(src.DefaultValue.String) { // Use receiver s.
					action = fmt.Sprintf("SET DEFAULT %s", s.formatDefaultValue(src.DefaultValue.String, src.MappedType)) // Use receiver s.
				}
				ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s %s;", quotedTable, quotedCol, action))
			}
			// TODO: Handle Collation change with ALTER COLUMN ... SET DATA TYPE ... COLLATE?
		}

	case "sqlite":
		// SQLite has very limited ALTER TABLE capabilities. Log errors for unsupported changes.
		for _, diff := range diffTypes {
			if strings.HasPrefix(diff, "type") { log.Error("Cannot ALTER COLUMN TYPE in SQLite", zap.String("column", src.Name)) }
			if strings.HasPrefix(diff, "nullability") { log.Error("Cannot ALTER COLUMN NULL constraint directly in SQLite", zap.String("column", src.Name)) }
			if strings.HasPrefix(diff, "default") { log.Error("Cannot ALTER COLUMN DEFAULT directly in SQLite", zap.String("column", src.Name)) }
		}
		log.Warn("Required column modification(s) not supported by SQLite ALTER TABLE", zap.String("column", src.Name), zap.Strings("changes", diffTypes))
		// Return empty DDLs for SQLite modifications

	default:
		log.Error("Unsupported dialect for ALTER COLUMN modifications", zap.String("dialect", s.dstDialect))
	}

	return ddls
}


// generateDropIndexDDL generates DROP INDEX statement.
func (s *SchemaSyncer) generateDropIndexDDL(table string, idx IndexInfo) (string, error) {
	quotedIndexName := utils.QuoteIdentifier(idx.Name, s.dstDialect)
	switch s.dstDialect {
	case "mysql": rawTableName := utils.QuoteIdentifier(table, s.dstDialect); return fmt.Sprintf("DROP INDEX %s ON %s;", quotedIndexName, rawTableName), nil
	case "postgres", "sqlite": return fmt.Sprintf("DROP INDEX IF EXISTS %s;", quotedIndexName), nil // Use IF EXISTS
	default: return "", fmt.Errorf("unsupported dialect for DROP INDEX: %s", s.dstDialect)
	}
}

// generateDropConstraintDDL generates DROP CONSTRAINT statement.
func (s *SchemaSyncer) generateDropConstraintDDL(table string, constraint ConstraintInfo) (string, error) {
	quotedTable := utils.QuoteIdentifier(table, s.dstDialect); quotedConstraintName := utils.QuoteIdentifier(constraint.Name, s.dstDialect)
	switch s.dstDialect {
	case "mysql":
		if constraint.Type == "FOREIGN KEY" { return fmt.Sprintf("ALTER TABLE %s DROP FOREIGN KEY %s;", quotedTable, quotedConstraintName), nil }
		if constraint.Type == "CHECK" { return fmt.Sprintf("ALTER TABLE %s DROP CHECK %s;", quotedTable, quotedConstraintName), nil }
		if constraint.Type == "UNIQUE" { return fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s;", quotedTable, quotedConstraintName), nil } // Coba drop constraint
		return "", fmt.Errorf("unsupported drop constraint type MySQL: %s (%s)", constraint.Type, constraint.Name)
	case "postgres", "sqlite": return fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s;", quotedTable, quotedConstraintName), nil // Use IF EXISTS
	default: return "", fmt.Errorf("unsupported dialect for DROP CONSTRAINT: %s", s.dstDialect)
	}
}