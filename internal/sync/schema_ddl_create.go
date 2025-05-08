// internal/sync/schema_ddl_create.go
package sync

import (
	"fmt"
	"sort"
	"strconv" // Diimpor karena digunakan oleh formatDefaultValue
	"strings"

	"go.uber.org/zap"

	"github.com/arwahdevops/dbsync/internal/utils"
)

// --- DDL Generation for CREATE Strategy ---

// generateCreateTableDDL generates the CREATE TABLE statement.
func (s *SchemaSyncer) generateCreateTableDDL(table string, columns []ColumnInfo) (string, []string, error) {
	var builder strings.Builder
	log := s.logger.With(zap.String("table", table), zap.String("action", "CREATE TABLE"))

	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)
	builder.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", quotedTable))

	primaryKeysQuoted := make([]string, 0)
	columnDefs := make([]string, len(columns))

	for i, col := range columns {
		if col.MappedType == "" && !col.IsGenerated {
			log.Error("Cannot generate CREATE TABLE DDL: MappedType is missing for non-generated column.",
				zap.String("column", col.Name))
			return "", nil, fmt.Errorf("mapped type missing for column '%s' in CREATE TABLE DDL generation for table '%s'", col.Name, table)
		}

		columnDef, err := s.mapColumnDefinition(col)
		if err != nil {
			return "", nil, fmt.Errorf("failed to map column definition for column '%s' in table '%s': %w", col.Name, table, err)
		}
		columnDefs[i] = "  " + columnDef

		if col.IsPrimary {
			primaryKeysQuoted = append(primaryKeysQuoted, utils.QuoteIdentifier(col.Name, s.dstDialect))
		}
	}

	builder.WriteString(strings.Join(columnDefs, ",\n"))

	if len(primaryKeysQuoted) > 0 {
		sort.Strings(primaryKeysQuoted)
		builder.WriteString(",\n  PRIMARY KEY (")
		builder.WriteString(strings.Join(primaryKeysQuoted, ", "))
		builder.WriteString(")")
	}

	builder.WriteString("\n);")

	log.Debug("Generated CREATE TABLE DDL successfully.")
	return builder.String(), primaryKeysQuoted, nil
}

// mapColumnDefinition generates the column definition part of CREATE TABLE or MODIFY COLUMN.
func (s *SchemaSyncer) mapColumnDefinition(col ColumnInfo) (string, error) {
	var definition strings.Builder
	log := s.logger.With(zap.String("column", col.Name))

	definition.WriteString(utils.QuoteIdentifier(col.Name, s.dstDialect))
	definition.WriteString(" ")

	targetType := col.MappedType
	if col.IsGenerated {
		if targetType == "" { targetType = col.Type }
		definition.WriteString(targetType)
		log.Warn("Mapping definition for a GENERATED column. Expression handling might require manual setup or enhanced features.",
			zap.String("target_type_used", targetType))
	} else {
		if targetType == "" {
			return "", fmt.Errorf("mapped type is missing for non-generated column '%s'", col.Name)
		}
		definition.WriteString(targetType)

		if col.AutoIncrement && col.IsPrimary {
			if autoIncSyntax := s.getAutoIncrementSyntax(targetType); autoIncSyntax != "" {
				definition.WriteString(" ")
				definition.WriteString(autoIncSyntax)
			}
		}

		if col.DefaultValue.Valid && !col.AutoIncrement && !col.IsGenerated {
			defaultValue := col.DefaultValue.String
			// Panggil helper statis dari compare_utils.go
			// Pastikan tidak ada 's.' di depannya
			if !isDefaultNullOrFunction(defaultValue) { // <--- Pemanggilan yang benar
				definition.WriteString(" DEFAULT ")
				definition.WriteString(s.formatDefaultValue(defaultValue, targetType)) // formatDefaultValue tetap method
			}
		}
	}

	if !col.IsNullable {
		definition.WriteString(" NOT NULL")
	}

	if col.Collation.Valid && col.Collation.String != "" && isStringType(normalizeTypeName(targetType)) {
		collationSyntax := s.getCollationSyntax(col.Collation.String)
		if collationSyntax != "" {
			definition.WriteString(" ")
			definition.WriteString(collationSyntax)
		}
	}

	if col.Comment.Valid && col.Comment.String != "" {
		commentSyntax := s.getCommentSyntax(col.Comment.String)
		if commentSyntax != "" && s.dstDialect == "mysql" { // Hanya MySQL yang mendukung COMMENT inline
			definition.WriteString(" ")
			definition.WriteString(commentSyntax)
		} else if commentSyntax != "" {
			log.Debug("Column comment found, but adding it inline is not supported for this dialect. Use COMMENT ON statement if needed.", zap.String("dialect", s.dstDialect), zap.String("comment", col.Comment.String))
		}
	}

	return definition.String(), nil
}

// generateCreateIndexDDLs generates CREATE INDEX statements.
func (s *SchemaSyncer) generateCreateIndexDDLs(table string, indexes []IndexInfo) []string {
	ddls := make([]string, 0, len(indexes))
	log := s.logger.With(zap.String("table", table), zap.String("action", "CREATE INDEX"))
	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)

	for _, idx := range indexes {
		if idx.IsPrimary {
			log.Debug("Skipping CREATE INDEX DDL generation for PRIMARY KEY index", zap.String("index", idx.Name))
			continue
		}
		if len(idx.Columns) == 0 {
			log.Warn("Skipping CREATE INDEX DDL generation due to empty column list", zap.String("index", idx.Name))
			continue
		}

		quotedIndexName := utils.QuoteIdentifier(idx.Name, s.dstDialect)
		quotedCols := make([]string, len(idx.Columns))
		for i, col := range idx.Columns {
			quotedCols[i] = utils.QuoteIdentifier(col, s.dstDialect)
		}

		uniqueKeyword := ""; if idx.IsUnique { uniqueKeyword = "UNIQUE " }
		ifExistsKeyword := ""; if s.dstDialect == "postgres" || s.dstDialect == "sqlite" { ifExistsKeyword = "IF NOT EXISTS "}

		ddl := fmt.Sprintf("CREATE %sINDEX %s%s ON %s (%s);",
			uniqueKeyword, ifExistsKeyword, quotedIndexName, quotedTable, strings.Join(quotedCols, ", "))

		log.Debug("Generated CREATE INDEX DDL", zap.String("ddl", ddl))
		ddls = append(ddls, ddl)
	}
	return ddls
}

// generateAddConstraintDDLs generates ADD CONSTRAINT statements (for UNIQUE, FK, CHECK).
func (s *SchemaSyncer) generateAddConstraintDDLs(table string, constraints []ConstraintInfo) []string {
	ddls := make([]string, 0, len(constraints))
	log := s.logger.With(zap.String("table", table), zap.String("action", "ADD CONSTRAINT"))
	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)

	for _, c := range constraints {
		if c.Type == "PRIMARY KEY" {
			log.Debug("Skipping ADD CONSTRAINT DDL generation for PRIMARY KEY", zap.String("constraint", c.Name))
			continue
		}
		if len(c.Columns) == 0 && c.Type != "CHECK" {
			log.Warn("Skipping ADD CONSTRAINT DDL generation due to empty column list", zap.String("constraint", c.Name), zap.String("type", c.Type))
			continue
		}

		quotedConstraintName := utils.QuoteIdentifier(c.Name, s.dstDialect)
		quotedCols := make([]string, len(c.Columns))
		for i, col := range c.Columns {
			quotedCols[i] = utils.QuoteIdentifier(col, s.dstDialect)
		}

		var ddl string
		constraintTypeUpper := strings.ToUpper(c.Type)

		switch constraintTypeUpper {
		case "UNIQUE":
			if len(quotedCols) > 0 {
				ddl = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s);",
					quotedTable, quotedConstraintName, strings.Join(quotedCols, ", "))
			} else {
				log.Warn("Cannot generate ADD UNIQUE CONSTRAINT without columns", zap.String("constraint", c.Name))
			}

		case "FOREIGN KEY":
			if len(quotedCols) == 0 || len(c.ForeignColumns) == 0 || c.ForeignTable == "" {
				log.Warn("Skipping ADD FOREIGN KEY due to missing columns or foreign table info", zap.String("constraint", c.Name))
				continue
			}
			if len(quotedCols) != len(c.ForeignColumns) {
				log.Warn("Skipping ADD FOREIGN KEY due to mismatch between local and foreign column counts", zap.String("constraint", c.Name), zap.Int("local_cols", len(quotedCols)), zap.Int("foreign_cols", len(c.ForeignColumns)))
				continue
			}

			quotedForeignTable := utils.QuoteIdentifier(c.ForeignTable, s.dstDialect)
			quotedForeignCols := make([]string, len(c.ForeignColumns))
			for i, fcol := range c.ForeignColumns {
				quotedForeignCols[i] = utils.QuoteIdentifier(fcol, s.dstDialect)
			}

			fkActions := ""
			onDeleteNorm := normalizeFKAction(c.OnDelete); onUpdateNorm := normalizeFKAction(c.OnUpdate)
			if onDeleteNorm != "NO ACTION" { fkActions += " ON DELETE " + onDeleteNorm }
			if onUpdateNorm != "NO ACTION" { fkActions += " ON UPDATE " + onUpdateNorm }

			ddl = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)%s;",
				quotedTable, quotedConstraintName, strings.Join(quotedCols, ", "),
				quotedForeignTable, strings.Join(quotedForeignCols, ", "), fkActions)

		case "CHECK":
			if c.Definition != "" {
				ddl = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s);",
					quotedTable, quotedConstraintName, c.Definition)
			} else {
				log.Warn("Cannot generate ADD CHECK CONSTRAINT without definition", zap.String("constraint", c.Name))
			}

		default:
			log.Warn("Unsupported constraint type for ADD CONSTRAINT DDL generation", zap.String("type", c.Type), zap.String("constraint", c.Name))
		}

		if ddl != "" {
			log.Debug("Generated ADD CONSTRAINT DDL", zap.String("ddl", ddl))
			ddls = append(ddls, ddl)
		}
	}
	return ddls
}

// --- Helper untuk DDL Create/Modify ---

// getAutoIncrementSyntax mengembalikan sintaks AUTO_INCREMENT/IDENTITY untuk dialek tujuan.
func (s *SchemaSyncer) getAutoIncrementSyntax(mappedDestType string) string {
	mappedDestTypeLower := strings.ToLower(mappedDestType)
	normMappedDestType := normalizeTypeName(mappedDestTypeLower)

	switch s.dstDialect {
	case "mysql":
		if isIntegerType(normMappedDestType) {
			return "AUTO_INCREMENT"
		}
	case "postgres":
		if isIntegerType(normMappedDestType) {
			return "GENERATED BY DEFAULT AS IDENTITY"
		}
	case "sqlite":
		if normMappedDestType == "integer" {
			return ""
		}
	}
	return ""
}

// formatDefaultValue memformat nilai default untuk DDL, termasuk quoting jika perlu.
func (s *SchemaSyncer) formatDefaultValue(value, mappedDataType string) string {
	normType := normalizeTypeName(mappedDataType)
	normValue := normalizeDefaultValue(value)

	switch normValue {
	case "current_timestamp", "current_date", "current_time", "nextval", "uuid_function", "null":
		if normValue == "null" { return "NULL"}
		return value
	}

	if normType == "bool" || normType == "boolean" {
		if normValue == "1" { return "TRUE" }
		if normValue == "0" { return "FALSE" }
		s.logger.Warn("Formatting non-standard boolean default", zap.String("original_value", value))
		return value
	}
	if normType == "tinyint" && strings.Contains(mappedDataType, "(1)") {
		if normValue == "1" { return "'1'" }
		if normValue == "0" { return "'0'" }
		s.logger.Warn("Formatting non-standard boolean (tinyint(1)) default", zap.String("original_value", value))
		return value
	}

	if isNumericType(mappedDataType) { // isNumericType dari compare_utils
		if _, err := strconv.ParseFloat(value, 64); err == nil { // strconv diimpor di file ini
			return value
		}
		// Panggil helper statis isDefaultNullOrFunction
		// Pastikan tidak ada 's.' di depannya
		if !isDefaultNullOrFunction(value) { // <--- Pemanggilan yang benar
			s.logger.Warn("Formatting potentially non-numeric default for a numeric type", zap.String("value", value), zap.String("type", mappedDataType))
		}
		return value
	}

	escapedValue := strings.ReplaceAll(value, "'", "''")
	return fmt.Sprintf("'%s'", escapedValue)
}


// getCollationSyntax mengembalikan klausa COLLATE jika didukung.
func (s *SchemaSyncer) getCollationSyntax(collationName string) string {
	if collationName == "" { return "" }
	switch s.dstDialect {
	case "mysql": return fmt.Sprintf("COLLATE %s", collationName)
	case "postgres": return fmt.Sprintf("COLLATE %s", utils.QuoteIdentifier(collationName, s.dstDialect))
	case "sqlite": return fmt.Sprintf("COLLATE %s", collationName)
	default: return ""
	}
}

// getCommentSyntax mengembalikan sintaks untuk menambahkan comment inline (hanya MySQL).
func (s *SchemaSyncer) getCommentSyntax(comment string) string {
	if comment == "" { return "" }
	if s.dstDialect == "mysql" {
		escapedComment := strings.ReplaceAll(comment, "'", "''")
		escapedComment = strings.ReplaceAll(escapedComment, "\\", "\\\\")
		return fmt.Sprintf("COMMENT '%s'", escapedComment)
	}
	return ""
}
