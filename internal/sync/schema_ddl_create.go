package sync

import (
	"fmt"
	"regexp" // <<< TAMBAHKAN IMPOR INI
	"sort"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/arwahdevops/dbsync/internal/utils"
)

// generateCreateTableDDL menghasilkan DDL CREATE TABLE, nama kolom PK (sudah di-quote dan diurutkan), dan error.
func (s *SchemaSyncer) generateCreateTableDDL(table string, columns []ColumnInfo) (string, []string, error) {
	var builder strings.Builder
	log := s.logger.With(zap.String("table", table), zap.String("action", "CREATE TABLE"), zap.String("dst_dialect", s.dstDialect))

	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)
	ifNotExistsClause := ""
	if s.dstDialect == "postgres" || s.dstDialect == "sqlite" {
		ifNotExistsClause = "IF NOT EXISTS "
	}

	builder.WriteString(fmt.Sprintf("CREATE TABLE %s%s (\n", ifNotExistsClause, quotedTable))

	primaryKeyColumnNames := make([]string, 0)
	columnDefs := make([]string, len(columns))

	if len(columns) == 0 {
		log.Error("Cannot generate CREATE TABLE DDL: no columns provided.")
		return "", nil, fmt.Errorf("cannot create table '%s' with no columns", table)
	}

	for i, col := range columns {
		if col.MappedType == "" && !col.IsGenerated {
			log.Error("Cannot generate CREATE TABLE DDL: MappedType is missing for non-generated column",
				zap.String("column_name", col.Name))
			return "", nil, fmt.Errorf("cannot generate CREATE TABLE DDL for table '%s': MappedType is missing for non-generated column '%s'", table, col.Name)
		}

		columnDef, err := s.mapColumnDefinition(col)
		if err != nil {
			return "", nil, fmt.Errorf("failed to generate column definition for CREATE TABLE '%s', column '%s': %w", table, col.Name, err)
		}
		columnDefs[i] = "  " + columnDef

		if col.IsPrimary {
			primaryKeyColumnNames = append(primaryKeyColumnNames, col.Name)
		}
	}

	builder.WriteString(strings.Join(columnDefs, ",\n"))

	if len(primaryKeyColumnNames) > 0 {
		sort.Strings(primaryKeyColumnNames)
		quotedPKs := make([]string, len(primaryKeyColumnNames))
		for i, pkName := range primaryKeyColumnNames {
			quotedPKs[i] = utils.QuoteIdentifier(pkName, s.dstDialect)
		}
		builder.WriteString(",\n  PRIMARY KEY (")
		builder.WriteString(strings.Join(quotedPKs, ", "))
		builder.WriteString(")")
	}

	builder.WriteString("\n);")

	log.Debug("Generated CREATE TABLE DDL successfully.")
	return builder.String(), primaryKeyColumnNames, nil
}

// mapColumnDefinition menghasilkan definisi kolom lengkap untuk CREATE TABLE atau ADD COLUMN.
func (s *SchemaSyncer) mapColumnDefinition(col ColumnInfo) (string, error) {
	var definition strings.Builder
	log := s.logger.With(zap.String("column", col.Name),
		zap.String("src_type_raw", col.Type),
		zap.String("mapped_type_target", col.MappedType),
		zap.String("dst_dialect", s.dstDialect),
		zap.String("action", "mapColumnDefinition"))

	definition.WriteString(utils.QuoteIdentifier(col.Name, s.dstDialect))
	definition.WriteString(" ")

	targetType := col.MappedType
	if col.IsGenerated {
		if strings.Contains(strings.ToUpper(col.Type), "GENERATED") || strings.Contains(strings.ToUpper(col.Type), " AS ") {
			definition.WriteString(col.Type)
			log.Debug("Using original source type for GENERATED column definition as it may contain expression.", zap.String("original_type_used", col.Type))
		} else {
			if targetType == "" {
				log.Error("MappedType is empty for generated column, falling back to original type. This might be incorrect.", zap.String("original_type", col.Type))
				definition.WriteString(col.Type)
			} else {
				definition.WriteString(targetType)
			}
		}
	} else {
		if targetType == "" {
			log.Error("MappedType is missing for non-generated column.")
			return "", fmt.Errorf("mappedType is missing for non-generated column '%s'", col.Name)
		}
		definition.WriteString(targetType)

		if col.AutoIncrement && col.IsPrimary {
			if autoIncSyntax := s.getAutoIncrementSyntax(targetType); autoIncSyntax != "" {
				definition.WriteString(" ")
				definition.WriteString(autoIncSyntax)
			}
		}

		if col.DefaultValue.Valid && col.DefaultValue.String != "" && !col.AutoIncrement && !col.IsGenerated {
			normalizedDefault := normalizeDefaultValue(col.DefaultValue.String, s.dstDialect) // dari schema_compare.go
			if !isDefaultNullOrFunction(normalizedDefault) { // dari schema_compare.go
				definition.WriteString(" DEFAULT ")
				definition.WriteString(s.formatDefaultValue(col.DefaultValue.String, targetType))
			} else {
				log.Debug("Skipping explicit DEFAULT clause for NULL or DB function.", zap.String("normalized_default", normalizedDefault))
			}
		}
	}

	if !col.IsNullable {
		definition.WriteString(" NOT NULL")
	}

	if col.Collation.Valid && col.Collation.String != "" && isStringType(normalizeTypeName(targetType)) { // isStringType & normalizeTypeName dari syncer_type_mapper.go
		if collationSyntax := s.getCollationSyntax(col.Collation.String); collationSyntax != "" {
			definition.WriteString(" ")
			definition.WriteString(collationSyntax)
		}
	}

	if col.Comment.Valid && col.Comment.String != "" {
		if commentSyntax := s.getCommentSyntax(col.Comment.String); commentSyntax != "" {
			definition.WriteString(" ")
			definition.WriteString(commentSyntax)
		} else if s.dstDialect != "mysql" {
			log.Debug("Column comment found, but adding it inline is not supported for this dialect. Use COMMENT ON statement if needed.", zap.String("comment", col.Comment.String))
		}
	}

	return definition.String(), nil
}

// generateCreateIndexDDLs menghasilkan DDL CREATE INDEX.
func (s *SchemaSyncer) generateCreateIndexDDLs(table string, indexes []IndexInfo) []string {
	ddls := make([]string, 0, len(indexes))
	log := s.logger.With(zap.String("table", table), zap.String("action", "CREATE INDEX"), zap.String("dst_dialect", s.dstDialect))
	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)

	for _, idx := range indexes {
		if idx.IsPrimary {
			log.Debug("Skipping CREATE INDEX DDL generation for PRIMARY KEY index (handled by CREATE TABLE).", zap.String("index", idx.Name))
			continue
		}
		if len(idx.Columns) == 0 {
			log.Warn("Skipping CREATE INDEX DDL generation due to empty column list.", zap.String("index", idx.Name))
			continue
		}

		quotedIndexName := utils.QuoteIdentifier(idx.Name, s.dstDialect)
		quotedCols := make([]string, len(idx.Columns))
		for i, colName := range idx.Columns {
			quotedCols[i] = utils.QuoteIdentifier(colName, s.dstDialect)
		}

		uniqueKeyword := ""; if idx.IsUnique { uniqueKeyword = "UNIQUE " }
		ifNotExistsKeyword := ""
		if s.dstDialect == "postgres" || s.dstDialect == "sqlite" {
			ifNotExistsKeyword = "IF NOT EXISTS "
		} else if s.dstDialect == "mysql" {
			log.Debug("MySQL does not directly support 'IF NOT EXISTS' for CREATE INDEX. Error 1061 (Duplicate key name) will be ignored if index already exists.", zap.String("index", idx.Name))
		}

		usingClause := ""
		if idx.RawDef != "" && s.dstDialect == "postgres" {
			reUsing := regexp.MustCompile(`USING\s+([a-zA-Z0-9_]+)`) // Regex didefinisikan di sini
			matches := reUsing.FindStringSubmatch(strings.ToUpper(idx.RawDef))
			if len(matches) > 1 && strings.ToUpper(matches[1]) != "BTREE" {
				usingClause = fmt.Sprintf(" USING %s", strings.ToLower(matches[1]))
			}
		}

		ddl := fmt.Sprintf("CREATE %sINDEX %s%s ON %s%s (%s);",
			uniqueKeyword, ifNotExistsKeyword, quotedIndexName, quotedTable, usingClause, strings.Join(quotedCols, ", "))

		log.Debug("Generated CREATE INDEX DDL", zap.String("ddl", ddl))
		ddls = append(ddls, ddl)
	}
	return ddls
}

// generateAddConstraintDDLs menghasilkan DDL ADD CONSTRAINT (UNIQUE, FK, CHECK).
func (s *SchemaSyncer) generateAddConstraintDDLs(table string, constraints []ConstraintInfo) []string {
	ddls := make([]string, 0, len(constraints))
	log := s.logger.With(zap.String("table", table), zap.String("action", "ADD CONSTRAINT"), zap.String("dst_dialect", s.dstDialect))
	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)

	for _, c := range constraints {
		if c.Type == "PRIMARY KEY" {
			log.Debug("Skipping ADD CONSTRAINT DDL generation for PRIMARY KEY (handled by CREATE TABLE).", zap.String("constraint", c.Name))
			continue
		}
		if len(c.Columns) == 0 && (c.Type == "UNIQUE" || c.Type == "FOREIGN KEY") {
			log.Warn("Skipping ADD CONSTRAINT DDL due to empty column list for non-CHECK constraint.",
				zap.String("constraint", c.Name), zap.String("type", c.Type))
			continue
		}

		quotedConstraintName := utils.QuoteIdentifier(c.Name, s.dstDialect)
		quotedCols := make([]string, len(c.Columns))
		for i, colName := range c.Columns {
			quotedCols[i] = utils.QuoteIdentifier(colName, s.dstDialect)
		}

		var ddl string
		constraintTypeUpper := strings.ToUpper(c.Type)

		// Variabel ifNotExistsClause tidak digunakan di sini karena ADD CONSTRAINT IF NOT EXISTS tidak standar
		// dan kita mengandalkan shouldIgnoreDDLError.
		// ifNotExistsClause := "" // <<< HAPUS BARIS INI

		if s.dstDialect == "mysql" {
			log.Debug("MySQL does not support 'IF NOT EXISTS' for ADD CONSTRAINT. Relevant errors will be ignored.", zap.String("constraint", c.Name))
		}

		switch constraintTypeUpper {
		case "UNIQUE":
			if len(quotedCols) > 0 {
				ddl = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s);",
					quotedTable, quotedConstraintName, strings.Join(quotedCols, ", "))
			} else {
				log.Warn("Cannot generate ADD UNIQUE CONSTRAINT without columns.", zap.String("constraint", c.Name))
			}

		case "FOREIGN KEY":
			if len(quotedCols) == 0 || len(c.ForeignColumns) == 0 || c.ForeignTable == "" {
				log.Warn("Skipping ADD FOREIGN KEY due to missing local/foreign columns or foreign table.", zap.String("constraint", c.Name))
				continue
			}
			if len(quotedCols) != len(c.ForeignColumns) {
				log.Warn("Skipping ADD FOREIGN KEY due to column count mismatch between local and foreign columns.", zap.String("constraint", c.Name))
				continue
			}

			quotedForeignTable := utils.QuoteIdentifier(c.ForeignTable, s.dstDialect)
			quotedForeignCols := make([]string, len(c.ForeignColumns))
			for i, fcolName := range c.ForeignColumns {
				quotedForeignCols[i] = utils.QuoteIdentifier(fcolName, s.dstDialect)
			}

			onDeleteAction := normalizeFKAction(c.OnDelete) // dari schema_compare.go
			onUpdateAction := normalizeFKAction(c.OnUpdate) // dari schema_compare.go
			fkActions := ""
			if onDeleteAction != "NO ACTION" && onDeleteAction != "" { fkActions += " ON DELETE " + onDeleteAction }
			if onUpdateAction != "NO ACTION" && onUpdateAction != "" { fkActions += " ON UPDATE " + onUpdateAction }

			deferrableClause := ""
			if s.dstDialect == "postgres" {
				deferrableClause = " DEFERRABLE INITIALLY DEFERRED"
				log.Debug("Generating PostgreSQL FOREIGN KEY constraint as DEFERRABLE INITIALLY DEFERRED.", zap.String("constraint", c.Name))
			}

			ddl = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)%s%s;",
				quotedTable, quotedConstraintName, strings.Join(quotedCols, ", "),
				quotedForeignTable, strings.Join(quotedForeignCols, ", "), fkActions, deferrableClause)

		case "CHECK":
			if c.Definition != "" {
				ddl = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s);",
					quotedTable, quotedConstraintName, c.Definition)
			} else {
				log.Warn("Cannot generate ADD CHECK CONSTRAINT without definition.", zap.String("constraint", c.Name))
			}

		default:
			log.Warn("Unsupported constraint type for ADD CONSTRAINT DDL generation.",
				zap.String("type", c.Type), zap.String("constraint", c.Name))
		}

		if ddl != "" {
			log.Debug("Generated ADD CONSTRAINT DDL", zap.String("ddl", ddl))
			ddls = append(ddls, ddl)
		}
	}
	return ddls
}

// --- Helper untuk DDL Create/Modify ---

func (s *SchemaSyncer) getAutoIncrementSyntax(mappedDestType string) string {
	normMappedDestType := normalizeTypeName(mappedDestType)
	switch s.dstDialect {
	case "mysql":
		if isIntegerType(normMappedDestType) {
			return "AUTO_INCREMENT"
		}
	case "postgres":
		switch normMappedDestType {
		case "smallint", "int", "bigint":
			return "GENERATED BY DEFAULT AS IDENTITY"
		case "serial", "smallserial", "bigserial":
			return ""
		}
	case "sqlite":
		if normMappedDestType == "integer" {
			return ""
		}
	}
	return ""
}

func (s *SchemaSyncer) formatDefaultValue(value, mappedDataType string) string {
	normType := normalizeTypeName(mappedDataType) // normalizeTypeName dari syncer_type_mapper.go atau compare_utils.go
	if s.dstDialect == "mysql" && (mappedDataType == "tinyint(1)" || mappedDataType == "boolean") {
		if value == "0" || strings.EqualFold(value, "false") { return "'0'" }
		if value == "1" || strings.EqualFold(value, "true") { return "'1'" }
	}
	if s.dstDialect == "postgres" && normType == "bool" {
	    if value == "0" || strings.EqualFold(value, "false") { return "FALSE" }
		if value == "1" || strings.EqualFold(value, "true") { return "TRUE" }
	}

	if isIntegerType(normType) || isNumericType(normType) { // isIntegerType & isNumericType dari syncer_type_mapper.go atau compare_utils.go
		if _, err := strconv.ParseFloat(value, 64); err == nil {
			if s.dstDialect == "mysql" && (strings.Contains(strings.ToUpper(value), "CURRENT_TIMESTAMP") || strings.Contains(strings.ToUpper(value), "NOW()")) {
			    return value
			}
			return value
		}
	}

	if s.dstDialect == "postgres" && (normType == "bit" || normType == "varbit") {
		if (strings.HasPrefix(value, "B'") || strings.HasPrefix(value, "b'")) && strings.HasSuffix(value, "'") {
			return value
		}
	}

	escapedValue := strings.ReplaceAll(value, "'", "''")
	return fmt.Sprintf("'%s'", escapedValue)
}

func (s *SchemaSyncer) getCollationSyntax(collationName string) string {
	if collationName == "" {
		return ""
	}
	if s.srcDialect != s.dstDialect {
		s.logger.Debug("Skipping explicit COLLATE clause due to cross-dialect sync.",
			zap.String("source_collation", collationName),
			zap.String("src_dialect", s.srcDialect),
			zap.String("dst_dialect", s.dstDialect))
		return ""
	}
	switch s.dstDialect {
	case "mysql", "postgres":
		return fmt.Sprintf("COLLATE %s", utils.QuoteIdentifier(collationName, s.dstDialect))
	case "sqlite":
		return fmt.Sprintf("COLLATE %s", collationName)
	default:
		return ""
	}
}

func (s *SchemaSyncer) getCommentSyntax(comment string) string {
	if comment == "" {
		return ""
	}
	if s.dstDialect == "mysql" {
		escapedComment := strings.ReplaceAll(comment, "\\", "\\\\")
		escapedComment = strings.ReplaceAll(escapedComment, "'", "''")
		return fmt.Sprintf("COMMENT '%s'", escapedComment)
	}
	return ""
}
