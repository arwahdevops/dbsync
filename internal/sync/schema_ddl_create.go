package sync

import (
	"fmt"
	"sort"
	"strings"

	"go.uber.org/zap" // Mungkin diperlukan untuk logging di helper

	"github.com/arwahdevops/dbsync/internal/utils"
)

// --- DDL Generation for CREATE Strategy ---

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

// generateCreateIndexDDLs generates CREATE INDEX statements.
func (s *SchemaSyncer) generateCreateIndexDDLs(table string, indexes []IndexInfo) []string {
    ddls := make([]string, 0, len(indexes)); quotedTable := utils.QuoteIdentifier(table, s.dstDialect)
    for _, idx := range indexes {
        if idx.IsPrimary { continue } // PK handled in CREATE TABLE
        quotedIndexName := utils.QuoteIdentifier(idx.Name, s.dstDialect); quotedCols := make([]string, len(idx.Columns))
        for i, col := range idx.Columns { quotedCols[i] = utils.QuoteIdentifier(col, s.dstDialect) }
        uniqueKeyword := ""; if idx.IsUnique { uniqueKeyword = "UNIQUE " }
        ifExistsKeyword := "IF NOT EXISTS "; if s.dstDialect == "mysql" { ifExistsKeyword = "" }
        ddl := fmt.Sprintf("CREATE %sINDEX %s%s ON %s (%s);", uniqueKeyword, ifExistsKeyword, quotedIndexName, quotedTable, strings.Join(quotedCols, ", "))
        ddls = append(ddls, ddl)
    }
    return ddls
}

// generateAddConstraintDDLs generates ADD CONSTRAINT statements (for UNIQUE, FK, CHECK).
func (s *SchemaSyncer) generateAddConstraintDDLs(table string, constraints []ConstraintInfo) []string {
    ddls := make([]string, 0, len(constraints)); quotedTable := utils.QuoteIdentifier(table, s.dstDialect)
    for _, c := range constraints {
        if c.Type == "PRIMARY KEY" { continue } // PK handled in CREATE TABLE
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

// --- Helper untuk DDL Create ---
func (s *SchemaSyncer) getAutoIncrementSyntax(mappedDestType string) string { mappedDestTypeLower := strings.ToLower(mappedDestType); switch s.dstDialect { case "mysql": if strings.Contains(mappedDestTypeLower, "int") && !strings.Contains(mappedDestTypeLower, "auto_increment") { return "AUTO_INCREMENT" }; case "postgres": if strings.Contains(mappedDestTypeLower, "int") { return "GENERATED BY DEFAULT AS IDENTITY" }; case "sqlite": if mappedDestTypeLower == "integer" { return "" } }; return "" }
func (s *SchemaSyncer) formatDefaultValue(value, mappedDataType string) string { normType := strings.ToLower(mappedDataType); normValue := strings.ToLower(value); if strings.Contains(normValue, "nextval(") || normValue == "current_timestamp" || normValue == "now()" || normValue == "current_date" || normValue == "current_time" || strings.HasPrefix(normValue, "gen_random_uuid()") || normValue == "uuid()" { return value }; if strings.Contains(normType, "bool") || strings.Contains(normType, "tinyint(1)") { if normValue == "true" || normValue == "1" || normValue == "'t'" || normValue == "'y'" || normValue == "'1'" { return "'1'" }; if normValue == "false" || normValue == "0" || normValue == "'f'" || normValue == "'n'" || normValue == "'0'" { return "'0'" } }; if strings.Contains(normType, "int") || strings.Contains(normType, "serial") || strings.Contains(normType, "numeric") || strings.Contains(normType, "decimal") || strings.Contains(normType, "float") || strings.Contains(normType, "double") || strings.Contains(normType, "real") { return value }; return fmt.Sprintf("'%s'", strings.ReplaceAll(value, "'", "''")) }
func (s *SchemaSyncer) isDefaultNullOrFunction(defaultValue string) bool { normValue := strings.ToLower(strings.TrimSpace(defaultValue)); if normValue == "null" || normValue == "" { return true }; return strings.Contains(normValue, "nextval(") || strings.Contains(normValue, "current_") || strings.Contains(normValue, "now()") || strings.Contains(normValue, "uuid(") }