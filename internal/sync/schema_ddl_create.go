// internal/sync/schema_ddl_create.go
package sync

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/arwahdevops/dbsync/internal/utils" // Diperlukan untuk QuoteIdentifier
)

// --- DDL Generation for CREATE Strategy ---

// generateCreateTableDDL generates the CREATE TABLE statement.
// Mengembalikan DDL tabel, daftar nama kolom PK (sudah di-quote dan diurutkan), dan error.
func (s *SchemaSyncer) generateCreateTableDDL(table string, columns []ColumnInfo) (string, []string, error) {
	var builder strings.Builder
	log := s.logger.With(zap.String("table", table), zap.String("action", "CREATE TABLE"), zap.String("dst_dialect", s.dstDialect))

	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)
	builder.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", quotedTable))

	primaryKeyColumnNames := make([]string, 0) // Nama kolom PK asli, belum di-quote
	columnDefs := make([]string, len(columns))

	if len(columns) == 0 {
		log.Error("Cannot generate CREATE TABLE DDL: no columns provided.")
		return "", nil, fmt.Errorf("cannot create table '%s' with no columns", table)
	}

	for i, col := range columns {
		// MappedType seharusnya sudah diisi oleh pemanggil (SyncTableSchema -> populateMappedTypesForSourceColumns)
		if col.MappedType == "" && !col.IsGenerated {
			log.Error("Cannot generate CREATE TABLE DDL: MappedType is missing for non-generated column",
				zap.String("table_name", table), zap.String("column_name", col.Name))
			return "", nil, fmt.Errorf("cannot generate CREATE TABLE DDL for table '%s': MappedType is missing for non-generated column '%s'", table, col.Name) // PERBAIKAN
		}

		// Panggil mapColumnDefinition untuk mendapatkan definisi lengkap satu kolom
		columnDef, err := s.mapColumnDefinition(col)
		if err != nil {
			// Error sudah termasuk konteks kolom dari mapColumnDefinition
			return "", nil, fmt.Errorf("failed to generate column definition for CREATE TABLE '%s': %w", table, err) // Gunakan %w
		}
		columnDefs[i] = "  " + columnDef // Tambahkan indentasi

		if col.IsPrimary {
			primaryKeyColumnNames = append(primaryKeyColumnNames, col.Name) // Simpan nama asli
		}
	}

	builder.WriteString(strings.Join(columnDefs, ",\n"))

	// Tambahkan klausa PRIMARY KEY jika ada
	if len(primaryKeyColumnNames) > 0 {
		sort.Strings(primaryKeyColumnNames) // Urutkan nama kolom PK untuk konsistensi DDL
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
	// Mengembalikan nama PK yang sudah di-quote dan diurutkan
	quotedAndSortedPKs := make([]string, len(primaryKeyColumnNames))
	for i, name := range primaryKeyColumnNames { // primaryKeyColumnNames sudah diurutkan
		quotedAndSortedPKs[i] = utils.QuoteIdentifier(name, s.dstDialect)
	}
	return builder.String(), quotedAndSortedPKs, nil
}

// mapColumnDefinition generates the column definition part of CREATE TABLE or ADD COLUMN.
// Ini adalah method dari SchemaSyncer (s).
func (s *SchemaSyncer) mapColumnDefinition(col ColumnInfo) (string, error) {
	var definition strings.Builder
	// Logger di-scope di sini untuk konteks kolom spesifik
	log := s.logger.With(zap.String("column", col.Name), zap.String("dst_dialect", s.dstDialect), zap.String("action", "mapColumnDefinition"))

	definition.WriteString(utils.QuoteIdentifier(col.Name, s.dstDialect))
	definition.WriteString(" ")

	// MappedType adalah tipe data tujuan yang sudah diproses (termasuk modifier)
	targetType := col.MappedType
	if col.IsGenerated {
		if targetType == "" { targetType = col.Type } // Fallback jika MappedType kosong untuk generated
		definition.WriteString(targetType)
		// TODO: Implementasi sintaks GENERATED AS spesifik dialek jika diperlukan
		log.Debug("Mapping definition for a GENERATED column. Expression syntax (GENERATED AS...) needs dialect-specific handling not fully implemented.", zap.String("target_type_used", targetType))
	} else {
		// Untuk kolom non-generated
		if targetType == "" {
			log.Error("Mapped type is missing for non-generated column", zap.String("column_name", col.Name))
			return "", fmt.Errorf("mapped type is missing for non-generated column '%s'", col.Name) // PERBAIKAN
		}
		definition.WriteString(targetType) // targetType sudah termasuk modifier

		// Tambahkan sintaks AUTO_INCREMENT / IDENTITY
		if col.AutoIncrement && col.IsPrimary {
			// Memanggil helper yang ada di file ini atau di schema_ddl_utils.go
			if autoIncSyntax := s.getAutoIncrementSyntax(targetType); autoIncSyntax != "" {
				definition.WriteString(" ")
				definition.WriteString(autoIncSyntax)
			}
		}

		// Tambahkan DEFAULT value jika relevan
		if col.DefaultValue.Valid && col.DefaultValue.String != "" && !col.AutoIncrement {
			// Periksa menggunakan nilai default yang sudah dinormalisasi
			// `normalizeDefaultValue` dari `compare_utils.go`
			normalizedDefault := normalizeDefaultValue(col.DefaultValue.String, s.dstDialect) // Gunakan dialek tujuan untuk normalisasi
			// `isDefaultNullOrFunction` dari `compare_utils.go`
			if !isDefaultNullOrFunction(normalizedDefault) { // Hanya tambahkan DEFAULT jika itu literal
				definition.WriteString(" DEFAULT ")
				// `formatDefaultValue` akan menangani quoting
				definition.WriteString(s.formatDefaultValue(col.DefaultValue.String, targetType))
			} else {
				log.Debug("Skipping explicit DEFAULT clause because the value is NULL or a DB function.", zap.String("normalized_default", normalizedDefault))
			}
		}
	}

	// Tambahkan NOT NULL
	if !col.IsNullable {
		definition.WriteString(" NOT NULL")
	}

	// Tambahkan Collation jika ada dan relevan
	// Helper `isStringType` dan `normalizeTypeName` dari `compare_utils.go`
	if col.Collation.Valid && col.Collation.String != "" && isStringType(normalizeTypeName(targetType)) {
		// Helper `getCollationSyntax` dari file ini atau schema_ddl_utils.go
		if collationSyntax := s.getCollationSyntax(col.Collation.String); collationSyntax != "" {
			definition.WriteString(" ")
			definition.WriteString(collationSyntax)
		}
	}

	// Tambahkan Comment jika ada dan didukung inline
	if col.Comment.Valid && col.Comment.String != "" {
		// Helper `getCommentSyntax` dari file ini atau schema_ddl_utils.go
		if commentSyntax := s.getCommentSyntax(col.Comment.String); commentSyntax != "" {
			definition.WriteString(" ")
			definition.WriteString(commentSyntax)
		} else if s.dstDialect != "mysql" {
			log.Debug("Column comment found, but adding it inline is not supported for this dialect. Use COMMENT ON statement if needed.", zap.String("comment", col.Comment.String))
		}
	}

	return definition.String(), nil
}

// generateCreateIndexDDLs generates CREATE INDEX statements.
// Ini adalah method dari SchemaSyncer (s).
func (s *SchemaSyncer) generateCreateIndexDDLs(table string, indexes []IndexInfo) []string {
	ddls := make([]string, 0, len(indexes))
	log := s.logger.With(zap.String("table", table), zap.String("action", "CREATE INDEX"), zap.String("dst_dialect", s.dstDialect))
	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)

	for _, idx := range indexes {
		if idx.IsPrimary {
			log.Debug("Skipping CREATE INDEX DDL generation for PRIMARY KEY index.", zap.String("index", idx.Name))
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
			log.Debug("MySQL does not support IF NOT EXISTS for CREATE INDEX.", zap.String("index", idx.Name))
		}

		// TODO: Handle idx.IndexType untuk klausa USING di PostgreSQL/MySQL
		// usingClause := ""
		// if s.dstDialect == "postgres" && idx.IndexType != "" && strings.ToUpper(idx.IndexType) != "BTREE" { // Asumsi BTREE default
		//    usingClause = fmt.Sprintf(" USING %s", strings.ToUpper(idx.IndexType))
		// }
		// ddl := fmt.Sprintf("CREATE %sINDEX %s%s ON %s%s (%s);", ...)

		ddl := fmt.Sprintf("CREATE %sINDEX %s%s ON %s (%s);",
			uniqueKeyword, ifNotExistsKeyword, quotedIndexName, quotedTable, strings.Join(quotedCols, ", "))

		log.Debug("Generated CREATE INDEX DDL", zap.String("ddl", ddl))
		ddls = append(ddls, ddl)
	}
	return ddls
}

// generateAddConstraintDDLs generates ADD CONSTRAINT statements (for UNIQUE, FK, CHECK).
// Ini adalah method dari SchemaSyncer (s).
func (s *SchemaSyncer) generateAddConstraintDDLs(table string, constraints []ConstraintInfo) []string {
	ddls := make([]string, 0, len(constraints))
	log := s.logger.With(zap.String("table", table), zap.String("action", "ADD CONSTRAINT"), zap.String("dst_dialect", s.dstDialect))
	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)

	for _, c := range constraints {
		if c.Type == "PRIMARY KEY" {
			log.Debug("Skipping ADD CONSTRAINT DDL generation for PRIMARY KEY.", zap.String("constraint", c.Name))
			continue
		}
		if len(c.Columns) == 0 && c.Type != "CHECK" {
			log.Warn("Skipping ADD CONSTRAINT DDL generation due to empty column list for non-CHECK constraint.",
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
				log.Warn("Skipping ADD FOREIGN KEY due to missing info.", zap.String("constraint", c.Name))
				continue
			}
			if len(quotedCols) != len(c.ForeignColumns) {
				log.Warn("Skipping ADD FOREIGN KEY due to column count mismatch.", zap.String("constraint", c.Name))
				continue
			}

			quotedForeignTable := utils.QuoteIdentifier(c.ForeignTable, s.dstDialect)
			quotedForeignCols := make([]string, len(c.ForeignColumns))
			for i, fcolName := range c.ForeignColumns {
				quotedForeignCols[i] = utils.QuoteIdentifier(fcolName, s.dstDialect)
			}

			// Helper `normalizeFKAction` dari `compare_utils.go`
			onDeleteAction := normalizeFKAction(c.OnDelete)
			onUpdateAction := normalizeFKAction(c.OnUpdate)
			fkActions := ""
			if onDeleteAction != "NO ACTION" && onDeleteAction != "" { fkActions += " ON DELETE " + onDeleteAction }
			if onUpdateAction != "NO ACTION" && onUpdateAction != "" { fkActions += " ON UPDATE " + onUpdateAction }

			deferrableClause := ""
			if s.dstDialect == "postgres" {
				// Membuat FK deferrable di PG untuk memungkinkan data load yang mungkin melanggar FK sementara
				deferrableClause = " DEFERRABLE INITIALLY DEFERRED"
				log.Debug("Adding DEFERRABLE INITIALLY DEFERRED for PostgreSQL FOREIGN KEY constraint.", zap.String("constraint", c.Name))
			}

			ddl = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)%s%s;",
				quotedTable, quotedConstraintName, strings.Join(quotedCols, ", "),
				quotedForeignTable, strings.Join(quotedForeignCols, ", "), fkActions, deferrableClause)

		case "CHECK":
			if c.Definition != "" {
				// Menggunakan definisi CHECK apa adanya. Normalisasi/validasi lanjutan bisa ditambahkan.
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

// getAutoIncrementSyntax mengembalikan sintaks AUTO_INCREMENT/IDENTITY.
func (s *SchemaSyncer) getAutoIncrementSyntax(mappedDestType string) string {
	normMappedDestType := normalizeTypeName(mappedDestType)
	switch s.dstDialect {
	case "mysql":
		if isIntegerType(normMappedDestType) { return "AUTO_INCREMENT" }
	case "postgres":
		if normMappedDestType == "smallint" { return "GENERATED BY DEFAULT AS IDENTITY" }
		if normMappedDestType == "int" { return "GENERATED BY DEFAULT AS IDENTITY" }
		if normMappedDestType == "bigint" { return "GENERATED BY DEFAULT AS IDENTITY" }
	case "sqlite":
		return "" // Ditangani oleh INTEGER PRIMARY KEY
	}
	return ""
}

// formatDefaultValue memformat nilai default untuk DDL.
func (s *SchemaSyncer) formatDefaultValue(value, mappedDataType string) string {
	normType := normalizeTypeName(mappedDataType)
	normValue := normalizeDefaultValue(value, s.dstDialect) // Normalisasi dengan dialek tujuan

	// `isDefaultNullOrFunction` sudah dicek oleh pemanggil (`mapColumnDefinition`)
	// Jadi, kita hanya perlu menangani formatting nilai literal di sini.

	// Boolean
	if normType == "bool" {
		if normValue == "1" { return "TRUE" }
		if normValue == "0" { return "FALSE" }
	}
	if s.dstDialect == "mysql" && mappedDataType == "tinyint(1)" {
		if normValue == "1" { return "'1'" } // MySQL booleans as tinyint(1) often store '0' or '1' as strings
		if normValue == "0" { return "'0'" }
	}

	// Numerik
	if isNumericType(normType) {
		if _, err := strconv.ParseFloat(value, 64); err == nil {
			return value // Kembalikan angka apa adanya (tanpa quote)
		}
		// Jika bukan angka valid setelah normalisasi (misal, fungsi yg tidak dikenal lolos filter isDefaultNullOrFunction)
		s.logger.Warn("Attempting to format a non-standard numeric default. Quoting as string.",
			zap.String("value", value), zap.String("normalized", normValue), zap.String("type", mappedDataType))
	}

	// Default: quote sebagai string literal
	escapedValue := strings.ReplaceAll(value, "'", "''") // Basic escaping untuk single quote
	return fmt.Sprintf("'%s'", escapedValue)
}

// getCollationSyntax mengembalikan klausa COLLATE.
func (s *SchemaSyncer) getCollationSyntax(collationName string) string {
	if collationName == "" { return "" }
	switch s.dstDialect {
	case "mysql": return fmt.Sprintf("COLLATE %s", utils.QuoteIdentifier(collationName, s.dstDialect))
	case "postgres": return fmt.Sprintf("COLLATE %s", utils.QuoteIdentifier(collationName, s.dstDialect)) // PG juga bisa meng-quote nama collation
	case "sqlite": return fmt.Sprintf("COLLATE %s", collationName) // SQLite tidak meng-quote nama collation
	default: return ""
	}
}

// getCommentSyntax mengembalikan sintaks COMMENT inline (hanya MySQL).
func (s *SchemaSyncer) getCommentSyntax(comment string) string {
	if comment == "" { return "" }
	if s.dstDialect == "mysql" {
		// Escape single quotes and backslashes for MySQL COMMENT
		escapedComment := strings.ReplaceAll(comment, "\\", "\\\\")
		escapedComment = strings.ReplaceAll(escapedComment, "'", "''")
		return fmt.Sprintf("COMMENT '%s'", escapedComment)
	}
	return "" // Dialek lain biasanya menggunakan statement `COMMENT ON COLUMN` terpisah
}
