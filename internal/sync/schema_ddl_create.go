package sync

import (
	"fmt"
	"regexp"
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

	primaryKeyColumnNames := make([]string, 0) // Nama PK (unquoted) untuk referensi internal
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

		// *** PERUBAHAN DI SINI ***
		// mapColumnDefinition sekarang mengembalikan nama ter-quote dan definisi tipe+atribut secara terpisah.
		quotedColName, typeAndAttrs, err := s.mapColumnDefinition(col)
		if err != nil {
			return "", nil, fmt.Errorf("failed to generate column definition for CREATE TABLE '%s', column '%s': %w", table, col.Name, err)
		}
		columnDefs[i] = fmt.Sprintf("  %s %s", quotedColName, typeAndAttrs) // Gabungkan untuk DDL

		if col.IsPrimary {
			primaryKeyColumnNames = append(primaryKeyColumnNames, col.Name) // Tetap simpan nama unquoted
		}
	}

	builder.WriteString(strings.Join(columnDefs, ",\n"))

	if len(primaryKeyColumnNames) > 0 {
		// Urutkan nama PK unquoted untuk konsistensi definisi PK
		sort.Strings(primaryKeyColumnNames)
		quotedPKs := make([]string, len(primaryKeyColumnNames))
		for i, pkName := range primaryKeyColumnNames {
			quotedPKs[i] = utils.QuoteIdentifier(pkName, s.dstDialect) // Quote nama PK untuk DDL
		}
		builder.WriteString(",\n  PRIMARY KEY (")
		builder.WriteString(strings.Join(quotedPKs, ", "))
		builder.WriteString(")")
	}

	builder.WriteString("\n);")

	log.Debug("Generated CREATE TABLE DDL successfully.")
	return builder.String(), primaryKeyColumnNames, nil // Kembalikan nama PK unquoted
}

// mapColumnDefinition menghasilkan nama kolom yang sudah di-quote,
// dan string definisi tipe+atribut untuk CREATE TABLE atau ADD COLUMN.
// *** SIGNATURE DAN IMPLEMENTASI DIUBAH ***
func (s *SchemaSyncer) mapColumnDefinition(col ColumnInfo) (quotedName string, typeAndAttributes string, err error) {
	var typeAttrBuilder strings.Builder
	log := s.logger.With(zap.String("column", col.Name),
		zap.String("src_type_raw", col.Type),
		zap.String("mapped_type_target", col.MappedType),
		zap.String("dst_dialect", s.dstDialect),
		zap.String("action", "mapColumnDefinition"))

	quotedName = utils.QuoteIdentifier(col.Name, s.dstDialect) // Nama kolom di-quote di awal

	targetType := col.MappedType
	if col.IsGenerated {
		// Untuk kolom generated, definisi tipe biasanya sudah mencakup ekspresi generated
		// dan 'GENERATED ALWAYS AS (...) STORED/VIRTUAL'.
		// col.Type (tipe sumber asli) mungkin lebih cocok di sini jika sudah berisi sintaks GENERATED AS.
		// Atau, jika MappedType sudah di-resolve ke tipe dasar, kita perlu membangun ulang sintaks generated.
		// Untuk saat ini, kita asumsikan col.Type untuk generated column sudah mengandung sintaks yang benar
		// atau setidaknya lebih dekat daripada MappedType (yang mungkin hanya tipe dasar).
		// Ini area yang bisa disempurnakan jika parsing ekspresi generated diperlukan.
		if strings.Contains(strings.ToUpper(col.Type), "GENERATED") || strings.Contains(strings.ToUpper(col.Type), " AS ") {
			typeAttrBuilder.WriteString(col.Type) // Gunakan tipe asli jika ada keyword generated
			log.Debug("Using original source type for GENERATED column definition as it may contain expression.", zap.String("original_type_used", col.Type))
		} else {
			// Jika tipe sumber tidak mengandung keyword generated, tapi MappedType ada (tipe dasar)
			// dan IsGenerated true, ini situasi aneh. Log warning.
			// Seharusnya SchemaSyncer memastikan ini konsisten.
			if targetType == "" {
				log.Error("MappedType is empty for generated column where source type also lacks GENERATED keyword. Using source type.", zap.String("original_type", col.Type))
				typeAttrBuilder.WriteString(col.Type)
			} else {
				log.Warn("Generated column source type does not contain GENERATED keyword. Using MappedType and hoping for the best. Review DDL.", zap.String("mapped_type", targetType))
				typeAttrBuilder.WriteString(targetType)
				// Di sini kita TIDAK menambahkan atribut seperti DEFAULT, NULL, AUTO_INCREMENT
				// karena kolom generated memiliki aturannya sendiri.
			}
		}
	} else { // Kolom non-generated
		if targetType == "" {
			log.Error("MappedType is missing for non-generated column.")
			return "", "", fmt.Errorf("mappedType is missing for non-generated column '%s'", col.Name)
		}
		typeAttrBuilder.WriteString(targetType)

		if col.AutoIncrement && col.IsPrimary { // AutoIncrement hanya relevan jika juga PK
			if autoIncSyntax := s.getAutoIncrementSyntax(targetType); autoIncSyntax != "" {
				typeAttrBuilder.WriteString(" ")
				typeAttrBuilder.WriteString(autoIncSyntax)
			}
		}

		// Default value hanya jika tidak auto-increment dan tidak generated
		if col.DefaultValue.Valid && col.DefaultValue.String != "" && !col.AutoIncrement && !col.IsGenerated {
			normalizedDefault := normalizeDefaultValue(col.DefaultValue.String, s.srcDialect) // Normalisasi dari DIALEK SUMBER untuk default
			if !isDefaultNullOrFunction(normalizedDefault) {
				typeAttrBuilder.WriteString(" DEFAULT ")
				// Format default value untuk DIALEK TUJUAN
				typeAttrBuilder.WriteString(s.formatDefaultValue(col.DefaultValue.String, targetType))
			} else {
				log.Debug("Skipping explicit DEFAULT clause for NULL or DB function.", zap.String("normalized_default", normalizedDefault))
			}
		}
	}

	// NOT NULL berlaku untuk generated dan non-generated, kecuali jika IsNullable true
	if !col.IsNullable {
		typeAttrBuilder.WriteString(" NOT NULL")
	}

	// Collation dan Comment hanya untuk kolom non-generated
	if !col.IsGenerated {
		if col.Collation.Valid && col.Collation.String != "" && isStringType(normalizeTypeName(targetType)) {
			if collationSyntax := s.getCollationSyntax(col.Collation.String); collationSyntax != "" {
				typeAttrBuilder.WriteString(" ")
				typeAttrBuilder.WriteString(collationSyntax)
			}
		}

		if col.Comment.Valid && col.Comment.String != "" {
			if commentSyntax := s.getCommentSyntax(col.Comment.String); commentSyntax != "" {
				typeAttrBuilder.WriteString(" ")
				typeAttrBuilder.WriteString(commentSyntax)
			} else if s.dstDialect != "mysql" && s.dstDialect != "postgres" { // PG bisa COMMENT ON
				log.Debug("Column comment found, but adding it inline is not supported for this dialect. Use COMMENT ON statement if needed.", zap.String("comment", col.Comment.String))
			}
		}
	}

	typeAndAttributes = strings.TrimSpace(typeAttrBuilder.String())
	return // quotedName, typeAndAttributes, nil
}

// generateCreateIndexDDLs (tetap sama)
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

		uniqueKeyword := ""
		if idx.IsUnique {
			uniqueKeyword = "UNIQUE "
		}
		ifNotExistsKeyword := ""
		if s.dstDialect == "postgres" || s.dstDialect == "sqlite" {
			ifNotExistsKeyword = "IF NOT EXISTS "
		} else if s.dstDialect == "mysql" {
			log.Debug("MySQL does not directly support 'IF NOT EXISTS' for CREATE INDEX. Error 1061 (Duplicate key name) will be ignored if index already exists.", zap.String("index", idx.Name))
		}

		usingClause := ""
		if idx.RawDef != "" && s.dstDialect == "postgres" {
			reUsing := regexp.MustCompile(`USING\s+([a-zA-Z0-9_]+)`)
			matches := reUsing.FindStringSubmatch(strings.ToUpper(idx.RawDef))
			if len(matches) > 1 && strings.ToUpper(matches[1]) != "BTREE" { // BTREE adalah default, tidak perlu eksplisit
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

// generateAddConstraintDDLs (tetap sama)
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

			onDeleteAction := normalizeFKAction(c.OnDelete)
			onUpdateAction := normalizeFKAction(c.OnUpdate)
			fkActions := ""
			if onDeleteAction != "NO ACTION" && onDeleteAction != "" {
				fkActions += " ON DELETE " + onDeleteAction
			}
			if onUpdateAction != "NO ACTION" && onUpdateAction != "" {
				fkActions += " ON UPDATE " + onUpdateAction
			}

			deferrableClause := ""
			if s.dstDialect == "postgres" {
				// Cek apakah constraint didefinisikan sebagai DEFERRABLE dari sumber
				// Untuk sementara, kita buat semua FK baru di PG sebagai DEFERRABLE INITIALLY DEFERRED
				// jika ini perilaku yang diinginkan.
				// Jika ingin lebih presisi, `ConstraintInfo` perlu field `IsDeferrable` dan `IsInitiallyDeferred`.
				deferrableClause = " DEFERRABLE INITIALLY DEFERRED"
				log.Debug("Generating PostgreSQL FOREIGN KEY constraint as DEFERRABLE INITIALLY DEFERRED by default.", zap.String("constraint", c.Name))
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

// --- Helper untuk DDL Create/Modify (getAutoIncrementSyntax, formatDefaultValue, getCollationSyntax, getCommentSyntax tetap sama) ---
func (s *SchemaSyncer) getAutoIncrementSyntax(mappedDestType string) string {
	normMappedDestType := normalizeTypeName(mappedDestType)
	switch s.dstDialect {
	case "mysql":
		if isIntegerType(normMappedDestType) {
			return "AUTO_INCREMENT"
		}
	case "postgres":
		switch normMappedDestType {
		case "smallint", "int", "bigint": // Untuk tipe integer standar
			return "GENERATED BY DEFAULT AS IDENTITY"
		case "serial", "smallserial", "bigserial": // Tipe serial PG sudah auto-increment by definition
			return ""
		}
	case "sqlite":
		// Di SQLite, INTEGER PRIMARY KEY adalah auto-increment by default (alias ROWID).
		// Menambahkan AUTOINCREMENT keyword memberlakukan aturan stricter (ID tidak di-reuse).
		// Untuk CREATE TABLE, kita biarkan GORM atau definisi dasar yang menentukannya.
		// Jika kita ingin eksplisit, ini bisa jadi "AUTOINCREMENT", tapi hanya jika `INTEGER PRIMARY KEY`.
		if normMappedDestType == "integer" { // Hanya untuk tipe dasar INTEGER
			return "" // Atau "AUTOINCREMENT" jika perilaku strict diinginkan & itu PK
		}
	}
	return ""
}

func (s *SchemaSyncer) formatDefaultValue(value, mappedDataType string) string {
	normType := normalizeTypeName(mappedDataType)
	// Penanganan khusus untuk boolean
	if (s.dstDialect == "mysql" && (normType == "tinyint" && strings.Contains(mappedDataType, "(1)"))) ||
		(s.dstDialect == "postgres" && normType == "bool") ||
		(s.dstDialect == "sqlite" && normType == "boolean") { // SQLite boolean sering INT 0/1

		normVal := normalizeDefaultValue(value, s.srcDialect) // Normalisasi dari nilai sumber
		if normVal == "1" {                                   // "1" adalah hasil normalisasi untuk true-ish values
			if s.dstDialect == "postgres" {
				return "TRUE"
			}
			if s.dstDialect == "sqlite" {
				return "1"
			} // SQLite boolean biasanya 1
			return "'1'" // MySQL tinyint(1) defaultnya perlu di-quote jika diset sebagai string
		}
		if normVal == "0" { // "0" adalah hasil normalisasi untuk false-ish values
			if s.dstDialect == "postgres" {
				return "FALSE"
			}
			if s.dstDialect == "sqlite" {
				return "0"
			} // SQLite boolean biasanya 0
			return "'0'" // MySQL tinyint(1)
		}
	}

	// Untuk tipe numerik, jika `value` adalah angka valid, jangan di-quote.
	if isNumericType(normType) {
		// Cek apakah value adalah fungsi DB (misal dari normalisasi)
		// Jika sumbernya adalah literal numerik, tapi di-quote ('123'), normalisasi akan menghapus quote.
		// Di sini kita ingin memastikan default numerik tidak di-quote di DDL tujuan.
		valToCheck := normalizeDefaultValue(value, s.srcDialect) // Normalisasi lagi dari nilai asli sumber
		if !isDefaultNullOrFunction(valToCheck) {                // Bukan NULL atau fungsi
			if _, err := strconv.ParseFloat(valToCheck, 64); err == nil {
				// Jika value bisa di-parse sebagai float setelah normalisasi (menghapus quote),
				// gunakan valToCheck (yang sudah tanpa quote).
				// Pengecualian: jika DDL sumbernya adalah CURRENT_TIMESTAMP untuk kolom numerik (tidak umum, tapi mungkin).
				if s.dstDialect == "mysql" && (strings.Contains(strings.ToUpper(value), "CURRENT_TIMESTAMP") || strings.Contains(strings.ToUpper(value), "NOW()")) {
					return value // Biarkan fungsi DB apa adanya jika itu nilai asli sumber
				}
				return valToCheck
			}
		}
		// Jika valToCheck adalah fungsi (mis. CURRENT_TIMESTAMP), atau tidak bisa di-parse sbg float,
		// biarkan jatuh ke logika quoting di bawah, yang mungkin benar untuk fungsi atau string default yang kompleks.
	}

	// Untuk tipe bit/varbit PostgreSQL
	if s.dstDialect == "postgres" && (normType == "bit" || normType == "varbit") {
		// Jika value sudah dalam format B'...' atau X'...', biarkan.
		if (strings.HasPrefix(value, "B'") || strings.HasPrefix(value, "b'") || strings.HasPrefix(value, "X'") || strings.HasPrefix(value, "x'")) && strings.HasSuffix(value, "'") {
			return value
		}
		// Jika tidak, dan itu adalah string biner 0101, coba format ke B'...'
		if regexp.MustCompile("^[01]+$").MatchString(value) {
			return fmt.Sprintf("B'%s'", value)
		}
	}

	// Default: escape single quotes dan apit dengan single quotes
	escapedValue := strings.ReplaceAll(value, "'", "''")
	return fmt.Sprintf("'%s'", escapedValue)
}

func (s *SchemaSyncer) getCollationSyntax(collationName string) string {
	if collationName == "" {
		return ""
	}
	// Hanya terapkan collation jika dialek sumber dan tujuan sama,
	// karena nama collation tidak portabel.
	if s.srcDialect != s.dstDialect {
		s.logger.Debug("Skipping explicit COLLATE clause due to cross-dialect sync. Collation names are not portable.",
			zap.String("source_collation", collationName),
			zap.String("src_dialect", s.srcDialect),
			zap.String("dst_dialect", s.dstDialect))
		return ""
	}
	switch s.dstDialect {
	case "mysql", "postgres": // PostgreSQL juga menggunakan COLLATE
		return fmt.Sprintf("COLLATE %s", utils.QuoteIdentifier(collationName, s.dstDialect))
	case "sqlite":
		// SQLite COLLATE clause adalah bagian dari definisi kolom atau ekspresi,
		// bukan modifier terpisah seperti di MySQL/PG untuk CREATE TABLE.
		// Namun, untuk `CREATE TABLE ... (... COLLATE name)` didukung.
		return fmt.Sprintf("COLLATE %s", collationName) // SQLite tidak quote nama collation
	default:
		return ""
	}
}

func (s *SchemaSyncer) getCommentSyntax(comment string) string {
	if comment == "" {
		return ""
	}
	if s.dstDialect == "mysql" {
		// MySQL mendukung COMMENT inline
		escapedComment := strings.ReplaceAll(comment, "\\", "\\\\")    // Escape backslash
		escapedComment = strings.ReplaceAll(escapedComment, "'", "''") // Escape single quote
		return fmt.Sprintf("COMMENT '%s'", escapedComment)
	}
	// PostgreSQL dan SQLite memerlukan statement COMMENT ON terpisah, jadi tidak di-handle di sini.
	// Fungsi DDL generation untuk COMMENT ON akan berbeda.
	return ""
}
