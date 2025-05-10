package sync

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/arwahdevops/dbsync/internal/config"
)

// normalizeTypeName (tetap sama seperti sebelumnya)
func normalizeTypeName(typeName string) string {
	name := strings.ToLower(strings.TrimSpace(typeName))
	name = strings.ReplaceAll(name, " unsigned", "")
	name = strings.ReplaceAll(name, " zerofill", "")
	name = strings.TrimSpace(name)
	name = regexp.MustCompile(`\s*\([^)]*\)\s*$`).ReplaceAllString(name, "")
	name = regexp.MustCompile(`\s*\([^)]*\)$`).ReplaceAllString(name, "")
	name = strings.TrimSpace(name)
	aliases := map[string]string{
		"character varying":         "varchar",
		"double precision":          "double",
		"boolean":                   "bool",
		"timestamp with time zone":    "timestamptz",
		"timestamp without time zone": "timestamp",
		"time with time zone":         "timetz",
		"time without time zone":      "time",
		"integer":                   "int",
		"int4":                      "int",
		"int8":                      "bigint",
		"serial4":                   "serial",
		"serial8":                   "bigserial",
	}
	if mapped, ok := aliases[name]; ok {
		name = mapped
	}
	return strings.TrimSpace(name)
}

// isStringType, isBinaryType, isNumericType, isIntegerType, isPrecisionRelevant, isScaleRelevant (tetap sama)
func isStringType(normTypeName string) bool {
	return strings.Contains(normTypeName, "char") ||
		strings.Contains(normTypeName, "text") ||
		strings.Contains(normTypeName, "clob") ||
		normTypeName == "enum" ||
		normTypeName == "set" ||
		normTypeName == "uuid" ||
		normTypeName == "json" ||
		normTypeName == "xml"
}
func isBinaryType(normTypeName string) bool {
	return strings.Contains(normTypeName, "binary") ||
		strings.Contains(normTypeName, "blob") ||
		normTypeName == "bytea"
}
func isNumericType(normTypeName string) bool {
	return isIntegerType(normTypeName) ||
		strings.Contains(normTypeName, "decimal") ||
		strings.Contains(normTypeName, "numeric") ||
		strings.Contains(normTypeName, "float") ||
		strings.Contains(normTypeName, "double") ||
		strings.Contains(normTypeName, "real") ||
		strings.Contains(normTypeName, "money")
}
func isIntegerType(normTypeName string) bool {
	return strings.Contains(normTypeName, "int") ||
		strings.Contains(normTypeName, "serial")
}
func isPrecisionRelevant(normTypeName string) bool {
	return strings.Contains(normTypeName, "decimal") ||
		strings.Contains(normTypeName, "numeric") ||
		strings.Contains(normTypeName, "time") ||
		(strings.Contains(normTypeName, "datetime") && strings.Contains(normTypeName, "offset")) // Biasanya DATETIMEOFFSET (SQL Server)
}
func isScaleRelevant(normTypeName string) bool {
	return strings.Contains(normTypeName, "decimal") ||
		strings.Contains(normTypeName, "numeric")
}


// populateMappedTypesForSourceColumns (tetap sama)
func (s *SchemaSyncer) populateMappedTypesForSourceColumns(columns []ColumnInfo, tableName string) error {
	log := s.logger.With(zap.String("table", tableName), zap.String("phase", "populate-source-mapped-types"))
	for i := range columns {
		if columns[i].IsGenerated {
			baseSrcTypeForGenerated := extractBaseTypeFromGenerated(columns[i].Type, log)
			columns[i].MappedType = baseSrcTypeForGenerated
			log.Debug("Using extracted base type as MappedType for generated source column, for comparison purposes.",
				zap.String("column", columns[i].Name),
				zap.String("original_full_type", columns[i].Type),
				zap.String("mapped_type_for_comp", columns[i].MappedType))
			continue
		}
		mapped, mapErr := s.mapDataType(columns[i].Type)
		if mapErr != nil {
			log.Error("Fatal type mapping error for source column",
				zap.String("column", columns[i].Name), zap.String("original_type", columns[i].Type), zap.Error(mapErr))
			return fmt.Errorf("fatal type mapping error for column '%s' in table '%s': %w", columns[i].Name, tableName, mapErr)
		}
		columns[i].MappedType = mapped
		log.Debug("Populated MappedType for source column", zap.String("column", columns[i].Name), zap.String("original_type", columns[i].Type), zap.String("mapped_type", columns[i].MappedType))
	}
	return nil
}

// populateMappedTypesForDestinationColumns (tetap sama)
func (s *SchemaSyncer) populateMappedTypesForDestinationColumns(columns []ColumnInfo) {
	for i := range columns {
		if columns[i].MappedType == "" {
			if columns[i].IsGenerated {
				columns[i].MappedType = extractBaseTypeFromGenerated(columns[i].Type, s.logger)
			} else {
				columns[i].MappedType = columns[i].Type
			}
		}
	}
}

// mapDataType memetakan tipe data dari dialek sumber ke dialek tujuan.
// Disempurnakan dengan internal default mappings.
func (s *SchemaSyncer) mapDataType(srcType string) (string, error) {
	log := s.logger.With(zap.String("source_type_raw", srcType),
		zap.String("src_dialect", s.srcDialect), zap.String("dst_dialect", s.dstDialect),
		zap.String("action", "mapDataType"))

	fullSrcTypeLower := strings.ToLower(strings.TrimSpace(srcType))
	normalizedSrcTypeKey := normalizeTypeName(fullSrcTypeLower)

	// 1. Coba mapping dari file JSON kustom (jika ada)
	typeMappingConfigEntry := config.GetTypeMappingForDialects(s.srcDialect, s.dstDialect)
	if typeMappingConfigEntry != nil {
		log.Debug("Attempting to map type using external JSON configuration.")
		// Cek special_mappings dulu (berbasis regex)
		for _, sm := range typeMappingConfigEntry.SpecialMappings {
			re, errComp := regexp.Compile(sm.SourceTypePattern)
			if errComp != nil {
				log.Error("Invalid regex pattern in special type mapping, skipping this pattern.",
					zap.String("pattern", sm.SourceTypePattern), zap.Error(errComp))
				continue
			}
			if re.MatchString(fullSrcTypeLower) {
				log.Info("Matched special type mapping from JSON config.",
					zap.String("pattern", sm.SourceTypePattern),
					zap.String("raw_source_type", fullSrcTypeLower),
					zap.String("target_base_type", sm.TargetType))
				return s.applyTypeModifiers(srcType, sm.TargetType), nil
			}
		}
		// Cek mappings standar (berbasis normalized key)
		if mappedType, ok := typeMappingConfigEntry.Mappings[normalizedSrcTypeKey]; ok {
			log.Info("Matched standard type mapping from JSON config.",
				zap.String("normalized_source_key", normalizedSrcTypeKey),
				zap.String("raw_source_type", srcType),
				zap.String("target_base_type", mappedType))
			return s.applyTypeModifiers(srcType, mappedType), nil
		}
		log.Debug("No match found in external JSON configuration for this type.")
	} else {
		log.Debug("No external type mapping JSON configuration found for current dialect pair.")
	}

	// 2. Jika dialek sumber dan tujuan sama, gunakan tipe sumber apa adanya (dengan modifier)
	if s.srcDialect == s.dstDialect {
		log.Info("Source and destination dialects are the same. Using source type directly (with modifiers applied).",
			zap.String("source_type", srcType))
		return s.applyTypeModifiers(srcType, srcType), nil
	}

	// 3. *** BARU: Coba internal default mappings untuk pasangan dialek umum ***
	internalMappedType := s.getInternalDefaultMapping(normalizedSrcTypeKey, fullSrcTypeLower)
	if internalMappedType != "" {
		log.Info("Matched internal default type mapping.",
			zap.String("normalized_source_key_or_full", normalizedSrcTypeKey),
			zap.String("raw_source_type", srcType),
			zap.String("target_base_type", internalMappedType))
		return s.applyTypeModifiers(srcType, internalMappedType), nil
	}

	// 4. Jika tidak ada mapping (eksternal, sama dialek, atau internal default), gunakan fallback generik
	fallbackType := s.getGenericFallbackType()
	log.Warn("No specific type mapping found (external, same-dialect, or internal default). Using generic fallback type.",
		zap.String("normalized_source_key_used_for_lookup", normalizedSrcTypeKey),
		zap.String("original_source_type", srcType),
		zap.String("fallback_target_type", fallbackType))
	return s.applyTypeModifiers(srcType, fallbackType), nil
}

// getInternalDefaultMapping menyediakan pemetaan tipe default internal jika tidak ada di JSON.
func (s *SchemaSyncer) getInternalDefaultMapping(normalizedSrcKey, fullSrcTypeLower string) string {
	// MySQL to PostgreSQL Internal Defaults
	if s.srcDialect == "mysql" && s.dstDialect == "postgres" {
		// Handle special cases first (regex-like, tapi di sini hardcode untuk kesederhanaan)
		if normalizedSrcKey == "tinyint" && strings.HasPrefix(fullSrcTypeLower, "tinyint(1)") {
			return "BOOLEAN" // tinyint(1) -> BOOLEAN (ini juga ada di typemap.json contoh)
		}
		// Standar mappings
		switch normalizedSrcKey {
		case "bit": return "VARBIT" // Atau BIT, VARBIT lebih fleksibel
		case "tinyint": return "SMALLINT" // Selain tinyint(1)
		case "smallint": return "SMALLINT"
		case "mediumint": return "INTEGER"
		case "int", "integer": return "INTEGER"
		case "bigint": return "BIGINT"
		case "float": return "REAL"
		case "double": return "DOUBLE PRECISION"
		case "decimal", "numeric": return "NUMERIC"
		case "char": return "CHAR" // Dengan asumsi panjang akan ditambahkan oleh applyTypeModifiers
		case "varchar": return "VARCHAR"
		case "tinytext", "text", "mediumtext", "longtext": return "TEXT"
		case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob": return "BYTEA"
		case "json": return "JSONB" // JSONB umumnya lebih disukai di PG
		case "enum": return "VARCHAR" // Fallback aman untuk enum, panjang 255 bisa dipertimbangkan
		case "set": return "TEXT"      // SET bisa direpresentasikan sebagai CSV text
		case "date": return "DATE"
		case "time": return "TIME WITHOUT TIME ZONE"
		case "datetime": return "TIMESTAMP WITHOUT TIME ZONE"
		case "timestamp": return "TIMESTAMP WITH TIME ZONE" // MySQL TIMESTAMP punya info TZ implisit sesi
		case "year": return "SMALLINT"
		}
	}

	// PostgreSQL to MySQL Internal Defaults
	if s.srcDialect == "postgres" && s.dstDialect == "mysql" {
		// Handle special cases (seperti array PG -> JSON di MySQL jika diinginkan)
		if strings.HasSuffix(normalizedSrcKey, "[]") { // array
			return "JSON" // Atau TEXT jika JSON tidak diinginkan
		}
		switch normalizedSrcKey {
		case "bool": return "TINYINT(1)"
		case "smallint", "int2": return "SMALLINT"
		case "int", "int4", "serial", "serial4": return "INT"
		case "bigint", "int8", "bigserial", "serial8": return "BIGINT"
		case "real", "float4": return "FLOAT"
		case "double", "float8": return "DOUBLE"
		case "numeric", "decimal": return "DECIMAL" // Modifier akan ditambahkan
		case "money": return "DECIMAL" // MySQL tidak punya tipe money, DECIMAL(19,2) umum
		case "char", "bpchar": return "CHAR"
		case "varchar": return "VARCHAR"
		case "text": return "LONGTEXT"
		case "bytea": return "LONGBLOB"
		case "json", "jsonb": return "JSON"
		case "uuid": return "CHAR(36)" // UUID standar
		case "date": return "DATE"
		case "time": return "TIME"
		case "timetz": return "TIME" // MySQL TIME tidak secara native menyimpan TZ, tapi bisa distore dengan konversi
		case "timestamp": return "DATETIME" // Atau TIMESTAMP jika presisi MySQL TIMESTAMP cukup
		case "timestamptz": return "TIMESTAMP" // MySQL TIMESTAMP menyimpan UTC dan konversi ke sesi TZ
		case "interval": return "VARCHAR(100)" // Representasi string untuk interval
		case "point": return "POINT"
		case "line", "lseg", "box", "path", "polygon", "circle": return "GEOMETRY" // Atau TEXT
		case "cidr", "inet": return "VARCHAR(43)"
		case "macaddr", "macaddr8": return "VARCHAR(23)"
		case "bit", "varbit": return "VARBINARY" // Atau BINARY jika panjang tetap
		}
	}

	// SQLite to PostgreSQL
	if s.srcDialect == "sqlite" && s.dstDialect == "postgres" {
		switch normalizedSrcKey {
		case "integer", "int": return "BIGINT" // SQLite INTEGER bisa sangat besar
		case "real": return "DOUBLE PRECISION"
		case "text", "varchar", "char": return "TEXT"
		case "blob": return "BYTEA"
		case "numeric", "decimal": return "NUMERIC"
		case "datetime", "timestamp": return "TIMESTAMP WITHOUT TIME ZONE"
		case "date": return "DATE"
		case "boolean": return "BOOLEAN"
		}
	}
	// SQLite to MySQL
	if s.srcDialect == "sqlite" && s.dstDialect == "mysql" {
		switch normalizedSrcKey {
		case "integer", "int": return "BIGINT"
		case "real": return "DOUBLE"
		case "text", "varchar", "char": return "LONGTEXT"
		case "blob": return "LONGBLOB"
		case "numeric", "decimal": return "DECIMAL" // Mungkin perlu presisi/skala default
		case "datetime", "timestamp": return "DATETIME(6)" // DATETIME dengan presisi
		case "date": return "DATE"
		case "boolean": return "TINYINT(1)"
		}
	}

	// Tambahkan pasangan dialek lain di sini jika perlu

	return "" // Tidak ada mapping internal yang cocok
}


// applyTypeModifiers (tetap sama)
func (s *SchemaSyncer) applyTypeModifiers(srcTypeRaw, mappedBaseType string) string {
	log := s.logger.With(
		zap.String("src_type_raw_for_modifier", srcTypeRaw),
		zap.String("mapped_base_type_for_modifier", mappedBaseType),
		zap.String("dst_dialect_for_modifier", s.dstDialect),
		zap.String("action", "applyTypeModifiers"),
	)

	re := regexp.MustCompile(`\((.+?)\)`)
	matches := re.FindStringSubmatch(srcTypeRaw)
	var modifierContent string
	if len(matches) > 1 {
		modifierContent = strings.TrimSpace(matches[1])
	}

	baseTypeWithoutExistingModifiers := strings.Split(mappedBaseType, "(")[0]
	normBaseType := normalizeTypeName(baseTypeWithoutExistingModifiers)

	if modifierContent == "" {
		// Jika tipe dasar tujuan sudah memiliki modifier (misalnya "VARCHAR(255)" dari mapping),
		// dan tipe sumber tidak memiliki modifier, gunakan modifier dari tipe dasar tujuan.
		if strings.Contains(mappedBaseType, "(") {
			log.Debug("No modifier in source type, but mapped base type has one. Using mapped base type with its modifier.", zap.String("final_type", mappedBaseType))
			return mappedBaseType
		}
		log.Debug("No modifier found in source type. Returning mapped base type as is.", zap.String("final_type", baseTypeWithoutExistingModifiers))
		return baseTypeWithoutExistingModifiers
	}

	// Jika mappedBaseType sudah mengandung modifier (misalnya, dari `typemap.json` "enum": "VARCHAR(255)"),
	// dan source type juga punya modifier (misal `ENUM('a','b')`), kita harus hati-hati.
	// Umumnya, modifier dari `typemap.json` lebih diutamakan untuk kasus seperti ENUM -> VARCHAR(X).
	// Namun, untuk tipe seperti `DECIMAL` dari sumber yang punya (P,S) dan target `NUMERIC` dari map,
	// kita ingin (P,S) dari sumber diterapkan.
	// Solusi: Jika mappedBaseType sudah punya modifier, DAN tipe sumber BUKAN tipe yang modifiernya kompleks (seperti enum),
	// maka kita coba ambil modifier dari sumber. Jika tipe sumber adalah ENUM/SET, modifier dari map lebih mungkin diinginkan.
	if strings.Contains(mappedBaseType, "(") {
		normSrcRawForCheck := normalizeTypeName(strings.Split(srcTypeRaw,"(")[0])
		if normSrcRawForCheck == "enum" || normSrcRawForCheck == "set" {
			log.Debug("Mapped base type already has modifier and source is ENUM/SET. Preferring modifier from mapped base type.",
				zap.String("mapped_base_with_modifier", mappedBaseType),
				zap.String("source_type", srcTypeRaw))
			return mappedBaseType // Gunakan mappedBaseType apa adanya
		}
		// Untuk tipe lain (non-enum/set), modifier dari sumber lebih diutamakan.
		// Jadi, kita lanjutkan untuk mengekstrak dan menerapkan modifier dari sumber.
		// baseTypeWithoutExistingModifiers sudah benar.
	}


	canHaveModifier := false
	finalModifier := modifierContent

	switch {
	case isStringType(normBaseType):
		if !isLargeTextOrBlob(normBaseType) &&
			normBaseType != "uuid" && normBaseType != "json" && normBaseType != "xml" &&
			normBaseType != "enum" && normBaseType != "set" { // ENUM/SET modifiernya berbeda
			canHaveModifier = true
		}
	case isBinaryType(normBaseType):
		if !isLargeTextOrBlob(normBaseType) && normBaseType != "bytea" {
			canHaveModifier = true
		}
	case isScaleRelevant(normBaseType): // decimal, numeric
		// Pastikan modifierContent adalah P,S atau P
		parts := strings.Split(modifierContent, ",")
		if len(parts) == 1 || len(parts) == 2 {
			if _, err1 := strconv.Atoi(strings.TrimSpace(parts[0])); err1 == nil {
				if len(parts) == 2 {
					if _, err2 := strconv.Atoi(strings.TrimSpace(parts[1])); err2 != nil {
						canHaveModifier = false // Skala tidak valid
					} else {
						canHaveModifier = true
					}
				} else { // Hanya presisi
					canHaveModifier = true
				}
			}
		}
	case isPrecisionRelevant(normBaseType) && (strings.Contains(normBaseType, "time") || strings.Contains(normBaseType, "timestamp")):
		parts := strings.Split(modifierContent, ",")
		if len(parts) == 1 { // Hanya presisi untuk waktu/timestamp
			if pVal, err := strconv.Atoi(strings.TrimSpace(parts[0])); err == nil {
				// Beberapa DB (seperti MySQL DATETIME(fsp)) punya batas presisi (0-6)
				// PG TIMESTAMP (p)
				if s.dstDialect == "mysql" && (normBaseType == "datetime" || normBaseType == "timestamp" || normBaseType == "time") {
					if pVal >= 0 && pVal <= 6 {
						finalModifier = strconv.Itoa(pVal)
						canHaveModifier = true
					} else {
						log.Debug("MySQL time/datetime/timestamp precision from source out of range (0-6), removing modifier.", zap.Int("source_precision", pVal))
						finalModifier = "" // Hapus modifier jika di luar jangkauan
						canHaveModifier = false // Atau false agar base type saja yang digunakan
					}
				} else if s.dstDialect == "postgres" && (normBaseType == "timestamp" || normBaseType == "timestamptz" || normBaseType == "time" || normBaseType == "timetz") {
					// PG (p) tanpa range eksplisit di sini, tapi umumnya 0-6 juga.
					// Biarkan DB yang validasi.
					finalModifier = strconv.Itoa(pVal)
					canHaveModifier = true
				} else { // Dialek lain atau tipe tidak cocok
					finalModifier = strings.TrimSpace(parts[0])
					canHaveModifier = true
				}
			}
		}
	}

	if canHaveModifier && finalModifier != "" {
		log.Debug("Applying modifier to base type.", zap.String("modifier_to_apply", finalModifier))
		return fmt.Sprintf("%s(%s)", baseTypeWithoutExistingModifiers, finalModifier)
	}

	log.Debug("Modifier found in source, but target base type does not typically support it, modifier format is incompatible, or modifier was removed due to range constraints; returning base type without modifier.",
		zap.String("modifier_content_from_source", modifierContent),
		zap.String("final_type", baseTypeWithoutExistingModifiers))
	return baseTypeWithoutExistingModifiers
}

// getGenericFallbackType (tetap sama)
func (s *SchemaSyncer) getGenericFallbackType() string {
	switch s.dstDialect {
	case "mysql": return "LONGTEXT"
	case "postgres": return "TEXT"
	case "sqlite": return "TEXT"
	default:
		s.logger.Error("Unknown destination dialect for generic fallback type, defaulting to TEXT.", zap.String("unknown_dialect", s.dstDialect))
		return "TEXT"
	}
}
