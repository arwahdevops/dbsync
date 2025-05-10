package sync

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/arwahdevops/dbsync/internal/config"
)

func normalizeTypeName(typeName string) string {
	name := strings.ToLower(strings.TrimSpace(typeName))

	// 1. Hapus modifier umum dialek (seperti unsigned) terlebih dahulu
	name = strings.ReplaceAll(name, " unsigned", "")
	name = strings.ReplaceAll(name, " zerofill", "")
	name = strings.TrimSpace(name)

	// 2. Regex untuk menghapus modifier numerik (L), (P,S) DARI AKHIR tipe dasar.
	//    Ini akan mengubah "varchar(255)" menjadi "varchar", "int(11)" menjadi "int".
	//    Tidak akan mengubah "geometry(point,4326)" atau "func()".
	//    Pola: sebuah kata tipe (\w+ atau kata-dengan-spasi), diikuti oleh modifier numerik di akhir.
	//    Kita coba beberapa pola dari yang paling spesifik ke yang lebih umum.

	// Pola 1: Menangkap tipe multi-kata yang diikuti modifier numerik
	// e.g., "timestamp with time zone (3)" -> "timestamp with time zone" (modifier (3) di sini akan jadi masalah dengan pendekatan ini)
	// Regex ini lebih baik untuk "bit varying (10)" -> "bit varying"
	reMultiWordTypeWithNumericMod := regexp.MustCompile(`^([a-z\s]+[a-z])\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\)$`)
	if matches := reMultiWordTypeWithNumericMod.FindStringSubmatch(name); len(matches) > 1 {
		name = strings.TrimSpace(matches[1])
	} else {
		// Pola 2: Menangkap tipe satu kata yang diikuti modifier numerik
		// e.g., "varchar(255)" -> "varchar"
		reSingleWordTypeWithNumericMod := regexp.MustCompile(`^([a-z]+)\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\)$`)
		if matches := reSingleWordTypeWithNumericMod.FindStringSubmatch(name); len(matches) > 1 {
			name = strings.TrimSpace(matches[1])
		}
		// Jika tidak ada yang cocok, `name` tetap seperti adanya (misalnya, "geometry(point,4326)" atau "func_returns_type()")
	}
	
	// Untuk kasus seperti "timestamp(3) with time zone", modifier ada di tengah.
	// Kita perlu menangani ini secara terpisah jika pendekatan di atas tidak cukup.
	// Regex yang lebih canggih: cari tipe dasar, lalu modifier numerik, lalu sisa.
	// Grup 1: Tipe dasar (bisa multi-kata)
	// Grup 2: Modifier numerik (opsional)
	// Grup 3: Sisa string (opsional)
	reComplexStructure := regexp.MustCompile(`^([a-z][a-z\s]*[a-z]|[a-z])` + // G1: Tipe dasar
	    `(\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\))?` + // G2: Modifier numerik opsional
	    `(.*)$`) // G3: Sisa

	matchesComplex := reComplexStructure.FindStringSubmatch(name)
	if len(matchesComplex) > 1 {
	    basePart := strings.TrimSpace(matchesComplex[1])
	    numericModifierFound := len(matchesComplex) > 2 && matchesComplex[2] != ""
	    remainingPart := ""
	    if len(matchesComplex) > 3 {
	        remainingPart = matchesComplex[3] // Jangan trim dulu
	    }

	    if numericModifierFound {
	        // Jika modifier numerik ditemukan, kita gabungkan basePart dengan remainingPart (yang sudah di-trim)
	        name = strings.TrimSpace(basePart + " " + strings.TrimSpace(remainingPart))
	    } else {
	        // Jika tidak ada modifier numerik standar, gabungkan basePart dengan remainingPart apa adanya.
	        // Ini akan mempertahankan "(Point,4326)" atau "()"
	        name = strings.TrimSpace(basePart + remainingPart)
	    }
	}
	// Bersihkan spasi ganda yang mungkin timbul
	name = regexp.MustCompile(`\s+`).ReplaceAllString(name, " ")
	name = strings.TrimSpace(name)


	// 3. Terapkan alias umum
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
		"character":                 "char",
		"bit varying":               "varbit",
	}
	if mapped, ok := aliases[name]; ok {
		name = mapped
	}

	return strings.TrimSpace(name)
}

// ... (sisa file tetap sama seperti revisi terakhir) ...
func isStringType(normTypeName string) bool { /* ... */ return strings.Contains(normTypeName, "char") || strings.Contains(normTypeName, "text") || strings.Contains(normTypeName, "clob") || normTypeName == "enum" || normTypeName == "set" || normTypeName == "uuid" || normTypeName == "json" || normTypeName == "xml" }
func isBinaryType(normTypeName string) bool { /* ... */ return strings.Contains(normTypeName, "binary") || strings.Contains(normTypeName, "blob") || normTypeName == "bytea" || normTypeName == "bit" || normTypeName == "varbit" }
func isNumericType(normTypeName string) bool { /* ... */ return isIntegerType(normTypeName) || strings.Contains(normTypeName, "decimal") || strings.Contains(normTypeName, "numeric") || strings.Contains(normTypeName, "float") || strings.Contains(normTypeName, "double") || strings.Contains(normTypeName, "real") || strings.Contains(normTypeName, "money") }
func isIntegerType(normTypeName string) bool { /* ... */ return strings.Contains(normTypeName, "int") || strings.Contains(normTypeName, "serial") }
func isPrecisionRelevant(normTypeName string) bool { /* ... */ return strings.Contains(normTypeName, "decimal") || strings.Contains(normTypeName, "numeric") || normTypeName == "time" || normTypeName == "timetz" || normTypeName == "timestamp" || normTypeName == "timestamptz" || normTypeName == "datetime" }
func isScaleRelevant(normTypeName string) bool { /* ... */ return strings.Contains(normTypeName, "decimal") || strings.Contains(normTypeName, "numeric") }
func (s *SchemaSyncer) populateMappedTypesForSourceColumns(columns []ColumnInfo, tableName string) error {
	log := s.logger.With(zap.String("table", tableName), zap.String("phase", "populate-source-mapped-types"))
	for i := range columns {
		if columns[i].IsGenerated {
			baseSrcTypeForGenerated := extractBaseTypeFromGenerated(columns[i].Type, log)
			columns[i].MappedType = baseSrcTypeForGenerated
			log.Debug("Using extracted base type as MappedType for generated source column.", zap.String("column", columns[i].Name), zap.String("original_full_type", columns[i].Type), zap.String("mapped_type_for_comp", columns[i].MappedType))
			continue
		}
		mapped, mapErr := s.mapDataType(columns[i].Type)
		if mapErr != nil {
			log.Error("Fatal type mapping error for source column", zap.String("column", columns[i].Name), zap.String("original_type", columns[i].Type), zap.Error(mapErr))
			return fmt.Errorf("fatal type mapping error for column '%s' in table '%s': %w", columns[i].Name, tableName, mapErr)
		}
		columns[i].MappedType = mapped
		log.Debug("Populated MappedType for source column", zap.String("column", columns[i].Name), zap.String("original_type", columns[i].Type), zap.String("mapped_type", columns[i].MappedType))
	}
	return nil
}
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
func (s *SchemaSyncer) mapDataType(srcType string) (string, error) {
	log := s.logger.With(zap.String("source_type_raw", srcType), zap.String("src_dialect", s.srcDialect), zap.String("dst_dialect", s.dstDialect), zap.String("action", "mapDataType"))
	fullSrcTypeLower := strings.ToLower(strings.TrimSpace(srcType))
	normalizedSrcTypeKey := normalizeTypeName(fullSrcTypeLower)
	typeMappingConfigEntry := config.GetTypeMappingForDialects(s.srcDialect, s.dstDialect)
	if typeMappingConfigEntry != nil {
		log.Debug("Attempting to map type using external JSON configuration.")
		for _, sm := range typeMappingConfigEntry.SpecialMappings {
			re, errComp := regexp.Compile(sm.SourceTypePattern)
			if errComp != nil {
				log.Error("Invalid regex pattern in special type mapping.", zap.String("pattern", sm.SourceTypePattern), zap.Error(errComp))
				continue
			}
			if re.MatchString(fullSrcTypeLower) {
				log.Info("Matched special type mapping from JSON config.", zap.String("pattern", sm.SourceTypePattern), zap.String("target_base_type", sm.TargetType))
				return s.applyTypeModifiers(srcType, sm.TargetType), nil
			}
		}
		if mappedType, ok := typeMappingConfigEntry.Mappings[normalizedSrcTypeKey]; ok {
			log.Info("Matched standard type mapping from JSON config.",	zap.String("normalized_source_key", normalizedSrcTypeKey), zap.String("target_base_type", mappedType))
			return s.applyTypeModifiers(srcType, mappedType), nil
		}
		log.Debug("No match found in external JSON configuration for this type.")
	} else {
		log.Debug("No external type mapping JSON configuration for current dialect pair.")
	}
	if s.srcDialect == s.dstDialect {
		log.Info("Source and destination dialects are the same. Using source type directly.", zap.String("source_type", srcType))
		return s.applyTypeModifiers(srcType, srcType), nil
	}
	internalMappedType := s.getInternalDefaultMapping(normalizedSrcTypeKey, fullSrcTypeLower)
	if internalMappedType != "" {
		log.Info("Matched internal default type mapping.", zap.String("target_base_type", internalMappedType))
		return s.applyTypeModifiers(srcType, internalMappedType), nil
	}
	fallbackType := s.getGenericFallbackType()
	log.Warn("No specific type mapping found. Using generic fallback type.", zap.String("normalized_source_key", normalizedSrcTypeKey),	zap.String("original_source_type", srcType), zap.String("fallback_target_type", fallbackType))
	return s.applyTypeModifiers(srcType, fallbackType), nil
}
func (s *SchemaSyncer) getInternalDefaultMapping(normalizedSrcKey, fullSrcTypeLower string) string {
	if s.srcDialect == "mysql" && s.dstDialect == "postgres" {
		if normalizedSrcKey == "tinyint" && strings.HasPrefix(fullSrcTypeLower, "tinyint(1)") { return "BOOLEAN" }
		switch normalizedSrcKey {
		case "bit": return "VARBIT"; case "tinyint": return "SMALLINT"; case "smallint": return "SMALLINT";
		case "mediumint": return "INTEGER"; case "int", "integer": return "INTEGER"; case "bigint": return "BIGINT";
		case "float": return "REAL"; case "double": return "DOUBLE PRECISION"; case "decimal", "numeric": return "NUMERIC";
		case "char": return "CHAR"; case "varchar": return "VARCHAR";
		case "tinytext", "text", "mediumtext", "longtext": return "TEXT";
		case "binary": return "BYTEA"; case "varbinary": return "BYTEA";
		case "tinyblob", "blob", "mediumblob", "longblob": return "BYTEA";
		case "json": return "JSONB"; case "enum": return "VARCHAR"; case "set": return "TEXT";
		case "date": return "DATE"; case "time": return "TIME WITHOUT TIME ZONE";
		case "datetime": return "TIMESTAMP WITHOUT TIME ZONE"; case "timestamp": return "TIMESTAMP WITH TIME ZONE";
		case "year": return "SMALLINT";
		}
	}
	if s.srcDialect == "postgres" && s.dstDialect == "mysql" {
		if strings.HasSuffix(normalizedSrcKey, "[]") { return "JSON" }
		switch normalizedSrcKey {
		case "bool": return "TINYINT(1)"; case "smallint", "int2": return "SMALLINT";
		case "int", "int4", "serial", "serial4": return "INT";
		case "bigint", "int8", "bigserial", "serial8": return "BIGINT";
		case "real", "float4": return "FLOAT"; case "double", "float8": return "DOUBLE";
		case "numeric", "decimal": return "DECIMAL"; case "money": return "DECIMAL(19,2)";
		case "char", "bpchar": return "CHAR"; case "varchar": return "VARCHAR"; case "text": return "LONGTEXT";
		case "bytea": return "LONGBLOB"; case "json", "jsonb": return "JSON"; case "uuid": return "CHAR(36)";
		case "date": return "DATE"; case "time": return "TIME"; case "timetz": return "TIME";
		case "timestamp": return "DATETIME(6)"; case "timestamptz": return "TIMESTAMP(6)";
		case "interval": return "VARCHAR(100)"; case "point": return "POINT";
		case "line", "lseg", "box", "path", "polygon", "circle": return "GEOMETRY";
		case "cidr", "inet": return "VARCHAR(43)"; case "macaddr": return "VARCHAR(17)";  case "macaddr8": return "VARCHAR(23)";
		case "bit": return "BIT"; case "varbit": return "VARBINARY";
		}
	}
	if s.srcDialect == "sqlite" && s.dstDialect == "postgres" {
		switch normalizedSrcKey {
		case "integer", "int": return "BIGINT"; case "real": return "DOUBLE PRECISION";
		case "text", "varchar", "char": return "TEXT"; case "blob": return "BYTEA";
		case "numeric", "decimal": return "NUMERIC";
		case "datetime", "timestamp": return "TIMESTAMP WITHOUT TIME ZONE"; case "date": return "DATE";
		case "boolean": return "BOOLEAN";
		}
	}
	if s.srcDialect == "sqlite" && s.dstDialect == "mysql" {
		switch normalizedSrcKey {
		case "integer", "int": return "BIGINT"; case "real": return "DOUBLE";
		case "text", "varchar", "char": return "LONGTEXT"; case "blob": return "LONGBLOB";
		case "numeric", "decimal": return "DECIMAL(38,10)";
		case "datetime", "timestamp": return "DATETIME(6)"; case "date": return "DATE";
		case "boolean": return "TINYINT(1)";
		}
	}
	return ""
}
func (s *SchemaSyncer) applyTypeModifiers(srcTypeRaw, mappedBaseType string) string {
	log := s.logger.With(
		zap.String("src_type_raw", srcTypeRaw),
		zap.String("mapped_base_type", mappedBaseType),
		zap.String("dst_dialect", s.dstDialect),
		zap.String("action", "applyTypeModifiers"),
	)
	reModifier := regexp.MustCompile(`\((.+?)\)`)
	srcModifierMatches := reModifier.FindStringSubmatch(srcTypeRaw)
	var srcModifierContent string
	if len(srcModifierMatches) > 1 {
		srcModifierContent = strings.TrimSpace(srcModifierMatches[1])
	}
	baseTypeOnly := mappedBaseType
	var mappedBaseModifierContent string
	if idxParen := strings.Index(mappedBaseType, "("); idxParen != -1 {
		if strings.HasSuffix(mappedBaseType, ")") {
			baseTypeOnly = strings.TrimSpace(mappedBaseType[:idxParen])
			mappedBaseModifierContent = strings.TrimSpace(mappedBaseType[idxParen+1 : len(mappedBaseType)-1])
		}
	}
	normMappedBaseTypeOnly := normalizeTypeName(baseTypeOnly)
	normSrcBaseTypeOnly := normalizeTypeName(strings.Split(srcTypeRaw, "(")[0])

	if srcModifierContent == "" {
		log.Debug("Source type has no modifier. Using mapped base type as is.", zap.String("result", mappedBaseType))
		return mappedBaseType
	}
	if mappedBaseModifierContent != "" {
		if normSrcBaseTypeOnly == "enum" || normSrcBaseTypeOnly == "set" {
			log.Debug("Source is ENUM/SET and mapped base type has a modifier. Preferring modifier from mapped base type.", zap.String("result", mappedBaseType))
			return mappedBaseType
		}
		log.Debug("Both source and mapped base have modifiers. Will attempt to apply source modifier to the un-modified target base.",
			zap.String("src_modifier", srcModifierContent), zap.String("unmodified_target_base", baseTypeOnly))
	}
	modifierIsGenerallyTransferable := true
	isSrcModNumericFormat := regexp.MustCompile(`^\s*\d+(?:\s*,\s*\d+)?\s*$`).MatchString(srcModifierContent)
	if !isSrcModNumericFormat {
		modifierIsGenerallyTransferable = false
	} else {
		switch normSrcBaseTypeOnly {
		case "text", "tinytext", "mediumtext", "longtext", "json", "uuid", "clob", "blob", "tinyblob", "mediumblob", "longblob", "bytea":
			modifierIsGenerallyTransferable = false
		case "int", "tinyint", "smallint", "mediumint", "bigint", "integer":
			if !(normMappedBaseTypeOnly == "bit" && normSrcBaseTypeOnly == "bit") && !(normMappedBaseTypeOnly == "varbit" && normSrcBaseTypeOnly == "varbit") {
				if !((normSrcBaseTypeOnly == "bit" && srcModifierContent == "1") && (normMappedBaseTypeOnly == "bool" || normMappedBaseTypeOnly == "tinyint")) {
					modifierIsGenerallyTransferable = false
				}
			}
		}
	}
	if !modifierIsGenerallyTransferable {
		log.Debug("Source type's modifier is not semantically transferable as a standard numeric length/precision/scale for the target base type.",
			zap.String("src_base_type", normSrcBaseTypeOnly), zap.String("src_modifier", srcModifierContent), zap.String("target_base_type", normMappedBaseTypeOnly))
		if mappedBaseModifierContent != "" {
			log.Debug("Using mapped base type with its original modifier.", zap.String("result", mappedBaseType))
			return mappedBaseType
		}
		log.Debug("Using mapped base type without any modifier.", zap.String("result", baseTypeOnly))
		return baseTypeOnly
	}
	canApplySrcModifier := false
	finalAppliedModifier := srcModifierContent
	switch {
	case isStringType(normMappedBaseTypeOnly):
		if !isLargeTextOrBlob(normMappedBaseTypeOnly) && normMappedBaseTypeOnly != "uuid" && normMappedBaseTypeOnly != "json" && normMappedBaseTypeOnly != "xml" && normMappedBaseTypeOnly != "enum" && normMappedBaseTypeOnly != "set" {
			if _, err := strconv.Atoi(srcModifierContent); err == nil { canApplySrcModifier = true
			} else { log.Debug("Src mod for string type is not a simple number.", zap.String("src_mod", srcModifierContent)) }
		}
	case isBinaryType(normMappedBaseTypeOnly):
		if normMappedBaseTypeOnly == "bit" || normMappedBaseTypeOnly == "varbit" || normMappedBaseTypeOnly == "binary" || normMappedBaseTypeOnly == "varbinary" {
			if _, err := strconv.Atoi(srcModifierContent); err == nil { canApplySrcModifier = true
			} else { log.Debug("Src mod for BIT/VARBIT/BINARY/VARBINARY type is not a simple number.", zap.String("src_mod", srcModifierContent)) }
		} else if !isLargeTextOrBlob(normMappedBaseTypeOnly) && normMappedBaseTypeOnly != "bytea" {
			if _, err := strconv.Atoi(srcModifierContent); err == nil { canApplySrcModifier = true
			} else { log.Debug("Src mod for binary type (non-BIT) is not a simple number.", zap.String("src_mod", srcModifierContent)) }
		}
	case isScaleRelevant(normMappedBaseTypeOnly):
		parts := strings.Split(srcModifierContent, ",")
		if len(parts) == 1 || len(parts) == 2 {
			if pVal, errP := strconv.Atoi(strings.TrimSpace(parts[0])); errP == nil && pVal > 0 {
				validScale := true
				if len(parts) == 2 {
					if sVal, errS := strconv.Atoi(strings.TrimSpace(parts[1])); errS != nil || sVal < 0 || sVal > pVal { validScale = false }
				}
				if validScale { canApplySrcModifier = true
				} else { log.Debug("Invalid scale in src mod for decimal/numeric.", zap.String("src_mod", srcModifierContent)) }
			} else { log.Debug("Invalid precision in src mod for decimal/numeric.", zap.String("src_mod", srcModifierContent)) }
		}
	case isPrecisionRelevant(normMappedBaseTypeOnly):
		if s.dstDialect == "sqlite" && (strings.Contains(normMappedBaseTypeOnly, "time") || strings.Contains(normMappedBaseTypeOnly, "timestamp") || normMappedBaseTypeOnly == "datetime") {
			log.Debug("SQLite does not use precision mods for time/date types in DDL.", zap.String("src_mod", srcModifierContent))
			canApplySrcModifier = false; finalAppliedModifier = ""
			break
		}
		partsTime := strings.Split(srcModifierContent, ",")
		if len(partsTime) == 1 {
			if pVal, err := strconv.Atoi(strings.TrimSpace(partsTime[0])); err == nil {
				isValidForDialect := false
				if s.dstDialect == "mysql" && (normMappedBaseTypeOnly == "datetime" || normMappedBaseTypeOnly == "timestamp" || normMappedBaseTypeOnly == "time") {
					if pVal >= 0 && pVal <= 6 { finalAppliedModifier = strconv.Itoa(pVal); isValidForDialect = true
					} else { log.Debug("MySQL time/datetime/ts precision out of range (0-6).", zap.Int("src_prec", pVal)); finalAppliedModifier = "" }
				} else if s.dstDialect == "postgres" && (strings.HasPrefix(normMappedBaseTypeOnly, "time") || strings.HasPrefix(normMappedBaseTypeOnly, "timestamp")) {
					if pVal >= 0 && pVal <= 6 { finalAppliedModifier = strconv.Itoa(pVal); isValidForDialect = true
					} else { log.Debug("PG time/ts precision out of common range (0-6), applying as is.", zap.Int("src_prec", pVal)); finalAppliedModifier = strconv.Itoa(pVal); isValidForDialect = true }
				} else if normMappedBaseTypeOnly == "time" || normMappedBaseTypeOnly == "timestamp" || normMappedBaseTypeOnly == "datetime" {
					finalAppliedModifier = strings.TrimSpace(partsTime[0]); isValidForDialect = true
				}
				canApplySrcModifier = isValidForDialect
			} else { log.Debug("Src mod for time/ts/datetime type is not a simple number.", zap.String("src_mod", srcModifierContent)) }
		} else { log.Debug("Src mod for time/ts/datetime type has >1 part.", zap.String("src_mod", srcModifierContent)) }
	}

	if canApplySrcModifier && finalAppliedModifier != "" {
		log.Debug("Applying src modifier to (unmodified) mapped base type.", zap.String("target_base", baseTypeOnly), zap.String("modifier", finalAppliedModifier))
		return fmt.Sprintf("%s(%s)", baseTypeOnly, finalAppliedModifier)
	}
	if srcModifierContent != "" && finalAppliedModifier == "" {
		log.Debug("Source modifier was invalidated (e.g. out of range for target dialect). Returning target base type without any modifier.", zap.String("result", baseTypeOnly))
		return baseTypeOnly
	}
	if !canApplySrcModifier && mappedBaseModifierContent != "" {
		log.Debug("Source modifier not applicable or not transferable. Mapped base type has its own modifier, using that.", zap.String("result", mappedBaseType))
		return mappedBaseType
	}
	log.Debug("Source modifier not applied, and mapped base has no modifier (or previous conditions not met). Returning target base type only.", zap.String("result", baseTypeOnly))
	return baseTypeOnly
}

func (s *SchemaSyncer) getGenericFallbackType() string {
	switch s.dstDialect {
	case "mysql": return "LONGTEXT"; case "postgres": return "TEXT"; case "sqlite": return "TEXT"
	default: s.logger.Error("Unknown destination dialect for generic fallback type.", zap.String("unknown_dialect", s.dstDialect)); return "TEXT"
	}
}
