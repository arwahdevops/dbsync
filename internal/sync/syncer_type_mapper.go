// internal/sync/syncer_type_mapper.go
package sync

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/arwahdevops/dbsync/internal/config"
)

type MappedTypeResult struct {
	FinalMappedType      string
	PostgresUsingExpr    string
	ModifierHandlingUsed config.ModifierHandlingStrategy
}

// Fungsi isType... tetap di sini karena spesifik untuk logika applyTypeModifiers
func isStringType(normTypeName string) bool           { return strings.Contains(normTypeName, "char") || strings.Contains(normTypeName, "text") || strings.Contains(normTypeName, "clob") || normTypeName == "enum" || normTypeName == "set" || normTypeName == "uuid" || normTypeName == "json" || normTypeName == "xml" }
func isBinaryType(normTypeName string) bool          { return strings.Contains(normTypeName, "binary") || strings.Contains(normTypeName, "blob") || normTypeName == "bytea" || normTypeName == "bit" || normTypeName == "varbit" }
func isNumericType(normTypeName string) bool         { return isIntegerType(normTypeName) || strings.Contains(normTypeName, "decimal") || strings.Contains(normTypeName, "numeric") || strings.Contains(normTypeName, "float") || strings.Contains(normTypeName, "double") || strings.Contains(normTypeName, "real") || strings.Contains(normTypeName, "money") }
func isIntegerType(normTypeName string) bool         { return strings.Contains(normTypeName, "int") || strings.Contains(normTypeName, "serial") }
func isPrecisionRelevant(normTypeName string) bool   { return strings.Contains(normTypeName, "decimal") || strings.Contains(normTypeName, "numeric") || normTypeName == "time" || normTypeName == "timetz" || normTypeName == "timestamp" || normTypeName == "timestamptz" || normTypeName == "datetime" }
func isScaleRelevant(normTypeName string) bool       { return strings.Contains(normTypeName, "decimal") || strings.Contains(normTypeName, "numeric") }


func (s *SchemaSyncer) populateMappedTypesForSourceColumns(columns []ColumnInfo, tableName string) error {
	log := s.logger.With(zap.String("table", tableName), zap.String("phase", "populate-source-mapped-types"))
	for i := range columns {
		var typeToMap string
		if columns[i].IsGenerated {
			typeToMap = extractBaseTypeFromGenerated(columns[i].Type, log) // dari compare_helpers.go
			log.Debug("Using extracted base type as mapping key for generated source column.", zap.String("column", columns[i].Name), zap.String("base_type_for_mapping_key", typeToMap))
		} else {
			typeToMap = columns[i].Type
		}
		mappedResult, mapErr := s.mapDataType(typeToMap, columns[i].Type)
		if mapErr != nil {
			log.Error("Fatal type mapping error for source column", zap.String("column", columns[i].Name), zap.String("type_key", typeToMap), zap.String("original_full_type", columns[i].Type), zap.Error(mapErr))
			return fmt.Errorf("fatal type mapping error for column '%s' (type key: '%s', full type: '%s') in table '%s': %w", columns[i].Name, typeToMap, columns[i].Type, tableName, mapErr)
		}
		columns[i].MappedType = mappedResult.FinalMappedType
		// Simpan juga PostgresUsingExpr jika relevan. Ini bisa ditambahkan ke ColumnInfo
		// atau diakses lagi saat membuat DDL ALTER. Untuk sekarang, kita fokus pada MappedType.
		log.Debug("Populated MappedType for source column", zap.String("column", columns[i].Name), zap.String("original_full_type", columns[i].Type),
			zap.String("final_mapped_type", columns[i].MappedType), zap.String("modifier_handling_used", string(mappedResult.ModifierHandlingUsed)),
			zap.String("postgres_using_expr_from_map", mappedResult.PostgresUsingExpr))
	}
	return nil
}

func (s *SchemaSyncer) populateMappedTypesForDestinationColumns(columns []ColumnInfo) {
	for i := range columns {
		if columns[i].MappedType == "" { // Hanya jika belum di-override
			if columns[i].IsGenerated {
				columns[i].MappedType = extractBaseTypeFromGenerated(columns[i].Type, s.logger) // dari compare_helpers.go
			} else {
				columns[i].MappedType = columns[i].Type // Gunakan tipe asli tujuan jika tidak ada mapping khusus
			}
		}
	}
}

func (s *SchemaSyncer) mapDataType(srcTypeForMappingKey string, fullOriginalSrcType string) (*MappedTypeResult, error) {
	logFields := []zap.Field{
		zap.String("src_type_for_mapping_key", srcTypeForMappingKey), zap.String("full_original_src_type", fullOriginalSrcType),
		zap.String("src_dialect", s.srcDialect), zap.String("dst_dialect", s.dstDialect), zap.String("action", "mapDataType"),
	}
	log := s.logger.With(logFields...)
	fullSrcTypeLowerForPattern := strings.ToLower(strings.TrimSpace(fullOriginalSrcType))
	normalizedSrcTypeKeyForLookup := normalizeTypeName(srcTypeForMappingKey) // dari compare_helpers.go
	result := &MappedTypeResult{ModifierHandlingUsed: config.ModifierHandlingApplySource}
	typeMappingConfigEntry := config.GetTypeMappingForDialects(s.srcDialect, s.dstDialect)

	if typeMappingConfigEntry != nil {
		log.Debug("Attempting to map type using internal type mapping configuration.")
		for _, sm := range typeMappingConfigEntry.SpecialMappings {
			re, errComp := regexp.Compile(sm.SourceTypePattern)
			if errComp != nil { log.Error("Invalid regex pattern in special type mapping, skipping.", zap.String("pattern", sm.SourceTypePattern), zap.Error(errComp)); continue }
			if re.MatchString(fullSrcTypeLowerForPattern) {
				log.Info("Matched special type mapping from internal config.", zap.String("pattern_matched", sm.SourceTypePattern), zap.String("target_type_defined", sm.TargetType))
				result.ModifierHandlingUsed, result.PostgresUsingExpr = sm.ModifierHandling, sm.PostgresUsingExpr
				result.FinalMappedType = s.applyTypeModifiers(fullOriginalSrcType, sm.TargetType, result.ModifierHandlingUsed)
				return result, nil
			}
		}
		if mappedInfo, ok := typeMappingConfigEntry.Mappings[normalizedSrcTypeKeyForLookup]; ok {
			log.Info("Matched standard type mapping from internal config.", zap.String("normalized_source_key_used", normalizedSrcTypeKeyForLookup), zap.String("target_type_defined", mappedInfo.TargetType))
			result.ModifierHandlingUsed, result.PostgresUsingExpr = mappedInfo.ModifierHandling, mappedInfo.PostgresUsingExpr
			result.FinalMappedType = s.applyTypeModifiers(fullOriginalSrcType, mappedInfo.TargetType, result.ModifierHandlingUsed)
			return result, nil
		}
		log.Debug("No match found in internal type mapping configuration.")
	} else {
		log.Debug("No internal type mapping configuration defined for current dialect pair.")
	}

	if s.srcDialect == s.dstDialect {
		log.Info("Source and destination dialects are the same. Using source type directly.", zap.String("using_source_type", fullOriginalSrcType))
		result.FinalMappedType = s.applyTypeModifiers(fullOriginalSrcType, fullOriginalSrcType, result.ModifierHandlingUsed)
		return result, nil
	}
	internalMappedBaseType := s.getInternalDefaultMapping(normalizedSrcTypeKeyForLookup, fullSrcTypeLowerForPattern)
	if internalMappedBaseType != "" {
		log.Info("Matched internal fallback default type mapping.", zap.String("target_base_type", internalMappedBaseType))
		result.FinalMappedType = s.applyTypeModifiers(fullOriginalSrcType, internalMappedBaseType, result.ModifierHandlingUsed)
		return result, nil
	}
	fallbackType := s.getGenericFallbackType()
	log.Warn("No specific type mapping found. Using generic fallback type.", zap.String("fallback_target_type_used", fallbackType))
	result.FinalMappedType = s.applyTypeModifiers(fullOriginalSrcType, fallbackType, result.ModifierHandlingUsed)
	return result, nil
}

func (s *SchemaSyncer) applyTypeModifiers(srcTypeRaw, mappedTypeFromConfig string, modHandling config.ModifierHandlingStrategy) string {
	log := s.logger.With(zap.String("src_type_raw_for_modifier", srcTypeRaw), zap.String("mapped_type_from_config", mappedTypeFromConfig),
		zap.String("modifier_handling_strategy", string(modHandling)), zap.String("dst_dialect", s.dstDialect), zap.String("action", "applyTypeModifiers"))

	var baseTypeOfMappedConfig, modifierOfMappedConfig string
	if matchesMappedMod := regexp.MustCompile(`^(.+?)\s*\((.+)\)$`).FindStringSubmatch(mappedTypeFromConfig); len(matchesMappedMod) > 2 {
		baseTypeOfMappedConfig, modifierOfMappedConfig = strings.TrimSpace(matchesMappedMod[1]), strings.TrimSpace(matchesMappedMod[2])
	} else {
		baseTypeOfMappedConfig = strings.TrimSpace(mappedTypeFromConfig)
	}
	normMappedBaseTypeOnly := normalizeTypeName(baseTypeOfMappedConfig) // dari compare_helpers.go
	log.Debug("Parsed 'mappedTypeFromConfig'", zap.String("derived_base", baseTypeOfMappedConfig), zap.String("derived_modifier", modifierOfMappedConfig))

	switch modHandling {
	case config.ModifierHandlingUseTargetDefined, config.ModifierHandlingIgnoreSource:
		log.Debug("Strategy 'use_target_defined' or 'ignore_source'. Using mapped type from config.", zap.String("result", mappedTypeFromConfig))
		return mappedTypeFromConfig
	case config.ModifierHandlingApplySource:
		log.Debug("Strategy 'apply_source': Attempting to apply source modifiers.")
		var srcModifierContent string
		if srcModifierMatches := regexp.MustCompile(`\((.+?)\)`).FindStringSubmatch(srcTypeRaw); len(srcModifierMatches) > 1 {
			srcModifierContent = strings.TrimSpace(srcModifierMatches[1])
		}
		normSrcBaseTypeOnly := normalizeTypeName(strings.Split(srcTypeRaw, "(")[0]) // dari compare_helpers.go
		if srcModifierContent == "" {
			log.Debug("Source type has no modifier. Using mapped type from config.", zap.String("result", mappedTypeFromConfig))
			return mappedTypeFromConfig
		}
		if modifierOfMappedConfig != "" && (normSrcBaseTypeOnly == "enum" || normSrcBaseTypeOnly == "set") {
			log.Debug("Source is ENUM/SET, preferring mapped type's own modifier.", zap.String("result", mappedTypeFromConfig))
			return mappedTypeFromConfig
		}
		modifierIsGenerallyTransferable := regexp.MustCompile(`^\s*\d+(?:\s*,\s*\d+)?\s*$`).MatchString(srcModifierContent)
		if modifierIsGenerallyTransferable {
			switch normSrcBaseTypeOnly {
			case "text", "tinytext", "mediumtext", "longtext", "json", "uuid", "clob", "blob", "tinyblob", "mediumblob", "longblob", "bytea":
				modifierIsGenerallyTransferable = false
			case "int", "tinyint", "smallint", "mediumint", "bigint", "integer":
				if !((normSrcBaseTypeOnly == "bit" && srcModifierContent == "1") && (normMappedBaseTypeOnly == "bool" || normMappedBaseTypeOnly == "tinyint")) &&
					!(normMappedBaseTypeOnly == "bit" && normSrcBaseTypeOnly == "bit") &&
					!(normMappedBaseTypeOnly == "varbit" && normSrcBaseTypeOnly == "varbit") {
					modifierIsGenerallyTransferable = false
				}
			}
		}
		if !modifierIsGenerallyTransferable {
			log.Debug("Source modifier not generally transferable.")
			if modifierOfMappedConfig != "" { return mappedTypeFromConfig }
			return baseTypeOfMappedConfig
		}
		canApplySrcModifier, finalAppliedModifier := false, srcModifierContent
		switch {
		case isStringType(normMappedBaseTypeOnly):
			if !isLargeTextOrBlob(normMappedBaseTypeOnly) && normMappedBaseTypeOnly != "uuid" && normMappedBaseTypeOnly != "json" && normMappedBaseTypeOnly != "xml" && normMappedBaseTypeOnly != "enum" && normMappedBaseTypeOnly != "set" { // isLargeTextOrBlob dari compare_helpers.go
				if _, err := strconv.Atoi(srcModifierContent); err == nil { canApplySrcModifier = true }
			}
		case isBinaryType(normMappedBaseTypeOnly):
			if normMappedBaseTypeOnly == "bit" || normMappedBaseTypeOnly == "varbit" || normMappedBaseTypeOnly == "binary" || normMappedBaseTypeOnly == "varbinary" {
				if _, err := strconv.Atoi(srcModifierContent); err == nil { canApplySrcModifier = true }
			} else if !isLargeTextOrBlob(normMappedBaseTypeOnly) && normMappedBaseTypeOnly != "bytea" { // isLargeTextOrBlob dari compare_helpers.go
				if _, err := strconv.Atoi(srcModifierContent); err == nil { canApplySrcModifier = true }
			}
		case isScaleRelevant(normMappedBaseTypeOnly):
			parts := strings.Split(srcModifierContent, ",")
			if len(parts) == 1 || len(parts) == 2 {
				if pVal, errP := strconv.Atoi(strings.TrimSpace(parts[0])); errP == nil && pVal > 0 {
					validScale := true
					if len(parts) == 2 {
						if sVal, errS := strconv.Atoi(strings.TrimSpace(parts[1])); errS != nil || sVal < 0 || sVal > pVal { validScale = false }
					}
					if validScale { canApplySrcModifier = true }
				}
			}
		case isPrecisionRelevant(normMappedBaseTypeOnly):
			if s.dstDialect == "sqlite" && (strings.Contains(normMappedBaseTypeOnly, "time") || strings.Contains(normMappedBaseTypeOnly, "timestamp") || normMappedBaseTypeOnly == "datetime") {
				canApplySrcModifier, finalAppliedModifier = false, "" ; break
			}
			partsTime := strings.Split(srcModifierContent, ",")
			if len(partsTime) == 1 {
				if pVal, err := strconv.Atoi(strings.TrimSpace(partsTime[0])); err == nil {
					isValidForDialect := false
					if s.dstDialect == "mysql" && (normMappedBaseTypeOnly == "datetime" || normMappedBaseTypeOnly == "timestamp" || normMappedBaseTypeOnly == "time") {
						if pVal >= 0 && pVal <= 6 { finalAppliedModifier, isValidForDialect = strconv.Itoa(pVal), true } else { finalAppliedModifier, isValidForDialect = "", false }
					} else if s.dstDialect == "postgres" && (strings.HasPrefix(normMappedBaseTypeOnly, "time") || strings.HasPrefix(normMappedBaseTypeOnly, "timestamp")) {
						finalAppliedModifier, isValidForDialect = strconv.Itoa(pVal), true
					} else if normMappedBaseTypeOnly == "time" || normMappedBaseTypeOnly == "timestamp" || normMappedBaseTypeOnly == "datetime" {
						finalAppliedModifier, isValidForDialect = strings.TrimSpace(partsTime[0]), true
					}
					canApplySrcModifier = isValidForDialect
				}
			}
		}
		if canApplySrcModifier && finalAppliedModifier != "" { return fmt.Sprintf("%s(%s)", baseTypeOfMappedConfig, finalAppliedModifier) }
		if srcModifierContent != "" && finalAppliedModifier == "" { return baseTypeOfMappedConfig }
		if !canApplySrcModifier && modifierOfMappedConfig != "" { return mappedTypeFromConfig }
		return baseTypeOfMappedConfig
	default:
		log.Error("Unknown ModifierHandlingStrategy in applyTypeModifiers. BUG.", zap.String("unknown_strategy", string(modHandling)))
		return mappedTypeFromConfig
	}
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
		case "timestamp": return "DATETIME"; case "timestamptz": return "TIMESTAMP";
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
		case "numeric", "decimal": return "DECIMAL(38,18)";
		case "datetime", "timestamp": return "DATETIME(6)"; case "date": return "DATE";
		case "boolean": return "TINYINT(1)";
		}
	}
	return ""
}

func (s *SchemaSyncer) getGenericFallbackType() string {
	switch s.dstDialect {
	case "mysql": return "LONGTEXT"; case "postgres": return "TEXT"; case "sqlite": return "TEXT"
	default: s.logger.Error("Unknown destination dialect for generic fallback type.", zap.String("unknown_dialect", s.dstDialect)); return "TEXT"
	}
}
