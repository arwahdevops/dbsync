package sync

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/arwahdevops/dbsync/internal/config" // Impor config
)

// MappedTypeResult menyimpan hasil dari pemetaan tipe, termasuk info tambahan.
type MappedTypeResult struct {
	FinalMappedType      string
	PostgresUsingExpr    string // Hanya relevan jika target adalah PostgreSQL
	ModifierHandlingUsed config.ModifierHandlingStrategy
}

// normalizeTypeName (tetap sama)
func normalizeTypeName(typeName string) string {
	name := strings.ToLower(strings.TrimSpace(typeName))
	name = strings.ReplaceAll(name, " unsigned", "")
	name = strings.ReplaceAll(name, " zerofill", "")
	name = strings.TrimSpace(name)
	reMultiWordTypeWithNumericMod := regexp.MustCompile(`^([a-z\s]+[a-z])\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\)$`)
	if matches := reMultiWordTypeWithNumericMod.FindStringSubmatch(name); len(matches) > 1 {
		name = strings.TrimSpace(matches[1])
	} else {
		reSingleWordTypeWithNumericMod := regexp.MustCompile(`^([a-z]+)\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\)$`)
		if matches := reSingleWordTypeWithNumericMod.FindStringSubmatch(name); len(matches) > 1 {
			name = strings.TrimSpace(matches[1])
		}
	}
	reComplexStructure := regexp.MustCompile(`^([a-z][a-z\s]*[a-z]|[a-z])` +
		`(\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\))?` +
		`(.*)$`)
	matchesComplex := reComplexStructure.FindStringSubmatch(name)
	if len(matchesComplex) > 1 {
		basePart := strings.TrimSpace(matchesComplex[1])
		numericModifierFound := len(matchesComplex) > 2 && matchesComplex[2] != ""
		remainingPart := ""
		if len(matchesComplex) > 3 {
			remainingPart = matchesComplex[3]
		}
		if numericModifierFound {
			name = strings.TrimSpace(basePart + " " + strings.TrimSpace(remainingPart))
		} else {
			name = strings.TrimSpace(basePart + remainingPart)
		}
	}
	name = regexp.MustCompile(`\s+`).ReplaceAllString(name, " ")
	name = strings.TrimSpace(name)
	aliases := map[string]string{
		"character varying":         "varchar", "double precision": "double", "boolean": "bool",
		"timestamp with time zone": "timestamptz", "timestamp without time zone": "timestamp",
		"time with time zone": "timetz", "time without time zone": "time", "integer": "int",
		"int4": "int", "int8": "bigint", "serial4": "serial", "serial8": "bigserial",
		"character": "char", "bit varying": "varbit",
	}
	if mapped, ok := aliases[name]; ok {
		name = mapped
	}
	return strings.TrimSpace(name)
}

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
			typeToMap = extractBaseTypeFromGenerated(columns[i].Type, log)
			log.Debug("Using extracted base type as mapping key for generated source column.",
				zap.String("column", columns[i].Name),
				zap.String("original_full_type", columns[i].Type),
				zap.String("base_type_for_mapping_key", typeToMap))
		} else {
			typeToMap = columns[i].Type
		}
		mappedResult, mapErr := s.mapDataType(typeToMap, columns[i].Type)
		if mapErr != nil {
			log.Error("Fatal type mapping error for source column",
				zap.String("column", columns[i].Name),
				zap.String("type_used_as_mapping_key", typeToMap),
				zap.String("full_original_src_type", columns[i].Type),
				zap.Error(mapErr))
			return fmt.Errorf("fatal type mapping error for column '%s' (type key: '%s', full type: '%s') in table '%s': %w",
				columns[i].Name, typeToMap, columns[i].Type, tableName, mapErr)
		}
		columns[i].MappedType = mappedResult.FinalMappedType
		log.Debug("Populated MappedType for source column",
			zap.String("column", columns[i].Name),
			zap.String("type_used_as_mapping_key", typeToMap),
			zap.String("full_original_src_type", columns[i].Type),
			zap.String("final_mapped_type", columns[i].MappedType),
			zap.String("modifier_handling_used", string(mappedResult.ModifierHandlingUsed)),
			zap.String("postgres_using_expr_from_map", mappedResult.PostgresUsingExpr),
		)
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

func (s *SchemaSyncer) mapDataType(srcTypeForMappingKey string, fullOriginalSrcType string) (*MappedTypeResult, error) {
	logFields := []zap.Field{
		zap.String("src_type_for_mapping_key", srcTypeForMappingKey),
		zap.String("full_original_src_type", fullOriginalSrcType),
		zap.String("src_dialect", s.srcDialect),
		zap.String("dst_dialect", s.dstDialect),
		zap.String("action", "mapDataType"),
	}
	log := s.logger.With(logFields...)

	fullSrcTypeLowerForPattern := strings.ToLower(strings.TrimSpace(fullOriginalSrcType))
	normalizedSrcTypeKeyForLookup := normalizeTypeName(srcTypeForMappingKey)

	result := &MappedTypeResult{
		ModifierHandlingUsed: config.ModifierHandlingApplySource,
	}

	typeMappingConfigEntry := config.GetTypeMappingForDialects(s.srcDialect, s.dstDialect)

	if typeMappingConfigEntry != nil {
		log.Debug("Attempting to map type using internal type mapping configuration.")
		for _, sm := range typeMappingConfigEntry.SpecialMappings {
			re, errComp := regexp.Compile(sm.SourceTypePattern)
			if errComp != nil {
				log.Error("Invalid regex pattern in special type mapping, skipping.",
					zap.String("pattern", sm.SourceTypePattern), zap.Error(errComp))
				continue
			}
			if re.MatchString(fullSrcTypeLowerForPattern) {
				log.Info("Matched special type mapping from internal config.",
					zap.String("pattern_matched", sm.SourceTypePattern),
					zap.String("target_type_defined_in_config", sm.TargetType),
					zap.String("modifier_handling_from_config", string(sm.ModifierHandling)),
					zap.String("postgres_using_expr_from_config", sm.PostgresUsingExpr))

				result.ModifierHandlingUsed = sm.ModifierHandling
				result.PostgresUsingExpr = sm.PostgresUsingExpr
				result.FinalMappedType = s.applyTypeModifiers(fullOriginalSrcType, sm.TargetType, result.ModifierHandlingUsed)
				log.Debug("Final mapped type after special mapping and modifier application.", zap.String("final_type", result.FinalMappedType))
				return result, nil
			}
		}

		if mappedInfo, ok := typeMappingConfigEntry.Mappings[normalizedSrcTypeKeyForLookup]; ok {
			log.Info("Matched standard type mapping from internal config.",
				zap.String("normalized_source_key_used", normalizedSrcTypeKeyForLookup),
				// PERBAIKAN DI SINI:
				zap.String("target_type_defined_in_config", mappedInfo.TargetType),
				zap.String("modifier_handling_from_config", string(mappedInfo.ModifierHandling)),
				zap.String("postgres_using_expr_from_config", mappedInfo.PostgresUsingExpr))

			result.ModifierHandlingUsed = mappedInfo.ModifierHandling
			result.PostgresUsingExpr = mappedInfo.PostgresUsingExpr
			// PERBAIKAN DI SINI:
			result.FinalMappedType = s.applyTypeModifiers(fullOriginalSrcType, mappedInfo.TargetType, result.ModifierHandlingUsed)
			log.Debug("Final mapped type after standard mapping and modifier application.", zap.String("final_type", result.FinalMappedType))
			return result, nil
		}
		log.Debug("No match found in internal type mapping configuration after checking special and standard mappings.")
	} else {
		log.Debug("No internal type mapping configuration defined for current dialect pair (this might be an issue if dialects differ).")
	}

	if s.srcDialect == s.dstDialect {
		log.Info("Source and destination dialects are the same. Using source type directly as no specific mapping found.",
			zap.String("using_source_type", fullOriginalSrcType))
		result.ModifierHandlingUsed = config.ModifierHandlingApplySource
		result.FinalMappedType = s.applyTypeModifiers(fullOriginalSrcType, fullOriginalSrcType, result.ModifierHandlingUsed)
		log.Debug("Final mapped type (same dialect, apply_source).", zap.String("final_type", result.FinalMappedType))
		return result, nil
	}

	internalMappedBaseType := s.getInternalDefaultMapping(normalizedSrcTypeKeyForLookup, fullSrcTypeLowerForPattern)
	if internalMappedBaseType != "" {
		log.Info("Matched internal fallback default type mapping.",
			zap.String("target_base_type_from_internal_map", internalMappedBaseType))
		result.ModifierHandlingUsed = config.ModifierHandlingApplySource
		result.FinalMappedType = s.applyTypeModifiers(fullOriginalSrcType, internalMappedBaseType, result.ModifierHandlingUsed)
		log.Debug("Final mapped type after internal fallback mapping and modifier application.", zap.String("final_type", result.FinalMappedType))
		return result, nil
	}

	fallbackType := s.getGenericFallbackType()
	log.Warn("No specific type mapping found (internal config or fallback). Using generic fallback type.",
		zap.String("normalized_source_key", normalizedSrcTypeKeyForLookup),
		zap.String("original_source_type", fullOriginalSrcType),
		zap.String("fallback_target_type_used", fallbackType))
	result.ModifierHandlingUsed = config.ModifierHandlingApplySource
	result.FinalMappedType = s.applyTypeModifiers(fullOriginalSrcType, fallbackType, result.ModifierHandlingUsed)
	log.Debug("Final mapped type after generic fallback and modifier application.", zap.String("final_type", result.FinalMappedType))
	return result, nil
}

func (s *SchemaSyncer) applyTypeModifiers(srcTypeRaw, mappedTypeFromConfig string, modHandling config.ModifierHandlingStrategy) string {
	log := s.logger.With(
		zap.String("src_type_raw_for_modifier", srcTypeRaw),
		zap.String("mapped_type_from_config", mappedTypeFromConfig),
		zap.String("modifier_handling_strategy", string(modHandling)),
		zap.String("dst_dialect", s.dstDialect),
		zap.String("action", "applyTypeModifiers"),
	)

	var baseTypeOfMappedConfig string
	var modifierOfMappedConfig string
	reMappedMod := regexp.MustCompile(`^(.+?)\s*\((.+)\)$`)
	matchesMappedMod := reMappedMod.FindStringSubmatch(mappedTypeFromConfig)
	if len(matchesMappedMod) > 2 {
		baseTypeOfMappedConfig = strings.TrimSpace(matchesMappedMod[1])
		modifierOfMappedConfig = strings.TrimSpace(matchesMappedMod[2])
	} else {
		baseTypeOfMappedConfig = strings.TrimSpace(mappedTypeFromConfig)
	}
	normMappedBaseTypeOnly := normalizeTypeName(baseTypeOfMappedConfig)

	log.Debug("Parsed 'mappedTypeFromConfig'",
		zap.String("derived_base_of_mapped_config", baseTypeOfMappedConfig),
		zap.String("derived_modifier_of_mapped_config", modifierOfMappedConfig),
		zap.String("normalized_base_of_mapped_config", normMappedBaseTypeOnly))

	switch modHandling {
	case config.ModifierHandlingUseTargetDefined:
		log.Debug("Strategy 'use_target_defined': Using mapped type from config as is.",
			zap.String("result", mappedTypeFromConfig))
		return mappedTypeFromConfig

	case config.ModifierHandlingIgnoreSource:
		log.Debug("Strategy 'ignore_source': Using mapped type from config (potentially with its own defined modifiers), ignoring source modifiers.",
			zap.String("result", mappedTypeFromConfig))
		return mappedTypeFromConfig

	case config.ModifierHandlingApplySource:
		log.Debug("Strategy 'apply_source': Attempting to apply source modifiers to the base of mapped config type.")
		reSrcMod := regexp.MustCompile(`\((.+?)\)`)
		srcModifierMatches := reSrcMod.FindStringSubmatch(srcTypeRaw)
		var srcModifierContent string
		if len(srcModifierMatches) > 1 {
			srcModifierContent = strings.TrimSpace(srcModifierMatches[1])
		}
		normSrcBaseTypeOnly := normalizeTypeName(strings.Split(srcTypeRaw, "(")[0])

		if srcModifierContent == "" {
			log.Debug("Source type has no modifier. Using mapped type from config as is (respecting its potential modifiers).",
				zap.String("result", mappedTypeFromConfig))
			return mappedTypeFromConfig
		}

		if modifierOfMappedConfig != "" {
			if normSrcBaseTypeOnly == "enum" || normSrcBaseTypeOnly == "set" {
				log.Debug("Source is ENUM/SET. Even with 'apply_source', if mapped type from config has its own modifier, preferring that modifier.",
					zap.String("result", mappedTypeFromConfig))
				return mappedTypeFromConfig
			}
			log.Debug("Mapped type from config has its own modifier, but 'apply_source' strategy will attempt to apply source modifier to the *base* of the mapped type.",
				zap.String("base_of_mapped_config_to_use", baseTypeOfMappedConfig),
				zap.String("source_modifier_to_attempt", srcModifierContent))
		}
		modifierIsGenerallyTransferable := true
		isSrcModNumericFormat := regexp.MustCompile(`^\s*\d+(?:\s*,\s*\d+)?\s*$`).MatchString(srcModifierContent)
		if !isSrcModNumericFormat {
			log.Debug("Source modifier is not in a standard numeric format (e.g., 'N' or 'P,S'). Considered not generally transferable.", zap.String("src_modifier", srcModifierContent))
			modifierIsGenerallyTransferable = false
		} else {
			switch normSrcBaseTypeOnly {
			case "text", "tinytext", "mediumtext", "longtext", "json", "uuid", "clob", "blob", "tinyblob", "mediumblob", "longblob", "bytea":
				log.Debug("Source type is a large object/text/JSON/UUID; its numeric modifier is not generally transferable.", zap.String("src_base_type", normSrcBaseTypeOnly))
				modifierIsGenerallyTransferable = false
			case "int", "tinyint", "smallint", "mediumint", "bigint", "integer":
				if !((normSrcBaseTypeOnly == "bit" && srcModifierContent == "1") && (normMappedBaseTypeOnly == "bool" || normMappedBaseTypeOnly == "tinyint")) &&
					!(normMappedBaseTypeOnly == "bit" && normSrcBaseTypeOnly == "bit") &&
					!(normMappedBaseTypeOnly == "varbit" && normSrcBaseTypeOnly == "varbit") {
					log.Debug("Source type is an integer with a display-width like modifier; this modifier is not generally transferable to target integer types.", zap.String("src_base_type", normSrcBaseTypeOnly))
					modifierIsGenerallyTransferable = false
				}
			}
		}

		if !modifierIsGenerallyTransferable {
			log.Debug("Source modifier is not generally transferable.")
			if modifierOfMappedConfig != "" {
				log.Debug("Using mapped type from config with its own modifier.", zap.String("result", mappedTypeFromConfig))
				return mappedTypeFromConfig
			}
			log.Debug("Using base of mapped type from config without any modifier.", zap.String("result", baseTypeOfMappedConfig))
			return baseTypeOfMappedConfig
		}
		canApplySrcModifier := false
		finalAppliedModifier := srcModifierContent
		switch {
		case isStringType(normMappedBaseTypeOnly):
			if !isLargeTextOrBlob(normMappedBaseTypeOnly) && normMappedBaseTypeOnly != "uuid" && normMappedBaseTypeOnly != "json" && normMappedBaseTypeOnly != "xml" && normMappedBaseTypeOnly != "enum" && normMappedBaseTypeOnly != "set" {
				if _, err := strconv.Atoi(srcModifierContent); err == nil {
					canApplySrcModifier = true
				} else {
					log.Debug("Source modifier for target string type is not a simple number.", zap.String("src_mod", srcModifierContent))
				}
			}
		case isBinaryType(normMappedBaseTypeOnly):
			if normMappedBaseTypeOnly == "bit" || normMappedBaseTypeOnly == "varbit" || normMappedBaseTypeOnly == "binary" || normMappedBaseTypeOnly == "varbinary" {
				if _, err := strconv.Atoi(srcModifierContent); err == nil {
					canApplySrcModifier = true
				} else {
					log.Debug("Source modifier for target BIT/VARBIT/BINARY/VARBINARY type is not a simple number.", zap.String("src_mod", srcModifierContent))
				}
			} else if !isLargeTextOrBlob(normMappedBaseTypeOnly) && normMappedBaseTypeOnly != "bytea" {
				if _, err := strconv.Atoi(srcModifierContent); err == nil {
					canApplySrcModifier = true
				} else {
					log.Debug("Source modifier for other target binary type is not a simple number.", zap.String("src_mod", srcModifierContent))
				}
			}
		case isScaleRelevant(normMappedBaseTypeOnly):
			parts := strings.Split(srcModifierContent, ",")
			if len(parts) == 1 || len(parts) == 2 {
				if pVal, errP := strconv.Atoi(strings.TrimSpace(parts[0])); errP == nil && pVal > 0 {
					validScale := true
					if len(parts) == 2 {
						sStr := strings.TrimSpace(parts[1])
						if sVal, errS := strconv.Atoi(sStr); errS != nil || sVal < 0 || sVal > pVal {
							validScale = false
							log.Debug("Invalid scale part in source modifier for target decimal/numeric type.", zap.String("scale_part", sStr), zap.Int("precision_part", pVal))
						}
					}
					if validScale {
						canApplySrcModifier = true
					}
				} else {
					log.Debug("Invalid precision part in source modifier for target decimal/numeric type.", zap.String("precision_part", parts[0]))
				}
			} else {
				log.Debug("Source modifier for target decimal/numeric type has unexpected format (not P or P,S).", zap.String("src_mod", srcModifierContent))
			}
		case isPrecisionRelevant(normMappedBaseTypeOnly):
			if s.dstDialect == "sqlite" && (strings.Contains(normMappedBaseTypeOnly, "time") || strings.Contains(normMappedBaseTypeOnly, "timestamp") || normMappedBaseTypeOnly == "datetime") {
				log.Debug("SQLite does not use precision modifiers for time/date types in DDL. Modifier will not be applied.", zap.String("src_mod", srcModifierContent))
				canApplySrcModifier = false
				finalAppliedModifier = ""
				break
			}
			partsTime := strings.Split(srcModifierContent, ",")
			if len(partsTime) == 1 {
				precStr := strings.TrimSpace(partsTime[0])
				if pVal, err := strconv.Atoi(precStr); err == nil {
					isValidForDialect := false
					if s.dstDialect == "mysql" && (normMappedBaseTypeOnly == "datetime" || normMappedBaseTypeOnly == "timestamp" || normMappedBaseTypeOnly == "time") {
						if pVal >= 0 && pVal <= 6 {
							finalAppliedModifier = strconv.Itoa(pVal)
							isValidForDialect = true
						} else {
							log.Debug("MySQL time/datetime/timestamp precision from source is out of range (0-6). Modifier will not be applied.", zap.Int("src_prec", pVal))
							finalAppliedModifier = ""
							isValidForDialect = false
						}
					} else if s.dstDialect == "postgres" && (strings.HasPrefix(normMappedBaseTypeOnly, "time") || strings.HasPrefix(normMappedBaseTypeOnly, "timestamp")) {
						if pVal >= 0 && pVal <= 6 {
							finalAppliedModifier = strconv.Itoa(pVal)
							isValidForDialect = true
						} else {
							log.Debug("PostgreSQL time/timestamp precision from source is outside common 0-6 range. Applying as is, but review DDL.", zap.Int("src_prec", pVal))
							finalAppliedModifier = strconv.Itoa(pVal)
							isValidForDialect = true
						}
					} else if normMappedBaseTypeOnly == "time" || normMappedBaseTypeOnly == "timestamp" || normMappedBaseTypeOnly == "datetime" {
						finalAppliedModifier = precStr
						isValidForDialect = true
					}
					canApplySrcModifier = isValidForDialect
				} else {
					log.Debug("Source modifier for target time/timestamp/datetime type is not a simple number.", zap.String("src_mod", srcModifierContent))
				}
			} else {
				log.Debug("Source modifier for target time/timestamp/datetime type has more than one part (expected single precision).", zap.String("src_mod", srcModifierContent))
			}
		}

		if canApplySrcModifier && finalAppliedModifier != "" {
			log.Debug("Successfully determined source modifier can be applied to the base of mapped config type.",
				zap.String("target_base_for_apply", baseTypeOfMappedConfig),
				zap.String("modifier_to_apply", finalAppliedModifier))
			return fmt.Sprintf("%s(%s)", baseTypeOfMappedConfig, finalAppliedModifier)
		}
		if srcModifierContent != "" && finalAppliedModifier == "" {
			log.Debug("Source modifier was present but invalidated for target dialect (e.g., out of range). Using base of mapped config type without any modifier.",
				zap.String("result", baseTypeOfMappedConfig))
			return baseTypeOfMappedConfig
		}
		if !canApplySrcModifier && modifierOfMappedConfig != "" {
			log.Debug("Source modifier not applicable or transferable. Mapped type from config has its own modifier, using that.",
				zap.String("result", mappedTypeFromConfig))
			return mappedTypeFromConfig
		}
		log.Debug("Source modifier not applied, and mapped type from config has no (or irrelevant) modifier. Using base of mapped config type only.",
			zap.String("result", baseTypeOfMappedConfig))
		return baseTypeOfMappedConfig
	default:
		log.Error("Unknown ModifierHandlingStrategy encountered in applyTypeModifiers. THIS IS A BUG.",
			zap.String("unknown_strategy", string(modHandling)))
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
	case "mysql": return "LONGTEXT"
	case "postgres": return "TEXT"
	case "sqlite": return "TEXT"
	default:
		s.logger.Error("Unknown destination dialect for generic fallback type.", zap.String("unknown_dialect", s.dstDialect))
		return "TEXT"
	}
}
