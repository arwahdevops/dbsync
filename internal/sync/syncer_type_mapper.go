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

// MappedTypeResult menyimpan hasil pemetaan tipe termasuk ekspresi USING untuk PostgreSQL.
type MappedTypeResult struct {
	FinalMappedType      string
	PostgresUsingExpr    string
	ModifierHandlingUsed config.ModifierHandlingStrategy
}

// --- Helper Functions for Type Category Checks (kept for applyTypeModifiers logic) ---

func isStringType(normTypeName string) bool {
	return strings.Contains(normTypeName, "char") || strings.Contains(normTypeName, "text") ||
		strings.Contains(normTypeName, "clob") || normTypeName == "enum" ||
		normTypeName == "set" || normTypeName == "uuid" || normTypeName == "json" || normTypeName == "xml"
}

func isBinaryType(normTypeName string) bool {
	return strings.Contains(normTypeName, "binary") || strings.Contains(normTypeName, "blob") ||
		normTypeName == "bytea" || normTypeName == "bit" || normTypeName == "varbit"
}

func isNumericType(normTypeName string) bool {
	return isIntegerType(normTypeName) || strings.Contains(normTypeName, "decimal") ||
		strings.Contains(normTypeName, "numeric") || strings.Contains(normTypeName, "float") ||
		strings.Contains(normTypeName, "double") || strings.Contains(normTypeName, "real") ||
		normTypeName == "money"
}

func isIntegerType(normTypeName string) bool {
	return strings.Contains(normTypeName, "int") || strings.Contains(normTypeName, "serial")
}

func isPrecisionRelevant(normTypeName string) bool {
	return strings.Contains(normTypeName, "decimal") || strings.Contains(normTypeName, "numeric") ||
		normTypeName == "time" || normTypeName == "timetz" || normTypeName == "timestamp" ||
		normTypeName == "timestamptz" || normTypeName == "datetime"
}

func isScaleRelevant(normTypeName string) bool {
	return strings.Contains(normTypeName, "decimal") || strings.Contains(normTypeName, "numeric")
}

// --- Core Type Mapping Logic ---

// populateMappedTypesForSourceColumns mengisi MappedType untuk kolom sumber berdasarkan konfigurasi.
func (s *SchemaSyncer) populateMappedTypesForSourceColumns(columns []ColumnInfo, tableName string) error {
	log := s.logger.With(zap.String("table", tableName), zap.String("phase", "populate-source-mapped-types"))
	for i := range columns {
		var typeToMapForKey string
		if columns[i].IsGenerated {
			typeToMapForKey = extractBaseTypeFromGenerated(columns[i].Type, log)
			log.Debug("Using extracted base type as mapping key for generated source column.",
				zap.String("column", columns[i].Name),
				zap.String("original_full_type", columns[i].Type),
				zap.String("base_type_for_mapping_key", typeToMapForKey))
		} else {
			typeToMapForKey = columns[i].Type
		}

		mappedResult, mapErr := s.mapDataType(typeToMapForKey, columns[i].Type)
		if mapErr != nil {
			log.Error("Fatal type mapping error for source column",
				zap.String("column", columns[i].Name),
				zap.String("type_key_used_for_lookup", typeToMapForKey),
				zap.String("original_full_type", columns[i].Type),
				zap.Error(mapErr))
			return fmt.Errorf("fatal type mapping error for column '%s' (type key: '%s', full type: '%s') in table '%s': %w",
				columns[i].Name, typeToMapForKey, columns[i].Type, tableName, mapErr)
		}

		columns[i].MappedType = mappedResult.FinalMappedType
		log.Debug("Populated MappedType for source column",
			zap.String("column", columns[i].Name),
			zap.String("original_full_type", columns[i].Type),
			zap.String("final_mapped_type", columns[i].MappedType),
			zap.String("modifier_handling_used", string(mappedResult.ModifierHandlingUsed)),
			zap.String("postgres_using_expr_from_map", mappedResult.PostgresUsingExpr))
	}
	return nil
}

// populateMappedTypesForDestinationColumns mengisi MappedType untuk kolom tujuan (biasanya untuk perbandingan).
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

// mapDataType menentukan tipe data tujuan berdasarkan tipe sumber dan konfigurasi.
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
		log.Debug("Attempting to map type using loaded internal type mapping configuration.")
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
					zap.String("target_type_defined", sm.TargetType),
					zap.String("modifier_handling", string(sm.ModifierHandling)),
					zap.String("postgres_using_expr", sm.PostgresUsingExpr))
				result.ModifierHandlingUsed = sm.ModifierHandling
				if result.ModifierHandlingUsed == "" {
					result.ModifierHandlingUsed = config.ModifierHandlingApplySource
				}
				result.PostgresUsingExpr = sm.PostgresUsingExpr
				result.FinalMappedType = s.applyTypeModifiers(fullOriginalSrcType, sm.TargetType, result.ModifierHandlingUsed)
				return result, nil
			}
		}
		if mappedInfo, ok := typeMappingConfigEntry.Mappings[normalizedSrcTypeKeyForLookup]; ok {
			log.Info("Matched standard type mapping from internal config.",
				zap.String("normalized_source_key_used", normalizedSrcTypeKeyForLookup),
				zap.String("target_type_defined", mappedInfo.TargetType),
				zap.String("modifier_handling", string(mappedInfo.ModifierHandling)),
				zap.String("postgres_using_expr", mappedInfo.PostgresUsingExpr))
			result.ModifierHandlingUsed = mappedInfo.ModifierHandling
			if result.ModifierHandlingUsed == "" {
				result.ModifierHandlingUsed = config.ModifierHandlingApplySource
			}
			result.PostgresUsingExpr = mappedInfo.PostgresUsingExpr
			result.FinalMappedType = s.applyTypeModifiers(fullOriginalSrcType, mappedInfo.TargetType, result.ModifierHandlingUsed)
			return result, nil
		}
		log.Debug("No specific match (special or standard) found in internal type mapping configuration for this dialect pair and source type key.",
			zap.String("normalized_src_type_key_for_lookup", normalizedSrcTypeKeyForLookup))
	} else {
		log.Debug("No internal type mapping configuration defined for current dialect pair.")
	}

	if s.srcDialect == s.dstDialect {
		log.Info("Source and destination dialects are the same. Using source type directly (after applying modifiers).",
			zap.String("using_source_type_for_target_definition", fullOriginalSrcType))
		result.FinalMappedType = s.applyTypeModifiers(fullOriginalSrcType, fullOriginalSrcType, result.ModifierHandlingUsed)
		return result, nil
	}

	fallbackType := s.getGenericFallbackType()
	log.Warn("No specific type mapping found (config, same dialect). Using generic fallback type.",
		zap.String("fallback_target_type_used", fallbackType))
	result.FinalMappedType = s.applyTypeModifiers(fullOriginalSrcType, fallbackType, result.ModifierHandlingUsed)
	return result, nil
}

// applyTypeModifiers menerapkan modifier (panjang, presisi, skala) ke tipe dasar target
// berdasarkan strategi yang diberikan dan modifier dari tipe sumber.
func (s *SchemaSyncer) applyTypeModifiers(srcTypeRaw, mappedTypeFromConfig string, modHandling config.ModifierHandlingStrategy) string {
	log := s.logger.With(
		zap.String("src_type_raw_for_modifier", srcTypeRaw),
		zap.String("mapped_type_from_config", mappedTypeFromConfig),
		zap.String("modifier_handling_strategy", string(modHandling)),
		zap.String("dst_dialect", s.dstDialect),
		zap.String("action", "applyTypeModifiers"),
	)

	var baseTypeOfMappedConfig, modifierOfMappedConfig string
	if matchesMappedMod := regexp.MustCompile(`^(.+?)\s*\((.+)\)$`).FindStringSubmatch(mappedTypeFromConfig); len(matchesMappedMod) > 2 {
		baseTypeOfMappedConfig = strings.TrimSpace(matchesMappedMod[1])
		modifierOfMappedConfig = strings.TrimSpace(matchesMappedMod[2])
	} else {
		baseTypeOfMappedConfig = strings.TrimSpace(mappedTypeFromConfig)
	}
	normMappedBaseTypeOnly := normalizeTypeName(baseTypeOfMappedConfig)

	log.Debug("Parsed 'mappedTypeFromConfig' for modifier application.",
		zap.String("derived_base_from_mapped_config", baseTypeOfMappedConfig),
		zap.String("derived_modifier_from_mapped_config", modifierOfMappedConfig))

	switch modHandling {
	case config.ModifierHandlingUseTargetDefined:
		log.Debug("Strategy 'use_target_defined'. Using mapped type from config directly.",
			zap.String("result", mappedTypeFromConfig))
		return mappedTypeFromConfig

	case config.ModifierHandlingIgnoreSource:
		log.Debug("Strategy 'ignore_source'. Using mapped type from config (could be base or with its own defined modifier), source modifiers ignored.",
			zap.String("result", mappedTypeFromConfig))
		return mappedTypeFromConfig

	case config.ModifierHandlingApplySource:
		log.Debug("Strategy 'apply_source': Attempting to apply source modifiers to the base of 'mappedTypeFromConfig'.")

		var srcModifierContent string
		if srcModifierMatches := regexp.MustCompile(`\((.+?)\)`).FindStringSubmatch(srcTypeRaw); len(srcModifierMatches) > 1 {
			srcModifierContent = strings.TrimSpace(srcModifierMatches[1])
		}
		normSrcBaseTypeOnly := normalizeTypeName(strings.Split(srcTypeRaw, "(")[0])

		if srcModifierContent == "" {
			log.Debug("Source type has no modifier. Using mapped type from config as is.",
				zap.String("result", mappedTypeFromConfig))
			return mappedTypeFromConfig
		}

		if (normSrcBaseTypeOnly == "enum" || normSrcBaseTypeOnly == "set") && modifierOfMappedConfig != "" {
			log.Debug("Source is ENUM/SET and mapped type from config has its own modifier. Preferring mapped type's own modifier.",
				zap.String("result", mappedTypeFromConfig))
			return mappedTypeFromConfig
		}

		modifierIsGenerallyTransferable := regexp.MustCompile(`^\s*\d+(?:\s*,\s*\d+)?\s*$`).MatchString(srcModifierContent)
		if modifierIsGenerallyTransferable {
			switch normSrcBaseTypeOnly {
			case "text", "tinytext", "mediumtext", "longtext", "json", "uuid", "clob",
				"blob", "tinyblob", "mediumblob", "longblob", "bytea":
				modifierIsGenerallyTransferable = false
			case "int", "tinyint", "smallint", "mediumint", "bigint", "integer":
				isBitToBoolOrBit := (normSrcBaseTypeOnly == "bit" && srcModifierContent == "1" && (normMappedBaseTypeOnly == "boolean" || normMappedBaseTypeOnly == "tinyint")) ||
					(normSrcBaseTypeOnly == "bit" && normMappedBaseTypeOnly == "bit") ||
					(normSrcBaseTypeOnly == "bit" && normMappedBaseTypeOnly == "varbit")
				if !isBitToBoolOrBit {
					modifierIsGenerallyTransferable = false
				}
			}
		}

		if !modifierIsGenerallyTransferable {
			log.Debug("Source modifier is not considered generally transferable. Using mapped type from config as is (or its base if it has no modifier).",
				zap.String("src_modifier_content", srcModifierContent))
			return mappedTypeFromConfig
		}

		canApplySrcModifierToMappedBase := false
		finalAppliedModifier := srcModifierContent

		switch {
		case isStringType(normMappedBaseTypeOnly):
			if !isLargeTextOrBlob(normMappedBaseTypeOnly) && normMappedBaseTypeOnly != "uuid" &&
				normMappedBaseTypeOnly != "json" && normMappedBaseTypeOnly != "xml" &&
				normMappedBaseTypeOnly != "enum" && normMappedBaseTypeOnly != "set" {
				if _, err := strconv.Atoi(srcModifierContent); err == nil {
					canApplySrcModifierToMappedBase = true
				}
			}
		case isBinaryType(normMappedBaseTypeOnly):
			if normMappedBaseTypeOnly == "bit" || normMappedBaseTypeOnly == "varbit" ||
				normMappedBaseTypeOnly == "binary" || normMappedBaseTypeOnly == "varbinary" {
				if _, err := strconv.Atoi(srcModifierContent); err == nil {
					canApplySrcModifierToMappedBase = true
				}
			} else if !isLargeTextOrBlob(normMappedBaseTypeOnly) && normMappedBaseTypeOnly != "bytea" {
				if _, err := strconv.Atoi(srcModifierContent); err == nil {
					canApplySrcModifierToMappedBase = true
				}
			}
		case isScaleRelevant(normMappedBaseTypeOnly):
			parts := strings.Split(srcModifierContent, ",")
			if len(parts) == 1 || len(parts) == 2 {
				if pVal, errP := strconv.Atoi(strings.TrimSpace(parts[0])); errP == nil && pVal > 0 {
					validScale := true
					if len(parts) == 2 {
						if sVal, errS := strconv.Atoi(strings.TrimSpace(parts[1])); errS != nil || sVal < 0 || sVal > pVal {
							validScale = false
						}
					}
					if validScale {
						canApplySrcModifierToMappedBase = true
					}
				}
			}
		case isPrecisionRelevant(normMappedBaseTypeOnly):
			if s.dstDialect == "sqlite" && (strings.Contains(normMappedBaseTypeOnly, "time") ||
				strings.Contains(normMappedBaseTypeOnly, "timestamp") || normMappedBaseTypeOnly == "datetime") {
				canApplySrcModifierToMappedBase = false
				finalAppliedModifier = ""
				break
			}
			partsTime := strings.Split(srcModifierContent, ",")
			if len(partsTime) == 1 {
				if pVal, err := strconv.Atoi(strings.TrimSpace(partsTime[0])); err == nil {
					isValidForDialect := false
					if s.dstDialect == "mysql" && (normMappedBaseTypeOnly == "datetime" || normMappedBaseTypeOnly == "timestamp" || normMappedBaseTypeOnly == "time") {
						if pVal >= 0 && pVal <= 6 {
							finalAppliedModifier = strconv.Itoa(pVal)
							isValidForDialect = true
						} else {
							finalAppliedModifier = "" // Modifier sumber tidak valid untuk MySQL datetime/time/timestamp
							isValidForDialect = false
						}
					} else if s.dstDialect == "postgres" && (strings.HasPrefix(normMappedBaseTypeOnly, "time") || strings.HasPrefix(normMappedBaseTypeOnly, "timestamp")) {
						finalAppliedModifier = strconv.Itoa(pVal)
						isValidForDialect = true
					} else if normMappedBaseTypeOnly == "time" || normMappedBaseTypeOnly == "timestamp" || normMappedBaseTypeOnly == "datetime" {
						finalAppliedModifier = strings.TrimSpace(partsTime[0])
						isValidForDialect = true
					}
					canApplySrcModifierToMappedBase = isValidForDialect
				}
			}
		}

		if canApplySrcModifierToMappedBase && finalAppliedModifier != "" {
			resultType := fmt.Sprintf("%s(%s)", baseTypeOfMappedConfig, finalAppliedModifier)
			log.Debug("Applied source modifier to mapped base type.", zap.String("result", resultType))
			return resultType
		}

		// *** PERUBAHAN LOGIKA FALLBACK DI SINI ***
		// Jika modifier sumber tidak bisa diterapkan (canApplySrcModifierToMappedBase=false)
		// atau jika finalAppliedModifier menjadi kosong (karena presisi sumber tidak valid untuk target),
		// kita gunakan HANYA tipe dasar dari target (baseTypeOfMappedConfig).
		// Ini akan menghasilkan "DATETIME" untuk kasus yang gagal, sesuai ekspektasi tes.
		log.Debug("Source modifier could not be applied or was invalid for the target base type. Using the base of the mapped type from config.",
			zap.Bool("can_apply_src_modifier_to_mapped_base", canApplySrcModifierToMappedBase),
			zap.String("final_applied_modifier_value_from_source_attempt", finalAppliedModifier),
			zap.String("base_of_mapped_config_used_as_result", baseTypeOfMappedConfig),
			zap.String("full_mapped_type_from_config_for_reference", mappedTypeFromConfig))
		return baseTypeOfMappedConfig // <-- KEMBALIKAN HANYA TIPE DASAR TARGET

	default:
		log.Error("Unknown ModifierHandlingStrategy in applyTypeModifiers. This is a bug.",
			zap.String("unknown_strategy", string(modHandling)))
		return mappedTypeFromConfig
	}
}

// getGenericFallbackType mengembalikan tipe data fallback generik untuk dialek tujuan.
func (s *SchemaSyncer) getGenericFallbackType() string {
	switch s.dstDialect {
	case "mysql":
		return "LONGTEXT"
	case "postgres":
		return "TEXT"
	case "sqlite":
		return "TEXT"
	default:
		s.logger.Error("Unknown destination dialect encountered when determining generic fallback type. Defaulting to TEXT.",
			zap.String("unknown_dialect", s.dstDialect))
		return "TEXT"
	}
}
