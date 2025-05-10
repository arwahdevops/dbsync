package sync

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/arwahdevops/dbsync/internal/config" // Diperlukan untuk config.GetTypeMappingForDialects
)

// --- Fungsi yang dipindahkan dari compare_utils.go ---

// normalizeTypeName menormalisasi nama tipe data untuk perbandingan.
// Menghapus ukuran generik, spasi berlebih, dan modifier umum.
func normalizeTypeName(typeName string) string {
	name := strings.ToLower(strings.TrimSpace(typeName))

	// Hapus modifier umum MySQL terlebih dahulu
	name = strings.ReplaceAll(name, " unsigned", "")
	name = strings.ReplaceAll(name, " zerofill", "")
	name = strings.TrimSpace(name) // Penting setelah replace

	// Sekarang hapus ukuran/modifier generik dalam tanda kurung, misal (11) atau (255) atau (10,2)
	name = regexp.MustCompile(`\s*\([^)]*\)\s*$`).ReplaceAllString(name, "")
	name = regexp.MustCompile(`\s*\([^)]*\)$`).ReplaceAllString(name, "")
	name = strings.TrimSpace(name)

	// Alias umum
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

// isStringType memeriksa apakah tipe adalah tipe string (setelah normalisasi nama tipe).
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

// isBinaryType memeriksa apakah tipe adalah tipe biner (setelah normalisasi nama tipe).
func isBinaryType(normTypeName string) bool {
	return strings.Contains(normTypeName, "binary") ||
		strings.Contains(normTypeName, "blob") ||
		normTypeName == "bytea"
}

// isNumericType memeriksa apakah tipe adalah tipe numerik (integer atau desimal/float).
func isNumericType(normTypeName string) bool {
	return isIntegerType(normTypeName) ||
		strings.Contains(normTypeName, "decimal") ||
		strings.Contains(normTypeName, "numeric") ||
		strings.Contains(normTypeName, "float") ||
		strings.Contains(normTypeName, "double") ||
		strings.Contains(normTypeName, "real") ||
		strings.Contains(normTypeName, "money")
}

// isIntegerType memeriksa apakah tipe adalah tipe integer (termasuk serial).
func isIntegerType(normTypeName string) bool {
	return strings.Contains(normTypeName, "int") ||
		strings.Contains(normTypeName, "serial")
}

// isPrecisionRelevant memeriksa apakah presisi relevan untuk tipe ini.
func isPrecisionRelevant(normTypeName string) bool {
	return strings.Contains(normTypeName, "decimal") ||
		strings.Contains(normTypeName, "numeric") ||
		strings.Contains(normTypeName, "time") ||
		(strings.Contains(normTypeName, "datetime") && strings.Contains(normTypeName, "offset"))
}

// isScaleRelevant memeriksa apakah skala relevan untuk tipe ini.
func isScaleRelevant(normTypeName string) bool {
	return strings.Contains(normTypeName, "decimal") ||
		strings.Contains(normTypeName, "numeric")
}

// --- Fungsi asli dari syncer_type_mapper.go ---

// populateMappedTypesForSourceColumns mengisi field MappedType untuk kolom sumber.
func (s *SchemaSyncer) populateMappedTypesForSourceColumns(columns []ColumnInfo, tableName string) error {
	log := s.logger.With(zap.String("table", tableName), zap.String("phase", "populate-source-mapped-types"))
	for i := range columns {
		if columns[i].IsGenerated {
			baseSrcTypeForGenerated := extractBaseTypeFromGenerated(columns[i].Type, log) // extractBaseTypeFromGenerated dari compare_columns.go
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

// populateMappedTypesForDestinationColumns mengisi MappedType untuk kolom tujuan.
func (s *SchemaSyncer) populateMappedTypesForDestinationColumns(columns []ColumnInfo) {
	for i := range columns {
		if columns[i].MappedType == "" {
			if columns[i].IsGenerated {
				columns[i].MappedType = extractBaseTypeFromGenerated(columns[i].Type, s.logger) // extractBaseTypeFromGenerated dari compare_columns.go
			} else {
				columns[i].MappedType = columns[i].Type
			}
		}
	}
}

// mapDataType memetakan tipe data dari dialek sumber ke dialek tujuan.
func (s *SchemaSyncer) mapDataType(srcType string) (string, error) {
	log := s.logger.With(zap.String("source_type_raw", srcType),
		zap.String("src_dialect", s.srcDialect), zap.String("dst_dialect", s.dstDialect),
		zap.String("action", "mapDataType"))

	fullSrcTypeLower := strings.ToLower(strings.TrimSpace(srcType))
	normalizedSrcTypeKey := normalizeTypeName(fullSrcTypeLower) // Fungsi yang baru dipindahkan

	typeMappingConfigEntry := config.GetTypeMappingForDialects(s.srcDialect, s.dstDialect)

	if typeMappingConfigEntry != nil {
		log.Debug("Found external type mapping configuration for dialect pair.")
		for _, sm := range typeMappingConfigEntry.SpecialMappings {
			re, errComp := regexp.Compile(sm.SourceTypePattern)
			if errComp != nil {
				log.Error("Invalid regex pattern in special type mapping, skipping this pattern.",
					zap.String("pattern", sm.SourceTypePattern), zap.Error(errComp))
				continue
			}
			if re.MatchString(fullSrcTypeLower) {
				log.Info("Matched special type mapping",
					zap.String("pattern", sm.SourceTypePattern),
					zap.String("raw_source_type", fullSrcTypeLower),
					zap.String("target_base_type", sm.TargetType))
				return s.applyTypeModifiers(srcType, sm.TargetType), nil
			}
		}

		if mappedType, ok := typeMappingConfigEntry.Mappings[normalizedSrcTypeKey]; ok {
			log.Info("Matched standard type mapping",
				zap.String("normalized_source_key", normalizedSrcTypeKey),
				zap.String("raw_source_type", srcType),
				zap.String("target_base_type", mappedType))
			return s.applyTypeModifiers(srcType, mappedType), nil
		}
	} else {
		log.Debug("No external type mapping configuration found for current dialect pair. Will use internal fallbacks or direct type if dialects are same.")
	}

	if s.srcDialect == s.dstDialect {
		log.Info("Source and destination dialects are the same, using source type directly (with modifiers).",
			zap.String("source_type", srcType))
		return s.applyTypeModifiers(srcType, srcType), nil
	}

	fallbackType := s.getGenericFallbackType()
	log.Warn("No specific type mapping found through external config or direct match for different dialects. Using generic fallback type.",
		zap.String("normalized_source_key_used_for_lookup", normalizedSrcTypeKey),
		zap.String("original_source_type", srcType),
		zap.String("fallback_target_type", fallbackType))
	return s.applyTypeModifiers(srcType, fallbackType), nil
}

// applyTypeModifiers menerapkan modifier (panjang, presisi, skala) dari tipe sumber ke tipe dasar tujuan.
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
	normBaseType := normalizeTypeName(baseTypeWithoutExistingModifiers) // Fungsi yang baru dipindahkan

	if modifierContent == "" {
		log.Debug("No modifier found in source type. Returning mapped base type as is.", zap.String("final_type", baseTypeWithoutExistingModifiers))
		return baseTypeWithoutExistingModifiers
	}

	canHaveModifier := false
	finalModifier := modifierContent

	switch {
	case isStringType(normBaseType): // Fungsi yang baru dipindahkan
		if !isLargeTextOrBlob(normBaseType) && // isLargeTextOrBlob dari compare_columns.go
			normBaseType != "uuid" && normBaseType != "json" && normBaseType != "xml" &&
			normBaseType != "enum" && normBaseType != "set" {
			canHaveModifier = true
		}
	case isBinaryType(normBaseType): // Fungsi yang baru dipindahkan
		if !isLargeTextOrBlob(normBaseType) && normBaseType != "bytea" { // isLargeTextOrBlob dari compare_columns.go
			canHaveModifier = true
		}
	case isScaleRelevant(normBaseType): // Fungsi yang baru dipindahkan (decimal, numeric)
		canHaveModifier = true
	case isPrecisionRelevant(normBaseType) && (strings.Contains(normBaseType, "time") || strings.Contains(normBaseType, "timestamp")): // Fungsi yang baru dipindahkan
		parts := strings.Split(modifierContent, ",")
		if len(parts) == 1 {
			if _, err := strconv.Atoi(strings.TrimSpace(parts[0])); err == nil {
				finalModifier = strings.TrimSpace(parts[0])
				canHaveModifier = true
			}
		}
	}

	if canHaveModifier {
		log.Debug("Applying modifier to base type.", zap.String("modifier_to_apply", finalModifier))
		return fmt.Sprintf("%s(%s)", baseTypeWithoutExistingModifiers, finalModifier)
	}

	log.Debug("Modifier found in source, but target base type does not typically support it or modifier format is incompatible; returning base type without modifier.",
		zap.String("modifier_content_from_source", modifierContent),
		zap.String("final_type", baseTypeWithoutExistingModifiers))
	return baseTypeWithoutExistingModifiers
}

// getGenericFallbackType mengembalikan tipe fallback generik berdasarkan dialek tujuan.
func (s *SchemaSyncer) getGenericFallbackType() string {
	switch s.dstDialect {
	case "mysql":
		return "LONGTEXT"
	case "postgres":
		return "TEXT"
	case "sqlite":
		return "TEXT"
	default:
		s.logger.Error("Unknown destination dialect for generic fallback type, defaulting to TEXT.", zap.String("unknown_dialect", s.dstDialect))
		return "TEXT"
	}
}
