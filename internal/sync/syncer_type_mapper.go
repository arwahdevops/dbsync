package sync

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/arwahdevops/dbsync/internal/config" // Diperlukan untuk config.GetTypeMappingForDialects
)

// populateMappedTypesForSourceColumns mengisi field MappedType untuk kolom sumber.
func (s *SchemaSyncer) populateMappedTypesForSourceColumns(columns []ColumnInfo, tableName string) error {
	log := s.logger.With(zap.String("table", tableName), zap.String("phase", "populate-source-mapped-types"))
	for i := range columns {
		// Untuk kolom yang di-generate, MappedType biasanya sama dengan Type aslinya,
		// karena kita tidak melakukan pemetaan tipe untuknya, hanya mengambil tipe dasarnya
		// saat perbandingan. DDL akan menggunakan tipe asli sumber.
		if columns[i].IsGenerated {
			// Ekstrak tipe dasar dari definisi generated untuk digunakan sebagai MappedType sementara,
			// ini membantu dalam logika perbandingan jika tujuan juga generated.
			// Namun, untuk DDL CREATE/ADD, kita akan menggunakan `columns[i].Type` asli.
			baseSrcTypeForGenerated := extractBaseTypeFromGenerated(columns[i].Type, log)
			columns[i].MappedType = baseSrcTypeForGenerated
			log.Debug("Using extracted base type as MappedType for generated source column, for comparison purposes.",
				zap.String("column", columns[i].Name),
				zap.String("original_full_type", columns[i].Type),
				zap.String("mapped_type_for_comp", columns[i].MappedType))
			continue
		}
		// Untuk kolom non-generated, lakukan pemetaan tipe penuh.
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

// populateMappedTypesForDestinationColumns mengisi MappedType untuk kolom tujuan (biasanya sama dengan Type-nya).
// Ini digunakan untuk konsistensi dalam logika perbandingan.
func (s *SchemaSyncer) populateMappedTypesForDestinationColumns(columns []ColumnInfo) {
	for i := range columns {
		if columns[i].MappedType == "" { // Hanya jika belum diisi
			if columns[i].IsGenerated {
				// Sama seperti source, gunakan tipe dasar yang diekstrak untuk perbandingan.
				columns[i].MappedType = extractBaseTypeFromGenerated(columns[i].Type, s.logger)
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
	normalizedSrcTypeKey := normalizeTypeName(fullSrcTypeLower) // normalizeTypeName dari compare_utils.go

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
		// Kita tetap panggil applyTypeModifiers untuk memastikan format modifier konsisten,
		// meskipun base type tidak berubah.
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

	// Hapus modifier yang mungkin sudah ada di mappedBaseType (misal, dari "VARCHAR(255)" di typemap.json)
	// agar kita bisa menerapkan modifier dari srcTypeRaw.
	baseTypeWithoutExistingModifiers := strings.Split(mappedBaseType, "(")[0]
	normBaseType := normalizeTypeName(baseTypeWithoutExistingModifiers) // normalizeTypeName dari compare_utils.go

	if modifierContent == "" {
		log.Debug("No modifier found in source type. Returning mapped base type as is.", zap.String("final_type", baseTypeWithoutExistingModifiers))
		return baseTypeWithoutExistingModifiers
	}

	canHaveModifier := false
	finalModifier := modifierContent // Default ke modifier sumber

	switch {
	// isStringType, isBinaryType, dll. dari compare_utils.go
	case isStringType(normBaseType):
		// Kecualikan tipe teks besar yang tidak mengambil panjang (atau panjangnya implisit maks)
		if !isLargeTextOrBlob(normBaseType) && // isLargeTextOrBlob dari compare_columns.go
			normBaseType != "uuid" && normBaseType != "json" && normBaseType != "xml" &&
			normBaseType != "enum" && normBaseType != "set" { // enum/set ditangani khusus
			canHaveModifier = true
		}
	case isBinaryType(normBaseType):
		if !isLargeTextOrBlob(normBaseType) && normBaseType != "bytea" {
			canHaveModifier = true
		}
	case isScaleRelevant(normBaseType): // decimal, numeric
		canHaveModifier = true
	case isPrecisionRelevant(normBaseType) && (strings.Contains(normBaseType, "time") || strings.Contains(normBaseType, "timestamp")):
		// Untuk tipe waktu, modifier biasanya hanya presisi tunggal (bukan P,S)
		parts := strings.Split(modifierContent, ",")
		if len(parts) == 1 {
			if _, err := strconv.Atoi(strings.TrimSpace(parts[0])); err == nil {
				finalModifier = strings.TrimSpace(parts[0]) // Ambil hanya presisi
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
