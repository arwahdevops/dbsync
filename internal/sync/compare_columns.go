// internal/sync/compare_columns.go
package sync

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/apd/v3"
	"go.uber.org/zap"
)

// --- Fungsi Helper Ekstraksi dan Normalisasi Tipe/Kolasi/Default ---

func extractBaseTypeFromGenerated(fullType string, log *zap.Logger) string {
	trimmedFullType := strings.TrimSpace(fullType)
	originalInputForLog := trimmedFullType // Simpan untuk logging jika fallback

	// Pola Regex untuk klausa yang memulai ekspresi generated (paling jelas).
	// Diurutkan dari yang paling spesifik/panjang.
	// Ini akan mencari klausa AS (...) atau GENERATED ... AS/IDENTITY (...)
	// dan mengambil semua sebelumnya sebagai tipe.
	// (?i) untuk case-insensitive.
	// Grup 1: (.*?) - Tipe data dasar (non-greedy)
	//         Diikuti oleh spasi wajib \s+
	//         Lalu klausa generated.
	regexesForGeneratedClauses := []*regexp.Regexp{
		regexp.MustCompile(`(?i)(.*?)\s+GENERATED\s+BY\s+DEFAULT\s+AS\s+IDENTITY\s*\(.*\)`),
		regexp.MustCompile(`(?i)(.*?)\s+GENERATED\s+ALWAYS\s+AS\s+IDENTITY\s*\(.*\)`),
		regexp.MustCompile(`(?i)(.*?)\s+GENERATED\s+ALWAYS\s+AS\s*\(.*\)`),
		regexp.MustCompile(`(?i)(.*?)\s+AS\s*\(.*\)`), // Paling umum, harus setelah yang lebih spesifik
		// Untuk klausa identity tanpa kurung (biasanya di akhir string)
		regexp.MustCompile(`(?i)(.*?)\s+GENERATED\s+BY\s+DEFAULT\s+AS\s+IDENTITY\s*$`),
		regexp.MustCompile(`(?i)(.*?)\s+GENERATED\s+ALWAYS\s+AS\s+IDENTITY\s*$`),
	}

	for i, re := range regexesForGeneratedClauses {
		matches := re.FindStringSubmatch(trimmedFullType)
		if len(matches) > 1 {
			baseType := strings.TrimSpace(matches[1])
			if baseType != "" {
				// Validasi tambahan: pastikan baseType tidak kosong dan tidak mengandung kata kunci generated itu sendiri
				// Ini untuk menghindari kecocokan yang salah jika tipe tidak ada di awal.
				upperBaseType := strings.ToUpper(baseType)
				if upperBaseType == "GENERATED" || upperBaseType == "AS" || upperBaseType == "IDENTITY" ||
					strings.HasPrefix(upperBaseType, "GENERATED ") ||
					strings.HasPrefix(upperBaseType, "AS ") {
					log.Debug("Regex for generated clause matched, but extracted base type seems invalid (is a keyword itself). Continuing search.",
						zap.Int("regex_index", i),
						zap.String("pattern", re.String()),
						zap.String("matched_base", baseType))
					continue // Coba regex berikutnya
				}

				log.Debug("Successfully extracted base type by matching a generated clause starter.",
					zap.Int("regex_index", i),
					zap.String("pattern", re.String()),
					zap.String("original_trimmed_full_type", trimmedFullType),
					zap.String("extracted_base_type", baseType))
				return baseType
			}
		}
	}

	// Jika tidak ada klausa 'AS (...)' atau 'GENERATED ... AS ...' yang cocok,
	// coba cari kata kunci suffix (VIRTUAL, STORED, PERSISTENT) yang mungkin
	// mengikuti tipe dasar tanpa ekspresi eksplisit.
	// Ini harus lebih hati-hati.
	// Grup 1: Tipe data (non-greedy)
	// Grup 2: Kata kunci suffix
	reSuffixKeywords := regexp.MustCompile(`(?i)^(.+?)\s+(VIRTUAL|STORED|PERSISTENT)\s*$`)
	matchesSuffix := reSuffixKeywords.FindStringSubmatch(trimmedFullType)
	if len(matchesSuffix) > 2 { // matchesSuffix[0] adalah full match, [1] grup 1, [2] grup 2
		baseType := strings.TrimSpace(matchesSuffix[1])
		suffixKeyword := matchesSuffix[2]

		// Pastikan baseType yang diekstrak tidak terlalu pendek atau hanya kata kunci lain.
		// Ini adalah heuristic.
		if baseType != "" && len(baseType) > 1 && !isCommonSQLKeyword(strings.ToUpper(baseType)) {
			log.Debug("Extracted base type by finding a suffix keyword (VIRTUAL, STORED, PERSISTENT).",
				zap.String("original_trimmed_full_type", trimmedFullType),
				zap.String("extracted_base_type", baseType),
				zap.String("suffix_keyword_found", suffixKeyword))
			return baseType
		}
		log.Debug("Suffix keyword found, but extracted base type before it seems invalid or too short. Ignoring this match.",
			zap.String("potential_base_type", baseType),
			zap.String("suffix_keyword", suffixKeyword))
	}

	// Fallback terakhir: jika ada "GENERATED" atau "IDENTITY" di akhir tanpa "AS"
	// Ini untuk kasus seperti "INT GENERATED"
	reSimpleSuffix := regexp.MustCompile(`(?i)^(.+?)\s+(GENERATED|IDENTITY)\s*$`)
	matchesSimpleSuffix := reSimpleSuffix.FindStringSubmatch(trimmedFullType)
	if len(matchesSimpleSuffix) > 2 {
		baseType := strings.TrimSpace(matchesSimpleSuffix[1])
		suffixKeyword := matchesSimpleSuffix[2]
		if baseType != "" && len(baseType) > 1 && !isCommonSQLKeyword(strings.ToUpper(baseType)) {
			log.Debug("Extracted base type by finding a simple suffix (GENERATED, IDENTITY) at the end.",
				zap.String("original_trimmed_full_type", trimmedFullType),
				zap.String("extracted_base_type", baseType),
				zap.String("suffix_keyword_found", suffixKeyword))
			return baseType
		}
	}


	log.Warn("Could not reliably extract base type from generated column definition using any defined method. Falling back to using the full (trimmed) type string.",
		zap.String("trimmed_full_type", originalInputForLog))
	return originalInputForLog // Fallback jika semua gagal
}

// isCommonSQLKeyword checks if a string is a common SQL keyword that is unlikely to be a base type
// when found одиночный before a generation marker.
func isCommonSQLKeyword(s string) bool {
	// Daftar ini bisa diperluas. Fokus pada kata kunci yang mungkin salah diidentifikasi sebagai tipe.
	keywords := []string{"AS", "GENERATED", "IDENTITY", "BY", "DEFAULT", "ALWAYS", "STORED", "VIRTUAL", "PERSISTENT"}
	for _, kw := range keywords {
		if s == kw {
			return true
		}
	}
	return false
}


// normalizeCollation menormalisasi nama kolasi.
// ... (fungsi ini dan helper lainnya seperti isLargeTextOrBlob, stripQuotes, isKnownDbFunction tetap sama) ...
func normalizeCollation(coll, dialect string) string {
	c := strings.ToLower(strings.TrimSpace(coll))
	if c == "default" || c == "" {
		return ""
	}
	return c
}

func isLargeTextOrBlob(normalizedTypeName string) bool {
	return strings.Contains(normalizedTypeName, "text") ||
		strings.Contains(normalizedTypeName, "blob") ||
		normalizedTypeName == "clob" ||
		normalizedTypeName == "bytea" ||
		normalizedTypeName == "json" ||
		normalizedTypeName == "xml"
}

func stripQuotes(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 {
		first, last := s[0], s[len(s)-1]
		if (first == '\'' && last == '\'') ||
			(first == '"' && last == '"') ||
			(first == '`' && last == '`') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

func isKnownDbFunction(normalizedDefaultValue string) bool {
	switch normalizedDefaultValue {
	case "current_timestamp", "current_date", "current_time",
		"nextval", "uuid_function":
		return true
	}
	return false
}
// --- Fungsi Perbandingan Inti ---

func (s *SchemaSyncer) determineSourceTypeForComparison(src ColumnInfo, columnLogger *zap.Logger) string {
	if src.IsGenerated {
		baseSrcType := extractBaseTypeFromGenerated(src.Type, columnLogger)
		columnLogger.Debug("Source column is generated. Using its extracted base type for comparison logic.",
			zap.String("original_src_type_with_expr", src.Type),
			zap.String("extracted_base_src_type_for_comp", baseSrcType))
		return baseSrcType
	}
	if src.MappedType != "" {
		return src.MappedType
	}
	columnLogger.Error("Source column MappedType is empty and column is not generated. Using original source type for comparison. This might not reflect the intended cross-dialect mapping and could lead to incorrect difference detection.",
		zap.String("src_original_type", src.Type))
	return src.Type
}

func (s *SchemaSyncer) getColumnModifications(src, dst ColumnInfo, log *zap.Logger) []string {
	diffs := []string{}
	columnLogger := log.With(zap.String("column", src.Name))
	srcTypeForComparison := s.determineSourceTypeForComparison(src, columnLogger)

	if !s.areTypesEquivalent(srcTypeForComparison, dst.Type, src, dst, columnLogger) {
		diffs = append(diffs, fmt.Sprintf("type (src: %s, dst: %s)", src.Type, dst.Type))
	}
	if src.IsNullable != dst.IsNullable {
		diffs = append(diffs, fmt.Sprintf("nullability (src: %t, dst: %t)", src.IsNullable, dst.IsNullable))
	}
	if !src.AutoIncrement && !src.IsGenerated && !dst.AutoIncrement && !dst.IsGenerated {
		srcDefValStr := ""; if src.DefaultValue.Valid { srcDefValStr = src.DefaultValue.String }
		dstDefValStr := ""; if dst.DefaultValue.Valid { dstDefValStr = dst.DefaultValue.String }
		if !s.areDefaultsEquivalent(srcDefValStr, dstDefValStr, srcTypeForComparison, columnLogger) {
			srcDefLog := "NULL"; if src.DefaultValue.Valid { srcDefLog = fmt.Sprintf("'%s'", src.DefaultValue.String) }
			dstDefLog := "NULL"; if dst.DefaultValue.Valid { dstDefLog = fmt.Sprintf("'%s'", dst.DefaultValue.String) }
			diffs = append(diffs, fmt.Sprintf("default (src: %s, dst: %s)", srcDefLog, dstDefLog))
		}
	}
	if src.AutoIncrement != dst.AutoIncrement {
		diffs = append(diffs, fmt.Sprintf("auto_increment (src: %t, dst: %t)", src.AutoIncrement, dst.AutoIncrement))
		columnLogger.Warn("AutoIncrement/Identity status difference detected.", zap.Bool("src_auto_inc", src.AutoIncrement), zap.Bool("dst_auto_inc", dst.AutoIncrement))
	}
	if src.IsGenerated != dst.IsGenerated {
		diffs = append(diffs, fmt.Sprintf("generated_status (src: %t, dst: %t)", src.IsGenerated, dst.IsGenerated))
		columnLogger.Warn("Generated column status difference detected.")
	} else if src.IsGenerated && dst.IsGenerated {
		columnLogger.Debug("Both columns are generated. Generation expression comparison is currently skipped.")
	}
	srcCollationStr := ""; if src.Collation.Valid { srcCollationStr = src.Collation.String }
	dstCollationStr := ""; if dst.Collation.Valid { dstCollationStr = dst.Collation.String }
	if isStringType(normalizeTypeName(srcTypeForComparison)) && isStringType(normalizeTypeName(dst.Type)) {
		if srcCollationStr != "" || dstCollationStr != "" {
			normSrcColl := normalizeCollation(srcCollationStr, s.srcDialect)
			normDstColl := normalizeCollation(dstCollationStr, s.dstDialect)
			if !strings.EqualFold(normSrcColl, normDstColl) {
				diffs = append(diffs, fmt.Sprintf("collation (src: %s, dst: %s)", srcCollationStr, dstCollationStr))
			}
		}
	}
	srcCommentStr := ""; if src.Comment.Valid { srcCommentStr = src.Comment.String }
	dstCommentStr := ""; if dst.Comment.Valid { dstCommentStr = dst.Comment.String }
	if srcCommentStr != dstCommentStr {
		columnLogger.Debug("Column comment difference detected (non-critical).", zap.String("src_comment", srcCommentStr), zap.String("dst_comment", dstCommentStr))
	}
	if len(diffs) > 0 {
		columnLogger.Info("Column differences identified.", zap.Strings("differences", diffs))
	}
	return diffs
}

func (s *SchemaSyncer) areTypesEquivalent(srcTypeForComp, dstRawType string, srcInfo, dstInfo ColumnInfo, log *zap.Logger) bool {
	log.Debug("Comparing types for equivalence",
		zap.String("src_type_for_comparison", srcTypeForComp),
		zap.String("dst_raw_type", dstRawType))
	normSrcCompType := normalizeTypeName(srcTypeForComp)
	normDstRawType := normalizeTypeName(dstRawType)
	log.Debug("Normalized base types for equivalence check",
		zap.String("norm_src_comp_type", normSrcCompType),
		zap.String("norm_dst_raw_type", normDstRawType))
	if normSrcCompType == normDstRawType {
		if isStringType(normSrcCompType) || isBinaryType(normSrcCompType) {
			srcLen := int64(-1); if srcInfo.Length.Valid { srcLen = srcInfo.Length.Int64 } else if isLargeTextOrBlob(normSrcCompType) { srcLen = -2 }
			dstLen := int64(-1); if dstInfo.Length.Valid { dstLen = dstInfo.Length.Int64 } else if isLargeTextOrBlob(normDstRawType) { dstLen = -2 }
			isSrcFixed := (strings.HasPrefix(normSrcCompType, "char") && !strings.HasPrefix(normSrcCompType, "varchar")) ||
				(strings.HasPrefix(normSrcCompType, "binary") && !strings.HasPrefix(normSrcCompType, "varbinary"))
			isDstFixed := (strings.HasPrefix(normDstRawType, "char") && !strings.HasPrefix(normDstRawType, "varchar")) ||
				(strings.HasPrefix(normDstRawType, "binary") && !strings.HasPrefix(normDstRawType, "varbinary"))
			if isSrcFixed != isDstFixed && srcLen == dstLen && srcLen > 0 {
				log.Debug("Type fixed/variable nature mismatch with same length, considered different.", zap.Bool("src_fixed", isSrcFixed), zap.Bool("dst_fixed", isDstFixed), zap.Int64("length", srcLen))
				return false
			}
			if !(srcLen == -2 && dstLen == -2) && srcLen != dstLen {
				log.Debug("Type length mismatch.", zap.Int64("src_len", srcLen), zap.Int64("dst_len", dstLen))
				return false
			}
		}
		if isPrecisionRelevant(normSrcCompType) {
			srcPrec := int64(-1); if srcInfo.Precision.Valid { srcPrec = srcInfo.Precision.Int64 }
			dstPrec := int64(-1); if dstInfo.Precision.Valid { dstPrec = dstInfo.Precision.Int64 }
			isTimeType := strings.Contains(normSrcCompType, "time") || strings.Contains(normSrcCompType, "timestamp")
			if isTimeType {
				if !((srcPrec <= 0 || !srcInfo.Precision.Valid) && (dstPrec <= 0 || !dstInfo.Precision.Valid)) && (srcPrec != dstPrec) {
					log.Debug("Time type precision mismatch.", zap.Int64("src_prec", srcPrec), zap.Bool("src_prec_valid", srcInfo.Precision.Valid), zap.Int64("dst_prec", dstPrec), zap.Bool("dst_prec_valid", dstInfo.Precision.Valid))
					return false
				}
			} else {
				if srcInfo.Precision.Valid != dstInfo.Precision.Valid || (srcInfo.Precision.Valid && dstInfo.Precision.Valid && srcPrec != dstPrec) {
					log.Debug("Decimal/Numeric type precision mismatch or one is not set.", zap.Int64("src_prec", srcPrec), zap.Bool("src_prec_valid", srcInfo.Precision.Valid), zap.Int64("dst_prec", dstPrec), zap.Bool("dst_prec_valid", dstInfo.Precision.Valid))
					return false
				}
			}
		}
		if isScaleRelevant(normSrcCompType) {
			srcScale := int64(0); if srcInfo.Scale.Valid { srcScale = srcInfo.Scale.Int64 }
			dstScale := int64(0); if dstInfo.Scale.Valid { dstScale = dstInfo.Scale.Int64 }
			if (srcInfo.Scale.Valid != dstInfo.Scale.Valid && !(srcScale == 0 && dstScale == 0)) ||
				(srcInfo.Scale.Valid && dstInfo.Scale.Valid && srcScale != dstScale) {
				log.Debug("Decimal/Numeric type scale mismatch or one is explicitly set while other is default 0.",
					zap.Int64("src_scale", srcScale), zap.Bool("src_scale_valid_original", srcInfo.Scale.Valid),
					zap.Int64("dst_scale", dstScale), zap.Bool("dst_scale_valid_original", dstInfo.Scale.Valid))
				return false
			}
		}
		log.Debug("Base types match and relevant attributes (length/precision/scale) appear equivalent.")
		return true
	}
	if (s.srcDialect == "mysql" && (s.dstDialect == "postgres" || s.dstDialect == "sqlite")) ||
		((s.srcDialect == "postgres" || s.srcDialect == "sqlite") && s.dstDialect == "mysql") {
		isSrcEffectivelyBool := normSrcCompType == "bool" || (normSrcCompType == "tinyint" && srcInfo.Length.Valid && srcInfo.Length.Int64 == 1 && s.srcDialect == "mysql")
		isDstEffectivelyBool := normDstRawType == "bool" || (normDstRawType == "tinyint" && dstInfo.Length.Valid && dstInfo.Length.Int64 == 1 && s.dstDialect == "mysql")
		if isSrcEffectivelyBool && isDstEffectivelyBool {
			log.Debug("Equivalent boolean types (e.g., tinyint(1) <=> bool) across dialects found.")
			return true
		}
	}
	if (normSrcCompType == "datetime" && normDstRawType == "timestamp") || (normSrcCompType == "timestamp" && normDstRawType == "datetime") {
		srcPrec := int64(-1); if srcInfo.Precision.Valid { srcPrec = srcInfo.Precision.Int64 }
		dstPrec := int64(-1); if dstInfo.Precision.Valid { dstPrec = dstInfo.Precision.Int64 }
		if ((srcPrec <= 0 || !srcInfo.Precision.Valid) && (dstPrec <= 0 || !dstInfo.Precision.Valid)) || (srcPrec == dstPrec) {
			log.Debug("Equivalent datetime/timestamp types with similar/default precision found.")
			return true
		}
		log.Debug("Types datetime/timestamp differ in precision.", zap.Int64("src_prec", srcPrec), zap.Int64("dst_prec", dstPrec))
		return false
	}
	if (normSrcCompType == "decimal" && normDstRawType == "numeric") || (normSrcCompType == "numeric" && normDstRawType == "decimal") {
		srcPrec := int64(-1); if srcInfo.Precision.Valid { srcPrec = srcInfo.Precision.Int64 }
		dstPrec := int64(-1); if dstInfo.Precision.Valid { dstPrec = dstInfo.Precision.Int64 }
		srcScale := int64(0); if srcInfo.Scale.Valid { srcScale = srcInfo.Scale.Int64 }
		dstScale := int64(0); if dstInfo.Scale.Valid { dstScale = dstInfo.Scale.Int64 }
		precisionMatch := (srcInfo.Precision.Valid == dstInfo.Precision.Valid && srcPrec == dstPrec) || (!srcInfo.Precision.Valid && !dstInfo.Precision.Valid)
		scaleMatch := srcScale == dstScale
		if precisionMatch && scaleMatch {
			log.Debug("Equivalent decimal/numeric types with matching precision/scale (or both default) found.")
			return true
		}
		log.Debug("Types decimal/numeric differ in precision/scale or explicit settings.",
			zap.Int64("src_prec", srcPrec), zap.Bool("src_prec_valid", srcInfo.Precision.Valid), zap.Int64("dst_prec", dstPrec), zap.Bool("dst_prec_valid", dstInfo.Precision.Valid),
			zap.Int64("src_scale", srcScale), zap.Bool("src_scale_valid_orig", srcInfo.Scale.Valid), zap.Int64("dst_scale", dstScale), zap.Bool("dst_scale_valid_orig", dstInfo.Scale.Valid))
		return false
	}
	log.Debug("Types are not equivalent after normalization and all cross-dialect checks.",
		zap.String("norm_src_comp_type_final", normSrcCompType),
		zap.String("norm_dst_raw_type_final", normDstRawType))
	return false
}

func (s *SchemaSyncer) areDefaultsEquivalent(srcDefRaw, dstDefRaw, typeForDefaultComparison string, log *zap.Logger) bool {
	normSrcDef := normalizeDefaultValue(srcDefRaw, s.srcDialect)
	normDstDef := normalizeDefaultValue(dstDefRaw, s.dstDialect)
	log.Debug("Comparing default values",
		zap.String("src_def_raw", srcDefRaw), zap.String("dst_def_raw", dstDefRaw),
		zap.String("norm_src_def", normSrcDef), zap.String("norm_dst_def", normDstDef),
		zap.String("type_for_comparison_context", typeForDefaultComparison),
	)
	normTypeForDefaultCtx := normalizeTypeName(typeForDefaultComparison)

	srcIsQuotedOriginal := (strings.HasPrefix(srcDefRaw, "'") && strings.HasSuffix(srcDefRaw, "'")) ||
		(strings.HasPrefix(srcDefRaw, "\"") && strings.HasSuffix(srcDefRaw, "\""))
	dstIsQuotedOriginal := (strings.HasPrefix(dstDefRaw, "'") && strings.HasSuffix(dstDefRaw, "'")) ||
		(strings.HasPrefix(dstDefRaw, "\"") && strings.HasSuffix(dstDefRaw, "\""))
	_, errSrcRawNumeric := strconv.ParseFloat(srcDefRaw, 64)
	_, errDstRawNumeric := strconv.ParseFloat(dstDefRaw, 64)
	srcIsPotentiallyRawNumeric := errSrcRawNumeric == nil && !srcIsQuotedOriginal
	dstIsPotentiallyRawNumeric := errDstRawNumeric == nil && !dstIsQuotedOriginal

	if (srcIsPotentiallyRawNumeric && dstIsQuotedOriginal) || (dstIsPotentiallyRawNumeric && srcIsQuotedOriginal) {
		if normSrcDef == normDstDef {
			log.Debug("Default values have different DDL natures (raw numeric vs. quoted string) but normalize to the same string. Considered different for DDL precision.",
				zap.String("src_raw", srcDefRaw), zap.Bool("src_is_raw_num_check", srcIsPotentiallyRawNumeric), zap.Bool("src_is_quoted_check", srcIsQuotedOriginal),
				zap.String("dst_raw", dstDefRaw), zap.Bool("dst_is_raw_num_check", dstIsPotentiallyRawNumeric), zap.Bool("dst_is_quoted_check", dstIsQuotedOriginal),
				zap.String("norm_val_match", normSrcDef))
			return false
		}
	}

	if normSrcDef == normDstDef {
		log.Debug("Normalized default values are string-identical, considered equivalent.")
		return true
	}
	if (normSrcDef == "null" && normDstDef == "") || (normSrcDef == "" && normDstDef == "null") {
		log.Debug("One default is explicit 'null' (normalized) and other is empty string (no default set), considered equivalent.")
		return true
	}
	if isNumericType(normTypeForDefaultCtx) {
		if !isKnownDbFunction(normSrcDef) && !isKnownDbFunction(normDstDef) {
			if normTypeForDefaultCtx == "decimal" || normTypeForDefaultCtx == "numeric" {
				srcAPD, _, srcErrAPD := apd.NewFromString(normSrcDef)
				dstAPD, _, dstErrAPD := apd.NewFromString(normDstDef)
				if srcErrAPD == nil && dstErrAPD == nil {
					if srcAPD.Cmp(dstAPD) == 0 {
						log.Debug("Decimal/Numeric default values (from normalized strings) are APD-equivalent.", zap.String("norm_src_for_apd", normSrcDef), zap.String("norm_dst_for_apd", normDstDef))
						return true
					}
					log.Debug("Decimal/Numeric default values (from normalized strings, parsed by APD) differ.", zap.String("norm_src_for_apd", normSrcDef), zap.String("norm_dst_for_apd", normDstDef))
					return false
				}
				log.Debug("Could not parse one or both normalized decimal/numeric defaults as APD.",
					zap.String("norm_src_val", normSrcDef), zap.NamedError("src_apd_err", srcErrAPD),
					zap.String("norm_dst_val", normDstDef), zap.NamedError("dst_apd_err", dstErrAPD))
			}
			srcNumVal, errSrcFloat := strconv.ParseFloat(normSrcDef, 64)
			dstNumVal, errDstFloat := strconv.ParseFloat(normDstDef, 64)
			if errSrcFloat == nil && errDstFloat == nil {
				if srcNumVal == dstNumVal {
					log.Debug("Numeric default values (from normalized strings) are float-equivalent.", zap.Float64("src_num_norm", srcNumVal), zap.Float64("dst_num_norm", dstNumVal))
					return true
				}
				log.Debug("Numeric default values (from normalized strings, parsed as float64) differ.", zap.Float64("src_num_norm", srcNumVal), zap.Float64("dst_num_norm", dstNumVal))
				return false
			}
			if (errSrcFloat == nil && errDstFloat != nil) || (errSrcFloat != nil && errDstFloat == nil) {
				log.Debug("One normalized default is float-parsable, the other is not (and neither are known DB functions or identical strings). Considered different.",
					zap.String("norm_src_for_float", normSrcDef), zap.Error(errSrcFloat),
					zap.String("norm_dst_for_float", normDstDef), zap.Error(errDstFloat))
				return false
			}
		}
	}
	log.Debug("Default values are not equivalent after all checks (fell through to final false).",
		zap.String("final_norm_src_def", normSrcDef), zap.String("final_norm_dst_def", normDstDef))
	return false
}
