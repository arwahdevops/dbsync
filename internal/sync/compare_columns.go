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

// extractBaseTypeFromGenerated mencoba mengekstrak tipe data dasar dari definisi kolom generated.
// Contoh: "INT AS (...)" -> "INT"; "VARCHAR(255) GENERATED ALWAYS AS (...)" -> "VARCHAR(255)"
func extractBaseTypeFromGenerated(fullType string, log *zap.Logger) string {
	// Regex untuk mencocokkan tipe data (termasuk modifier dalam kurung) sebelum AS atau GENERATED.
	// Grup 1: Menangkap tipe data dasar termasuk modifiernya (e.g., "VARCHAR(255)", "INT UNSIGNED")
	re := regexp.MustCompile(`(?i)^((?:[a-zA-Z_][a-zA-Z0-9_]*(?:\s*\([^)]+\))?)(?:\s+[a-zA-Z_]+)*)\s+(?:AS\s*\(|GENERATED\s+(?:BY\s+DEFAULT\s+AS\s+IDENTITY|ALWAYS\s+AS)\s*\()`)
	matches := re.FindStringSubmatch(fullType)

	if len(matches) > 1 && strings.TrimSpace(matches[1]) != "" {
		extracted := strings.TrimSpace(matches[1])
		log.Debug("Successfully extracted base type from generated column definition.",
			zap.String("full_type", fullType),
			zap.String("extracted_base_type", extracted))
		return extracted
	}

	// Fallback jika regex utama tidak cocok, coba cari tipe di awal sebelum kurung buka pertama
	// Ini untuk kasus seperti "INT VIRTUAL" atau "TEXT STORED" tanpa "AS (...)".
	// Namun, ini kurang ideal karena bisa salah jika ada tipe dengan spasi.
	// Lebih baik jika format fetcher selalu menyertakan "AS" untuk generated.
	parts := strings.SplitN(fullType, "(", 2)
	potentialBase := strings.TrimSpace(parts[0])
	// Cek apakah `potentialBase` diikuti oleh kata kunci generated yang umum jika tidak ada "AS ("
	if strings.Contains(strings.ToUpper(fullType), " VIRTUAL") ||
		strings.Contains(strings.ToUpper(fullType), " STORED") ||
		strings.Contains(strings.ToUpper(fullType), " PERSISTENT") { // SQL Server
		// Hapus kata kunci generated tersebut
		reKeywords := regexp.MustCompile(`(?i)\s+(?:VIRTUAL|STORED|PERSISTENT)$`)
		potentialBase = reKeywords.ReplaceAllString(potentialBase, "")
		potentialBase = strings.TrimSpace(potentialBase) // Trim lagi setelah replace
		if potentialBase != fullType {                   // Hanya jika ada perubahan
			log.Debug("Attempted fallback extraction for generated column type.",
				zap.String("full_type", fullType),
				zap.String("fallback_extracted_base", potentialBase))
			return potentialBase
		}
	}

	log.Warn("Could not reliably extract base type from generated column definition using regex. Falling back to using full type string. This might lead to incorrect type comparisons if the full string contains generation expressions.",
		zap.String("full_type", fullType))
	return fullType // Fallback jika regex tidak cocok
}

// getColumnModifications mengembalikan daftar string perbedaan untuk sebuah kolom.
func (s *SchemaSyncer) getColumnModifications(src, dst ColumnInfo, log *zap.Logger) []string {
	diffs := []string{}
	columnLogger := log.With(zap.String("column", src.Name))

	// 1. Tipe Data
	var typeForEquivalenceCheckSrc string
	addTypeDiff := false

	if src.IsGenerated {
		baseSrcType := extractBaseTypeFromGenerated(src.Type, columnLogger)
		typeForEquivalenceCheckSrc = baseSrcType
		columnLogger.Debug("Source column is generated. Comparing its extracted base type with destination type.",
			zap.String("original_src_type_with_expr", src.Type),
			zap.String("extracted_base_src_type", baseSrcType),
			zap.String("dst_type", dst.Type))
		if !s.areTypesEquivalent(typeForEquivalenceCheckSrc, dst.Type, src, dst, columnLogger) {
			addTypeDiff = true
		}
	} else {
		typeForEquivalenceCheckSrc = src.MappedType
		if typeForEquivalenceCheckSrc == "" { // MappedType kosong berarti ada masalah saat mapping tipe sumber
			columnLogger.Error("Source column MappedType is empty and column is not generated. Type comparison will use original source type against destination type, which might not reflect intended mapping.",
				zap.String("src_original_type", src.Type),
				zap.String("dst_type", dst.Type))
			typeForEquivalenceCheckSrc = src.Type // Fallback ke tipe asli sumber
			if !s.areTypesEquivalent(typeForEquivalenceCheckSrc, dst.Type, src, dst, columnLogger) {
				addTypeDiff = true
			}
		} else {
			if !s.areTypesEquivalent(typeForEquivalenceCheckSrc, dst.Type, src, dst, columnLogger) {
				addTypeDiff = true
			}
		}
	}

	if addTypeDiff {
		diffs = append(diffs, fmt.Sprintf("type (src: %s, dst: %s)", src.Type, dst.Type))
	}

	// 2. Nullability
	if src.IsNullable != dst.IsNullable {
		diffs = append(diffs, fmt.Sprintf("nullability (src: %t, dst: %t)", src.IsNullable, dst.IsNullable))
	}

	// 3. Default Value
	if !src.AutoIncrement && !src.IsGenerated && !dst.AutoIncrement && !dst.IsGenerated {
		srcDefValStr := ""; if src.DefaultValue.Valid { srcDefValStr = src.DefaultValue.String }
		dstDefValStr := ""; if dst.DefaultValue.Valid { dstDefValStr = dst.DefaultValue.String }

		typeForDefaultComparison := src.MappedType
		if src.IsGenerated { // typeForEquivalenceCheckSrc sudah berisi base type dari src.IsGenerated di atas
			typeForDefaultComparison = typeForEquivalenceCheckSrc
		} else if typeForDefaultComparison == "" {
			typeForDefaultComparison = src.Type
			columnLogger.Warn("MappedType for source column is empty (non-generated), using original source type for default value comparison context.",
				zap.String("src_original_type", src.Type))
		}

		if !s.areDefaultsEquivalent(srcDefValStr, dstDefValStr, typeForDefaultComparison, columnLogger) {
			srcDefLog := "NULL"; if src.DefaultValue.Valid { srcDefLog = fmt.Sprintf("'%s'", src.DefaultValue.String) }
			dstDefLog := "NULL"; if dst.DefaultValue.Valid { dstDefLog = fmt.Sprintf("'%s'", dst.DefaultValue.String) }
			diffs = append(diffs, fmt.Sprintf("default (src: %s, dst: %s)", srcDefLog, dstDefLog))
		}
	}

	// 4. AutoIncrement Status
	if src.AutoIncrement != dst.AutoIncrement {
		diffs = append(diffs, fmt.Sprintf("auto_increment (src: %t, dst: %t)", src.AutoIncrement, dst.AutoIncrement))
		columnLogger.Warn("AutoIncrement/Identity status difference detected. Applying this change via ALTER is often complex or unsupported by dbsync.",
			zap.Bool("src_auto_inc", src.AutoIncrement), zap.Bool("dst_auto_inc", dst.AutoIncrement))
	}

	// 5. Generated Column Status & Expression
	if src.IsGenerated != dst.IsGenerated {
		diffs = append(diffs, fmt.Sprintf("generated_status (src: %t, dst: %t)", src.IsGenerated, dst.IsGenerated))
		columnLogger.Warn("Generated column status difference detected. Modifying this often requires dropping and re-adding the column or is unsupported by dbsync.")
	} else if src.IsGenerated && dst.IsGenerated {
		columnLogger.Debug("Both columns are generated. Generation expression comparison is currently skipped by dbsync.")
	}

	// 6. Collation
	srcCollation := ""; if src.Collation.Valid { srcCollation = src.Collation.String }
	dstCollation := ""; if dst.Collation.Valid { dstCollation = dst.Collation.String }

	typeForCollationCheck := src.MappedType
	if src.IsGenerated { // typeForEquivalenceCheckSrc sudah berisi base type dari src.IsGenerated di atas
		typeForCollationCheck = typeForEquivalenceCheckSrc
	} else if typeForCollationCheck == "" {
		typeForCollationCheck = src.Type
	}

	if isStringType(normalizeTypeName(typeForCollationCheck)) && isStringType(normalizeTypeName(dst.Type)) {
		if srcCollation != "" || dstCollation != "" {
			normSrcColl := normalizeCollation(srcCollation, s.srcDialect)
			normDstColl := normalizeCollation(dstCollation, s.dstDialect)
			if !strings.EqualFold(normSrcColl, normDstColl) {
				columnLogger.Debug("Collation difference detected.",
					zap.String("src_coll_raw", srcCollation), zap.String("dst_coll_raw", dstCollation),
					zap.String("src_coll_norm", normSrcColl), zap.String("dst_coll_norm", normDstColl))
				diffs = append(diffs, fmt.Sprintf("collation (src: %s, dst: %s)", srcCollation, dstCollation))
			}
		}
	}

	// 7. Comment
	srcComment := ""; if src.Comment.Valid { srcComment = src.Comment.String }
	dstComment := ""; if dst.Comment.Valid { dstComment = dst.Comment.String }
	if srcComment != dstComment {
		columnLogger.Debug("Column comment difference detected (considered non-critical by dbsync for ALTER DDL generation).",
			zap.String("src_comment", srcComment), zap.String("dst_comment", dstComment))
	}

	if len(diffs) > 0 {
		columnLogger.Info("Column differences identified.", zap.Strings("differences", diffs))
	}
	return diffs
}


// areTypesEquivalent melakukan perbandingan tipe data yang lebih canggih.
func (s *SchemaSyncer) areTypesEquivalent(srcMappedOrBaseType, dstRawType string, srcInfo, dstInfo ColumnInfo, log *zap.Logger) bool {
	log.Debug("Comparing types for equivalence",
		zap.String("src_type_for_comparison", srcMappedOrBaseType),
		zap.String("dst_raw_type", dstRawType))

	normSrcCompType := normalizeTypeName(srcMappedOrBaseType)
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
				log.Debug("Type fixed/variable nature mismatch with same length, considered different.",
					zap.Bool("src_fixed", isSrcFixed), zap.Bool("dst_fixed", isDstFixed), zap.Int64("length", srcLen))
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
					log.Debug("Time type precision mismatch.",
						zap.Int64("src_prec", srcPrec), zap.Bool("src_prec_valid", srcInfo.Precision.Valid),
						zap.Int64("dst_prec", dstPrec), zap.Bool("dst_prec_valid", dstInfo.Precision.Valid))
					return false
				}
			} else {
				if srcInfo.Precision.Valid != dstInfo.Precision.Valid || (srcInfo.Precision.Valid && dstInfo.Precision.Valid && srcPrec != dstPrec) {
					log.Debug("Decimal/Numeric type precision mismatch or one is not set.",
						zap.Int64("src_prec", srcPrec), zap.Bool("src_prec_valid", srcInfo.Precision.Valid),
						zap.Int64("dst_prec", dstPrec), zap.Bool("dst_prec_valid", dstInfo.Precision.Valid))
					return false
				}
			}
		}
		if isScaleRelevant(normSrcCompType) {
			srcScale := int64(0); if srcInfo.Scale.Valid { srcScale = srcInfo.Scale.Int64 }
			dstScale := int64(0); if dstInfo.Scale.Valid { dstScale = dstInfo.Scale.Int64 }
			if (srcInfo.Scale.Valid != dstInfo.Scale.Valid && !(srcScale == 0 && dstScale ==0)) ||
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

// areDefaultsEquivalent membandingkan nilai default.
func (s *SchemaSyncer) areDefaultsEquivalent(srcDefRaw, dstDefRaw, typeForDefaultComparison string, log *zap.Logger) bool {
	normSrcDef := normalizeDefaultValue(srcDefRaw, s.srcDialect)
	normDstDef := normalizeDefaultValue(dstDefRaw, s.dstDialect)

	log.Debug("Comparing default values",
		zap.String("src_def_raw", srcDefRaw), zap.String("dst_def_raw", dstDefRaw),
		zap.String("norm_src_def", normSrcDef), zap.String("norm_dst_def", normDstDef),
		zap.String("type_for_comparison_context", typeForDefaultComparison),
	)

	normTypeForDefaultCtx := normalizeTypeName(typeForDefaultComparison)

	srcIsQuoted := (strings.HasPrefix(srcDefRaw, "'") && strings.HasSuffix(srcDefRaw, "'")) ||
		(strings.HasPrefix(srcDefRaw, "\"") && strings.HasSuffix(srcDefRaw, "\""))
	dstIsQuoted := (strings.HasPrefix(dstDefRaw, "'") && strings.HasSuffix(dstDefRaw, "'")) ||
		(strings.HasPrefix(dstDefRaw, "\"") && strings.HasSuffix(dstDefRaw, "\""))

	_, errSrcRawNumeric := strconv.ParseFloat(srcDefRaw, 64)
	_, errDstRawNumeric := strconv.ParseFloat(dstDefRaw, 64)
	srcIsRawNumeric := errSrcRawNumeric == nil
	dstIsRawNumeric := errDstRawNumeric == nil

	if isNumericType(normTypeForDefaultCtx) {
		if (srcIsRawNumeric && dstIsQuoted) || (dstIsRawNumeric && srcIsQuoted) {
			log.Debug("Default value nature mismatch for numeric column: one raw numeric, other quoted string.",
				zap.Bool("src_is_raw_numeric", srcIsRawNumeric), zap.Bool("src_is_quoted", srcIsQuoted),
				zap.Bool("dst_is_raw_numeric", dstIsRawNumeric), zap.Bool("dst_is_quoted", dstIsQuoted))
			return false
		}
	} else if isStringType(normTypeForDefaultCtx) {
		// Jika tipe kolomnya string, namun salah satu default adalah angka mentah (tanpa quote)
		// dan yang lain adalah string (di-quote), dan nilai normalisasinya sama (misal 123 vs '123' -> "123"),
		// maka anggap berbeda karena DDL aslinya berbeda.
		if (srcIsRawNumeric && !srcIsQuoted && dstIsQuoted && normSrcDef == normDstDef) ||
			(dstIsRawNumeric && !dstIsQuoted && srcIsQuoted && normSrcDef == normDstDef) {
			log.Debug("Default value nature mismatch for string column but normalized values are same: one raw numeric, other quoted string. Considered different for DDL precision.",
				zap.Bool("src_is_raw_numeric", srcIsRawNumeric), zap.Bool("src_is_quoted", srcIsQuoted),
				zap.Bool("dst_is_raw_numeric", dstIsRawNumeric), zap.Bool("dst_is_quoted", dstIsQuoted),
				zap.String("norm_src", normSrcDef), zap.String("norm_dst", normDstDef))
			return false
		}
	}

	if normSrcDef == normDstDef {
		log.Debug("Normalized default values are identical.")
		return true
	}

	if (normSrcDef == "null" && normDstDef == "") || (normSrcDef == "" && normDstDef == "null") {
		log.Debug("One default is explicit 'null' (normalized) and other is empty string (no default set), considered equivalent.")
		return true
	}

	if isNumericType(normTypeForDefaultCtx) {
		if !isKnownDbFunction(normSrcDef) && !isKnownDbFunction(normDstDef) {
			if normTypeForDefaultCtx == "decimal" || normTypeForDefaultCtx == "numeric" {
				// Untuk APD, stripQuotes penting karena nilai mentah bisa di-quote (misal, "'10.0'")
				srcAPD, _, srcErrAPD := apd.NewFromString(stripQuotes(srcDefRaw))
				dstAPD, _, dstErrAPD := apd.NewFromString(stripQuotes(dstDefRaw))
				if srcErrAPD == nil && dstErrAPD == nil {
					if srcAPD.Cmp(dstAPD) == 0 {
						log.Debug("Decimal/Numeric default values are equivalent using APD.", zap.String("src_apd", srcAPD.String()), zap.String("dst_apd", dstAPD.String()))
						return true
					}
					log.Debug("Decimal/Numeric default values (parsed by APD) differ.", zap.String("src_apd", srcAPD.String()), zap.String("dst_apd", dstAPD.String()))
					return false
				}
				log.Debug("Could not parse one or both decimal/numeric defaults as APD for precise comparison.", zap.NamedError("src_apd_err", srcErrAPD), zap.NamedError("dst_apd_err", dstErrAPD))
				// Jika APD gagal, biarkan jatuh ke perbandingan string di akhir jika tidak ada parse float yang berhasil
			}

			// Coba parse nilai mentah (raw) sebagai float, bukan yang sudah dinormalisasi sepenuhnya.
			// Ini karena normalisasi bisa mengubah 'true' menjadi '1' yang akan lolos parse float.
			// Namun, jika salah satunya raw numeric dan yang lain quoted string, itu sudah ditangani di atas.
			// Blok ini akan menangani kasus dimana keduanya adalah string yang merepresentasikan angka,
			// atau keduanya angka mentah.
			srcNumValForComp, errSrcFloatParse := strconv.ParseFloat(srcDefRaw, 64)
			dstNumValForComp, errDstFloatParse := strconv.ParseFloat(dstDefRaw, 64)

			if errSrcFloatParse == nil && errDstFloatParse == nil { // Keduanya adalah angka valid dari string mentah
				if srcNumValForComp == dstNumValForComp {
					log.Debug("Numeric default values (from raw strings) are equivalent after parsing to float64.", zap.Float64("src_num_raw", srcNumValForComp), zap.Float64("dst_num_raw", dstNumValForComp))
					return true
				}
				log.Debug("Numeric default values (from raw strings, parsed as float64) differ.", zap.Float64("src_num_raw", srcNumValForComp), zap.Float64("dst_num_raw", dstNumValForComp))
				return false
			}
		}
	}

	log.Debug("Default values are not equivalent after all checks.",
	    zap.String("final_norm_src_def", normSrcDef), zap.String("final_norm_dst_def", normDstDef))
	return false
}

// --- Helper Functions (dipertahankan di sini untuk saat ini) ---
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
