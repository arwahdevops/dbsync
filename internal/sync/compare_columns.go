// internal/sync/compare_columns.go
package sync

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/apd/v3"
	"go.uber.org/zap"
)

// getColumnModifications mengembalikan daftar string perbedaan untuk sebuah kolom.
func (s *SchemaSyncer) getColumnModifications(src, dst ColumnInfo, log *zap.Logger) []string {
	diffs := []string{}
	columnLogger := log.With(zap.String("column", src.Name))

	// 1. Tipe Data
	var srcTypeForComparison string
	var addTypeDiff bool = false // Flag untuk menandai apakah perbedaan tipe perlu ditambahkan

	if src.IsGenerated {
		srcTypeForComparison = src.Type
		columnLogger.Debug("Column is generated, using original source type for type comparison", zap.String("original_src_type", src.Type))
		// Untuk kolom generated, kita bandingkan tipe asli src dengan tipe dst
		if src.Type != dst.Type && !s.areTypesEquivalent(src.Type, dst.Type, src, dst, columnLogger) {
			addTypeDiff = true
		}
	} else {
		srcTypeForComparison = src.MappedType
		if srcTypeForComparison == "" {
			columnLogger.Error("Source column MappedType is empty and column is not generated. Cannot compare types accurately.",
				zap.String("src_original_type", src.Type))
			// Jika MappedType kosong (error mapping), bandingkan tipe asli src dengan dst
			// dan tandai untuk perbedaan jika tidak ekuivalen.
			if !s.areTypesEquivalent(src.Type, dst.Type, src, dst, columnLogger) {
				addTypeDiff = true
			}
			// srcTypeForComparison akan tetap kosong atau diisi src.Type untuk fallback,
			// tapi pesan diff utama didasarkan pada src.Type vs dst.Type
		} else {
			// MappedType ada, bandingkan MappedType dengan dst.Type
			if !s.areTypesEquivalent(src.MappedType, dst.Type, src, dst, columnLogger) {
				addTypeDiff = true
			}
		}
	}

	// Tambahkan pesan perbedaan tipe HANYA JIKA flag addTypeDiff adalah true
	if addTypeDiff {
		// **KOREKSI FINAL DAN UTAMA UNTUK FORMAT PESAN TIPE**
		// Selalu gunakan src.Type (tipe asli sumber) dan dst.Type (tipe asli tujuan) dalam pesan.
		diffs = append(diffs, fmt.Sprintf("type (src: %s, dst: %s)", src.Type, dst.Type))
	}


	// 2. Nullability
	if src.IsNullable != dst.IsNullable {
		diffs = append(diffs, fmt.Sprintf("nullability (src: %t, dst: %t)", src.IsNullable, dst.IsNullable))
	}

	// 3. Default Value
	if !src.AutoIncrement && !src.IsGenerated && !dst.AutoIncrement && !dst.IsGenerated {
		srcDefVal := ""; if src.DefaultValue.Valid { srcDefVal = src.DefaultValue.String }
		dstDefVal := ""; if dst.DefaultValue.Valid { dstDefVal = dst.DefaultValue.String }

		typeForDefaultComparison := src.MappedType
		if src.IsGenerated || typeForDefaultComparison == "" { // Jika generated atau MappedType kosong, gunakan tipe asli
			typeForDefaultComparison = src.Type
		}


		if !s.areDefaultsEquivalent(srcDefVal, dstDefVal, typeForDefaultComparison, columnLogger) {
			// Format pesan default (quote tunggal)
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
		columnLogger.Debug("Both columns are generated. Expression comparison is currently skipped by dbsync.")
	}

	// 6. Collation
	srcCollation := ""; if src.Collation.Valid { srcCollation = src.Collation.String }
	dstCollation := ""; if dst.Collation.Valid { dstCollation = dst.Collation.String }

	// Tentukan tipe yang akan digunakan untuk memeriksa apakah collation relevan
	typeForCollationCheck := src.Type // Default ke tipe asli sumber
	if !src.IsGenerated && src.MappedType != "" {
		typeForCollationCheck = src.MappedType // Gunakan mapped type jika ada dan bukan generated
	}


	if isStringType(normalizeTypeName(typeForCollationCheck)) && isStringType(normalizeTypeName(dst.Type)) {
		if srcCollation != "" || dstCollation != "" {
			if !strings.EqualFold(normalizeCollation(srcCollation, s.srcDialect), normalizeCollation(dstCollation, s.dstDialect)) {
				columnLogger.Debug("Collation difference detected", zap.String("src_coll", srcCollation), zap.String("dst_coll", dstCollation))
				diffs = append(diffs, fmt.Sprintf("collation (src: %s, dst: %s)", srcCollation, dstCollation))
			}
		}
	}

	// 7. Comment
	srcComment := ""; if src.Comment.Valid { srcComment = src.Comment.String }
	dstComment := ""; if dst.Comment.Valid { dstComment = dst.Comment.String }
	if srcComment != dstComment {
		columnLogger.Debug("Column comment difference detected (considered non-critical for ALTER by default by dbsync)",
			zap.String("src_comment", srcComment), zap.String("dst_comment", dstComment))
	}

	if len(diffs) > 0 {
		columnLogger.Info("Column differences identified", zap.Strings("differences", diffs))
	}
	return diffs
}

// ... (Fungsi areTypesEquivalent, areDefaultsEquivalent, dan helper lainnya tetap sama seperti versi sebelumnya yang sudah benar) ...

// Pastikan seluruh fungsi ini juga ada di file compare_columns.go Anda

// areTypesEquivalent melakukan perbandingan tipe data yang lebih canggih.
func (s *SchemaSyncer) areTypesEquivalent(srcMappedOrOriginalType, dstRawType string, srcInfo, dstInfo ColumnInfo, log *zap.Logger) bool {
	log.Debug("Comparing types for equivalence",
		zap.String("src_mapped_or_original_type", srcMappedOrOriginalType),
		zap.String("dst_raw_type", dstRawType))
	normMappedSrc, normDst := normalizeTypeName(srcMappedOrOriginalType), normalizeTypeName(dstRawType)
	log.Debug("Normalized base types for equivalence check", zap.String("norm_mapped_src", normMappedSrc), zap.String("norm_dst", normDst))
	if normMappedSrc == normDst {
		if isStringType(normMappedSrc) || isBinaryType(normMappedSrc) {
			srcLen := int64(-1); if srcInfo.Length.Valid { srcLen = srcInfo.Length.Int64 } else if isLargeTextOrBlob(normMappedSrc) { srcLen = -2 }
			dstLen := int64(-1); if dstInfo.Length.Valid { dstLen = dstInfo.Length.Int64 } else if isLargeTextOrBlob(normDst) { dstLen = -2 }
			isSrcFixed := (strings.HasPrefix(normMappedSrc, "char") && !strings.HasPrefix(normMappedSrc, "varchar")) ||
				(strings.HasPrefix(normMappedSrc, "binary") && !strings.HasPrefix(normMappedSrc, "varbinary"))
			isDstFixed := (strings.HasPrefix(normDst, "char") && !strings.HasPrefix(normDst, "varchar")) ||
				(strings.HasPrefix(normDst, "binary") && !strings.HasPrefix(normDst, "varbinary"))
			if isSrcFixed != isDstFixed && srcLen == dstLen && srcLen > 0 {
				log.Debug("Type fixed/variable nature mismatch with same length", zap.Bool("src_fixed", isSrcFixed), zap.Bool("dst_fixed", isDstFixed), zap.Int64("length", srcLen))
				return false
			}
			if !(srcLen == -2 && dstLen == -2) && srcLen != dstLen {
				log.Debug("Type length mismatch", zap.Int64("src_len", srcLen), zap.Int64("dst_len", dstLen))
				return false
			}
		}
		if isPrecisionRelevant(normMappedSrc) {
			srcPrec := int64(-1); if srcInfo.Precision.Valid { srcPrec = srcInfo.Precision.Int64 }
			dstPrec := int64(-1); if dstInfo.Precision.Valid { dstPrec = dstInfo.Precision.Int64 }
			isTimeType := strings.Contains(normMappedSrc, "time") || strings.Contains(normMappedSrc, "timestamp")
			if isTimeType {
				if !((srcPrec <= 0 || !srcInfo.Precision.Valid) && (dstPrec <= 0 || !dstInfo.Precision.Valid)) && (srcPrec != dstPrec) {
					log.Debug("Time type precision mismatch", zap.Int64("src_prec", srcPrec), zap.Int64("dst_prec", dstPrec))
					return false
				}
			} else {
				if srcInfo.Precision.Valid != dstInfo.Precision.Valid || (srcInfo.Precision.Valid && dstInfo.Precision.Valid && srcPrec != dstPrec) {
					log.Debug("Decimal/Numeric type precision mismatch or one is not set", zap.Int64("src_prec", srcPrec), zap.Bool("src_prec_valid", srcInfo.Precision.Valid), zap.Int64("dst_prec", dstPrec), zap.Bool("dst_prec_valid", dstInfo.Precision.Valid))
					return false
				}
			}
		}
		if isScaleRelevant(normMappedSrc) {
			srcScale := int64(-1); if srcInfo.Scale.Valid { srcScale = srcInfo.Scale.Int64 } else { srcScale = 0 }
			dstScale := int64(-1); if dstInfo.Scale.Valid { dstScale = dstInfo.Scale.Int64 } else { dstScale = 0 }
			if srcInfo.Scale.Valid != dstInfo.Scale.Valid || (srcInfo.Scale.Valid && dstInfo.Scale.Valid && srcScale != dstScale) {
				log.Debug("Decimal/Numeric type scale mismatch or one is not set", zap.Int64("src_scale", srcScale), zap.Bool("src_scale_valid", srcInfo.Scale.Valid), zap.Int64("dst_scale", dstScale), zap.Bool("dst_scale_valid", dstInfo.Scale.Valid))
				return false
			}
		}
		log.Debug("Types and relevant attributes appear equivalent after base type match.")
		return true
	}
	if (s.srcDialect == "mysql" && (s.dstDialect == "postgres" || s.dstDialect == "sqlite")) ||
		((s.srcDialect == "postgres" || s.srcDialect == "sqlite") && s.dstDialect == "mysql") {
		isSrcTinyInt1 := normMappedSrc == "tinyint" && srcInfo.Length.Valid && srcInfo.Length.Int64 == 1
		isDstTinyInt1 := normDst == "tinyint" && dstInfo.Length.Valid && dstInfo.Length.Int64 == 1
		isSrcBool := normMappedSrc == "bool"
		isDstBool := normDst == "bool"
		if (isSrcTinyInt1 && isDstBool) || (isSrcBool && isDstTinyInt1) {
			log.Debug("Equivalent types (tinyint(1) <=> bool) across dialects found.")
			return true
		}
	}
	if (normMappedSrc == "datetime" && normDst == "timestamp") || (normMappedSrc == "timestamp" && normDst == "datetime") {
		srcPrec := int64(-1); if srcInfo.Precision.Valid { srcPrec = srcInfo.Precision.Int64 }
		dstPrec := int64(-1); if dstInfo.Precision.Valid { dstPrec = dstInfo.Precision.Int64 }
		if ((srcPrec <= 0 || !srcInfo.Precision.Valid) && (dstPrec <= 0 || !dstInfo.Precision.Valid)) || (srcPrec == dstPrec) {
			log.Debug("Equivalent types (datetime <=> timestamp) with similar/default precision found.")
			return true
		}
		log.Debug("Types datetime/timestamp differ in precision.", zap.Int64("src_prec", srcPrec), zap.Int64("dst_prec", dstPrec))
		return false
	}
	if (normMappedSrc == "decimal" && normDst == "numeric") || (normMappedSrc == "numeric" && normDst == "decimal") {
		srcPrec := int64(-1); if srcInfo.Precision.Valid { srcPrec = srcInfo.Precision.Int64 }
		dstPrec := int64(-1); if dstInfo.Precision.Valid { dstPrec = dstInfo.Precision.Int64 }
		srcScale := int64(-1); if srcInfo.Scale.Valid { srcScale = srcInfo.Scale.Int64 } else { srcScale = 0 }
		dstScale := int64(-1); if dstInfo.Scale.Valid { dstScale = dstInfo.Scale.Int64 } else { dstScale = 0 }
		if srcInfo.Precision.Valid == dstInfo.Precision.Valid && srcInfo.Scale.Valid == dstInfo.Scale.Valid {
			if srcPrec == dstPrec && srcScale == dstScale {
				log.Debug("Equivalent types (decimal <=> numeric) with matching precision/scale found.")
				return true
			}
		} else if !srcInfo.Precision.Valid && !dstInfo.Precision.Valid && !srcInfo.Scale.Valid && !dstInfo.Scale.Valid {
			log.Debug("Equivalent types (decimal <=> numeric) with no explicit precision/scale found.")
			return true
		}
		log.Debug("Types decimal/numeric differ in precision/scale or one has explicit setting while other does not.",
			zap.Int64("src_prec", srcPrec), zap.Int64("dst_prec", dstPrec),
			zap.Int64("src_scale", srcScale), zap.Int64("dst_scale", dstScale))
		return false
	}
	log.Debug("Types are not equivalent after normalization and all checks.")
	return false
}

// areDefaultsEquivalent membandingkan nilai default.
func (s *SchemaSyncer) areDefaultsEquivalent(srcDefRaw, dstDefRaw, typeForDefaultComparison string, log *zap.Logger) bool {
	normSrcDef := normalizeDefaultValue(srcDefRaw, s.srcDialect)
	normDstDef := normalizeDefaultValue(dstDefRaw, s.dstDialect)
	log.Debug("Comparing default values",
		zap.String("src_def_raw", srcDefRaw), zap.String("dst_def_raw", dstDefRaw),
		zap.String("norm_src_def", normSrcDef), zap.String("norm_dst_def", normDstDef),
		zap.String("type_for_comparison", typeForDefaultComparison),
	)
	if normSrcDef == normDstDef {
		log.Debug("Normalized default values are identical.")
		return true
	}
	if (normSrcDef == "null" && normDstDef == "") || (normSrcDef == "" && normDstDef == "null") {
		log.Debug("One default is explicit 'null' and other is empty string (no default set), considered equivalent.")
		return true
	}
	normTypeForDefault := normalizeTypeName(typeForDefaultComparison)
	if isNumericType(normTypeForDefault) {
		if normTypeForDefault == "decimal" || normTypeForDefault == "numeric" {
			srcAPD, _, srcErrAPD := apd.NewFromString(stripQuotes(srcDefRaw))
			dstAPD, _, dstErrAPD := apd.NewFromString(stripQuotes(dstDefRaw))
			if srcErrAPD == nil && dstErrAPD == nil {
				if srcAPD.Cmp(dstAPD) == 0 {
					log.Debug("Decimal/Numeric default values are equivalent using APD.", zap.String("src_apd", srcAPD.String()), zap.String("dst_apd", dstAPD.String()))
					return true
				}
				log.Debug("Decimal/Numeric default values differ using APD.", zap.String("src_apd", srcAPD.String()), zap.String("dst_apd", dstAPD.String()))
				return false
			}
			log.Debug("Could not parse one or both decimal/numeric defaults as APD, falling back.", zap.Error(srcErrAPD), zap.Error(dstErrAPD))
		}
		if !isKnownDbFunction(normSrcDef) && !isKnownDbFunction(normDstDef) {
			srcNum, errSrcFloat := strconv.ParseFloat(normSrcDef, 64)
			dstNum, errDstFloat := strconv.ParseFloat(normDstDef, 64)
			if errSrcFloat == nil && errDstFloat == nil {
				if srcNum == dstNum {
					log.Debug("Numeric default values are equivalent after parsing to float64.", zap.Float64("src_num", srcNum), zap.Float64("dst_num", dstNum))
					return true
				}
				log.Debug("Numeric default values (parsed as float64) differ.", zap.Float64("src_num", srcNum), zap.Float64("dst_num", dstNum))
				return false
			}
			if (errSrcFloat == nil && errDstFloat != nil) || (errSrcFloat != nil && errDstFloat == nil) {
				log.Debug("One default is float-parsable, the other is not (and neither are known DB functions).")
				return false
			}
		}
	}
	if normTypeForDefault == "bool" || (normTypeForDefault == "tinyint" && strings.Contains(typeForDefaultComparison, "(1)")) {
	}
	log.Debug("Default values are not equivalent after all checks.")
	return false
}

// --- Helper Functions ---
func normalizeCollation(coll, dialect string) string {
	c := strings.ToLower(strings.TrimSpace(coll))
	if c == "default" || c == "" {return ""}
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
		if (s[0] == '\'' && s[len(s)-1] == '\'') ||
			(s[0] == '"' && s[len(s)-1] == '"') ||
			(s[0] == '`' && s[len(s)-1] == '`') {
			return s[1 : len(s)-1]
		}
	}
	return s
}
func isKnownDbFunction(normalizedDefaultValue string) bool {
	switch normalizedDefaultValue {
	case "current_timestamp", "current_date", "current_time",
		"nextval", "uuid_function",
		"autoincrement", "auto_increment":
		return true
	default:
		if strings.HasPrefix(normalizedDefaultValue, "nextval(") {return true}
		return false
	}
}
