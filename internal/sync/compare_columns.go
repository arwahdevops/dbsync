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
	// Grup 1: Nama tipe dasar (e.g., "VARCHAR")
	// Grup 2: Opsional, modifier dalam kurung (e.g., "(255)")
	// Grup 3: Opsional, sisa dari modifier (e.g., " unsigned")
	// Pola ini mencoba lebih fleksibel.
	re := regexp.MustCompile(`(?i)^((?:[a-zA-Z_][a-zA-Z0-9_]*(?:\s*\([^)]+\))?)(?:\s+[a-zA-Z_]+)*)\s+(?:AS\s*\(|GENERATED\s+ALWAYS\s+AS\s*\()`)
	matches := re.FindStringSubmatch(fullType)

	if len(matches) > 1 && strings.TrimSpace(matches[1]) != "" {
		extracted := strings.TrimSpace(matches[1])
		log.Debug("Successfully extracted base type from generated column definition.",
			zap.String("full_type", fullType),
			zap.String("extracted_base_type", extracted))
		return extracted
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
	var typeForEquivalenceCheckSrc string // Tipe sumber yang akan digunakan dalam areTypesEquivalent
	var addTypeDiff bool = false

	if src.IsGenerated {
		// Untuk kolom generated, kita bandingkan tipe data dasarnya.
		baseSrcType := extractBaseTypeFromGenerated(src.Type, columnLogger)
		typeForEquivalenceCheckSrc = baseSrcType // Ini yang akan dinormalisasi dan dibandingkan

		columnLogger.Debug("Source column is generated. Comparing its extracted base type with destination type.",
			zap.String("original_src_type_with_expr", src.Type),
			zap.String("extracted_base_src_type", baseSrcType),
			zap.String("dst_type", dst.Type))

		// Bandingkan tipe dasar sumber yang diekstrak dengan tipe tujuan.
		if !s.areTypesEquivalent(typeForEquivalenceCheckSrc, dst.Type, src, dst, columnLogger) {
			addTypeDiff = true
		}
	} else {
		// Untuk kolom non-generated, gunakan MappedType sumber (jika ada) vs. tipe tujuan.
		typeForEquivalenceCheckSrc = src.MappedType
		if typeForEquivalenceCheckSrc == "" {
			columnLogger.Error("Source column MappedType is empty and column is not generated. Type comparison will use original source type against destination type, which might not reflect intended mapping.",
				zap.String("src_original_type", src.Type),
				zap.String("dst_type", dst.Type))
			// Jika MappedType kosong (error mapping), bandingkan tipe asli src dengan dst.
			// Ini mungkin tidak ideal karena tidak mencerminkan mapping yang diinginkan, tapi ini fallback terbaik.
			typeForEquivalenceCheckSrc = src.Type
			if !s.areTypesEquivalent(typeForEquivalenceCheckSrc, dst.Type, src, dst, columnLogger) {
				addTypeDiff = true
			}
		} else {
			// MappedType ada, bandingkan MappedType dengan dst.Type.
			if !s.areTypesEquivalent(typeForEquivalenceCheckSrc, dst.Type, src, dst, columnLogger) {
				addTypeDiff = true
			}
		}
	}

	if addTypeDiff {
		// Pesan perbedaan selalu menggunakan tipe asli dari sumber dan tujuan untuk kejelasan.
		diffs = append(diffs, fmt.Sprintf("type (src: %s, dst: %s)", src.Type, dst.Type))
	}

	// 2. Nullability
	if src.IsNullable != dst.IsNullable {
		diffs = append(diffs, fmt.Sprintf("nullability (src: %t, dst: %t)", src.IsNullable, dst.IsNullable))
	}

	// 3. Default Value
	// Default value tidak dibandingkan jika salah satu kolom adalah auto_increment atau generated.
	if !src.AutoIncrement && !src.IsGenerated && !dst.AutoIncrement && !dst.IsGenerated {
		srcDefValStr := ""; if src.DefaultValue.Valid { srcDefValStr = src.DefaultValue.String }
		dstDefValStr := ""; if dst.DefaultValue.Valid { dstDefValStr = dst.DefaultValue.String }

		// Tentukan tipe yang akan digunakan untuk konteks perbandingan default.
		// Untuk sumber non-generated, gunakan MappedType.
		// Untuk sumber generated, gunakan tipe dasar yang diekstrak.
		typeForDefaultComparison := src.MappedType // Default untuk non-generated
		if src.IsGenerated {
			typeForDefaultComparison = extractBaseTypeFromGenerated(src.Type, columnLogger)
		} else if typeForDefaultComparison == "" { // Fallback jika MappedType kosong untuk non-generated
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
		// TODO: Pertimbangkan perbandingan ekspresi generated jika diperlukan di masa depan.
		// srcExpr := extractGenerationExpression(src.Type, s.srcDialect) // Memerlukan fungsi parsing ekspresi
		// dstExpr := extractGenerationExpression(dst.Type, s.dstDialect) // atau dari field terpisah dst.GenerationExpression
		// if normalizeSQLExpression(srcExpr) != normalizeSQLExpression(dstExpr) {
		//    diffs = append(diffs, fmt.Sprintf("generation_expression (src_expr: %s, dst_expr: %s)", srcExpr, dstExpr))
		// }
		columnLogger.Debug("Both columns are generated. Generation expression comparison is currently skipped by dbsync.")
	}

	// 6. Collation
	srcCollation := ""; if src.Collation.Valid { srcCollation = src.Collation.String }
	dstCollation := ""; if dst.Collation.Valid { dstCollation = dst.Collation.String }

	// Tentukan tipe yang akan digunakan untuk memeriksa apakah collation relevan.
	typeForCollationCheck := src.MappedType // Default untuk non-generated
	if src.IsGenerated {
		typeForCollationCheck = extractBaseTypeFromGenerated(src.Type, columnLogger)
	} else if typeForCollationCheck == "" { // Fallback jika MappedType kosong untuk non-generated
		typeForCollationCheck = src.Type
	}

	// Collation hanya relevan untuk tipe string dan jika setidaknya salah satu memiliki collation.
	if isStringType(normalizeTypeName(typeForCollationCheck)) && isStringType(normalizeTypeName(dst.Type)) {
		if srcCollation != "" || dstCollation != "" { // Hanya bandingkan jika salah satu memiliki collation eksplisit
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

	// 7. Comment (dianggap non-kritis untuk DDL ALTER secara default)
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
		zap.String("src_type_for_comparison", srcMappedOrBaseType), // Ini bisa MappedType atau BaseType dari generated
		zap.String("dst_raw_type", dstRawType))

	// Normalisasi tipe dasar sebelum perbandingan atribut
	normSrcCompType := normalizeTypeName(srcMappedOrBaseType)
	normDstRawType := normalizeTypeName(dstRawType)

	log.Debug("Normalized base types for equivalence check",
		zap.String("norm_src_comp_type", normSrcCompType),
		zap.String("norm_dst_raw_type", normDstRawType))

	if normSrcCompType == normDstRawType {
		// Tipe dasar sama, sekarang periksa modifier (panjang, presisi, skala)
		// Panjang (untuk string, biner)
		if isStringType(normSrcCompType) || isBinaryType(normSrcCompType) {
			// Gunakan info asli dari srcInfo (sumber data) dan dstInfo untuk modifier
			srcLen := int64(-1); if srcInfo.Length.Valid { srcLen = srcInfo.Length.Int64 } else if isLargeTextOrBlob(normSrcCompType) { srcLen = -2 } // -2 menandakan tipe besar tanpa panjang eksplisit
			dstLen := int64(-1); if dstInfo.Length.Valid { dstLen = dstInfo.Length.Int64 } else if isLargeTextOrBlob(normDstRawType) { dstLen = -2 }

			// Periksa apakah salah satunya tipe fixed-length dan yang lain variable-length dengan panjang sama
			isSrcFixed := (strings.HasPrefix(normSrcCompType, "char") && !strings.HasPrefix(normSrcCompType, "varchar")) ||
				(strings.HasPrefix(normSrcCompType, "binary") && !strings.HasPrefix(normSrcCompType, "varbinary"))
			isDstFixed := (strings.HasPrefix(normDstRawType, "char") && !strings.HasPrefix(normDstRawType, "varchar")) ||
				(strings.HasPrefix(normDstRawType, "binary") && !strings.HasPrefix(normDstRawType, "varbinary"))

			if isSrcFixed != isDstFixed && srcLen == dstLen && srcLen > 0 { // Hanya jika panjangnya sama dan > 0
				log.Debug("Type fixed/variable nature mismatch with same length, considered different.",
					zap.Bool("src_fixed", isSrcFixed), zap.Bool("dst_fixed", isDstFixed), zap.Int64("length", srcLen))
				return false
			}
			// Jika keduanya tipe besar (misal TEXT vs LONGTEXT), anggap sama jika panjang tidak diset atau -2.
			// Jika salah satu tipe besar dan yang lain tidak, atau panjangnya berbeda, anggap berbeda.
			if !(srcLen == -2 && dstLen == -2) && srcLen != dstLen {
				log.Debug("Type length mismatch.", zap.Int64("src_len", srcLen), zap.Int64("dst_len", dstLen))
				return false
			}
		}

		// Presisi (untuk desimal/numerik, waktu)
		if isPrecisionRelevant(normSrcCompType) {
			srcPrec := int64(-1); if srcInfo.Precision.Valid { srcPrec = srcInfo.Precision.Int64 }
			dstPrec := int64(-1); if dstInfo.Precision.Valid { dstPrec = dstInfo.Precision.Int64 }
			isTimeType := strings.Contains(normSrcCompType, "time") || strings.Contains(normSrcCompType, "timestamp")

			if isTimeType {
				// Untuk tipe waktu, presisi 0 atau tidak valid seringkali berarti presisi default/maksimum.
				// Anggap sama jika keduanya tidak punya presisi eksplisit, atau jika presisinya sama.
				// MySQL 'TIME' tanpa presisi adalah 'TIME(0)', PG 'time' defaultnya 6. Ini rumit.
				// Untuk penyederhanaan: anggap berbeda jika salah satu punya presisi dan yang lain tidak, ATAU jika presisinya berbeda.
				// Kecuali jika kedua presisi adalah <=0 (atau tidak valid), yang kita anggap sebagai "default/tidak diset".
				if !((srcPrec <= 0 || !srcInfo.Precision.Valid) && (dstPrec <= 0 || !dstInfo.Precision.Valid)) && (srcPrec != dstPrec) {
					log.Debug("Time type precision mismatch.",
						zap.Int64("src_prec", srcPrec), zap.Bool("src_prec_valid", srcInfo.Precision.Valid),
						zap.Int64("dst_prec", dstPrec), zap.Bool("dst_prec_valid", dstInfo.Precision.Valid))
					return false
				}
			} else { // Untuk decimal/numeric
				// Anggap berbeda jika salah satu punya presisi dan yang lain tidak, ATAU jika presisinya berbeda.
				if srcInfo.Precision.Valid != dstInfo.Precision.Valid || (srcInfo.Precision.Valid && dstInfo.Precision.Valid && srcPrec != dstPrec) {
					log.Debug("Decimal/Numeric type precision mismatch or one is not set.",
						zap.Int64("src_prec", srcPrec), zap.Bool("src_prec_valid", srcInfo.Precision.Valid),
						zap.Int64("dst_prec", dstPrec), zap.Bool("dst_prec_valid", dstInfo.Precision.Valid))
					return false
				}
			}
		}

		// Skala (untuk desimal/numerik)
		if isScaleRelevant(normSrcCompType) {
			// Jika skala tidak valid, anggap 0 untuk perbandingan.
			srcScale := int64(0); if srcInfo.Scale.Valid { srcScale = srcInfo.Scale.Int64 }
			dstScale := int64(0); if dstInfo.Scale.Valid { dstScale = dstInfo.Scale.Int64 }

			// Anggap berbeda jika salah satu punya skala eksplisit dan yang lain tidak (setelah normalisasi ke 0), ATAU jika skalanya berbeda.
			if (srcInfo.Scale.Valid != dstInfo.Scale.Valid && !(srcScale == 0 && dstScale ==0)) || // Salah satu diset, yang lain tidak (dan bukan keduanya 0)
			   (srcInfo.Scale.Valid && dstInfo.Scale.Valid && srcScale != dstScale) { // Keduanya diset tapi berbeda
				log.Debug("Decimal/Numeric type scale mismatch or one is explicitly set while other is default 0.",
					zap.Int64("src_scale", srcScale), zap.Bool("src_scale_valid_original", srcInfo.Scale.Valid),
					zap.Int64("dst_scale", dstScale), zap.Bool("dst_scale_valid_original", dstInfo.Scale.Valid))
				return false
			}
		}
		log.Debug("Base types match and relevant attributes (length/precision/scale) appear equivalent.")
		return true // Tipe dasar sama dan semua modifier relevan juga sama
	}

	// Logika untuk ekuivalensi antar dialek (contoh: MySQL tinyint(1) vs PG bool)
	// Ini menggunakan normSrcCompType (dari MappedType atau BaseType sumber) dan normDstRawType
	if (s.srcDialect == "mysql" && (s.dstDialect == "postgres" || s.dstDialect == "sqlite")) ||
		((s.srcDialect == "postgres" || s.srcDialect == "sqlite") && s.dstDialect == "mysql") {
		// Cek tinyint(1) vs bool. normSrcCompType sudah merupakan tipe dasar (e.g., "boolean" jika dimapping).
		// Jadi, kita perlu cek tipe *asli* sumber jika normSrcCompType adalah "bool" atau "tinyint"
		isSrcEffectivelyBool := normSrcCompType == "bool" || (normSrcCompType == "tinyint" && srcInfo.Length.Valid && srcInfo.Length.Int64 == 1 && s.srcDialect == "mysql")
		isDstEffectivelyBool := normDstRawType == "bool" || (normDstRawType == "tinyint" && dstInfo.Length.Valid && dstInfo.Length.Int64 == 1 && s.dstDialect == "mysql")

		if isSrcEffectivelyBool && isDstEffectivelyBool {
			log.Debug("Equivalent boolean types (e.g., tinyint(1) <=> bool) across dialects found.")
			return true
		}
	}

	// Perbandingan datetime vs timestamp (dengan presisi yang sama atau default)
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

	// Perbandingan decimal vs numeric (dengan presisi/skala yang sama atau default)
	if (normSrcCompType == "decimal" && normDstRawType == "numeric") || (normSrcCompType == "numeric" && normDstRawType == "decimal") {
		srcPrec := int64(-1); if srcInfo.Precision.Valid { srcPrec = srcInfo.Precision.Int64 }
		dstPrec := int64(-1); if dstInfo.Precision.Valid { dstPrec = dstInfo.Precision.Int64 }
		srcScale := int64(0); if srcInfo.Scale.Valid { srcScale = srcInfo.Scale.Int64 } // Default 0 jika tidak diset
		dstScale := int64(0); if dstInfo.Scale.Valid { dstScale = dstInfo.Scale.Int64 } // Default 0 jika tidak diset

		// Sama jika presisi sama (atau keduanya tidak diset) DAN skala sama (atau keduanya default 0)
		precisionMatch := (srcInfo.Precision.Valid == dstInfo.Precision.Valid && srcPrec == dstPrec) || (!srcInfo.Precision.Valid && !dstInfo.Precision.Valid)
		scaleMatch := srcScale == dstScale // Setelah normalisasi ke 0 jika tidak valid

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
	// Normalisasi nilai default mentah dari sumber dan tujuan.
	// typeForDefaultComparison adalah tipe dasar yang sudah dimapping atau diekstrak dari sumber.
	normSrcDef := normalizeDefaultValue(srcDefRaw, s.srcDialect)
	normDstDef := normalizeDefaultValue(dstDefRaw, s.dstDialect)

	log.Debug("Comparing default values",
		zap.String("src_def_raw", srcDefRaw), zap.String("dst_def_raw", dstDefRaw),
		zap.String("norm_src_def", normSrcDef), zap.String("norm_dst_def", normDstDef),
		zap.String("type_for_comparison_context", typeForDefaultComparison),
	)

	if normSrcDef == normDstDef {
		log.Debug("Normalized default values are identical.")
		return true
	}

	// Kasus di mana satu adalah 'null' (string literal) dan yang lain adalah string kosong (tidak ada default eksplisit).
	// Ini sering dianggap ekuivalen karena tidak adanya default berarti NULL.
	if (normSrcDef == "null" && normDstDef == "") || (normSrcDef == "" && normDstDef == "null") {
		log.Debug("One default is explicit 'null' (normalized) and other is empty string (no default set), considered equivalent.")
		return true
	}

	// Normalisasi tipe dasar yang digunakan untuk konteks perbandingan default.
	normTypeForDefaultCtx := normalizeTypeName(typeForDefaultComparison)

	// Perbandingan spesifik tipe
	if isNumericType(normTypeForDefaultCtx) {
		// Jika keduanya bukan fungsi DB yang dikenal (karena itu sudah ditangani oleh normSrcDef == normDstDef jika sama)
		if !isKnownDbFunction(normSrcDef) && !isKnownDbFunction(normDstDef) {
			// Untuk decimal/numeric, gunakan perbandingan presisi tinggi
			if normTypeForDefaultCtx == "decimal" || normTypeForDefaultCtx == "numeric" {
				// stripQuotes diperlukan karena nilai mentah bisa jadi "'10.00'"
				srcAPD, _, srcErrAPD := apd.NewFromString(stripQuotes(srcDefRaw))
				dstAPD, _, dstErrAPD := apd.NewFromString(stripQuotes(dstDefRaw))

				if srcErrAPD == nil && dstErrAPD == nil {
					if srcAPD.Cmp(dstAPD) == 0 {
						log.Debug("Decimal/Numeric default values are equivalent using APD.", zap.String("src_apd", srcAPD.String()), zap.String("dst_apd", dstAPD.String()))
						return true
					}
					log.Debug("Decimal/Numeric default values (parsed by APD) differ.", zap.String("src_apd", srcAPD.String()), zap.String("dst_apd", dstAPD.String()))
					return false // Berbeda menurut APD
				}
				log.Debug("Could not parse one or both decimal/numeric defaults as APD for precise comparison, falling back to string or float comparison if applicable.",
					zap.NamedError("src_apd_err", srcErrAPD), zap.NamedError("dst_apd_err", dstErrAPD))
				// Jika parse APD gagal, biarkan jatuh ke perbandingan string di akhir.
			}

			// Untuk tipe numerik lain, coba parse sebagai float.
			// Perhatikan: normSrcDef/normDstDef mungkin sudah '0' atau '1' dari normalisasi boolean.
			// Jika nilai asli adalah numerik yang di-quote (misal, DEFAULT '0'), stripQuotes akan membantu.
			srcNum, errSrcFloat := strconv.ParseFloat(stripQuotes(srcDefRaw), 64)
			dstNum, errDstFloat := strconv.ParseFloat(stripQuotes(dstDefRaw), 64)

			if errSrcFloat == nil && errDstFloat == nil {
				if srcNum == dstNum {
					log.Debug("Numeric default values are equivalent after parsing to float64.", zap.Float64("src_num", srcNum), zap.Float64("dst_num", dstNum))
					return true
				}
				log.Debug("Numeric default values (parsed as float64) differ.", zap.Float64("src_num", srcNum), zap.Float64("dst_num", dstNum))
				return false // Berbeda sebagai float
			}
			// Jika salah satu bisa di-parse sebagai float dan yang lain tidak (dan bukan fungsi DB), maka berbeda.
			if (errSrcFloat == nil && errDstFloat != nil) || (errSrcFloat != nil && errDstFloat == nil) {
				log.Debug("One numeric default is float-parsable, the other is not (and neither are known DB functions). Considered different.",
				    zap.String("src_def_raw_for_float_parse", stripQuotes(srcDefRaw)), zap.Error(errSrcFloat),
					zap.String("dst_def_raw_for_float_parse", stripQuotes(dstDefRaw)), zap.Error(errDstFloat))
				return false
			}
		}
	}

	// Perbandingan Boolean (setelah normalisasi, boolean akan menjadi "0" atau "1")
	// Ini sudah ditangani oleh normSrcDef == normDstDef jika keduanya berhasil dinormalisasi ke "0" atau "1".
	// Contoh: srcDef='true', dstDef='1' -> normSrcDef='1', normDstDef='1' -> match.
	// Contoh: srcDef='true', dstDef='false' -> normSrcDef='1', normDstDef='0' -> no match (sudah benar).
	// if normTypeForDefaultCtx == "bool" || (normTypeForDefaultCtx == "tinyint" && strings.Contains(typeForDefaultComparison, "(1)")) {
	//    // Logika ini sudah tercakup oleh perbandingan string normSrcDef == normDstDef di atas
	// }

	log.Debug("Default values are not equivalent after all checks.",
	    zap.String("final_norm_src_def", normSrcDef), zap.String("final_norm_dst_def", normDstDef))
	return false // Default: tidak ekuivalen
}


// --- Helper Functions (pindahkan ke compare_utils.go jika menjadi terlalu banyak) ---

// normalizeCollation menormalisasi nama kolasi.
func normalizeCollation(coll, dialect string) string {
	// Implementasi sederhana: lowercase dan trim. Bisa diperluas.
	// Beberapa DB mungkin memiliki alias untuk kolasi atau mengabaikan case.
	c := strings.ToLower(strings.TrimSpace(coll))
	if c == "default" || c == "" { // "default" atau string kosong berarti menggunakan default database/kolom
		return "" // Representasi internal untuk "tidak ada kolasi eksplisit"
	}
	// TODO: Pertimbangkan normalisasi spesifik dialek jika diperlukan
	// misal, mysql `utf8mb4_0900_ai_ci` vs `utf8mb4_general_ci` mungkin dianggap "cukup dekat" dalam beberapa skenario
	// tapi untuk sinkronisasi skema, biasanya kita ingin kecocokan yang lebih ketat.
	return c
}

// isLargeTextOrBlob memeriksa apakah tipe yang dinormalisasi adalah tipe teks/blob besar
// yang biasanya tidak memiliki panjang eksplisit atau panjangnya sangat besar.
func isLargeTextOrBlob(normalizedTypeName string) bool {
	return strings.Contains(normalizedTypeName, "text") || // tinytext, text, mediumtext, longtext
		strings.Contains(normalizedTypeName, "blob") || // tinyblob, blob, mediumblob, longblob
		normalizedTypeName == "clob" || // Umumnya Oracle/DB2, tapi bisa muncul
		normalizedTypeName == "bytea" || // PostgreSQL
		normalizedTypeName == "json" || // json, jsonb
		normalizedTypeName == "xml"
}

// stripQuotes menghapus satu lapis kutipan terluar (single, double, backtick).
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

// isKnownDbFunction memeriksa apakah nilai default yang *sudah dinormalisasi* adalah fungsi DB umum.
// Ini digunakan dalam areDefaultsEquivalent untuk menghindari parse error numerik.
func isKnownDbFunction(normalizedDefaultValue string) bool {
	// Fungsi ini akan dibandingkan dengan output dari normalizeDefaultValue.
	switch normalizedDefaultValue {
	case "current_timestamp", "current_date", "current_time",
		"nextval", // Penanda umum untuk sequence (PostgreSQL)
		"uuid_function": // Penanda umum untuk fungsi UUID
		return true
	default:
		// MySQL auto_increment tidak muncul sebagai default eksplisit 'auto_increment' di information_schema
		// SQLite auto_increment juga tidak.
		// PostgreSQL sequence muncul sebagai nextval('sequence_name'::regclass) yang dinormalisasi menjadi "nextval"
		return false
	}
}
