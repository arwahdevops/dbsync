// internal/sync/compare_columns.go
package sync

import (
	"fmt"
	"strconv" // Diperlukan untuk parse non-desimal
	"strings"

	"github.com/cockroachdb/apd/v3" // Untuk perbandingan desimal presisi tinggi
	"go.uber.org/zap"
)

// getColumnModifications mengembalikan daftar string perbedaan untuk sebuah kolom.
// Ini adalah method dari SchemaSyncer karena mungkin memanggil helper lain dari s
// seperti s.mapDataType atau s.areTypesEquivalent.
func (s *SchemaSyncer) getColumnModifications(src, dst ColumnInfo, log *zap.Logger) []string {
	diffs := []string{}
	// Pastikan logger yang diteruskan sudah memiliki konteks kolom jika memungkinkan
	// Jika belum, tambahkan di sini:
	columnLogger := log.With(zap.String("column", src.Name))

	// 1. Tipe Data
	var srcTypeForComparison string
	if src.IsGenerated {
		srcTypeForComparison = src.Type // Untuk generated, bandingkan tipe asli
		columnLogger.Debug("Column is generated, using original source type for type comparison", zap.String("original_src_type", src.Type))
	} else {
		srcTypeForComparison = src.MappedType // src.MappedType seharusnya sudah diisi
		if srcTypeForComparison == "" {
			columnLogger.Error("Source column MappedType is empty and column is not generated. Cannot compare types accurately.",
				zap.String("src_original_type", src.Type))
			// Tetap coba bandingkan dengan tipe asli sumber jika MappedType kosong, tapi ini kurang ideal
			diffs = append(diffs, fmt.Sprintf("type (src: %s [MAPPING ERROR], dst: %s)", src.Type, dst.Type))
			srcTypeForComparison = src.Type // Fallback untuk perbandingan dasar
		}
	}

	// Hanya lanjutkan jika kita punya tipe sumber untuk dibandingkan (meskipun ada error mapping)
	if !s.areTypesEquivalent(srcTypeForComparison, dst.Type, src, dst, columnLogger) {
		comparisonTypeString := src.Type
		// Tampilkan mapped type jika berbeda dan bukan generated, untuk kejelasan
		if !src.IsGenerated && src.MappedType != "" && src.MappedType != src.Type {
			comparisonTypeString = fmt.Sprintf("%s [mapped to: %s]", src.Type, src.MappedType)
		}
		diffs = append(diffs, fmt.Sprintf("type (src: %s, dst: %s)", comparisonTypeString, dst.Type))
	}

	// 2. Nullability
	if src.IsNullable != dst.IsNullable {
		diffs = append(diffs, fmt.Sprintf("nullability (src: %t, dst: %t)", src.IsNullable, dst.IsNullable))
	}

	// 3. Default Value
	// Jangan bandingkan default jika salah satu adalah auto_increment atau generated
	// karena defaultnya di-handle oleh DB atau tidak relevan untuk disinkronkan.
	if !src.AutoIncrement && !src.IsGenerated && !dst.AutoIncrement && !dst.IsGenerated {
		srcDefVal := ""; if src.DefaultValue.Valid { srcDefVal = src.DefaultValue.String }
		dstDefVal := ""; if dst.DefaultValue.Valid { dstDefVal = dst.DefaultValue.String }

		// Tipe yang digunakan untuk perbandingan default adalah MappedType dari sumber,
		// karena itu yang akan di-set di tujuan. Jika MappedType kosong (error mapping),
		// fallback ke tipe asli sumber.
		typeForDefaultComparison := src.MappedType
		if typeForDefaultComparison == "" {
			typeForDefaultComparison = src.Type
		}

		if !s.areDefaultsEquivalent(srcDefVal, dstDefVal, typeForDefaultComparison, columnLogger) {
			// Format nilai default untuk logging agar lebih mudah dibaca
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
		// Perbandingan generation_expression saat ini dilewati karena kompleksitas normalisasi SQL.
		// Jika src.GenerationExpression dan dst.GenerationExpression diisi saat fetch,
		// bisa ditambahkan perbandingan string sederhana di sini sebagai deteksi dasar.
		// srcGenExprVal := ""; if src.GenerationExpression.Valid { srcGenExprVal = src.GenerationExpression.String }
		// dstGenExprVal := ""; if dst.GenerationExpression.Valid { dstGenExprVal = dst.GenerationExpression.String }
		// if normalizeSqlExpression(srcGenExprVal) != normalizeSqlExpression(dstGenExprVal) { // Perlu normalizeSqlExpression
		//    diffs = append(diffs, fmt.Sprintf("generation_expression (src: '%s', dst: '%s')", srcGenExprVal, dstGenExprVal))
		// }
		columnLogger.Debug("Both columns are generated. Expression comparison is currently skipped by dbsync.")
	}

	// 6. Collation
	srcCollation := ""; if src.Collation.Valid { srcCollation = src.Collation.String }
	dstCollation := ""; if dst.Collation.Valid { dstCollation = dst.Collation.String }

	// Hanya bandingkan collation jika srcTypeForComparison (setelah mapping) adalah string-like,
	// dan dstType juga string-like.
	// Ini untuk menghindari perbandingan collation pada tipe numerik, dll.
	// srcTypeForComparison sudah diisi di atas (MappedType atau Type jika generated/error).
	if srcTypeForComparison != "" && isStringType(normalizeTypeName(srcTypeForComparison)) && isStringType(normalizeTypeName(dst.Type)) {
		if srcCollation != "" || dstCollation != "" { // Hanya bandingkan jika salah satu memiliki collation
			// Perbandingan collation bisa rumit karena "default" collation.
			// Untuk sekarang, perbedaan apa pun dianggap.
			// Normalisasi sederhana bisa membantu (misal, lowercase).
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
		// Aktifkan baris di bawah jika ingin DDL ALTER COMMENT di-generate (membutuhkan logika DDL terpisah)
		// diffs = append(diffs, fmt.Sprintf("comment (src: '%s', dst: '%s')", srcComment, dstComment))
	}

	if len(diffs) > 0 {
		columnLogger.Info("Column differences identified", zap.Strings("differences", diffs))
	}
	return diffs
}

// areTypesEquivalent melakukan perbandingan tipe data yang lebih canggih.
// `srcMappedOrOriginalType` adalah tipe sumber yang sudah dimapping ke dialek tujuan (atau tipe asli jika generated/mapping gagal).
// `dstRawType` adalah tipe asli dari database tujuan.
func (s *SchemaSyncer) areTypesEquivalent(srcMappedOrOriginalType, dstRawType string, srcInfo, dstInfo ColumnInfo, log *zap.Logger) bool {
	// Logger sudah memiliki konteks kolom dari pemanggil getColumnModifications
	log.Debug("Comparing types for equivalence",
		zap.String("src_mapped_or_original_type", srcMappedOrOriginalType),
		zap.String("dst_raw_type", dstRawType))

	normMappedSrc, normDst := normalizeTypeName(srcMappedOrOriginalType), normalizeTypeName(dstRawType)
	log.Debug("Normalized base types for equivalence check", zap.String("norm_mapped_src", normMappedSrc), zap.String("norm_dst", normDst))

	if normMappedSrc == normDst {
		// Tipe dasar sama, sekarang bandingkan atribut (panjang, presisi, skala) jika relevan.
		// Panjang (untuk string dan binary):
		if isStringType(normMappedSrc) || isBinaryType(normMappedSrc) {
			srcLen := int64(-1); if srcInfo.Length.Valid { srcLen = srcInfo.Length.Int64 } else if isLargeTextOrBlob(normMappedSrc) { srcLen = -2 } // -2 untuk tipe besar tanpa panjang eksplisit
			dstLen := int64(-1); if dstInfo.Length.Valid { dstLen = dstInfo.Length.Int64 } else if isLargeTextOrBlob(normDst) { dstLen = -2 }

			// Jika salah satunya tipe panjang tetap (CHAR, BINARY) dan yang lain variabel (VARCHAR, VARBINARY) dengan panjang sama,
			// itu dianggap berbeda.
			isSrcFixed := (strings.HasPrefix(normMappedSrc, "char") && !strings.HasPrefix(normMappedSrc, "varchar")) ||
				(strings.HasPrefix(normMappedSrc, "binary") && !strings.HasPrefix(normMappedSrc, "varbinary"))
			isDstFixed := (strings.HasPrefix(normDst, "char") && !strings.HasPrefix(normDst, "varchar")) ||
				(strings.HasPrefix(normDst, "binary") && !strings.HasPrefix(normDst, "varbinary"))

			if isSrcFixed != isDstFixed && srcLen == dstLen && srcLen > 0 { // Panjang harus > 0 untuk perbandingan ini
				log.Debug("Type fixed/variable nature mismatch with same length", zap.Bool("src_fixed", isSrcFixed), zap.Bool("dst_fixed", isDstFixed), zap.Int64("length", srcLen))
				return false
			}

			// Hanya bandingkan panjang jika keduanya BUKAN tipe "besar" tanpa panjang eksplisit
			// ATAU jika keduanya adalah tipe "besar" (srcLen == -2 && dstLen == -2).
			// Perbandingan antara TEXT dan VARCHAR(X) akan gagal di normMappedSrc == normDst.
			// Jika srcLen == -2 (misal TEXT) dan dstLen > 0 (misal VARCHAR(255)), itu perbedaan.
			if !(srcLen == -2 && dstLen == -2) && srcLen != dstLen {
				log.Debug("Type length mismatch", zap.Int64("src_len", srcLen), zap.Int64("dst_len", dstLen))
				return false
			}
		}

		// Presisi & Skala (untuk numerik dan waktu):
		if isPrecisionRelevant(normMappedSrc) { // DECIMAL, NUMERIC, TIME, TIMESTAMP
			srcPrec := int64(-1); if srcInfo.Precision.Valid { srcPrec = srcInfo.Precision.Int64 }
			dstPrec := int64(-1); if dstInfo.Precision.Valid { dstPrec = dstInfo.Precision.Int64 }

			// Untuk tipe waktu, presisi 0 atau tidak diset bisa berarti default DB.
			// Jika satu 0/-1 dan yang lain >0, itu perbedaan. Jika keduanya 0/-1, OK.
			// Untuk DECIMAL/NUMERIC, presisi harus cocok jika diset. Jika salah satu tidak diset dan yang lain diset, itu perbedaan.
			isTimeType := strings.Contains(normMappedSrc, "time") || strings.Contains(normMappedSrc, "timestamp")
			if isTimeType {
				if !((srcPrec <= 0 || !srcInfo.Precision.Valid) && (dstPrec <= 0 || !dstInfo.Precision.Valid)) && (srcPrec != dstPrec) {
					log.Debug("Time type precision mismatch", zap.Int64("src_prec", srcPrec), zap.Int64("dst_prec", dstPrec))
					return false
				}
			} else { // Untuk DECIMAL/NUMERIC
				if srcInfo.Precision.Valid != dstInfo.Precision.Valid || (srcInfo.Precision.Valid && dstInfo.Precision.Valid && srcPrec != dstPrec) {
					log.Debug("Decimal/Numeric type precision mismatch or one is not set", zap.Int64("src_prec", srcPrec), zap.Bool("src_prec_valid", srcInfo.Precision.Valid), zap.Int64("dst_prec", dstPrec), zap.Bool("dst_prec_valid", dstInfo.Precision.Valid))
					return false
				}
			}
		}
		if isScaleRelevant(normMappedSrc) { // DECIMAL, NUMERIC
			srcScale := int64(-1); if srcInfo.Scale.Valid { srcScale = srcInfo.Scale.Int64 } else { srcScale = 0 } // Default skala ke 0 jika tidak diset untuk perbandingan
			dstScale := int64(-1); if dstInfo.Scale.Valid { dstScale = dstInfo.Scale.Int64 } else { dstScale = 0 }

			if srcInfo.Scale.Valid != dstInfo.Scale.Valid || (srcInfo.Scale.Valid && dstInfo.Scale.Valid && srcScale != dstScale) {
				log.Debug("Decimal/Numeric type scale mismatch or one is not set", zap.Int64("src_scale", srcScale), zap.Bool("src_scale_valid", srcInfo.Scale.Valid), zap.Int64("dst_scale", dstScale), zap.Bool("dst_scale_valid", dstInfo.Scale.Valid))
				return false
			}
		}
		log.Debug("Types and relevant attributes appear equivalent after base type match.")
		return true
	}

	// Fallback equivalencies (sebaiknya juga bisa dikonfigurasi di typemap.json sebagai special mappings jika mungkin)
	// MySQL tinyint(1) vs PostgreSQL/SQLite boolean
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

	// MySQL DATETIME vs PostgreSQL TIMESTAMP WITHOUT TIME ZONE
	if (normMappedSrc == "datetime" && normDst == "timestamp") || (normMappedSrc == "timestamp" && normDst == "datetime") {
		// Ini adalah perbandingan yang sangat umum. Bandingkan juga presisi jika ada.
		srcPrec := int64(-1); if srcInfo.Precision.Valid { srcPrec = srcInfo.Precision.Int64 }
		dstPrec := int64(-1); if dstInfo.Precision.Valid { dstPrec = dstInfo.Precision.Int64 }
		// Anggap presisi default (0 atau -1) sama.
		if ((srcPrec <= 0 || !srcInfo.Precision.Valid) && (dstPrec <= 0 || !dstInfo.Precision.Valid)) || (srcPrec == dstPrec) {
			log.Debug("Equivalent types (datetime <=> timestamp) with similar/default precision found.")
			return true
		}
		log.Debug("Types datetime/timestamp differ in precision.", zap.Int64("src_prec", srcPrec), zap.Int64("dst_prec", dstPrec))
		return false
	}

	// DECIMAL vs NUMERIC (seringkali sinonim)
	if (normMappedSrc == "decimal" && normDst == "numeric") || (normMappedSrc == "numeric" && normDst == "decimal") {
		srcPrec := int64(-1); if srcInfo.Precision.Valid { srcPrec = srcInfo.Precision.Int64 }
		dstPrec := int64(-1); if dstInfo.Precision.Valid { dstPrec = dstInfo.Precision.Int64 }
		srcScale := int64(-1); if srcInfo.Scale.Valid { srcScale = srcInfo.Scale.Int64 } else { srcScale = 0 }
		dstScale := int64(-1); if dstInfo.Scale.Valid { dstScale = dstInfo.Scale.Int64 } else { dstScale = 0 }

		// Presisi dan skala harus cocok jika keduanya diset, atau jika salah satu tidak diset, yang lain juga tidak diset (atau default).
		// Perlu penanganan yang lebih baik jika satu diset dan yang lain tidak (artinya default DB berlaku).
		// Untuk saat ini, jika satu diset dan yang lain tidak, anggap berbeda.
		if srcInfo.Precision.Valid == dstInfo.Precision.Valid && srcInfo.Scale.Valid == dstInfo.Scale.Valid {
			if srcPrec == dstPrec && srcScale == dstScale {
				log.Debug("Equivalent types (decimal <=> numeric) with matching precision/scale found.")
				return true
			}
		} else if !srcInfo.Precision.Valid && !dstInfo.Precision.Valid && !srcInfo.Scale.Valid && !dstInfo.Scale.Valid {
			// Keduanya tidak memiliki presisi/skala eksplisit, anggap sama (menggunakan default DB)
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
// `typeForDefaultComparison` adalah tipe dari kolom sumber (bisa MappedType atau tipe asli jika generated/mapping gagal)
func (s *SchemaSyncer) areDefaultsEquivalent(srcDefRaw, dstDefRaw, typeForDefaultComparison string, log *zap.Logger) bool {
	// Logger sudah memiliki konteks kolom dari pemanggil getColumnModifications
	normSrcDef := normalizeDefaultValue(srcDefRaw, s.srcDialect) // Berikan dialek untuk normalisasi yang lebih baik
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

	// Jika satu NULL eksplisit ("null") dan yang lain string kosong (tidak ada default), anggap sama.
	// Ini penting karena beberapa DB mungkin menampilkan NULL sebagai tidak adanya default.
	if (normSrcDef == "null" && normDstDef == "") || (normSrcDef == "" && normDstDef == "null") {
		log.Debug("One default is explicit 'null' and other is empty string (no default set), considered equivalent.")
		return true
	}

	// Perbandingan khusus untuk tipe numerik
	normTypeForDefault := normalizeTypeName(typeForDefaultComparison)
	if isNumericType(normTypeForDefault) { // isNumericType dari compare_utils.go
		// Gunakan APD untuk DECIMAL/NUMERIC
		if normTypeForDefault == "decimal" || normTypeForDefault == "numeric" {
			srcAPD, _, srcErrAPD := apd.NewFromString(stripQuotes(srcDefRaw)) // Coba parse dari raw yang belum dinormalisasi tapi sudah di-strip quote-nya
			dstAPD, _, dstErrAPD := apd.NewFromString(stripQuotes(dstDefRaw))

			if srcErrAPD == nil && dstErrAPD == nil { // Keduanya berhasil diparse sebagai APD
				if srcAPD.Cmp(dstAPD) == 0 {
					log.Debug("Decimal/Numeric default values are equivalent using APD.", zap.String("src_apd", srcAPD.String()), zap.String("dst_apd", dstAPD.String()))
					return true
				}
				log.Debug("Decimal/Numeric default values differ using APD.", zap.String("src_apd", srcAPD.String()), zap.String("dst_apd", dstAPD.String()))
				return false
			}
			// Jika salah satu gagal parse APD, lanjutkan ke float64 atau perbandingan string
			log.Debug("Could not parse one or both decimal/numeric defaults as APD, falling back.", zap.Error(srcErrAPD), zap.Error(dstErrAPD))
		}

		// Coba parse sebagai float64 untuk tipe numerik lain (int, float, double)
		// Hati-hati dengan string yang merupakan fungsi DB (misal, 'nextval(...)')
		if !isKnownDbFunction(normSrcDef) && !isKnownDbFunction(normDstDef) {
			srcNum, errSrcFloat := strconv.ParseFloat(normSrcDef, 64)
			dstNum, errDstFloat := strconv.ParseFloat(normDstDef, 64)

			if errSrcFloat == nil && errDstFloat == nil { // Keduanya berhasil diparse sebagai float
				if srcNum == dstNum {
					log.Debug("Numeric default values are equivalent after parsing to float64.", zap.Float64("src_num", srcNum), zap.Float64("dst_num", dstNum))
					return true
				}
				log.Debug("Numeric default values (parsed as float64) differ.", zap.Float64("src_num", srcNum), zap.Float64("dst_num", dstNum))
				return false
			}

			// Jika salah satu angka, yang lain bukan (dan bukan fungsi yang diketahui)
			if (errSrcFloat == nil && errDstFloat != nil) || (errSrcFloat != nil && errDstFloat == nil) {
				log.Debug("One default is float-parsable, the other is not (and neither are known DB functions).")
				return false
			}
		}
	}

	// Perbandingan untuk tipe boolean (setelah normalisasi ke "0" atau "1" atau "true"/"false" oleh normalizeDefaultValue)
	if normTypeForDefault == "bool" || (normTypeForDefault == "tinyint" && strings.Contains(typeForDefaultComparison, "(1)")) {
		// normalizeDefaultValue sudah mencoba mengubah ke "0"/"1".
		// Jika keduanya "0" atau keduanya "1", normSrcDef == normDstDef akan menangkapnya.
		// Jika ada perbedaan seperti "true" vs "1", perlu penanganan tambahan jika normalizeDefaultValue tidak menanganinya.
		// (Saat ini, normalizeDefaultValue sudah mengubah true/false ke 1/0).
	}


	// Jika sampai sini, dan normSrcDef != normDstDef, maka dianggap berbeda.
	// Ini termasuk kasus di mana satu adalah fungsi DB dan yang lain adalah literal, atau keduanya fungsi berbeda.
	log.Debug("Default values are not equivalent after all checks.")
	return false
}


// --- Helper Functions (dipindahkan dari compare_utils.go jika hanya dipakai di sini, atau tetap di sana jika dipakai luas) ---

// normalizeCollation (contoh sederhana, mungkin perlu lebih canggih)
func normalizeCollation(coll, dialect string) string {
	c := strings.ToLower(strings.TrimSpace(coll))
	// Contoh: MySQL sering punya _ci, _cs, _bin. PostgreSQL bisa lebih kompleks.
	// "utf8mb4_unicode_ci" -> "utf8mb4_unicode_ci"
	// "default" -> "" (atau mapping ke default DB jika diketahui)
	if c == "default" || c == "" {
		// Mengembalikan string kosong untuk default bisa jadi pilihan,
		// tapi ini berarti jika satu sisi eksplisit set default dan sisi lain implisit, akan dianggap beda.
		// Ini mungkin yang diinginkan.
		return ""
	}
	return c
}

// isLargeTextOrBlob membantu areTypesEquivalent
func isLargeTextOrBlob(normalizedTypeName string) bool {
	return strings.Contains(normalizedTypeName, "text") || // tinytext, text, mediumtext, longtext
		strings.Contains(normalizedTypeName, "blob") || // tinyblob, blob, mediumblob, longblob
		normalizedTypeName == "clob" || // Oracle, dll.
		normalizedTypeName == "bytea" || // PostgreSQL
		normalizedTypeName == "json" || // JSON types
		normalizedTypeName == "xml"
}

// stripQuotes adalah helper kecil untuk membersihkan string default sebelum parsing numerik.
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

// isKnownDbFunction memeriksa apakah string default yang dinormalisasi adalah fungsi DB yang diketahui.
// Ini digunakan untuk menghindari parse error saat membandingkan default numerik.
func isKnownDbFunction(normalizedDefaultValue string) bool {
	// Fungsi-fungsi ini harus cocok dengan output dari normalizeDefaultValue
	switch normalizedDefaultValue {
	case "current_timestamp", "current_date", "current_time",
		"now()", // Beberapa normalisasi mungkin menghasilkan ini
		"nextval", "uuid_generate_v4()", "gen_random_uuid()", "newid()", "uuid()", // Contoh fungsi UUID
		"autoincrement", // Mungkin dari SQLite
		"auto_increment":  // Mungkin dari MySQL setelah normalisasi tertentu
		return true
	default:
		// Cek pola nextval yang lebih umum, misal: nextval('my_seq'::regclass)
		if strings.HasPrefix(normalizedDefaultValue, "nextval(") {
			return true
		}
		return false
	}
}
