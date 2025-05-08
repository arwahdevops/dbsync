// internal/sync/compare_columns.go
package sync

import (
	"fmt"
	"strconv" // Diperlukan untuk areDefaultsEquivalent
	"strings"

	"go.uber.org/zap"
)

// getColumnModifications mengembalikan daftar string perbedaan untuk sebuah kolom.
// Ini adalah method dari SchemaSyncer karena mungkin memanggil helper lain dari s
// seperti s.mapDataType atau s.areTypesEquivalent.
func (s *SchemaSyncer) getColumnModifications(src, dst ColumnInfo, log *zap.Logger) []string {
	diffs := []string{}
	log = log.With(zap.String("column", src.Name))

	// 1. Tipe Data
	var srcTypeForComparison string
	if src.IsGenerated {
		srcTypeForComparison = src.Type // Untuk generated, bandingkan tipe asli (meskipun mungkin tidak di-ALTER)
		log.Debug("Column is generated, using original type for comparison (if any)", zap.String("original_type", src.Type))
	} else {
		srcTypeForComparison = src.MappedType // src.MappedType seharusnya sudah diisi
		if srcTypeForComparison == "" {
			log.Error("Source column MappedType is empty and column is not generated. Cannot compare types.",
				zap.String("src_original_type", src.Type))
			diffs = append(diffs, fmt.Sprintf("type (src: %s [MAPPING ERROR], dst: %s)", src.Type, dst.Type))
		}
	}

	if srcTypeForComparison != "" { // Lanjutkan hanya jika kita punya tipe sumber untuk dibandingkan
		if !s.areTypesEquivalent(srcTypeForComparison, dst.Type, src, dst, log) {
			comparisonTypeString := src.Type
			// Tampilkan mapped type jika berbeda dan bukan generated, untuk kejelasan
			if !src.IsGenerated && src.MappedType != "" && src.MappedType != src.Type {
				comparisonTypeString = fmt.Sprintf("%s [mapped to: %s]", src.Type, src.MappedType)
			}
			diffs = append(diffs, fmt.Sprintf("type (src: %s, dst: %s)", comparisonTypeString, dst.Type))
		}
	}

	// 2. Nullability
	if src.IsNullable != dst.IsNullable {
		diffs = append(diffs, fmt.Sprintf("nullability (src: %t, dst: %t)", src.IsNullable, dst.IsNullable))
	}

	// 3. Default Value
	if !src.AutoIncrement && !src.IsGenerated && !dst.AutoIncrement && !dst.IsGenerated {
		srcDefVal := ""; if src.DefaultValue.Valid { srcDefVal = src.DefaultValue.String }
		dstDefVal := ""; if dst.DefaultValue.Valid { dstDefVal = dst.DefaultValue.String }

		typeForDefaultComparison := src.Type // Default ke tipe asli jika MappedType tidak ada
		if !src.IsGenerated && src.MappedType != "" {
			typeForDefaultComparison = src.MappedType
		}

		if !s.areDefaultsEquivalent(srcDefVal, dstDefVal, typeForDefaultComparison, log) {
			// Format nilai default untuk logging agar lebih mudah dibaca
			srcDefLog := "NULL"; if src.DefaultValue.Valid { srcDefLog = fmt.Sprintf("'%s'", src.DefaultValue.String) }
			dstDefLog := "NULL"; if dst.DefaultValue.Valid { dstDefLog = fmt.Sprintf("'%s'", dst.DefaultValue.String) }
			diffs = append(diffs, fmt.Sprintf("default (src: %s, dst: %s)", srcDefLog, dstDefLog))
		}
	}


	// 4. AutoIncrement Status
	if src.AutoIncrement != dst.AutoIncrement {
		diffs = append(diffs, fmt.Sprintf("auto_increment (src: %t, dst: %t)", src.AutoIncrement, dst.AutoIncrement))
		log.Warn("AutoIncrement/Identity status difference detected. Applying this change via ALTER is often complex or unsupported.",
			zap.Bool("src_auto_inc", src.AutoIncrement), zap.Bool("dst_auto_inc", dst.AutoIncrement))
	}

	// 5. Generated Column Status & Expression
	if src.IsGenerated != dst.IsGenerated {
		diffs = append(diffs, fmt.Sprintf("generated_status (src: %t, dst: %t)", src.IsGenerated, dst.IsGenerated))
		log.Warn("Generated column status difference detected. Modifying this often requires dropping and re-adding the column or is unsupported.")
	} else if src.IsGenerated && dst.IsGenerated {
		// Perbandingan generation_expression saat ini dilewati karena kompleksitas.
		// Jika ingin diimplementasikan, Anda perlu mengambil GENERATION_EXPRESSION dari kolom tujuan juga.
		// srcGenExprVal := ""; if src.GenerationExpression.Valid { srcGenExprVal = src.GenerationExpression.String }
		// dstGenExprVal := ""; if dst.GenerationExpression.Valid { dstGenExprVal = dst.GenerationExpression.String }
		// if normalizeSqlExpression(srcGenExprVal) != normalizeSqlExpression(dstGenExprVal) {
		//    diffs = append(diffs, fmt.Sprintf("generation_expression (src: '%s', dst: '%s')", srcGenExprVal, dstGenExprVal))
		// }
		log.Debug("Both columns are generated. Expression comparison is currently skipped.")
	}


	// 6. Collation
	srcCollation := ""; if src.Collation.Valid { srcCollation = src.Collation.String }
	dstCollation := ""; if dst.Collation.Valid { dstCollation = dst.Collation.String }

	// Hanya bandingkan collation jika srcTypeForComparison adalah string-like, dan dstType juga string-like
	// Ini untuk menghindari perbandingan collation pada tipe numerik, dll.
	if srcTypeForComparison != "" && isStringType(normalizeTypeName(srcTypeForComparison)) && isStringType(normalizeTypeName(dst.Type)) {
		if srcCollation != "" || dstCollation != "" { // Hanya bandingkan jika salah satu memiliki collation
			if !strings.EqualFold(srcCollation, dstCollation) {
				// Ini bisa menjadi false positive jika satu adalah default collation database
				// dan yang lain adalah nama spesifik untuk default itu.
				// Untuk saat ini, perbedaan apa pun dianggap.
				log.Debug("Collation difference detected", zap.String("src_coll", srcCollation), zap.String("dst_coll", dstCollation))
				diffs = append(diffs, fmt.Sprintf("collation (src: %s, dst: %s)", srcCollation, dstCollation))
			}
		}
	}

	// 7. Comment
	srcComment := ""; if src.Comment.Valid { srcComment = src.Comment.String }
	dstComment := ""; if dst.Comment.Valid { dstComment = dst.Comment.String }
	if srcComment != dstComment {
		log.Debug("Column comment difference detected (considered non-critical for ALTER by default)",
			zap.String("src_comment", srcComment), zap.String("dst_comment", dstComment))
		// diffs = append(diffs, fmt.Sprintf("comment (src: '%s', dst: '%s')", srcComment, dstComment)) // Aktifkan jika ingin ALTER COMMENT
	}

	if len(diffs) > 0 {
		log.Info("Column differences identified", zap.Strings("differences", diffs))
	}
	return diffs
}

// areTypesEquivalent melakukan perbandingan tipe data yang lebih canggih.
// `mappedSrcType` adalah tipe sumber yang sudah dimapping ke dialek tujuan (atau tipe asli jika generated).
// `dstType` adalah tipe asli dari database tujuan.
func (s *SchemaSyncer) areTypesEquivalent(mappedSrcType, dstType string, srcInfo, dstInfo ColumnInfo, log *zap.Logger) bool {
	log = log.With(zap.String("column", srcInfo.Name), zap.String("mapped_src_type", mappedSrcType), zap.String("dst_type", dstType))

	normMappedSrc, normDst := normalizeTypeName(mappedSrcType), normalizeTypeName(dstType)
	log.Debug("Normalized types for equivalence check", zap.String("norm_mapped_src", normMappedSrc), zap.String("norm_dst", normDst))

	if normMappedSrc == normDst {
		// Tipe dasar sama, sekarang bandingkan atribut (panjang, presisi, skala) jika relevan.
		// Panjang:
		if (isStringType(normMappedSrc) || isBinaryType(normMappedSrc)) {
			srcLen := int64(-1); if srcInfo.Length.Valid { srcLen = srcInfo.Length.Int64 } else if strings.Contains(normMappedSrc, "text") || strings.Contains(normMappedSrc, "blob") { srcLen = 0 } // Anggap 0 jika TEXT/BLOB tanpa panjang eksplisit
			dstLen := int64(-1); if dstInfo.Length.Valid { dstLen = dstInfo.Length.Int64 } else if strings.Contains(normDst, "text") || strings.Contains(normDst, "blob") { dstLen = 0 }

			// Jika salah satunya tipe panjang tetap (CHAR, BINARY) dan yang lain variabel (VARCHAR, VARBINARY) dengan panjang sama,
			// itu dianggap berbeda.
			isSrcFixed := strings.HasPrefix(normMappedSrc, "char") || strings.HasPrefix(normMappedSrc, "binary") && !strings.HasPrefix(normMappedSrc, "var")
			isDstFixed := strings.HasPrefix(normDst, "char") || strings.HasPrefix(normDst, "binary") && !strings.HasPrefix(normDst, "var")
			if isSrcFixed != isDstFixed && srcLen == dstLen && srcLen > 0 {
				log.Debug("Type fixed/variable mismatch with same length", zap.Bool("src_fixed", isSrcFixed), zap.Bool("dst_fixed", isDstFixed))
				return false
			}
			
			// Hanya bandingkan panjang jika keduanya bukan tipe "besar" tanpa panjang eksplisit (misal, TEXT, BLOB)
			// atau jika keduanya tipe "besar". Perbandingan antara TEXT dan VARCHAR(X) akan gagal di normMappedSrc == normDst.
			if !(srcLen == 0 && dstLen > 0 && (strings.Contains(normMappedSrc, "text") || strings.Contains(normMappedSrc, "blob"))) &&
			   !(dstLen == 0 && srcLen > 0 && (strings.Contains(normDst, "text") || strings.Contains(normDst, "blob"))) {
				if srcLen != dstLen {
					log.Debug("Type length mismatch", zap.Int64("src_len", srcLen), zap.Int64("dst_len", dstLen))
					return false
				}
			}
		}

		// Presisi & Skala:
		if isPrecisionRelevant(normMappedSrc) {
			srcPrec := int64(-1); if srcInfo.Precision.Valid {srcPrec = srcInfo.Precision.Int64}
			dstPrec := int64(-1); if dstInfo.Precision.Valid {dstPrec = dstInfo.Precision.Int64}
			// Untuk tipe waktu, presisi 0 atau tidak diset bisa berarti default DB.
			// Jika satu 0 dan yang lain >0, itu perbedaan. Jika keduanya 0 atau tidak diset, OK.
			isTimeType := strings.Contains(normMappedSrc, "time") || strings.Contains(normMappedSrc, "timestamp")
			if isTimeType && ((srcPrec == 0 || srcPrec == -1) && (dstPrec == 0 || dstPrec == -1)) {
				// Keduanya default precision untuk time, anggap sama
			} else if srcPrec != dstPrec {
				log.Debug("Type precision mismatch", zap.Int64("src_prec", srcPrec), zap.Int64("dst_prec", dstPrec))
				return false
			}
		}
		if isScaleRelevant(normMappedSrc) {
			srcScale := int64(-1); if srcInfo.Scale.Valid {srcScale = srcInfo.Scale.Int64}
			dstScale := int64(-1); if dstInfo.Scale.Valid {dstScale = dstInfo.Scale.Int64}
			if srcScale != dstScale {
				log.Debug("Type scale mismatch", zap.Int64("src_scale", srcScale), zap.Int64("dst_scale", dstScale))
				return false
			}
		}
		log.Debug("Types and relevant attributes appear equivalent.")
		return true
	}

	// Fallback equivalencies (sebaiknya ada di typemap.json atau special mappings)
	if (s.srcDialect == "mysql" && (s.dstDialect == "postgres" || s.dstDialect == "sqlite")) ||
	   ((s.srcDialect == "postgres" || s.srcDialect == "sqlite") && s.dstDialect == "mysql") {
		// tinyint(1) [mysql] vs bool [postgres/sqlite]
		if (normMappedSrc == "tinyint" && srcInfo.Length.Valid && srcInfo.Length.Int64 == 1 && normDst == "bool") ||
			(normMappedSrc == "bool" && normDst == "tinyint" && dstInfo.Length.Valid && dstInfo.Length.Int64 == 1) {
			log.Debug("Equivalent types (tinyint(1) <=> bool) across dialects.")
			return true
		}
	}
	if (normMappedSrc == "datetime" && normDst == "timestamp") || (normMappedSrc == "timestamp" && normDst == "datetime") {
		// Ini adalah perbandingan yang sangat umum antar MySQL (DATETIME) dan PostgreSQL (TIMESTAMP WITHOUT TIME ZONE)
		// Jika presisi juga dibandingkan dan cocok, maka bisa dianggap ekuivalen.
		srcPrec := int64(-1); if srcInfo.Precision.Valid {srcPrec = srcInfo.Precision.Int64}
		dstPrec := int64(-1); if dstInfo.Precision.Valid {dstPrec = dstInfo.Precision.Int64}
		if ((srcPrec == 0 || srcPrec == -1) && (dstPrec == 0 || dstPrec == -1)) || (srcPrec == dstPrec) {
			log.Debug("Equivalent types (datetime <=> timestamp) with similar/default precision.")
			return true
		}
		log.Debug("Types datetime/timestamp differ in precision.", zap.Int64("srcP",srcPrec), zap.Int64("dstP",dstPrec))
        return false
	}
	if (normMappedSrc == "decimal" && normDst == "numeric") || (normMappedSrc == "numeric" && normDst == "decimal") {
		srcPrec := int64(-1); if srcInfo.Precision.Valid {srcPrec = srcInfo.Precision.Int64}
		dstPrec := int64(-1); if dstInfo.Precision.Valid {dstPrec = dstInfo.Precision.Int64}
		srcScale := int64(-1); if srcInfo.Scale.Valid {srcScale = srcInfo.Scale.Int64}
		dstScale := int64(-1); if dstInfo.Scale.Valid {dstScale = dstInfo.Scale.Int64}
		if srcPrec == dstPrec && srcScale == dstScale {
			log.Debug("Equivalent types (decimal <=> numeric) with matching precision/scale.")
			return true
		}
		log.Debug("Types decimal/numeric differ in precision/scale.", zap.Int64("srcP",srcPrec), zap.Int64("dstP",dstPrec), zap.Int64("srcS",srcScale), zap.Int64("dstS",dstScale))
        return false
	}

	log.Debug("Types are not equivalent after normalization and all checks.")
	return false
}

// areDefaultsEquivalent membandingkan nilai default.
// `typeForDefaultComparison` adalah tipe dari kolom sumber (bisa MappedType atau tipe asli jika generated/mapping gagal)
func (s *SchemaSyncer) areDefaultsEquivalent(srcDefRaw, dstDefRaw, typeForDefaultComparison string, log *zap.Logger) bool {
	normSrcDef := normalizeDefaultValue(srcDefRaw)
	normDstDef := normalizeDefaultValue(dstDefRaw)

	log = log.With(
		zap.String("src_def_raw", srcDefRaw), zap.String("dst_def_raw", dstDefRaw),
		zap.String("norm_src_def", normSrcDef), zap.String("norm_dst_def", normDstDef),
		zap.String("type_for_comparison", typeForDefaultComparison),
	)

	if normSrcDef == normDstDef {
		log.Debug("Normalized default values are identical.")
		return true
	}

	// Jika satu NULL eksplisit ("null") dan yang lain string kosong (tidak ada default), anggap sama.
	if (normSrcDef == "null" && normDstDef == "") || (normSrcDef == "" && normDstDef == "null") {
		log.Debug("One default is explicit 'null' and other is empty (no default), considered equivalent.")
		return true
	}


	// Perbandingan khusus untuk tipe numerik
	if isNumericType(typeForDefaultComparison) {
		srcNum, errSrc := strconv.ParseFloat(normSrcDef, 64)
		dstNum, errDst := strconv.ParseFloat(normDstDef, 64)

		if errSrc == nil && errDst == nil { // Keduanya berhasil diparse sebagai angka
			if srcNum == dstNum {
				log.Debug("Numeric default values are equivalent after parsing.", zap.Float64("src_num", srcNum), zap.Float64("dst_num", dstNum))
				return true
			}
			log.Debug("Numeric default values differ.", zap.Float64("src_num", srcNum), zap.Float64("dst_num", dstNum))
			return false // Keduanya angka tapi nilainya beda
		}

		// Jika salah satu angka, yang lain bukan (dan bukan fungsi yang diketahui)
		isSrcKnownFunc := normSrcDef == "nextval" || normSrcDef == "current_timestamp" || normSrcDef == "current_date" || normSrcDef == "current_time" || normSrcDef == "uuid_function"
		isDstKnownFunc := normDstDef == "nextval" || normDstDef == "current_timestamp" || normDstDef == "current_date" || normDstDef == "current_time" || normDstDef == "uuid_function"

		if (errSrc == nil && errDst != nil && !isDstKnownFunc) || (errSrc != nil && errDst == nil && !isSrcKnownFunc) {
			log.Debug("One default is numeric, the other is not (and not a known function).")
			return false
		}
	}

	// Periksa apakah perbedaan hanya pada quoting untuk string.
	// `normalizeDefaultValue` sudah menghapus quote terluar.
	// Jika setelah itu masih berbeda, maka memang berbeda.

	log.Debug("Default values are not equivalent after all checks.")
	return false
}
