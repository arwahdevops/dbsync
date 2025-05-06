package sync

import (
	"fmt"
	"reflect" // Diperlukan untuk DeepEqual
	"regexp"
	"sort"    // Diperlukan untuk sorting kolom
	"strconv" // Diperlukan untuk konversi numerik
	"strings"

	"go.uber.org/zap"
)

// --- Perbandingan Skema untuk Strategi ALTER ---

// needsColumnModification mengembalikan true jika ada perbedaan signifikan.
func (s *SchemaSyncer) needsColumnModification(src, dst ColumnInfo, log *zap.Logger) bool {
	return len(s.getColumnModifications(src, dst, log)) > 0
}

// getColumnModifications mengembalikan daftar string perbedaan.
func (s *SchemaSyncer) getColumnModifications(src, dst ColumnInfo, log *zap.Logger) []string {
	diffs := []string{}
	// Pastikan tipe sumber dipetakan
	if src.MappedType == "" {
		var mapErr error
		src.MappedType, mapErr = s.mapDataType(src.Type) // Use receiver s.
		if mapErr != nil {
			log.Warn("Cannot map source type during comparison, skipping type check", zap.String("column", src.Name), zap.String("src_type", src.Type), zap.Error(mapErr))
			// Jangan lanjutkan perbandingan tipe jika mapping gagal
		} else {
			// 1. Tipe (Perbandingan yang disempurnakan)
			if !s.areTypesEquivalent(src.MappedType, dst.Type, src, dst, log) { // Use receiver s.
				diffs = append(diffs, fmt.Sprintf("type (src: %s [%s], dst: %s)", src.Type, src.MappedType, dst.Type))
			}
		}
	} else {
		// Tipe sudah ada, lakukan perbandingan
		if !s.areTypesEquivalent(src.MappedType, dst.Type, src, dst, log) { // Use receiver s.
			diffs = append(diffs, fmt.Sprintf("type (src: %s [%s], dst: %s)", src.Type, src.MappedType, dst.Type))
		}
	}


	// 2. Nullability
	if src.IsNullable != dst.IsNullable {
		diffs = append(diffs, fmt.Sprintf("nullability (src: %t, dst: %t)", src.IsNullable, dst.IsNullable))
	}
	// 3. Default
	srcDef := ""; if src.DefaultValue.Valid { srcDef = src.DefaultValue.String }
	dstDef := ""; if dst.DefaultValue.Valid { dstDef = dst.DefaultValue.String }
	if !s.areDefaultsEquivalent(srcDef, dstDef, src.MappedType, log) { // Use receiver s.
		diffs = append(diffs, fmt.Sprintf("default (src: '%s', dst: '%s')", srcDef, dstDef))
	}
	// 4. Collation (jika relevan & terdeteksi)
	srcCollation := ""; if src.Collation.Valid { srcCollation = src.Collation.String }
	dstCollation := ""; if dst.Collation.Valid { dstCollation = dst.Collation.String }
	if srcCollation != "" && dstCollation != "" && !strings.EqualFold(srcCollation, dstCollation) {
		diffs = append(diffs, fmt.Sprintf("collation (src: %s, dst: %s)", srcCollation, dstCollation))
	}
	// 5. Comment (opsional)
	srcComment := ""; if src.Comment.Valid { srcComment = src.Comment.String }
	dstComment := ""; if dst.Comment.Valid { dstComment = dst.Comment.String }
	if srcComment != dstComment {
		// log.Debug("Column comment mismatch", zap.String("col", src.Name)) // Non-blocking difference
	}
	// 6. AutoIncrement / Identity / Generated
	if src.AutoIncrement != dst.AutoIncrement {
		diffs = append(diffs, fmt.Sprintf("auto_increment (src: %t, dst: %t)", src.AutoIncrement, dst.AutoIncrement))
		log.Warn("AutoIncrement modification detected, may require complex ALTER or table rebuild.", zap.String("col", src.Name))
	}
	if src.IsGenerated != dst.IsGenerated { // Perlu ambil IsGenerated untuk dst juga
		diffs = append(diffs, fmt.Sprintf("generated_status (src: %t, dst: %t)", src.IsGenerated, dst.IsGenerated))
		log.Warn("Generated column status modification detected, may require complex ALTER.", zap.String("col", src.Name))
	}

	if len(diffs) > 0 {
		log.Debug("Column differences detected", zap.String("column", src.Name), zap.Strings("differences", diffs))
	}
	return diffs
}

// areTypesEquivalent - Perbandingan tipe yang lebih canggih
func (s *SchemaSyncer) areTypesEquivalent(mappedSrcType, dstType string, srcInfo, dstInfo ColumnInfo, log *zap.Logger) bool {
	// Normalisasi dasar (lowercase, trim, hapus ukuran integer generik)
	normSrc, normDst := normalizeTypeName(mappedSrcType), normalizeTypeName(dstType)

	if normSrc == normDst {
		// Jika tipe dasar sama, bandingkan atribut (length, precision, scale)
		if srcInfo.Length.Valid != dstInfo.Length.Valid || (srcInfo.Length.Valid && srcInfo.Length.Int64 != dstInfo.Length.Int64) {
			if isStringType(normSrc) { log.Debug("Type length mismatch", zap.String("col", srcInfo.Name)); return false }
		}
		if srcInfo.Precision.Valid != dstInfo.Precision.Valid || (srcInfo.Precision.Valid && srcInfo.Precision.Int64 != dstInfo.Precision.Int64) {
			if isPrecisionRelevant(normSrc) { log.Debug("Type precision mismatch", zap.String("col", srcInfo.Name)); return false }
		}
		if srcInfo.Scale.Valid != dstInfo.Scale.Valid || (srcInfo.Scale.Valid && srcInfo.Scale.Int64 != dstInfo.Scale.Int64) {
			if isScaleRelevant(normSrc) { log.Debug("Type scale mismatch", zap.String("col", srcInfo.Name)); return false }
		}
		return true // Tipe dasar & atribut relevan cocok
	}

	// Handle alias umum
	aliases := map[string]string{"integer": "int", "character varying": "varchar", "double precision": "double", "boolean": "bool"}
	if mapped, ok := aliases[normSrc]; ok { normSrc = mapped }
	if mapped, ok := aliases[normDst]; ok { normDst = mapped }
	if normSrc == normDst { return true } // Cocok setelah alias

	// Handle kasus khusus antar dialek (contoh)
	if (strings.Contains(normSrc, "tinyint") && strings.Contains(normDst, "bool")) || (strings.Contains(normSrc, "bool") && strings.Contains(normDst, "tinyint")) { return true }
	if (strings.Contains(normSrc, "decimal") && strings.Contains(normDst, "numeric")) || (strings.Contains(normSrc, "numeric") && strings.Contains(normDst, "decimal")) {
		// Sebaiknya bandingkan precision/scale juga di sini
		return srcInfo.Precision == dstInfo.Precision && srcInfo.Scale == dstInfo.Scale
	}
	if (normSrc == "timestamp with time zone" && normDst == "timestamp") || (normSrc == "timestamp" && normDst == "timestamp with time zone") {
		log.Warn("Comparing timestamp tz vs non-tz, assuming compatible", zap.String("col", srcInfo.Name))
		return true
	}


	log.Debug("Type comparison failed", zap.String("srcMap", mappedSrcType), zap.String("dstRaw", dstType), zap.String("normSrc", normSrc), zap.String("normDst", normDst))
	return false
}

// areDefaultsEquivalent - Perbandingan default yang lebih canggih
func (s *SchemaSyncer) areDefaultsEquivalent(srcDef, dstDef, mappedType string, log *zap.Logger) bool {
	srcIsNull := (srcDef == "" || strings.ToLower(srcDef) == "null"); dstIsNull := (dstDef == "" || strings.ToLower(dstDef) == "null")
	if srcIsNull && dstIsNull { return true }; if srcIsNull != dstIsNull { return false }

	// Normalisasi nilai default (hapus quote, lowercase untuk fungsi umum)
	normSrc := normalizeDefaultValue(srcDef)
	normDst := normalizeDefaultValue(dstDef)

	if normSrc == normDst { return true }

	// Coba perbandingan numerik jika tipe memungkinkan
	if isNumericType(mappedType) {
		srcNum, errSrc := strconv.ParseFloat(normSrc, 64)
		dstNum, errDst := strconv.ParseFloat(normDst, 64)
		if errSrc == nil && errDst == nil && srcNum == dstNum {
			return true
		}
	}

	log.Debug("Default value comparison failed", zap.String("src", srcDef), zap.String("dst", dstDef), zap.String("normSrc", normSrc), zap.String("normDst", normDst))
	return false
}

// needsIndexModification - Membandingkan definisi indeks (dasar)
func (s *SchemaSyncer) needsIndexModification(src, dst IndexInfo, log *zap.Logger) bool {
	if src.IsUnique != dst.IsUnique { log.Debug("Index unique mismatch", zap.String("idx", src.Name)); return true }
	// Urutkan kolom sebelum membandingkan slice
	sort.Strings(src.Columns) // Gunakan sort dari paket sort
	sort.Strings(dst.Columns) // Gunakan sort dari paket sort
	if !reflect.DeepEqual(src.Columns, dst.Columns) { log.Debug("Index columns mismatch", zap.String("idx", src.Name)); return true }
	// TODO: Bandingkan tipe indeks (BTREE, HASH, dll.) jika info tersedia? RawDef?
	return false
}

// needsConstraintModification - Membandingkan definisi constraint (dasar)
func (s *SchemaSyncer) needsConstraintModification(src, dst ConstraintInfo, log *zap.Logger) bool {
	if src.Type != dst.Type { log.Debug("Constraint type mismatch", zap.String("cons", src.Name)); return true }
	// Urutkan kolom sebelum membandingkan slice
	sort.Strings(src.Columns) // Gunakan sort dari paket sort
	sort.Strings(dst.Columns) // Gunakan sort dari paket sort
	if !reflect.DeepEqual(src.Columns, dst.Columns) { log.Debug("Constraint columns mismatch", zap.String("cons", src.Name)); return true }
	switch src.Type {
	case "FOREIGN KEY":
		if src.ForeignTable != dst.ForeignTable { log.Debug("FK foreign table mismatch", zap.String("cons", src.Name)); return true }
		sort.Strings(src.ForeignColumns); sort.Strings(dst.ForeignColumns) // Gunakan sort dari paket sort
		if !reflect.DeepEqual(src.ForeignColumns, dst.ForeignColumns) { log.Debug("FK foreign columns mismatch", zap.String("cons", src.Name)); return true }
		srcDel, srcUpd := normalizeFKAction(src.OnDelete), normalizeFKAction(src.OnUpdate); dstDel, dstUpd := normalizeFKAction(dst.OnDelete), normalizeFKAction(dst.OnUpdate)
		if srcDel != dstDel { log.Debug("FK ON DELETE mismatch", zap.String("cons", src.Name)); return true }
		if srcUpd != dstUpd { log.Debug("FK ON UPDATE mismatch", zap.String("cons", src.Name)); return true }
	case "CHECK":
		// Perbandingan definisi CHECK bisa rumit (whitespace, case, dll.)
		normSrcDef := normalizeCheckDefinition(src.Definition)
		normDstDef := normalizeCheckDefinition(dst.Definition)
		if normSrcDef != normDstDef { log.Debug("CHECK definition mismatch", zap.String("cons", src.Name)); return true }
	}
	return false
}

// --- Helper Normalisasi & Perbandingan Lanjutan ---
func normalizeTypeName(typeName string) string { name := strings.ToLower(strings.TrimSpace(typeName)); if strings.HasPrefix(name, "int") || strings.HasPrefix(name, "bigint") || strings.HasPrefix(name, "smallint") || strings.HasPrefix(name, "mediumint") { name = regexp.MustCompile(`\(\d+\)`).ReplaceAllString(name, "") }; name = strings.ReplaceAll(name, " unsigned", ""); name = strings.ReplaceAll(name, " zerofill", ""); return name }
func normalizeDefaultValue(def string) string { lower := strings.ToLower(strings.TrimSpace(def)); lower = strings.Trim(lower, "'\"`"); if lower == "now()" || lower == "current_timestamp" { return "current_timestamp" }; return lower }
func normalizeFKAction(action string) string { upper := strings.ToUpper(strings.TrimSpace(action)); if upper == "" { return "NO ACTION" }; return upper }
func normalizeCheckDefinition(def string) string { norm := strings.ToLower(def); norm = regexp.MustCompile(`\s+`).ReplaceAllString(norm, " "); return strings.TrimSpace(norm) }
func isStringType(normTypeName string) bool { return strings.Contains(normTypeName, "char") || strings.Contains(normTypeName, "text") || strings.Contains(normTypeName, "enum") || strings.Contains(normTypeName, "set") }
func isNumericType(typeName string) bool { norm := normalizeTypeName(typeName); return strings.Contains(norm, "int") || strings.Contains(norm, "serial") || strings.Contains(norm, "numeric") || strings.Contains(norm, "decimal") || strings.Contains(norm, "float") || strings.Contains(norm, "double") || strings.Contains(norm, "real") }
func isPrecisionRelevant(normTypeName string) bool { return strings.Contains(normTypeName, "decimal") || strings.Contains(normTypeName, "numeric") || strings.Contains(normTypeName, "float") || strings.Contains(normTypeName, "double") || strings.Contains(normTypeName, "real") || strings.Contains(normTypeName, "time") || strings.Contains(normTypeName, "timestamp") }
func isScaleRelevant(normTypeName string) bool { return strings.Contains(normTypeName, "decimal") || strings.Contains(normTypeName, "numeric") }