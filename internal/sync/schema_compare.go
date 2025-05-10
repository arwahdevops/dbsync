// internal/sync/schema_compare.go
package sync

import (
	"reflect" // Tetap dibutuhkan untuk DeepEqual
	"regexp"
	// "sort" // Tidak dibutuhkan lagi untuk sorting kolom di dalam needsIndex/ConstraintModification
	"strings"

	"go.uber.org/zap"
)

// normalizeDefaultValue menormalisasi nilai default untuk perbandingan.
func normalizeDefaultValue(def string, dialect string) string {
	if def == "" {
		return ""
	}
	lower := strings.ToLower(strings.TrimSpace(def))

	stripOuterQuotes := func(s string) string {
		s = strings.TrimSpace(s)
		if len(s) >= 2 {
			firstChar, lastChar := s[0], s[len(s)-1]
			if (firstChar == '\'' && lastChar == '\'') ||
				(firstChar == '"' && lastChar == '"') ||
				(firstChar == '`' && lastChar == '`') {
				return s[1 : len(s)-1]
			}
		}
		return s
	}

	switch strings.ToLower(dialect) {
	case "mysql":
		if strings.Contains(lower, "on update current_timestamp") {
			parts := strings.Split(lower, "on update current_timestamp")
			lower = strings.TrimSpace(parts[0])
		}
		if strings.HasPrefix(lower, "b'") && strings.HasSuffix(lower, "'") && (lower == "b'0'" || lower == "b'1'") {
			lower = lower[2:3]
		}
	case "postgres":
		if strings.HasPrefix(lower, "nextval(") {
			return "nextval"
		}
		previousLower := ""
		for lower != previousLower {
			previousLower = lower
			reCast := regexp.MustCompile(`^(.*?)\s*::\s*[a-zA-Z_][a-zA-Z0-9_."\[\]<> ]*(?:\([^)]*\))?\s*$`)
			matches := reCast.FindStringSubmatch(lower)

			if len(matches) > 1 {
				potentialValue := strings.TrimSpace(matches[1])
				if len(potentialValue) >= 2 && potentialValue[0] == '(' && potentialValue[len(potentialValue)-1] == ')' {
					isKnownWrapper := false
					tempLowerPotential := strings.ToLower(potentialValue)
					knownWrappers := []string{"array[", "row(", "json_build_object(", "jsonb_build_object(", "json_object(", "jsonb_object("}
					for _, prefix := range knownWrappers {
						if strings.HasPrefix(tempLowerPotential, prefix) {
							isKnownWrapper = true
							break
						}
					}
					if !isKnownWrapper {
						balance := 0
						isTrulyOuterPair := true
						for i := 1; i < len(potentialValue)-1; i++ {
							if potentialValue[i] == '(' {
								balance++
							} else if potentialValue[i] == ')' {
								balance--
							}
							if balance < 0 {
								isTrulyOuterPair = false
								break
							}
						}
						if balance != 0 {
							isTrulyOuterPair = false
						}
						if isTrulyOuterPair {
							potentialValue = strings.TrimSpace(potentialValue[1 : len(potentialValue)-1])
						}
					}
				}
				lower = potentialValue
				lower = stripOuterQuotes(lower)
			}
		}
	case "sqlite":
		if strings.EqualFold(lower, "null") {
			return "null"
		}
	}

	switch lower {
	case "now()", "current_timestamp", "current_timestamp()", "getdate()", "sysdatetime()":
		return "current_timestamp"
	case "current_date", "current_date()":
		return "current_date"
	case "current_time", "current_time()":
		return "current_time"
	}

	switch lower {
	case "uuid()", "gen_random_uuid()", "newid()", "uuid_generate_v4()":
		return "uuid_function"
	}

	switch lower {
	case "true", "t", "yes", "y", "on", "1":
		return "1"
	case "false", "f", "no", "n", "off", "0":
		return "0"
	}

	lower = stripOuterQuotes(lower)
	if lower == "null" {
		return "null"
	}
	return lower
}

// normalizeFKAction menormalisasi aksi Foreign Key.
func normalizeFKAction(action string) string {
	upper := strings.ToUpper(strings.TrimSpace(action))
	if upper == "SET DEFAULT" { return "SET DEFAULT" }
	if upper == "SET NULL" { return "SET NULL" }
	if upper == "CASCADE" { return "CASCADE" }
	if upper == "RESTRICT" { return "RESTRICT" }
	if upper == "" || upper == "NO ACTION" { return "NO ACTION" }
	return upper
}

// isBalanced memeriksa apakah string memiliki jumlah tanda kurung buka dan tutup yang sama
// dan tidak pernah memiliki lebih banyak tutup daripada buka pada titik mana pun.
func isBalanced(s string) bool {
	balance := 0
	for _, char := range s {
		if char == '(' {
			balance++
		} else if char == ')' {
			balance--
		}
		if balance < 0 { // Lebih banyak ')' daripada '(' pada suatu titik
			return false
		}
	}
	return balance == 0 // Harus seimbang di akhir
}

// normalizeCheckDefinition menormalisasi definisi CHECK constraint.
func normalizeCheckDefinition(def string) string {
	if def == "" { return "" }
	reCommentLine := regexp.MustCompile(`--.*`)
	norm := reCommentLine.ReplaceAllString(def, "")
	reCommentBlock := regexp.MustCompile(`/\*.*?\*/`) // Non-greedy match
	norm = reCommentBlock.ReplaceAllString(norm, "")
	norm = strings.ToLower(norm) // Lowercase dulu
	norm = regexp.MustCompile(`\s+`).ReplaceAllString(norm, " ")
	norm = strings.TrimSpace(norm)

	for {
		if len(norm) < 2 || norm[0] != '(' || norm[len(norm)-1] != ')' {
			break
		}
		innerContent := norm[1 : len(norm)-1]
		if !isBalanced(innerContent) {
			break
		}
		norm = strings.TrimSpace(innerContent)
	}
	return norm
}

// isDefaultNullOrFunction memeriksa apakah nilai default yang sudah dinormalisasi adalah NULL atau fungsi database umum.
func isDefaultNullOrFunction(normalizedDefaultValue string) bool {
	if normalizedDefaultValue == "" || normalizedDefaultValue == "null" {
		return true
	}
	switch normalizedDefaultValue {
	case "current_timestamp", "current_date", "current_time", "nextval", "uuid_function":
		return true
	}
	return false
}


func (s *SchemaSyncer) needsColumnModification(src, dst ColumnInfo, log *zap.Logger) bool {
	return len(s.getColumnModifications(src, dst, log)) > 0
}

func (s *SchemaSyncer) needsIndexModification(src, dst IndexInfo, log *zap.Logger) bool {
	log = log.With(zap.String("index_name", src.Name))
	if src.IsUnique != dst.IsUnique {
		log.Debug("Index unique flag mismatch", zap.Bool("src_is_unique", src.IsUnique), zap.Bool("dst_is_unique", dst.IsUnique))
		return true
	}

	// *** PERBAIKAN: Bandingkan kolom dengan urutan yang dipertahankan ***
	if !reflect.DeepEqual(src.Columns, dst.Columns) {
		// Log perbedaan jika ada, meskipun kita tidak lagi mengurutkan di sini untuk perbandingan utama.
		// Ini berguna untuk debugging jika urutan yang diterima dari DB berbeda.
		log.Debug("Index columns mismatch (order matters)",
			zap.Strings("src_cols_ordered", src.Columns),
			zap.Strings("dst_cols_ordered", dst.Columns))

		// Sebagai fallback atau pemeriksaan tambahan, kita bisa log perbandingan yang diurutkan
		// untuk melihat apakah hanya urutannya yang berbeda atau kolomnya sendiri berbeda.
		// Namun, untuk `needsModification`, perbedaan urutan sudah cukup.
		// srcColsSorted := make([]string, len(src.Columns)); copy(srcColsSorted, src.Columns); sort.Strings(srcColsSorted)
		// dstColsSorted := make([]string, len(dst.Columns)); copy(dstColsSorted, dst.Columns); sort.Strings(dstColsSorted)
		// if !reflect.DeepEqual(srcColsSorted, dstColsSorted) {
		//  log.Debug("Index columns also mismatch when sorted alphabetically, indicating different column sets.",
		//      zap.Strings("src_cols_sorted", srcColsSorted),
		//      zap.Strings("dst_cols_sorted", dstColsSorted))
		// }
		return true
	}

	// TODO: Pertimbangkan perbandingan IndexType/RawDef jika diperlukan di masa depan,
	// terutama untuk indeks fungsional atau tipe indeks non-standar (GIN, GIST).
	// Saat ini, jika nama, keunikan, dan kolom (dalam urutan) sama, dianggap tidak perlu modifikasi.

	log.Debug("Index definitions appear equivalent based on unique flag and ordered column list.")
	return false
}

func (s *SchemaSyncer) needsConstraintModification(src, dst ConstraintInfo, log *zap.Logger) bool {
	log = log.With(zap.String("constraint_name", src.Name), zap.String("constraint_type", src.Type))
	if src.Type != dst.Type {
		log.Debug("Constraint type mismatch", zap.String("src_type", src.Type), zap.String("dst_type", dst.Type))
		return true
	}

	// *** PERBAIKAN: Bandingkan kolom dengan urutan yang dipertahankan ***
	if !reflect.DeepEqual(src.Columns, dst.Columns) {
		log.Debug("Constraint columns mismatch (order matters)",
			zap.Strings("src_cols_ordered", src.Columns),
			zap.Strings("dst_cols_ordered", dst.Columns))
		return true
	}

	switch src.Type {
	case "FOREIGN KEY":
		if src.ForeignTable != dst.ForeignTable {
			log.Debug("FK foreign table mismatch", zap.String("src_foreign_table", src.ForeignTable), zap.String("dst_foreign_table", dst.ForeignTable))
			return true
		}
		// *** PERBAIKAN: Bandingkan kolom foreign dengan urutan yang dipertahankan ***
		if !reflect.DeepEqual(src.ForeignColumns, dst.ForeignColumns) {
			log.Debug("FK foreign columns mismatch (order matters)",
				zap.Strings("src_foreign_cols_ordered", src.ForeignColumns),
				zap.Strings("dst_foreign_cols_ordered", dst.ForeignColumns))
			return true
		}
		srcDelNorm := normalizeFKAction(src.OnDelete)
		dstDelNorm := normalizeFKAction(dst.OnDelete)
		if srcDelNorm != dstDelNorm {
			log.Debug("FK ON DELETE action mismatch", zap.String("src_on_delete", srcDelNorm), zap.String("dst_on_delete", dstDelNorm))
			return true
		}
		srcUpdNorm := normalizeFKAction(src.OnUpdate)
		dstUpdNorm := normalizeFKAction(dst.OnUpdate)
		if srcUpdNorm != dstUpdNorm {
			log.Debug("FK ON UPDATE action mismatch", zap.String("src_on_update", srcUpdNorm), zap.String("dst_on_update", dstUpdNorm))
			return true
		}
	case "CHECK":
		normSrcDef := normalizeCheckDefinition(src.Definition)
		normDstDef := normalizeCheckDefinition(dst.Definition)
		if normSrcDef != normDstDef {
			log.Debug("CHECK constraint definition mismatch",
				zap.String("src_check_def_norm", normSrcDef), zap.String("dst_check_def_norm", normDstDef),
				zap.String("src_check_def_raw", src.Definition), zap.String("dst_check_def_raw", dst.Definition))
			return true
		}
	case "UNIQUE", "PRIMARY KEY":
		// Untuk UNIQUE dan PK, perbandingan tipe dan kolom (dengan urutan dipertahankan) sudah cukup.
		break
	}
	log.Debug("Constraint definitions appear equivalent.")
	return false
}
