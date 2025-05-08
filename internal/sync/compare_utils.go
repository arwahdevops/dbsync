// internal/sync/compare_utils.go
package sync

import (
	"regexp"
	"strings"
	// strconv tidak diimpor di sini kecuali ada fungsi LAIN di file ini yang menggunakannya.
	// Saat ini, parsing numerik untuk default ada di compare_columns.go.
)

// normalizeTypeName menormalisasi nama tipe data untuk perbandingan.
// Menghapus ukuran generik, spasi berlebih, dan modifier umum.
func normalizeTypeName(typeName string) string {
	name := strings.ToLower(strings.TrimSpace(typeName))
	// Hapus ukuran generik untuk integer (int(11) -> int)
	// dan juga untuk tipe lain seperti varchar(255) -> varchar
	name = regexp.MustCompile(`\s*\(\s*\d+\s*(,\s*\d+\s*)?\)`).ReplaceAllString(name, "")

	// Hapus modifier umum MySQL
	name = strings.ReplaceAll(name, " unsigned", "")
	name = strings.ReplaceAll(name, " zerofill", "")

	// Alias umum
	aliases := map[string]string{
		"integer":                   "int",
		"character varying":         "varchar",
		"double precision":          "double",
		"boolean":                   "bool",
		"timestamp with time zone":    "timestamptz",
		"timestamp without time zone": "timestamp",
		"time with time zone":         "timetz",
		"time without time zone":      "time",
		// Tambahkan alias lain jika perlu, misal dari PostgreSQL
		"int4": "int",
		"int8": "bigint",
		"serial4": "serial", // serial adalah alias untuk int dengan sequence
		"serial8": "bigserial", // bigserial adalah alias untuk bigint dengan sequence
	}
	if mapped, ok := aliases[name]; ok {
		name = mapped
	}
	return strings.TrimSpace(name)
}

// normalizeDefaultValue menormalisasi nilai default untuk perbandingan.
func normalizeDefaultValue(def string) string {
	lower := strings.ToLower(strings.TrimSpace(def))
	// Hapus quote di awal dan akhir
	lower = strings.Trim(lower, "'\"`")

	// Normalisasi fungsi umum
	if lower == "now()" || lower == "current_timestamp" || lower == "getdate()" {
		return "current_timestamp" // Normalisasi ke satu bentuk
	}
	if lower == "current_date" {
		return "current_date"
	}
	if lower == "current_time" {
		return "current_time"
	}
	if strings.HasPrefix(lower, "nextval(") && strings.Contains(lower, "::regclass") { // Postgres nextval
		return "nextval" // Hanya penanda, detail sequence tidak dibandingkan di sini
	}
	if strings.HasPrefix(lower, "gen_random_uuid()") || lower == "uuid()" || lower == "newid()" /* SQL Server */ {
		return "uuid_function"
	}
	if lower == "null" { // Eksplisit NULL
	    return "null"
    }


	// Untuk boolean, coba normalisasi ke "0" atau "1" jika memungkinkan
	if lower == "true" || lower == "t" || lower == "yes" || lower == "y" || lower == "on" {
		return "1"
	}
	if lower == "false" || lower == "f" || lower == "no" || lower == "n" || lower == "off" {
		return "0"
	}

	return lower // Kembalikan versi yang sudah di-trim dan lowercase (atau string kosong jika inputnya kosong)
}

// normalizeFKAction menormalisasi aksi Foreign Key.
func normalizeFKAction(action string) string {
	upper := strings.ToUpper(strings.TrimSpace(action))
	if upper == "" || upper == "NO ACTION" || upper == "RESTRICT" { // RESTRICT seringkali sama dengan NO ACTION secara default
		return "NO ACTION" // Atau "RESTRICT" jika ingin lebih eksplisit dan membedakannya
	}
	return upper
}

// normalizeCheckDefinition menormalisasi definisi CHECK constraint.
func normalizeCheckDefinition(def string) string {
	if def == "" {
		return ""
	}
	// Hapus komentar SQL (-- style)
	reComment := regexp.MustCompile(`--.*`)
	norm := reComment.ReplaceAllString(def, "")

	// Lowercase, ganti spasi berlebih dengan satu spasi, trim
	norm = strings.ToLower(norm)
	norm = regexp.MustCompile(`\s+`).ReplaceAllString(norm, " ") // Ganti multiple whitespace dengan satu spasi
	norm = strings.TrimSpace(norm)

	return norm
}

// isStringType memeriksa apakah tipe adalah tipe string.
func isStringType(normTypeName string) bool {
	return strings.Contains(normTypeName, "char") ||
		strings.Contains(normTypeName, "text") ||
		strings.Contains(normTypeName, "enum") ||
		strings.Contains(normTypeName, "set") ||
		normTypeName == "uuid" ||
		normTypeName == "json" ||
		normTypeName == "jsonb" ||
		normTypeName == "xml"
}

// isBinaryType memeriksa apakah tipe adalah tipe biner.
func isBinaryType(normTypeName string) bool {
	return strings.Contains(normTypeName, "binary") ||
		strings.Contains(normTypeName, "blob") ||
		normTypeName == "bytea"
}

// isNumericType memeriksa apakah tipe adalah tipe numerik (integer atau desimal/float).
func isNumericType(typeName string) bool {
	norm := normalizeTypeName(typeName)
	return strings.Contains(norm, "int") ||
		strings.Contains(norm, "serial") ||
		strings.Contains(norm, "decimal") ||
		strings.Contains(norm, "numeric") ||
		strings.Contains(norm, "float") ||
		strings.Contains(norm, "double") ||
		strings.Contains(norm, "real") ||
		strings.Contains(norm, "money")
}

// isIntegerType memeriksa apakah tipe adalah tipe integer (termasuk serial).
func isIntegerType(typeName string) bool {
	norm := normalizeTypeName(typeName)
	return strings.Contains(norm, "int") || strings.Contains(norm, "serial")
}

// isPrecisionRelevant memeriksa apakah presisi relevan untuk tipe ini.
func isPrecisionRelevant(normTypeName string) bool {
	return strings.Contains(normTypeName, "decimal") ||
		strings.Contains(normTypeName, "numeric") ||
		strings.Contains(normTypeName, "time") ||
		strings.Contains(normTypeName, "timestamp")
}

// isScaleRelevant memeriksa apakah skala relevan untuk tipe ini.
func isScaleRelevant(normTypeName string) bool {
	return strings.Contains(normTypeName, "decimal") ||
		strings.Contains(normTypeName, "numeric")
}

// isDefaultNullOrFunction memeriksa apakah nilai default adalah NULL atau fungsi database umum.
// Mengembalikan true jika default adalah null, kosong, atau fungsi yang diketahui.
// INI ADALAH FUNGSI STATIS.
func isDefaultNullOrFunction(defaultValueRaw string) bool {
	normValue := normalizeDefaultValue(defaultValueRaw) // Gunakan helper normalisasi kita

	if normValue == "" || normValue == "null" { // Tidak ada default atau eksplisit NULL
		return true
	}
	// Cek fungsi umum yang sudah dinormalisasi oleh normalizeDefaultValue
	switch normValue {
	case "current_timestamp", "current_date", "current_time", "nextval", "uuid_function":
		return true
	}
	// Jika tidak cocok dengan di atas, anggap itu nilai literal.
	return false
}
