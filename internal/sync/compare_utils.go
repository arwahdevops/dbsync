// internal/sync/compare_utils.go
package sync

import (
	"regexp"
	"strings"
	// strconv tidak secara langsung digunakan di file ini lagi, karena parsing default numerik ada di compare_columns.go
)

// normalizeTypeName menormalisasi nama tipe data untuk perbandingan.
// Menghapus ukuran generik, spasi berlebih, dan modifier umum.
func normalizeTypeName(typeName string) string {
	name := strings.ToLower(strings.TrimSpace(typeName))

	// Hapus modifier umum MySQL terlebih dahulu
	name = strings.ReplaceAll(name, " unsigned", "")
	name = strings.ReplaceAll(name, " zerofill", "")
	name = strings.TrimSpace(name) // Penting setelah replace

	// Sekarang hapus ukuran/modifier generik dalam tanda kurung, misal (11) atau (255) atau (10,2)
	// Regex ini akan menghapus '(...)' dari akhir string tipe, atau jika diikuti spasi.
	// Kita buat agar bisa menangani spasi setelahnya juga atau jika itu satu-satunya modifier
	name = regexp.MustCompile(`\s*\([^)]*\)\s*$`).ReplaceAllString(name, "") // Modifikasi regex di sini
	name = regexp.MustCompile(`\s*\([^)]*\)$`).ReplaceAllString(name, "")   // Jalankan lagi untuk kasus tanpa spasi di akhir
	name = strings.TrimSpace(name)                                       // Trim lagi


	// Alias umum (perlu diperluas berdasarkan kebutuhan)
	// Urutan penting jika ada alias yang merupakan substring dari yang lain.
	aliases := map[string]string{
		"character varying":         "varchar", // PostgreSQL
		"double precision":          "double",  // PostgreSQL
		"boolean":                   "bool",
		"timestamp with time zone":    "timestamptz", // PostgreSQL
		"timestamp without time zone": "timestamp",   // PostgreSQL
		"time with time zone":         "timetz",      // PostgreSQL
		"time without time zone":      "time",        // PostgreSQL
		"integer":                   "int",         // Bisa dari berbagai DB
		"int4":                      "int",         // PostgreSQL alias
		"int8":                      "bigint",      // PostgreSQL alias
		"serial4":                   "serial",      // PostgreSQL (seringkali int + sequence)
		"serial8":                   "bigserial",   // PostgreSQL (seringkali bigint + sequence)
		// Tambahkan alias lain jika perlu
	}
	if mapped, ok := aliases[name]; ok {
		name = mapped
	}
	return strings.TrimSpace(name)
}

// normalizeDefaultValue menormalisasi nilai default untuk perbandingan.
// Menerima `dialect` untuk normalisasi spesifik.
func normalizeDefaultValue(def string, dialect string) string {
	if def == "" {
		return ""
	}

	lower := strings.ToLower(strings.TrimSpace(def))

	// Helper internal untuk menghapus quote terluar
	stripOuterQuotes := func(s string) string {
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
		// MySQL boolean literal b'0' atau b'1'
		if strings.HasPrefix(lower, "b'") && strings.HasSuffix(lower, "'") && (lower == "b'0'" || lower == "b'1'") {
			lower = lower[2:3] // "0" atau "1"
		}
	case "postgres":
		// Cek nextval DULU, karena bisa mengandung cast di dalamnya
		if strings.HasPrefix(lower, "nextval(") {
			return "nextval" // Jika ini nextval, kita selesai
		}

		// Loop untuk menghapus cast secara berulang (setelah cek nextval)
		for {
			// Regex yang lebih sederhana: cocokkan apa saja sebelum '::' diikuti oleh tipe
			// Kita tidak perlu menangkap tipe setelah '::' secara detail
			reCast := regexp.MustCompile(`^(.*?)\s*::\s*[a-zA-Z_].*$`) // Cocokkan hingga akhir baris setelah ::type
			matches := reCast.FindStringSubmatch(lower)
			if len(matches) > 1 {
				// Ambil bagian sebelum '::'
				lower = strings.TrimSpace(matches[1])
				// Periksa apakah hasil stripping adalah string yang di-quote, jika ya, hapus quotenya
				lower = stripOuterQuotes(lower)
			} else {
				break // Tidak ada lagi cast yang cocok
			}
		}
	case "sqlite":
		if strings.EqualFold(lower, "null") { // "null" literal case-insensitive
			return "null"
		}
	}

	// Normalisasi fungsi umum waktu (setelah potensi stripping cast)
	switch lower {
	case "now()", "current_timestamp", "current_timestamp()", "getdate()", "sysdatetime()":
		return "current_timestamp"
	case "current_date", "current_date()":
		return "current_date"
	case "current_time", "current_time()":
		return "current_time"
	}

	// Normalisasi fungsi UUID umum
	switch lower {
	case "uuid()", "gen_random_uuid()", "newid()", "uuid_generate_v4()":
		return "uuid_function"
	}

	// Normalisasi nilai boolean umum ke "0" atau "1"
	switch lower {
	case "true", "t", "yes", "y", "on", "1":
		return "1"
	case "false", "f", "no", "n", "off", "0":
		return "0"
	}

	// Hapus quote terluar LAGI setelah semua transformasi, karena proses di atas
	// mungkin telah mengekspos string yang sebelumnya di-quote (misalnya dari dalam cast)
	lower = stripOuterQuotes(lower)

	// Jika string adalah "null" (setelah lowercase dan unquote), kembalikan sebagai penanda khusus.
	if lower == "null" {
		return "null"
	}

	return lower
}


// normalizeFKAction menormalisasi aksi Foreign Key.
func normalizeFKAction(action string) string {
	upper := strings.ToUpper(strings.TrimSpace(action))
	// Urutan penting: SET DEFAULT dulu karena bisa mengandung SET NULL
	if upper == "SET DEFAULT" {
		return "SET DEFAULT"
	}
	if upper == "SET NULL" {
		return "SET NULL"
	}
	if upper == "CASCADE" {
		return "CASCADE"
	}
	if upper == "RESTRICT" {
		return "RESTRICT"
	}
	// "NO ACTION" adalah default jika tidak dispesifikasikan atau jika diset secara eksplisit.
	// Di banyak DB, RESTRICT dan NO ACTION berperilaku sama atau sangat mirip.
	// Untuk konsistensi perbandingan, kita bisa memetakan keduanya ke salah satu.
	if upper == "" || upper == "NO ACTION" { // Anggap kosong sebagai NO ACTION
		return "NO ACTION"
	}
	// Jika ada nilai lain yang tidak dikenal, kembalikan apa adanya (uppercase)
	return upper
}

// normalizeCheckDefinition menormalisasi definisi CHECK constraint.
// Ini adalah tugas yang sulit karena melibatkan parsing ekspresi SQL.
// Implementasi ini melakukan normalisasi dasar.
func normalizeCheckDefinition(def string) string {
	if def == "" {
		return ""
	}
	// Hapus komentar SQL (-- style dan /* ... */ style)
	reCommentLine := regexp.MustCompile(`--.*`)
	norm := reCommentLine.ReplaceAllString(def, "")
	reCommentBlock := regexp.MustCompile(`/\*.*?\*/`) // Non-greedy match
	norm = reCommentBlock.ReplaceAllString(norm, "")

	// Lowercase untuk konsistensi (kecuali string literal)
	// Ini rumit karena kita tidak mau mengubah case string literal di dalam CHECK.
	// Untuk sekarang, kita lowercase semuanya, tapi ini bisa jadi masalah.
	// Solusi yang lebih baik memerlukan parser SQL.
	norm = strings.ToLower(norm)

	// Ganti spasi berlebih dengan satu spasi
	norm = regexp.MustCompile(`\s+`).ReplaceAllString(norm, " ")
	norm = strings.TrimSpace(norm)

	// Hapus tanda kurung terluar jika hanya membungkus seluruh ekspresi
	// Contoh: (col > 0) -> col > 0
	if strings.HasPrefix(norm, "(") && strings.HasSuffix(norm, ")") {
		// Pastikan tanda kurung ini memang pasangan terluar
		// Ini bisa salah jika ada tanda kurung bersarang yang kompleks.
		// Untuk normalisasi sederhana, kita coba saja.
		openParens := 0
		balanced := true
		for i := 0; i < len(norm)-1; i++ {
			if norm[i] == '(' {
				openParens++
			} else if norm[i] == ')' {
				openParens--
			}
			if openParens == 0 && i < len(norm)-2 { // Ada penutup sebelum akhir string
				balanced = false
				break
			}
		}
		if balanced && openParens == 1 { // Hanya jika ada satu pasang kurung terluar
			norm = strings.TrimSpace(norm[1 : len(norm)-1])
		}
	}

	return norm
}

// isStringType memeriksa apakah tipe adalah tipe string (setelah normalisasi nama tipe).
func isStringType(normTypeName string) bool {
	return strings.Contains(normTypeName, "char") || // char, varchar, nchar, nvarchar
		strings.Contains(normTypeName, "text") || // tinytext, text, mediumtext, longtext, ntext
		strings.Contains(normTypeName, "clob") ||
		normTypeName == "enum" || // MySQL enum
		normTypeName == "set" || // MySQL set
		normTypeName == "uuid" ||
		normTypeName == "json" || // json, jsonb
		normTypeName == "xml"
}

// isBinaryType memeriksa apakah tipe adalah tipe biner (setelah normalisasi nama tipe).
func isBinaryType(normTypeName string) bool {
	return strings.Contains(normTypeName, "binary") || // binary, varbinary
		strings.Contains(normTypeName, "blob") || // tinyblob, blob, mediumblob, longblob
		normTypeName == "bytea" // PostgreSQL
}

// isNumericType memeriksa apakah tipe adalah tipe numerik (integer atau desimal/float).
func isNumericType(normTypeName string) bool {
	return isIntegerType(normTypeName) ||
		strings.Contains(normTypeName, "decimal") ||
		strings.Contains(normTypeName, "numeric") ||
		strings.Contains(normTypeName, "float") ||
		strings.Contains(normTypeName, "double") ||
		strings.Contains(normTypeName, "real") ||
		strings.Contains(normTypeName, "money") // PostgreSQL, SQL Server
}

// isIntegerType memeriksa apakah tipe adalah tipe integer (termasuk serial).
func isIntegerType(normTypeName string) bool {
	return strings.Contains(normTypeName, "int") || // tinyint, smallint, mediumint, int, bigint
		strings.Contains(normTypeName, "serial") // serial, bigserial
}

// isPrecisionRelevant memeriksa apakah presisi relevan untuk tipe ini (setelah normalisasi nama tipe).
// Presisi adalah jumlah total digit untuk DECIMAL/NUMERIC.
// Presisi adalah jumlah digit fraksional detik untuk TIME/TIMESTAMP.
func isPrecisionRelevant(normTypeName string) bool {
	return strings.Contains(normTypeName, "decimal") ||
		strings.Contains(normTypeName, "numeric") ||
		strings.Contains(normTypeName, "time") || // time, timetz, timestamp, timestamptz
		strings.Contains(normTypeName, "datetime") && strings.Contains(normTypeName, "offset") // SQL Server datetimeoffset
}

// isScaleRelevant memeriksa apakah skala relevan untuk tipe ini (setelah normalisasi nama tipe).
// Skala adalah jumlah digit di belakang koma untuk DECIMAL/NUMERIC.
func isScaleRelevant(normTypeName string) bool {
	return strings.Contains(normTypeName, "decimal") ||
		strings.Contains(normTypeName, "numeric")
}

// isDefaultNullOrFunction memeriksa apakah nilai default yang sudah dinormalisasi adalah NULL atau fungsi database umum.
// Fungsi ini akan dipanggil dari `mapColumnDefinition` di `schema_ddl_create.go`
// atau `generatePostgresModifyColumnDDLs` di `ddl_alter_column_postgres.go`.
// Ini berbeda dari `isKnownDbFunction` di `compare_columns.go` yang tujuannya untuk menghindari parse error.
// Fungsi ini untuk menentukan apakah klausa `DEFAULT` perlu ditambahkan ke DDL.
func isDefaultNullOrFunction(normalizedDefaultValue string) bool {
	if normalizedDefaultValue == "" || normalizedDefaultValue == "null" {
		return true // Tidak ada default atau eksplisit NULL
	}
	// Cek fungsi umum yang sudah dinormalisasi oleh normalizeDefaultValue
	switch normalizedDefaultValue {
	case "current_timestamp", "current_date", "current_time",
		"nextval", // PostgreSQL sequence
		"uuid_function": // Penanda umum untuk fungsi UUID
		return true
	}
	// Jika tidak cocok dengan di atas, anggap itu nilai literal yang perlu klausa DEFAULT.
	return false
}
