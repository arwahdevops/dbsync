// internal/sync/syncer_helpers.go
package sync

import (
	"sort"
	"strings"
)

// getPKColumnNames mengekstrak nama kolom kunci primer dari daftar ColumnInfo.
// Mengembalikan slice string nama kolom PK yang sudah diurutkan.
func getPKColumnNames(columns []ColumnInfo) []string {
	var pks []string
	for _, c := range columns {
		if c.IsPrimary {
			pks = append(pks, c.Name)
		}
	}
	// Mengurutkan nama PK memastikan urutan yang konsisten,
	// yang penting untuk perbandingan dan pembuatan DDL yang deterministik.
	sort.Strings(pks)
	return pks
}

// isSystemTable memeriksa apakah nama tabel yang diberikan adalah tabel sistem
// berdasarkan dialek database yang umum.
// Ini digunakan untuk mengabaikan sinkronisasi tabel sistem.
func (s *SchemaSyncer) isSystemTable(table, dialect string) bool {
	lowerTableName := strings.ToLower(table)

	// Cek umum untuk skema sistem yang sering ditemukan
	if strings.HasPrefix(lowerTableName, "information_schema.") {
		return true
	}

	switch strings.ToLower(dialect) { // Normalisasi dialek juga
	case "mysql":
		// Tabel/skema sistem umum di MySQL
		return strings.HasPrefix(lowerTableName, "performance_schema.") ||
			strings.HasPrefix(lowerTableName, "mysql.") ||
			strings.HasPrefix(lowerTableName, "sys.") ||
			lowerTableName == "performance_schema" || // Jika hanya nama skema yang diberikan
			lowerTableName == "mysql" ||
			lowerTableName == "sys"
	case "postgres":
		// Tabel/skema sistem umum di PostgreSQL
		return strings.HasPrefix(lowerTableName, "pg_catalog.") ||
			strings.HasPrefix(lowerTableName, "pg_toast.") ||
			lowerTableName == "pg_catalog" || // Jika hanya nama skema
			lowerTableName == "pg_toast"
	case "sqlite":
		// Tabel sistem SQLite biasanya diawali dengan "sqlite_"
		return strings.HasPrefix(lowerTableName, "sqlite_")
	default:
		// Jika dialek tidak diketahui, lebih aman untuk tidak menganggapnya tabel sistem
		// kecuali jika ada pola yang sangat umum yang bisa ditambahkan di sini.
		// SchemaSyncer memiliki logger, jadi bisa log warning jika dialek tidak dikenal.
		// s.logger.Warn("isSystemTable called with unknown dialect, cannot determine if table is system table.",
		//    zap.String("table_name", table),
		//    zap.String("unknown_dialect", dialect))
		return false
	}
}

// truncateForLog adalah helper untuk mempersingkat string untuk logging.
// Fungsi ini akan:
// 1. Mengganti semua karakter newline dan tab dengan spasi tunggal.
// 2. Menggabungkan beberapa spasi menjadi satu spasi.
// 3. Memotong string jika panjangnya melebihi maxLength, menambahkan "..." di akhir.
func truncateForLog(s string, maxLength int) string {
	if maxLength <= 3 { // Perlu setidaknya ruang untuk "..."
		if maxLength <= 0 {
			return "" // Atau "..." jika itu lebih disukai untuk panjang 0 atau negatif
		}
		return strings.Repeat(".", maxLength) // "..." atau ".." atau "."
	}

	// Ganti newline dan tab dengan spasi
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ") // Handle CR juga
	s = strings.ReplaceAll(s, "\t", " ")

	// Gabungkan beberapa spasi menjadi satu
	s = strings.Join(strings.Fields(s), " ") // strings.Fields memisahkan berdasarkan spasi dan menghapus spasi di awal/akhir

	if len(s) > maxLength {
		return s[:maxLength-3] + "..."
	}
	return s
}
