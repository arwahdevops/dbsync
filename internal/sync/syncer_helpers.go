package sync

import (
	"sort"
	"strings"
)

// getPKColumnNames mengekstrak nama kolom kunci primer dari daftar ColumnInfo.
func getPKColumnNames(columns []ColumnInfo) []string {
	var pks []string
	for _, c := range columns {
		if c.IsPrimary {
			pks = append(pks, c.Name)
		}
	}
	sort.Strings(pks) // Pastikan urutan konsisten
	return pks
}

// isSystemTable memeriksa apakah tabel adalah tabel sistem berdasarkan dialek.
func (s *SchemaSyncer) isSystemTable(table, dialect string) bool {
	lT := strings.ToLower(table)

	// Cek umum untuk schema system
	if strings.HasPrefix(lT, "information_schema.") {
		return true
	}

	switch dialect {
	case "mysql":
		return strings.HasPrefix(lT, "performance_schema.") ||
			strings.HasPrefix(lT, "mysql.") ||
			strings.HasPrefix(lT, "sys.") ||
			lT == "performance_schema" || lT == "mysql" || lT == "sys"
	case "postgres":
		return strings.HasPrefix(lT, "pg_catalog.") ||
			strings.HasPrefix(lT, "pg_toast.") ||
			lT == "pg_catalog" || lT == "pg_toast"
	case "sqlite":
		// Tabel sistem SQLite biasanya diawali dengan "sqlite_"
		return strings.HasPrefix(lT, "sqlite_")
	default:
		// Jika dialek tidak diketahui, lebih aman untuk tidak menganggapnya tabel sistem
		// kecuali jika ada pola yang sangat umum.
		return false
	}
}
