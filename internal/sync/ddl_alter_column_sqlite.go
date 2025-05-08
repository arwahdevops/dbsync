// internal/sync/ddl_alter_column_sqlite.go
package sync

import (
	// "fmt"
	// "strings"

	"go.uber.org/zap"
	// "github.com/arwahdevops/dbsync/internal/utils"
)

// generateSQLiteModifyColumnDDLs memberikan peringatan karena keterbatasan SQLite.
// SQLite memiliki dukungan yang sangat terbatas untuk `ALTER TABLE MODIFY COLUMN`.
// Perubahan tipe, constraint NOT NULL, DEFAULT biasanya tidak didukung secara langsung.
// Pengguna mungkin perlu membuat ulang tabel.
func (s *SchemaSyncer) generateSQLiteModifyColumnDDLs(table string, src ColumnInfo, dst ColumnInfo, diffs []string, log *zap.Logger) []string {
	log = log.With(zap.String("table", table), zap.String("column", src.Name), zap.String("dialect", "sqlite"))

	if len(diffs) == 0 {
		log.Debug("No differences found, no ALTER DDL needed for column.")
		return []string{}
	}

	if src.IsGenerated {
		log.Warn("Skipping ALTER COLUMN DDL generation for generated SQLite column.", zap.Strings("differences", diffs))
		return []string{}
	}


	log.Warn("SQLite has very limited support for altering column definitions directly (type, nullability, default). "+
		"Detected differences will likely require manual table recreation or will be ignored.",
		zap.Strings("differences", diffs))

	// SQLite >= 3.25.0 mendukung RENAME COLUMN.
	// SQLite >= 3.35.0 mendukung DROP COLUMN.
	// ADD COLUMN didukung.
	// MODIFY COLUMN (mengubah tipe, constraint) TIDAK didukung secara umum.

	// Jika ada perbedaan, kita tidak menghasilkan DDL karena kemungkinan besar akan gagal.
	return []string{}
}
