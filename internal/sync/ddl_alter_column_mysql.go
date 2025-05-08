// internal/sync/ddl_alter_column_mysql.go
package sync

import (
	"fmt"
	// "strings" // Mungkin tidak dibutuhkan di sini

	"go.uber.org/zap"
	"github.com/arwahdevops/dbsync/internal/utils" // Untuk QuoteIdentifier
)

// generateMySQLModifyColumnDDLs menghasilkan DDL untuk mengubah kolom di MySQL.
// MySQL umumnya menggunakan satu statement `MODIFY COLUMN` dengan definisi kolom penuh yang baru.
// Parameter `diffs` (string perbedaan) tidak secara langsung digunakan untuk membentuk DDL MySQL
// karena MySQL memerlukan definisi penuh, tetapi berguna untuk logging atau keputusan di level atas.
func (s *SchemaSyncer) generateMySQLModifyColumnDDLs(table string, src ColumnInfo, dst ColumnInfo, diffs []string, log *zap.Logger) []string {
	ddls := make([]string, 0)
	log = log.With(zap.String("table", table), zap.String("column", src.Name), zap.String("dialect", "mysql"))

	// Jika tidak ada perbedaan, tidak perlu DDL
	if len(diffs) == 0 {
		log.Debug("No differences found, no ALTER DDL needed for column.")
		return ddls
	}

	// Untuk kolom generated, kita biasanya tidak mencoba ALTER definisinya melalui dbsync.
	// Pengguna mungkin perlu menghapus dan membuat ulang secara manual jika ekspresi berubah.
	if src.IsGenerated {
		log.Warn("Skipping ALTER COLUMN DDL generation for generated MySQL column. Changes to generated columns usually require manual intervention or DROP/ADD.",
			zap.Strings("differences", diffs))
		return ddls
	}

	// MappedType pada 'src' harus sudah valid dan merepresentasikan tipe tujuan.
	if src.MappedType == "" {
		log.Error("Cannot generate MySQL MODIFY COLUMN DDL: MappedType for source column is missing and column is not generated.")
		return ddls
	}

	// MySQL memerlukan definisi kolom penuh untuk `MODIFY COLUMN`.
	// Kita menggunakan `src` ColumnInfo (yang sudah memiliki MappedType untuk tujuan)
	// untuk menghasilkan definisi ini.
	// Fungsi mapColumnDefinition akan menggunakan src.MappedType, src.IsNullable, src.DefaultValue, dll.
	// untuk membuat string definisi kolom yang sesuai untuk dialek tujuan (MySQL).
	fullColumnDefinition, err := s.mapColumnDefinition(src)
	if err != nil {
		log.Error("Failed to map target column definition for MySQL MODIFY COLUMN", zap.Error(err))
		return ddls
	}

	quotedTable := utils.QuoteIdentifier(table, s.dstDialect) // s.dstDialect akan "mysql"
	// Quoting untuk nama kolom sudah ditangani oleh mapColumnDefinition
	// jadi fullColumnDefinition sudah berisi nama kolom yang di-quote.

	// Contoh DDL: ALTER TABLE `my_table` MODIFY COLUMN `my_column` VARCHAR(100) NULL DEFAULT 'hello';
	ddl := fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s;", quotedTable, fullColumnDefinition)
	log.Info("Generated MySQL MODIFY COLUMN DDL", zap.String("ddl", ddl), zap.Strings("based_on_diffs", diffs))
	ddls = append(ddls, ddl)

	return ddls
}
