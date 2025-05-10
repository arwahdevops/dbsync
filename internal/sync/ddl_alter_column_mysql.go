// internal/sync/ddl_alter_column_mysql.go
package sync

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
	"github.com/arwahdevops/dbsync/internal/utils"
)

func (s *SchemaSyncer) generateMySQLModifyColumnDDLs(table string, src ColumnInfo, dst ColumnInfo, diffs []string, log *zap.Logger) []string {
	ddls := make([]string, 0)

	hasTypeChange := false
	for _, diff := range diffs {
		if strings.HasPrefix(diff, "type") {
			hasTypeChange = true
			break
		}
	}

	if hasTypeChange {
		log.Warn("Significant column type change detected for MySQL. The generated MODIFY/CHANGE COLUMN statement might fail or cause data loss/truncation if the conversion is not directly supported or safe by MySQL. Manual data migration steps might be safer for complex type changes.",
			zap.String("source_original_type", src.Type), // Tipe asli dari sumber
			zap.String("destination_original_type", dst.Type), // Tipe asli saat ini di tujuan
			zap.String("target_type_from_source_mapping", src.MappedType)) // Tipe yang diinginkan di tujuan, berdasarkan mapping dari sumber
	}

	// *** PERUBAHAN DI SINI ***
	// mapColumnDefinition sekarang mengembalikan nama (tidak digunakan di sini karena nama tidak berubah)
	// dan definisi tipe+atribut secara terpisah.
	// `src` ColumnInfo (yang MappedType-nya sudah diisi sesuai keinginan di tujuan) digunakan.
	_, columnTypeDefAndAttributes, errMap := s.mapColumnDefinition(src)
	if errMap != nil {
		log.Error("Failed to map target column definition for MySQL MODIFY COLUMN", zap.Error(errMap))
		return ddls // Kembalikan slice kosong jika gagal memetakan
	}

	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)
	quotedColumnNameToModify := utils.QuoteIdentifier(src.Name, s.dstDialect) // Nama kolom yang akan diubah

	// Buat statement MODIFY COLUMN.
	// Contoh DDL: ALTER TABLE `my_table` MODIFY COLUMN `my_column` VARCHAR(100) NULL DEFAULT 'hello';
	// `columnTypeDefAndAttributes` sudah berisi semua yang dibutuhkan setelah nama kolom.
	ddl := fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s;",
		quotedTable,
		quotedColumnNameToModify,
		columnTypeDefAndAttributes)

	log.Info("Generated MySQL MODIFY COLUMN DDL.",
		zap.String("ddl", ddl),
		zap.Strings("based_on_diffs", diffs))
	ddls = append(ddls, ddl)

	return ddls
}
