// internal/sync/ddl_alter_column_mysql.go
package sync

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
	"github.com/arwahdevops/dbsync/internal/utils"
)

// generateMySQLModifyColumnDDLs menghasilkan DDLs untuk mengubah kolom di MySQL.
// MySQL biasanya menggunakan `MODIFY COLUMN` untuk sebagian besar perubahan atribut kolom.
// Fungsi ini menerima `diffs` dari `getColumnModifications` untuk logging dan konteks.
func (s *SchemaSyncer) generateMySQLModifyColumnDDLs(table string, src ColumnInfo, dst ColumnInfo, diffs []string, log *zap.Logger) []string {
	ddls := make([]string, 0)
	// Logger sudah di-scope oleh pemanggil (misalnya, generateAlterDDLs atau langsung dari processSingleTable).
	// Jika log dipanggil langsung dari sini, pastikan ia memiliki konteks yang cukup.

	hasTypeChange := false
	originalSourceTypeForLog := src.Type // Simpan tipe asli sumber untuk logging jika MappedType berbeda
	targetTypeDDLFromMapping := src.MappedType // Tipe yang diinginkan di tujuan, dari hasil mapping

	for _, diff := range diffs {
		if strings.HasPrefix(diff, "type") {
			hasTypeChange = true
			break
		}
	}

	if hasTypeChange {
		// Peringatan ini penting karena perubahan tipe di MySQL bisa berisiko.
		log.Warn("Significant column type change detected for MySQL. The generated MODIFY COLUMN statement might fail or cause data loss/truncation if the conversion is not directly supported or safe by MySQL. Manual data migration steps might be safer for complex type changes.",
			zap.String("table", table), // Tambahkan konteks tabel ke log
			zap.String("column", src.Name), // Tambahkan konteks kolom
			zap.String("source_original_type_for_reference", originalSourceTypeForLog),
			zap.String("destination_current_type", dst.Type), 
			zap.String("desired_target_type_from_mapping", targetTypeDDLFromMapping))
	}

	// `src` ColumnInfo sudah berisi MappedType yang benar dan atribut lain yang diinginkan untuk target.
	// `mapColumnDefinition` akan menggunakan `src.MappedType` sebagai tipe dasar target
	// dan menggabungkannya dengan atribut lain (NULL, DEFAULT, AUTO_INCREMENT, COLLATE, COMMENT)
	// dari `src` untuk membentuk definisi kolom lengkap untuk dialek MySQL.

	// Kita tidak membutuhkan `quotedColNameFromMap` karena nama kolom tidak berubah saat MODIFY.
	_ /* quotedColNameFromMap */, columnTypeDefAndAttributes, errMap := s.mapColumnDefinition(src)
	if errMap != nil {
		log.Error("Failed to map target column definition for MySQL MODIFY COLUMN. No DDL will be generated for this modification.",
			zap.String("table", table),
			zap.String("column", src.Name),
			zap.String("source_original_type", originalSourceTypeForLog),
			zap.String("mapped_target_type_attempted", targetTypeDDLFromMapping),
			zap.Error(errMap))
		return ddls // Kembalikan slice kosong jika gagal memetakan definisi kolom
	}

	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)
	quotedColumnNameToModify := utils.QuoteIdentifier(src.Name, s.dstDialect)

	// Buat statement MODIFY COLUMN.
	// Contoh DDL: ALTER TABLE `my_table` MODIFY COLUMN `my_column` VARCHAR(100) NULL DEFAULT 'hello' COMMENT 'komen';
	ddl := fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s;",
		quotedTable,
		quotedColumnNameToModify,
		columnTypeDefAndAttributes) // Ini adalah definisi kolom lengkap setelah nama.

	log.Info("Generated MySQL MODIFY COLUMN DDL.",
		zap.String("table", table),
		zap.String("column", src.Name),
		zap.String("ddl", ddl),
		zap.Strings("based_on_diffs", diffs)) // Log perbedaan yang memicu DDL ini
	ddls = append(ddls, ddl)

	return ddls
}
