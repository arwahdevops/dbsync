// internal/sync/ddl_alter_column_mysql.go
package sync

import (
	"fmt"
	"strings" // Diperlukan untuk cek tipe

	"go.uber.org/zap"
	"github.com/arwahdevops/dbsync/internal/utils" // Untuk QuoteIdentifier
)

// generateMySQLModifyColumnDDLs menghasilkan DDL untuk mengubah kolom di MySQL.
// MySQL umumnya menggunakan satu statement `MODIFY COLUMN` (atau `CHANGE COLUMN`)
// dengan definisi kolom penuh yang baru.
// Fungsi ini menerima `diffs` dari `getColumnModifications`.
func (s *SchemaSyncer) generateMySQLModifyColumnDDLs(table string, src ColumnInfo, dst ColumnInfo, diffs []string, log *zap.Logger) []string {
	ddls := make([]string, 0)
	// Logger sudah di-scope oleh pemanggil

	// Tidak perlu cek len(diffs) == 0, IsGenerated, MappedType == "" di sini, pemanggil sudah melakukannya.

	// --- Logika Utama untuk MySQL MODIFY/CHANGE ---

	// Cek apakah ada perubahan tipe data yang signifikan. Perubahan tipe di MySQL
	// melalui MODIFY/CHANGE bisa berisiko kehilangan data atau gagal jika tidak kompatibel.
	hasTypeChange := false
	for _, diff := range diffs {
		if strings.HasPrefix(diff, "type") {
			hasTypeChange = true
			break
		}
	}

	if hasTypeChange {
		// Berikan peringatan keras tentang risiko perubahan tipe di MySQL.
		log.Warn("Significant column type change detected for MySQL. The generated MODIFY/CHANGE COLUMN statement might fail or cause data loss/truncation if the conversion is not directly supported or safe by MySQL. Manual data migration steps (e.g., add new column, copy data, drop old, rename new) might be safer for complex type changes.",
			zap.String("source_original_type", src.Type),
			zap.String("destination_original_type", dst.Type),
			zap.String("target_type", src.MappedType)) // src.MappedType adalah tipe tujuan yang diinginkan

		// === Opsi Kebijakan Lebih Ketat (Opsional) ===
		// Anda bisa memilih untuk *tidak* menghasilkan DDL sama sekali jika ada perubahan tipe
		// yang dianggap terlalu berisiko, memaksa pengguna melakukan intervensi manual.
		// Contoh:
		// if isRiskyMySQLTypeChange(src.Type, dst.Type, src.MappedType) {
		//     log.Error("Risky type change detected for MySQL, MODIFY/CHANGE DDL generation skipped for safety. Manual intervention required.",
		//         zap.String("from_type", dst.Type), zap.String("to_type", src.MappedType))
		//     return []string{} // Jangan generate DDL
		// }
		// ==========================================
	}

	// MySQL memerlukan definisi kolom penuh yang baru.
	// Kita gunakan `src` ColumnInfo (yang sudah memiliki MappedType dan atribut lain yang diinginkan)
	// untuk menghasilkan definisi ini.
	// Fungsi `mapColumnDefinition` akan menggunakan src.MappedType, src.IsNullable, src.DefaultValue, dll.
	// untuk membuat string definisi kolom yang sesuai untuk dialek tujuan (MySQL).
	fullTargetColumnDefinition, errMap := s.mapColumnDefinition(src) // mapColumnDefinition dari schema_ddl_create.go
	if errMap != nil {
		log.Error("Failed to map target column definition for MySQL MODIFY/CHANGE COLUMN", zap.Error(errMap))
		// Jika gagal memetakan definisi, kita tidak bisa membuat DDL yang valid.
		return ddls // Kembalikan slice kosong
	}

	quotedTable := utils.QuoteIdentifier(table, s.dstDialect) // s.dstDialect akan "mysql"
	quotedColumnName := utils.QuoteIdentifier(src.Name, s.dstDialect) // Nama kolom asli

	// MySQL bisa menggunakan MODIFY COLUMN atau CHANGE COLUMN.
	// CHANGE COLUMN memungkinkan penggantian nama kolom juga, tapi kita tidak menanganinya di sini.
	// MODIFY COLUMN cukup untuk mengubah tipe dan atribut lain.
	// Kita perlu memastikan `fullTargetColumnDefinition` *tidak* mengandung nama kolom lagi,
	// karena `mapColumnDefinition` mungkin mengembalikannya. Kita perlu mengekstraknya.

	// `mapColumnDefinition` mengembalikan: `quotedName type attributes`
	// Kita perlu memisahkan `quotedName` dari `type attributes`.
	parts := strings.SplitN(fullTargetColumnDefinition, " ", 2)
	if len(parts) != 2 || parts[0] != quotedColumnName {
		log.Error("Internal error: Mapped column definition format unexpected for MySQL MODIFY.",
			zap.String("mapped_definition", fullTargetColumnDefinition),
			zap.String("expected_quoted_name", quotedColumnName))
		return ddls
	}
	columnTypeDefAndAttributes := parts[1] // Bagian setelah nama kolom (misal, "VARCHAR(100) NULL DEFAULT 'hello'")

	// Buat statement MODIFY COLUMN.
	// Contoh DDL: ALTER TABLE `my_table` MODIFY COLUMN `my_column` VARCHAR(100) NULL DEFAULT 'hello';
	ddl := fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s;",
		quotedTable,
		quotedColumnName,             // Nama kolom yang akan diubah
		columnTypeDefAndAttributes) // Definisi baru (tipe + atribut)

	log.Info("Generated MySQL MODIFY COLUMN DDL.",
		zap.String("ddl", ddl),
		zap.Strings("based_on_diffs", diffs)) // Log perbedaan yang memicu DDL ini
	ddls = append(ddls, ddl)

	// Pertimbangan Tambahan (Tidak Diimplementasikan Otomatis):
	// - Perubahan AUTO_INCREMENT: MODIFY COLUMN bisa mengaturnya, tapi menambahkannya ke kolom
	//   yang ada mungkin memerlukan kolom tersebut menjadi PRIMARY KEY atau UNIQUE.
	//   `mapColumnDefinition` sudah menyertakan AUTO_INCREMENT jika `src.AutoIncrement` true.
	// - Mengubah kolom menjadi/dari GENERATED: Ini biasanya memerlukan DROP dan ADD.

	return ddls
}

/*
// isRiskyMySQLTypeChange (Contoh Fungsi Placeholder - Opsional)
// Fungsi ini akan berisi logika untuk menentukan apakah perubahan tipe
// dari dstType ke targetType dianggap berisiko di MySQL.
func isRiskyMySQLTypeChange(srcOriginalType, dstOriginalType, targetMappedType string) bool {
	normSrc := normalizeTypeName(srcOriginalType)
	normDst := normalizeTypeName(dstOriginalType)
	normTarget := normalizeTypeName(targetMappedType)

	// Contoh kasus berisiko:
	// - Dari tipe data besar (TEXT, BLOB) ke tipe data lebih kecil (VARCHAR, INT) -> Potensi Truncation
	if (strings.Contains(normDst, "text") || strings.Contains(normDst, "blob")) &&
	   !(strings.Contains(normTarget, "text") || strings.Contains(normTarget, "blob")) {
		return true
	}
	// - Dari tipe numerik ke string (jika tidak semua data bisa dikonversi)
	// - Dari string ke numerik (jika ada data non-numerik) -> Gagal atau 0/NULL
	if isNumericType(normDst) && isStringType(normTarget) {
		// Mungkin tidak selalu berisiko, tapi perlu perhatian
	}
	if isStringType(normDst) && isNumericType(normTarget) {
		return true // Berisiko tinggi gagal jika data tidak bersih
	}
	// - Perubahan antara Signed dan Unsigned (jika data negatif ada)
	if strings.Contains(dstOriginalType, "unsigned") && !strings.Contains(targetMappedType, "unsigned") {
		// Potensi masalah jika data besar ada di kolom unsigned asli
	}
	if !strings.Contains(dstOriginalType, "unsigned") && strings.Contains(targetMappedType, "unsigned") {
		// Potensi masalah jika data negatif ada di kolom asli
		return true
	}

	// Tambahkan aturan lain di sini...

	return false // Default: tidak dianggap berisiko oleh fungsi ini
}
*/
