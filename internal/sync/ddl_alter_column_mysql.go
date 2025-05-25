// internal/sync/ddl_alter_column_mysql.go
package sync

import (
	"fmt"
	"strings"

	"github.com/arwahdevops/dbsync/internal/utils"
	"go.uber.org/zap"
)

// generateMySQLModifyColumnDDLs menghasilkan DDLs untuk mengubah kolom di MySQL.
func (s *SchemaSyncer) generateMySQLModifyColumnDDLs(table string, src ColumnInfo, dst ColumnInfo, diffs []string, log *zap.Logger) []string {
	ddls := make([]string, 0)
	// Logger sudah di-scope oleh pemanggil.

	hasTypeChange := false
	originalSourceTypeForLog := src.Type       // Untuk logging
	targetTypeDDLFromMapping := src.MappedType // Tipe dasar yang diinginkan di tujuan dari hasil mapping

	isSrcGenerated := src.IsGenerated
	isDstGenerated := dst.IsGenerated
	srcGenExpr := ""
	if src.GenerationExpression.Valid {
		srcGenExpr = src.GenerationExpression.String
	}
	dstGenExpr := ""
	if dst.GenerationExpression.Valid {
		dstGenExpr = dst.GenerationExpression.String
	}

	// Periksa perbedaan signifikan
	for _, diff := range diffs {
		if strings.HasPrefix(diff, "type") ||
			(isSrcGenerated != isDstGenerated) ||
			(isSrcGenerated && isDstGenerated && normalizeGenerationExpression(srcGenExpr, s.srcDialect) != normalizeGenerationExpression(dstGenExpr, s.dstDialect)) {
			hasTypeChange = true // Anggap perubahan status/ekspresi generated sebagai "perubahan tipe" signifikan
			break
		}
	}

	if hasTypeChange {
		log.Warn("Significant column modification detected for MySQL (type, generated status, or expression change). The generated MODIFY COLUMN statement might fail or cause data loss/truncation if the conversion is not directly supported or safe. Manual data migration steps might be safer for complex changes.",
			zap.String("table", table),
			zap.String("column", src.Name),
			zap.String("source_original_type_for_reference", originalSourceTypeForLog),
			zap.String("destination_current_type", dst.Type),
			zap.String("desired_target_type_base_from_mapping", targetTypeDDLFromMapping),
			zap.Bool("desired_is_generated", isSrcGenerated),
			zap.String("desired_gen_expr", srcGenExpr),
			zap.Bool("actual_dst_is_generated", isDstGenerated),
			zap.String("actual_dst_gen_expr", dstGenExpr))
	}

	// mapColumnDefinition akan menggunakan src (yang mewakili state yang diinginkan di tujuan)
	// untuk membuat definisi kolom lengkap untuk dialek MySQL.
	// Jika src.IsGenerated true, mapColumnDefinition akan mencoba membuat sintaks GENERATED AS.
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
	// columnTypeDefAndAttributes sekarang seharusnya sudah mencakup 'GENERATED ALWAYS AS ...' jika src.IsGenerated.
	ddl := fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s;",
		quotedTable,
		quotedColumnNameToModify,
		columnTypeDefAndAttributes)

	log.Info("Generated MySQL MODIFY COLUMN DDL.",
		zap.String("table", table),
		zap.String("column", src.Name),
		zap.String("ddl", ddl),
		zap.Strings("based_on_diffs", diffs))
	ddls = append(ddls, ddl)

	return ddls
}
