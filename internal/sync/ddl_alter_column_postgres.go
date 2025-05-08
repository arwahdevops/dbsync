// internal/sync/ddl_alter_column_postgres.go
package sync

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
	"github.com/arwahdevops/dbsync/internal/utils" // Untuk QuoteIdentifier
)

// generatePostgresModifyColumnDDLs menghasilkan DDLs untuk mengubah kolom di PostgreSQL.
// PostgreSQL memungkinkan pengubahan atribut kolom secara terpisah.
// Fungsi ini menerima `diffs` dari `getColumnModifications`.
func (s *SchemaSyncer) generatePostgresModifyColumnDDLs(table string, src ColumnInfo, dst ColumnInfo, diffs []string, log *zap.Logger) []string {
	ddls := make([]string, 0)
	// Logger sudah di-scope oleh pemanggil (generateAlterDDLs -> generateModifyColumnDDLs)
	// dengan konteks tabel dan kolom.

	// Tidak perlu cek len(diffs) == 0 di sini, karena pemanggil sudah melakukannya.
	// Tidak perlu cek IsGenerated atau MappedType == "" di sini, pemanggil sudah melakukannya.

	quotedTable := utils.QuoteIdentifier(table, s.dstDialect) // s.dstDialect akan "postgres"
	quotedColumnName := utils.QuoteIdentifier(src.Name, s.dstDialect)

	hasAlteredType := false // Lacak apakah tipe diubah, karena ini bisa mereset default/nullability

	// Iterasi melalui perbedaan yang terdeteksi oleh getColumnModifications
	for _, diff := range diffs {
		log.Debug("Processing column difference for PostgreSQL ALTER", zap.String("difference", diff))
		var ddl string

		switch {
		case strings.HasPrefix(diff, "type"):
			// src.MappedType adalah tipe tujuan yang diinginkan
			targetType := src.MappedType
			// Coba buat klausa USING dasar. Ini mungkin perlu disesuaikan manual untuk konversi kompleks.
			// Klausa USING dasar mengasumsikan cast langsung dari nama kolom ke tipe baru.
			usingClause := fmt.Sprintf("USING %s::%s", quotedColumnName, targetType) // Perhatikan: targetType mungkin perlu di-quote atau ditangani jika itu array, dll. Ini asumsi sederhana.

			ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s %s;",
				quotedTable, quotedColumnName, targetType, usingClause)

			log.Warn("Generated ALTER COLUMN TYPE for PostgreSQL. A basic 'USING column::new_type' clause was added automatically. Review this DDL carefully! Manual adjustment of the USING clause might be required for complex or lossy type conversions.",
				zap.String("ddl_generated", ddl),
				zap.String("source_original_type", src.Type),
				zap.String("destination_original_type", dst.Type),
				zap.String("target_type", targetType))
			hasAlteredType = true

		case strings.HasPrefix(diff, "nullability"):
			if src.IsNullable { // Sumber mengizinkan NULL, tujuan tidak -> hapus NOT NULL
				ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;", quotedTable, quotedColumnName)
			} else { // Sumber TIDAK mengizinkan NULL, tujuan mengizinkan -> tambahkan NOT NULL
				ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL;", quotedTable, quotedColumnName)
			}

		case strings.HasPrefix(diff, "default"):
			// Sumber memiliki default literal (bukan NULL/fungsi)
			if src.DefaultValue.Valid && src.DefaultValue.String != "" {
				// Gunakan helper dari compare_utils.go untuk memeriksa apakah default sumber adalah NULL atau fungsi
				normalizedSrcDefault := normalizeDefaultValue(src.DefaultValue.String, s.srcDialect)
				if !isDefaultNullOrFunction(normalizedSrcDefault) {
					// Sumber memiliki default literal, set di tujuan.
					// formatDefaultValue dari schema_ddl_create.go (atau util) akan menangani quoting.
					// Gunakan MappedType sumber sebagai petunjuk tipe untuk formatting.
					formattedDefault := s.formatDefaultValue(src.DefaultValue.String, src.MappedType)
					ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;", quotedTable, quotedColumnName, formattedDefault)
				} else {
					// Sumber memiliki default NULL atau fungsi, sementara tujuan memiliki default yang berbeda (atau tidak ada).
					// Hapus default di tujuan.
					ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;", quotedTable, quotedColumnName)
				}
			} else {
				// Sumber TIDAK memiliki default (atau defaultnya NULL secara eksplisit menurut DB sumber),
				// sementara tujuan memiliki default yang berbeda. Hapus default di tujuan.
				ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;", quotedTable, quotedColumnName)
			}

		case strings.HasPrefix(diff, "collation"):
			// Sumber memiliki collation spesifik
			if src.Collation.Valid && src.Collation.String != "" {
				// Mengubah collation di PostgreSQL seringkali memerlukan ALTER TYPE juga.
				targetType := src.MappedType // Gunakan tipe tujuan yang diinginkan
				quotedCollation := utils.QuoteIdentifier(src.Collation.String, s.dstDialect) // Collation name mungkin perlu di-quote
				ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s COLLATE %s;",
					quotedTable, quotedColumnName, targetType, quotedCollation)
				log.Warn("Generated ALTER COLUMN COLLATE for PostgreSQL. This statement also re-specifies the TYPE. Ensure the type and collation are compatible and review potential side effects.", zap.String("ddl_generated", ddl))
				// Karena ini juga mengubah TYPE, kita set hasAlteredType
				hasAlteredType = true
			} else {
				// Sumber tidak memiliki collation eksplisit (menggunakan default), sementara tujuan punya.
				// Menghapus collation kembali ke default DB biasanya tidak dilakukan dengan 'DROP COLLATE'.
				// Seringkali ini terjadi secara otomatis jika tipe data diubah ke tipe yang tidak mendukung collation,
				// atau perlu ALTER TYPE ke tipe yang sama dengan collation default.
				log.Warn("Cannot directly 'DROP' collation to default in PostgreSQL without ALTER TYPE. If the destination column needs the default collation, a manual ALTER TYPE to the same type using the database's default collation might be needed, or this difference might be acceptable.",
					zap.String("column", src.Name),
					zap.String("destination_collation", dst.Collation.String))
				// Tidak menghasilkan DDL otomatis untuk menghapus collation.
			}

		// Aktifkan jika ingin ALTER COMMENT (perlu helper DDL terpisah)
		// case strings.HasPrefix(diff, "comment"):
		//  commentVal := "NULL"; // Default ke NULL jika string kosong atau tidak valid
		//  if src.Comment.Valid && src.Comment.String != "" {
		//      escapedComment := strings.ReplaceAll(src.Comment.String, "'", "''") // Escape single quotes for SQL string literal
		//      commentVal = "'" + escapedComment + "'"
		//  }
		//  // Statement COMMENT ON terpisah
		//  ddl = fmt.Sprintf("COMMENT ON COLUMN %s.%s IS %s;", quotedTable, quotedColumnName, commentVal)


		case strings.HasPrefix(diff, "auto_increment"):
			// Mengubah status auto_increment/identity pada kolom yang ada sangat kompleks
			// dan seringkali tidak didukung secara langsung atau aman melalui ALTER di PostgreSQL.
			// Misalnya, menambahkan IDENTITY ke kolom yang ada, atau menghapusnya.
			// Ini biasanya melibatkan pembuatan sequence baru, mengaitkannya, atau menghapus sequence.
			log.Warn("Difference in auto_increment/identity status detected. Modifying identity properties on an existing PostgreSQL column via standard ALTER is complex and not automatically handled by dbsync. Manual intervention (e.g., managing sequences) is likely required.",
				zap.Bool("src_auto_inc", src.AutoIncrement),
				zap.Bool("dst_auto_inc", dst.AutoIncrement))
			// Tidak menghasilkan DDL otomatis.

		default:
			log.Warn("Unhandled difference type for PostgreSQL ALTER COLUMN DDL generation. This might indicate an uncompared attribute or an issue in getColumnModifications.", zap.String("difference_prefix", strings.Split(diff, " ")[0]))
		}

		if ddl != "" {
			log.Info("Generated PostgreSQL ALTER DDL part", zap.String("ddl_part", ddl))
			ddls = append(ddls, ddl)
		}
	}

	// Peringatan tambahan jika tipe diubah
	if hasAlteredType {
		// Di PostgreSQL, ALTER TYPE dapat mereset beberapa properti kolom lain seperti DEFAULT.
		// Meskipun kita menghasilkan DDL terpisah untuk DEFAULT jika terdeteksi berbeda,
		// mungkin ada kasus di mana DEFAULT *sebelumnya* cocok, tetapi setelah ALTER TYPE,
		// default tersebut hilang dan perlu di-set ulang (meskipun tidak muncul di `diffs` awal).
		// Ini agak rumit untuk dideteksi secara otomatis. Peringatan ini penting.
		log.Warn("Column type was altered for PostgreSQL. While DDLs for other detected differences (like default or nullability) were generated based on the initial comparison, review the final table structure. PostgreSQL's ALTER TYPE might reset column defaults or other properties, potentially requiring manual re-application even if they weren't initially detected as different against the *original* destination column.",
			zap.String("column", src.Name))
	}

	return ddls
}
