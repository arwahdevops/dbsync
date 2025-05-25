// internal/sync/ddl_alter_column_postgres.go
package sync

import (
	"fmt"
	"strings"

	"github.com/arwahdevops/dbsync/internal/utils"
	"go.uber.org/zap"
)

// generatePostgresModifyColumnDDLs menghasilkan DDLs untuk mengubah kolom di PostgreSQL.
func (s *SchemaSyncer) generatePostgresModifyColumnDDLs(table string, src ColumnInfo, dst ColumnInfo, diffs []string, log *zap.Logger) []string {
	ddls := make([]string, 0)
	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)
	quotedColumnName := utils.QuoteIdentifier(src.Name, s.dstDialect)

	hasAlteredTypeOrGeneratedStatus := false // Flag untuk melacak apakah tipe atau status generated diubah

	// Proses perbedaan status generated terlebih dahulu, karena ini bisa mengubah sifat kolom
	// dan mungkin membuat perubahan tipe dasar atau default menjadi tidak relevan atau memerlukan DDL berbeda.
	if src.IsGenerated != dst.IsGenerated {
		hasAlteredTypeOrGeneratedStatus = true
		if src.IsGenerated { // Ubah menjadi generated
			if src.GenerationExpression.Valid && src.GenerationExpression.String != "" {
				// PostgreSQL mendukung ADD GENERATED. STORED adalah default.
				// Untuk tipe dasar, kita akan mengandalkan src.MappedType yang sudah diset.
				// Jika tipe dasar juga berubah, ALTER TYPE terpisah mungkin diperlukan *sebelum* ADD GENERATED,
				// atau sebagai bagian dari definisi tipe saat ADD GENERATED (jika didukung).
				// Untuk kesederhanaan, kita asumsikan tipe dasar sudah sesuai atau akan di-handle terpisah.
				// DDL-nya akan menjadi: ALTER TABLE ... ALTER COLUMN ... TYPE existing_type_or_mapped_type; (jika tipe berubah)
				// LALU: ALTER TABLE ... ALTER COLUMN ... ADD GENERATED ALWAYS AS (expression) STORED;

				// Langkah 1: Pastikan tipe dasar sudah benar (jika berbeda dari dst.Type dan src.MappedType berbeda)
				// Ini akan ditangani oleh case "type" di bawah jika MappedType berbeda dari dst.Type.
				// Di sini, kita fokus pada penambahan klausa GENERATED.
				ddlGen := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s ADD GENERATED ALWAYS AS (%s) STORED;",
					quotedTable, quotedColumnName, src.GenerationExpression.String)
				log.Info("Generated ADD GENERATED DDL for PostgreSQL.", zap.String("ddl", ddlGen))
				ddls = append(ddls, ddlGen)
			} else {
				log.Warn("Source column is generated but has no expression. Cannot generate ADD GENERATED DDL.",
					zap.String("column", src.Name))
			}
		} else { // Ubah dari generated menjadi non-generated
			// PostgreSQL 12+
			ddlDropExpr := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP EXPRESSION IF EXISTS;",
				quotedTable, quotedColumnName)
			log.Info("Generated DROP EXPRESSION DDL for PostgreSQL.", zap.String("ddl", ddlDropExpr))
			ddls = append(ddls, ddlDropExpr)
			// Jika kolom juga merupakan identity, itu juga perlu di-drop
			if strings.Contains(strings.ToLower(dst.Type), "identity") || dst.AutoIncrement { // Cek apakah dst adalah identity
				ddlDropIdent := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP IDENTITY IF EXISTS;",
					quotedTable, quotedColumnName)
				log.Info("Generated DROP IDENTITY DDL for PostgreSQL.", zap.String("ddl", ddlDropIdent))
				ddls = append(ddls, ddlDropIdent)
			}
			// Setelah DROP EXPRESSION, kolom mungkin memerlukan SET DEFAULT atau SET NOT NULL baru jika src menentukannya.
			// Ini akan ditangani oleh case "default" dan "nullability" di bawah.
		}
	} else if src.IsGenerated && dst.IsGenerated { // Keduanya generated, cek apakah ekspresi berubah
		normSrcExpr := normalizeGenerationExpression(src.GenerationExpression.String, s.srcDialect)
		normDstExpr := normalizeGenerationExpression(dst.GenerationExpression.String, s.dstDialect)
		if normSrcExpr != normDstExpr {
			hasAlteredTypeOrGeneratedStatus = true
			log.Warn("Generation expression changed. This typically requires DROP EXPRESSION and ADD GENERATED in PostgreSQL.",
				zap.String("column", src.Name), zap.String("src_expr", normSrcExpr), zap.String("dst_expr", normDstExpr))
			// Buat DDL untuk drop dan add expression
			if src.GenerationExpression.Valid && src.GenerationExpression.String != "" {
				ddlDrop := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP EXPRESSION IF EXISTS;", quotedTable, quotedColumnName)
				ddlAdd := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s ADD GENERATED ALWAYS AS (%s) STORED;",
					quotedTable, quotedColumnName, src.GenerationExpression.String)
				ddls = append(ddls, ddlDrop, ddlAdd)
				log.Info("Generated DROP EXPRESSION + ADD GENERATED DDLs for PostgreSQL due to expression change.",
					zap.Strings("ddls", []string{ddlDrop, ddlAdd}))
			} else {
				log.Warn("Source column is generated but new expression is empty. Only generating DROP EXPRESSION.", zap.String("column", src.Name))
				ddlDrop := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP EXPRESSION IF EXISTS;", quotedTable, quotedColumnName)
				ddls = append(ddls, ddlDrop)
			}
		}
	}

	// Proses perbedaan lain (tipe, nullability, default, collation, auto_increment)
	for _, diff := range diffs {
		// Jika status generated sudah diubah, beberapa diff mungkin tidak lagi relevan
		// atau perlu ditangani secara berbeda.
		if hasAlteredTypeOrGeneratedStatus && (strings.HasPrefix(diff, "generated_status") || strings.HasPrefix(diff, "generation_expression")) {
			continue // Sudah ditangani di atas
		}

		log.Debug("Processing column difference for PostgreSQL ALTER", zap.String("difference", diff))
		var ddl string

		switch {
		case strings.HasPrefix(diff, "type"):
			// Hanya jalankan ALTER TYPE jika kolomnya *bukan* generated di tujuan (atau jika kita tidak mengubah status generatednya di DDL ini)
			// ATAU jika tipe dasar dari src.MappedType berbeda dari dst.Type bahkan jika keduanya generated.
			// src.MappedType adalah *tipe dasar* yang diinginkan.
			if !dst.IsGenerated || (src.IsGenerated && dst.IsGenerated && src.MappedType != extractBaseTypeFromGenerated(dst.Type, log)) {
				targetTypeDDL := src.MappedType // Ini adalah tipe dasar target + modifier yang sudah benar.

				// Ambil PostgresUsingExpr. originalSrcTypeForMappingKey sudah memperhitungkan jika src generated.
				var originalSrcTypeForMappingKey string
				if src.IsGenerated {
					originalSrcTypeForMappingKey = extractBaseTypeFromGenerated(src.Type, log)
				} else {
					originalSrcTypeForMappingKey = src.Type
				}
				mappedResult, mapErr := s.mapDataType(originalSrcTypeForMappingKey, src.Type)
				if mapErr != nil {
					log.Error("Failed to re-retrieve mapped type result for Postgres USING clause, using default.",
						zap.String("column", src.Name), zap.Error(mapErr))
					mappedResult = &MappedTypeResult{} // Fallback
				}

				usingClause := ""
				if mappedResult.PostgresUsingExpr != "" {
					usingClause = "USING " + strings.ReplaceAll(mappedResult.PostgresUsingExpr, "{{column_name}}", quotedColumnName)
					log.Info("Using custom PostgreSQL USING expression from type mapping configuration.",
						zap.String("column", src.Name),
						zap.String("raw_using_expr_from_config", mappedResult.PostgresUsingExpr),
						zap.String("rendered_using_clause", usingClause))
				} else {
					// Dapatkan tipe dasar dari targetTypeDDL untuk klausa USING default
					baseTargetTypeForCast := strings.Split(targetTypeDDL, "(")[0]
					baseTargetTypeForCast = normalizeTypeName(baseTargetTypeForCast) // Normalisasi lagi untuk CAST
					usingClause = fmt.Sprintf("USING %s::%s", quotedColumnName, baseTargetTypeForCast)
					log.Debug("Using default PostgreSQL USING clause.",
						zap.String("column", src.Name),
						zap.String("target_type_for_cast_in_using", baseTargetTypeForCast))
				}

				ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s %s;",
					quotedTable, quotedColumnName, targetTypeDDL, usingClause)

				log.Warn("Generated ALTER COLUMN TYPE for PostgreSQL. Review DDL and USING clause carefully! Manual adjustment of the USING clause might be required for complex or lossy type conversions if no custom expression was provided.",
					zap.String("ddl_generated", ddl),
					zap.String("source_original_type", src.Type),
					zap.String("destination_original_type", dst.Type),
					zap.String("target_type_ddl", targetTypeDDL))
				hasAlteredTypeOrGeneratedStatus = true
			} else if dst.IsGenerated {
				log.Debug("Skipping ALTER COLUMN TYPE because destination column is generated and its generated status is not being changed by this sync pass, or base types match.",
					zap.String("column", src.Name), zap.String("src_mapped_type", src.MappedType), zap.String("dst_base_type", extractBaseTypeFromGenerated(dst.Type, log)))
			}

		case strings.HasPrefix(diff, "nullability"):
			// Perubahan nullability bisa diterapkan meskipun kolomnya generated.
			if src.IsNullable {
				ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;", quotedTable, quotedColumnName)
			} else {
				ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL;", quotedTable, quotedColumnName)
			}

		case strings.HasPrefix(diff, "default"):
			// Default value tidak relevan jika kolom tujuan *akan menjadi* generated oleh DDL ini.
			// Jika kolom sumber *bukan* generated, maka kita set defaultnya.
			if !src.IsGenerated {
				if src.DefaultValue.Valid && src.DefaultValue.String != "" {
					// Format default value untuk DIALEK TUJUAN (PostgreSQL)
					// src.MappedType digunakan sebagai konteks tipe untuk formatting.
					formattedDefault := s.formatDefaultValue(src.DefaultValue.String, src.MappedType)
					// Pastikan kita tidak mencoba SET DEFAULT untuk fungsi DB yang sama (misal, nextval() yang mungkin sudah implisit)
					// Atau jika formattedDefault adalah keyword fungsi yang tidak perlu 'SET DEFAULT'
					if !isKnownDbFunction(normalizeDefaultValue(formattedDefault, s.dstDialect)) ||
						(s.dstDialect == "postgres" && strings.HasPrefix(strings.ToLower(formattedDefault), "nextval(")) { // nextval ditangani oleh identity/serial
						ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;", quotedTable, quotedColumnName, formattedDefault)
					} else {
						// Jika kolom sebelumnya punya default dan sekarang sumber tidak (atau defaultnya NULL/fungsi implisit), kita DROP DEFAULT.
						if dst.DefaultValue.Valid && dst.DefaultValue.String != "" {
							log.Debug("Source default is NULL or implicit function, while destination had a default. Generating DROP DEFAULT.",
								zap.String("column", src.Name), zap.String("dst_default", dst.DefaultValue.String))
							ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;", quotedTable, quotedColumnName)
						} else {
							log.Debug("Skipping SET DEFAULT as it's a known DB function or source default is NULL/empty, and destination had no default.",
								zap.String("column", src.Name), zap.String("formatted_default", formattedDefault))
						}
					}
				} else { // src.DefaultValue tidak valid atau stringnya kosong
					// Jika kolom tujuan sebelumnya memiliki default, kita perlu DROP DEFAULT.
					if dst.DefaultValue.Valid && dst.DefaultValue.String != "" {
						ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;", quotedTable, quotedColumnName)
					}
				}
			} else {
				log.Debug("Skipping default value modification as source column is (or will become) generated.", zap.String("column", src.Name))
				// Jika kolom tujuan *sebelumnya* punya default dan sekarang *akan menjadi* generated, kita mungkin perlu DROP DEFAULT.
				if dst.DefaultValue.Valid && dst.DefaultValue.String != "" && dst.IsGenerated == false && src.IsGenerated == true {
					ddlDropOldDefault := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;", quotedTable, quotedColumnName)
					log.Info("Destination column had a default and will become generated. Adding DROP DEFAULT.", zap.String("ddl_drop_default", ddlDropOldDefault))
					// Tambahkan ini sebagai DDL terpisah karena SET GENERATED mungkin perlu terjadi dulu.
					// Atau, idealnya, urutan operasi harus benar.
					// Untuk sekarang, tambahkan saja. Eksekutor DDL mungkin perlu lebih pintar.
					// Atau, kita bisa berasumsi ADD GENERATED akan menimpa/menghapus default.
					// Kita tambahkan saja, lebih aman.
					if ddlDropOldDefault != "" {
						ddls = append(ddls, ddlDropOldDefault)
					}

				}
			}

		case strings.HasPrefix(diff, "collation"):
			// Collation hanya bisa diubah bersamaan dengan tipe di PostgreSQL.
			// Hanya lakukan jika kolomnya tidak generated.
			if !src.IsGenerated && !dst.IsGenerated { // Atau jika generated status tidak berubah
				if src.Collation.Valid && src.Collation.String != "" {
					targetTypeForCollationChange := src.MappedType // Tipe dasar target
					collationName := src.Collation.String
					// Quote nama collation jika mengandung karakter khusus atau spasi dan belum di-quote
					// (Meskipun utils.QuoteIdentifier mungkin over-quoting untuk nama collation PG standar)
					// Lebih baik, biarkan collationName apa adanya jika dari sumber sudah benar.
					// if !(strings.HasPrefix(collationName, `"`) && strings.HasSuffix(collationName, `"`)) {
					// 	if strings.ContainsAny(collationName, " -.()/") { // Karakter yang mungkin butuh quote
					// 		collationName = utils.QuoteIdentifier(collationName, s.dstDialect)
					// 	}
					// }
					ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s COLLATE %s;", // Perlu USING juga di sini
						quotedTable, quotedColumnName, targetTypeForCollationChange, collationName /* TODO: Add USING clause */)
					log.Error("Generated ALTER COLUMN COLLATE for PostgreSQL. THIS IS A SIMPLIFICATION AND LIKELY INCORRECT as it's missing a USING clause that matches the TYPE change part. Collation changes in PG are complex with ALTER TYPE. Manual review is highly recommended.", zap.String("ddl_generated_incomplete", ddl))
					// SEHARUSNYA: Jika collation berubah, dan tipe juga berubah, gabungkan.
					// Jika hanya collation berubah, sintaks `ALTER COLUMN ... SET DATA TYPE ... COLLATE ... USING ...` tetap diperlukan.
					// Ini adalah area yang sangat kompleks untuk otomatisasi penuh.
					// Untuk sekarang, DDL ini mungkin tidak lengkap atau salah.
					hasAlteredTypeOrGeneratedStatus = true // Karena TYPE diubah
				} else { // src.Collation tidak valid atau kosong
					// Tidak ada cara mudah untuk "DROP COLLATE" ke default di PG tanpa ALTER TYPE.
					log.Warn("Cannot directly 'DROP' collation to default in PostgreSQL without ALTER TYPE. If the destination column needs the database's default collation, a manual ALTER TYPE to the same type (which would pick up the default collation) might be needed, or this difference might be acceptable.",
						zap.String("column", src.Name),
						zap.String("destination_collation_to_remove", dst.Collation.String))
				}
			} else {
				log.Debug("Skipping collation modification as column is generated.", zap.String("column", src.Name))
			}

		case strings.HasPrefix(diff, "auto_increment"):
			// Perubahan status identity (auto_increment)
			// Ini lebih kompleks daripada hanya SET/DROP DEFAULT.
			log.Warn("Difference in auto_increment/identity status detected for PostgreSQL. Modifying identity properties on an existing column is complex and not fully automatically handled by dbsync. Manual intervention (e.g., managing sequences, ADD/DROP IDENTITY) is likely required.",
				zap.String("column", src.Name),
				zap.Bool("src_auto_inc", src.AutoIncrement),
				zap.Bool("dst_auto_inc", dst.AutoIncrement))
			// Contoh DDL (membutuhkan PG10+):
			// To add identity: ALTER TABLE tbl ALTER COLUMN col ADD GENERATED { BY DEFAULT | ALWAYS } AS IDENTITY (sequence_options);
			// To drop identity: ALTER TABLE tbl ALTER COLUMN col DROP IDENTITY IF EXISTS;
			// Ini tidak akan dibuat secara otomatis saat ini karena kompleksitas sequence_options dan ALWAYS vs BY DEFAULT.

		default:
			log.Warn("Unhandled difference type for PostgreSQL ALTER COLUMN DDL generation.", zap.String("difference_prefix", strings.Split(diff, " ")[0]))
		}

		if ddl != "" {
			log.Info("Generated PostgreSQL ALTER DDL part", zap.String("ddl_part", ddl))
			ddls = append(ddls, ddl)
		}
	}

	if hasAlteredTypeOrGeneratedStatus {
		log.Warn("Column type or generated status was altered for PostgreSQL. Review DDLs carefully. PostgreSQL's ALTER TYPE or changes to generated status might reset column defaults or other properties, potentially requiring manual re-application even if they weren't initially detected as different against the *original* destination column.",
			zap.String("column", src.Name))
	}

	return ddls
}
