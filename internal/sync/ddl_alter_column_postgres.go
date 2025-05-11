// internal/sync/ddl_alter_column_postgres.go
package sync

import (
	"fmt"
	"strings"

	"go.uber.org/zap"

	// "github.com/arwahdevops/dbsync/internal/config" // DIHAPUS - Tidak digunakan secara langsung di file ini
	"github.com/arwahdevops/dbsync/internal/utils"
)

// generatePostgresModifyColumnDDLs menghasilkan DDLs untuk mengubah kolom di PostgreSQL.
func (s *SchemaSyncer) generatePostgresModifyColumnDDLs(table string, src ColumnInfo, dst ColumnInfo, diffs []string, log *zap.Logger) []string {
	ddls := make([]string, 0)
	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)
	quotedColumnName := utils.QuoteIdentifier(src.Name, s.dstDialect)

	hasAlteredType := false

	for _, diff := range diffs {
		log.Debug("Processing column difference for PostgreSQL ALTER", zap.String("difference", diff))
		var ddl string

		switch {
		case strings.HasPrefix(diff, "type"):
			targetTypeDDL := src.MappedType // Ini sudah termasuk modifier yang benar.

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
				baseTargetTypeForCast := strings.Split(targetTypeDDL, "(")[0] 
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
			hasAlteredType = true

		case strings.HasPrefix(diff, "nullability"):
			if src.IsNullable {
				ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;", quotedTable, quotedColumnName)
			} else {
				ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL;", quotedTable, quotedColumnName)
			}

		case strings.HasPrefix(diff, "default"):
			if src.DefaultValue.Valid && src.DefaultValue.String != "" {
				normalizedSrcDefault := normalizeDefaultValue(src.DefaultValue.String, s.srcDialect)
				if !isDefaultNullOrFunction(normalizedSrcDefault) {
					formattedDefault := s.formatDefaultValue(src.DefaultValue.String, src.MappedType)
					ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;", quotedTable, quotedColumnName, formattedDefault)
				} else {
					ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;", quotedTable, quotedColumnName)
				}
			} else {
				ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;", quotedTable, quotedColumnName)
			}

		case strings.HasPrefix(diff, "collation"):
			if src.Collation.Valid && src.Collation.String != "" {
				targetTypeForCollationChange := src.MappedType
				collationName := src.Collation.String
				if !(strings.HasPrefix(collationName, `"`) && strings.HasSuffix(collationName, `"`)) {
					if strings.ContainsAny(collationName, " -.()/") {
						collationName = utils.QuoteIdentifier(collationName, s.dstDialect)
					}
				}
				ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s COLLATE %s;",
					quotedTable, quotedColumnName, targetTypeForCollationChange, collationName)
				log.Warn("Generated ALTER COLUMN COLLATE for PostgreSQL. This statement also re-specifies the TYPE. Ensure the type and collation are compatible and review potential side effects.", zap.String("ddl_generated", ddl))
				hasAlteredType = true
			} else {
				log.Warn("Cannot directly 'DROP' collation to default in PostgreSQL without ALTER TYPE. If the destination column needs the default collation, a manual ALTER TYPE to the same type using the database's default collation might be needed, or this difference might be acceptable.",
					zap.String("column", src.Name),
					zap.String("destination_collation_to_remove", dst.Collation.String))
			}
		case strings.HasPrefix(diff, "auto_increment"):
			log.Warn("Difference in auto_increment/identity status detected. Modifying identity properties on an existing PostgreSQL column via standard ALTER is complex and not automatically handled by dbsync. Manual intervention (e.g., managing sequences) is likely required.",
				zap.Bool("src_auto_inc", src.AutoIncrement),
				zap.Bool("dst_auto_inc", dst.AutoIncrement))

		default:
			log.Warn("Unhandled difference type for PostgreSQL ALTER COLUMN DDL generation.", zap.String("difference_prefix", strings.Split(diff, " ")[0]))
		}

		if ddl != "" {
			log.Info("Generated PostgreSQL ALTER DDL part", zap.String("ddl_part", ddl))
			ddls = append(ddls, ddl)
		}
	}

	if hasAlteredType {
		log.Warn("Column type was altered for PostgreSQL. While DDLs for other detected differences (like default or nullability) were generated based on the initial comparison, review the final table structure. PostgreSQL's ALTER TYPE might reset column defaults or other properties, potentially requiring manual re-application even if they weren't initially detected as different against the *original* destination column.",
			zap.String("column", src.Name))
	}

	return ddls
}
