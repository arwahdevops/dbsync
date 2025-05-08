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
func (s *SchemaSyncer) generatePostgresModifyColumnDDLs(table string, src ColumnInfo, dst ColumnInfo, diffs []string, log *zap.Logger) []string {
	ddls := make([]string, 0)
	log = log.With(zap.String("table", table), zap.String("column", src.Name), zap.String("dialect", "postgres"))

	if len(diffs) == 0 {
		log.Debug("No differences found, no ALTER DDL needed for column.")
		return ddls
	}

	if src.IsGenerated {
		log.Warn("Skipping ALTER COLUMN DDL generation for generated PostgreSQL column. Changes usually require manual DROP/ADD.",
			zap.Strings("differences", diffs))
		return ddls
	}

	if src.MappedType == "" {
		log.Error("Cannot generate PostgreSQL ALTER COLUMN DDL: MappedType for source column is missing and column is not generated.")
		return ddls
	}

	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)
	quotedColumnName := utils.QuoteIdentifier(src.Name, s.dstDialect)

	hasAlteredType := false

	for _, diff := range diffs {
		log.Debug("Processing column difference for PostgreSQL ALTER", zap.String("difference", diff))
		var ddl string

		switch {
		case strings.HasPrefix(diff, "type"):
			ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s /* USING %s::%s */;",
				quotedTable, quotedColumnName, src.MappedType, quotedColumnName, src.MappedType)
			log.Warn("Generated ALTER COLUMN TYPE for PostgreSQL. A manual 'USING clause_name::new_type' might be needed if the conversion is not implicit.",
				zap.String("ddl_generated", ddl))
			hasAlteredType = true

		case strings.HasPrefix(diff, "nullability"):
			if src.IsNullable {
				ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;", quotedTable, quotedColumnName)
			} else {
				ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL;", quotedTable, quotedColumnName)
			}

		case strings.HasPrefix(diff, "default"):
			// Panggil helper statis isDefaultNullOrFunction dari compare_utils.go
			if src.DefaultValue.Valid && src.DefaultValue.String != "" && !isDefaultNullOrFunction(src.DefaultValue.String) {
				typeForFormatting := src.MappedType
				formattedDefault := s.formatDefaultValue(src.DefaultValue.String, typeForFormatting) // formatDefaultValue adalah method
				ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;", quotedTable, quotedColumnName, formattedDefault)
			} else { // Sumber tidak punya default (atau NULL/fungsi), maka hapus default di tujuan.
				ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;", quotedTable, quotedColumnName)
			}
		case strings.HasPrefix(diff, "collation"):
			if src.Collation.Valid && src.Collation.String != "" {
				targetType := src.MappedType
				quotedCollation := utils.QuoteIdentifier(src.Collation.String, s.dstDialect)
				ddl = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s COLLATE %s;",
					quotedTable, quotedColumnName, targetType, quotedCollation)
				log.Warn("Generated ALTER COLUMN COLLATE for PostgreSQL. This also re-specifies the TYPE. Ensure type and collation are compatible.", zap.String("ddl_generated", ddl))
			} else {
				log.Warn("Cannot 'DROP' collation to default directly in PostgreSQL without ALTER TYPE. Collation change to default might be ignored or require manual ALTER TYPE.", zap.String("column", src.Name))
			}
		// case strings.HasPrefix(diff, "comment"): // DDL Comment adalah statement terpisah
		// 	commentVal := "NULL"; if src.Comment.Valid { commentVal = "'" + strings.ReplaceAll(src.Comment.String, "'", "''") + "'" }
		// 	ddl = fmt.Sprintf("COMMENT ON COLUMN %s.%s IS %s;", quotedTable, quotedColumnName, commentVal)
		default:
			log.Warn("Unhandled difference type for PostgreSQL ALTER COLUMN DDL generation", zap.String("difference_type", diff))
		}

		if ddl != "" {
			log.Info("Generated PostgreSQL ALTER DDL part", zap.String("ddl_part", ddl))
			ddls = append(ddls, ddl)
		}
	}

	// Penanganan potensial untuk state yang direset oleh ALTER TYPE
	if hasAlteredType {
		log.Warn("Column type was altered for PostgreSQL. Review if nullability and default value need to be re-applied explicitly if they were not part of the initial 'diffs'.", zap.String("column", src.Name))
	}

	return ddls
}
