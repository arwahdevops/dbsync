// internal/sync/schema_fetch_postgres.go
package sync

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/lib/pq" // Diperlukan untuk pq.StringArray
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func (s *SchemaSyncer) getPostgresColumns(ctx context.Context, db *gorm.DB, table string) ([]ColumnInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "postgres"), zap.String("action", "getPostgresColumns"))

	var tableExists int64
	errTableCheck := db.WithContext(ctx).Raw(
		"SELECT COUNT(*) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = ? AND n.nspname = current_schema() AND c.relkind IN ('r', 'p', 'v', 'm', 'f')",
		table,
	).Scan(&tableExists).Error
	if errTableCheck != nil {
		return nil, fmt.Errorf("failed to check existence of PostgreSQL table '%s': %w", table, errTableCheck)
	}
	if tableExists == 0 {
		log.Warn("Table not found in current schema.")
		return []ColumnInfo{}, nil
	}

	query := `
	SELECT
		c.column_name, c.ordinal_position, c.column_default, c.is_nullable,
		c.data_type, c.udt_name, c.character_maximum_length, c.numeric_precision,
		c.datetime_precision, c.numeric_scale, c.collation_name,
		pgd.description AS column_comment,
		COALESCE(att.attidentity, '') AS identity_options,
		COALESCE(att.attgenerated, '') AS generated_options,
		pg_get_expr(attrdef.adbin, attrdef.adrelid) as generation_expression_from_attrdef,
		EXISTS (
			SELECT 1 FROM pg_catalog.pg_constraint con
			JOIN pg_catalog.pg_attribute attr_pk ON attr_pk.attnum = ANY(con.conkey) AND attr_pk.attrelid = con.conrelid
			WHERE con.conrelid = cla.oid AND con.contype = 'p' AND attr_pk.attname = c.column_name
		) AS is_primary_key
	FROM information_schema.columns c
	JOIN pg_catalog.pg_class cla ON cla.relname = c.table_name AND cla.relnamespace = (SELECT ns.oid FROM pg_catalog.pg_namespace ns WHERE ns.nspname = c.table_schema)
	JOIN pg_catalog.pg_attribute att ON att.attrelid = cla.oid AND att.attname = c.column_name
	LEFT JOIN pg_catalog.pg_attrdef attrdef ON attrdef.adrelid = cla.oid AND attrdef.adnum = att.attnum AND att.attgenerated != ''
	LEFT JOIN pg_catalog.pg_description pgd ON pgd.objoid = cla.oid AND pgd.objsubid = c.ordinal_position
	WHERE c.table_schema = current_schema() AND c.table_name = $1
	ORDER BY c.ordinal_position;
	`
	var columnsData []struct {
		ColumnName                      string         `gorm:"column:column_name"`
		OrdinalPosition                 int            `gorm:"column:ordinal_position"`
		ColumnDefault                   sql.NullString `gorm:"column:column_default"`
		IsNullable                      string         `gorm:"column:is_nullable"`
		DataType                        string         `gorm:"column:data_type"`
		UdtName                         string         `gorm:"column:udt_name"`
		CharacterMaximumLength          sql.NullInt64  `gorm:"column:character_maximum_length"`
		NumericPrecision                sql.NullInt64  `gorm:"column:numeric_precision"`
		DateTimePrecision               sql.NullInt64  `gorm:"column:datetime_precision"`
		NumericScale                    sql.NullInt64  `gorm:"column:numeric_scale"`
		CollationName                   sql.NullString `gorm:"column:collation_name"`
		ColumnComment                   sql.NullString `gorm:"column:column_comment"`
		IdentityOptions                 string         `gorm:"column:identity_options"`
		GeneratedOptions                string         `gorm:"column:generated_options"`
		GenerationExpressionFromAttrDef sql.NullString `gorm:"column:generation_expression_from_attrdef"`
		IsPrimaryKey                    bool           `gorm:"column:is_primary_key"`
	}

	err := db.WithContext(ctx).Raw(query, table).Scan(&columnsData).Error
	if err != nil {
		if strings.Contains(err.Error(), "column att.attidentity does not exist") ||
			strings.Contains(err.Error(), "column att.attgenerated does not exist") ||
			strings.Contains(err.Error(), "pg_get_expr") {
			log.Warn("Query failed (likely PostgreSQL < 12 or < 10), retrying with legacy column query.", zap.Error(err))
			queryLegacy := `
			SELECT
				c.column_name, c.ordinal_position, c.column_default, c.is_nullable, c.data_type, c.udt_name,
				c.character_maximum_length, c.numeric_precision, c.datetime_precision, c.numeric_scale, c.collation_name,
				pgd.description AS column_comment,
				'' AS identity_options, '' AS generated_options, NULL AS generation_expression_from_attrdef,
				EXISTS (
					SELECT 1 FROM pg_catalog.pg_constraint con
					JOIN pg_catalog.pg_attribute attr_pk ON attr_pk.attnum = ANY(con.conkey) AND attr_pk.attrelid = con.conrelid
					WHERE con.conrelid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = c.table_name AND relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = c.table_schema))
					  AND con.contype = 'p' AND attr_pk.attname = c.column_name
				) AS is_primary_key
			FROM information_schema.columns c
			LEFT JOIN pg_catalog.pg_class tbl ON tbl.relname = c.table_name AND tbl.relnamespace = (SELECT ns.oid FROM pg_catalog.pg_namespace ns WHERE ns.nspname = c.table_schema)
			LEFT JOIN pg_catalog.pg_description pgd ON pgd.objoid = tbl.oid AND pgd.objsubid = c.ordinal_position
			WHERE c.table_schema = current_schema() AND c.table_name = $1 ORDER BY c.ordinal_position;
			`
			err = db.WithContext(ctx).Raw(queryLegacy, table).Scan(&columnsData).Error
		}
		if err != nil {
			if err == gorm.ErrRecordNotFound || isTableNotExistError(err, "postgres") {
				log.Warn("Table not found or column query returned no rows (after potential retry).")
				return []ColumnInfo{}, nil
			}
			return nil, fmt.Errorf("postgres columns query failed for table '%s': %w", table, err)
		}
	}
	if len(columnsData) == 0 {
		log.Warn("Column query for PostgreSQL succeeded but returned no rows.")
		return []ColumnInfo{}, nil
	}

	result := make([]ColumnInfo, 0, len(columnsData))
	for _, c := range columnsData {
		srcType := c.DataType
		if c.DataType == "ARRAY" {
			if strings.HasPrefix(c.UdtName, "_") {
				srcType = normalizeTypeName(strings.TrimPrefix(c.UdtName, "_")) + "[]"
			} else {
				srcType = c.UdtName + "[]"
			}
		} else if c.DataType == "USER-DEFINED" {
			srcType = c.UdtName
		}

		isAutoIncrement := c.IdentityOptions != "" || (c.ColumnDefault.Valid && strings.HasPrefix(c.ColumnDefault.String, "nextval("))
		isGenerated := c.GeneratedOptions == "s"
		var generationExpr sql.NullString
		if isGenerated {
			generationExpr = c.GenerationExpressionFromAttrDef
		}

		precision := c.NumericPrecision
		if strings.Contains(c.DataType, "time") || strings.Contains(c.DataType, "interval") ||
			strings.Contains(c.UdtName, "time") || strings.Contains(c.UdtName, "interval") {
			precision = c.DateTimePrecision
		}

		result = append(result, ColumnInfo{
			Name: c.ColumnName, Type: srcType, IsNullable: c.IsNullable == "YES",
			IsPrimary: c.IsPrimaryKey, IsGenerated: isGenerated, DefaultValue: c.ColumnDefault,
			AutoIncrement: isAutoIncrement, OrdinalPosition: c.OrdinalPosition, Length: c.CharacterMaximumLength,
			Precision: precision, Scale: c.NumericScale, Collation: c.CollationName, Comment: c.ColumnComment,
			GenerationExpression: generationExpr,
		})
	}
	log.Debug("Fetched PostgreSQL column info successfully.", zap.Int("column_count", len(result)))
	return result, nil
}

func (s *SchemaSyncer) getPostgresIndexes(ctx context.Context, db *gorm.DB, table string) ([]IndexInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "postgres"), zap.String("action", "getPostgresIndexes"))

	var tableExists int64
	db.WithContext(ctx).Raw("SELECT count(*) from pg_class c join pg_namespace n on n.oid=c.relnamespace where c.relname=? and n.nspname=current_schema() and c.relkind IN ('r','m','p')", table).Scan(&tableExists)
	if tableExists == 0 {
		log.Warn("Table not found, cannot fetch indexes.")
		return []IndexInfo{}, nil
	}

	query := `
	SELECT i.relname AS index_name, idx.indisunique AS is_unique, idx.indisprimary AS is_primary,
		   pg_get_indexdef(idx.indexrelid) AS raw_def, am.amname AS index_method
	FROM pg_catalog.pg_class t
	JOIN pg_catalog.pg_index idx ON t.oid = idx.indrelid
	JOIN pg_catalog.pg_class i ON i.oid = idx.indexrelid
	JOIN pg_catalog.pg_am am ON i.relam = am.oid
	WHERE t.relkind IN ('r', 'm', 'p') AND t.relname = $1
	  AND t.relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = current_schema())
	ORDER BY index_name;
	`
	var results []struct {
		IndexName   string `gorm:"column:index_name"`
		IsUnique    bool   `gorm:"column:is_unique"`
		IsPrimary   bool   `gorm:"column:is_primary"`
		RawDef      string `gorm:"column:raw_def"`
		IndexMethod string `gorm:"column:index_method"`
	}
	err := db.WithContext(ctx).Raw(query, table).Scan(&results).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			log.Debug("No indexes found for table.")
			return []IndexInfo{}, nil
		}
		return nil, fmt.Errorf("postgres index query failed for table '%s': %w", table, err)
	}

	indexes := make([]IndexInfo, 0, len(results))
	for _, r := range results {
		columns := s.parsePostgresIndexColumnsFromDef(r.RawDef, table, log)
		indexes = append(indexes, IndexInfo{
			Name: r.IndexName, IsUnique: r.IsUnique, IsPrimary: r.IsPrimary,
			RawDef: r.RawDef, Columns: columns,
		})
	}

	sort.Slice(indexes, func(i, j int) bool { return indexes[i].Name < indexes[j].Name })
	log.Debug("Fetched PostgreSQL index info successfully.", zap.Int("index_count", len(indexes)))
	return indexes, nil
}

func (s *SchemaSyncer) parsePostgresIndexColumnsFromDef(rawDef, tableName string, log *zap.Logger) []string {
	reParenContent := regexp.MustCompile(`\((.+)\)`)
	matchesParen := reParenContent.FindStringSubmatch(rawDef)
	if len(matchesParen) < 2 {
		log.Debug("Could not find parenthesized column/expression list in index definition.", zap.String("raw_def_preview", truncateForLog(rawDef, 70)))
		return []string{}
	}
	contentInsideParens := matchesParen[1]
	var columns []string
	var currentPart strings.Builder
	parenLevel := 0
	for _, char := range contentInsideParens {
		if char == '(' { parenLevel++ } else if char == ')' { parenLevel-- }
		if char == ',' && parenLevel == 0 {
			columns = append(columns, strings.TrimSpace(currentPart.String()))
			currentPart.Reset()
		} else {
			currentPart.WriteRune(char)
		}
	}
	if currentPart.Len() > 0 {
		columns = append(columns, strings.TrimSpace(currentPart.String()))
	}
	cleanedColumns := make([]string, 0, len(columns))
	for _, colExpr := range columns {
		colExpr = regexp.MustCompile(`(?i)\s+(ASC|DESC|NULLS\s+(FIRST|LAST)|COLLATE\s+\S+|USING\s+\S+|\w+_ops)$`).ReplaceAllString(colExpr, "")
		cleanedColumns = append(cleanedColumns, strings.TrimSpace(colExpr))
	}
	if len(cleanedColumns) > 0 {
	    log.Debug("Parsed columns/expressions from index definition.", zap.String("raw_def_preview", truncateForLog(rawDef, 50)), zap.Strings("parsed_cols_exprs", cleanedColumns))
	} else if contentInsideParens != "" {
	    log.Warn("Found content within parentheses for index definition but failed to parse into distinct columns/expressions.", zap.String("content", contentInsideParens), zap.String("raw_def", rawDef))
	}
	return cleanedColumns
}

func (s *SchemaSyncer) getPostgresConstraints(ctx context.Context, db *gorm.DB, table string) ([]ConstraintInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "postgres"), zap.String("action", "getPostgresConstraints"))

	var tableExists int64
	db.WithContext(ctx).Raw("SELECT count(*) from pg_class c join pg_namespace n on n.oid=c.relnamespace where c.relname=? and n.nspname=current_schema() and c.relkind IN ('r','m','p')", table).Scan(&tableExists)
	if tableExists == 0 {
		log.Warn("Table not found, cannot fetch constraints.")
		return []ConstraintInfo{}, nil
	}

	query := `
	SELECT
		con.conname AS constraint_name,
		CASE con.contype WHEN 'p' THEN 'PRIMARY KEY' WHEN 'u' THEN 'UNIQUE' WHEN 'f' THEN 'FOREIGN KEY' WHEN 'c' THEN 'CHECK' ELSE con.contype::text END AS constraint_type,
		pg_get_constraintdef(con.oid, true) AS definition,
		(SELECT array_agg(att.attname ORDER BY array_position(con.conkey, att.attnum)) FROM pg_attribute att WHERE att.attrelid = con.conrelid AND att.attnum = ANY(con.conkey)) AS local_columns,
		confrelid.relname AS foreign_table_name,
		(SELECT array_agg(attf.attname ORDER BY array_position(con.confkey, attf.attnum)) FROM pg_attribute attf WHERE attf.attrelid = con.confrelid AND attf.attnum = ANY(con.confkey)) AS foreign_columns_array,
		CASE con.confupdtype WHEN 'a' THEN 'NO ACTION' WHEN 'r' THEN 'RESTRICT' WHEN 'c' THEN 'CASCADE' WHEN 'n' THEN 'SET NULL' WHEN 'd' THEN 'SET DEFAULT' ELSE '' END AS update_rule,
		CASE con.confdeltype WHEN 'a' THEN 'NO ACTION' WHEN 'r' THEN 'RESTRICT' WHEN 'c' THEN 'CASCADE' WHEN 'n' THEN 'SET NULL' WHEN 'd' THEN 'SET DEFAULT' ELSE '' END AS delete_rule
	FROM pg_catalog.pg_constraint con
	JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
	JOIN pg_catalog.pg_namespace nsp ON nsp.oid = rel.relnamespace
	LEFT JOIN pg_catalog.pg_class confrelid ON confrelid.oid = con.confrelid
	WHERE nsp.nspname = current_schema() AND rel.relname = $1
	ORDER BY con.conname;
	`
	var results []struct {
		ConstraintName      string         `gorm:"column:constraint_name"`
		ConstraintType      string         `gorm:"column:constraint_type"`
		Definition          string         `gorm:"column:definition"`
		LocalColumns        pq.StringArray `gorm:"column:local_columns"`
		ForeignTableName    sql.NullString `gorm:"column:foreign_table_name"`
		ForeignColumnsArray pq.StringArray `gorm:"column:foreign_columns_array"`
		UpdateRule          string         `gorm:"column:update_rule"`
		DeleteRule          string         `gorm:"column:delete_rule"`
	}

	err := db.WithContext(ctx).Raw(query, table).Scan(&results).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			log.Debug("No constraints found for table.")
			return []ConstraintInfo{}, nil
		}
		return nil, fmt.Errorf("postgres constraints query failed for table '%s': %w", table, err)
	}

	constraints := make([]ConstraintInfo, 0, len(results))
	for _, r := range results {
		cons := ConstraintInfo{
			Name: r.ConstraintName, Type: r.ConstraintType,
			Columns: r.LocalColumns, ForeignTable: r.ForeignTableName.String,
			ForeignColumns: r.ForeignColumnsArray, OnDelete: r.DeleteRule, OnUpdate: r.UpdateRule,
		}
		if cons.Type == "CHECK" && r.Definition != "" {
			if matches := regexp.MustCompile(`(?i)CHECK\s*\((.*)\)`).FindStringSubmatch(r.Definition); len(matches) > 1 {
				cons.Definition = strings.TrimSpace(matches[1])
			} else {
				log.Warn("Could not parse CHECK constraint definition from raw DDL.", zap.String("constraint", r.ConstraintName), zap.String("raw_def", r.Definition))
				cons.Definition = r.Definition
			}
		} else if cons.Type != "CHECK" {
			cons.Definition = ""
		}
		constraints = append(constraints, cons)
	}

	sort.Slice(constraints, func(i, j int) bool { return constraints[i].Name < constraints[j].Name })
	log.Debug("Fetched PostgreSQL constraint info successfully.", zap.Int("constraint_count", len(constraints)))
	return constraints, nil
}
