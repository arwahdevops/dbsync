// internal/sync/schema_fetch_postgres.go
package sync

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// --- PostgreSQL Specific Fetching ---

func (s *SchemaSyncer) getPostgresColumns(ctx context.Context, db *gorm.DB, table string) ([]ColumnInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "postgres"), zap.String("action", "getPostgresColumns"))

	// Query utama yang mencoba mengambil semua info, termasuk kolom PG12+
	query := `
	SELECT
		c.column_name,
		c.ordinal_position,
		c.column_default,
		c.is_nullable,        -- YES / NO
		c.data_type,          -- Tipe dasar (e.g., ARRAY, character varying, integer, USER-DEFINED)
		c.udt_name,           -- Tipe data user-defined atau tipe dasar array (e.g., _int4 for int[])
		c.character_maximum_length,
		c.numeric_precision,  -- Presisi total untuk numeric/decimal
		c.datetime_precision, -- Presisi untuk tipe waktu/tanggal
		c.numeric_scale,      -- Skala untuk numeric/decimal
		c.collation_name,
		pgd.description AS column_comment,
		c.is_identity,        -- YES / NO (PG10+)
		c.identity_generation,-- ALWAYS / BY DEFAULT (PG10+)
		c.is_generated,       -- NEVER / ALWAYS (PG12+)
		c.generation_expression, -- (PG12+)
		-- Cek PK menggunakan pg_constraint dan pg_attribute
		EXISTS (
			SELECT 1
			FROM pg_catalog.pg_constraint con
			JOIN pg_catalog.pg_attribute att ON att.attnum = ANY(con.conkey) AND att.attrelid = con.conrelid
			WHERE con.conrelid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = c.table_name AND relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = c.table_schema))
			  AND con.contype = 'p' -- Primary key
			  AND att.attname = c.column_name
		) AS is_primary_key
	FROM information_schema.columns c
	-- Join untuk comment
	LEFT JOIN pg_catalog.pg_class tbl ON tbl.relname = c.table_name AND tbl.relnamespace = (SELECT ns.oid FROM pg_catalog.pg_namespace ns WHERE ns.nspname = c.table_schema)
	LEFT JOIN pg_catalog.pg_description pgd ON pgd.objoid = tbl.oid AND pgd.objsubid = c.ordinal_position
	WHERE c.table_schema = current_schema() AND c.table_name = $1
	ORDER BY c.ordinal_position;
	`
	// Struct untuk menampung hasil query
	var columnsData []struct {
		ColumnName             string         `gorm:"column:column_name"`
		OrdinalPosition        int            `gorm:"column:ordinal_position"`
		ColumnDefault          sql.NullString `gorm:"column:column_default"`
		IsNullable             string         `gorm:"column:is_nullable"`
		DataType               string         `gorm:"column:data_type"`
		UdtName                string         `gorm:"column:udt_name"`
		CharacterMaximumLength sql.NullInt64  `gorm:"column:character_maximum_length"`
		NumericPrecision       sql.NullInt64  `gorm:"column:numeric_precision"`
		DateTimePrecision      sql.NullInt64  `gorm:"column:datetime_precision"` // Presisi untuk waktu
		NumericScale           sql.NullInt64  `gorm:"column:numeric_scale"`
		CollationName          sql.NullString `gorm:"column:collation_name"`
		ColumnComment          sql.NullString `gorm:"column:column_comment"`
		IsIdentity             string         `gorm:"column:is_identity"`        // YES / NO
		IdentityGeneration     sql.NullString `gorm:"column:identity_generation"`// ALWAYS / BY DEFAULT
		IsGenerated            sql.NullString `gorm:"column:is_generated"`       // ALWAYS / NEVER
		GenerationExpression   sql.NullString `gorm:"column:generation_expression"`
		IsPrimaryKey           bool           `gorm:"column:is_primary_key"` // Didapat dari pg_constraint
	}

	err := db.WithContext(ctx).Raw(query, table).Scan(&columnsData).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound || strings.Contains(strings.ToLower(err.Error()), "does not exist"){
             log.Warn("Table not found or query returned no rows.", zap.Error(err))
             return []ColumnInfo{}, nil
        }
		if strings.Contains(err.Error(), "column c.is_generated does not exist") ||
			strings.Contains(err.Error(), "column c.generation_expression does not exist") {
			log.Warn("Query failed, likely PostgreSQL < 12. Retrying without generated column fields.")
            // Definisi subquery PK untuk digunakan kembali
			subQueryPK := `EXISTS (
				SELECT 1
				FROM pg_catalog.pg_constraint con
				JOIN pg_catalog.pg_attribute att ON att.attnum = ANY(con.conkey) AND att.attrelid = con.conrelid
				WHERE con.conrelid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = c.table_name AND relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = c.table_schema))
				  AND con.contype = 'p' AND att.attname = c.column_name
			)`
            // Definisi join comment untuk digunakan kembali
            joinComment := `LEFT JOIN pg_catalog.pg_class tbl ON tbl.relname = c.table_name AND tbl.relnamespace = (SELECT ns.oid FROM pg_catalog.pg_namespace ns WHERE ns.nspname = c.table_schema) LEFT JOIN pg_catalog.pg_description pgd ON pgd.objoid = tbl.oid AND pgd.objsubid = c.ordinal_position`

			queryLegacy := fmt.Sprintf(`
				SELECT
					c.column_name, c.ordinal_position, c.column_default, c.is_nullable, c.data_type, c.udt_name,
					c.character_maximum_length, c.numeric_precision, c.datetime_precision, c.numeric_scale, c.collation_name,
					pgd.description AS column_comment, c.is_identity, c.identity_generation,
					'NEVER' AS is_generated, NULL AS generation_expression, -- Fallback values
					%s AS is_primary_key
				FROM information_schema.columns c
				%s
				WHERE c.table_schema = current_schema() AND c.table_name = $1 ORDER BY c.ordinal_position;
			`, subQueryPK, joinComment) // Gunakan definisi yang disimpan

			err = db.WithContext(ctx).Raw(queryLegacy, table).Scan(&columnsData).Error
			if err != nil {
				if err == gorm.ErrRecordNotFound || strings.Contains(strings.ToLower(err.Error()), "does not exist"){
                     log.Warn("Table not found on legacy query retry.", zap.Error(err))
                     return []ColumnInfo{}, nil
                }
				return nil, fmt.Errorf("postgres legacy columns query failed for table '%s': %w", table, err)
			}
		} else {
			return nil, fmt.Errorf("postgres columns query failed for table '%s': %w", table, err)
		}
	}

	result := make([]ColumnInfo, 0, len(columnsData))
	for _, c := range columnsData {
		srcType := c.DataType
		if c.DataType == "ARRAY" && strings.HasPrefix(c.UdtName, "_") {
			baseType := strings.TrimPrefix(c.UdtName, "_")
			// PERBAIKAN: Panggil normalizeTypeName dengan benar
			mappedBase := normalizeTypeName(baseType)
			srcType = mappedBase + "[]"
		} else if c.DataType == "USER-DEFINED" && c.UdtName != "" {
			srcType = c.UdtName
		}

		isAutoIncrement := c.IsIdentity == "YES" || (c.ColumnDefault.Valid && strings.HasPrefix(c.ColumnDefault.String, "nextval("))
		isGenerated := c.IsGenerated.Valid && c.IsGenerated.String == "ALWAYS"

		precision := c.NumericPrecision
		if strings.Contains(c.DataType, "time") || strings.Contains(c.DataType, "interval") {
			precision = c.DateTimePrecision
		}

		colInfo := ColumnInfo{
			Name:            c.ColumnName,
			Type:            srcType,
			MappedType:      "",
			IsNullable:      c.IsNullable == "YES",
			IsPrimary:       c.IsPrimaryKey,
			IsGenerated:     isGenerated,
			DefaultValue:    c.ColumnDefault,
			AutoIncrement:   isAutoIncrement,
			OrdinalPosition: c.OrdinalPosition,
			Length:          c.CharacterMaximumLength,
			Precision:       precision,
			Scale:           c.NumericScale,
			Collation:       c.CollationName,
			Comment:         c.ColumnComment,
		}
		result = append(result, colInfo)
	}
	log.Debug("Fetched column info successfully.", zap.Int("column_count", len(result)))
	return result, nil
}

func (s *SchemaSyncer) getPostgresIndexes(ctx context.Context, db *gorm.DB, table string) ([]IndexInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "postgres"), zap.String("action", "getPostgresIndexes"))
	indexes := make([]IndexInfo, 0)
	// PERBAIKAN: Hapus deklarasi idxMap yang tidak digunakan
	// idxMap := make(map[string]*IndexInfo)

	query := `
	SELECT
		i.relname AS index_name,
		idx.indisunique AS is_unique,
		idx.indisprimary AS is_primary,
		a.attname AS column_name,
		array_position(idx.indkey::int[], a.attnum::int) AS column_seq,
		pg_get_indexdef(idx.indexrelid) AS raw_def,
		am.amname AS index_method
	FROM pg_catalog.pg_class t
	JOIN pg_catalog.pg_index idx ON t.oid = idx.indrelid
	JOIN pg_catalog.pg_class i ON i.oid = idx.indexrelid
	JOIN pg_catalog.pg_am am ON i.relam = am.oid
	LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(idx.indkey)
	WHERE t.relkind IN ('r', 'm', 'p')
	  AND t.relname = $1
	  AND t.relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = current_schema())
	ORDER BY index_name, column_seq;
	`
	var results []struct {
		IndexName   string
		IsUnique    bool
		IsPrimary   bool
		ColumnName  sql.NullString
		ColumnSeq   sql.NullInt64
		RawDef      string
		IndexMethod string
	}
	err := db.WithContext(ctx).Raw(query, table).Scan(&results).Error
	if err != nil {
        if err == gorm.ErrRecordNotFound || strings.Contains(strings.ToLower(err.Error()), "does not exist"){
             log.Warn("Table not found or no indexes found.", zap.Error(err))
             return indexes, nil
        }
		return nil, fmt.Errorf("postgres index query failed for table '%s': %w", table, err)
	}

	tempColMap := make(map[string][]struct {
		Name string
		Seq  int64
	})
	idxDetails := make(map[string]*IndexInfo) // Gunakan ini sebagai gantinya

	for _, r := range results {
		idx, ok := idxDetails[r.IndexName]
		if !ok {
			idx = &IndexInfo{
				Name:      r.IndexName,
				IsUnique:  r.IsUnique,
				IsPrimary: r.IsPrimary,
				RawDef:    r.RawDef,
				// IndexType: r.IndexMethod,
			}
			idxDetails[r.IndexName] = idx
			tempColMap[r.IndexName] = make([]struct { Name string; Seq int64 }, 0)
		}
		if r.ColumnName.Valid && r.ColumnName.String != "" {
			seq := int64(1)
			if r.ColumnSeq.Valid {
				seq = r.ColumnSeq.Int64
			}
			tempColMap[r.IndexName] = append(tempColMap[r.IndexName], struct { Name string; Seq int64 }{r.ColumnName.String, seq})
		} else if !r.ColumnName.Valid && len(tempColMap[r.IndexName]) == 0 {
			log.Debug("Index might be functional or expression-based (no direct column name).", zap.String("index", r.IndexName), zap.String("raw_def", r.RawDef))
		}
	}

	// Finalisasi: Urutkan kolom per indeks dan tambahkan ke hasil
	for name, idx := range idxDetails { // Iterasi map yang benar
		cols := tempColMap[name]
		sort.Slice(cols, func(i, j int) bool { return cols[i].Seq < cols[j].Seq })
		idx.Columns = make([]string, len(cols))
		for i, c := range cols {
			idx.Columns[i] = c.Name
		}
		sort.Strings(idx.Columns)
		indexes = append(indexes, *idx)
	}

	sort.Slice(indexes, func(i, j int) bool { return indexes[i].Name < indexes[j].Name })
	log.Debug("Fetched index info successfully.", zap.Int("index_count", len(indexes)))
	return indexes, nil
}


func (s *SchemaSyncer) getPostgresConstraints(ctx context.Context, db *gorm.DB, table string) ([]ConstraintInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "postgres"), zap.String("action", "getPostgresConstraints"))
	constraints := make([]ConstraintInfo, 0)
	consMap := make(map[string]*ConstraintInfo)

	query := `
	SELECT
		con.conname AS constraint_name,
		CASE con.contype
			WHEN 'p' THEN 'PRIMARY KEY' WHEN 'u' THEN 'UNIQUE' WHEN 'f' THEN 'FOREIGN KEY' WHEN 'c' THEN 'CHECK'
			ELSE con.contype::text
		END AS constraint_type,
		att.attname AS column_name,
		array_position(con.conkey, att.attnum) AS ordinal_position,
		confrelid.relname AS foreign_table_name,
		attf.attname AS foreign_column_name,
		array_position(con.confkey, attf.attnum) as fk_ordinal_position,
		CASE con.confupdtype
			WHEN 'a' THEN 'NO ACTION' WHEN 'r' THEN 'RESTRICT' WHEN 'c' THEN 'CASCADE' WHEN 'n' THEN 'SET NULL'  WHEN 'd' THEN 'SET DEFAULT' ELSE ''
		END AS update_rule,
		CASE con.confdeltype
			WHEN 'a' THEN 'NO ACTION' WHEN 'r' THEN 'RESTRICT' WHEN 'c' THEN 'CASCADE' WHEN 'n' THEN 'SET NULL'  WHEN 'd' THEN 'SET DEFAULT' ELSE ''
		END AS delete_rule,
		pg_get_constraintdef(con.oid, true) AS definition
	FROM pg_catalog.pg_constraint con
	JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
	JOIN pg_catalog.pg_namespace nsp ON nsp.oid = rel.relnamespace
	LEFT JOIN pg_catalog.pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = ANY(con.conkey)
	LEFT JOIN pg_catalog.pg_class confrelid ON confrelid.oid = con.confrelid
	LEFT JOIN pg_catalog.pg_attribute attf ON attf.attrelid = con.confrelid AND attf.attnum = ANY(con.confkey)
	WHERE nsp.nspname = current_schema() AND rel.relname = $1
	ORDER BY constraint_name, ordinal_position, fk_ordinal_position;
	`
	var results []struct {
		ConstraintName    string
		ConstraintType    string
		ColumnName        sql.NullString
		OrdinalPosition   sql.NullInt64
		ForeignTableName  sql.NullString
		ForeignColumnName sql.NullString
		FkOrdinalPosition sql.NullInt64 // PERBAIKAN: Deklarasi fkSeq tidak perlu lagi
		UpdateRule        string
		DeleteRule        string
		Definition        string
	}
	err := db.WithContext(ctx).Raw(query, table).Scan(&results).Error
	if err != nil {
        if err == gorm.ErrRecordNotFound || strings.Contains(strings.ToLower(err.Error()), "does not exist"){
             log.Warn("Table not found or no constraints found.", zap.Error(err))
             return constraints, nil
        }
		return nil, fmt.Errorf("postgres constraints query failed for table '%s': %w", table, err)
	}

	tempFKCols := make(map[string][]struct{ Seq int64; Local, Foreign string })
	tempCols := make(map[string][]struct{ Seq int64; Name string })

	for _, r := range results {
		constraintName := r.ConstraintName
		constraintType := r.ConstraintType

		cons, ok := consMap[constraintName]
		if !ok {
			cons = &ConstraintInfo{
				Name:           constraintName,
				Type:           constraintType,
				Columns:        make([]string, 0),
				ForeignTable:   r.ForeignTableName.String,
				ForeignColumns: make([]string, 0),
				OnDelete:       r.DeleteRule,
				OnUpdate:       r.UpdateRule,
				Definition: func() string {
					if constraintType == "CHECK" && strings.HasPrefix(r.Definition, "CHECK ") {
						checkBody := strings.TrimPrefix(r.Definition, "CHECK ")
						if strings.HasPrefix(checkBody, "(") && strings.HasSuffix(checkBody, ")") {
							return checkBody[1 : len(checkBody)-1]
						}
						return checkBody
					}
					return ""
				}(),
			}
			consMap[constraintName] = cons
			if constraintType == "FOREIGN KEY" {
				tempFKCols[constraintName] = make([]struct{ Seq int64; Local, Foreign string }, 0)
			} else if constraintType == "PRIMARY KEY" || constraintType == "UNIQUE" {
				tempCols[constraintName] = make([]struct{ Seq int64; Name string }, 0)
			}
		}

		if r.ColumnName.Valid && r.ColumnName.String != "" {
			seq := int64(1); if r.OrdinalPosition.Valid { seq = r.OrdinalPosition.Int64 }
			// PERBAIKAN: Hapus deklarasi fkSeq yang tidak digunakan
			// fkSeq := int64(1); if r.FkOrdinalPosition.Valid { fkSeq = r.FkOrdinalPosition.Int64 }

			if constraintType == "FOREIGN KEY" && r.ForeignColumnName.Valid {
				tempFKCols[constraintName] = append(tempFKCols[constraintName], struct{ Seq int64; Local, Foreign string }{
					Seq:     seq,
					Local:   r.ColumnName.String,
					Foreign: r.ForeignColumnName.String,
				})
			} else if constraintType == "PRIMARY KEY" || constraintType == "UNIQUE" {
				tempCols[constraintName] = append(tempCols[constraintName], struct{ Seq int64; Name string }{
					Seq:  seq,
					Name: r.ColumnName.String,
				})
			}
		}
	}

	for constraintName, cons := range consMap {
		if cons.Type == "FOREIGN KEY" {
			fkColPairs := tempFKCols[constraintName]
			sort.Slice(fkColPairs, func(i, j int) bool { return fkColPairs[i].Seq < fkColPairs[j].Seq })
			cons.Columns = make([]string, len(fkColPairs))
			cons.ForeignColumns = make([]string, len(fkColPairs))
			for i, pair := range fkColPairs {
				cons.Columns[i] = pair.Local
				cons.ForeignColumns[i] = pair.Foreign
			}
		} else if cons.Type == "PRIMARY KEY" || cons.Type == "UNIQUE" {
			cols := tempCols[constraintName]
			sort.Slice(cols, func(i, j int) bool { return cols[i].Seq < cols[j].Seq })
			cons.Columns = make([]string, len(cols))
			for i, c := range cols { cons.Columns[i] = c.Name }
			sort.Strings(cons.Columns)
		}
	}

	for _, cons := range consMap { constraints = append(constraints, *cons) }
	sort.Slice(constraints, func(i, j int) bool { return constraints[i].Name < constraints[j].Name })
	log.Debug("Fetched constraint info successfully.", zap.Int("constraint_count", len(constraints)))
	return constraints, nil
}
