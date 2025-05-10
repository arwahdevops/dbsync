// internal/sync/schema_fetch_postgres.go
package sync

import (
	"context"
	"database/sql"
	"fmt"
	"sort" // Tetap dibutuhkan untuk sorting *slice of indexes* di akhir, bukan kolom di dalam indeks
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

	query := `
	SELECT
		i.relname AS index_name,
		idx.indisunique AS is_unique,
		idx.indisprimary AS is_primary,
		a.attname AS column_name,
		-- array_position gives 1-based index, or NULL if attnum is not in indkey (e.g. expression index)
		array_position(idx.indkey::int[], a.attnum::int) AS column_seq,
		pg_get_indexdef(idx.indexrelid) AS raw_def,
		am.amname AS index_method
	FROM pg_catalog.pg_class t
	JOIN pg_catalog.pg_index idx ON t.oid = idx.indrelid
	JOIN pg_catalog.pg_class i ON i.oid = idx.indexrelid
	JOIN pg_catalog.pg_am am ON i.relam = am.oid
	-- For actual columns:
	LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(idx.indkey)
	WHERE t.relkind IN ('r', 'm', 'p') -- r = ordinary table, m = materialized view, p = partitioned table
	  AND t.relname = $1
	  AND t.relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = current_schema())
	ORDER BY index_name, column_seq; -- column_seq will ensure columns are fetched in their index order
	`
	var results []struct {
		IndexName   string
		IsUnique    bool
		IsPrimary   bool
		ColumnName  sql.NullString // Can be NULL for expression indexes
		ColumnSeq   sql.NullInt64  // 1-based sequence of the column in the index key
		RawDef      string
		IndexMethod string
	}
	err := db.WithContext(ctx).Raw(query, table).Scan(&results).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound || strings.Contains(strings.ToLower(err.Error()), "does not exist") {
			log.Warn("Table not found or no indexes found.", zap.Error(err))
			return indexes, nil
		}
		return nil, fmt.Errorf("postgres index query failed for table '%s': %w", table, err)
	}

	// Map untuk mengumpulkan kolom per indeks, mempertahankan urutan
	idxMap := make(map[string]*IndexInfo)
	// Map untuk menyimpan kolom-kolom sementara dengan urutan aslinya
	tempColMap := make(map[string][]struct{ Name string; Seq int64 })

	for _, r := range results {
		idx, ok := idxMap[r.IndexName]
		if !ok {
			idx = &IndexInfo{
				Name:      r.IndexName,
				IsUnique:  r.IsUnique,
				IsPrimary: r.IsPrimary,
				RawDef:    r.RawDef,
				Columns:   make([]string, 0), // Akan diisi nanti
			}
			idxMap[r.IndexName] = idx
			tempColMap[r.IndexName] = make([]struct{ Name string; Seq int64 }, 0)
		}

		if r.ColumnName.Valid && r.ColumnName.String != "" && r.ColumnSeq.Valid {
			tempColMap[r.IndexName] = append(tempColMap[r.IndexName], struct{ Name string; Seq int64 }{
				Name: r.ColumnName.String,
				Seq:  r.ColumnSeq.Int64, // Ini sudah 1-based dan menjaga urutan
			})
		} else if !r.ColumnName.Valid {
			// Ini adalah indeks ekspresi, kita akan menggunakan RawDef untuk representasi
			// dan mungkin mencoba mem-parsing kolom dari RawDef jika diperlukan (kompleks).
			// Untuk saat ini, tandai saja bahwa kolomnya tidak sederhana.
			log.Debug("Index appears to be functional/expression-based (no direct column name).",
				zap.String("index", r.IndexName), zap.String("raw_def", r.RawDef))
			// Kita bisa menambahkan placeholder atau parsing RawDef di sini jika diperlukan.
			// Misalnya, jika RawDef adalah "CREATE INDEX my_idx ON my_table (lower(email))", kita ingin (lower(email))
			// Untuk sekarang, biarkan Columns kosong jika itu indeks ekspresi dan kita tidak mem-parsingnya.
		}
	}

	for name, idx := range idxMap {
		colData := tempColMap[name]
		// Urutkan kolom berdasarkan Seq yang diambil dari database (penting untuk konsistensi)
		// Sebenarnya, query `ORDER BY column_seq` sudah melakukannya, jadi sort ini mungkin redundan
		// tapi tidak berbahaya.
		sort.Slice(colData, func(i, j int) bool {
			return colData[i].Seq < colData[j].Seq
		})

		idx.Columns = make([]string, len(colData))
		for i, cd := range colData {
			idx.Columns[i] = cd.Name
		}
		// *** PENTING: HAPUS sort.Strings(idx.Columns) ***
		// Urutan kolom harus dipertahankan seperti dari database.

		indexes = append(indexes, *idx)
	}

	// Urutkan slice `indexes` berdasarkan nama untuk output yang konsisten
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
		-- array_position is 1-based, gives the order of column in the constraint key
		array_position(con.conkey, att.attnum) AS ordinal_position,
		confrelid.relname AS foreign_table_name,
		attf.attname AS foreign_column_name,
		-- array_position for foreign columns to maintain order
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
	-- For local columns
	LEFT JOIN pg_catalog.pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = ANY(con.conkey)
	-- For foreign key's referenced table and columns
	LEFT JOIN pg_catalog.pg_class confrelid ON confrelid.oid = con.confrelid
	LEFT JOIN pg_catalog.pg_attribute attf ON attf.attrelid = con.confrelid AND attf.attnum = ANY(con.confkey)
	WHERE nsp.nspname = current_schema() AND rel.relname = $1
	ORDER BY constraint_name, ordinal_position, fk_ordinal_position; -- Ensure consistent order
	`
	var results []struct {
		ConstraintName    string
		ConstraintType    string
		ColumnName        sql.NullString
		OrdinalPosition   sql.NullInt64 // 1-based
		ForeignTableName  sql.NullString
		ForeignColumnName sql.NullString
		FkOrdinalPosition sql.NullInt64 // 1-based
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

	// Temporary maps to gather columns in their correct order
	tempLocalCols := make(map[string][]struct{ Seq int64; Name string })
	tempForeignCols := make(map[string][]struct{ Seq int64; Name string })


	for _, r := range results {
		constraintName := r.ConstraintName
		constraintType := r.ConstraintType

		cons, ok := consMap[constraintName]
		if !ok {
			cons = &ConstraintInfo{
				Name:           constraintName,
				Type:           constraintType,
				Columns:        make([]string, 0), // Will be populated later
				ForeignTable:   r.ForeignTableName.String,
				ForeignColumns: make([]string, 0), // Will be populated later
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
			// Initialize slices for this constraint if not already done
			if _, exists := tempLocalCols[constraintName]; !exists {
				tempLocalCols[constraintName] = make([]struct{ Seq int64; Name string }, 0)
			}
			if constraintType == "FOREIGN KEY" {
				if _, exists := tempForeignCols[constraintName]; !exists {
					tempForeignCols[constraintName] = make([]struct{ Seq int64; Name string }, 0)
				}
			}
		}

		// Populate local columns
		if r.ColumnName.Valid && r.ColumnName.String != "" && r.OrdinalPosition.Valid {
			// Check for duplicates before appending (important if query returns multiple rows for same col in same constraint)
			alreadyExists := false
			for _, lc := range tempLocalCols[constraintName] {
				if lc.Seq == r.OrdinalPosition.Int64 && lc.Name == r.ColumnName.String {
					alreadyExists = true
					break
				}
			}
			if !alreadyExists {
				tempLocalCols[constraintName] = append(tempLocalCols[constraintName], struct{ Seq int64; Name string }{
					Seq:  r.OrdinalPosition.Int64,
					Name: r.ColumnName.String,
				})
			}
		}

		// Populate foreign columns for FKs
		if constraintType == "FOREIGN KEY" && r.ForeignColumnName.Valid && r.ForeignColumnName.String != "" && r.FkOrdinalPosition.Valid {
			alreadyExists := false
			for _, fc := range tempForeignCols[constraintName] {
				if fc.Seq == r.FkOrdinalPosition.Int64 && fc.Name == r.ForeignColumnName.String {
					alreadyExists = true
					break
				}
			}
			if !alreadyExists {
				tempForeignCols[constraintName] = append(tempForeignCols[constraintName], struct{ Seq int64; Name string }{
					Seq:  r.FkOrdinalPosition.Int64,
					Name: r.ForeignColumnName.String,
				})
			}
		}
	}

	// Finalize column lists for each constraint, ensuring correct order
	for constraintName, cons := range consMap {
		// Local columns
		localColData := tempLocalCols[constraintName]
		sort.Slice(localColData, func(i, j int) bool { return localColData[i].Seq < localColData[j].Seq })
		cons.Columns = make([]string, len(localColData))
		for i, cd := range localColData {
			cons.Columns[i] = cd.Name
		}
		// *** PENTING: JANGAN sort.Strings(cons.Columns) ***

		// Foreign columns (if FK)
		if cons.Type == "FOREIGN KEY" {
			foreignColData := tempForeignCols[constraintName]
			sort.Slice(foreignColData, func(i, j int) bool { return foreignColData[i].Seq < foreignColData[j].Seq })
			cons.ForeignColumns = make([]string, len(foreignColData))
			for i, cd := range foreignColData {
				cons.ForeignColumns[i] = cd.Name
			}
			// *** PENTING: JANGAN sort.Strings(cons.ForeignColumns) ***
		}
		constraints = append(constraints, *cons)
	}

	sort.Slice(constraints, func(i, j int) bool { return constraints[i].Name < constraints[j].Name })
	log.Debug("Fetched constraint info successfully.", zap.Int("constraint_count", len(constraints)))
	return constraints, nil
}
