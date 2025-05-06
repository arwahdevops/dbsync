package sync

import (
	"context"
	"database/sql"
	"fmt"
	"sort" // <-- Pastikan impor sort ada
	"strings"

	"go.uber.org/zap"
	"gorm.io/gorm"
	// "github.com/arwahdevops/dbsync/internal/utils" // Tidak perlu di file ini
)

// --- PostgreSQL Specific Fetching ---

func (s *SchemaSyncer) getPostgresColumns(ctx context.Context, db *gorm.DB, table string) ([]ColumnInfo, error) {
	query := `
	SELECT c.column_name, c.ordinal_position, c.column_default, c.is_nullable, c.data_type, c.udt_name,
	       c.character_maximum_length, c.numeric_precision, c.numeric_scale, c.collation_name,
	       pgd.description AS column_comment, c.is_identity, c.identity_generation,
		   c.is_generated, c.generation_expression,
	       CASE WHEN tc.constraint_type = 'PRIMARY KEY' THEN TRUE ELSE FALSE END AS is_primary_key
	FROM information_schema.columns c
	LEFT JOIN information_schema.key_column_usage kcu ON c.table_schema = kcu.table_schema AND c.table_name = kcu.table_name AND c.column_name = kcu.column_name
	LEFT JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name AND kcu.table_schema = tc.table_schema AND kcu.table_name = tc.table_name AND tc.constraint_type = 'PRIMARY KEY'
	LEFT JOIN pg_catalog.pg_statio_all_tables AS st ON c.table_schema = st.schemaname AND c.table_name = st.relname
	LEFT JOIN pg_catalog.pg_description pgd ON pgd.objoid = st.relid AND pgd.objsubid = c.ordinal_position
	WHERE c.table_schema = current_schema() AND c.table_name = $1 ORDER BY c.ordinal_position;
	`
	var columnsData []struct {
		ColumnName             string         `gorm:"column:column_name"`
		OrdinalPosition        int            `gorm:"column:ordinal_position"`
		ColumnDefault          sql.NullString `gorm:"column:column_default"`
		IsNullable             string         `gorm:"column:is_nullable"`
		DataType               string         `gorm:"column:data_type"`
		UdtName                string         `gorm:"column:udt_name"`
		CharacterMaximumLength sql.NullInt64  `gorm:"column:character_maximum_length"`
		NumericPrecision       sql.NullInt64  `gorm:"column:numeric_precision"`
		NumericScale           sql.NullInt64  `gorm:"column:numeric_scale"`
		CollationName          sql.NullString `gorm:"column:collation_name"`
		ColumnComment          sql.NullString `gorm:"column:column_comment"`
		IsIdentity             string         `gorm:"column:is_identity"`
		IdentityGeneration     sql.NullString `gorm:"column:identity_generation"`
		IsGenerated            sql.NullString `gorm:"column:is_generated"`
		GenerationExpression   sql.NullString `gorm:"column:generation_expression"`
		IsPrimaryKey           bool           `gorm:"column:is_primary_key"`
	}
	err := db.WithContext(ctx).Raw(query, table).Scan(&columnsData).Error
	if err != nil {
		if strings.Contains(err.Error(), "column c.is_generated does not exist") || strings.Contains(err.Error(), "column c.generation_expression does not exist") {
			s.logger.Debug("PG < 12 detected, retrying column query without generated fields.")
			queryLegacy := `SELECT c.column_name, c.ordinal_position, c.column_default, c.is_nullable, c.data_type, c.udt_name, c.character_maximum_length, c.numeric_precision, c.numeric_scale, c.collation_name, pgd.description AS column_comment, c.is_identity, c.identity_generation, NULL AS is_generated, NULL AS generation_expression, CASE WHEN tc.constraint_type = 'PRIMARY KEY' THEN TRUE ELSE FALSE END AS is_primary_key FROM information_schema.columns c LEFT JOIN information_schema.key_column_usage kcu ON c.table_schema = kcu.table_schema AND c.table_name = kcu.table_name AND c.column_name = kcu.column_name LEFT JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name AND kcu.table_schema = tc.table_schema AND kcu.table_name = tc.table_name AND tc.constraint_type = 'PRIMARY KEY' LEFT JOIN pg_catalog.pg_statio_all_tables AS st ON c.table_schema = st.schemaname AND c.table_name = st.relname LEFT JOIN pg_catalog.pg_description pgd ON pgd.objoid = st.relid AND pgd.objsubid = c.ordinal_position WHERE c.table_schema = current_schema() AND c.table_name = $1 ORDER BY c.ordinal_position;`
			err = db.WithContext(ctx).Raw(queryLegacy, table).Scan(&columnsData).Error; if err != nil { return nil, fmt.Errorf("pg legacy columns query failed: %w", err) }
		} else { return nil, fmt.Errorf("pg columns query failed: %w", err) }
	}

	result := make([]ColumnInfo, 0, len(columnsData))
	for _, c := range columnsData {
		srcType := c.DataType; if c.UdtName != "" && (c.DataType == "ARRAY" || strings.HasPrefix(c.UdtName, "_")) { cleanUdtName := strings.TrimPrefix(c.UdtName, "_"); srcType = cleanUdtName + "[]" } else if c.UdtName != "" && c.DataType == "USER-DEFINED" { srcType = c.UdtName }
		isAutoIncrement := c.IsIdentity == "YES" || (c.ColumnDefault.Valid && strings.HasPrefix(c.ColumnDefault.String, "nextval(")); isGenerated := c.IsGenerated.Valid && c.IsGenerated.String == "ALWAYS"
		colInfo := ColumnInfo{ Name: c.ColumnName, Type: srcType, IsNullable: c.IsNullable == "YES", IsPrimary: c.IsPrimaryKey, DefaultValue: c.ColumnDefault, AutoIncrement: isAutoIncrement, IsGenerated: isGenerated, OrdinalPosition: c.OrdinalPosition, Length: c.CharacterMaximumLength, Precision: c.NumericPrecision, Scale: c.NumericScale, Collation: c.CollationName, Comment: c.ColumnComment, }
		var mappedType string; var mapErr error
		if !isGenerated { mappedType, mapErr = s.mapDataType(colInfo.Type); if mapErr != nil { s.logger.Warn("Mapping failed", zap.Error(mapErr)); mappedType = colInfo.Type } } else { mappedType = colInfo.Type; s.logger.Warn("Mapping skipped for generated col", zap.String("col", c.ColumnName)) }
		colInfo.MappedType = mappedType; result = append(result, colInfo)
	}
	return result, nil
}


func (s *SchemaSyncer) getPostgresIndexes(ctx context.Context, db *gorm.DB, table string) ([]IndexInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "postgres"))
	log.Debug("Fetching PostgreSQL indexes")
	indexes := make([]IndexInfo, 0)
	idxMap := make(map[string]*IndexInfo)

	query := `
	SELECT i.relname as index_name, idx.indisunique as is_unique, idx.indisprimary as is_primary,
		   a.attname as column_name,
		   array_position(idx.indkey::int[], a.attnum::int) as column_seq,
		   pg_get_indexdef(idx.indexrelid) as raw_def
	FROM pg_class t JOIN pg_index idx ON t.oid = idx.indrelid JOIN pg_class i ON i.oid = idx.indexrelid
		 JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(idx.indkey)
	WHERE t.relkind = 'r' AND t.relname = $1
	  AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())
	ORDER BY index_name, column_seq;
	`
	var results []struct { IndexName string; IsUnique bool; IsPrimary bool; ColumnName string; ColumnSeq sql.NullInt64; RawDef string; }
	if err := db.WithContext(ctx).Raw(query, table).Scan(&results).Error; err != nil {
		if strings.Contains(err.Error(), "does not exist") { return indexes, nil }
		return nil, fmt.Errorf("postgres index query failed for %s: %w", table, err)
	}
	tempColMap := make(map[string][]struct{ Name string; Seq int64 }); idxDetails := make(map[string]*IndexInfo)
	for _, r := range results {
		if _, ok := idxDetails[r.IndexName]; !ok { idxDetails[r.IndexName] = &IndexInfo{Name: r.IndexName, IsUnique: r.IsUnique, IsPrimary: r.IsPrimary, RawDef: r.RawDef}; tempColMap[r.IndexName] = make([]struct{ Name string; Seq int64 }, 0) }
		seq := int64(1); if r.ColumnSeq.Valid { seq = r.ColumnSeq.Int64 }; tempColMap[r.IndexName] = append(tempColMap[r.IndexName], struct{ Name string; Seq int64 }{r.ColumnName, seq})
	}
	for name, idx := range idxDetails {
		cols := tempColMap[name]; sort.Slice(cols, func(i, j int) bool { return cols[i].Seq < cols[j].Seq }); idx.Columns = make([]string, len(cols)); for i, c := range cols { idx.Columns[i] = c.Name }; idxMap[name] = idx
	}

	for _, idx := range idxMap { sort.Strings(idx.Columns); indexes = append(indexes, *idx) } // Gunakan sort
	sort.Slice(indexes, func(i, j int) bool { return indexes[i].Name < indexes[j].Name }) // Gunakan sort
	return indexes, nil
}


func (s *SchemaSyncer) getPostgresConstraints(ctx context.Context, db *gorm.DB, table string) ([]ConstraintInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "postgres"))
	log.Debug("Fetching PostgreSQL constraints")
	constraints := make([]ConstraintInfo, 0)
	consMap := make(map[string]*ConstraintInfo)

	var results []struct {
		ConstraintName    string         `gorm:"column:constraint_name"`
		ConstraintType    string         `gorm:"column:constraint_type"`
		ColumnName        string         `gorm:"column:column_name"`
		OrdinalPosition   sql.NullInt64  `gorm:"column:ordinal_position"`
		ForeignTableName  sql.NullString `gorm:"column:foreign_table_name"`
		ForeignColumnName sql.NullString `gorm:"column:foreign_column_name"`
		UpdateRule        sql.NullString `gorm:"column:update_rule"`
		DeleteRule        sql.NullString `gorm:"column:delete_rule"`
		CheckClause       sql.NullString `gorm:"column:check_clause"`
	}
	query := `
	SELECT DISTINCT tc.constraint_name, tc.constraint_type, kcu.column_name, kcu.ordinal_position,
		   ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name,
		   rc.update_rule, rc.delete_rule, cons.check_clause
	FROM information_schema.table_constraints tc
	JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema AND tc.table_name = kcu.table_name
	LEFT JOIN information_schema.referential_constraints rc ON tc.constraint_name = rc.constraint_name AND tc.table_schema = rc.constraint_schema
	LEFT JOIN information_schema.constraint_column_usage ccu ON rc.unique_constraint_name = ccu.constraint_name AND rc.unique_constraint_schema = ccu.constraint_schema AND kcu.position_in_unique_constraint = ccu.ordinal_position
	LEFT JOIN information_schema.check_constraints cons ON tc.constraint_name = cons.constraint_name AND tc.constraint_schema = cons.constraint_schema
	WHERE tc.table_name = $1 AND tc.table_schema = current_schema() ORDER BY tc.constraint_name, kcu.ordinal_position;
	`
	schemaName := "public" // Default
	errSchema := db.WithContext(ctx).Raw("SELECT current_schema()").Scan(&schemaName).Error
	if errSchema != nil { log.Warn("Could not determine current_schema", zap.Error(errSchema)) }

	err := db.WithContext(ctx).Raw(query, table).Scan(&results).Error
	if err != nil {
		if strings.Contains(err.Error(), "does not exist"){ return constraints, nil }
		if strings.Contains(err.Error(), "relation \"information_schema.check_constraints\" does not exist") {
			log.Warn("check_constraints not found, retrying without CHECK.")
			queryNoCheck := strings.Replace(query, "cons.check_clause", "NULL AS check_clause", 1); queryNoCheck = strings.Replace(queryNoCheck, "LEFT JOIN information_schema.check_constraints cons", "-- LEFT JOIN", 1); queryNoCheck = strings.Replace(queryNoCheck, "AND tc.constraint_schema = cons.constraint_schema", "", 1)
			err = db.WithContext(ctx).Raw(queryNoCheck, table).Scan(&results).Error
			if err != nil { return nil, fmt.Errorf("pg legacy constraint query failed: %w", err) }
		} else { return nil, fmt.Errorf("pg constraint query failed: %w", err) }
	}

	tempFKCols := make(map[string][]struct{ Local, Foreign string; Seq int64 })
	for _, r := range results {
		if _, ok := consMap[r.ConstraintName]; !ok { consMap[r.ConstraintName] = &ConstraintInfo{ Name: r.ConstraintName, Type: r.ConstraintType, Columns: make([]string, 0), ForeignTable: r.ForeignTableName.String, ForeignColumns: make([]string, 0), OnDelete: r.DeleteRule.String, OnUpdate: r.UpdateRule.String, Definition: r.CheckClause.String, }; if r.ConstraintType == "FOREIGN KEY" { tempFKCols[r.ConstraintName] = make([]struct{ Local, Foreign string; Seq int64 }, 0) } }
		currentLen := len(consMap[r.ConstraintName].Columns); neededLen := 1; if r.OrdinalPosition.Valid { neededLen = int(r.OrdinalPosition.Int64) }; if neededLen > currentLen { consMap[r.ConstraintName].Columns = append(consMap[r.ConstraintName].Columns, make([]string, neededLen-currentLen)...) }; if neededLen > 0 { consMap[r.ConstraintName].Columns[neededLen-1] = r.ColumnName } else { consMap[r.ConstraintName].Columns = append(consMap[r.ConstraintName].Columns, r.ColumnName) }
		if r.ConstraintType == "FOREIGN KEY" && r.ForeignColumnName.Valid { seq := int64(1); if r.OrdinalPosition.Valid { seq = r.OrdinalPosition.Int64 }; tempFKCols[r.ConstraintName] = append(tempFKCols[r.ConstraintName], struct{ Local, Foreign string; Seq int64 }{r.ColumnName, r.ForeignColumnName.String, seq}) }
	}
	for name, fkCons := range consMap {
		if fkCons.Type == "FOREIGN KEY" { fkCols := tempFKCols[name]; sort.Slice(fkCols, func(i, j int) bool { return fkCols[i].Seq < fkCols[j].Seq }); fkCons.ForeignColumns = make([]string, len(fkCols)); fkCons.Columns = make([]string, len(fkCols)); for i, c := range fkCols { fkCons.Columns[i] = c.Local; fkCons.ForeignColumns[i] = c.Foreign } } else { sort.Strings(fkCons.Columns) } // Gunakan sort
		cleanedCols := make([]string, 0, len(fkCons.Columns)); for _, c := range fkCons.Columns { if c != "" { cleanedCols = append(cleanedCols, c) } }; fkCons.Columns = cleanedCols
	}

	for _, cons := range consMap { constraints = append(constraints, *cons) }
	sort.Slice(constraints, func(i, j int) bool { return constraints[i].Name < constraints[j].Name }) // Gunakan sort
	return constraints, nil
}