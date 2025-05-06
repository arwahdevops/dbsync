package sync

import (
	"context"
	"database/sql"
	"fmt"
	"sort" // <-- Pastikan impor sort ada
	"strings"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/arwahdevops/dbsync/internal/utils" // <-- Pastikan impor utils ada
)

// --- MySQL Specific Fetching ---

func (s *SchemaSyncer) getMySQLColumns(ctx context.Context, db *gorm.DB, table string) ([]ColumnInfo, error) {
	var columnsData []struct {
		Field           string         `gorm:"column:COLUMN_NAME"`
		OrdinalPosition int            `gorm:"column:ORDINAL_POSITION"`
		Default         sql.NullString `gorm:"column:COLUMN_DEFAULT"`
		IsNullable      string         `gorm:"column:IS_NULLABLE"` // YES / NO
		Type            string         `gorm:"column:DATA_TYPE"`   // Mis. varchar, int
		FullType        string         `gorm:"column:COLUMN_TYPE"` // Mis. varchar(255), int(11)
		Length          sql.NullInt64  `gorm:"column:CHARACTER_MAXIMUM_LENGTH"`
		Precision       sql.NullInt64  `gorm:"column:NUMERIC_PRECISION"`
		Scale           sql.NullInt64  `gorm:"column:NUMERIC_SCALE"`
		Key             string         `gorm:"column:COLUMN_KEY"` // PRI, UNI, MUL
		Extra           string         `gorm:"column:EXTRA"`      // auto_increment, DEFAULT_GENERATED, VIRTUAL GENERATED, STORED GENERATED
		Collation       sql.NullString `gorm:"column:COLLATION_NAME"`
		Comment         sql.NullString `gorm:"column:COLUMN_COMMENT"`
		GenerationExpr  sql.NullString `gorm:"column:GENERATION_EXPRESSION"` // MySQL 5.7+
	}
	// Query information_schema, termasuk GENERATION_EXPRESSION jika ada
	query := `
		SELECT COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE,
		       COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
		       COLUMN_KEY, EXTRA, COLLATION_NAME, COLUMN_COMMENT,
		       CASE WHEN @@version NOT LIKE '5.6%' THEN GENERATION_EXPRESSION ELSE NULL END AS GENERATION_EXPRESSION
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`
	err := db.WithContext(ctx).Raw(query, table).Scan(&columnsData).Error
	if err != nil {
		// Cek apakah error karena kolom GENERATION_EXPRESSION tidak ada (MySQL < 5.7)
		if strings.Contains(err.Error(), "Unknown column 'GENERATION_EXPRESSION'") {
			s.logger.Debug("GENERATION_EXPRESSION column not found, likely MySQL < 5.7. Retrying without it.")
			queryLegacy := `
				SELECT COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE,
					   COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
					   COLUMN_KEY, EXTRA, COLLATION_NAME, COLUMN_COMMENT,
					   NULL AS GENERATION_EXPRESSION
				FROM information_schema.COLUMNS
				WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
				ORDER BY ORDINAL_POSITION
			`
			err = db.WithContext(ctx).Raw(queryLegacy, table).Scan(&columnsData).Error
			if err != nil {
				return nil, fmt.Errorf("mysql columns legacy query failed for %s: %w", table, err)
			}
		} else {
			return nil, fmt.Errorf("mysql columns query failed for %s: %w", table, err)
		}
	}

	result := make([]ColumnInfo, 0, len(columnsData))
	for _, c := range columnsData {
		isGenerated := strings.Contains(c.Extra, "GENERATED") || c.GenerationExpr.Valid
		colInfo := ColumnInfo{
			Name:            c.Field, Type: c.FullType, IsNullable: strings.ToUpper(c.IsNullable) == "YES",
			IsPrimary: c.Key == "PRI", DefaultValue: c.Default, AutoIncrement: strings.Contains(c.Extra, "auto_increment"),
			IsGenerated: isGenerated, OrdinalPosition: c.OrdinalPosition,
			Length: c.Length, Precision: c.Precision, Scale: c.Scale, Collation: c.Collation, Comment: c.Comment,
		}
		// Jangan petakan tipe jika generated (biarkan DB tujuan yang handle jika memungkinkan)
		var mappedType string
		var mapErr error
		if !isGenerated {
			mappedType, mapErr = s.mapDataType(colInfo.Type) // Use receiver s.
			if mapErr != nil { s.logger.Warn("Mapping failed", zap.String("col", c.Field), zap.Error(mapErr)); mappedType = colInfo.Type }
		} else {
			mappedType = colInfo.Type // Gunakan tipe asli untuk generated column
			s.logger.Warn("Mapping skipped for generated column", zap.String("col", c.Field), zap.String("type", colInfo.Type))
		}
		colInfo.MappedType = mappedType
		result = append(result, colInfo)
	}
	return result, nil
}


func (s *SchemaSyncer) getMySQLIndexes(ctx context.Context, db *gorm.DB, table string) ([]IndexInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "mysql"))
	log.Debug("Fetching MySQL indexes")
	indexes := make([]IndexInfo, 0)
	idxMap := make(map[string]*IndexInfo)

	var results []struct {
		Table      string `gorm:"column:Table"`
		NonUnique  int    `gorm:"column:Non_unique"`
		KeyName    string `gorm:"column:Key_name"`
		SeqInIndex int    `gorm:"column:Seq_in_index"`
		ColumnName string `gorm:"column:Column_name"`
		IndexType  string `gorm:"column:Index_type"`
	}
	query := fmt.Sprintf("SHOW INDEX FROM %s", utils.QuoteIdentifier(table, "mysql")) // Gunakan utils
	if err := db.WithContext(ctx).Raw(query).Scan(&results).Error; err != nil {
		if strings.Contains(err.Error(), "doesn't exist") { return indexes, nil }
		return nil, fmt.Errorf("mysql show index failed for %s: %w", table, err)
	}
	for _, r := range results {
		if _, ok := idxMap[r.KeyName]; !ok { idxMap[r.KeyName] = &IndexInfo{ Name: r.KeyName, Columns: make([]string, 0), IsUnique: r.NonUnique == 0, IsPrimary: r.KeyName == "PRIMARY", }}
		currentLen := len(idxMap[r.KeyName].Columns); neededLen := r.SeqInIndex
		if neededLen > currentLen { idxMap[r.KeyName].Columns = append(idxMap[r.KeyName].Columns, make([]string, neededLen-currentLen)...)}
		idxMap[r.KeyName].Columns[r.SeqInIndex-1] = r.ColumnName
	}

	for _, idx := range idxMap {
		sort.Strings(idx.Columns) // Gunakan sort
		indexes = append(indexes, *idx)
	}
	sort.Slice(indexes, func(i, j int) bool { return indexes[i].Name < indexes[j].Name }) // Gunakan sort
	return indexes, nil
}


func (s *SchemaSyncer) getMySQLConstraints(ctx context.Context, db *gorm.DB, table string) ([]ConstraintInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "mysql"))
	log.Debug("Fetching MySQL constraints")
	constraints := make([]ConstraintInfo, 0)
	consMap := make(map[string]*ConstraintInfo)

	var results []struct {
		ConstraintName    string         `gorm:"column:constraint_name"`
		ConstraintType    string         `gorm:"column:constraint_type"` // PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK
		ColumnName        string         `gorm:"column:column_name"`
		OrdinalPosition   sql.NullInt64  `gorm:"column:ordinal_position"`
		ForeignTableName  sql.NullString `gorm:"column:referenced_table_name"` // Note: column name difference from PG
		ForeignColumnName sql.NullString `gorm:"column:referenced_column_name"`
		UpdateRule        sql.NullString `gorm:"column:update_rule"`
		DeleteRule        sql.NullString `gorm:"column:delete_rule"`
		CheckClause       sql.NullString `gorm:"column:check_clause"` // From check_constraints table
	}

	// Query for PK, UNIQUE, FK
	queryCore := `
	SELECT DISTINCT tc.constraint_name, tc.constraint_type, kcu.column_name, kcu.ordinal_position,
	       kcu.referenced_table_name AS foreign_table_name, kcu.referenced_column_name AS foreign_column_name,
	       rc.update_rule, rc.delete_rule, NULL AS check_clause -- Check clause queried separately
	FROM information_schema.table_constraints tc
	JOIN information_schema.key_column_usage kcu
	    ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema AND tc.table_name = kcu.table_name
	LEFT JOIN information_schema.referential_constraints rc
	    ON tc.constraint_name = rc.constraint_name AND tc.table_schema = rc.constraint_schema
	WHERE tc.table_name = ? AND tc.table_schema = DATABASE()
	ORDER BY tc.constraint_name, kcu.ordinal_position;
	`
	err := db.WithContext(ctx).Raw(queryCore, table).Scan(&results).Error
	if err != nil {
		if strings.Contains(err.Error(), "doesn't exist") { return constraints, nil }
		return nil, fmt.Errorf("mysql core constraint query failed: %w", err)
	}

	// Query for CHECK constraints (MySQL 8.0.16+) separately
	var checkResults []struct {
		ConstraintName string `gorm:"column:CONSTRAINT_NAME"`
		CheckClause    string `gorm:"column:CHECK_CLAUSE"`
	}
	checkQuery := `
	SELECT CONSTRAINT_NAME, CHECK_CLAUSE
	FROM information_schema.CHECK_CONSTRAINTS
	WHERE TABLE_NAME = ? AND CONSTRAINT_SCHEMA = DATABASE();
	`
	var checkTableExists int
	errCheckTable := db.WithContext(ctx).Raw("SELECT COUNT(*) FROM information_schema.tables WHERE TABLE_SCHEMA = 'information_schema' AND TABLE_NAME = 'CHECK_CONSTRAINTS'").Scan(&checkTableExists).Error
	if errCheckTable == nil && checkTableExists > 0 {
		errCheck := db.WithContext(ctx).Raw(checkQuery, table).Scan(&checkResults).Error
		if errCheck != nil { log.Warn("Failed to query MySQL CHECK_CONSTRAINTS", zap.Error(errCheck)) }
	} else if errCheckTable != nil { log.Warn("Failed to check CHECK_CONSTRAINTS existence", zap.Error(errCheckTable)) } else { log.Debug("CHECK_CONSTRAINTS table not found") }


	// Process results
	tempFKCols := make(map[string][]struct{ Local, Foreign string; Seq int64 })
	checkMap := make(map[string]string); for _, c := range checkResults { checkMap[c.ConstraintName] = c.CheckClause }

	for _, r := range results {
		if _, ok := consMap[r.ConstraintName]; !ok { consMap[r.ConstraintName] = &ConstraintInfo{ Name: r.ConstraintName, Type: r.ConstraintType, Columns: make([]string, 0), ForeignTable: r.ForeignTableName.String, ForeignColumns: make([]string, 0), OnDelete: r.DeleteRule.String, OnUpdate: r.UpdateRule.String, Definition: checkMap[r.ConstraintName], }; if r.ConstraintType == "FOREIGN KEY" { tempFKCols[r.ConstraintName] = make([]struct{ Local, Foreign string; Seq int64 }, 0) } }
		currentLen := len(consMap[r.ConstraintName].Columns); neededLen := 1; if r.OrdinalPosition.Valid { neededLen = int(r.OrdinalPosition.Int64) }
		if neededLen > currentLen { consMap[r.ConstraintName].Columns = append(consMap[r.ConstraintName].Columns, make([]string, neededLen-currentLen)...) }
		if neededLen > 0 { consMap[r.ConstraintName].Columns[neededLen-1] = r.ColumnName } else { consMap[r.ConstraintName].Columns = append(consMap[r.ConstraintName].Columns, r.ColumnName) }
		if r.ConstraintType == "FOREIGN KEY" && r.ForeignColumnName.Valid { seq := int64(1); if r.OrdinalPosition.Valid { seq = r.OrdinalPosition.Int64 }; tempFKCols[r.ConstraintName] = append(tempFKCols[r.ConstraintName], struct{ Local, Foreign string; Seq int64 }{r.ColumnName, r.ForeignColumnName.String, seq}) }
	}
	for name, fkCons := range consMap {
		if fkCons.Type == "FOREIGN KEY" {
			fkCols := tempFKCols[name]; sort.Slice(fkCols, func(i, j int) bool { return fkCols[i].Seq < fkCols[j].Seq }); fkCons.ForeignColumns = make([]string, len(fkCols)); fkCons.Columns = make([]string, len(fkCols))
			for i, c := range fkCols { fkCons.Columns[i] = c.Local; fkCons.ForeignColumns[i] = c.Foreign }
		} else { sort.Strings(fkCons.Columns) } // Gunakan sort
		cleanedCols := make([]string, 0, len(fkCons.Columns)); for _, c := range fkCons.Columns { if c != "" { cleanedCols = append(cleanedCols, c) } }; fkCons.Columns = cleanedCols
	}

	for _, cons := range consMap { constraints = append(constraints, *cons) }
	sort.Slice(constraints, func(i, j int) bool { return constraints[i].Name < constraints[j].Name }) // Gunakan sort
	return constraints, nil
}