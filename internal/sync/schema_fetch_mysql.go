package sync

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/arwahdevops/dbsync/internal/utils" // Diperlukan untuk QuoteIdentifier
)

// getMySQLColumns mengambil informasi kolom untuk sebuah tabel dari MySQL.
func (s *SchemaSyncer) getMySQLColumns(ctx context.Context, db *gorm.DB, table string) ([]ColumnInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "mysql"), zap.String("action", "getMySQLColumns"))
	var columnsData []struct {
		Field           string         `gorm:"column:COLUMN_NAME"`
		OrdinalPosition int            `gorm:"column:ORDINAL_POSITION"`
		Default         sql.NullString `gorm:"column:COLUMN_DEFAULT"`
		IsNullable      string         `gorm:"column:IS_NULLABLE"`
		Type            string         `gorm:"column:DATA_TYPE"`
		FullType        string         `gorm:"column:COLUMN_TYPE"`
		Length          sql.NullInt64  `gorm:"column:CHARACTER_MAXIMUM_LENGTH"`
		Precision       sql.NullInt64  `gorm:"column:NUMERIC_PRECISION"`
		Scale           sql.NullInt64  `gorm:"column:NUMERIC_SCALE"`
		Key             string         `gorm:"column:COLUMN_KEY"` // PRI, UNI, MUL
		Extra           string         `gorm:"column:EXTRA"`
		Collation       sql.NullString `gorm:"column:COLLATION_NAME"`
		Comment         sql.NullString `gorm:"column:COLUMN_COMMENT"`
		GenerationExpr  sql.NullString `gorm:"column:GENERATION_EXPRESSION"`
	}

	var version string
	errVersion := db.WithContext(ctx).Raw("SELECT VERSION()").Scan(&version).Error
	canQueryGenExpr := false
	if errVersion == nil {
		if !strings.HasPrefix(version, "5.6.") { // GENERATION_EXPRESSION ada di MySQL 5.7+
			canQueryGenExpr = true
		}
	} else {
		log.Warn("Could not determine MySQL version, attempting query with GENERATION_EXPRESSION by default.", zap.Error(errVersion))
		canQueryGenExpr = true // Default untuk mencoba, dengan fallback jika gagal
	}

	var sqlQuery string
	if canQueryGenExpr {
		sqlQuery = `
			SELECT COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE,
				   COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
				   COLUMN_KEY, EXTRA, COLLATION_NAME, COLUMN_COMMENT,
				   GENERATION_EXPRESSION
			FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
			ORDER BY ORDINAL_POSITION;`
	} else {
		sqlQuery = `
			SELECT COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE,
				   COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
				   COLUMN_KEY, EXTRA, COLLATION_NAME, COLUMN_COMMENT,
				   NULL AS GENERATION_EXPRESSION
			FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
			ORDER BY ORDINAL_POSITION;`
	}

	log.Debug("Executing column info query", zap.Bool("with_generation_expression", canQueryGenExpr), zap.String("query_preview", strings.Split(sqlQuery, "\n")[1]))
	err := db.WithContext(ctx).Raw(sqlQuery, table).Scan(&columnsData).Error

	if err != nil {
		if err == gorm.ErrRecordNotFound ||
			strings.Contains(strings.ToLower(err.Error()), "doesn't exist") ||
			strings.Contains(strings.ToLower(err.Error()), "unknown table") {
			log.Warn("Table not found or column query returned no rows.", zap.String("table", table))
			return []ColumnInfo{}, nil
		}
		if canQueryGenExpr && strings.Contains(strings.ToLower(err.Error()), "unknown column 'generation_expression'") {
			log.Warn("Column query failed with 'GENERATION_EXPRESSION', retrying without it.", zap.String("table", table))
			queryLegacy := `
				SELECT COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE,
					   COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
					   COLUMN_KEY, EXTRA, COLLATION_NAME, COLUMN_COMMENT,
					   NULL AS GENERATION_EXPRESSION
				FROM information_schema.COLUMNS
				WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
				ORDER BY ORDINAL_POSITION;`
			err = db.WithContext(ctx).Raw(queryLegacy, table).Scan(&columnsData).Error
			if err != nil {
				if err == gorm.ErrRecordNotFound {
					log.Warn("Table not found on legacy column query retry.", zap.String("table", table))
					return []ColumnInfo{}, nil
				}
				return nil, fmt.Errorf("mysql columns legacy query failed for table '%s': %w", table, err)
			}
		} else {
			return nil, fmt.Errorf("mysql columns query failed for table '%s': %w", table, err)
		}
	}
	if len(columnsData) == 0 && err == nil {
		log.Warn("Column query succeeded but returned no rows. Assuming table does not exist or has no columns.", zap.String("table", table))
		return []ColumnInfo{}, nil
	}

	result := make([]ColumnInfo, 0, len(columnsData))
	for _, c := range columnsData {
		isGenerated := (strings.Contains(strings.ToUpper(c.Extra), "VIRTUAL GENERATED") ||
			strings.Contains(strings.ToUpper(c.Extra), "STORED GENERATED")) ||
			(c.GenerationExpr.Valid && c.GenerationExpr.String != "")
		isAutoIncrement := strings.Contains(strings.ToLower(c.Extra), "auto_increment")
		colInfo := ColumnInfo{
			Name:            c.Field,
			Type:            c.FullType,
			MappedType:      "", // Akan diisi oleh SchemaSyncer
			IsNullable:      strings.ToUpper(c.IsNullable) == "YES",
			IsPrimary:       strings.ToUpper(c.Key) == "PRI",
			IsGenerated:     isGenerated,
			DefaultValue:    c.Default,
			AutoIncrement:   isAutoIncrement,
			OrdinalPosition: c.OrdinalPosition,
			Length:          c.Length,
			Precision:       c.Precision,
			Scale:           c.Scale,
			Collation:       c.Collation,
			Comment:         c.Comment,
		}
		result = append(result, colInfo)
	}
	log.Debug("Fetched column info successfully.", zap.Int("column_count", len(result)))
	return result, nil
}

// getMySQLIndexes mengambil informasi indeks untuk sebuah tabel dari MySQL.
func (s *SchemaSyncer) getMySQLIndexes(ctx context.Context, db *gorm.DB, table string) ([]IndexInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "mysql"), zap.String("action", "getMySQLIndexes"))

	var tableExists int64
	errTableCheck := db.WithContext(ctx).Raw("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?", table).Scan(&tableExists).Error
	if errTableCheck != nil {
		return nil, fmt.Errorf("failed to check existence of table '%s' before fetching indexes: %w", table, errTableCheck)
	}
	if tableExists == 0 {
		log.Warn("Table not found, cannot fetch indexes.", zap.String("table", table))
		return []IndexInfo{}, nil
	}

	var results []struct {
		NonUnique  int    `gorm:"column:Non_unique"`
		KeyName    string `gorm:"column:Key_name"`
		SeqInIndex int    `gorm:"column:Seq_in_index"`
		ColumnName string `gorm:"column:Column_name"`
		IndexType  string `gorm:"column:Index_type"`
		Comment    string `gorm:"column:Index_comment"`
	}

	showIndexQuery := fmt.Sprintf("SHOW INDEX FROM %s", utils.QuoteIdentifier(table, "mysql"))

	log.Debug("Executing SHOW INDEX query", zap.String("query", showIndexQuery))
	err := db.WithContext(ctx).Raw(showIndexQuery).Scan(&results).Error
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "doesn't exist") || strings.Contains(strings.ToLower(err.Error()), "unknown table") {
			log.Warn("Table not found when executing SHOW INDEX (double check).", zap.String("table", table))
			return []IndexInfo{}, nil
		}
		return nil, fmt.Errorf("mysql SHOW INDEX failed for table '%s': %w", table, err)
	}

	if len(results) == 0 {
		log.Debug("No indexes found for table.")
		return []IndexInfo{}, nil
	}

	idxColsMap := make(map[string][]struct{ Seq int; ColName string })
	idxDetailsMap := make(map[string]IndexInfo)

	for _, r := range results {
		if _, ok := idxDetailsMap[r.KeyName]; !ok {
			idxDetailsMap[r.KeyName] = IndexInfo{
				Name:      r.KeyName,
				IsUnique:  r.NonUnique == 0,
				IsPrimary: r.KeyName == "PRIMARY",
				RawDef:    fmt.Sprintf("TYPE %s COMMENT '%s'", r.IndexType, r.Comment),
				Columns:   make([]string, 0),
			}
		}
		idxColsMap[r.KeyName] = append(idxColsMap[r.KeyName], struct{ Seq int; ColName string }{
			Seq:     r.SeqInIndex,
			ColName: r.ColumnName,
		})
	}

	indexes := make([]IndexInfo, 0, len(idxDetailsMap))
	for keyName, idxInfo := range idxDetailsMap {
		colsWithSeq := idxColsMap[keyName]
		sort.Slice(colsWithSeq, func(i, j int) bool {
			return colsWithSeq[i].Seq < colsWithSeq[j].Seq
		})
		for _, colData := range colsWithSeq {
			idxInfo.Columns = append(idxInfo.Columns, colData.ColName)
		}
		indexes = append(indexes, idxInfo)
	}

	sort.Slice(indexes, func(i, j int) bool { return indexes[i].Name < indexes[j].Name })
	log.Debug("Fetched index info successfully.", zap.Int("index_count", len(indexes)))
	return indexes, nil
}

// getMySQLConstraints mengambil informasi constraint untuk sebuah tabel dari MySQL.
func (s *SchemaSyncer) getMySQLConstraints(ctx context.Context, db *gorm.DB, table string) ([]ConstraintInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "mysql"), zap.String("action", "getMySQLConstraints"))

	var tableExists int64
	errTableCheck := db.WithContext(ctx).Raw("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?", table).Scan(&tableExists).Error
	if errTableCheck != nil {
		return nil, fmt.Errorf("failed to check existence of table '%s' before fetching constraints: %w", table, errTableCheck)
	}
	if tableExists == 0 {
		log.Warn("Table not found, cannot fetch constraints.", zap.String("table", table))
		return []ConstraintInfo{}, nil
	}

	var tcResults []struct {
		ConstraintName    string         `gorm:"column:CONSTRAINT_NAME"`
		ConstraintType    string         `gorm:"column:CONSTRAINT_TYPE"`
		TableName         string         `gorm:"column:TABLE_NAME"`      // Untuk verifikasi, meskipun sudah filter by table
		ColumnName        sql.NullString `gorm:"column:COLUMN_NAME"`     // Bisa NULL untuk CHECK
		OrdinalPosition   sql.NullInt64  `gorm:"column:ORDINAL_POSITION"`// Urutan kolom dalam constraint
		ForeignTableName  sql.NullString `gorm:"column:REFERENCED_TABLE_NAME"`
		ForeignColumnName sql.NullString `gorm:"column:REFERENCED_COLUMN_NAME"`
		UpdateRule        sql.NullString `gorm:"column:UPDATE_RULE"`
		DeleteRule        sql.NullString `gorm:"column:DELETE_RULE"`
	}

	// Query TABLE_CONSTRAINTS di-join dengan KEY_COLUMN_USAGE dan REFERENTIAL_CONSTRAINTS
	queryCore := `
		SELECT
			tc.CONSTRAINT_NAME,
			tc.CONSTRAINT_TYPE,
			tc.TABLE_NAME,
			kcu.COLUMN_NAME,
			kcu.ORDINAL_POSITION,
			kcu.REFERENCED_TABLE_NAME,
			kcu.REFERENCED_COLUMN_NAME,
			rc.UPDATE_RULE,
			rc.DELETE_RULE
		FROM information_schema.TABLE_CONSTRAINTS tc
		LEFT JOIN information_schema.KEY_COLUMN_USAGE kcu
			ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
			AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
			AND tc.TABLE_NAME = kcu.TABLE_NAME 
		LEFT JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
			ON tc.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
			AND tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
			AND tc.TABLE_NAME = rc.TABLE_NAME
		WHERE tc.TABLE_SCHEMA = DATABASE() AND tc.TABLE_NAME = ?
		ORDER BY tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION;`

	err := db.WithContext(ctx).Raw(queryCore, table).Scan(&tcResults).Error
	if err != nil {
		// Error tabel tidak ada seharusnya sudah ditangani, ini untuk error query lain
		return nil, fmt.Errorf("mysql core constraints query failed for table '%s': %w", table, err)
	}

	// Peta untuk menyimpan CHECK_CLAUSE
	checkClauses := make(map[string]string)
	var checkTableExistsCount int
	// Periksa apakah tabel CHECK_CONSTRAINTS ada (MySQL 8.0.16+)
	errCheckTable := db.WithContext(ctx).Raw("SELECT COUNT(*) FROM information_schema.tables WHERE TABLE_SCHEMA = 'information_schema' AND TABLE_NAME = 'CHECK_CONSTRAINTS'").Scan(&checkTableExistsCount).Error

	if errCheckTable == nil && checkTableExistsCount > 0 {
		var rawCheckConstraints []struct {
			ConstraintName string `gorm:"column:CONSTRAINT_NAME"`
			CheckClause    string `gorm:"column:CHECK_CLAUSE"`
		}
		// Query CHECK_CONSTRAINTS, join dengan TABLE_CONSTRAINTS untuk filter berdasarkan TABLE_NAME
		checkQuery := `
            SELECT cc.CONSTRAINT_NAME, cc.CHECK_CLAUSE
            FROM information_schema.CHECK_CONSTRAINTS cc
            JOIN information_schema.TABLE_CONSTRAINTS tc
                ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            WHERE tc.TABLE_SCHEMA = DATABASE() AND tc.TABLE_NAME = ? AND tc.CONSTRAINT_TYPE = 'CHECK';`
		errCheck := db.WithContext(ctx).Raw(checkQuery, table).Scan(&rawCheckConstraints).Error
		if errCheck != nil && errCheck != gorm.ErrRecordNotFound {
			log.Warn("Failed to query MySQL CHECK_CONSTRAINTS table. CHECK constraint definitions might be missed.", zap.Error(errCheck))
		} else {
			for _, c := range rawCheckConstraints {
				checkClauses[c.ConstraintName] = c.CheckClause
			}
		}
	} else if errCheckTable != nil {
		log.Warn("Failed to check for CHECK_CONSTRAINTS table existence.", zap.Error(errCheckTable))
	} else {
		log.Debug("CHECK_CONSTRAINTS table not found (likely MySQL < 8.0.16).")
	}

	// Proses hasil query tcResults dan gabungkan dengan checkClauses
	consMap := make(map[string]*ConstraintInfo)
	// Struktur sementara untuk mengumpulkan kolom sebelum diurutkan
	colsForConstraint := make(map[string][]struct{ Seq int64; Name string })        // Untuk PK, UNIQUE
	fkColsForConstraint := make(map[string][]struct{ Seq int64; Local, Foreign string }) // Untuk FK

	for _, r := range tcResults {
		constraintName := r.ConstraintName
		constraintType := strings.ToUpper(r.ConstraintType) // Normalisasi tipe

		if _, ok := consMap[constraintName]; !ok {
			consMap[constraintName] = &ConstraintInfo{
				Name:           constraintName,
				Type:           constraintType,
				Columns:        []string{}, // Akan diisi setelah pengurutan
				ForeignTable:   r.ForeignTableName.String,
				ForeignColumns: []string{}, // Akan diisi setelah pengurutan
				OnDelete:       r.DeleteRule.String,
				OnUpdate:       r.UpdateRule.String,
			}
			if constraintType == "CHECK" {
				if clause, found := checkClauses[constraintName]; found {
					consMap[constraintName].Definition = clause
				} else {
					// Ini bisa terjadi jika CHECK constraint didefinisikan inline saat CREATE TABLE
					// dan versi MySQL < 8.0.16 (sehingga tidak ada di CHECK_CONSTRAINTS)
					// atau jika ada masalah dengan query CHECK_CONSTRAINTS.
					log.Info("CHECK constraint found in TABLE_CONSTRAINTS but no explicit definition retrieved from CHECK_CONSTRAINTS.",
						zap.String("constraint_name", constraintName),
						zap.String("table", table))
					// Definition akan tetap kosong, yang menandakan perlu perbandingan lebih lanjut jika memungkinkan
				}
			}
		}

		// Kumpulkan kolom jika ada
		if r.ColumnName.Valid && r.ColumnName.String != "" {
			seq := int64(1) // Default jika OrdinalPosition NULL
			if r.OrdinalPosition.Valid {
				seq = r.OrdinalPosition.Int64
			}

			if constraintType == "FOREIGN KEY" {
				if r.ForeignColumnName.Valid {
					fkColsForConstraint[constraintName] = append(fkColsForConstraint[constraintName], struct{ Seq int64; Local, Foreign string }{
						Seq: seq, Local: r.ColumnName.String, Foreign: r.ForeignColumnName.String,
					})
				}
			} else if constraintType == "PRIMARY KEY" || constraintType == "UNIQUE" {
				colsForConstraint[constraintName] = append(colsForConstraint[constraintName], struct{ Seq int64; Name string }{
					Seq: seq, Name: r.ColumnName.String,
				})
			}
		}
	}

	// Finalisasi kolom untuk setiap constraint dengan pengurutan yang benar
	constraints := make([]ConstraintInfo, 0, len(consMap))
	for name, cons := range consMap {
		if cons.Type == "FOREIGN KEY" {
			colPairs := fkColsForConstraint[name]
			sort.Slice(colPairs, func(i, j int) bool { return colPairs[i].Seq < colPairs[j].Seq })
			cons.Columns = make([]string, len(colPairs))
			cons.ForeignColumns = make([]string, len(colPairs))
			for i, pair := range colPairs {
				cons.Columns[i] = pair.Local
				cons.ForeignColumns[i] = pair.Foreign
			}
		} else if cons.Type == "PRIMARY KEY" || cons.Type == "UNIQUE" {
			cols := colsForConstraint[name]
			sort.Slice(cols, func(i, j int) bool { return cols[i].Seq < cols[j].Seq })
			cons.Columns = make([]string, len(cols))
			for i, c := range cols {
				cons.Columns[i] = c.Name
			}
		}
		// Untuk CHECK, cons.Columns tetap kosong, yang valid.
		constraints = append(constraints, *cons)
	}

	// Urutkan slice constraint akhir berdasarkan nama
	sort.Slice(constraints, func(i, j int) bool { return constraints[i].Name < constraints[j].Name })
	log.Debug("Fetched constraint info successfully.", zap.Int("constraint_count", len(constraints)))
	return constraints, nil
}
