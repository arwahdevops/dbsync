// internal/sync/schema_fetch_mysql.go
package sync

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/arwahdevops/dbsync/internal/utils"
)

func (s *SchemaSyncer) getMySQLColumns(ctx context.Context, db *gorm.DB, table string) ([]ColumnInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "mysql"), zap.String("action", "getMySQLColumns"))
	var columnsData []struct {
		Field           string         `gorm:"column:COLUMN_NAME"`
		OrdinalPosition int            `gorm:"column:ORDINAL_POSITION"`
		Default         sql.NullString `gorm:"column:COLUMN_DEFAULT"`
		IsNullable      string         `gorm:"column:IS_NULLABLE"`
		// Type            string         `gorm:"column:DATA_TYPE"` // DATA_TYPE kurang lengkap untuk MySQL
		FullType       string         `gorm:"column:COLUMN_TYPE"` // COLUMN_TYPE lebih baik, misal: varchar(255), int(11) unsigned
		Length         sql.NullInt64  `gorm:"column:CHARACTER_MAXIMUM_LENGTH"`
		Precision      sql.NullInt64  `gorm:"column:NUMERIC_PRECISION"`
		Scale          sql.NullInt64  `gorm:"column:NUMERIC_SCALE"`
		Key            string         `gorm:"column:COLUMN_KEY"`
		Extra          string         `gorm:"column:EXTRA"`
		Collation      sql.NullString `gorm:"column:COLLATION_NAME"`
		Comment        sql.NullString `gorm:"column:COLUMN_COMMENT"`
		GenerationExpr sql.NullString `gorm:"column:GENERATION_EXPRESSION"`
	}

	var version string
	errVersion := db.WithContext(ctx).Raw("SELECT VERSION()").Scan(&version).Error
	canQueryGenExpr := false // Default ke false
	if errVersion == nil {
		// GENERATION_EXPRESSION ada di MySQL 5.7+ dan MariaDB 10.2+
		// Cek versi sederhana. Untuk MariaDB, versi string bisa "10.2.X-MariaDB..."
		if !strings.HasPrefix(version, "5.6.") && !strings.HasPrefix(version, "5.5.") {
			canQueryGenExpr = true
		}
	} else {
		log.Warn("Could not determine MySQL/MariaDB version, attempting query with GENERATION_EXPRESSION by default. This might fail on older versions.", zap.Error(errVersion))
		canQueryGenExpr = true // Coba saja, dengan fallback
	}

	var sqlQuery string
	if canQueryGenExpr {
		sqlQuery = `
			SELECT COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, 
			       COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
			       COLUMN_KEY, EXTRA, COLLATION_NAME, COLUMN_COMMENT,
			       GENERATION_EXPRESSION
			FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
			ORDER BY ORDINAL_POSITION;`
	} else {
		log.Info("MySQL/MariaDB version likely < 5.7/10.2, querying columns without GENERATION_EXPRESSION field.")
		sqlQuery = `
			SELECT COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, 
			       COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
			       COLUMN_KEY, EXTRA, COLLATION_NAME, COLUMN_COMMENT,
			       NULL AS GENERATION_EXPRESSION
			FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
			ORDER BY ORDINAL_POSITION;`
	}

	log.Debug("Executing column info query.", zap.Bool("with_generation_expression_field", canQueryGenExpr))
	err := db.WithContext(ctx).Raw(sqlQuery, table).Scan(&columnsData).Error

	if err != nil {
		// Jika query utama gagal KARENA kolom GENERATION_EXPRESSION tidak ada (di versi lama)
		if canQueryGenExpr && (strings.Contains(strings.ToLower(err.Error()), "unknown column") && strings.Contains(strings.ToLower(err.Error()), "generation_expression")) {
			log.Warn("Column query failed with 'GENERATION_EXPRESSION', retrying without it (likely older MySQL/MariaDB).", zap.String("table", table))
			// Query fallback tanpa GENERATION_EXPRESSION
			sqlQuery = `
				SELECT COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, 
				       COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
				       COLUMN_KEY, EXTRA, COLLATION_NAME, COLUMN_COMMENT,
				       NULL AS GENERATION_EXPRESSION
				FROM information_schema.COLUMNS
				WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
				ORDER BY ORDINAL_POSITION;`
			err = db.WithContext(ctx).Raw(sqlQuery, table).Scan(&columnsData).Error
		}
		// Setelah potensi retry, cek error lagi
		if err != nil {
			if err == gorm.ErrRecordNotFound || isTableNotExistError(err, "mysql") {
				log.Warn("Table not found or column query returned no rows.", zap.String("table", table))
				return []ColumnInfo{}, nil // Bukan error aplikasi jika tabel tidak ada
			}
			return nil, fmt.Errorf("mysql columns query failed for table '%s': %w", table, err)
		}
	}

	if len(columnsData) == 0 { // Bisa terjadi jika tabel ada tapi tidak punya kolom (jarang) atau jika query filter lain
		log.Warn("Column query succeeded but returned no rows. Assuming table has no columns or was filtered out.", zap.String("table", table))
		return []ColumnInfo{}, nil
	}

	result := make([]ColumnInfo, 0, len(columnsData))
	for _, c := range columnsData {
		isGenerated := (strings.Contains(strings.ToUpper(c.Extra), "VIRTUAL GENERATED") ||
			strings.Contains(strings.ToUpper(c.Extra), "STORED GENERATED")) ||
			(c.GenerationExpr.Valid && c.GenerationExpr.String != "")
		isAutoIncrement := strings.Contains(strings.ToLower(c.Extra), "auto_increment")

		colInfo := ColumnInfo{
			Name:                 c.Field,
			Type:                 c.FullType, // Menggunakan COLUMN_TYPE yang lebih lengkap
			MappedType:           "",         // Akan diisi oleh pemanggil
			IsNullable:           strings.ToUpper(c.IsNullable) == "YES",
			IsPrimary:            strings.ToUpper(c.Key) == "PRI",
			IsGenerated:          isGenerated,
			DefaultValue:         c.Default,
			AutoIncrement:        isAutoIncrement,
			OrdinalPosition:      c.OrdinalPosition,
			Length:               c.Length,
			Precision:            c.Precision,
			Scale:                c.Scale,
			Collation:            c.Collation,
			Comment:              c.Comment,
			GenerationExpression: c.GenerationExpr,
		}
		result = append(result, colInfo)
	}
	log.Debug("Fetched MySQL column info successfully.", zap.Int("column_count", len(result)))
	return result, nil
}

func (s *SchemaSyncer) getMySQLIndexes(ctx context.Context, db *gorm.DB, table string) ([]IndexInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "mysql"), zap.String("action", "getMySQLIndexes"))

	// Cek dulu apakah tabel ada, agar tidak error saat SHOW INDEX jika tabel tidak ada
	var tableExists int64
	errTableCheck := db.WithContext(ctx).Raw("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?", table).Scan(&tableExists).Error
	if errTableCheck != nil {
		return nil, fmt.Errorf("failed to check existence of MySQL table '%s' before fetching indexes: %w", table, errTableCheck)
	}
	if tableExists == 0 {
		log.Warn("Table not found, cannot fetch indexes.", zap.String("table", table))
		return []IndexInfo{}, nil // Tabel tidak ada, kembalikan slice kosong
	}

	var results []struct {
		NonUnique  int    `gorm:"column:Non_unique"`
		KeyName    string `gorm:"column:Key_name"`
		SeqInIndex int    `gorm:"column:Seq_in_index"`
		ColumnName string `gorm:"column:Column_name"`
		IndexType  string `gorm:"column:Index_type"` // BTREE, FULLTEXT, HASH, SPATIAL
		Comment    string `gorm:"column:Index_comment"`
		// Expression sql.NullString `gorm:"column:Expression"` // Untuk indeks fungsional (MySQL 8.0.13+)
	}

	// Query SHOW INDEX. Untuk indeks fungsional, kolom 'Expression' perlu ditambahkan.
	// Kita perlu cek versi MySQL untuk 'Expression'.
	// Untuk penyederhanaan, kita tidak akan mengambil 'Expression' saat ini,
	// tapi 'RawDef' bisa diisi dengan info tipe jika relevan (FULLTEXT, SPATIAL).
	showIndexQuery := fmt.Sprintf("SHOW INDEX FROM %s", utils.QuoteIdentifier(table, "mysql"))

	log.Debug("Executing SHOW INDEX query.", zap.String("query_preview", showIndexQuery))
	err := db.WithContext(ctx).Raw(showIndexQuery).Scan(&results).Error
	if err != nil {
		// Error "doesn't exist" seharusnya sudah ditangani oleh cek di atas,
		// tapi sebagai fallback jika ada kondisi balapan atau error lain.
		if isTableNotExistError(err, "mysql") {
			log.Warn("Table not found when executing SHOW INDEX (double check).", zap.String("table", table))
			return []IndexInfo{}, nil
		}
		return nil, fmt.Errorf("mysql SHOW INDEX failed for table '%s': %w", table, err)
	}

	if len(results) == 0 {
		log.Debug("No indexes found for table.")
		return []IndexInfo{}, nil
	}

	idxColsMap := make(map[string][]struct {
		Seq     int
		ColName string
	})
	idxDetailsMap := make(map[string]IndexInfo)

	for _, r := range results {
		if _, ok := idxDetailsMap[r.KeyName]; !ok {
			rawDefParts := []string{}
			if r.IndexType != "" && strings.ToUpper(r.IndexType) != "BTREE" { // BTREE adalah default
				rawDefParts = append(rawDefParts, "TYPE "+r.IndexType)
			}
			if r.Comment != "" {
				rawDefParts = append(rawDefParts, "COMMENT '"+strings.ReplaceAll(r.Comment, "'", "''")+"'")
			}
			idxDetailsMap[r.KeyName] = IndexInfo{
				Name:      r.KeyName,
				IsUnique:  r.NonUnique == 0,
				IsPrimary: r.KeyName == "PRIMARY", // "PRIMARY" adalah nama standar untuk PK index di MySQL
				RawDef:    strings.Join(rawDefParts, " "),
				Columns:   make([]string, 0), // Akan diisi setelah diurutkan
			}
		}
		idxColsMap[r.KeyName] = append(idxColsMap[r.KeyName], struct {
			Seq     int
			ColName string
		}{
			Seq:     r.SeqInIndex,
			ColName: r.ColumnName,
		})
	}

	indexes := make([]IndexInfo, 0, len(idxDetailsMap))
	for keyName, idxInfo := range idxDetailsMap {
		colsWithSeq := idxColsMap[keyName]
		// Urutkan kolom berdasarkan SeqInIndex (penting untuk kunci komposit)
		sort.Slice(colsWithSeq, func(i, j int) bool {
			return colsWithSeq[i].Seq < colsWithSeq[j].Seq
		})
		for _, colData := range colsWithSeq {
			idxInfo.Columns = append(idxInfo.Columns, colData.ColName)
		}
		indexes = append(indexes, idxInfo)
	}

	// Urutkan slice indeks berdasarkan nama untuk output yang konsisten
	sort.Slice(indexes, func(i, j int) bool { return indexes[i].Name < indexes[j].Name })
	log.Debug("Fetched MySQL index info successfully.", zap.Int("index_count", len(indexes)))
	return indexes, nil
}

func (s *SchemaSyncer) getMySQLConstraints(ctx context.Context, db *gorm.DB, table string) ([]ConstraintInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "mysql"), zap.String("action", "getMySQLConstraints"))

	var tableExists int64
	errTableCheck := db.WithContext(ctx).Raw("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?", table).Scan(&tableExists).Error
	if errTableCheck != nil {
		return nil, fmt.Errorf("failed to check existence of MySQL table '%s' before fetching constraints: %w", table, errTableCheck)
	}
	if tableExists == 0 {
		log.Warn("Table not found, cannot fetch constraints.", zap.String("table", table))
		return []ConstraintInfo{}, nil
	}

	var tcResults []struct {
		ConstraintName    string         `gorm:"column:CONSTRAINT_NAME"`
		ConstraintType    string         `gorm:"column:CONSTRAINT_TYPE"` // PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK
		TableName         string         `gorm:"column:TABLE_NAME"`
		ColumnName        sql.NullString `gorm:"column:COLUMN_NAME"`      // Untuk PK, UNIQUE, FK (satu kolom per baris)
		OrdinalPosition   sql.NullInt64  `gorm:"column:ORDINAL_POSITION"` // Urutan kolom dalam constraint
		ForeignTableName  sql.NullString `gorm:"column:REFERENCED_TABLE_NAME"`
		ForeignColumnName sql.NullString `gorm:"column:REFERENCED_COLUMN_NAME"`
		UpdateRule        sql.NullString `gorm:"column:UPDATE_RULE"` // Untuk FK
		DeleteRule        sql.NullString `gorm:"column:DELETE_RULE"` // Untuk FK
	}

	// Query KEY_COLUMN_USAGE untuk mendapatkan kolom constraint, dan REFERENTIAL_CONSTRAINTS untuk detail FK
	// TABLE_CONSTRAINTS memberikan tipe constraint.
	// CHECK constraints ada di information_schema.CHECK_CONSTRAINTS (MySQL 8.0.16+)
	queryCore := `
		SELECT
			tc.CONSTRAINT_NAME,
			tc.CONSTRAINT_TYPE,
			tc.TABLE_NAME,
			kcu.COLUMN_NAME,
			kcu.ORDINAL_POSITION,
			rc.REFERENCED_TABLE_NAME, -- Dari rc untuk FK
			rc.UNIQUE_CONSTRAINT_NAME AS REFERENCED_COLUMN_NAME, -- Placeholder, akan diambil dari kcu untuk kolom referensi
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
			-- AND tc.TABLE_NAME = rc.TABLE_NAME -- Tidak perlu join TABLE_NAME di sini untuk rc
		WHERE tc.TABLE_SCHEMA = DATABASE() AND tc.TABLE_NAME = ?
		ORDER BY tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION;`
	// ORDINAL_POSITION di kcu adalah kunci untuk urutan kolom

	err := db.WithContext(ctx).Raw(queryCore, table).Scan(&tcResults).Error
	if err != nil {
		if isTableNotExistError(err, "mysql") { // Redundan jika cek di atas berhasil, tapi sbg fallback
			log.Warn("Table not found during constraints query (double check).", zap.String("table", table))
			return []ConstraintInfo{}, nil
		}
		return nil, fmt.Errorf("mysql core constraints query failed for table '%s': %w", table, err)
	}

	// Ambil kolom referensi FK secara terpisah karena KEY_COLUMN_USAGE tidak langsung memberikannya dengan baik
	fkReferencedColsMap := make(map[string][]string)
	if len(tcResults) > 0 { // Hanya query jika ada constraint yang mungkin FK
		var fkRefResults []struct {
			ConstraintName       string `gorm:"column:CONSTRAINT_NAME"`
			ReferencedColumnName string `gorm:"column:REFERENCED_COLUMN_NAME"`
			OrdinalPosition      int    `gorm:"column:ORDINAL_POSITION"` // Untuk mengurutkan kolom referensi
		}
		// Query untuk mendapatkan kolom yang direferensikan oleh FK, diurutkan
		queryFkRefCols := `
			SELECT
				kcu.CONSTRAINT_NAME, 
				kcu.REFERENCED_COLUMN_NAME,
				kcu.POSITION_IN_UNIQUE_CONSTRAINT AS ORDINAL_POSITION 
			FROM information_schema.KEY_COLUMN_USAGE kcu
			JOIN information_schema.TABLE_CONSTRAINTS tc 
				ON kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA 
				AND kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
				AND kcu.TABLE_NAME = tc.TABLE_NAME
			WHERE kcu.CONSTRAINT_SCHEMA = DATABASE() 
			  AND kcu.TABLE_NAME = ?
			  AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
			  AND kcu.REFERENCED_COLUMN_NAME IS NOT NULL
			ORDER BY kcu.CONSTRAINT_NAME, kcu.POSITION_IN_UNIQUE_CONSTRAINT;`

		if errRef := db.WithContext(ctx).Raw(queryFkRefCols, table).Scan(&fkRefResults).Error; errRef != nil {
			log.Warn("Failed to query referenced columns for foreign keys. ForeignColumn info might be incomplete.", zap.Error(errRef))
		} else {
			for _, ref := range fkRefResults {
				fkReferencedColsMap[ref.ConstraintName] = append(fkReferencedColsMap[ref.ConstraintName], ref.ReferencedColumnName)
			}
		}
	}

	checkClauses := make(map[string]string)
	var checkTableExistsCount int
	errCheckTable := db.WithContext(ctx).Raw("SELECT COUNT(*) FROM information_schema.tables WHERE TABLE_SCHEMA = 'information_schema' AND TABLE_NAME = 'CHECK_CONSTRAINTS'").Scan(&checkTableExistsCount).Error

	if errCheckTable == nil && checkTableExistsCount > 0 {
		var rawCheckConstraints []struct {
			ConstraintName string `gorm:"column:CONSTRAINT_NAME"`
			CheckClause    string `gorm:"column:CHECK_CLAUSE"`
		}
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
		log.Debug("CHECK_CONSTRAINTS table not found (likely MySQL < 8.0.16 or MariaDB < 10.2.1).")
	}

	consMap := make(map[string]*ConstraintInfo)
	colsForConstraint := make(map[string][]struct {
		Seq  int64
		Name string
	})

	for _, r := range tcResults {
		constraintName := r.ConstraintName
		constraintType := strings.ToUpper(r.ConstraintType)

		if _, ok := consMap[constraintName]; !ok {
			consMap[constraintName] = &ConstraintInfo{
				Name:           constraintName,
				Type:           constraintType,
				Columns:        []string{},
				ForeignTable:   r.ForeignTableName.String,           // Akan diisi hanya untuk FK
				ForeignColumns: fkReferencedColsMap[constraintName], // Isi dengan kolom referensi yang sudah diambil & diurutkan
				OnDelete:       r.DeleteRule.String,
				OnUpdate:       r.UpdateRule.String,
			}
			if constraintType == "CHECK" {
				if clause, found := checkClauses[constraintName]; found {
					consMap[constraintName].Definition = clause
				} else {
					log.Info("CHECK constraint found but no explicit definition retrieved from CHECK_CONSTRAINTS (might be inline or older MySQL).",
						zap.String("constraint_name", constraintName))
				}
			}
		}

		if r.ColumnName.Valid && r.ColumnName.String != "" {
			seq := int64(1)
			if r.OrdinalPosition.Valid {
				seq = r.OrdinalPosition.Int64
			}
			// Untuk PK, UNIQUE, dan kolom LOKAL dari FK
			if constraintType == "PRIMARY KEY" || constraintType == "UNIQUE" || constraintType == "FOREIGN KEY" {
				colsForConstraint[constraintName] = append(colsForConstraint[constraintName], struct {
					Seq  int64
					Name string
				}{
					Seq: seq, Name: r.ColumnName.String,
				})
			}
		}
	}

	constraints := make([]ConstraintInfo, 0, len(consMap))
	for name, cons := range consMap {
		// Urutkan kolom lokal untuk PK, UNIQUE, FK
		if colData, ok := colsForConstraint[name]; ok {
			sort.Slice(colData, func(i, j int) bool { return colData[i].Seq < colData[j].Seq })
			cons.Columns = make([]string, len(colData))
			for i, cd := range colData {
				cons.Columns[i] = cd.Name
			}
		}
		// ForeignColumns sudah diisi sebelumnya dari fkReferencedColsMap yang sudah diurutkan.
		constraints = append(constraints, *cons)
	}

	sort.Slice(constraints, func(i, j int) bool { return constraints[i].Name < constraints[j].Name })
	log.Debug("Fetched MySQL constraint info successfully.", zap.Int("constraint_count", len(constraints)))
	return constraints, nil
}

// isTableNotExistError adalah helper untuk memeriksa pesan error spesifik dialek.
func isTableNotExistError(err error, dialect string) bool {
	if err == nil {
		return false
	}
	errStrLower := strings.ToLower(err.Error())
	switch dialect {
	case "mysql":
		// Error 1146: Table 'dbname.tablename' doesn't exist
		// Error 1051: Unknown table 'tablename' (untuk SHOW INDEX, dll)
		return strings.Contains(errStrLower, "doesn't exist") || strings.Contains(errStrLower, "unknown table")
	case "postgres":
		// relation "tablename" does not exist
		return strings.Contains(errStrLower, "does not exist") && strings.Contains(errStrLower, "relation")
	case "sqlite":
		// no such table: tablename
		return strings.Contains(errStrLower, "no such table")
	}
	return false
}
