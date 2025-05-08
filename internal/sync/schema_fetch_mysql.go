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

	"github.com/arwahdevops/dbsync/internal/utils" // Diperlukan untuk QuoteIdentifier
)

// --- MySQL Specific Fetching ---

func (s *SchemaSyncer) getMySQLColumns(ctx context.Context, db *gorm.DB, table string) ([]ColumnInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "mysql"), zap.String("action", "getMySQLColumns"))
	var columnsData []struct {
		Field           string         `gorm:"column:COLUMN_NAME"`
		OrdinalPosition int            `gorm:"column:ORDINAL_POSITION"`
		Default         sql.NullString `gorm:"column:COLUMN_DEFAULT"`
		IsNullable      string         `gorm:"column:IS_NULLABLE"` // YES / NO
		Type            string         `gorm:"column:DATA_TYPE"`   // Mis. varchar, int
		FullType        string         `gorm:"column:COLUMN_TYPE"` // Mis. varchar(255), int(11) unsigned
		Length          sql.NullInt64  `gorm:"column:CHARACTER_MAXIMUM_LENGTH"`
		Precision       sql.NullInt64  `gorm:"column:NUMERIC_PRECISION"`
		Scale           sql.NullInt64  `gorm:"column:NUMERIC_SCALE"`
		Key             string         `gorm:"column:COLUMN_KEY"` // PRI, UNI, MUL
		Extra           string         `gorm:"column:EXTRA"`      // auto_increment, DEFAULT_GENERATED, VIRTUAL GENERATED, STORED GENERATED
		Collation       sql.NullString `gorm:"column:COLLATION_NAME"`
		Comment         sql.NullString `gorm:"column:COLUMN_COMMENT"`
		// GenerationExpr akan diisi jika versi mendukung, jika tidak NULL
		GenerationExpr sql.NullString `gorm:"column:GENERATION_EXPRESSION"`
	}

	// Cek versi untuk menentukan query yang tepat
	var version string
	errVersion := db.WithContext(ctx).Raw("SELECT VERSION()").Scan(&version).Error
	canQueryGenExpr := false
	if errVersion == nil && !strings.HasPrefix(version, "5.6.") {
		// Diasumsikan 5.7+ atau 8.0+ mendukung GENERATION_EXPRESSION
		canQueryGenExpr = true
	} else if errVersion != nil {
		log.Warn("Could not determine MySQL version, attempting query without GENERATION_EXPRESSION.", zap.Error(errVersion))
	}

	var query string
	if canQueryGenExpr {
		query = `
			SELECT COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE,
				   COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
				   COLUMN_KEY, EXTRA, COLLATION_NAME, COLUMN_COMMENT,
				   GENERATION_EXPRESSION -- Langsung ambil jika didukung
			FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
			ORDER BY ORDINAL_POSITION
		`
	} else {
		query = `
			SELECT COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE,
				   COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
				   COLUMN_KEY, EXTRA, COLLATION_NAME, COLUMN_COMMENT,
				   NULL AS GENERATION_EXPRESSION -- Set NULL jika tidak bisa query
			FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
			ORDER BY ORDINAL_POSITION
		`
	}

	log.Debug("Executing column info query", zap.Bool("with_generation_expression", canQueryGenExpr))
	err := db.WithContext(ctx).Raw(query, table).Scan(&columnsData).Error
	if err != nil {
		// Jika tabel tidak ada, GORM biasanya mengembalikan ErrRecordNotFound atau error serupa
		if err == gorm.ErrRecordNotFound || strings.Contains(strings.ToLower(err.Error()), "doesn't exist") {
			log.Warn("Table not found or query returned no rows.", zap.Error(err))
			return []ColumnInfo{}, nil // Kembalikan slice kosong jika tabel tidak ada
		}
		// Coba lagi tanpa GENERATION_EXPRESSION jika error awal mungkin karena itu (meskipun cek versi gagal)
		if strings.Contains(strings.ToLower(err.Error()), "unknown column 'generation_expression'") {
			log.Warn("Query failed with unknown column 'GENERATION_EXPRESSION', retrying without it.")
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
				return nil, fmt.Errorf("mysql columns legacy query failed for table '%s': %w", table, err)
			}
		} else {
			return nil, fmt.Errorf("mysql columns query failed for table '%s': %w", table, err)
		}
	}

	result := make([]ColumnInfo, 0, len(columnsData))
	for _, c := range columnsData {
		// Cek IsGenerated berdasarkan EXTRA dan GenerationExpr
		// MySQL 5.7: EXTRA = VIRTUAL GENERATED / STORED GENERATED
		// MySQL 8.0: EXTRA mungkin juga berisi DEFAULT_GENERATED (untuk ekspresi default, bukan kolom generated),
		//            jadi kita juga cek GenerationExpr.Valid
		isGenerated := (strings.Contains(c.Extra, "VIRTUAL GENERATED") || strings.Contains(c.Extra, "STORED GENERATED")) ||
			(c.GenerationExpr.Valid && c.GenerationExpr.String != "")

		// Cek AutoIncrement hanya dari EXTRA
		isAutoIncrement := strings.Contains(c.Extra, "auto_increment")

		colInfo := ColumnInfo{
			Name:            c.Field,
			Type:            c.FullType, // Gunakan FullType (misal, int(11) unsigned) sebagai tipe asli
			MappedType:      "",         // JANGAN isi di sini, diisi oleh SchemaSyncer
			IsNullable:      strings.ToUpper(c.IsNullable) == "YES",
			IsPrimary:       c.Key == "PRI",
			IsGenerated:     isGenerated,
			DefaultValue:    c.Default, // Ambil default mentah
			AutoIncrement:   isAutoIncrement,
			OrdinalPosition: c.OrdinalPosition,
			Length:          c.Length,
			Precision:       c.Precision,
			Scale:           c.Scale,
			Collation:       c.Collation,
			Comment:         c.Comment,
			// Simpan GenerationExpression mentah jika diperlukan
			// GenerationExpression: c.GenerationExpr,
		}
		result = append(result, colInfo)
	}
	log.Debug("Fetched column info successfully.", zap.Int("column_count", len(result)))
	return result, nil
}

func (s *SchemaSyncer) getMySQLIndexes(ctx context.Context, db *gorm.DB, table string) ([]IndexInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "mysql"), zap.String("action", "getMySQLIndexes"))
	indexes := make([]IndexInfo, 0)
	idxMap := make(map[string]*IndexInfo) // Peta: Nama Indeks -> Info Indeks

	var results []struct {
		// Table      string `gorm:"column:Table"` // Tidak perlu
		NonUnique  int    `gorm:"column:Non_unique"` // 0 = Unique, 1 = Non-unique
		KeyName    string `gorm:"column:Key_name"`   // Nama indeks, PRIMARY untuk PK
		SeqInIndex int    `gorm:"column:Seq_in_index"` // Urutan kolom dalam indeks komposit
		ColumnName string `gorm:"column:Column_name"`  // Nama kolom
		// IndexType  string `gorm:"column:Index_type"` // BTREE, HASH, FULLTEXT, SPATIAL
		// Tambahkan informasi lain jika perlu, misal Sub_part, Index_comment
	}
	// Gunakan utils.QuoteIdentifier untuk nama tabel dalam query mentah
	query := fmt.Sprintf("SHOW INDEX FROM %s", utils.QuoteIdentifier(table, "mysql"))
	err := db.WithContext(ctx).Raw(query).Scan(&results).Error
	if err != nil {
		// Tangani error jika tabel tidak ada
		if strings.Contains(strings.ToLower(err.Error()), "doesn't exist") {
			log.Warn("Table not found when executing SHOW INDEX.", zap.Error(err))
			return indexes, nil // Kembalikan slice kosong
		}
		return nil, fmt.Errorf("mysql SHOW INDEX failed for table '%s': %w", table, err)
	}

	if len(results) == 0 {
		log.Debug("No indexes found for table.")
		return indexes, nil
	}

	// Proses hasil query untuk membangun struct IndexInfo
	for _, r := range results {
		idx, ok := idxMap[r.KeyName]
		if !ok {
			// Indeks baru, buat entri baru
			idx = &IndexInfo{
				Name:      r.KeyName,
				Columns:   make([]string, 0), // Akan diisi berdasarkan SeqInIndex
				IsUnique:  r.NonUnique == 0,
				IsPrimary: r.KeyName == "PRIMARY",
				// IndexType: r.IndexType, // Jika ingin menyimpan tipe indeks
			}
			idxMap[r.KeyName] = idx
		}
		// Pastikan slice Columns cukup besar untuk menampung kolom sesuai urutannya
		neededLen := r.SeqInIndex
		currentLen := len(idx.Columns)
		if neededLen > currentLen {
			// Tambahkan elemen kosong jika perlu
			idx.Columns = append(idx.Columns, make([]string, neededLen-currentLen)...)
		}
		// Masukkan nama kolom pada posisi yang benar (SeqInIndex dimulai dari 1)
		if neededLen > 0 {
			idx.Columns[neededLen-1] = r.ColumnName
		} else {
			// Kasus aneh jika SeqInIndex <= 0, tambahkan ke akhir saja
			log.Warn("Unexpected SeqInIndex value found.", zap.Int("seq", r.SeqInIndex), zap.String("index", r.KeyName), zap.String("column", r.ColumnName))
			idx.Columns = append(idx.Columns, r.ColumnName)
		}
	}

	// Finalisasi: Urutkan kolom dalam setiap indeks dan tambahkan ke hasil akhir
	for _, idx := range idxMap {
		// Hapus elemen kosong jika ada (dari alokasi berlebih)
		finalCols := make([]string, 0, len(idx.Columns))
		for _, col := range idx.Columns {
			if col != "" {
				finalCols = append(finalCols, col)
			}
		}
		idx.Columns = finalCols
		sort.Strings(idx.Columns) // Urutkan nama kolom secara alfabetis untuk konsistensi perbandingan
		indexes = append(indexes, *idx)
	}

	// Urutkan slice indeks berdasarkan nama untuk hasil yang deterministik
	sort.Slice(indexes, func(i, j int) bool { return indexes[i].Name < indexes[j].Name })
	log.Debug("Fetched index info successfully.", zap.Int("index_count", len(indexes)))
	return indexes, nil
}

func (s *SchemaSyncer) getMySQLConstraints(ctx context.Context, db *gorm.DB, table string) ([]ConstraintInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "mysql"), zap.String("action", "getMySQLConstraints"))
	constraints := make([]ConstraintInfo, 0)
	consMap := make(map[string]*ConstraintInfo) // Peta: Nama Constraint -> Info Constraint

	// Struct untuk menampung hasil query gabungan
	var results []struct {
		ConstraintName    string         `gorm:"column:CONSTRAINT_NAME"`
		ConstraintType    string         `gorm:"column:CONSTRAINT_TYPE"` // PRIMARY KEY, UNIQUE, FOREIGN KEY
		TableName         string         `gorm:"column:TABLE_NAME"`      // Nama tabel (untuk konfirmasi)
		ColumnName        string         `gorm:"column:COLUMN_NAME"`     // Kolom di tabel ini
		OrdinalPosition   int            `gorm:"column:ORDINAL_POSITION"` // Posisi kolom dalam constraint komposit
		ForeignTableName  sql.NullString `gorm:"column:REFERENCED_TABLE_NAME"`
		ForeignColumnName sql.NullString `gorm:"column:REFERENCED_COLUMN_NAME"`
		UpdateRule        sql.NullString `gorm:"column:UPDATE_RULE"` // Dari referential_constraints
		DeleteRule        sql.NullString `gorm:"column:DELETE_RULE"` // Dari referential_constraints
	}

	// Query untuk mendapatkan PK, UNIQUE, FK dari information_schema
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
		JOIN information_schema.KEY_COLUMN_USAGE kcu
			ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
			AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
			AND tc.TABLE_NAME = kcu.TABLE_NAME
		LEFT JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
			ON tc.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
			AND tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
			AND tc.TABLE_NAME = rc.TABLE_NAME -- Join tambahan untuk akurasi rc
		WHERE tc.TABLE_SCHEMA = DATABASE() AND tc.TABLE_NAME = ?
		ORDER BY tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION;
	`
	err := db.WithContext(ctx).Raw(queryCore, table).Scan(&results).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound || strings.Contains(strings.ToLower(err.Error()), "doesn't exist") {
			log.Warn("Table not found or no constraints found via core query.")
			// Lanjutkan untuk cek CHECK constraints jika ada
		} else {
			return nil, fmt.Errorf("mysql core constraints query failed for table '%s': %w", table, err)
		}
	}

	// Query untuk CHECK constraints (MySQL 8.0.16+)
	var checkResults []struct {
		ConstraintName string `gorm:"column:CONSTRAINT_NAME"`
		CheckClause    string `gorm:"column:CHECK_CLAUSE"`
	}
	// Cek apakah tabel CHECK_CONSTRAINTS ada sebelum query
	var checkTableExists int
	errCheckTable := db.WithContext(ctx).Raw("SELECT COUNT(*) FROM information_schema.tables WHERE TABLE_SCHEMA = 'information_schema' AND TABLE_NAME = 'CHECK_CONSTRAINTS'").Scan(&checkTableExists).Error

	if errCheckTable == nil && checkTableExists > 0 {
		checkQuery := `
			SELECT CONSTRAINT_NAME, CHECK_CLAUSE
			FROM information_schema.CHECK_CONSTRAINTS
			WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = ?;
		`
		errCheck := db.WithContext(ctx).Raw(checkQuery, table).Scan(&checkResults).Error
		if errCheck != nil && errCheck != gorm.ErrRecordNotFound {
			log.Warn("Failed to query MySQL CHECK_CONSTRAINTS, CHECK constraints might be missed.", zap.Error(errCheck))
		}
	} else if errCheckTable != nil {
		log.Warn("Failed to check for CHECK_CONSTRAINTS table existence.", zap.Error(errCheckTable))
	} else {
		log.Debug("CHECK_CONSTRAINTS table not found (likely MySQL < 8.0.16).")
	}
	checkMap := make(map[string]string)
	for _, c := range checkResults { checkMap[c.ConstraintName] = c.CheckClause }

	// Proses hasil query inti (PK, UNIQUE, FK)
	tempFKCols := make(map[string][]struct{ Seq int; Local, Foreign string }) // Untuk mengurutkan kolom FK

	for _, r := range results {
		constraintName := r.ConstraintName
		constraintType := r.ConstraintType // PRIMARY KEY, UNIQUE, FOREIGN KEY

		cons, ok := consMap[constraintName]
		if !ok {
			cons = &ConstraintInfo{
				Name:           constraintName,
				Type:           constraintType,
				Columns:        make([]string, 0),
				ForeignTable:   r.ForeignTableName.String, // Akan kosong jika bukan FK
				ForeignColumns: make([]string, 0),       // Akan diisi nanti untuk FK
				OnDelete:       r.DeleteRule.String,       // Akan kosong jika bukan FK
				OnUpdate:       r.UpdateRule.String,       // Akan kosong jika bukan FK
				Definition:     checkMap[constraintName],  // Ambil dari checkMap jika ada
			}
			consMap[constraintName] = cons
			if constraintType == "FOREIGN KEY" {
				tempFKCols[constraintName] = make([]struct{ Seq int; Local, Foreign string }, 0)
			}
		}

		// Tambahkan kolom ke constraint, perhatikan urutan
		neededLen := r.OrdinalPosition
		currentLen := len(cons.Columns)
		if neededLen > currentLen {
			cons.Columns = append(cons.Columns, make([]string, neededLen-currentLen)...)
		}
		if neededLen > 0 {
			cons.Columns[neededLen-1] = r.ColumnName
		} else {
			log.Warn("Unexpected OrdinalPosition value found for constraint.", zap.Int("pos", neededLen), zap.String("constraint", constraintName), zap.String("column", r.ColumnName))
			cons.Columns = append(cons.Columns, r.ColumnName) // Fallback: tambahkan ke akhir
		}

		// Kumpulkan kolom FK untuk diurutkan nanti
		if constraintType == "FOREIGN KEY" && r.ForeignColumnName.Valid {
			tempFKCols[constraintName] = append(tempFKCols[constraintName], struct{ Seq int; Local, Foreign string }{
				Seq:     r.OrdinalPosition,
				Local:   r.ColumnName,
				Foreign: r.ForeignColumnName.String,
			})
		}
	}

	// Proses kolom FK yang sudah dikumpulkan
	for constraintName, fkCons := range consMap {
		if fkCons.Type == "FOREIGN KEY" {
			fkColPairs := tempFKCols[constraintName]
			// Urutkan berdasarkan OrdinalPosition (Seq)
			sort.Slice(fkColPairs, func(i, j int) bool {
				return fkColPairs[i].Seq < fkColPairs[j].Seq
			})
			// Isi kembali Columns dan ForeignColumns yang sudah terurut
			fkCons.Columns = make([]string, len(fkColPairs))
			fkCons.ForeignColumns = make([]string, len(fkColPairs))
			for i, pair := range fkColPairs {
				fkCons.Columns[i] = pair.Local
				fkCons.ForeignColumns[i] = pair.Foreign
			}
		} else {
			// Untuk constraint lain (PK, UNIQUE), urutkan kolom secara alfabetis
			// setelah memastikan tidak ada elemen kosong
			finalCols := make([]string, 0, len(fkCons.Columns))
			for _, col := range fkCons.Columns {
				if col != "" { finalCols = append(finalCols, col) }
			}
			fkCons.Columns = finalCols
			sort.Strings(fkCons.Columns)
		}
	}

	// Tambahkan constraint CHECK yang mungkin belum masuk (jika hanya ada di check_constraints)
	for name, definition := range checkMap {
		if _, exists := consMap[name]; !exists {
			consMap[name] = &ConstraintInfo{
				Name:       name,
				Type:       "CHECK",
				Columns:    []string{}, // Kolom tidak relevan secara langsung untuk CHECK di info schema ini
				Definition: definition,
			}
		}
	}

	// Masukkan hasil ke slice final dan urutkan
	for _, cons := range consMap {
		constraints = append(constraints, *cons)
	}
	sort.Slice(constraints, func(i, j int) bool { return constraints[i].Name < constraints[j].Name })
	log.Debug("Fetched constraint info successfully.", zap.Int("constraint_count", len(constraints)))
	return constraints, nil
}
