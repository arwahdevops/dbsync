// internal/sync/schema_fetch_sqlite.go
package sync

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/arwahdevops/dbsync/internal/utils"
)

func (s *SchemaSyncer) getSQLiteColumns(ctx context.Context, db *gorm.DB, table string) ([]ColumnInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "sqlite"), zap.String("action", "getSQLiteColumns"))

	var count int64
	errTbl := db.WithContext(ctx).Raw("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&count).Error
	if errTbl != nil {
		return nil, fmt.Errorf("sqlite: failed to check table existence for '%s': %w", table, errTbl)
	}
	if count == 0 {
		log.Warn("Table not found.")
		return []ColumnInfo{}, nil
	}

	var createTableSQL string
	errCreateSQL := db.WithContext(ctx).Raw("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&createTableSQL).Error
	if errCreateSQL != nil {
		log.Warn("Could not fetch CREATE TABLE statement for SQLite. Generated column expressions and some specific attributes (like collation) might be missed.", zap.Error(errCreateSQL))
		createTableSQL = "" // Lanjutkan tanpa itu jika gagal, dengan fungsionalitas terbatas
	}

	var columnsDataXInfo []struct {
		Cid       int            `gorm:"column:cid"`        // Urutan kolom
		Name      string         `gorm:"column:name"`       // Nama kolom
		Type      string         `gorm:"column:type"`       // Tipe afinitas deklarasi
		NotNull   int            `gorm:"column:notnull"`    // 0 jika NULL diizinkan, 1 jika NOT NULL
		DfltValue sql.NullString `gorm:"column:dflt_value"` // Nilai default
		Pk        int            `gorm:"column:pk"`         // Bagian dari PK (1-based index jika PK, 0 jika tidak)
		Hidden    int            `gorm:"column:hidden"`     // 0=normal, 1=hidden by alter, 2=STORED generated, 3=VIRTUAL generated
	}

	// Coba PRAGMA table_xinfo (SQLite >= 3.26.0 untuk 'hidden', >= 3.31.0 secara umum lebih baik)
	pragmaXInfoQuery := fmt.Sprintf("PRAGMA table_xinfo(%s)", utils.QuoteIdentifier(table, "sqlite"))
	errXInfo := db.WithContext(ctx).Raw(pragmaXInfoQuery).Scan(&columnsDataXInfo).Error

	if errXInfo == nil && len(columnsDataXInfo) > 0 {
		log.Debug("Using PRAGMA table_xinfo for column details.")
		result := make([]ColumnInfo, 0, len(columnsDataXInfo))
		for _, c := range columnsDataXInfo {
			isGenerated := c.Hidden == 2 || c.Hidden == 3 // 2 for STORED, 3 for VIRTUAL
			var generationExpr sql.NullString
			var finalType = c.Type // Tipe dasar dari pragma

			if isGenerated && createTableSQL != "" {
				if expr := s.extractSQLiteGeneratedColumnExpr(createTableSQL, c.Name); expr != "" {
					generationExpr.String = expr
					generationExpr.Valid = true
					// Untuk IsGenerated, ColumnInfo.Type idealnya adalah tipe dasar saja,
					// karena ekspresi sudah disimpan di GenerationExpression.
					log.Debug("Found generated column expression via DDL parsing.",
						zap.String("column", c.Name), zap.String("expression", expr))
				} else {
					log.Warn("Could not extract generation expression for generated SQLite column from DDL. Type comparison might be affected.",
						zap.String("column", c.Name))
				}
			}

			// Collation per kolom di SQLite didefinisikan dalam CREATE TABLE
			var collation sql.NullString
			if createTableSQL != "" {
				collation.String = s.extractSQLiteColumnCollation(createTableSQL, c.Name)
				collation.Valid = collation.String != ""
			}

			isAutoIncrement := c.Pk > 0 && strings.ToUpper(finalType) == "INTEGER" && !isGenerated
			// AUTOINCREMENT keyword di SQLite hanya berlaku untuk ROWID alias (INTEGER PRIMARY KEY)
			// dan memberlakukan aturan ID tidak di-reuse. Cek dari DDL.
			if isAutoIncrement && createTableSQL != "" {
				isAutoIncrement = s.checkSQLiteColumnAutoincrement(createTableSQL, c.Name)
			}

			result = append(result, ColumnInfo{
				Name: c.Name, Type: finalType, IsNullable: c.NotNull == 0,
				IsPrimary: c.Pk > 0, IsGenerated: isGenerated, DefaultValue: c.DfltValue,
				AutoIncrement: isAutoIncrement, OrdinalPosition: c.Cid,
				GenerationExpression: generationExpr, Collation: collation,
				// Length, Precision, Scale, Comment tidak langsung tersedia dari PRAGMA.
				// SQLite tidak punya comment kolom native.
			})
		}
		log.Debug("Fetched column info successfully using table_xinfo.", zap.Int("column_count", len(result)))
		return result, nil
	}

	if errXInfo != nil {
		log.Warn("PRAGMA table_xinfo failed, falling back to PRAGMA table_info. Generated column info will be missed.", zap.Error(errXInfo))
	} else if len(columnsDataXInfo) == 0 { // errXInfo == nil tapi tidak ada hasil
		log.Debug("PRAGMA table_xinfo returned no data (table might be empty or old SQLite), falling back to PRAGMA table_info.")
	}

	// Fallback ke PRAGMA table_info
	var columnsDataTableInfo []struct {
		Cid       int            `gorm:"column:cid"`
		Name      string         `gorm:"column:name"`
		Type      string         `gorm:"column:type"`
		NotNull   int            `gorm:"column:notnull"`
		DfltValue sql.NullString `gorm:"column:dflt_value"`
		Pk        int            `gorm:"column:pk"`
	}
	pragmaTableInfoQuery := fmt.Sprintf("PRAGMA table_info(%s)", utils.QuoteIdentifier(table, "sqlite"))
	err := db.WithContext(ctx).Raw(pragmaTableInfoQuery).Scan(&columnsDataTableInfo).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound || isTableNotExistError(err, "sqlite") {
			log.Warn("Table not found via PRAGMA table_info.")
			return []ColumnInfo{}, nil
		}
		return nil, fmt.Errorf("sqlite: PRAGMA table_info failed for table '%s': %w", table, err)
	}
	if len(columnsDataTableInfo) == 0 {
		log.Warn("PRAGMA table_info returned no columns.")
		return []ColumnInfo{}, nil
	}

	result := make([]ColumnInfo, 0, len(columnsDataTableInfo))
	for _, c := range columnsDataTableInfo {
		isAutoIncrement := c.Pk > 0 && strings.ToUpper(c.Type) == "INTEGER"
		if isAutoIncrement && createTableSQL != "" {
			isAutoIncrement = s.checkSQLiteColumnAutoincrement(createTableSQL, c.Name)
		}
		var collation sql.NullString
		if createTableSQL != "" {
			collation.String = s.extractSQLiteColumnCollation(createTableSQL, c.Name)
			collation.Valid = collation.String != ""
		}

		result = append(result, ColumnInfo{
			Name: c.Name, Type: c.Type, IsNullable: c.NotNull == 0,
			IsPrimary: c.Pk > 0, IsGenerated: false, // Tidak bisa dideteksi dari table_info
			DefaultValue: c.DfltValue, AutoIncrement: isAutoIncrement, OrdinalPosition: c.Cid,
			Collation: collation,
		})
	}
	log.Debug("Fetched column info successfully using table_info (fallback).", zap.Int("column_count", len(result)))
	return result, nil
}

// extractSQLiteGeneratedColumnExpr (tetap sama dari iterasi sebelumnya, mungkin perlu penyempurnaan regex)
func (s *SchemaSyncer) extractSQLiteGeneratedColumnExpr(createTableSQL, columnName string) string {
	if createTableSQL == "" {
		return ""
	}
	patterns := []string{
		`(?i)\b` + regexp.QuoteMeta(columnName) + `\b\s+[^,)]+\s+GENERATED\s+ALWAYS\s+AS\s*\((.*?)\)`,
		`(?i)"` + regexp.QuoteMeta(columnName) + `"\s+[^,)]+\s+GENERATED\s+ALWAYS\s+AS\s*\((.*?)\)`,
		`(?i)\x60` + regexp.QuoteMeta(columnName) + `\x60\s+[^,)]+\s+GENERATED\s+ALWAYS\s+AS\s*\((.*?)\)`,
		`(?i)\[` + regexp.QuoteMeta(columnName) + `\]\s+[^,)]+\s+GENERATED\s+ALWAYS\s+AS\s*\((.*?)\)`,
	}
	for _, pattern := range patterns {
		re := regexp.MustCompile(pattern)
		matches := re.FindStringSubmatch(createTableSQL)
		if len(matches) > 1 && matches[1] != "" {
			return strings.TrimSpace(matches[1])
		}
	}
	return ""
}

// extractSQLiteColumnCollation mencoba mengekstrak klausa COLLATE untuk kolom tertentu.
func (s *SchemaSyncer) extractSQLiteColumnCollation(createTableSQL, columnName string) string {
	if createTableSQL == "" {
		return ""
	}
	// Pola: `colName` type ... COLLATE `collationName`
	// Perlu menangani quoting nama kolom dan nama kolasi.
	// Ini adalah parser sederhana.
	patterns := []string{
		`(?i)\b` + regexp.QuoteMeta(columnName) + `\b[^,]*?\s+COLLATE\s+("?\w+"?|\w+)`,
		`(?i)"` + regexp.QuoteMeta(columnName) + `"[^,]*?\s+COLLATE\s+("?\w+"?|\w+)`,
		`(?i)\x60` + regexp.QuoteMeta(columnName) + `\x60[^,]*?\s+COLLATE\s+("?\w+"?|\w+)`,
		`(?i)\[` + regexp.QuoteMeta(columnName) + `\][^,]*?\s+COLLATE\s+("?\w+"?|\w+)`,
	}
	for _, pattern := range patterns {
		re := regexp.MustCompile(pattern)
		matches := re.FindStringSubmatch(createTableSQL)
		if len(matches) > 1 && matches[1] != "" {
			return strings.Trim(matches[1], "\"`") // Hapus quote jika ada
		}
	}
	return ""
}

// checkSQLiteColumnAutoincrement memeriksa apakah keyword AUTOINCREMENT ada untuk kolom PK.
func (s *SchemaSyncer) checkSQLiteColumnAutoincrement(createTableSQL, columnName string) bool {
	if createTableSQL == "" {
		return false
	}
	// Pola: `colName` INTEGER PRIMARY KEY AUTOINCREMENT
	patterns := []string{
		`(?i)\b` + regexp.QuoteMeta(columnName) + `\b\s+INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT`,
		`(?i)"` + regexp.QuoteMeta(columnName) + `"\s+INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT`,
		`(?i)\x60` + regexp.QuoteMeta(columnName) + `\x60\s+INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT`,
		`(?i)\[` + regexp.QuoteMeta(columnName) + `\]\s+INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT`,
	}
	for _, pattern := range patterns {
		re := regexp.MustCompile(pattern)
		if re.MatchString(createTableSQL) {
			return true
		}
	}
	return false
}

func (s *SchemaSyncer) getSQLiteIndexes(ctx context.Context, db *gorm.DB, table string) ([]IndexInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "sqlite"), zap.String("action", "getSQLiteIndexes"))

	var count int64
	db.WithContext(ctx).Raw("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&count)
	if count == 0 {
		log.Warn("Table not found, cannot fetch indexes.")
		return []IndexInfo{}, nil
	}

	var indexList []struct {
		Name   string `gorm:"column:name"`
		Unique int    `gorm:"column:unique"`
		Origin string `gorm:"column:origin"`
	}
	pragmaIndexListQuery := fmt.Sprintf("PRAGMA index_list(%s)", utils.QuoteIdentifier(table, "sqlite"))
	if err := db.WithContext(ctx).Raw(pragmaIndexListQuery).Scan(&indexList).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			log.Debug("No indexes found for table via PRAGMA index_list.")
			return []IndexInfo{}, nil
		}
		return nil, fmt.Errorf("sqlite: PRAGMA index_list failed for table '%s': %w", table, err)
	}
	if len(indexList) == 0 {
		log.Debug("No indexes returned by PRAGMA index_list.")
		return []IndexInfo{}, nil
	}

	indexes := make([]IndexInfo, 0, len(indexList))
	for _, idxItem := range indexList {
		if strings.HasPrefix(idxItem.Name, "sqlite_autoindex_") {
			log.Debug("Skipping SQLite auto-generated index.", zap.String("index_name", idxItem.Name))
			continue
		}
		var columnsForThisIndex []struct {
			SeqNo int            `gorm:"column:seqno"`
			Name  sql.NullString `gorm:"column:name"`
		}
		pragmaIndexInfoQuery := fmt.Sprintf("PRAGMA index_info(%s)", utils.QuoteIdentifier(idxItem.Name, "sqlite"))
		if errInfo := db.WithContext(ctx).Raw(pragmaIndexInfoQuery).Scan(&columnsForThisIndex).Error; errInfo != nil {
			log.Warn("sqlite: PRAGMA index_info failed, skipping this index.", zap.String("index_name", idxItem.Name), zap.Error(errInfo))
			continue
		}
		if len(columnsForThisIndex) == 0 {
			log.Warn("PRAGMA index_info returned no columns for a listed index.", zap.String("index_name", idxItem.Name))
			continue
		}

		tempCols := make(map[int]string)
		maxSeqNo := -1
		hasExpression := false
		for _, colInfo := range columnsForThisIndex {
			colName := fmt.Sprintf("<expression_seq_%d>", colInfo.SeqNo) // Default jika nama kolom NULL
			if colInfo.Name.Valid && colInfo.Name.String != "" {
				colName = colInfo.Name.String
			} else {
				hasExpression = true
			}
			tempCols[colInfo.SeqNo] = colName
			if colInfo.SeqNo > maxSeqNo {
				maxSeqNo = colInfo.SeqNo
			}
		}
		finalColumns := make([]string, 0, maxSeqNo+1) // Inisialisasi dengan kapasitas
		for i := 0; i <= maxSeqNo; i++ {              // Iterasi dari 0 sampai maxSeqNo
			if name, ok := tempCols[i]; ok {
				finalColumns = append(finalColumns, name)
			} else {
				// Ini seharusnya tidak terjadi jika SeqNo continuous dan 0-based
				log.Warn("Missing sequence number in PRAGMA index_info results, index columns might be incomplete.", zap.Int("missing_seq", i), zap.String("index_name", idxItem.Name))
			}
		}

		var rawDefSQL string
		// Ambil DDL jika ada ekspresi atau jika origin bukan 'c' (karena PK/UQ mungkin tidak punya DDL CREATE INDEX eksplisit)
		if hasExpression || idxItem.Origin != "c" {
			errDef := db.WithContext(ctx).Raw("SELECT sql FROM sqlite_master WHERE type='index' AND name=?", idxItem.Name).Scan(&rawDefSQL).Error
			if errDef != nil && errDef != gorm.ErrRecordNotFound { // Abaikan jika indeks tidak ditemukan (misal, PK tanpa nama eksplisit)
				log.Warn("Could not fetch CREATE INDEX statement.", zap.String("index_name", idxItem.Name), zap.Error(errDef))
			}
		}
		indexes = append(indexes, IndexInfo{
			Name: idxItem.Name, Columns: finalColumns, IsUnique: idxItem.Unique == 1,
			IsPrimary: idxItem.Origin == "pk", RawDef: rawDefSQL,
		})
	}
	sort.Slice(indexes, func(i, j int) bool { return indexes[i].Name < indexes[j].Name })
	log.Debug("Fetched SQLite index info successfully.", zap.Int("index_count", len(indexes)))
	return indexes, nil
}

func (s *SchemaSyncer) getSQLiteConstraints(ctx context.Context, db *gorm.DB, table string) ([]ConstraintInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "sqlite"), zap.String("action", "getSQLiteConstraints"))

	var count int64
	db.WithContext(ctx).Raw("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&count)
	if count == 0 {
		log.Warn("Table not found, cannot fetch constraints.")
		return []ConstraintInfo{}, nil
	}

	consMap := make(map[string]*ConstraintInfo)
	var fkList []struct {
		ID       int    `gorm:"column:id"`
		Seq      int    `gorm:"column:seq"`
		Table    string `gorm:"column:table"`
		From     string `gorm:"column:from"`
		To       string `gorm:"column:to"`
		OnUpdate string `gorm:"column:on_update"`
		OnDelete string `gorm:"column:on_delete"`
	}
	pragmaFKQuery := fmt.Sprintf("PRAGMA foreign_key_list(%s)", utils.QuoteIdentifier(table, "sqlite"))
	if errFK := db.WithContext(ctx).Raw(pragmaFKQuery).Scan(&fkList).Error; errFK != nil && errFK != gorm.ErrRecordNotFound {
		log.Warn("sqlite: PRAGMA foreign_key_list failed, FK constraints might be missed.", zap.Error(errFK))
	} else if len(fkList) > 0 {
		fkGroup := make(map[int][]struct {
			Seq            int
			Local, Foreign string
		})
		fkDetails := make(map[int]ConstraintInfo)
		for _, fk := range fkList {
			if _, ok := fkDetails[fk.ID]; !ok {
				fkName := fmt.Sprintf("fk_%s_auto_id%d_pragma", table, fk.ID) // Nama sementara dari pragma
				fkDetails[fk.ID] = ConstraintInfo{Name: fkName, Type: "FOREIGN KEY", ForeignTable: fk.Table, OnUpdate: fk.OnUpdate, OnDelete: fk.OnDelete}
				fkGroup[fk.ID] = make([]struct {
					Seq            int
					Local, Foreign string
				}, 0)
			}
			fkGroup[fk.ID] = append(fkGroup[fk.ID], struct {
				Seq            int
				Local, Foreign string
			}{Seq: fk.Seq, Local: fk.From, Foreign: fk.To})
		}
		for id, details := range fkDetails {
			colPairs := fkGroup[id]
			sort.Slice(colPairs, func(i, j int) bool { return colPairs[i].Seq < colPairs[j].Seq })
			localCols, foreignCols := make([]string, len(colPairs)), make([]string, len(colPairs))
			for i, pair := range colPairs {
				localCols[i], foreignCols[i] = pair.Local, pair.Foreign
			}
			details.Columns, details.ForeignColumns = localCols, foreignCols
			consMap[details.Name] = &details
		}
	}

	var createTableSQL string
	if errSQL := db.WithContext(ctx).Raw("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&createTableSQL).Error; errSQL != nil {
		log.Warn("Could not fetch CREATE TABLE statement to parse explicit PK/UNIQUE/CHECK constraints.", zap.Error(errSQL))
	} else {
		// Parse PK, UNIQUE, CHECK dari DDL. Fungsi parse helper akan menangani nama constraint.
		// Jika ada nama yang sama dengan FK dari pragma, FK dari pragma akan di-overwrite jika nama sama,
		// atau keduanya akan ada jika namanya berbeda (misal, FK punya nama eksplisit di DDL).
		// Ini perlu dipertimbangkan. Idealnya, parser DDL constraint juga mencoba mengekstrak detail FK.
		// Untuk sekarang, kita tambahkan hasil parsing DDL ke map.
		pkInfo := parseSQLitePKFromDDL(createTableSQL, table) // Beri nama tabel untuk PK default
		if pkInfo.Name != "" {
			consMap[pkInfo.Name] = &pkInfo
		}

		uniqueInfos := parseSQLiteUniqueFromDDL(createTableSQL, table)
		for _, uqInfo := range uniqueInfos {
			consMap[uqInfo.Name] = &uqInfo
		}

		checkInfos := parseSQLiteChecksFromDDL(createTableSQL, table)
		for _, chkInfo := range checkInfos {
			consMap[chkInfo.Name] = &chkInfo
		}
	}

	constraints := make([]ConstraintInfo, 0, len(consMap))
	for _, cons := range consMap {
		constraints = append(constraints, *cons)
	}
	sort.Slice(constraints, func(i, j int) bool { return constraints[i].Name < constraints[j].Name })
	log.Debug("Fetched SQLite constraint info successfully.", zap.Int("constraint_count", len(constraints)))
	return constraints, nil
}

// --- Helper Parsing SQLite CREATE TABLE ---
// parseSQLitePKFromDDL, parseSQLiteUniqueFromDDL, parseSQLiteChecksFromDDL
// (Versi ini dimodifikasi sedikit untuk menerima nama tabel sebagai argumen
//  untuk membuat nama constraint default jika tidak ada nama eksplisit)

func parseSQLitePKFromDDL(sql string, tableName string) ConstraintInfo {
	rePK := regexp.MustCompile(`(?i)(?:CONSTRAINT\s+("?(?P<name>\w+)"?)\s+)?PRIMARY\s+KEY\s*\((?P<cols>[^)]+)\)`)
	matches := rePK.FindStringSubmatch(sql)
	pkInfo := ConstraintInfo{Type: "PRIMARY KEY"}
	if len(matches) > 0 {
		resultMap := make(map[string]string)
		for i, name := range rePK.SubexpNames() {
			if i != 0 && name != "" && i < len(matches) {
				resultMap[name] = matches[i]
			}
		}
		pkInfo.Name = strings.Trim(resultMap["name"], "\"`[]")
		if pkInfo.Name == "" { // Jika PK tidak punya nama eksplisit, buat nama default
			pkInfo.Name = fmt.Sprintf("pk_%s", tableName)
		}
		colsStr := resultMap["cols"]
		for _, rawCol := range strings.Split(colsStr, ",") {
			if unquoted := strings.Trim(strings.TrimSpace(rawCol), "\"`[]"); unquoted != "" {
				pkInfo.Columns = append(pkInfo.Columns, unquoted)
			}
		}
		if len(pkInfo.Columns) > 0 {
			// Urutan kolom PK dari DDL harus dipertahankan, jangan sort di sini.
			return pkInfo
		}
	}
	return ConstraintInfo{} // Tidak ada PK eksplisit (mungkin inline di kolom)
}

func parseSQLiteUniqueFromDDL(sql string, tableName string) []ConstraintInfo {
	var uniques []ConstraintInfo
	reUQ := regexp.MustCompile(`(?i)CONSTRAINT\s+("?(?P<name>\w+)"?)\s+UNIQUE\s*\((?P<cols>[^)]+)\)`)
	allMatches := reUQ.FindAllStringSubmatch(sql, -1)
	for i, matches := range allMatches {
		uqInfo := ConstraintInfo{Type: "UNIQUE"}
		resultMap := make(map[string]string)
		for j, name := range reUQ.SubexpNames() {
			if j != 0 && name != "" && j < len(matches) {
				resultMap[name] = matches[j]
			}
		}
		uqInfo.Name = strings.Trim(resultMap["name"], "\"`[]")
		if uqInfo.Name == "" { // Beri nama default jika tidak ada
			uqInfo.Name = fmt.Sprintf("uq_%s_auto_%d", tableName, i+1)
		}
		colsStr := resultMap["cols"]
		for _, rawCol := range strings.Split(colsStr, ",") {
			if unquoted := strings.Trim(strings.TrimSpace(rawCol), "\"`[]"); unquoted != "" {
				uqInfo.Columns = append(uqInfo.Columns, unquoted)
			}
		}
		if len(uqInfo.Columns) > 0 {
			// Urutan kolom UNIQUE dari DDL dipertahankan.
			uniques = append(uniques, uqInfo)
		}
	}
	return uniques
}

func parseSQLiteChecksFromDDL(sql string, tableName string) []ConstraintInfo {
	var checks []ConstraintInfo
	reCheck := regexp.MustCompile(`(?i)CONSTRAINT\s+("?(?P<name>\w+)"?)\s+CHECK\s*\((?P<expr>.*?)\)(?:\s*,|\s*\)|$)`)
	allMatches := reCheck.FindAllStringSubmatch(sql, -1)
	for i, matches := range allMatches {
		chkInfo := ConstraintInfo{Type: "CHECK"}
		resultMap := make(map[string]string)
		for j, name := range reCheck.SubexpNames() {
			if j != 0 && name != "" && j < len(matches) {
				resultMap[name] = matches[j]
			}
		}
		chkInfo.Name = strings.Trim(resultMap["name"], "\"`[]")
		if chkInfo.Name == "" { // Beri nama default jika tidak ada
			chkInfo.Name = fmt.Sprintf("chk_%s_auto_%d", tableName, i+1)
		}
		chkInfo.Definition = strings.TrimSpace(resultMap["expr"])
		if chkInfo.Definition != "" { // Nama boleh kosong jika inline, tapi definisi harus ada
			checks = append(checks, chkInfo)
		}
	}
	return checks
}
