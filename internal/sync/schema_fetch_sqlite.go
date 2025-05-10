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

// --- SQLite Specific Fetching ---

func (s *SchemaSyncer) getSQLiteColumns(ctx context.Context, db *gorm.DB, table string) ([]ColumnInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "sqlite"), zap.String("action", "getSQLiteColumns"))

	var count int64
	errTbl := db.WithContext(ctx).Raw("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&count).Error
	if errTbl != nil {
		return nil, fmt.Errorf("sqlite check table existence failed for '%s': %w", table, errTbl)
	}
	if count == 0 {
		log.Warn("Table not found.")
		return []ColumnInfo{}, nil
	}

	// Coba PRAGMA table_xinfo (SQLite >= 3.31.0) untuk info kolom generated
	var columnsDataXInfo []struct {
		Cid       int            `gorm:"column:cid"`
		Name      string         `gorm:"column:name"`
		Type      string         `gorm:"column:type"`
		NotNull   int            `gorm:"column:notnull"`
		DfltValue sql.NullString `gorm:"column:dflt_value"`
		Pk        int            `gorm:"column:pk"`
		Hidden    int            `gorm:"column:hidden"` // 0: normal, 1: hidden by ALTER, 2: STORED gen, 3: VIRTUAL gen
		// Tidak ada kolom 'expr' langsung di PRAGMA table_xinfo.
		// Ekspresi harus diambil dari CREATE TABLE statement.
	}

	var createTableSQL string // Untuk mengambil ekspresi generated column
	errCreateSQL := db.WithContext(ctx).Raw("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&createTableSQL).Error
	if errCreateSQL != nil {
		log.Warn("Could not fetch CREATE TABLE statement for SQLite, generated column expressions might be missed.", zap.Error(errCreateSQL))
		createTableSQL = "" // Lanjutkan tanpa itu jika gagal
	}


	errXInfo := db.WithContext(ctx).Raw(fmt.Sprintf("PRAGMA table_xinfo(%s)", utils.QuoteIdentifier(table, "sqlite"))).Scan(&columnsDataXInfo).Error

	if errXInfo == nil && len(columnsDataXInfo) > 0 {
		log.Debug("Using PRAGMA table_xinfo for column details.")
		result := make([]ColumnInfo, 0, len(columnsDataXInfo))
		for _, c := range columnsDataXInfo {
			isGenerated := c.Hidden == 2 || c.Hidden == 3 // 2 for STORED, 3 for VIRTUAL
			var generationExpr sql.NullString
			var finalType = c.Type // Tipe dasar dari pragma

			if isGenerated && createTableSQL != "" {
				// Coba parse ekspresi dari CREATE TABLE statement
				// Ini bisa kompleks, untuk sekarang kita coba regex sederhana
				// Contoh: "col_gen" TEXT GENERATED ALWAYS AS (UPPER(name)) STORED
				// Regex mencari: `colName` (spasi) `type` (spasi) `GENERATED ... AS` (spasi) `(expression)`
				// Perlu hati-hati dengan quoting nama kolom.
				// Untuk kesederhanaan, kita akan mengasumsikan `c.Type` dari `table_xinfo` adalah tipe dasar yang benar.
				// Dan kita akan coba ekstrak ekspresinya.
				// Regex untuk mengambil ekspresi setelah "AS" dalam kurung
				// Ini akan menangkap ekspresi dalam kurung setelah "GENERATED ALWAYS AS"
				// Perlu nama kolom yang di-quote dengan benar atau tidak di-quote sama sekali dalam regex.
				// Mengingat nama kolom bisa di-quote atau tidak, ini rumit.
				// Alternatif: Biarkan `Type` dari `table_xinfo` dan catat bahwa ekspresi tidak diekstrak
				// atau biarkan `ColumnInfo.Type` menjadi string lengkap dari DDL jika bisa diekstrak.
				//
				// Pendekatan saat ini: Gunakan tipe dari `table_xinfo` sebagai tipe dasar.
				// Ekspresi tidak diekstrak secara otomatis oleh pragma ini.
				// Kita bisa mencoba mengambilnya dari `sqlite_master.sql` untuk kolom ini.
				// Ini adalah penyederhanaan, parsing SQL lengkap sangat rumit.
				//
				// Untuk contoh ini, kita akan menandai IsGenerated dan membiarkan Type dari xinfo.
				// MappedType akan diisi nanti. Jika IsGenerated=true, Type asli sumber mungkin perlu
				// menyertakan ekspresi jika sinkronisasi ke DB lain yang mendukungnya.
				// Kita akan coba ekstrak ekspresi dari createTableSQL
				// Pola: col_name ANY_TYPE GENERATED ALWAYS AS (expression) [STORED|VIRTUAL]
				// Kita perlu membuat regex yang lebih spesifik untuk nama kolom c.Name
				// dan menangkap (expression)
				// Contoh sederhana (mungkin perlu disempurnakan untuk quoting dan case):
				// `(?i)\b`+regexp.QuoteMeta(c.Name)+`\b\s+\w+(\s*\([^)]*\))?\s+GENERATED\s+ALWAYS\s+AS\s*\((.*?)\)\s*(?:STORED|VIRTUAL)?`
				// Ini masih sangat rumit. Untuk sekarang, kita tidak akan mengisi GenerationExpression dari parsing.
				// Kita akan mengandalkan `extractBaseTypeFromGenerated` di `compare_columns.go` jika `col.Type`
				// entah bagaimana sudah mengandung ekspresi.
				// Namun, `table_xinfo` mengembalikan tipe dasar, bukan definisi lengkap dengan ekspresi.
				// Jadi, kita perlu mendapatkan definisi lengkap dari CREATE TABLE statement.

				if expr := s.extractSQLiteGeneratedColumnExpr(createTableSQL, c.Name); expr != "" {
					generationExpr.String = expr
					generationExpr.Valid = true
					// Jika kita dapat ekspresi, Type di ColumnInfo idealnya adalah definisi lengkap
					// "BASE_TYPE GENERATED ALWAYS AS (EXPR) STORED/VIRTUAL"
					// tapi `c.Type` dari table_xinfo adalah tipe dasar.
					// Kita simpan tipe dasar di `finalType` dan akan mengisi ekspresi di `ColumnInfo`.
					// Saat DDL dibuat, kita akan gabungkan lagi.
					// Untuk `compare_columns`, MappedType akan jadi tipe dasar.
					log.Debug("Found generated column expression via DDL parsing.",
						zap.String("column", c.Name), zap.String("expression", expr))
				} else {
					log.Warn("Could not extract generation expression for generated SQLite column from DDL. Type comparison might be affected.",
						zap.String("column", c.Name))
				}
			}


			isAutoIncrement := c.Pk > 0 && strings.ToUpper(finalType) == "INTEGER" && !isGenerated
			// Jika PK dan tipe INTEGER dan BUKAN generated, itu bisa auto-increment (ROWID alias)

			colInfo := ColumnInfo{
				Name:                 c.Name,
				Type:                 finalType, // Tipe dasar dari table_xinfo
				MappedType:           "",      // Akan diisi oleh SchemaSyncer
				IsNullable:           c.NotNull == 0,
				IsPrimary:            c.Pk > 0,
				IsGenerated:          isGenerated,
				DefaultValue:         c.DfltValue,
				AutoIncrement:        isAutoIncrement,
				OrdinalPosition:      c.Cid,
				GenerationExpression: generationExpr, // Simpan ekspresi jika ditemukan
				// Length, Precision, Scale, Collation, Comment tidak langsung tersedia dari PRAGMA table_xinfo
				// Mungkin perlu parsing tipe lebih lanjut atau info dari CREATE TABLE. Untuk sekarang, biarkan kosong.
				Length:    sql.NullInt64{},
				Precision: sql.NullInt64{},
				Scale:     sql.NullInt64{},
				Collation: sql.NullString{}, // SQLite collation per-kolom ada di definisi CREATE TABLE
				Comment:   sql.NullString{}, // SQLite tidak punya comment kolom native
			}
			result = append(result, colInfo)
		}
		log.Debug("Fetched column info successfully using table_xinfo.", zap.Int("column_count", len(result)))
		return result, nil
	}

	// Fallback ke PRAGMA table_info jika table_xinfo gagal atau tidak mengembalikan data
	if errXInfo != nil {
		log.Warn("PRAGMA table_xinfo failed or returned no data, falling back to PRAGMA table_info. Generated column info will be missed.", zap.Error(errXInfo))
	} else if len(columnsDataXInfo) == 0 {
		log.Debug("PRAGMA table_xinfo returned no data, falling back to PRAGMA table_info.")
	}


	var columnsDataTableInfo []struct {
		Cid       int            `gorm:"column:cid"`
		Name      string         `gorm:"column:name"`
		Type      string         `gorm:"column:type"`
		NotNull   int            `gorm:"column:notnull"`
		DfltValue sql.NullString `gorm:"column:dflt_value"`
		Pk        int            `gorm:"column:pk"`
	}

	err := db.WithContext(ctx).Raw(fmt.Sprintf("PRAGMA table_info(%s)", utils.QuoteIdentifier(table, "sqlite"))).Scan(&columnsDataTableInfo).Error
	if err != nil {
		return nil, fmt.Errorf("sqlite PRAGMA table_info failed for table '%s': %w", table, err)
	}

	result := make([]ColumnInfo, 0, len(columnsDataTableInfo))
	for _, c := range columnsDataTableInfo {
		// Dengan table_info, kita tidak bisa tahu IsGenerated atau GenerationExpression.
		isAutoIncrement := c.Pk > 0 && strings.ToUpper(c.Type) == "INTEGER"
		colInfo := ColumnInfo{
			Name:            c.Name,
			Type:            c.Type,
			MappedType:      "",
			IsNullable:      c.NotNull == 0,
			IsPrimary:       c.Pk > 0,
			IsGenerated:     false, // Tidak bisa dideteksi dari table_info
			DefaultValue:    c.DfltValue,
			AutoIncrement:   isAutoIncrement,
			OrdinalPosition: c.Cid,
			Length:          sql.NullInt64{}, Precision: sql.NullInt64{}, Scale: sql.NullInt64{},
			Collation: sql.NullString{}, Comment: sql.NullString{},
		}
		result = append(result, colInfo)
	}
	log.Debug("Fetched column info successfully using table_info (fallback).", zap.Int("column_count", len(result)))
	return result, nil
}

// extractSQLiteGeneratedColumnExpr mencoba mengekstrak ekspresi kolom generated dari DDL CREATE TABLE.
// Ini adalah parser sederhana dan mungkin tidak mencakup semua kasus.
func (s *SchemaSyncer) extractSQLiteGeneratedColumnExpr(createTableSQL, columnName string) string {
	if createTableSQL == "" {
		return ""
	}
	// Regex mencari: `quoted_or_unquoted_column_name` (spasi) `ANY_TYPE` (spasi) `GENERATED ALWAYS AS` (spasi) `(expression_in_parens)`
	// Kita perlu menangani jika columnName di-quote atau tidak dalam DDL.
	// Pola ini mencoba mencocokkan nama kolom diikuti oleh definisi generated.
	// `[^,)]+` menangkap tipe data (segala sesuatu sampai koma atau tutup kurung berikutnya).
	// `\((.*?)\)` menangkap ekspresi di dalam kurung (non-greedy).
	// Pola ini dibuat lebih fleksibel untuk menangkap spasi dan variasi casing.
	// Ini masih bisa rapuh untuk DDL yang sangat kompleks.
	// Contoh: "col" TEXT GENERATED ALWAYS AS (UPPER(other_col)) STORED
	//         `"col"` TEXT GENERATED ALWAYS AS (UPPER(other_col)) STORED
	//         [col] TEXT GENERATED ALWAYS AS (UPPER(other_col)) STORED

	// Buat beberapa variasi pola untuk nama kolom (unquoted, double-quoted, backtick-quoted, bracket-quoted)
	patterns := []string{
		// Unquoted name, spasi fleksibel, tipe fleksibel, AS (expression)
		`(?i)\b` + regexp.QuoteMeta(columnName) + `\b\s+[^,)]+\s+GENERATED\s+ALWAYS\s+AS\s*\((.*?)\)`,
		// Double-quoted name
		`(?i)"` + regexp.QuoteMeta(columnName) + `"\s+[^,)]+\s+GENERATED\s+ALWAYS\s+AS\s*\((.*?)\)`,
		// Backtick-quoted name
		`(?i)\x60` + regexp.QuoteMeta(columnName) + `\x60\s+[^,)]+\s+GENERATED\s+ALWAYS\s+AS\s*\((.*?)\)`,
		// Bracket-quoted name
		`(?i)\[` + regexp.QuoteMeta(columnName) + `\]\s+[^,)]+\s+GENERATED\s+ALWAYS\s+AS\s*\((.*?)\)`,
	}

	for _, pattern := range patterns {
		re := regexp.MustCompile(pattern)
		matches := re.FindStringSubmatch(createTableSQL)
		if len(matches) > 1 && matches[1] != "" { // matches[1] adalah grup penangkap untuk ekspresi
			return strings.TrimSpace(matches[1])
		}
	}
	return ""
}


func (s *SchemaSyncer) getSQLiteIndexes(ctx context.Context, db *gorm.DB, table string) ([]IndexInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "sqlite"), zap.String("action", "getSQLiteIndexes"))

	var count int64
	errTbl := db.WithContext(ctx).Raw("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&count).Error
	if errTbl != nil { return nil, fmt.Errorf("sqlite check table exists failed: %w", errTbl) }
	if count == 0 {	return []IndexInfo{}, nil }

	var indexList []struct {
		Seq     int    `gorm:"column:seq"`    // Tidak terlalu berguna untuk ordering index itu sendiri
		Name    string `gorm:"column:name"`   // Nama indeks
		Unique  int    `gorm:"column:unique"` // 0 atau 1
		Origin  string `gorm:"column:origin"` // 'c' (CREATE INDEX), 'u' (UNIQUE constraint), 'pk' (PRIMARY KEY)
		Partial int    `gorm:"column:partial"`// 0 atau 1 (untuk WHERE clause di index)
	}
	err := db.WithContext(ctx).Raw(fmt.Sprintf("PRAGMA index_list(%s)", utils.QuoteIdentifier(table, "sqlite"))).Scan(&indexList).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			log.Debug("No indexes found for table via PRAGMA index_list.")
			return []IndexInfo{}, nil
		}
		return nil, fmt.Errorf("sqlite PRAGMA index_list failed for table '%s': %w", table, err)
	}

	if len(indexList) == 0 {
		log.Debug("No indexes found for table (empty index_list).")
		return []IndexInfo{}, nil
	}

	indexes := make([]IndexInfo, 0, len(indexList))
	for _, idxItem := range indexList {
		// Abaikan autoindex yang dibuat SQLite untuk PK atau UNIQUE constraint
		// kecuali jika kita ingin merepresentasikannya secara eksplisit.
		// 'pk' origin biasanya berarti indeks implisit untuk PK.
		// 'u' origin berarti indeks implisit untuk UNIQUE constraint.
		// Kita hanya tertarik pada 'c' (CREATE INDEX) atau yang perlu direplikasi.
		// Namun, untuk perbandingan, kita mungkin perlu semua indeks non-autoindex.
		if strings.HasPrefix(idxItem.Name, "sqlite_autoindex_") {
			log.Debug("Skipping SQLite auto-generated index.", zap.String("index_name", idxItem.Name))
			continue
		}

		// Dapatkan kolom untuk setiap indeks menggunakan PRAGMA index_info
		var columnsForThisIndex []struct {
			SeqNo    int            `gorm:"column:seqno"`   // Urutan kolom dalam indeks (0-based)
			Cid      sql.NullInt64  `gorm:"column:cid"`     // ID kolom (bisa NULL untuk ekspresi)
			Name     sql.NullString `gorm:"column:name"`    // Nama kolom (bisa NULL untuk ekspresi)
			// Ada juga 'desc' dan 'coll' yang bisa diambil jika perlu
		}
		queryInfo := fmt.Sprintf("PRAGMA index_info(%s)", utils.QuoteIdentifier(idxItem.Name, "sqlite"))
		errInfo := db.WithContext(ctx).Raw(queryInfo).Scan(&columnsForThisIndex).Error
		if errInfo != nil {
			log.Warn("sqlite PRAGMA index_info failed, skipping this index.", zap.String("index_name", idxItem.Name), zap.Error(errInfo))
			continue
		}

		if len(columnsForThisIndex) == 0 {
			log.Warn("PRAGMA index_info returned no columns for a listed index. This is unusual.", zap.String("index_name", idxItem.Name))
			continue
		}

		// Susun kolom berdasarkan SeqNo
		// *** PERBAIKAN: Gunakan map untuk mengumpulkan lalu urutkan berdasarkan SeqNo ***
		tempCols := make(map[int]string)
		maxSeqNo := -1
		hasExpression := false

		for _, colInfo := range columnsForThisIndex {
			colName := ""
			if colInfo.Name.Valid && colInfo.Name.String != "" {
				colName = colInfo.Name.String
			} else {
				// Ini adalah indeks ekspresi.
				// Kita butuh RawDef untuk mendapatkan ekspresinya.
				// Untuk sekarang, kita bisa gunakan placeholder.
				colName = fmt.Sprintf("<expression_at_seq_%d>", colInfo.SeqNo)
				hasExpression = true
			}
			tempCols[colInfo.SeqNo] = colName
			if colInfo.SeqNo > maxSeqNo {
				maxSeqNo = colInfo.SeqNo
			}
		}

		finalColumns := make([]string, maxSeqNo+1)
		for seq, name := range tempCols {
			if seq >= 0 && seq < len(finalColumns) {
				finalColumns[seq] = name
			} else {
				log.Error("Invalid sequence number from PRAGMA index_info, potential data corruption or bug.", zap.Int("seqno", seq), zap.Int("max_seqno_expected", maxSeqNo), zap.String("index_name", idxItem.Name))
				// Mungkin perlu menggagalkan di sini atau handle dengan lebih baik
			}
		}
		// Hapus entri kosong jika ada (misalnya jika sequence tidak continuous, tidak umum)
		cleanedFinalColumns := make([]string, 0, len(finalColumns))
		for _, colName := range finalColumns {
			if colName != "" {
				cleanedFinalColumns = append(cleanedFinalColumns, colName)
			}
		}


		var rawDefSQL string
		if hasExpression || idxItem.Partial == 1 { // Jika ada ekspresi atau partial index, ambil DDL
			errDef := db.WithContext(ctx).Raw("SELECT sql FROM sqlite_master WHERE type='index' AND name=?", idxItem.Name).Scan(&rawDefSQL).Error
			if errDef != nil {
				log.Warn("Could not fetch CREATE INDEX statement for index, RawDef will be empty.", zap.String("index_name", idxItem.Name), zap.Error(errDef))
			}
		}


		idx := IndexInfo{
			Name:      idxItem.Name,
			Columns:   cleanedFinalColumns, // Kolom sudah dalam urutan yang benar
			IsUnique:  idxItem.Unique == 1,
			IsPrimary: idxItem.Origin == "pk", // Indeks yang dibuat untuk PK
			RawDef:    rawDefSQL,
		}
		// *** PENTING: Tidak ada `sort.Strings(idx.Columns)` di sini ***

		indexes = append(indexes, idx)
	}

	// Urutkan slice `indexes` secara keseluruhan berdasarkan nama untuk output yang konsisten
	sort.Slice(indexes, func(i, j int) bool { return indexes[i].Name < indexes[j].Name })
	log.Debug("Fetched index info successfully.", zap.Int("index_count", len(indexes)))
	return indexes, nil
}


func (s *SchemaSyncer) getSQLiteConstraints(ctx context.Context, db *gorm.DB, table string) ([]ConstraintInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "sqlite"), zap.String("action", "getSQLiteConstraints"))
	constraints := make([]ConstraintInfo, 0)
	consMap := make(map[string]*ConstraintInfo) // Gunakan map untuk menghindari duplikat constraint dari parsing

	var count int64
	errTbl := db.WithContext(ctx).Raw("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&count).Error
	if errTbl != nil { return nil, fmt.Errorf("sqlite check table exists failed: %w", errTbl) }
	if count == 0 {	return constraints, nil }

	// 1. Foreign Keys from PRAGMA
	var fkList []struct {
		ID       int    `gorm:"column:id"`       // ID grup FK (untuk composite FK)
		Seq      int    `gorm:"column:seq"`      // Urutan kolom dalam composite FK
		Table    string `gorm:"column:table"`    // Tabel referensi
		From     string `gorm:"column:from"`     // Kolom lokal
		To       string `gorm:"column:to"`       // Kolom di tabel referensi
		OnUpdate string `gorm:"column:on_update"`
		OnDelete string `gorm:"column:on_delete"`
		Match    string `gorm:"column:match"`    // Tidak terlalu relevan untuk replikasi DDL
	}
	errFK := db.WithContext(ctx).Raw(fmt.Sprintf("PRAGMA foreign_key_list(%s)", utils.QuoteIdentifier(table, "sqlite"))).Scan(&fkList).Error
	if errFK != nil {
		if !strings.Contains(strings.ToLower(errFK.Error()), "no such module") && errFK != gorm.ErrRecordNotFound {
			log.Warn("sqlite PRAGMA foreign_key_list failed, FOREIGN KEY constraints might be missed.", zap.Error(errFK))
		} else {
			log.Debug("No foreign keys found or PRAGMA foreign_key_list not supported/enabled.")
		}
	} else {
		// Kelompokkan kolom FK berdasarkan ID (untuk composite keys)
		fkGroup := make(map[int][]struct{ Seq int; Local, Foreign string })
		fkDetails := make(map[int]ConstraintInfo) // Simpan detail FK per ID

		for _, fk := range fkList {
			if _, ok := fkDetails[fk.ID]; !ok {
				// Nama FK tidak tersedia dari PRAGMA, kita buat nama sementara.
				// DDL CREATE TABLE mungkin memiliki nama eksplisit.
				fkName := fmt.Sprintf("fk_%s_auto_id%d", table, fk.ID)
				fkDetails[fk.ID] = ConstraintInfo{
					Name:         fkName, // Nama sementara
					Type:         "FOREIGN KEY",
					ForeignTable: fk.Table,
					OnUpdate:     fk.OnUpdate,
					OnDelete:     fk.OnDelete,
				}
				fkGroup[fk.ID] = make([]struct{ Seq int; Local, Foreign string }, 0)
			}
			fkGroup[fk.ID] = append(fkGroup[fk.ID], struct{ Seq int; Local, Foreign string }{
				Seq: fk.Seq, Local: fk.From, Foreign: fk.To,
			})
		}

		for id, details := range fkDetails {
			colPairs := fkGroup[id]
			sort.Slice(colPairs, func(i, j int) bool { return colPairs[i].Seq < colPairs[j].Seq })

			localCols := make([]string, len(colPairs))
			foreignCols := make([]string, len(colPairs))
			for i, pair := range colPairs {
				localCols[i] = pair.Local
				foreignCols[i] = pair.Foreign
			}
			details.Columns = localCols
			details.ForeignColumns = foreignCols
			consMap[details.Name] = &details // Tambahkan ke map utama
		}
	}

	// 2. Ambil DDL CREATE TABLE untuk parse PK, UNIQUE, dan CHECK eksplisit
	var createTableSQL string
	errSQL := db.WithContext(ctx).Raw("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&createTableSQL).Error
	if errSQL != nil {
		log.Warn("Could not fetch CREATE TABLE statement to parse explicit PK/UNIQUE/CHECK constraints.", zap.Error(errSQL))
	} else {
		// Parse Primary Key
		pkInfo := parseSQLitePKFromDDL(createTableSQL)
		if pkInfo.Name != "" { // Jika ada PK eksplisit dari DDL
			if _, exists := consMap[pkInfo.Name]; !exists {
				consMap[pkInfo.Name] = &pkInfo
			} else {
				log.Debug("PK constraint name from DDL parsing conflicts with an existing FK name, prioritizing FK from PRAGMA.", zap.String("name", pkInfo.Name))
			}
		}

		// Parse Unique Constraints
		uniqueInfos := parseSQLiteUniqueFromDDL(createTableSQL)
		for _, uqInfo := range uniqueInfos {
			if _, exists := consMap[uqInfo.Name]; !exists {
				consMap[uqInfo.Name] = &uqInfo
			} else {
				log.Debug("UNIQUE constraint name from DDL parsing conflicts, might be auto-index or FK.", zap.String("name", uqInfo.Name))
			}
		}

		// Parse Check Constraints
		checkInfos := parseSQLiteChecksFromDDL(createTableSQL)
		for _, chkInfo := range checkInfos {
			if _, exists := consMap[chkInfo.Name]; !exists {
				consMap[chkInfo.Name] = &chkInfo
			}
		}
	}

	// Konversi map ke slice
	for _, cons := range consMap {
		constraints = append(constraints, *cons)
	}
	sort.Slice(constraints, func(i, j int) bool { return constraints[i].Name < constraints[j].Name })
	log.Debug("Fetched constraint info successfully.", zap.Int("constraint_count", len(constraints)))
	return constraints, nil
}

// --- Helper Parsing SQLite CREATE TABLE (Revisi dari sebelumnya) ---

func parseSQLitePKFromDDL(sql string) ConstraintInfo {
	// Pola mencari: CONSTRAINT pk_name PRIMARY KEY (col1, col2) atau PRIMARY KEY (col1, col2)
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
		if pkInfo.Name == "" {
			// Tidak ada nama constraint eksplisit, mungkin perlu nama default dari tabel
			// atau biarkan kosong jika ini PK yang didefinisikan inline pada kolom.
			// Untuk konsistensi, jika PK tabel, kita bisa beri nama default.
			// Namun, PRAGMA index_list dengan origin='pk' akan memberi nama indeksnya.
			// Kita fokus pada constraint bernama eksplisit di sini.
			// Jika tidak ada `CONSTRAINT name`, maka ini mungkin PK inline di kolom,
			// yang akan terdeteksi oleh `PRAGMA table_info.pk > 0`.
			// Fungsi ini lebih untuk PK yang didefinisikan di akhir tabel.
			return ConstraintInfo{} // Kembalikan kosong jika tidak ada nama constraint
		}

		colsStr := resultMap["cols"]
		rawCols := strings.Split(colsStr, ",")
		for _, rawCol := range rawCols {
			trimmed := strings.TrimSpace(rawCol)
			unquoted := strings.Trim(trimmed, "\"`[]") // SQLite bisa quote dengan [], `, atau "
			if unquoted != "" {
				pkInfo.Columns = append(pkInfo.Columns, unquoted)
			}
		}
		if len(pkInfo.Columns) > 0 {
			sort.Strings(pkInfo.Columns) // Urutkan untuk konsistensi perbandingan
			return pkInfo
		}
	}
	return ConstraintInfo{} // Kembalikan kosong jika tidak ada match
}


func parseSQLiteUniqueFromDDL(sql string) []ConstraintInfo {
	var uniques []ConstraintInfo
	// Pola mencari: CONSTRAINT uq_name UNIQUE (col1, col2)
	reUQ := regexp.MustCompile(`(?i)CONSTRAINT\s+("?(?P<name>\w+)"?)\s+UNIQUE\s*\((?P<cols>[^)]+)\)`)
	allMatches := reUQ.FindAllStringSubmatch(sql, -1)

	for _, matches := range allMatches {
		uqInfo := ConstraintInfo{Type: "UNIQUE"}
		resultMap := make(map[string]string)
		for i, name := range reUQ.SubexpNames() {
			if i != 0 && name != "" && i < len(matches) {
				resultMap[name] = matches[i]
			}
		}

		uqInfo.Name = strings.Trim(resultMap["name"], "\"`[]")
		if uqInfo.Name == "" { continue } // Harus ada nama constraint

		colsStr := resultMap["cols"]
		rawCols := strings.Split(colsStr, ",")
		for _, rawCol := range rawCols {
			trimmed := strings.TrimSpace(rawCol)
			unquoted := strings.Trim(trimmed, "\"`[]")
			if unquoted != "" {
				uqInfo.Columns = append(uqInfo.Columns, unquoted)
			}
		}
		if len(uqInfo.Columns) > 0 {
			sort.Strings(uqInfo.Columns)
			uniques = append(uniques, uqInfo)
		}
	}
	return uniques
}


func parseSQLiteChecksFromDDL(sql string) []ConstraintInfo {
	var checks []ConstraintInfo
	// Pola mencari: CONSTRAINT chk_name CHECK (expression)
	reCheck := regexp.MustCompile(`(?i)CONSTRAINT\s+("?(?P<name>\w+)"?)\s+CHECK\s*\((?P<expr>.*?)\)(?:\s*,|\s*\)|$)`)
	allMatches := reCheck.FindAllStringSubmatch(sql, -1)

	for _, matches := range allMatches {
		chkInfo := ConstraintInfo{Type: "CHECK"}
		resultMap := make(map[string]string)
		for i, name := range reCheck.SubexpNames() {
			if i != 0 && name != "" && i < len(matches) {
				resultMap[name] = matches[i]
			}
		}

		chkInfo.Name = strings.Trim(resultMap["name"], "\"`[]")
		chkInfo.Definition = strings.TrimSpace(resultMap["expr"])

		if chkInfo.Name != "" && chkInfo.Definition != "" {
			checks = append(checks, chkInfo)
		}
	}
	return checks
}
