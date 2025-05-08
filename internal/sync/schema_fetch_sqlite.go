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

	var columnsData []struct {
		Cid       int            `gorm:"column:cid"`
		Name      string         `gorm:"column:name"`
		Type      string         `gorm:"column:type"`
		NotNull   int            `gorm:"column:notnull"`
		DfltValue sql.NullString `gorm:"column:dflt_value"`
		Pk        int            `gorm:"column:pk"`
	}

	err := db.WithContext(ctx).Raw(fmt.Sprintf("PRAGMA table_info(%s)", utils.QuoteIdentifier(table, "sqlite"))).Scan(&columnsData).Error
	if err != nil {
		return nil, fmt.Errorf("sqlite PRAGMA table_info failed for table '%s': %w", table, err)
	}

	result := make([]ColumnInfo, 0, len(columnsData))
	for _, c := range columnsData {
		isAutoIncrement := c.Pk > 0 && strings.ToUpper(c.Type) == "INTEGER"

		colInfo := ColumnInfo{
			Name:            c.Name,
			Type:            c.Type,
			MappedType:      "",
			IsNullable:      c.NotNull == 0,
			IsPrimary:       c.Pk > 0,
			IsGenerated:     false,
			DefaultValue:    c.DfltValue,
			AutoIncrement:   isAutoIncrement,
			OrdinalPosition: c.Cid,
			Length:          sql.NullInt64{},
			Precision:       sql.NullInt64{},
			Scale:           sql.NullInt64{},
			Collation:       sql.NullString{},
			Comment:         sql.NullString{},
		}
		result = append(result, colInfo)
	}
	log.Debug("Fetched column info successfully.", zap.Int("column_count", len(result)))
	return result, nil
}

func (s *SchemaSyncer) getSQLiteIndexes(ctx context.Context, db *gorm.DB, table string) ([]IndexInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "sqlite"), zap.String("action", "getSQLiteIndexes"))
	indexes := make([]IndexInfo, 0)
	idxMap := make(map[string]*IndexInfo)

	var count int64
	errTbl := db.WithContext(ctx).Raw("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&count).Error
	if errTbl != nil { return nil, fmt.Errorf("sqlite check table exists failed: %w", errTbl) }
	if count == 0 {	return indexes, nil }

	var indexList []struct {
		Seq     int    `gorm:"column:seq"`
		Name    string `gorm:"column:name"`
		Unique  int    `gorm:"column:unique"`
		Origin  string `gorm:"column:origin"`
		Partial int    `gorm:"column:partial"`
	}
	err := db.WithContext(ctx).Raw(fmt.Sprintf("PRAGMA index_list(%s)", utils.QuoteIdentifier(table, "sqlite"))).Scan(&indexList).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			log.Debug("No indexes found for table.")
			return indexes, nil
		}
		return nil, fmt.Errorf("sqlite PRAGMA index_list failed for table '%s': %w", table, err)
	}

	if len(indexList) == 0 {
		log.Debug("No indexes found for table.")
		return indexes, nil
	}

	for _, idxItem := range indexList {
		if strings.HasPrefix(idxItem.Name, "sqlite_autoindex_") {
			continue
		}

		var indexInfoList []struct {
			SeqNo int            `gorm:"column:seqno"`
			Cid   int            `gorm:"column:cid"`
			Name  sql.NullString `gorm:"column:name"`
		}
		queryInfo := fmt.Sprintf("PRAGMA index_info(%s)", utils.QuoteIdentifier(idxItem.Name, "sqlite"))
		errInfo := db.WithContext(ctx).Raw(queryInfo).Scan(&indexInfoList).Error
		if errInfo != nil {
			log.Warn("sqlite PRAGMA index_info failed, skipping index.", zap.String("index", idxItem.Name), zap.Error(errInfo))
			continue
		}

		if len(indexInfoList) > 0 {
			idx := &IndexInfo{
				Name:      idxItem.Name,
				Columns:   make([]string, len(indexInfoList)),
				IsUnique:  idxItem.Unique == 1,
				IsPrimary: idxItem.Origin == "pk",
			}
			validCols := 0
			for _, colInfo := range indexInfoList {
				if colInfo.Name.Valid && colInfo.Name.String != "" {
					if colInfo.SeqNo >= 0 && colInfo.SeqNo < len(idx.Columns) {
						idx.Columns[colInfo.SeqNo] = colInfo.Name.String
						validCols++
					} else {
						log.Warn("Invalid SeqNo found in index_info.", zap.Int("seqno", colInfo.SeqNo), zap.String("index", idxItem.Name))
					}
				} else {
					log.Warn("Expression found in index (column name is NULL), column list might be incomplete or represent expressions.", zap.String("index", idxItem.Name))
					if colInfo.SeqNo >= 0 && colInfo.SeqNo < len(idx.Columns) {
						idx.Columns[colInfo.SeqNo] = fmt.Sprintf("<expression_%d>", colInfo.SeqNo)
						validCols++
					}
				}
			}

			if validCols < len(idx.Columns) {
				cleaned := make([]string, 0, validCols)
				for _, c := range idx.Columns {
					if c != "" {
						cleaned = append(cleaned, c)
					}
				}
				idx.Columns = cleaned
			}

			if len(idx.Columns) > 0 {
				sort.Strings(idx.Columns)
				idxMap[idx.Name] = idx
			}
		}
	}

	for _, idx := range idxMap {
		indexes = append(indexes, *idx)
	}
	sort.Slice(indexes, func(i, j int) bool { return indexes[i].Name < indexes[j].Name })
	log.Debug("Fetched index info successfully.", zap.Int("index_count", len(indexes)))
	return indexes, nil
}


func (s *SchemaSyncer) getSQLiteConstraints(ctx context.Context, db *gorm.DB, table string) ([]ConstraintInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "sqlite"), zap.String("action", "getSQLiteConstraints"))
	constraints := make([]ConstraintInfo, 0)
	consMap := make(map[string]*ConstraintInfo)

	var count int64
	errTbl := db.WithContext(ctx).Raw("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&count).Error
	if errTbl != nil { return nil, fmt.Errorf("sqlite check table exists failed: %w", errTbl) }
	if count == 0 {	return constraints, nil }

	// 1. Foreign Keys
	var fkList []struct {
		ID       int    `gorm:"column:id"`
		Seq      int    `gorm:"column:seq"`
		Table    string `gorm:"column:table"`
		From     string `gorm:"column:from"`
		To       string `gorm:"column:to"`
		OnUpdate string `gorm:"column:on_update"`
		OnDelete string `gorm:"column:on_delete"`
		Match    string `gorm:"column:match"`
	}
	errFK := db.WithContext(ctx).Raw(fmt.Sprintf("PRAGMA foreign_key_list(%s)", utils.QuoteIdentifier(table, "sqlite"))).Scan(&fkList).Error
	if errFK != nil {
		if !strings.Contains(strings.ToLower(errFK.Error()), "no such module") && errFK != gorm.ErrRecordNotFound {
			log.Warn("sqlite PRAGMA foreign_key_list failed, FOREIGN KEY constraints might be missed.", zap.Error(errFK))
		} else {
			log.Debug("No foreign keys found or PRAGMA foreign_key_list not supported.")
		}
	} else {
		fkGroup := make(map[int]*ConstraintInfo)
		fkColsGroup := make(map[int][]struct{ Seq int; Local, Foreign string })

		for _, fk := range fkList {
			cons, ok := fkGroup[fk.ID]
			if !ok {
				fkName := fmt.Sprintf("fk_%s_auto_%d", table, fk.ID)
				cons = &ConstraintInfo{
					Name:           fkName,
					Type:           "FOREIGN KEY",
					Columns:        make([]string, 0),
					ForeignTable:   fk.Table,
					ForeignColumns: make([]string, 0),
					OnUpdate:       fk.OnUpdate,
					OnDelete:       fk.OnDelete,
				}
				fkGroup[fk.ID] = cons
				fkColsGroup[fk.ID] = make([]struct{ Seq int; Local, Foreign string }, 0)
			}
			fkColsGroup[fk.ID] = append(fkColsGroup[fk.ID], struct{ Seq int; Local, Foreign string }{
				Seq:     fk.Seq,
				Local:   fk.From,
				Foreign: fk.To,
			})
		}
		for id, cons := range fkGroup {
			cols := fkColsGroup[id]
			sort.Slice(cols, func(i, j int) bool { return cols[i].Seq < cols[j].Seq })
			cons.Columns = make([]string, len(cols))
			cons.ForeignColumns = make([]string, len(cols))
			for i, c := range cols {
				cons.Columns[i] = c.Local
				// PERBAIKAN: Gunakan c.Foreign, bukan c.To
				cons.ForeignColumns[i] = c.Foreign
			}
			consMap[cons.Name] = cons
		}
	}

	// 2. Unique Constraints
	indexes, errIdx := s.getSQLiteIndexes(ctx, db, table)
	if errIdx != nil {
		log.Warn("Failed to get indexes again for UNIQUE constraint detection.", zap.Error(errIdx))
	} else {
		for _, idx := range indexes {
			if idx.IsUnique && !idx.IsPrimary {
				if _, ok := consMap[idx.Name]; !ok {
					consMap[idx.Name] = &ConstraintInfo{
						Name:    idx.Name,
						Type:    "UNIQUE",
						Columns: idx.Columns, // Kolom sudah diurutkan
					}
				} else {
					log.Debug("Index name conflicts with an existing constraint name, skipping UNIQUE constraint generation from index.", zap.String("name", idx.Name))
				}
			}
		}
	}

	// 3. CHECK & PRIMARY KEY Eksplisit (Parsing CREATE TABLE)
	var createSQL string
	errSQL := db.WithContext(ctx).Raw("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&createSQL).Error
	if errSQL != nil {
		log.Warn("Could not fetch CREATE TABLE statement to parse CHECK/PK constraints.", zap.Error(errSQL))
	} else {
		pkCols := parseSQLitePK(createSQL)
		if len(pkCols) > 0 {
			pkName := fmt.Sprintf("pk_%s", table)
			rePKName := regexp.MustCompile(`(?i)constraint\s+("?\w+"?)\s+primary\s+key`)
			if matches := rePKName.FindStringSubmatch(createSQL); len(matches) > 1 {
				pkName = strings.Trim(matches[1], "\"`")
			}
			if _, exists := consMap[pkName]; !exists {
				sort.Strings(pkCols)
				consMap[pkName] = &ConstraintInfo{Name: pkName, Type: "PRIMARY KEY", Columns: pkCols}
			}
		}

		checks := parseSQLiteChecks(createSQL)
		for name, definition := range checks {
			if _, exists := consMap[name]; !exists {
				consMap[name] = &ConstraintInfo{Name: name, Type: "CHECK", Definition: definition}
			}
		}
	}


	for _, cons := range consMap {
		constraints = append(constraints, *cons)
	}
	sort.Slice(constraints, func(i, j int) bool { return constraints[i].Name < constraints[j].Name })
	log.Debug("Fetched constraint info successfully.", zap.Int("constraint_count", len(constraints)))
	return constraints, nil
}

// --- Helper Parsing SQLite CREATE TABLE (Sederhana & Potensial Rapuh) ---
func parseSQLitePK(sql string) []string {
	rePK := regexp.MustCompile(`(?i)primary\s+key\s*\(([^)]+)\)`)
	matches := rePK.FindStringSubmatch(sql)
	if len(matches) > 1 {
		colsStr := matches[1]
		cols := make([]string, 0)
		rawCols := strings.Split(colsStr, ",")
		for _, rawCol := range rawCols {
			trimmed := strings.TrimSpace(rawCol)
			unquoted := strings.Trim(trimmed, "\"`")
			if unquoted != "" {
				cols = append(cols, unquoted)
			}
		}
		return cols
	}
	return []string{}
}

func parseSQLiteChecks(sql string) map[string]string {
	checks := make(map[string]string)
	reCheck := regexp.MustCompile(`(?i)(?:constraint\s+("?\w+"?)\s+)?check\s*\((.*?)\)`)
	allMatches := reCheck.FindAllStringSubmatch(sql, -1)

	for i, match := range allMatches {
		if len(match) >= 3 {
			name := strings.Trim(match[1], "\"`")
			definition := match[2]
			if name == "" {
				name = fmt.Sprintf("check_%d", i+1) // Nama otomatis
			}
			checks[name] = definition
		}
	}
	return checks
}
