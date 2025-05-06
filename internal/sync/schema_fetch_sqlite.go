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

// --- SQLite Specific Fetching ---

func (s *SchemaSyncer) getSQLiteColumns(ctx context.Context, db *gorm.DB, table string) ([]ColumnInfo, error) {
	var columnsData []struct { Cid int; Name string; Type string; NotNull int; DfltValue sql.NullString; Pk int }
	var count int64
	errTbl := db.WithContext(ctx).Raw("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&count).Error
	if errTbl != nil { return nil, fmt.Errorf("sqlite check table exists failed: %w", errTbl) }
	if count == 0 {	return []ColumnInfo{}, nil }

	if err := db.WithContext(ctx).Raw("PRAGMA table_info(?)", table).Scan(&columnsData).Error; err != nil {
		return nil, fmt.Errorf("sqlite columns query failed for %s: %w", table, err)
	}
	result := make([]ColumnInfo, 0, len(columnsData))
	for i, c := range columnsData {
		isAutoIncrement := c.Pk > 0 && strings.ToUpper(c.Type) == "INTEGER"
		colInfo := ColumnInfo{ Name: c.Name, Type: c.Type, IsNullable: c.NotNull == 0, IsPrimary: c.Pk > 0, DefaultValue: c.DfltValue, AutoIncrement: isAutoIncrement, OrdinalPosition: i, }
		mappedType, mapErr := s.mapDataType(colInfo.Type)
		if mapErr != nil { s.logger.Warn("Mapping failed", zap.Error(mapErr)); mappedType = colInfo.Type }
		colInfo.MappedType = mappedType; result = append(result, colInfo)
	}
	return result, nil
}


func (s *SchemaSyncer) getSQLiteIndexes(ctx context.Context, db *gorm.DB, table string) ([]IndexInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "sqlite"))
	log.Debug("Fetching SQLite indexes")
	indexes := make([]IndexInfo, 0)
	idxMap := make(map[string]*IndexInfo)

	var indexList []struct { Seq int; Name string; Unique int; Origin string; Partial int; }
	var count int64
	errTbl := db.WithContext(ctx).Raw("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&count).Error
	if errTbl != nil { return nil, fmt.Errorf("sqlite check table exists failed: %w", errTbl) }
	if count == 0 {	return indexes, nil }

	if err := db.WithContext(ctx).Raw("PRAGMA index_list(?)", table).Scan(&indexList).Error; err != nil {
		return nil, fmt.Errorf("sqlite pragma index_list failed for %s: %w", table, err)
	}
	for _, idxItem := range indexList {
		if strings.HasPrefix(idxItem.Name, "sqlite_autoindex_") { continue }
		var indexInfoList []struct { SeqNo int; Cid int; Name string; }
		queryInfo := fmt.Sprintf("PRAGMA index_info(%s)", utils.QuoteIdentifier(idxItem.Name, "sqlite")) // Gunakan utils
		if err := db.WithContext(ctx).Raw(queryInfo).Scan(&indexInfoList).Error; err != nil { log.Warn("sqlite pragma index_info failed", zap.String("idx", idxItem.Name), zap.Error(err)); continue }
		if len(indexInfoList) > 0 {
			idx := &IndexInfo{ Name: idxItem.Name, Columns: make([]string, len(indexInfoList)), IsUnique: idxItem.Unique == 1, IsPrimary: idxItem.Origin == "pk", }
			validCols := 0
			for _, colInfo := range indexInfoList { if colInfo.SeqNo >= 0 && colInfo.SeqNo < len(idx.Columns) && colInfo.Name != "" { idx.Columns[colInfo.SeqNo] = colInfo.Name; validCols++ } else { log.Warn("Invalid index_info data", zap.Any("info", colInfo)) } }
			if validCols < len(idx.Columns) { cleaned := make([]string, 0, validCols); for _, c := range idx.Columns { if c != "" { cleaned = append(cleaned, c) } }; idx.Columns = cleaned }
			if len(idx.Columns) > 0 { idxMap[idx.Name] = idx }
		}
	}

	for _, idx := range idxMap { sort.Strings(idx.Columns); indexes = append(indexes, *idx) } // Gunakan sort
	sort.Slice(indexes, func(i, j int) bool { return indexes[i].Name < indexes[j].Name }) // Gunakan sort
	return indexes, nil
}


func (s *SchemaSyncer) getSQLiteConstraints(ctx context.Context, db *gorm.DB, table string) ([]ConstraintInfo, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", "sqlite"))
	log.Debug("Fetching SQLite constraints")
	constraints := make([]ConstraintInfo, 0)
	consMap := make(map[string]*ConstraintInfo)

	// 1. Foreign Keys
	var fkList []struct { ID int; Seq int; Table string; From string; To string; OnUpdate string; OnDelete string; Match string; }
	var count int64
	errTbl := db.WithContext(ctx).Raw("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&count).Error
	if errTbl != nil { return nil, fmt.Errorf("sqlite check table exists failed: %w", errTbl) }
	if count == 0 {	return constraints, nil }

	if err := db.WithContext(ctx).Raw("PRAGMA foreign_key_list(?)", table).Scan(&fkList).Error; err != nil {
		if !strings.Contains(err.Error(), "no such module") { log.Warn("sqlite pragma foreign_key_list failed", zap.Error(err)) }
	} else {
		fkGroup := make(map[int]*ConstraintInfo)
		for _, fk := range fkList {
			if _, ok := fkGroup[fk.ID]; !ok { fkName := fmt.Sprintf("fk_%s_foreign_%d", table, fk.ID); fkGroup[fk.ID] = &ConstraintInfo{ Name: fkName, Type: "FOREIGN KEY", Columns: make([]string, 0), ForeignTable: fk.Table, ForeignColumns: make([]string, 0), OnUpdate: fk.OnUpdate, OnDelete: fk.OnDelete } }
			fkGroup[fk.ID].Columns = append(fkGroup[fk.ID].Columns, fk.From); fkGroup[fk.ID].ForeignColumns = append(fkGroup[fk.ID].ForeignColumns, fk.To)
		}
		for _, fkCons := range fkGroup { consMap[fkCons.Name] = fkCons }
	}

	// 2. Unique Constraints (dari Indeks Unik)
	indexes, _ := s.getIndexes(ctx, db, "sqlite", table) // Panggil lagi
	for _, idx := range indexes {
		if idx.IsUnique && !idx.IsPrimary { if _, ok := consMap[idx.Name]; !ok { consMap[idx.Name] = &ConstraintInfo{ Name: idx.Name, Type: "UNIQUE", Columns: idx.Columns, } } }
	}

	// 3. CHECK & PRIMARY KEY (Perlu parsing CREATE TABLE statement - Tidak diimplementasikan)
	log.Warn("CHECK and explicit PRIMARY KEY constraint detection for SQLite requires parsing CREATE TABLE statement (not implemented).")


	for _, cons := range consMap {
		sort.Strings(cons.Columns); if cons.Type == "FOREIGN KEY" { sort.Strings(cons.ForeignColumns) }; constraints = append(constraints, *cons) // Gunakan sort
	}
	sort.Slice(constraints, func(i, j int) bool { return constraints[i].Name < constraints[j].Name }) // Gunakan sort
	return constraints, nil
}