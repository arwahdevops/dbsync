package sync

import (
	// "fmt" // <-- HAPUS
	// "reflect" // Tidak perlu di sini
	// "regexp" // Tidak perlu di sini
	// "strconv" // Tidak perlu di sini
	// "strings" // <-- HAPUS

	"go.uber.org/zap"
	// "github.com/arwahdevops/dbsync/internal/utils" // Tidak perlu di sini
)

// --- Logika Inti untuk Strategi ALTER ---

// generateAlterDDLs menghasilkan DDL untuk mengubah skema tujuan agar cocok dengan sumber.
func (s *SchemaSyncer) generateAlterDDLs(table string,
	srcCols []ColumnInfo, srcIdxs []IndexInfo, srcCons []ConstraintInfo,
	dstCols []ColumnInfo, dstIdxs []IndexInfo, dstCons []ConstraintInfo) (
	alterColumnDDLs []string, indexDDLs []string, constraintDDLs []string, err error) {

	log := s.logger.With(zap.String("table", table), zap.String("dialect", s.dstDialect))
	alterColumnDDLs = make([]string, 0)
	indexDDLs = make([]string, 0)      // Holds DROP INDEX and CREATE INDEX
	constraintDDLs = make([]string, 0) // Holds DROP CONSTRAINT and ADD CONSTRAINT

	srcColMap := make(map[string]ColumnInfo); for _, c := range srcCols { srcColMap[c.Name] = c }
	dstColMap := make(map[string]ColumnInfo); for _, c := range dstCols { dstColMap[c.Name] = c }
	srcIdxMap := make(map[string]IndexInfo); for _, i := range srcIdxs { srcIdxMap[i.Name] = i }
	dstIdxMap := make(map[string]IndexInfo); for _, i := range dstIdxs { dstIdxMap[i.Name] = i }
	srcConsMap := make(map[string]ConstraintInfo); for _, c := range srcCons { srcConsMap[c.Name] = c }
	dstConsMap := make(map[string]ConstraintInfo); for _, c := range dstCons { dstConsMap[c.Name] = c }

	// --- Phase 1: Generate DROP statements for constraints/indexes not in source ---
	constraintsToDrop := []ConstraintInfo{}
	indexesToDrop := []IndexInfo{}
	constraintsToModify := []struct{ Src, Dst ConstraintInfo }{}
	indexesToModify := []struct{ Src, Dst IndexInfo }{}

	for name, dc := range dstConsMap {
		if sc, exists := srcConsMap[name]; !exists && dc.Type != "PRIMARY KEY" {
			constraintsToDrop = append(constraintsToDrop, dc)
		} else if exists && dc.Type != "PRIMARY KEY" {
			if s.needsConstraintModification(sc, dc, log) { // Panggil dari schema_compare.go
				constraintsToModify = append(constraintsToModify, struct{ Src, Dst ConstraintInfo }{sc, dc})
			}
		}
	}
	for name, di := range dstIdxMap {
		if si, exists := srcIdxMap[name]; !exists && !di.IsPrimary {
			indexesToDrop = append(indexesToDrop, di)
		} else if exists && !di.IsPrimary {
			if s.needsIndexModification(si, di, log) { // Panggil dari schema_compare.go
				indexesToModify = append(indexesToModify, struct{ Src, Dst IndexInfo }{si, di})
			}
		}
	}

	// Hasilkan DDL Drop (prioritaskan constraint sebelum index)
	for _, c := range constraintsToDrop {
		ddl, dropErr := s.generateDropConstraintDDL(table, c) // Panggil dari schema_ddl_alter.go
		if dropErr == nil { constraintDDLs = append(constraintDDLs, ddl) } else { log.Error("Failed gen drop cons", zap.Error(dropErr)) }
	}
	for _, i := range indexesToDrop {
		ddl, dropErr := s.generateDropIndexDDL(table, i) // Panggil dari schema_ddl_alter.go
		if dropErr == nil { indexDDLs = append(indexDDLs, ddl) } else { log.Error("Failed gen drop idx", zap.Error(dropErr)) }
	}
	// Untuk modifikasi, strategi paling aman adalah Drop + Add
	for _, mod := range constraintsToModify {
		log.Warn("Constraint modification detected, performing DROP+ADD", zap.String("constraint", mod.Src.Name))
		ddlDrop, eDrop := s.generateDropConstraintDDL(table, mod.Dst); if eDrop == nil { constraintDDLs = append(constraintDDLs, ddlDrop) } else { log.Error("Failed gen drop cons (modify)", zap.Error(eDrop)) }
	}
	for _, mod := range indexesToModify {
		log.Warn("Index modification detected, performing DROP+ADD", zap.String("index", mod.Src.Name))
		ddlDrop, eDrop := s.generateDropIndexDDL(table, mod.Dst); if eDrop == nil { indexDDLs = append(indexDDLs, ddlDrop) } else { log.Error("Failed gen drop idx (modify)", zap.Error(eDrop)) }
	}

	// --- Phase 2: Generate Column ADD/DROP/MODIFY ---
	columnsToDrop := []ColumnInfo{}
	columnsToAdd := []ColumnInfo{}
	columnsToModify := []struct{ Src, Dst ColumnInfo }{}

	for name, sc := range srcColMap {
		if dc, exists := dstColMap[name]; !exists {
			columnsToAdd = append(columnsToAdd, sc)
		} else {
			diffs := s.getColumnModifications(sc, dc, log) // Panggil dari schema_compare.go
			if len(diffs) > 0 {
				columnsToModify = append(columnsToModify, struct{ Src, Dst ColumnInfo }{sc, dc})
			}
		}
	}
	for name, dc := range dstColMap {
		if _, exists := srcColMap[name]; !exists {
			columnsToDrop = append(columnsToDrop, dc)
		}
	}
	// Generate column DDLs (Urutan: Add, Modify, Drop)
	for _, col := range columnsToAdd {
		ddl, addErr := s.generateAddColumnDDL(table, col); if addErr == nil { alterColumnDDLs = append(alterColumnDDLs, ddl) } else { log.Error("Failed gen add col", zap.Error(addErr)) }
	}
	for _, mod := range columnsToModify {
		modifyDDLs := s.generateModifyColumnDDLs(table, mod.Src, mod.Dst, log); alterColumnDDLs = append(alterColumnDDLs, modifyDDLs...)
	}
	for _, col := range columnsToDrop {
		ddl, dropErr := s.generateDropColumnDDL(table, col); if dropErr == nil { alterColumnDDLs = append(alterColumnDDLs, ddl) } else { log.Error("Failed gen drop col", zap.Error(dropErr)) }
	}


	// --- Phase 3: Generate ADD statements for constraints/indexes not in destination (or modified) ---
	indexesToAdd := []IndexInfo{}
	constraintsToAdd := []ConstraintInfo{}
    for name, si := range srcIdxMap {
        needsAdd := false
		if _, exists := dstIdxMap[name]; !exists && !si.IsPrimary { needsAdd = true } else { for _, mod := range indexesToModify { if mod.Src.Name == name { needsAdd = true; break } } }
		if needsAdd { indexesToAdd = append(indexesToAdd, si) }
    }
    for name, sc := range srcConsMap {
         needsAdd := false
		 if _, exists := dstConsMap[name]; !exists && sc.Type != "PRIMARY KEY" { needsAdd = true } else { for _, mod := range constraintsToModify { if mod.Src.Name == name { needsAdd = true; break } } }
		 if needsAdd { constraintsToAdd = append(constraintsToAdd, sc) }
    }
	// Append ADD DDLs
	indexDDLs = append(indexDDLs, s.generateCreateIndexDDLs(table, indexesToAdd)...) // Panggil dari schema_ddl_create.go
	constraintDDLs = append(constraintDDLs, s.generateAddConstraintDDLs(table, constraintsToAdd)...) // Panggil dari schema_ddl_create.go


	if len(alterColumnDDLs) == 0 && len(indexDDLs) == 0 && len(constraintDDLs) == 0 {
		log.Info("No schema differences found requiring ALTER.")
	}
	return
}