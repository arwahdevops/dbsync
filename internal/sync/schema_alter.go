// internal/sync/schema_alter.go
package sync

import (
	"fmt"
	"strings"

	"go.uber.org/multierr" // Digunakan untuk menggabungkan error
	"go.uber.org/zap"
	// utils digunakan secara tidak langsung oleh helper DDL yang dipanggil dari sini
	// Jika utils.QuoteIdentifier dipanggil langsung di sini, impor utils akan dibutuhkan.
)

// generateAlterDDLs menghasilkan DDL untuk mengubah skema tujuan agar cocok dengan sumber.
// Ini adalah fungsi koordinator utama untuk strategi ALTER.
// Mengembalikan:
// - alterColumnDDLsJoined: String tunggal berisi semua DDL ALTER COLUMN (ADD/DROP/MODIFY), dipisah ';'.
// - finalIndexDDLs: Slice string berisi DDL DROP INDEX dan CREATE INDEX.
// - finalConstraintDDLs: Slice string berisi DDL DROP CONSTRAINT dan ADD CONSTRAINT.
// - err: Error jika terjadi kegagalan fatal atau gabungan error non-fatal dalam proses generasi.
func (s *SchemaSyncer) generateAlterDDLs(
	table string,
	srcColumns []ColumnInfo, srcIndexes []IndexInfo, srcConstraints []ConstraintInfo,
	dstColumns []ColumnInfo, dstIndexes []IndexInfo, dstConstraints []ConstraintInfo,
) (
	alterColumnDDLsJoined string,
	finalIndexDDLs []string,
	finalConstraintDDLs []string,
	err error, // Menggunakan multierr.Error untuk mengumpulkan beberapa error jika ada
) {
	log := s.logger.With(zap.String("table", table), zap.String("strategy", "alter"), zap.String("dst_dialect", s.dstDialect))
	log.Info("Starting generation of ALTER DDLs for table.")

	var collectedAlterColumnDDLs []string
	finalIndexDDLs = make([]string, 0)
	finalConstraintDDLs = make([]string, 0)
	var accumulatedErrors error // Menggunakan error tunggal yang bisa di-append dengan multierr

	// Buat map untuk pencarian cepat objek sumber dan tujuan
	srcColMap := make(map[string]ColumnInfo)
	for _, c := range srcColumns {
		srcColMap[c.Name] = c
	}
	dstColMap := make(map[string]ColumnInfo)
	for _, c := range dstColumns {
		dstColMap[c.Name] = c
	}
	srcIdxMap := make(map[string]IndexInfo)
	for _, i := range srcIndexes {
		srcIdxMap[i.Name] = i
	}
	dstIdxMap := make(map[string]IndexInfo)
	for _, i := range dstIndexes {
		dstIdxMap[i.Name] = i
	}
	srcConsMap := make(map[string]ConstraintInfo)
	for _, c := range srcConstraints {
		srcConsMap[c.Name] = c
	}
	dstConsMap := make(map[string]ConstraintInfo)
	for _, c := range dstConstraints {
		dstConsMap[c.Name] = c
	}

	// --- Tahap 1: Hasilkan DDL DROP untuk objek di tujuan yang tidak ada di sumber atau dimodifikasi ---
	// Urutan drop: FK, Constraint lain (UNIQUE, CHECK), baru Indeks.
	log.Debug("Phase 1: Generating DROP DDLs for constraints and indexes to be removed or modified.")

	constraintsToDrop := []ConstraintInfo{}
	constraintsToRecreate := map[string]ConstraintInfo{} // map[srcName]srcConstraintInfo

	for dstConsName, dc := range dstConsMap {
		if dc.Type == "PRIMARY KEY" {
			log.Debug("Skipping explicit DROP/ADD for PRIMARY KEY constraint in ALTER strategy.", zap.String("constraint", dstConsName))
			continue
		}
		sc, srcConsExists := srcConsMap[dstConsName]
		if !srcConsExists {
			log.Info("Constraint exists in destination but not in source, marking for DROP.", zap.String("constraint", dstConsName), zap.String("type", dc.Type))
			constraintsToDrop = append(constraintsToDrop, dc)
		} else {
			if s.needsConstraintModification(sc, dc, log.With(zap.String("constraint_context", sc.Name))) {
				log.Info("Constraint needs modification, marking for DROP+ADD.", zap.String("constraint", dstConsName), zap.String("type", dc.Type))
				constraintsToDrop = append(constraintsToDrop, dc)
				constraintsToRecreate[sc.Name] = sc
			} else {
				log.Debug("Constraint exists in both and does not need modification.", zap.String("constraint", dstConsName))
			}
		}
	}

	for _, cons := range constraintsToDrop {
		ddl, dropErr := s.generateDropConstraintDDL(table, cons)
		if dropErr != nil {
			errLog := fmt.Errorf("failed to generate DROP CONSTRAINT DDL for '%s' (%s): %w", cons.Name, cons.Type, dropErr)
			log.Error("Error generating DROP CONSTRAINT DDL.", zap.Error(errLog))
			accumulatedErrors = multierr.Append(accumulatedErrors, errLog)
		} else if ddl != "" {
			finalConstraintDDLs = append(finalConstraintDDLs, ddl)
			log.Debug("Generated DROP CONSTRAINT DDL.", zap.String("ddl", ddl))
		}
	}

	indexesToDrop := []IndexInfo{}
	indexesToRecreate := map[string]IndexInfo{}

	for dstIdxName, di := range dstIdxMap {
		if di.IsPrimary {
			log.Debug("Skipping explicit DROP/ADD for PRIMARY KEY index in ALTER strategy.", zap.String("index", dstIdxName))
			continue
		}
		si, srcIdxExists := srcIdxMap[dstIdxName]
		if !srcIdxExists {
			log.Info("Index exists in destination but not in source, marking for DROP.", zap.String("index", dstIdxName))
			indexesToDrop = append(indexesToDrop, di)
		} else {
			if s.needsIndexModification(si, di, log.With(zap.String("index_context", si.Name))) {
				log.Info("Index needs modification, marking for DROP+ADD.", zap.String("index", dstIdxName))
				indexesToDrop = append(indexesToDrop, di)
				indexesToRecreate[si.Name] = si
			} else {
				log.Debug("Index exists in both and does not need modification.", zap.String("index", dstIdxName))
			}
		}
	}

	for _, idx := range indexesToDrop {
		ddl, dropErr := s.generateDropIndexDDL(table, idx)
		if dropErr != nil {
			errLog := fmt.Errorf("failed to generate DROP INDEX DDL for '%s': %w", idx.Name, dropErr)
			log.Error("Error generating DROP INDEX DDL.", zap.Error(errLog))
			accumulatedErrors = multierr.Append(accumulatedErrors, errLog)
		} else if ddl != "" {
			finalIndexDDLs = append(finalIndexDDLs, ddl)
			log.Debug("Generated DROP INDEX DDL.", zap.String("ddl", ddl))
		}
	}

	// --- Tahap 2: Hasilkan DDL Kolom (ADD, DROP, MODIFY) ---
	log.Debug("Phase 2: Generating ADD/DROP/MODIFY COLUMN DDLs.")

	for dstColName, dc := range dstColMap {
		if _, srcColExists := srcColMap[dstColName]; !srcColExists {
			log.Info("Column exists in destination but not in source, marking for DROP.", zap.String("column", dstColName))
			ddl, dropErr := s.generateDropColumnDDL(table, dc)
			if dropErr != nil {
				errLog := fmt.Errorf("failed to generate DROP COLUMN DDL for '%s': %w", dc.Name, dropErr)
				log.Error("Error generating DROP COLUMN DDL.", zap.Error(errLog))
				accumulatedErrors = multierr.Append(accumulatedErrors, errLog)
			} else if ddl != "" {
				collectedAlterColumnDDLs = append(collectedAlterColumnDDLs, ddl)
				log.Debug("Generated DROP COLUMN DDL.", zap.String("ddl", ddl))
			}
		}
	}

	// srcColumns seharusnya sudah terurut berdasarkan OrdinalPosition dari fetcher
	// Jika tidak, perlu diurutkan di sini:
	// sortedSrcColumns := make([]ColumnInfo, len(srcColumns))
	// copy(sortedSrcColumns, srcColumns)
	// sort.Slice(sortedSrcColumns, func(i, j int) bool {
	// 	return sortedSrcColumns[i].OrdinalPosition < sortedSrcColumns[j].OrdinalPosition
	// })

	for _, sc := range srcColumns { // Menggunakan srcColumns langsung (asumsi sudah terurut)
		dc, dstColExists := dstColMap[sc.Name]
		columnLog := log.With(zap.String("column_context", sc.Name))
		if !dstColExists {
			columnLog.Info("Column exists in source but not in destination, marking for ADD.")
			ddl, addErr := s.generateAddColumnDDL(table, sc)
			if addErr != nil {
				errLog := fmt.Errorf("failed to generate ADD COLUMN DDL for '%s': %w", sc.Name, addErr)
				columnLog.Error("Error generating ADD COLUMN DDL.", zap.Error(errLog))
				accumulatedErrors = multierr.Append(accumulatedErrors, errLog)
			} else if ddl != "" {
				collectedAlterColumnDDLs = append(collectedAlterColumnDDLs, ddl)
				columnLog.Debug("Generated ADD COLUMN DDL.", zap.String("ddl", ddl))
			}
		} else {
			modifyDDLs := s.generateModifyColumnDDLs(table, sc, dc, columnLog)
			if len(modifyDDLs) > 0 {
				columnLog.Info("Column needs modification, DDLs generated.", zap.Int("num_modify_ddls", len(modifyDDLs)))
				collectedAlterColumnDDLs = append(collectedAlterColumnDDLs, modifyDDLs...)
			} else {
				columnLog.Debug("Column exists in both and does not need modification.")
			}
		}
	}
	if len(collectedAlterColumnDDLs) > 0 {
		alterColumnDDLsJoined = strings.Join(collectedAlterColumnDDLs, ";\n")
		if !strings.HasSuffix(alterColumnDDLsJoined, ";") {
			alterColumnDDLsJoined += ";"
		}
		log.Debug("Collected ALTER COLUMN DDLs (joined).", zap.String("joined_ddl_preview", truncateForLog(alterColumnDDLsJoined, 100)))
	}

	// --- Tahap 3: Hasilkan DDL ADD untuk objek baru atau yang dibuat ulang ---
	log.Debug("Phase 3: Generating ADD DDLs for new/recreated constraints and indexes.")

	indexesToAdd := []IndexInfo{}
	for srcIdxName, si := range srcIdxMap {
		if si.IsPrimary {
			continue
		}
		_, dstIdxExists := dstIdxMap[srcIdxName]
		_, needsRecreate := indexesToRecreate[srcIdxName]
		if !dstIdxExists || needsRecreate {
			log.Info("Marking index for ADD (new or recreated).", zap.String("index", srcIdxName))
			indexesToAdd = append(indexesToAdd, si)
		}
	}
	addIndexDDLs := s.generateCreateIndexDDLs(table, indexesToAdd)
	finalIndexDDLs = append(finalIndexDDLs, addIndexDDLs...)
	if len(addIndexDDLs) > 0 {
		log.Debug("Generated ADD INDEX DDLs.", zap.Int("count", len(addIndexDDLs)))
	}

	constraintsToAdd := []ConstraintInfo{}
	for srcConsName, sc := range srcConsMap {
		if sc.Type == "PRIMARY KEY" {
			continue
		}
		_, dstConsExists := dstConsMap[srcConsName]
		_, needsRecreate := constraintsToRecreate[srcConsName]
		if !dstConsExists || needsRecreate {
			log.Info("Marking constraint for ADD (new or recreated).", zap.String("constraint", srcConsName), zap.String("type", sc.Type))
			constraintsToAdd = append(constraintsToAdd, sc)
		}
	}
	addConstraintDDLs := s.generateAddConstraintDDLs(table, constraintsToAdd)
	finalConstraintDDLs = append(finalConstraintDDLs, addConstraintDDLs...)
	if len(addConstraintDDLs) > 0 {
		log.Debug("Generated ADD CONSTRAINT DDLs.", zap.Int("count", len(addConstraintDDLs)))
	}

	if accumulatedErrors != nil {
		log.Error("Finished generating ALTER DDLs with errors for table.", zap.Error(accumulatedErrors))
		return alterColumnDDLsJoined, finalIndexDDLs, finalConstraintDDLs, accumulatedErrors
	}

	if alterColumnDDLsJoined == "" && len(finalIndexDDLs) == 0 && len(finalConstraintDDLs) == 0 {
		log.Info("No schema differences found requiring ALTER DDLs for table.")
	} else {
		log.Info("Finished generating ALTER DDLs successfully for table.",
			zap.Bool("has_column_alters", alterColumnDDLsJoined != ""),
			zap.Int("num_index_ddls_total", len(finalIndexDDLs)),           // Termasuk DROP dan ADD
			zap.Int("num_constraint_ddls_total", len(finalConstraintDDLs)), // Termasuk DROP dan ADD
		)
	}
	return alterColumnDDLsJoined, finalIndexDDLs, finalConstraintDDLs, nil
}
