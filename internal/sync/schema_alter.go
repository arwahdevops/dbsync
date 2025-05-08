// internal/sync/schema_alter.go
package sync

import (
	"strings" // Diperlukan untuk strings.Join

	"go.uber.org/zap"
	// utils tidak secara langsung digunakan di sini, tapi oleh helper DDL
)

// generateAlterDDLs menghasilkan DDL untuk mengubah skema tujuan agar cocok dengan sumber.
// Ini adalah fungsi koordinator utama untuk strategi ALTER.
func (s *SchemaSyncer) generateAlterDDLs(
	table string,
	srcColumns []ColumnInfo, srcIndexes []IndexInfo, srcConstraints []ConstraintInfo,
	dstColumns []ColumnInfo, dstIndexes []IndexInfo, dstConstraints []ConstraintInfo,
) (
	alterColumnDDLsJoined string, // Akan berisi semua DDL ALTER COLUMN, digabung dengan ;
	finalIndexDDLs []string,      // Berisi DROP INDEX dan CREATE INDEX
	finalConstraintDDLs []string, // Berisi DROP CONSTRAINT dan ADD CONSTRAINT
	err error,
) {
	log := s.logger.With(zap.String("table", table), zap.String("strategy", "alter"), zap.String("dst_dialect", s.dstDialect))
	log.Info("Starting generation of ALTER DDLs")

	// Inisialisasi slice untuk DDL
	var collectedAlterColumnDDLs []string
	finalIndexDDLs = make([]string, 0)
	finalConstraintDDLs = make([]string, 0)

	// Buat map untuk pencarian cepat objek sumber dan tujuan
	srcColMap := make(map[string]ColumnInfo); for _, c := range srcColumns { srcColMap[c.Name] = c }
	dstColMap := make(map[string]ColumnInfo); for _, c := range dstColumns { dstColMap[c.Name] = c }

	srcIdxMap := make(map[string]IndexInfo); for _, i := range srcIndexes { srcIdxMap[i.Name] = i }
	dstIdxMap := make(map[string]IndexInfo); for _, i := range dstIndexes { dstIdxMap[i.Name] = i }

	srcConsMap := make(map[string]ConstraintInfo); for _, c := range srcConstraints { srcConsMap[c.Name] = c }
	dstConsMap := make(map[string]ConstraintInfo); for _, c := range dstConstraints { dstConsMap[c.Name] = c }


	// --- Tahap 1: Hasilkan DDL DROP untuk objek di tujuan yang tidak ada di sumber atau dimodifikasi ---
	// Urutan drop: FK, Constraint lain (UNIQUE, CHECK), baru Indeks.
	// Ini untuk menghindari masalah dependensi.

	log.Debug("Phase 1: Generating DROP DDLs for modified/removed constraints and indexes")

	// Constraints (FK, UNIQUE, CHECK) yang akan di-drop atau di-drop-dan-dibuat-ulang
	constraintsToDrop := []ConstraintInfo{}
	constraintsToRecreate := []struct{ Src, Dst ConstraintInfo }{} // Untuk yang dimodifikasi

	for dstConsName, dc := range dstConsMap {
		if dc.Type == "PRIMARY KEY" { // PK biasanya tidak di-drop/add dengan cara ini dalam strategi ALTER
			continue
		}
		sc, srcConsExists := srcConsMap[dstConsName]
		if !srcConsExists {
			log.Debug("Constraint exists in destination but not in source, marking for DROP", zap.String("constraint", dstConsName))
			constraintsToDrop = append(constraintsToDrop, dc)
		} else {
			// Constraint ada di kedua sisi, cek apakah perlu modifikasi
			// s.needsConstraintModification diambil dari schema_compare.go
			if s.needsConstraintModification(sc, dc, log) {
				log.Debug("Constraint exists in both but needs modification, marking for DROP+ADD", zap.String("constraint", dstConsName))
				constraintsToRecreate = append(constraintsToRecreate, struct{ Src, Dst ConstraintInfo }{sc, dc})
			}
		}
	}

	// Tambahkan DDL DROP untuk constraint yang perlu di-drop atau dibuat ulang
	for _, cons := range constraintsToDrop {
		ddl, dropErr := s.generateDropConstraintDDL(table, cons) // Dari schema_ddl_alter.go
		if dropErr == nil && ddl != "" {
			finalConstraintDDLs = append(finalConstraintDDLs, ddl)
		} else if dropErr != nil {
			log.Error("Failed to generate DROP CONSTRAINT DDL", zap.String("constraint", cons.Name), zap.Error(dropErr))
			// Pertimbangkan apakah error ini fatal atau bisa diabaikan jika SKIP_FAILED_TABLES aktif
		}
	}
	for _, modCons := range constraintsToRecreate {
		ddl, dropErr := s.generateDropConstraintDDL(table, modCons.Dst)
		if dropErr == nil && ddl != "" {
			finalConstraintDDLs = append(finalConstraintDDLs, ddl)
		} else if dropErr != nil {
			log.Error("Failed to generate DROP CONSTRAINT DDL (for recreate)", zap.String("constraint", modCons.Dst.Name), zap.Error(dropErr))
		}
	}

	// Indeks yang akan di-drop atau di-drop-dan-dibuat-ulang
	indexesToDrop := []IndexInfo{}
	indexesToRecreate := []struct{ Src, Dst IndexInfo }{} // Untuk yang dimodifikasi

	for dstIdxName, di := range dstIdxMap {
		if di.IsPrimary { // PK Indeks biasanya tidak di-drop/add dengan cara ini
			continue
		}
		si, srcIdxExists := srcIdxMap[dstIdxName]
		if !srcIdxExists {
			log.Debug("Index exists in destination but not in source, marking for DROP", zap.String("index", dstIdxName))
			indexesToDrop = append(indexesToDrop, di)
		} else {
			// Indeks ada di kedua sisi, cek apakah perlu modifikasi
			// s.needsIndexModification diambil dari schema_compare.go
			if s.needsIndexModification(si, di, log) {
				log.Debug("Index exists in both but needs modification, marking for DROP+ADD", zap.String("index", dstIdxName))
				indexesToRecreate = append(indexesToRecreate, struct{ Src, Dst IndexInfo }{si, di})
			}
		}
	}
	// Tambahkan DDL DROP untuk indeks
	for _, idx := range indexesToDrop {
		ddl, dropErr := s.generateDropIndexDDL(table, idx) // Dari schema_ddl_alter.go
		if dropErr == nil && ddl != "" {
			finalIndexDDLs = append(finalIndexDDLs, ddl)
		} else if dropErr != nil {
			log.Error("Failed to generate DROP INDEX DDL", zap.String("index", idx.Name), zap.Error(dropErr))
		}
	}
	for _, modIdx := range indexesToRecreate {
		ddl, dropErr := s.generateDropIndexDDL(table, modIdx.Dst)
		if dropErr == nil && ddl != "" {
			finalIndexDDLs = append(finalIndexDDLs, ddl)
		} else if dropErr != nil {
			log.Error("Failed to generate DROP INDEX DDL (for recreate)", zap.String("index", modIdx.Dst.Name), zap.Error(dropErr))
		}
	}


	// --- Tahap 2: Hasilkan DDL Kolom (ADD, DROP, MODIFY) ---
	log.Debug("Phase 2: Generating ADD/DROP/MODIFY COLUMN DDLs")
	// Kolom yang akan di-drop dari tujuan
	for dstColName, dc := range dstColMap {
		if _, srcColExists := srcColMap[dstColName]; !srcColExists {
			log.Debug("Column exists in destination but not in source, marking for DROP", zap.String("column", dstColName))
			ddl, dropErr := s.generateDropColumnDDL(table, dc) // Dari schema_ddl_alter.go
			if dropErr == nil && ddl != "" {
				collectedAlterColumnDDLs = append(collectedAlterColumnDDLs, ddl)
			} else if dropErr != nil {
				log.Error("Failed to generate DROP COLUMN DDL", zap.String("column", dc.Name), zap.Error(dropErr))
			}
		}
	}

	// Kolom yang akan di-add ke tujuan atau di-modify
	for srcColName, sc := range srcColMap {
		dc, dstColExists := dstColMap[srcColName]
		if !dstColExists {
			log.Debug("Column exists in source but not in destination, marking for ADD", zap.String("column", srcColName))
			ddl, addErr := s.generateAddColumnDDL(table, sc) // Dari schema_ddl_alter.go
			if addErr == nil && ddl != "" {
				collectedAlterColumnDDLs = append(collectedAlterColumnDDLs, ddl)
			} else if addErr != nil {
				log.Error("Failed to generate ADD COLUMN DDL", zap.String("column", sc.Name), zap.Error(addErr))
			}
		} else {
			// Kolom ada di kedua sisi, cek apakah perlu modifikasi
			// s.generateModifyColumnDDLs adalah dispatcher di schema_ddl_alter.go
			// yang memanggil s.getColumnModifications di dalamnya.
			modifyDDLs := s.generateModifyColumnDDLs(table, sc, dc, log)
			if len(modifyDDLs) > 0 {
				log.Debug("Column exists in both and needs modification", zap.String("column", srcColName), zap.Int("num_modify_ddls", len(modifyDDLs)))
				collectedAlterColumnDDLs = append(collectedAlterColumnDDLs, modifyDDLs...)
			}
		}
	}
	// Gabungkan semua DDL ALTER COLUMN menjadi satu string jika ada
	if len(collectedAlterColumnDDLs) > 0 {
		// Urutkan DDL kolom (misalnya, DROP dulu, baru ADD, baru MODIFY)
		// Ini bisa rumit. Untuk sekarang, kita gabungkan sesuai urutan deteksi.
		// Pengurutan yang lebih baik bisa dilakukan di `ExecuteDDLs`.
		alterColumnDDLsJoined = strings.Join(collectedAlterColumnDDLs, ";\n")
		// Tambahkan titik koma di akhir jika belum ada, untuk konsistensi
		if !strings.HasSuffix(alterColumnDDLsJoined, ";") {
			alterColumnDDLsJoined += ";"
		}
	}


	// --- Tahap 3: Hasilkan DDL ADD untuk objek baru atau yang dibuat ulang ---
	log.Debug("Phase 3: Generating ADD DDLs for new/recreated constraints and indexes")

	// Indeks yang akan ditambahkan (baru atau dibuat ulang)
	indexesToAdd := []IndexInfo{}
	for srcIdxName, si := range srcIdxMap {
		if si.IsPrimary { continue } // PK Indeks di-handle oleh CREATE TABLE atau sudah ada

		_, dstIdxExists := dstIdxMap[srcIdxName]
		isRecreated := false
		for _, modIdx := range indexesToRecreate {
			if modIdx.Src.Name == srcIdxName {
				isRecreated = true
				break
			}
		}

		if !dstIdxExists || isRecreated {
			log.Debug("Marking index for ADD (either new or recreated)", zap.String("index", srcIdxName))
			indexesToAdd = append(indexesToAdd, si)
		}
	}
	// Tambahkan DDL CREATE INDEX ke finalIndexDDLs
	// s.generateCreateIndexDDLs berasal dari schema_ddl_create.go
	addIndexDDLs := s.generateCreateIndexDDLs(table, indexesToAdd)
	finalIndexDDLs = append(finalIndexDDLs, addIndexDDLs...)


	// Constraints yang akan ditambahkan (baru atau dibuat ulang)
	constraintsToAdd := []ConstraintInfo{}
	for srcConsName, sc := range srcConsMap {
		if sc.Type == "PRIMARY KEY" { continue } // PK sudah ada

		_, dstConsExists := dstConsMap[srcConsName]
		isRecreated := false
		for _, modCons := range constraintsToRecreate {
			if modCons.Src.Name == srcConsName {
				isRecreated = true
				break
			}
		}
		if !dstConsExists || isRecreated {
			log.Debug("Marking constraint for ADD (either new or recreated)", zap.String("constraint", srcConsName))
			constraintsToAdd = append(constraintsToAdd, sc)
		}
	}
	// Tambahkan DDL ADD CONSTRAINT ke finalConstraintDDLs
	// s.generateAddConstraintDDLs berasal dari schema_ddl_create.go
	addConstraintDDLs := s.generateAddConstraintDDLs(table, constraintsToAdd)
	finalConstraintDDLs = append(finalConstraintDDLs, addConstraintDDLs...)

	if alterColumnDDLsJoined == "" && len(finalIndexDDLs) == 0 && len(finalConstraintDDLs) == 0 {
		log.Info("No schema differences found requiring ALTER DDLs.")
	} else {
		log.Info("Finished generating ALTER DDLs.",
			zap.Bool("has_column_alters", alterColumnDDLsJoined != ""),
			zap.Int("num_index_ddls", len(finalIndexDDLs)),
			zap.Int("num_constraint_ddls", len(finalConstraintDDLs)),
		)
	}

	return // alterColumnDDLsJoined, finalIndexDDLs, finalConstraintDDLs, nil
}
