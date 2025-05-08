// internal/sync/schema_alter.go
package sync

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
	// utils digunakan oleh helper DDL yang dipanggil dari sini
)

// generateAlterDDLs menghasilkan DDL untuk mengubah skema tujuan agar cocok dengan sumber.
// Ini adalah fungsi koordinator utama untuk strategi ALTER.
// Mengembalikan:
// - alterColumnDDLsJoined: String tunggal berisi semua DDL ALTER COLUMN (ADD/DROP/MODIFY), dipisah ';'.
// - finalIndexDDLs: Slice string berisi DDL DROP INDEX dan CREATE INDEX.
// - finalConstraintDDLs: Slice string berisi DDL DROP CONSTRAINT dan ADD CONSTRAINT.
// - err: Error jika terjadi kegagalan fatal dalam proses generasi.
func (s *SchemaSyncer) generateAlterDDLs(
	table string,
	srcColumns []ColumnInfo, srcIndexes []IndexInfo, srcConstraints []ConstraintInfo,
	dstColumns []ColumnInfo, dstIndexes []IndexInfo, dstConstraints []ConstraintInfo,
) (
	alterColumnDDLsJoined string,
	finalIndexDDLs []string,
	finalConstraintDDLs []string,
	err error,
) {
	log := s.logger.With(zap.String("table", table), zap.String("strategy", "alter"), zap.String("dst_dialect", s.dstDialect))
	log.Info("Starting generation of ALTER DDLs")

	// Inisialisasi slice/map
	var collectedAlterColumnDDLs []string
	finalIndexDDLs = make([]string, 0)
	finalConstraintDDLs = make([]string, 0)
	errorsEncountered := make([]error, 0) // Kumpulkan error non-fatal

	// Buat map untuk pencarian cepat objek sumber dan tujuan
	srcColMap := make(map[string]ColumnInfo); for _, c := range srcColumns { srcColMap[c.Name] = c }
	dstColMap := make(map[string]ColumnInfo); for _, c := range dstColumns { dstColMap[c.Name] = c }
	srcIdxMap := make(map[string]IndexInfo); for _, i := range srcIndexes { srcIdxMap[i.Name] = i }
	dstIdxMap := make(map[string]IndexInfo); for _, i := range dstIndexes { dstIdxMap[i.Name] = i }
	srcConsMap := make(map[string]ConstraintInfo); for _, c := range srcConstraints { srcConsMap[c.Name] = c }
	dstConsMap := make(map[string]ConstraintInfo); for _, c := range dstConstraints { dstConsMap[c.Name] = c }


	// --- Tahap 1: Hasilkan DDL DROP untuk objek di tujuan yang tidak ada di sumber atau dimodifikasi ---
	// Urutan drop: FK, Constraint lain (UNIQUE, CHECK), baru Indeks.
	log.Debug("Phase 1: Generating DROP DDLs for constraints and indexes removed or modified in source.")

	// Constraints (FK, UNIQUE, CHECK) yang akan di-drop atau di-drop-dan-dibuat-ulang
	constraintsToDrop := []ConstraintInfo{}
	constraintsToRecreate := map[string]ConstraintInfo{} // map[srcName]srcConstraintInfo

	for dstConsName, dc := range dstConsMap {
		if dc.Type == "PRIMARY KEY" { // PK biasanya tidak di-drop/add dengan cara ini dalam strategi ALTER
			log.Debug("Skipping PRIMARY KEY constraint for DROP/ADD logic in ALTER.", zap.String("constraint", dstConsName))
			continue
		}
		sc, srcConsExists := srcConsMap[dstConsName]
		if !srcConsExists {
			log.Info("Constraint exists in destination but not in source, marking for DROP", zap.String("constraint", dstConsName), zap.String("type", dc.Type))
			constraintsToDrop = append(constraintsToDrop, dc)
		} else {
			// Constraint ada di kedua sisi, cek apakah perlu modifikasi
			// s.needsConstraintModification diambil dari schema_compare.go
			if s.needsConstraintModification(sc, dc, log) {
				log.Info("Constraint exists in both but needs modification, marking for DROP+ADD", zap.String("constraint", dstConsName), zap.String("type", dc.Type))
				// Tandai untuk drop (dc) dan simpan info sumber (sc) untuk dibuat ulang
				constraintsToDrop = append(constraintsToDrop, dc)
				constraintsToRecreate[sc.Name] = sc
			} else {
				log.Debug("Constraint exists in both and does not need modification.", zap.String("constraint", dstConsName))
			}
		}
	}

	// Tambahkan DDL DROP untuk constraint yang perlu di-drop
	for _, cons := range constraintsToDrop {
		ddl, dropErr := s.generateDropConstraintDDL(table, cons) // Dari schema_ddl_alter.go
		if dropErr != nil {
			errLog := fmt.Errorf("failed to generate DROP CONSTRAINT DDL for '%s' (%s): %w", cons.Name, cons.Type, dropErr)
			log.Error(errLog.Error())
			errorsEncountered = append(errorsEncountered, errLog)
			// Pertimbangkan apakah error ini fatal atau bisa diabaikan jika SKIP_FAILED_TABLES aktif
			// Untuk sekarang, kita kumpulkan error dan lanjutkan generasi DDL lain.
		} else if ddl != "" {
			finalConstraintDDLs = append(finalConstraintDDLs, ddl)
			log.Debug("Generated DROP CONSTRAINT DDL.", zap.String("ddl", ddl))
		}
	}

	// Indeks yang akan di-drop atau di-drop-dan-dibuat-ulang
	indexesToDrop := []IndexInfo{}
	indexesToRecreate := map[string]IndexInfo{} // map[srcName]srcIndexInfo

	for dstIdxName, di := range dstIdxMap {
		if di.IsPrimary { // Indeks PK biasanya tidak di-drop/add dengan cara ini
			log.Debug("Skipping PRIMARY KEY index for DROP/ADD logic in ALTER.", zap.String("index", dstIdxName))
			continue
		}
		si, srcIdxExists := srcIdxMap[dstIdxName]
		if !srcIdxExists {
			log.Info("Index exists in destination but not in source, marking for DROP", zap.String("index", dstIdxName))
			indexesToDrop = append(indexesToDrop, di)
		} else {
			// Indeks ada di kedua sisi, cek apakah perlu modifikasi
			// s.needsIndexModification diambil dari schema_compare.go
			if s.needsIndexModification(si, di, log) {
				log.Info("Index exists in both but needs modification, marking for DROP+ADD", zap.String("index", dstIdxName))
				indexesToDrop = append(indexesToDrop, di)
				indexesToRecreate[si.Name] = si
			} else {
				log.Debug("Index exists in both and does not need modification.", zap.String("index", dstIdxName))
			}
		}
	}

	// Tambahkan DDL DROP untuk indeks
	for _, idx := range indexesToDrop {
		ddl, dropErr := s.generateDropIndexDDL(table, idx) // Dari schema_ddl_alter.go
		if dropErr != nil {
			errLog := fmt.Errorf("failed to generate DROP INDEX DDL for '%s': %w", idx.Name, dropErr)
			log.Error(errLog.Error())
			errorsEncountered = append(errorsEncountered, errLog)
		} else if ddl != "" {
			finalIndexDDLs = append(finalIndexDDLs, ddl)
			log.Debug("Generated DROP INDEX DDL.", zap.String("ddl", ddl))
		}
	}


	// --- Tahap 2: Hasilkan DDL Kolom (ADD, DROP, MODIFY) ---
	log.Debug("Phase 2: Generating ADD/DROP/MODIFY COLUMN DDLs")

	// Kolom yang akan di-drop dari tujuan
	for dstColName, dc := range dstColMap {
		if _, srcColExists := srcColMap[dstColName]; !srcColExists {
			log.Info("Column exists in destination but not in source, marking for DROP", zap.String("column", dstColName))
			ddl, dropErr := s.generateDropColumnDDL(table, dc) // Dari schema_ddl_alter.go
			if dropErr != nil {
				errLog := fmt.Errorf("failed to generate DROP COLUMN DDL for '%s': %w", dc.Name, dropErr)
				log.Error(errLog.Error())
				errorsEncountered = append(errorsEncountered, errLog)
			} else if ddl != "" {
				collectedAlterColumnDDLs = append(collectedAlterColumnDDLs, ddl)
				log.Debug("Generated DROP COLUMN DDL.", zap.String("ddl", ddl))
			}
		}
	}

	// Kolom yang akan di-add ke tujuan atau di-modify
	for srcColName, sc := range srcColMap {
		dc, dstColExists := dstColMap[srcColName]
		if !dstColExists {
			log.Info("Column exists in source but not in destination, marking for ADD", zap.String("column", srcColName))
			ddl, addErr := s.generateAddColumnDDL(table, sc) // Dari schema_ddl_alter.go
			if addErr != nil {
				errLog := fmt.Errorf("failed to generate ADD COLUMN DDL for '%s': %w", sc.Name, addErr)
				log.Error(errLog.Error())
				errorsEncountered = append(errorsEncountered, errLog)
			} else if ddl != "" {
				collectedAlterColumnDDLs = append(collectedAlterColumnDDLs, ddl)
				log.Debug("Generated ADD COLUMN DDL.", zap.String("ddl", ddl))
			}
		} else {
			// Kolom ada di kedua sisi, cek apakah perlu modifikasi
			// s.generateModifyColumnDDLs adalah dispatcher di schema_ddl_alter.go
			// yang memanggil s.getColumnModifications di dalamnya.
			modifyDDLs := s.generateModifyColumnDDLs(table, sc, dc, log) // Menggunakan logger yang sudah di-scope
			if len(modifyDDLs) > 0 {
				log.Info("Column exists in both and needs modification", zap.String("column", srcColName), zap.Int("num_modify_ddls", len(modifyDDLs)))
				collectedAlterColumnDDLs = append(collectedAlterColumnDDLs, modifyDDLs...)
				// Logging DDL spesifik sudah dilakukan di dalam generateModifyColumnDDLs dan fungsi spesifik dialeknya
			} else {
				log.Debug("Column exists in both and does not need modification.", zap.String("column", srcColName))
			}
		}
	}
	// Gabungkan semua DDL ALTER COLUMN menjadi satu string jika ada
	if len(collectedAlterColumnDDLs) > 0 {
		// Urutkan DDL kolom (DROP dulu, baru MODIFY, baru ADD)
		// Pengurutan ini sekarang ditangani oleh `parseAndCategorizeDDLs` atau `ExecuteDDLs`.
		// Di sini kita gabungkan saja. ExecuteDDLs akan memisahkannya lagi jika perlu.
		alterColumnDDLsJoined = strings.Join(collectedAlterColumnDDLs, ";\n")
		// Tambahkan titik koma di akhir jika belum ada, untuk konsistensi
		if !strings.HasSuffix(alterColumnDDLsJoined, ";") {
			alterColumnDDLsJoined += ";"
		}
		log.Debug("Collected ALTER COLUMN DDLs (joined)", zap.String("joined_ddl", alterColumnDDLsJoined))
	}


	// --- Tahap 3: Hasilkan DDL ADD untuk objek baru atau yang dibuat ulang ---
	log.Debug("Phase 3: Generating ADD DDLs for new/recreated constraints and indexes")

	// Indeks yang akan ditambahkan (baru atau dibuat ulang)
	indexesToAdd := []IndexInfo{}
	for srcIdxName, si := range srcIdxMap {
		if si.IsPrimary { continue } // PK Indeks di-handle oleh CREATE TABLE atau sudah ada

		_, dstIdxExists := dstIdxMap[srcIdxName]
		_, needsRecreate := indexesToRecreate[srcIdxName]

		if !dstIdxExists || needsRecreate {
			log.Info("Marking index for ADD (either new or recreated)", zap.String("index", srcIdxName), zap.Bool("is_new", !dstIdxExists), zap.Bool("is_recreated", needsRecreate))
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
		_, needsRecreate := constraintsToRecreate[srcConsName]

		if !dstConsExists || needsRecreate {
			log.Info("Marking constraint for ADD (either new or recreated)", zap.String("constraint", srcConsName), zap.String("type", sc.Type), zap.Bool("is_new", !dstConsExists), zap.Bool("is_recreated", needsRecreate))
			constraintsToAdd = append(constraintsToAdd, sc)
		}
	}
	// Tambahkan DDL ADD CONSTRAINT ke finalConstraintDDLs
	// s.generateAddConstraintDDLs berasal dari schema_ddl_create.go
	addConstraintDDLs := s.generateAddConstraintDDLs(table, constraintsToAdd)
	finalConstraintDDLs = append(finalConstraintDDLs, addConstraintDDLs...)

	// Gabungkan semua error yang terkumpul (jika ada) menjadi satu error
	if len(errorsEncountered) > 0 {
		// Gunakan multierr jika tersedia, atau gabungkan string error
		errorMessages := make([]string, len(errorsEncountered))
		for i, e := range errorsEncountered {
			errorMessages[i] = e.Error()
		}
		err = fmt.Errorf("encountered %d error(s) during ALTER DDL generation for table '%s': %s",
			len(errorsEncountered), table, strings.Join(errorMessages, "; "))
		log.Error("Finished generating ALTER DDLs with errors.", zap.Error(err))
		// Kembalikan DDL yang berhasil dibuat sejauh ini, beserta error gabungan.
		// Pemanggil (SyncTableSchema) dapat memutuskan apakah akan melanjutkan atau tidak.
		return
	}


	if alterColumnDDLsJoined == "" && len(finalIndexDDLs) == 0 && len(finalConstraintDDLs) == 0 {
		log.Info("No schema differences found requiring ALTER DDLs.")
	} else {
		log.Info("Finished generating ALTER DDLs successfully.",
			zap.Bool("has_column_alters", alterColumnDDLsJoined != ""),
			zap.Int("num_index_ddls", len(finalIndexDDLs)),       // Ini termasuk DROP dan ADD
			zap.Int("num_constraint_ddls", len(finalConstraintDDLs)), // Ini termasuk DROP dan ADD
		)
	}

	return // alterColumnDDLsJoined, finalIndexDDLs, finalConstraintDDLs, nil
}
