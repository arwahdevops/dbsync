package sync

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"go.uber.org/multierr" // Untuk menggabungkan error
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// parseAndCategorizeDDLs mem-parse DDL dari SchemaExecutionResult menjadi kategori yang lebih spesifik.
func (s *SchemaSyncer) parseAndCategorizeDDLs(ddls *SchemaExecutionResult, table string) (*categorizedDDLs, error) {
	log := s.logger.With(zap.String("table", table), zap.String("phase", "parse-ddls"))
	parsed := &categorizedDDLs{}

	if ddls.TableDDL != "" {
		trimmedUpperTableDDL := strings.ToUpper(strings.TrimSpace(ddls.TableDDL))
		if strings.HasPrefix(trimmedUpperTableDDL, "CREATE TABLE") {
			parsed.CreateTableDDL = ddls.TableDDL
		} else if strings.HasPrefix(trimmedUpperTableDDL, "ALTER TABLE") { // Lebih spesifik untuk ALTER
			// Asumsikan TableDDL untuk ALTER adalah multiple statements yang dipisah ';'
			// atau satu statement ALTER yang panjang.
			// Kita akan split berdasarkan ';' untuk menangani multiple ALTERs.
			potentialAlters := strings.Split(ddls.TableDDL, ";")
			for _, ddl := range potentialAlters {
				trimmed := strings.TrimSpace(ddl)
				if trimmed != "" {
					// Periksa lagi apakah ini benar-benar ALTER TABLE (bukan komentar atau sisa)
					if strings.HasPrefix(strings.ToUpper(trimmed), "ALTER TABLE") {
						parsed.AlterColumnDDLs = append(parsed.AlterColumnDDLs, trimmed)
					} else if len(potentialAlters) == 1 && trimmed != "" {
						// Jika hanya ada satu "statement" dan itu bukan CREATE, anggap itu ALTER
						// Ini mungkin perlu diperhalus jika TableDDL bisa berisi hal lain.
						log.Debug("TableDDL is not CREATE, assuming single ALTER statement.", zap.String("ddl", trimmed))
						parsed.AlterColumnDDLs = append(parsed.AlterColumnDDLs, trimmed)
					}
				}
			}
		} else if ddls.TableDDL != "" {
			log.Warn("Unrecognized DDL type in TableDDL, attempting to treat as ALTER if non-empty.", zap.String("ddl", ddls.TableDDL))
			// Fallback: jika tidak kosong dan bukan CREATE, anggap sebagai ALTER
			potentialAlters := strings.Split(ddls.TableDDL, ";")
			for _, ddl := range potentialAlters {
				trimmed := strings.TrimSpace(ddl)
				if trimmed != "" {
						parsed.AlterColumnDDLs = append(parsed.AlterColumnDDLs, trimmed)
				}
			}
		}
	}

	for _, ddl := range ddls.IndexDDLs {
		trimmed := strings.TrimSpace(ddl)
		if trimmed == "" {
			continue
		}
		upperTrimmed := strings.ToUpper(trimmed)
		if strings.Contains(upperTrimmed, "DROP INDEX") {
			parsed.DropIndexDDLs = append(parsed.DropIndexDDLs, trimmed)
		} else if strings.HasPrefix(upperTrimmed, "CREATE INDEX") || strings.HasPrefix(upperTrimmed, "CREATE UNIQUE INDEX") {
			parsed.AddIndexDDLs = append(parsed.AddIndexDDLs, trimmed)
		} else {
			log.Warn("Unknown DDL type in IndexDDLs, skipping parsing for this DDL.", zap.String("ddl", trimmed))
		}
	}
	parsed.DropIndexDDLs = s.sortDropIndexes(parsed.DropIndexDDLs)
	parsed.AddIndexDDLs = s.sortAddIndexes(parsed.AddIndexDDLs)

	for _, ddl := range ddls.ConstraintDDLs {
		trimmed := strings.TrimSpace(ddl)
		if trimmed == "" {
			continue
		}
		upperTrimmed := strings.ToUpper(trimmed)
		if strings.Contains(upperTrimmed, "DROP CONSTRAINT") ||
			strings.Contains(upperTrimmed, "DROP FOREIGN KEY") ||
			strings.Contains(upperTrimmed, "DROP CHECK") ||
			strings.Contains(upperTrimmed, "DROP PRIMARY KEY") {
			parsed.DropConstraintDDLs = append(parsed.DropConstraintDDLs, trimmed)
		} else if strings.HasPrefix(upperTrimmed, "ADD CONSTRAINT") || // Umum
			(strings.HasPrefix(upperTrimmed, "ALTER TABLE") && strings.Contains(upperTrimmed, "ADD CONSTRAINT")) { // Alternatif
			parsed.AddConstraintDDLs = append(parsed.AddConstraintDDLs, trimmed)
		} else {
			log.Warn("Unknown DDL type in ConstraintDDLs, skipping parsing for this DDL.", zap.String("ddl", trimmed))
		}
	}
	parsed.DropConstraintDDLs = s.sortConstraintsForDrop(parsed.DropConstraintDDLs)
	parsed.AddConstraintDDLs = s.sortConstraintsForAdd(parsed.AddConstraintDDLs)

	parsed.AlterColumnDDLs = s.sortAlterColumns(parsed.AlterColumnDDLs)

	log.Debug("Finished parsing and categorizing DDLs.",
		zap.Bool("create_table_present", parsed.CreateTableDDL != ""),
		zap.Int("alter_column_ddls", len(parsed.AlterColumnDDLs)),
		zap.Int("add_index_ddls", len(parsed.AddIndexDDLs)),
		zap.Int("drop_index_ddls", len(parsed.DropIndexDDLs)),
		zap.Int("add_constraint_ddls", len(parsed.AddConstraintDDLs)),
		zap.Int("drop_constraint_ddls", len(parsed.DropConstraintDDLs)),
	)
	return parsed, nil
}

// executeDDLPhase adalah helper untuk mengeksekusi sekelompok DDL dalam satu fase.
// Mengembalikan error gabungan jika continueOnError true dan ada error yang tidak diabaikan.
func (s *SchemaSyncer) executeDDLPhase(tx *gorm.DB, phaseName string, ddlList []string, continueOnError bool, log *zap.Logger) error {
	if len(ddlList) == 0 {
		log.Debug("No DDLs to execute for phase.", zap.String("phase_name", phaseName))
		return nil
	}
	log.Info("Executing DDL phase.", zap.String("phase_name", phaseName), zap.Int("ddl_count", len(ddlList)))

	var accumulatedErrors error

	for _, ddl := range ddlList {
		if ddl == "" { // Lewati DDL kosong
			continue
		}
		log.Debug("Executing DDL.", zap.String("phase_name", phaseName), zap.String("ddl", ddl))
		if err := tx.Exec(ddl).Error; err != nil {
			execErrLog := log.With(zap.String("phase_name", phaseName), zap.String("failed_ddl", ddl), zap.Error(err))
			if s.shouldIgnoreDDLError(err) {
				execErrLog.Warn("DDL execution resulted in an ignorable error, continuing.")
			} else {
				// Format error dengan DDL yang gagal untuk konteks yang lebih baik
				currentErr := fmt.Errorf("failed DDL in phase '%s': [%s], error: %w", phaseName, ddl, err)
				if continueOnError {
					execErrLog.Error("Failed to execute DDL, but configured to continue. Error will be accumulated.")
					accumulatedErrors = multierr.Append(accumulatedErrors, currentErr)
				} else {
					execErrLog.Error("Failed to execute DDL, aborting phase.")
					return currentErr // Kembalikan error pertama yang tidak bisa diabaikan jika tidak continueOnError
				}
			}
		}
	}
	// Kembalikan error gabungan jika ada dan continueOnError true
	if accumulatedErrors != nil {
	    log.Warn("DDL phase completed with accumulated (non-ignorable) errors.",
	        zap.String("phase_name", phaseName),
	        zap.NamedError("accumulated_ddl_errors", accumulatedErrors))
	}
	return accumulatedErrors
}

// shouldIgnoreDDLError memeriksa apakah error DDL tertentu dapat diabaikan.
func (s *SchemaSyncer) shouldIgnoreDDLError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())

	ignorablePatterns := map[string][]string{
		"mysql": {
			"duplicate key name",                                // Error 1061
			"can't drop index '.*'; check that it exists",       // Error 1091
			"can't drop constraint '.*'; check that it exists",  // Error 3959 (MySQL 8.0.28+) atau error 1091 untuk FK lama
			"error 1091.*foreign key constraint",                // Lebih spesifik untuk FK drop error lama
			"check constraint .* already exists",
			"constraint .* does not exist",
			"table '.*' already exists",                         // Error 1050
			"unknown table '.*'",                                // Error 1051 (untuk DROP TABLE IF EXISTS yang mungkin masih error jika ada FK)
			"already exists",                                    // Frasa umum
			"doesn't exist",                                     // Frasa umum
		},
		"postgres": {
			"relation .* already exists",
			"index .* already exists",
			"constraint .* for relation .* already exists",
			"constraint .* on table .* does not exist",
			"index .* does not exist",
			"role .* already exists",
			"schema .* already exists",
			`table ".*" does not exist`,
			"already exists", // Frasa umum
			"does not exist", // Frasa umum
		},
		"sqlite": {
			"index .* already exists",
			"table .* already exists",
			// SQLite bisa lebih tricky dengan pesan error untuk "does not exist"
			"no such index",
			"no such table",
			"already exists", // Frasa umum
		},
	}

	if patterns, ok := ignorablePatterns[s.dstDialect]; ok {
		for _, pattern := range patterns {
			matched, _ := regexp.MatchString(pattern, errStr)
			if matched {
				s.logger.Debug("DDL error matched ignorable pattern",
					zap.String("dialect", s.dstDialect),
					zap.String("pattern", pattern),
					zap.String("error_string", errStr))
				return true
			}
		}
	}
	s.logger.Debug("DDL error did not match any ignorable patterns",
		zap.String("dialect", s.dstDialect),
		zap.String("error_string", errStr))
	return false
}

// splitPostgresFKsForDeferredExecution memisahkan DDL FK PostgreSQL.
func (s *SchemaSyncer) splitPostgresFKsForDeferredExecution(allConstraints []string) (deferredFKs []string, nonDeferredFKs []string) {
	for _, ddl := range allConstraints {
		// Periksa apakah DDL adalah untuk FOREIGN KEY dan mengandung DEFERRABLE
		// Kita periksa juga apakah ini "ADD CONSTRAINT" untuk lebih spesifik
		upperDDL := strings.ToUpper(ddl)
		if strings.Contains(upperDDL, "ADD CONSTRAINT") && strings.Contains(upperDDL, "FOREIGN KEY") {
			if strings.Contains(upperDDL, "DEFERRABLE") { // Asumsi DDL sudah benar menyertakan DEFERRABLE jika perlu
				deferredFKs = append(deferredFKs, ddl)
			} else {
				nonDeferredFKs = append(nonDeferredFKs, ddl)
			}
		} else {
			// Jika bukan ADD CONSTRAINT FK, masukkan ke non-deferred
			nonDeferredFKs = append(nonDeferredFKs, ddl)
		}
	}
	return
}

// --- DDL Sorting Helpers ---

// sortConstraintsForDrop mengurutkan DDL DROP CONSTRAINT (FKs dulu).
func (s *SchemaSyncer) sortConstraintsForDrop(ddls []string) []string {
	sorted := make([]string, len(ddls))
	copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		isFkI := strings.Contains(strings.ToUpper(sorted[i]), "FOREIGN KEY")
		isFkJ := strings.Contains(strings.ToUpper(sorted[j]), "FOREIGN KEY")
		if isFkI && !isFkJ { return true }  // FK_I sebelum NON_FK_J
		if !isFkI && isFkJ { return false } // NON_FK_I setelah FK_J
		// Jika keduanya FK atau keduanya bukan FK, bisa diurutkan berdasarkan nama untuk determinisme (opsional)
		// return sorted[i] < sorted[j]
		return false // Pertahankan urutan relatif jika tipenya sama
	})
	return sorted
}

// sortConstraintsForAdd mengurutkan DDL ADD CONSTRAINT (FKs belakangan).
func (s *SchemaSyncer) sortConstraintsForAdd(ddls []string) []string {
	sorted := make([]string, len(ddls))
	copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		isFkI := strings.Contains(strings.ToUpper(sorted[i]), "FOREIGN KEY")
		isFkJ := strings.Contains(strings.ToUpper(sorted[j]), "FOREIGN KEY")
		if !isFkI && isFkJ { return true }  // NON_FK_I sebelum FK_J
		if isFkI && !isFkJ { return false } // FK_I setelah NON_FK_J
		return false // Pertahankan urutan relatif jika tipenya sama
	})
	return sorted
}

// sortAlterColumns mengurutkan DDL ALTER COLUMN (DROP, lalu MODIFY, lalu ADD).
func (s *SchemaSyncer) sortAlterColumns(ddls []string) []string {
	priority := func(ddl string) int {
		upperDDL := strings.ToUpper(ddl)
		if strings.Contains(upperDDL, "DROP COLUMN") { return 1 }
		// MODIFY COLUMN (MySQL) atau ALTER COLUMN ... TYPE/SET/DROP (PostgreSQL)
		if strings.Contains(upperDDL, "MODIFY COLUMN") || strings.Contains(upperDDL, "ALTER COLUMN") { return 2 }
		if strings.Contains(upperDDL, "ADD COLUMN") { return 3 }
		s.logger.Warn("Encountered ALTER DDL with unknown priority category during sorting.", zap.String("ddl", ddl))
		return 4 // Kategori lain, prioritas terendah
	}
	sorted := make([]string, len(ddls))
	copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		return priority(sorted[i]) < priority(sorted[j])
	})
	return sorted
}

// sortDropIndexes tidak memerlukan logika khusus saat ini, tetapi bisa ditambahkan.
func (s *SchemaSyncer) sortDropIndexes(ddls []string) []string {
	// MySQL: DROP INDEX `idx_name` ON `table_name`
	// PG/SQLite: DROP INDEX IF EXISTS `idx_name`
	// Tidak ada dependensi yang jelas antar drop index, jadi urutan tidak krusial.
	// Bisa diurutkan berdasarkan nama untuk determinisme jika diperlukan.
	// sorted := make([]string, len(ddls)); copy(sorted, ddls); sort.Strings(sorted); return sorted
	return ddls
}

// sortAddIndexes tidak memerlukan logika khusus saat ini.
func (s *SchemaSyncer) sortAddIndexes(ddls []string) []string {
	// CREATE [UNIQUE] INDEX `idx_name` ON `table_name` (...)
	// Urutan pembuatan indeks biasanya tidak krusial kecuali ada indeks yang bergantung pada fungsi, dll.
	// sorted := make([]string, len(ddls)); copy(sorted, ddls); sort.Strings(sorted); return sorted
	return ddls
}
