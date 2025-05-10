package sync

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

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
		} else {
			// Asumsikan TableDDL untuk ALTER adalah multiple statements yang dipisah ';'
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
	// Urutkan DDL indeks setelah semua dikumpulkan
	parsed.DropIndexDDLs = s.sortDropIndexes(parsed.DropIndexDDLs)
	parsed.AddIndexDDLs = s.sortAddIndexes(parsed.AddIndexDDLs)

	for _, ddl := range ddls.ConstraintDDLs {
		trimmed := strings.TrimSpace(ddl)
		if trimmed == "" {
			continue
		}
		upperTrimmed := strings.ToUpper(trimmed)
		// Perluas cek untuk berbagai cara DROP constraint
		if strings.Contains(upperTrimmed, "DROP CONSTRAINT") ||
			strings.Contains(upperTrimmed, "DROP FOREIGN KEY") || // Spesifik MySQL
			strings.Contains(upperTrimmed, "DROP CHECK") || // Spesifik MySQL
			strings.Contains(upperTrimmed, "DROP PRIMARY KEY") { // Spesifik MySQL
			parsed.DropConstraintDDLs = append(parsed.DropConstraintDDLs, trimmed)
		} else if strings.HasPrefix(upperTrimmed, "ADD CONSTRAINT") || // Umum
			(strings.HasPrefix(upperTrimmed, "ALTER TABLE") && strings.Contains(upperTrimmed, "ADD CONSTRAINT")) { // MySQL
			parsed.AddConstraintDDLs = append(parsed.AddConstraintDDLs, trimmed)
		} else {
			log.Warn("Unknown DDL type in ConstraintDDLs, skipping parsing for this DDL.", zap.String("ddl", trimmed))
		}
	}
	// Urutkan DDL constraint setelah semua dikumpulkan
	parsed.DropConstraintDDLs = s.sortConstraintsForDrop(parsed.DropConstraintDDLs)
	parsed.AddConstraintDDLs = s.sortConstraintsForAdd(parsed.AddConstraintDDLs)

	// Urutkan DDL alter column
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
func (s *SchemaSyncer) executeDDLPhase(tx *gorm.DB, phaseName string, ddlList []string, continueOnError bool, log *zap.Logger) error {
	if len(ddlList) == 0 {
		log.Debug("No DDLs to execute for phase.", zap.String("phase_name", phaseName))
		return nil
	}
	log.Info("Executing DDL phase.", zap.String("phase_name", phaseName), zap.Int("ddl_count", len(ddlList)))
	for _, ddl := range ddlList {
		if ddl == "" { // Lewati DDL kosong
			continue
		}
		log.Debug("Executing DDL.", zap.String("phase_name", phaseName), zap.String("ddl", ddl))
		if err := tx.Exec(ddl).Error; err != nil {
			execErrLog := log.With(zap.String("phase_name", phaseName), zap.String("failed_ddl", ddl), zap.Error(err))
			if s.shouldIgnoreDDLError(err) {
				execErrLog.Warn("DDL execution resulted in an ignorable error, continuing.")
			} else if continueOnError {
				execErrLog.Error("Failed to execute DDL, but configured to continue.")
				// Jika continueOnError, kita tidak mengembalikan error, hanya log.
				// Jika ingin mengumpulkan error, perlu mekanisme lain.
			} else {
				execErrLog.Error("Failed to execute DDL, aborting phase.")
				return fmt.Errorf("failed to execute DDL in phase '%s': DDL: [%s], Error: %w", phaseName, ddl, err)
			}
		}
	}
	return nil
}

// shouldIgnoreDDLError memeriksa apakah error DDL tertentu dapat diabaikan.
func (s *SchemaSyncer) shouldIgnoreDDLError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())

	// Pola umum yang bisa diabaikan (misalnya, objek sudah ada atau tidak ada saat drop)
	// Ini perlu disesuaikan per dialek.
	ignorablePatterns := map[string][]string{
		"mysql": {
			"duplicate key name",                // Error 1061
			"can't drop index '.*'; check that it exists", // Error 1091 (Index)
			"can't drop constraint '.*'; check that it exists", // Error 3959 (Constraint, MySQL 8.0.28+)
			"check constraint .* already exists",
			"constraint .* does not exist",       // Untuk DROP
			"table '.*' already exists",          // Error 1050 (jika IF NOT EXISTS tidak digunakan/didukung)
			"unknown table '.*'",                 // Error 1051 (jika IF EXISTS tidak digunakan/didukung)
		},
		"postgres": {
			"relation .* already exists",
			"index .* already exists",
			"constraint .* for relation .* already exists",
			"constraint .* on table .* does not exist", // Untuk DROP IF EXISTS
			"index .* does not exist",                  // Untuk DROP IF EXISTS
			"role .* already exists",
			"schema .* already exists",
			`table ".*" does not exist`, // Untuk DROP IF EXISTS
		},
		"sqlite": {
			"index .* already exists",
			"table .* already exists",
			// SQLite sering memberikan error umum "constraint failed" jika constraint dilanggar
			// Ini mungkin *tidak* selalu aman untuk diabaikan, tergantung konteks.
			// "constraint .* failed",
			// SQLite "no such index" atau "no such table" untuk DROP.
		},
	}

	if patterns, ok := ignorablePatterns[s.dstDialect]; ok {
		for _, pattern := range patterns {
			// Menggunakan regexp untuk pencocokan yang lebih fleksibel
			matched, _ := regexp.MatchString(pattern, errStr)
			if matched {
				return true
			}
		}
	}
	return false
}

// splitPostgresFKsForDeferredExecution memisahkan DDL FK PostgreSQL.
func (s *SchemaSyncer) splitPostgresFKsForDeferredExecution(allConstraints []string) (deferredFKs []string, nonDeferredFKs []string) {
	for _, ddl := range allConstraints {
		if strings.Contains(strings.ToUpper(ddl), "FOREIGN KEY") {
			// Asumsikan DDL yang dihasilkan oleh generateAddConstraintDDLs sudah menyertakan DEFERRABLE jika relevan
			if strings.Contains(strings.ToUpper(ddl), "DEFERRABLE") {
				deferredFKs = append(deferredFKs, ddl)
			} else {
				nonDeferredFKs = append(nonDeferredFKs, ddl)
			}
		} else {
			nonDeferredFKs = append(nonDeferredFKs, ddl)
		}
	}
	return
}

// --- DDL Sorting Helpers ---
// Urutan eksekusi DDL penting, terutama untuk DROP dan ADD constraint/index.

// sortConstraintsForDrop mengurutkan DDL DROP CONSTRAINT (FKs dulu).
func (s *SchemaSyncer) sortConstraintsForDrop(ddls []string) []string {
	// Buat salinan agar tidak mengubah slice asli jika dilewatkan dari luar
	sorted := make([]string, len(ddls))
	copy(sorted, ddls)

	sort.SliceStable(sorted, func(i, j int) bool {
		// Prioritaskan DROP FOREIGN KEY
		isFkI := strings.Contains(strings.ToUpper(sorted[i]), "FOREIGN KEY")
		isFkJ := strings.Contains(strings.ToUpper(sorted[j]), "FOREIGN KEY")

		if isFkI && !isFkJ {
			return true // FK_I harus sebelum NON_FK_J
		}
		if !isFkI && isFkJ {
			return false // NON_FK_I harus setelah FK_J
		}
		// Jika keduanya FK atau keduanya bukan FK, pertahankan urutan relatif
		return false
	})
	return sorted
}

// sortConstraintsForAdd mengurutkan DDL ADD CONSTRAINT (FKs belakangan).
func (s *SchemaSyncer) sortConstraintsForAdd(ddls []string) []string {
	sorted := make([]string, len(ddls))
	copy(sorted, ddls)

	sort.SliceStable(sorted, func(i, j int) bool {
		// Prioritaskan NON-FK sebelum FK
		isFkI := strings.Contains(strings.ToUpper(sorted[i]), "FOREIGN KEY")
		isFkJ := strings.Contains(strings.ToUpper(sorted[j]), "FOREIGN KEY")

		if !isFkI && isFkJ {
			return true // NON_FK_I harus sebelum FK_J
		}
		if isFkI && !isFkJ {
			return false // FK_I harus setelah NON_FK_J
		}
		return false
	})
	return sorted
}

// sortAlterColumns mengurutkan DDL ALTER COLUMN (DROP, lalu MODIFY, lalu ADD).
func (s *SchemaSyncer) sortAlterColumns(ddls []string) []string {
	priority := func(ddl string) int {
		upperDDL := strings.ToUpper(ddl)
		if strings.Contains(upperDDL, "DROP COLUMN") {
			return 1
		}
		if strings.Contains(upperDDL, "MODIFY COLUMN") || strings.Contains(upperDDL, "ALTER COLUMN") { // ALTER COLUMN TYPE, SET DEFAULT, dll.
			return 2
		}
		if strings.Contains(upperDDL, "ADD COLUMN") {
			return 3
		}
		return 4 // Lainnya
	}

	sorted := make([]string, len(ddls))
	copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		return priority(sorted[i]) < priority(sorted[j])
	})
	return sorted
}

// sortDropIndexes tidak memerlukan logika khusus saat ini.
func (s *SchemaSyncer) sortDropIndexes(ddls []string) []string {
	// Bisa ditambahkan logika jika ada dependensi antar drop index
	return ddls
}

// sortAddIndexes tidak memerlukan logika khusus saat ini.
func (s *SchemaSyncer) sortAddIndexes(ddls []string) []string {
	// Bisa ditambahkan logika jika ada dependensi antar add index
	return ddls
}
