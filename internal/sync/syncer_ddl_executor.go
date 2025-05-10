package sync

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"go.uber.org/multierr" // Untuk menggabungkan error
	"go.uber.org/zap"
	"gorm.io/gorm"

	//"github.com/arwahdevops/dbsync/internal/utils" // Diperlukan untuk QuoteIdentifier di ExecuteDDLs
)

// parseAndCategorizeDDLs mem-parse DDL dari SchemaExecutionResult menjadi kategori yang lebih spesifik.
func (s *SchemaSyncer) parseAndCategorizeDDLs(ddls *SchemaExecutionResult, table string) (*categorizedDDLs, error) {
	log := s.logger.With(zap.String("table", table), zap.String("phase", "parse-ddls"))
	parsed := &categorizedDDLs{}

	if ddls.TableDDL != "" {
		// TableDDL bisa jadi satu statement CREATE atau beberapa statement ALTER yang dipisah ';'
		potentialStatements := strings.Split(ddls.TableDDL, ";")
		firstProcessed := false
		for _, stmt := range potentialStatements {
			trimmed := strings.TrimSpace(stmt)
			if trimmed == "" {
				continue
			}
			upperTrimmed := strings.ToUpper(trimmed)
			if !firstProcessed && strings.HasPrefix(upperTrimmed, "CREATE TABLE") {
				parsed.CreateTableDDL = trimmed // Hanya statement CREATE TABLE pertama yang dianggap
				firstProcessed = true
				// Jika ada statement lain setelah CREATE TABLE dalam TableDDL, itu mungkin aneh,
				// tapi kita bisa log atau abaikan. Untuk sekarang, kita fokus pada CREATE.
				if len(potentialStatements) > 1 {
					log.Warn("TableDDL contains CREATE TABLE and subsequent statements. Only the CREATE TABLE statement will be categorized as such.", zap.String("full_table_ddl", ddls.TableDDL))
					// Sisa statement akan coba dikategorikan sebagai ALTER jika ada.
                    for _, subsequentStmt := range potentialStatements[1:] {
                        trimmedSubsequent := strings.TrimSpace(subsequentStmt)
                        if trimmedSubsequent != "" && strings.HasPrefix(strings.ToUpper(trimmedSubsequent), "ALTER TABLE") {
                            parsed.AlterColumnDDLs = append(parsed.AlterColumnDDLs, trimmedSubsequent)
                        }
                    }
                    break // Keluar dari loop setelah memproses CREATE dan sisanya sebagai ALTER jika ada
				}
			} else if strings.HasPrefix(upperTrimmed, "ALTER TABLE") {
				parsed.AlterColumnDDLs = append(parsed.AlterColumnDDLs, trimmed)
				firstProcessed = true
			} else if !firstProcessed && trimmed != "" { // Jika statement pertama bukan CREATE/ALTER tapi ada
				log.Warn("TableDDL starts with an unrecognized statement, attempting to categorize as ALTER.", zap.String("statement", trimmed))
				parsed.AlterColumnDDLs = append(parsed.AlterColumnDDLs, trimmed)
				firstProcessed = true
			} else if firstProcessed && trimmed != "" { // Statement berikutnya setelah yang pertama
                 if strings.HasPrefix(upperTrimmed, "ALTER TABLE") {
                    parsed.AlterColumnDDLs = append(parsed.AlterColumnDDLs, trimmed)
                } else {
                    log.Warn("Ignoring unrecognized subsequent statement in TableDDL.", zap.String("statement", trimmed))
                }
            }
		}
	}


	for _, ddl := range ddls.IndexDDLs {
		trimmed := strings.TrimSpace(ddl)
		if trimmed == "" { continue }
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
		if trimmed == "" { continue }
		upperTrimmed := strings.ToUpper(trimmed)
		if strings.Contains(upperTrimmed, "DROP CONSTRAINT") ||
			strings.Contains(upperTrimmed, "DROP FOREIGN KEY") ||
			strings.Contains(upperTrimmed, "DROP CHECK") ||
			strings.Contains(upperTrimmed, "DROP PRIMARY KEY") {
			parsed.DropConstraintDDLs = append(parsed.DropConstraintDDLs, trimmed)
		} else if strings.HasPrefix(upperTrimmed, "ADD CONSTRAINT") ||
			(strings.HasPrefix(upperTrimmed, "ALTER TABLE") && strings.Contains(upperTrimmed, "ADD CONSTRAINT")) {
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
		if ddl == "" {
			continue
		}
		// Hapus titik koma di akhir jika ada, karena beberapa driver/DB mungkin tidak menyukainya
		// atau GORM Exec sudah menanganinya. Lebih aman tanpa.
		cleanDDL := strings.TrimRight(strings.TrimSpace(ddl), ";")
		if cleanDDL == "" {
			continue
		}

		log.Debug("Executing DDL.", zap.String("phase_name", phaseName), zap.String("ddl", cleanDDL))
		if err := tx.Exec(cleanDDL).Error; err != nil { // Gunakan cleanDDL
			// Sertakan DDL asli (dengan titik koma jika ada) dalam log untuk kejelasan
			execErrLog := log.With(zap.String("phase_name", phaseName), zap.String("failed_ddl_original", ddl), zap.Error(err))
			if s.shouldIgnoreDDLError(err) {
				execErrLog.Warn("DDL execution resulted in an ignorable error, continuing.")
			} else {
				currentErr := fmt.Errorf("failed DDL in phase '%s': [%s], error: %w", phaseName, ddl, err)
				if continueOnError {
					execErrLog.Error("Failed to execute DDL, but configured to continue. Error will be accumulated.")
					accumulatedErrors = multierr.Append(accumulatedErrors, currentErr)
				} else {
					execErrLog.Error("Failed to execute DDL, aborting phase and transaction.")
					return currentErr
				}
			}
		}
	}
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
	sqlState := ""

	// Coba ekstrak SQLSTATE jika ada (format PG: ERROR: ... (SQLSTATE XXXXX))
	reSQLState := regexp.MustCompile(`\(sqlstate\s+([a-z0-9]{5})\)`)
	matches := reSQLState.FindStringSubmatch(errStr)
	if len(matches) > 1 {
		sqlState = strings.ToUpper(matches[1]) // SQLSTATE biasanya uppercase
	}

	// Pola error berdasarkan SQLSTATE (lebih andal)
	ignorableSQLStates := map[string][]string{
		"postgres": {
			"42P07", // DUPLICATE_TABLE, juga bisa untuk indeks, sequence, view
			"42710", // DUPLICATE_OBJECT (misalnya, constraint, tipe)
			"42704", // UNDEFINED_OBJECT (berguna untuk DROP IF EXISTS yang gagal karena memang tidak ada)
			"23505", // UNIQUE_VIOLATION (bisa terjadi jika ADD UNIQUE CONSTRAINT dan data melanggar, tapi ini lebih ke data error)
			         // Mungkin tidak selalu ingin diabaikan.
		},
		"mysql": {
			// MySQL error codes dipetakan ke SQLSTATE, tapi pesan sering lebih berguna.
			// Error codes MySQL umum:
			// 1007: Can't create database; database exists
			// 1008: Can't drop database; database doesn't exist
			// 1050: Table '...' already exists
			// 1051: Unknown table '...'
			// 1060: Duplicate column name '...'
			// 1061: Duplicate key name '...'
			// 1062: Duplicate entry '...' for key ... (UNIQUE_VIOLATION)
			// 1091: Can't DROP '...'; check that it exists
			// 1826: Duplicate foreign key constraint name (MySQL 8+)
		},
	}

	if states, ok := ignorableSQLStates[s.dstDialect]; ok && sqlState != "" {
		for _, state := range states {
			if sqlState == state {
				s.logger.Debug("DDL error matched ignorable SQLSTATE.",
					zap.String("dialect", s.dstDialect),
					zap.String("sqlstate", sqlState),
					zap.String("error_string", errStr))
				return true
			}
		}
	}

	// Pola error berdasarkan string pesan (kurang andal, tapi fallback)
	ignorableMessagePatterns := map[string][]string{
		"mysql": {
			"duplicate key name",
			"can't drop index '.*'; check that it exists",
			"can't drop constraint '.*'; check that it exists",
			"check constraint .* already exists",
			"constraint .* does not exist",
			"table '.*' already exists",
			"unknown table '.*'", // Untuk DROP
			"already exists",
			"doesn't exist",
            "duplicate entry .* for key", // Untuk unique constraint add
            "foreign key constraint.*? already exists", // Error 1826
		},
		"postgres": {
			// Pola string jika SQLSTATE tidak cukup atau tidak ada
			`relation ".*" already exists`,
			`index ".*" already exists`,
			`constraint ".*" for relation ".*" already exists`,
			`constraint ".*" on table ".*" does not exist`,
			`index ".*" does not exist`,
			`table ".*" does not exist`,
			// Tambahkan jika ada pesan spesifik lain
		},
		"sqlite": {
			"index .* already exists",
			"table .* already exists",
			"no such index", // Untuk DROP
			"no such table", // Untuk DROP
			"already exists",
			"unique constraint failed", // Saat ADD UNIQUE CONSTRAINT dan data melanggar
		},
	}

	if patterns, ok := ignorableMessagePatterns[s.dstDialect]; ok {
		for _, pattern := range patterns {
			matched, _ := regexp.MatchString(pattern, errStr)
			if matched {
				s.logger.Debug("DDL error matched ignorable message pattern.",
					zap.String("dialect", s.dstDialect),
					zap.String("pattern", pattern),
					zap.String("error_string", errStr))
				return true
			}
		}
	}

	s.logger.Debug("DDL error did not match any ignorable SQLSTATEs or message patterns.",
		zap.String("dialect", s.dstDialect),
		zap.String("sqlstate_extracted", sqlState),
		zap.String("error_string", errStr))
	return false
}


// splitPostgresFKsForDeferredExecution memisahkan DDL FK PostgreSQL.
func (s *SchemaSyncer) splitPostgresFKsForDeferredExecution(allConstraints []string) (deferredFKs []string, nonDeferredFKs []string) {
	for _, ddl := range allConstraints {
		upperDDL := strings.ToUpper(ddl)
		if strings.Contains(upperDDL, "ADD CONSTRAINT") && strings.Contains(upperDDL, "FOREIGN KEY") {
			if strings.Contains(upperDDL, "DEFERRABLE") {
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
func (s *SchemaSyncer) sortConstraintsForDrop(ddls []string) []string {
	sorted := make([]string, len(ddls))
	copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		isFkI := strings.Contains(strings.ToUpper(sorted[i]), "FOREIGN KEY")
		isFkJ := strings.Contains(strings.ToUpper(sorted[j]), "FOREIGN KEY")
		if isFkI && !isFkJ { return true }
		if !isFkI && isFkJ { return false }
		return false
	})
	return sorted
}

func (s *SchemaSyncer) sortConstraintsForAdd(ddls []string) []string {
	sorted := make([]string, len(ddls))
	copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		isFkI := strings.Contains(strings.ToUpper(sorted[i]), "FOREIGN KEY")
		isFkJ := strings.Contains(strings.ToUpper(sorted[j]), "FOREIGN KEY")
		if !isFkI && isFkJ { return true }
		if isFkI && !isFkJ { return false }
		return false
	})
	return sorted
}

func (s *SchemaSyncer) sortAlterColumns(ddls []string) []string {
	priority := func(ddl string) int {
		upperDDL := strings.ToUpper(ddl)
		if strings.Contains(upperDDL, "DROP COLUMN") { return 1 }
		if strings.Contains(upperDDL, "MODIFY COLUMN") || strings.Contains(upperDDL, "ALTER COLUMN") { return 2 }
		if strings.Contains(upperDDL, "ADD COLUMN") { return 3 }
		s.logger.Warn("Encountered ALTER DDL with unknown priority category during sorting.", zap.String("ddl", ddl))
		return 4
	}
	sorted := make([]string, len(ddls))
	copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		return priority(sorted[i]) < priority(sorted[j])
	})
	return sorted
}

func (s *SchemaSyncer) sortDropIndexes(ddls []string) []string {
	return ddls
}

func (s *SchemaSyncer) sortAddIndexes(ddls []string) []string {
	return ddls
}
