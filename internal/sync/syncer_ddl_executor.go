package sync

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"go.uber.org/multierr"
	"go.uber.org/zap"
	"gorm.io/gorm"
	// "github.com/arwahdevops/dbsync/internal/utils" // Tidak digunakan
)

// parseAndCategorizeDDLs ... (tetap sama seperti versi lengkap terakhir Anda)
func (s *SchemaSyncer) parseAndCategorizeDDLs(ddls *SchemaExecutionResult, table string) (*categorizedDDLs, error) {
	log := s.logger.With(zap.String("table", table), zap.String("phase", "parse-ddls"))
	parsed := &categorizedDDLs{}

	if ddls.TableDDL != "" {
		potentialStatements := strings.Split(ddls.TableDDL, ";")
		firstProcessed := false
		var remainingAlters []string

		for i, stmt := range potentialStatements {
			trimmed := strings.TrimSpace(stmt)
			if trimmed == "" {
				continue
			}
			upperTrimmed := strings.ToUpper(trimmed)

			if i == 0 && strings.HasPrefix(upperTrimmed, "CREATE TABLE") {
				parsed.CreateTableDDL = trimmed
				firstProcessed = true
				if len(potentialStatements) > 1 {
					log.Warn("TableDDL contains CREATE TABLE and subsequent statements. Only the CREATE TABLE statement will be categorized as such.", zap.String("full_table_ddl", ddls.TableDDL))
                    for _, subsequentStmt := range potentialStatements[1:] {
                        trimmedSubsequent := strings.TrimSpace(subsequentStmt)
                        if trimmedSubsequent != "" && strings.HasPrefix(strings.ToUpper(trimmedSubsequent), "ALTER TABLE") {
                            parsed.AlterColumnDDLs = append(parsed.AlterColumnDDLs, trimmedSubsequent)
                        }
                    }
                    break 
				}
			} else if strings.HasPrefix(upperTrimmed, "ALTER TABLE") {
				parsed.AlterColumnDDLs = append(parsed.AlterColumnDDLs, trimmed)
				firstProcessed = true
			} else if !firstProcessed && trimmed != "" {
				log.Warn("TableDDL starts with an unrecognized statement, attempting to categorize as ALTER.", zap.String("statement", trimmed))
				parsed.AlterColumnDDLs = append(parsed.AlterColumnDDLs, trimmed)
				firstProcessed = true
			} else if firstProcessed && trimmed != "" { 
                 if strings.HasPrefix(upperTrimmed, "ALTER TABLE") {
                    parsed.AlterColumnDDLs = append(parsed.AlterColumnDDLs, trimmed)
                } else {
                    log.Warn("Ignoring unrecognized subsequent statement in TableDDL after first processed statement.", zap.String("statement", trimmed))
                }
            }
		}
		for _, stmt := range remainingAlters {
			trimmed := strings.TrimSpace(stmt)
			if trimmed != "" && strings.HasPrefix(strings.ToUpper(trimmed), "ALTER TABLE") {
				parsed.AlterColumnDDLs = append(parsed.AlterColumnDDLs, trimmed)
			} else if trimmed != "" {
				log.Warn("Ignoring unrecognized statement found after CREATE TABLE in TableDDL.", zap.String("statement", trimmed))
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
		cleanDDL := strings.TrimRight(strings.TrimSpace(ddl), ";")
		if cleanDDL == "" {
			continue
		}

		log.Debug("Executing DDL.", zap.String("phase_name", phaseName), zap.String("ddl", cleanDDL))
		if err := tx.Exec(cleanDDL).Error; err != nil {
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

func (s *SchemaSyncer) shouldIgnoreDDLError(err error) bool {
	if err == nil { return false }
	errStrOriginal := err.Error()
	errStrLower := strings.ToLower(errStrOriginal)
	sqlState := ""

	reSQLState := regexp.MustCompile(`\(sqlstate\s+([a-z0-9]{5})\)`)
	matches := reSQLState.FindStringSubmatch(errStrLower)
	if len(matches) > 1 {
		sqlState = strings.ToUpper(matches[1])
	}

	ignorableSQLStates := map[string][]string{
		"postgres": {
			"42P07", // DUPLICATE_TABLE, DUPLICATE_SCHEMA, DUPLICATE_INDEX, etc.
			"42710", // DUPLICATE_OBJECT (constraint, type)
			"42704", // UNDEFINED_OBJECT (index, sequence, type, etc. does not exist for DROP IF EXISTS)
			"42P01", // UNDEFINED_TABLE (table "..." does not exist for DROP IF EXISTS)
		},
		"mysql": {}, // MySQL errors lebih sering diidentifikasi dengan error code atau pesan
	}

	if states, ok := ignorableSQLStates[s.dstDialect]; ok && sqlState != "" {
		for _, state := range states {
			if sqlState == state {
				s.logger.Debug("DDL error matched ignorable SQLSTATE.",
					zap.String("dialect", s.dstDialect),
					zap.String("sqlstate", sqlState),
					zap.String("error_original", errStrOriginal))
				return true
			}
		}
	}

	ignorableMessagePatterns := map[string][]string{
		"mysql": {
			`duplicate key name`,                                                           // Error 1061
			`can't drop index ('[^']+'|` + "`[^`]*`" + `)\s*; check that it exists`,          // Error 1091 (Index) - menangani quote
			`can't drop constraint ('[^']+'|` + "`[^`]*`" + `)\s*; check that it exists`,    // Error 1091 (Constraint)
			`check constraint.*?already exists`,                                             // MySQL 8.0.16+ (nama constraint bisa di-quote atau tidak)
			`constraint '.*?' does not exist`,                                              // Untuk DROP
			`table '.*?' already exists`,                                                   // Error 1050
			`unknown table '.*?'`,                                                          // Error 1051 (untuk DROP)
			`duplicate column name`,                                                        // Error 1060
			`foreign key constraint.*?already exists`,                                      // Error 1826
			`index .*? already exists on table`,                                            // MariaDB
		},
		"postgres": {
			`relation ".*?" already exists`,
			`index ".*?" already exists`,
			`constraint ".*?" for relation ".*?" already exists`,
			`constraint ".*?" on relation ".*?" already exists`,
			`constraint ".*?" on table ".*?" does not exist`,
			`index ".*?" does not exist`,
			`table ".*?" does not exist`,
			`type ".*?" already exists`,
			`schema ".*?" already exists`,
		},
		"sqlite": {
			`index .*? already exists`, // Tambahkan ? untuk non-greedy jika nama bisa mengandung " already exists"
			`table .*? already exists`,
			`no such index`,
			`no such table`,
		},
	}

	if patterns, ok := ignorableMessagePatterns[s.dstDialect]; ok {
		for _, pattern := range patterns {
			matched, _ := regexp.MatchString("(?i)"+pattern, errStrLower) // (?i) untuk case-insensitive
			if matched {
				s.logger.Debug("DDL error matched ignorable message pattern.",
					zap.String("dialect", s.dstDialect),
					zap.String("pattern", pattern),
					zap.String("error_original", errStrOriginal))
				return true
			}
		}
	}

	s.logger.Debug("DDL error did not match any ignorable SQLSTATEs or message patterns and will NOT be ignored.",
		zap.String("dialect", s.dstDialect),
		zap.String("sqlstate_extracted", sqlState),
		zap.String("error_original", errStrOriginal))
	return false
}


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
