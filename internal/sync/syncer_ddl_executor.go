// internal/sync/syncer_ddl_executor.go
package sync

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"go.uber.org/multierr"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// categorizedDDLs struct didefinisikan di syncer_types.go

func cleanSingleDDL(ddl string) string {
	reCommentLine := regexp.MustCompile(`--.*`)
	cleaned := reCommentLine.ReplaceAllString(ddl, "")
	reCommentBlock := regexp.MustCompile(`/\*.*?\*/`)
	cleaned = reCommentBlock.ReplaceAllString(cleaned, "")
	return strings.TrimRight(strings.TrimSpace(cleaned), ";")
}

func (s *SchemaSyncer) parseAndCategorizeDDLs(ddls *SchemaExecutionResult, table string) (*categorizedDDLs, error) {
	log := s.logger.With(zap.String("table", table), zap.String("phase", "parse-ddls"))
	parsed := &categorizedDDLs{
		AlterColumnDDLs:    make([]string, 0), AddIndexDDLs: make([]string, 0),
		DropIndexDDLs:      make([]string, 0), AddConstraintDDLs: make([]string, 0),
		DropConstraintDDLs: make([]string, 0),
	}

	if ddls == nil {
		log.Debug("Input SchemaExecutionResult is nil, no DDLs to parse.")
		return parsed, nil
	}

	if ddls.TableDDL != "" {
		potentialStatements := strings.Split(ddls.TableDDL, ";")
		isCreateTableProcessed := false

		for _, stmt := range potentialStatements { // Variabel 'i' dihapus karena tidak digunakan
			cleanedStmt := cleanSingleDDL(stmt)
			if cleanedStmt == "" {
				continue
			}
			upperCleanedStmt := strings.ToUpper(cleanedStmt)

			if !isCreateTableProcessed && strings.HasPrefix(upperCleanedStmt, "CREATE TABLE") {
				if parsed.CreateTableDDL != "" {
					log.Error("Multiple CREATE TABLE statements found in TableDDL, this is unexpected.", zap.String("existing_create", parsed.CreateTableDDL), zap.String("new_create", cleanedStmt))
					return nil, fmt.Errorf("multiple CREATE TABLE statements found for table '%s'", table)
				}
				parsed.CreateTableDDL = cleanedStmt
				isCreateTableProcessed = true
				log.Debug("Categorized CREATE TABLE DDL.", zap.String("ddl", parsed.CreateTableDDL))
			} else if strings.HasPrefix(upperCleanedStmt, "ALTER TABLE") {
				parsed.AlterColumnDDLs = append(parsed.AlterColumnDDLs, cleanedStmt)
				log.Debug("Categorized ALTER TABLE DDL from TableDDL string.", zap.String("ddl", cleanedStmt))
			} else {
				log.Warn("Unrecognized DDL statement in TableDDL input, categorizing as general column alteration. Review DDL.", zap.String("statement", cleanedStmt))
				parsed.AlterColumnDDLs = append(parsed.AlterColumnDDLs, cleanedStmt)
			}
		}
	}

	for _, ddl := range ddls.IndexDDLs {
		cleanedDDL := cleanSingleDDL(ddl)
		if cleanedDDL == "" { continue }
		upperCleanedDDL := strings.ToUpper(cleanedDDL)
		if strings.HasPrefix(upperCleanedDDL, "DROP INDEX") {
			parsed.DropIndexDDLs = append(parsed.DropIndexDDLs, cleanedDDL)
		} else if strings.HasPrefix(upperCleanedDDL, "CREATE INDEX") || strings.HasPrefix(upperCleanedDDL, "CREATE UNIQUE INDEX") {
			parsed.AddIndexDDLs = append(parsed.AddIndexDDLs, cleanedDDL)
		} else {
			log.Warn("Unknown DDL type in IndexDDLs, skipping categorization for this DDL.", zap.String("ddl", cleanedDDL))
		}
	}

	for _, ddl := range ddls.ConstraintDDLs {
		cleanedDDL := cleanSingleDDL(ddl)
		if cleanedDDL == "" { continue }
		upperCleanedDDL := strings.ToUpper(cleanedDDL)
		isDrop := false
		if strings.HasPrefix(upperCleanedDDL, "ALTER TABLE") {
			if strings.Contains(upperCleanedDDL, "DROP CONSTRAINT") ||
				strings.Contains(upperCleanedDDL, "DROP FOREIGN KEY") ||
				strings.Contains(upperCleanedDDL, "DROP CHECK") ||
				strings.Contains(upperCleanedDDL, "DROP PRIMARY KEY") {
				isDrop = true
			}
		} else if strings.HasPrefix(upperCleanedDDL, "DROP CONSTRAINT") {
			isDrop = true
		}

		if isDrop {
			parsed.DropConstraintDDLs = append(parsed.DropConstraintDDLs, cleanedDDL)
		} else if strings.HasPrefix(upperCleanedDDL, "ALTER TABLE") && strings.Contains(upperCleanedDDL, "ADD CONSTRAINT") {
			parsed.AddConstraintDDLs = append(parsed.AddConstraintDDLs, cleanedDDL)
		} else if strings.HasPrefix(upperCleanedDDL, "ADD CONSTRAINT") {
			log.Debug("Found 'ADD CONSTRAINT' DDL without 'ALTER TABLE' prefix. Categorizing as ADD.", zap.String("ddl", cleanedDDL))
			parsed.AddConstraintDDLs = append(parsed.AddConstraintDDLs, cleanedDDL)
		} else {
			log.Warn("Unknown DDL type in ConstraintDDLs, skipping categorization for this DDL.", zap.String("ddl", cleanedDDL))
		}
	}

	parsed.AlterColumnDDLs = s.sortAlterColumns(parsed.AlterColumnDDLs)
	parsed.DropIndexDDLs = s.sortDropIndexes(parsed.DropIndexDDLs)
	parsed.AddIndexDDLs = s.sortAddIndexes(parsed.AddIndexDDLs)
	parsed.DropConstraintDDLs = s.sortConstraintsForDrop(parsed.DropConstraintDDLs)
	parsed.AddConstraintDDLs = s.sortConstraintsForAdd(parsed.AddConstraintDDLs)

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
		log.Debug("Executing DDL.", zap.String("phase_name", phaseName), zap.String("ddl", ddl))
		if err := tx.Exec(ddl).Error; err != nil {
			execErrLog := log.With(zap.String("phase_name", phaseName), zap.String("failed_ddl", ddl), zap.Error(err))
			if s.shouldIgnoreDDLError(err, ddl) {
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
		log.Warn("DDL phase completed with accumulated (ignorable or non-fatal) errors.",
			zap.String("phase_name", phaseName),
			zap.NamedError("accumulated_ddl_errors", accumulatedErrors))
		return accumulatedErrors
	}
	log.Debug("DDL phase executed successfully.", zap.String("phase_name", phaseName))
	return nil
}

func (s *SchemaSyncer) shouldIgnoreDDLError(err error, ddl string) bool {
	if err == nil { return false }

	errStrOriginal := err.Error()
	errStrLower := strings.ToLower(errStrOriginal)
	upperDDL := strings.ToUpper(ddl)
	sqlState, numericErrorCode := "", ""

	if matches := regexp.MustCompile(`\(sqlstate\s+([a-z0-9]{5})\)`).FindStringSubmatch(errStrLower); len(matches) > 1 {
		sqlState = strings.ToUpper(matches[1])
	}
	if matches := regexp.MustCompile(`error\s+(\d+)`).FindStringSubmatch(errStrLower); len(matches) > 1 {
		numericErrorCode = matches[1]
	} else if matches := regexp.MustCompile(`error\s+code:\s*(\d+)`).FindStringSubmatch(errStrLower); len(matches) > 1 {
		numericErrorCode = matches[1]
	}

	isDrop := strings.HasPrefix(upperDDL, "DROP") || strings.Contains(upperDDL, "DROP CONSTRAINT") ||
		strings.Contains(upperDDL, "DROP FOREIGN KEY") || strings.Contains(upperDDL, "DROP CHECK") ||
		strings.Contains(upperDDL, "DROP PRIMARY KEY") || strings.Contains(upperDDL, "DROP COLUMN")

	isCreateOrAdd := strings.HasPrefix(upperDDL, "CREATE") || strings.Contains(upperDDL, "ADD CONSTRAINT") ||
		strings.Contains(upperDDL, "ADD COLUMN")

	logCtx := s.logger.With(zap.String("dialect", s.dstDialect), zap.String("sqlstate", sqlState),
		zap.String("error_code", numericErrorCode), zap.String("ddl_preview", truncateForLog(ddl, 70)),
		zap.String("original_error", errStrOriginal))

	switch s.dstDialect {
	case "postgres":
		if (sqlState == "42P07" || sqlState == "42710") && isCreateOrAdd {
			logCtx.Debug("Ignoring PostgreSQL duplicate object error (CREATE/ADD).")
			return true
		}
		if (sqlState == "42704" || sqlState == "42P01") && isDrop {
			if sqlState == "42P01" && strings.Contains(upperDDL, "ADD CONSTRAINT") && strings.Contains(upperDDL, "FOREIGN KEY") {
				logCtx.Debug("PostgreSQL 42P01 (undefined_table) for ADD FOREIGN KEY not ignored.")
				return false
			}
			logCtx.Debug("Ignoring PostgreSQL object does not exist error (DROP).")
			return true
		}
	case "mysql":
		if isCreateOrAdd {
			if numericErrorCode == "1050" /*Table already exists*/ ||
				numericErrorCode == "1061" /*Duplicate key name (index)*/ ||
				numericErrorCode == "1060" /*Duplicate column name*/ ||
				numericErrorCode == "1826" /*FK constraint already exists*/ ||
				numericErrorCode == "3822" /*Check constraint already exists*/ ||
				numericErrorCode == "1022" /*Duplicate key (constraints)*/ {
				logCtx.Debug("MySQL: Ignoring 'already exists'/'duplicate' error by code (CREATE/ADD).")
				return true
			}
			// Fallback ke pesan untuk MySQL
			if regexp.MustCompile(`(?i)(already exists|duplicate (key|column|constraint|entry))`).MatchString(errStrLower) {
                 logCtx.Debug("MySQL: Ignoring 'already exists'/'duplicate' error by message (CREATE/ADD).")
                 return true
            }
		}
		if isDrop {
			if numericErrorCode == "1051" /*Unknown table*/ ||
				numericErrorCode == "1091" /*Can't DROP; check that it exists (index, constraint, column)*/ {
				logCtx.Debug("MySQL: Ignoring 'does not exist' error by code (DROP).")
				return true
			}
            // Fallback ke pesan untuk MySQL
            if regexp.MustCompile(`(?i)(unknown table|can't drop.*?check that.*?exists|does not exist)`).MatchString(errStrLower) {
                logCtx.Debug("MySQL: Ignoring 'does not exist' error by message (DROP).")
                return true
            }
		}
	case "sqlite":
		if (strings.Contains(errStrLower, "already exists") || strings.Contains(errStrLower, "duplicate column name")) && isCreateOrAdd {
			logCtx.Debug("SQLite: Ignoring 'already exists'/'duplicate' error (CREATE/ADD).")
			return true
		}
		if (strings.Contains(errStrLower, "no such table") && strings.HasPrefix(upperDDL, "DROP TABLE")) ||
			(strings.Contains(errStrLower, "no such index") && strings.HasPrefix(upperDDL, "DROP INDEX")) {
			logCtx.Debug("SQLite: Ignoring 'no such table/index' error (DROP).")
			return true
		}
	}
	logCtx.Debug("DDL error will NOT be ignored.")
	return false
}

func (s *SchemaSyncer) splitPostgresFKsForDeferredExecution(allConstraints []string) (deferredFKs []string, nonDeferredFKs []string) {
	for _, ddl := range allConstraints {
		upperDDL := strings.ToUpper(ddl)
		if strings.HasPrefix(upperDDL, "ALTER TABLE") && strings.Contains(upperDDL, "ADD CONSTRAINT") && strings.Contains(upperDDL, "FOREIGN KEY") {
			if strings.Contains(upperDDL, "DEFERRABLE") && strings.Contains(upperDDL, "INITIALLY DEFERRED") {
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

func extractConstraintNameAndTypeForDrop(ddl string, logger *zap.Logger) (string, string) {
	upperDDL := strings.ToUpper(ddl)
	var constraintName, constraintType string
	nameRegex := `("?\w+"?|` + "`\\w+`" + `|\w+)` // Regex untuk menangkap nama (quoted atau unquoted)

	if strings.Contains(upperDDL, "DROP PRIMARY KEY") {
		constraintName = "PRIMARY_KEY_IMPLICIT_DROP_NAME_FOR_SORT"
		constraintType = "PRIMARY KEY"
	} else if matches := regexp.MustCompile(`(?i)DROP\s+FOREIGN\s+KEY\s+` + nameRegex).FindStringSubmatch(upperDDL); len(matches) > 1 {
		constraintName = strings.Trim(matches[1], "\"`[]")
		constraintType = "FOREIGN KEY"
	} else if matches := regexp.MustCompile(`(?i)DROP\s+CHECK\s+` + nameRegex).FindStringSubmatch(upperDDL); len(matches) > 1 {
		constraintName = strings.Trim(matches[1], "\"`[]")
		constraintType = "CHECK"
	} else if matches := regexp.MustCompile(`(?i)DROP\s+CONSTRAINT\s+` + nameRegex).FindStringSubmatch(upperDDL); len(matches) > 1 {
		constraintName = strings.Trim(matches[1], "\"`[]")
		if constraintName != "" { // Menebak tipe dari nama jika ada nama
			lowerName := strings.ToLower(constraintName)
			if strings.Contains(lowerName, "fk_") || strings.Contains(lowerName, "_fk") || strings.Contains(lowerName, "foreign_key") { constraintType = "FOREIGN KEY"
			} else if strings.Contains(lowerName, "uq_") || strings.Contains(lowerName, "_uq") || strings.Contains(lowerName, "unique") { constraintType = "UNIQUE"
			} else if strings.Contains(lowerName, "pk_") || strings.Contains(lowerName, "_pk") || strings.Contains(lowerName, "primary_key") { constraintType = "PRIMARY KEY"
			} else if strings.Contains(lowerName, "chk_") || strings.Contains(lowerName, "_chk") || strings.Contains(lowerName, "check") { constraintType = "CHECK"
			} else { constraintType = "UNKNOWN_CONSTRAINT_TYPE_FROM_NAME" }
		} else { constraintType = "UNKNOWN_DROP_CONSTRAINT_NO_NAME_PARSED" }
	} else if matches := regexp.MustCompile(`(?i)DROP\s+INDEX\s+` + nameRegex).FindStringSubmatch(upperDDL); len(matches) > 1 {
		constraintName = strings.Trim(matches[1], "\"`[]")
		constraintType = "INDEX"
	} else {
		logger.Warn("Could not parse DDL for DROP operation to determine its type for sorting.", zap.String("ddl_for_sort", ddl))
		return ddl, "UNKNOWN_DDL_STRUCTURE_FOR_DROP_SORT"
	}

	if constraintName == "" && constraintType != "UNKNOWN_DDL_STRUCTURE_FOR_DROP_SORT" && constraintType != "" {
		if constraintType != "PRIMARY KEY" {
			constraintName = fmt.Sprintf("%s_IMPLICIT_DROP_NAME_FOR_SORT", strings.ReplaceAll(strings.ToLower(constraintType), " ", "_"))
		}
	} else if constraintName == "" {
		constraintName = fmt.Sprintf("UNNAMED_DROP_AT_%s", ddl[:min(len(ddl), 10)])
	}
	return constraintName, constraintType
}

func (s *SchemaSyncer) sortConstraintsForDrop(ddls []string) []string {
	priority := func(ddl string) int {
		_, consType := extractConstraintNameAndTypeForDrop(ddl, s.logger)
		switch consType {
		case "FOREIGN KEY": return 1; case "UNIQUE": return 2; case "CHECK": return 3
		case "PRIMARY KEY": return 4; case "INDEX": return 5
		case "UNKNOWN_CONSTRAINT_TYPE_FROM_NAME", "UNKNOWN_DROP_CONSTRAINT_NO_NAME_PARSED": return 6
		default: return 7 // UNKNOWN_DDL_STRUCTURE_FOR_DROP_SORT
		}
	}
	sorted := make([]string, len(ddls)); copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		priI, priJ := priority(sorted[i]), priority(sorted[j])
		if priI != priJ { return priI < priJ }
		return sorted[i] < sorted[j]
	})
	return sorted
}

func (s *SchemaSyncer) sortConstraintsForAdd(ddls []string) []string {
	priority := func(ddl string) int {
		upperDDL := strings.ToUpper(ddl)
		if strings.Contains(upperDDL, "PRIMARY KEY") { return 1 }
		if strings.Contains(upperDDL, "UNIQUE") { return 2 }
		if strings.Contains(upperDDL, "CHECK") { return 3 }
		if strings.Contains(upperDDL, "FOREIGN KEY") { return 4 }
		s.logger.Debug("ADD CONSTRAINT DDL with unknown priority category for sorting.", zap.String("ddl", ddl))
		return 5
	}
	sorted := make([]string, len(ddls)); copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		priI, priJ := priority(sorted[i]), priority(sorted[j])
		if priI != priJ { return priI < priJ }
		return sorted[i] < sorted[j]
	})
	return sorted
}

func (s *SchemaSyncer) sortAlterColumns(ddls []string) []string {
	priority := func(ddl string) int {
		upperDDL := strings.ToUpper(ddl)
		if strings.Contains(upperDDL, "DROP COLUMN") { return 1 }
		if strings.Contains(upperDDL, "MODIFY COLUMN") || (strings.Contains(upperDDL, "ALTER COLUMN") &&
			!strings.Contains(upperDDL, "ADD COLUMN") && !strings.Contains(upperDDL, "DROP COLUMN")) { return 2 }
		if strings.Contains(upperDDL, "ADD COLUMN") { return 3 }
		if ddl != "" && !strings.EqualFold(upperDDL, "UNKNOWN DDL STATEMENT") {
			s.logger.Debug("ALTER DDL with unknown priority category for sorting.", zap.String("ddl", ddl))
		}
		return 4
	}
	sorted := make([]string, len(ddls)); copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		priI, priJ := priority(sorted[i]), priority(sorted[j])
		if priI != priJ { return priI < priJ }
		return sorted[i] < sorted[j]
	})
	return sorted
}

func (s *SchemaSyncer) sortDropIndexes(ddls []string) []string {
	sorted := make([]string, len(ddls)); copy(sorted, ddls); sort.Strings(sorted); return sorted
}
func (s *SchemaSyncer) sortAddIndexes(ddls []string) []string {
	sorted := make([]string, len(ddls)); copy(sorted, ddls); sort.Strings(sorted); return sorted
}
