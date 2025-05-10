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

// categorizedDDLs (definisi ada di syncer_types.go)

// cleanSingleDDL membersihkan satu string DDL dari spasi berlebih dan titik koma akhir.
func cleanSingleDDL(ddl string) string {
	return strings.TrimRight(strings.TrimSpace(ddl), ";")
}

// parseAndCategorizeDDLs mem-parse dan mengkategorikan DDL dari SchemaExecutionResult.
// Semua DDL yang dikembalikan dalam categorizedDDLs akan di-trim dari spasi dan titik koma akhir.
func (s *SchemaSyncer) parseAndCategorizeDDLs(ddls *SchemaExecutionResult, table string) (*categorizedDDLs, error) {
	log := s.logger.With(zap.String("table", table), zap.String("phase", "parse-ddls"))
	parsed := &categorizedDDLs{
		AlterColumnDDLs:    make([]string, 0),
		AddIndexDDLs:       make([]string, 0),
		DropIndexDDLs:      make([]string, 0),
		AddConstraintDDLs:  make([]string, 0),
		DropConstraintDDLs: make([]string, 0),
	}

	// Proses TableDDL
	if ddls.TableDDL != "" {
		// Split TableDDL menjadi statement individual berdasarkan titik koma.
		// Ini penting jika TableDDL secara tidak sengaja berisi beberapa statement.
		potentialStatements := strings.Split(ddls.TableDDL, ";")
		isCreateTableProcessed := false

		for i, stmt := range potentialStatements {
			cleanedStmt := cleanSingleDDL(stmt)
			if cleanedStmt == "" {
				continue
			}
			upperCleanedStmt := strings.ToUpper(cleanedStmt)

			if !isCreateTableProcessed && strings.HasPrefix(upperCleanedStmt, "CREATE TABLE") {
				parsed.CreateTableDDL = cleanedStmt
				isCreateTableProcessed = true
				log.Debug("Categorized CREATE TABLE DDL.", zap.String("ddl", parsed.CreateTableDDL))
				// Jika ini adalah statement CREATE TABLE, kita tidak mengharapkan statement lain
				// yang signifikan untuk struktur tabel dasar dalam *TableDDL* yang sama.
				// Jika ada, itu mungkin anomali dari generator DDL atau input manual.
				if i < len(potentialStatements)-1 {
					// Periksa apakah ada statement non-kosong setelah CREATE TABLE
					hasNextNonEmpty := false
					for j := i + 1; j < len(potentialStatements); j++ {
						if cleanSingleDDL(potentialStatements[j]) != "" {
							hasNextNonEmpty = true
							break
						}
					}
					if hasNextNonEmpty {
						log.Warn("TableDDL contains CREATE TABLE followed by other non-empty statements. This is unusual. Only the CREATE TABLE statement is processed as such from this block.",
							zap.String("create_table_part", parsed.CreateTableDDL),
							zap.String("full_table_ddl", ddls.TableDDL))
						// Kita akan menghentikan pemrosesan TableDDL setelah CREATE TABLE ditemukan
						// untuk menghindari salah interpretasi. DDL lain harusnya ada di IndexDDLs atau ConstraintDDLs.
						// Jika DDL lain ini adalah ALTER, mereka seharusnya dihasilkan secara terpisah.
						break
					}
				}
			} else if strings.HasPrefix(upperCleanedStmt, "ALTER TABLE") {
				parsed.AlterColumnDDLs = append(parsed.AlterColumnDDLs, cleanedStmt)
				log.Debug("Categorized ALTER TABLE DDL from TableDDL.", zap.String("ddl", cleanedStmt))
			} else {
				// Jika bukan CREATE TABLE (dan belum diproses) atau ALTER TABLE.
				// Ini adalah statement yang tidak dikenal dalam konteks TableDDL.
				log.Warn("Unrecognized statement in TableDDL. If this is the first statement, it's categorized as a general column alteration; otherwise, it's ignored.",
					zap.String("statement", cleanedStmt),
					zap.Bool("is_first_statement_in_block", i == 0 && !isCreateTableProcessed))
				if i == 0 && !isCreateTableProcessed { // Hanya jika ini statement pertama dan bukan CREATE
					parsed.AlterColumnDDLs = append(parsed.AlterColumnDDLs, cleanedStmt)
				}
			}
		}
	}

	// Proses IndexDDLs
	for _, ddl := range ddls.IndexDDLs {
		cleanedDDL := cleanSingleDDL(ddl)
		if cleanedDDL == "" { continue }
		upperCleanedDDL := strings.ToUpper(cleanedDDL)
		if strings.Contains(upperCleanedDDL, "DROP INDEX") {
			parsed.DropIndexDDLs = append(parsed.DropIndexDDLs, cleanedDDL)
		} else if strings.HasPrefix(upperCleanedDDL, "CREATE INDEX") || strings.HasPrefix(upperCleanedDDL, "CREATE UNIQUE INDEX") {
			parsed.AddIndexDDLs = append(parsed.AddIndexDDLs, cleanedDDL)
		} else {
			log.Warn("Unknown DDL type in IndexDDLs, skipping categorization.", zap.String("ddl", cleanedDDL))
		}
	}

	// Proses ConstraintDDLs
	for _, ddl := range ddls.ConstraintDDLs {
		cleanedDDL := cleanSingleDDL(ddl)
		if cleanedDDL == "" { continue }
		upperCleanedDDL := strings.ToUpper(cleanedDDL)
		if strings.Contains(upperCleanedDDL, "DROP CONSTRAINT") ||
			strings.Contains(upperCleanedDDL, "DROP FOREIGN KEY") || // MySQL specific
			strings.Contains(upperCleanedDDL, "DROP CHECK") ||       // MySQL specific
			strings.Contains(upperCleanedDDL, "DROP PRIMARY KEY") { // MySQL specific
			parsed.DropConstraintDDLs = append(parsed.DropConstraintDDLs, cleanedDDL)
		} else if strings.HasPrefix(upperCleanedDDL, "ALTER TABLE") && strings.Contains(upperCleanedDDL, "ADD CONSTRAINT") {
			parsed.AddConstraintDDLs = append(parsed.AddConstraintDDLs, cleanedDDL)
		} else if strings.HasPrefix(upperCleanedDDL, "ADD CONSTRAINT") { // Sintaks non-ALTER TABLE (jarang, mungkin Oracle)
			log.Debug("Found 'ADD CONSTRAINT' DDL without 'ALTER TABLE' prefix. Categorizing as ADD.", zap.String("ddl", cleanedDDL))
			parsed.AddConstraintDDLs = append(parsed.AddConstraintDDLs, cleanedDDL)
		} else {
			log.Warn("Unknown DDL type in ConstraintDDLs, skipping categorization.", zap.String("ddl", cleanedDDL))
		}
	}

	// Urutkan DDL yang dikategorikan
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
		// DDL sudah di-clean oleh parseAndCategorizeDDLs
		if ddl == "" { continue }

		log.Debug("Executing DDL.", zap.String("phase_name", phaseName), zap.String("ddl", ddl))
		if err := tx.Exec(ddl).Error; err != nil {
			execErrLog := log.With(zap.String("phase_name", phaseName), zap.String("failed_ddl", ddl), zap.Error(err))
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
		log.Warn("DDL phase completed with accumulated (ignorable or non-fatal) errors.",
			zap.String("phase_name", phaseName),
			zap.NamedError("accumulated_ddl_errors", accumulatedErrors))
	}
	return accumulatedErrors
}

func (s *SchemaSyncer) shouldIgnoreDDLError(err error) bool {
	if err == nil { return false }

	errStrOriginal := err.Error()
	errStrLower := strings.ToLower(errStrOriginal)
	sqlStateOrCode := ""

	reSQLStateOrCode := regexp.MustCompile(`\((?:sqlstate|error code|error)\s+([a-z0-9_]{2,})\)`)
	matches := reSQLStateOrCode.FindStringSubmatch(errStrLower)
	if len(matches) > 1 {
		sqlStateOrCode = strings.ToUpper(matches[1])
	}

	ignorableSQLStates := map[string][]string{
		"postgres": {"42P07", "42710", "42704", "42P01"},
		"mysql":    {}, "sqlite": {},
	}

	if states, ok := ignorableSQLStates[s.dstDialect]; ok && sqlStateOrCode != "" {
		if len(sqlStateOrCode) == 5 && regexp.MustCompile(`^[A-Z0-9]{5}$`).MatchString(sqlStateOrCode) {
			for _, state := range states {
				if sqlStateOrCode == state {
					s.logger.Debug("DDL error matched ignorable SQLSTATE.", zap.String("dialect", s.dstDialect), zap.String("sqlstate", sqlStateOrCode), zap.String("error_original", errStrOriginal))
					return true
				}
			}
		}
	}

	// Pola untuk nama objek yang mungkin dikutip atau tidak.
	// `(?:` + "`" + `(?:[^` + "`" + `]|\` + "`" + `\` + "`" + `)*` + "`" + `|'[^']*'|"[^"]*"|[^\\s;"'` + "`" + `()[\],.:]+)`
	// Lebih disederhanakan untuk menghindari over-matching:
	// `(?:` + "`[^`]*`" + `|'[^']*'|"[^"]*"|[\w_.-]+)`
	//   - Diapit backtick (tanpa escape internal yang kompleks)
	//   - Diapit single quote
	//   - Diapit double quote
	//   - Sekumpulan karakter word (huruf, angka, underscore), titik, atau tanda hubung.
	objectNamePattern := `(?:` + "`[^`]*`" + `|'[^']*'|"[^"]*"|[\w.-]+)` // \w mencakup huruf, angka, _
	tableNamePattern := `(?:` + objectNamePattern + `\.` + objectNamePattern + `|` + objectNamePattern + `)`

	ignorableMessagePatterns := map[string][]string{
		"mysql": {
			`duplicate key name\s+` + objectNamePattern,
			`can't drop (?:index|constraint)\s+` + objectNamePattern + `(?:\s*on\s+` + tableNamePattern + `)?\s*;?\s*check that it exists`,
			`check constraint\s+` + objectNamePattern + `\s+already exists`,
			`(?:foreign key\s+)?constraint(?:\s+` + objectNamePattern + `)?\s+(?:for key\s+` + objectNamePattern + `\s*)?already exists`,
			`constraint\s+` + objectNamePattern + `\s+does not exist`,
			`table\s+` + tableNamePattern + `\s+already exists`,
			`unknown table\s+` + tableNamePattern,
			`duplicate column name\s+` + objectNamePattern,
			`index\s+` + objectNamePattern + `\s+already exists on table\s+` + tableNamePattern,
		},
		"postgres": {
			`relation\s+"[^"]+"\s+already exists`,
			`index\s+"[^"]+"\s+already exists`,
			`constraint\s+"[^"]+"\s+(?:for|on) relation\s+"[^"]+"\s+already exists`,
			`constraint\s+"[^"]+"\s+on table\s+"[^"]+"\s+does not exist`,
			`index\s+"[^"]+"\s+does not exist`,
			`table\s+"[^"]+"\s+does not exist`,
			`type\s+"[^"]+"\s+already exists`,
			`schema\s+"[^"]+"\s+already exists`,
		},
		"sqlite": {
			`index\s+` + objectNamePattern + `\s+already exists`,
			`table\s+` + objectNamePattern + `\s+already exists`,
			`no such index:\s*` + objectNamePattern,
			`no such table:\s*` + objectNamePattern,
			`(?:unique\s+)?constraint\s+` + objectNamePattern + `\s+already exists`,
			`constraint\s+` + objectNamePattern + `\s+failed`,
			`column\s+` + objectNamePattern + `\s+already exists`,
		},
	}

	if patterns, ok := ignorableMessagePatterns[s.dstDialect]; ok {
		for _, pattern := range patterns {
			fullPattern := `(?i)` + pattern
			matched, _ := regexp.MatchString(fullPattern, errStrLower)
			if matched {
				s.logger.Debug("DDL error matched ignorable message pattern.", zap.String("dialect", s.dstDialect), zap.String("pattern_used", fullPattern), zap.String("error_original", errStrOriginal))
				return true
			}
		}
	}

	s.logger.Debug("DDL error did not match any ignorable SQLSTATEs or message patterns and will NOT be ignored.", zap.String("dialect", s.dstDialect), zap.String("sqlstate_or_code_extracted", sqlStateOrCode), zap.String("error_original", errStrOriginal))
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

func (s *SchemaSyncer) sortConstraintsForDrop(ddls []string) []string {
	extractConstraintNameAndType := func(ddl string) (string, string) {
		upperDDL := strings.ToUpper(ddl)
		re := regexp.MustCompile(`(?i)DROP (?:CONSTRAINT\s+(` + "`?" + `[^` + "`" + `\s;]+` + "`?" + `)|(FOREIGN KEY)\s+(` + "`?" + `[^` + "`" + `\s;]+` + "`?" + `)|(CHECK)\s+(` + "`?" + `[^` + "`" + `\s;]+` + "`?" + `)|(PRIMARY KEY))`)
		matches := re.FindStringSubmatch(upperDDL)

		if len(matches) > 0 {
			if matches[6] != "" { // PRIMARY KEY
				return "PRIMARY_KEY_IMPLICIT", "PRIMARY KEY"
			} else if matches[2] != "" && matches[3] != "" { // FOREIGN KEY name
				return strings.Trim(matches[3], "`'\""), "FOREIGN KEY"
			} else if matches[4] != "" && matches[5] != "" { // CHECK name
				return strings.Trim(matches[5], "`'\""), "CHECK"
			} else if matches[1] != "" { // CONSTRAINT name
				name := strings.Trim(matches[1], "`'\"")
				if strings.HasPrefix(name, "FK_") || strings.Contains(name, "_FK_") || strings.HasSuffix(name, "_FK") || strings.HasPrefix(name, "fk_") { return name, "FOREIGN KEY" }
				if strings.HasPrefix(name, "UQ_") || strings.Contains(name, "_UQ_") || strings.HasSuffix(name, "_UQ") || strings.HasPrefix(name, "uq_") { return name, "UNIQUE" }
				if strings.HasPrefix(name, "PK_") || strings.Contains(name, "_PK_") || strings.HasSuffix(name, "_PK") || strings.HasPrefix(name, "pk_") { return name, "PRIMARY KEY" }
				if strings.HasPrefix(name, "CHK_") || strings.Contains(name, "_CHK_") || strings.HasSuffix(name, "_CHK") || strings.HasPrefix(name, "chk_") { return name, "CHECK" }
				return name, "UNKNOWN_FROM_NAME"
			}
		}
		return "", "UNKNOWN_DDL_STRUCTURE"
	}

	priority := func(ddl string) int {
		_, consType := extractConstraintNameAndType(ddl)
		switch consType {
		case "FOREIGN KEY": return 1
		case "UNIQUE": return 2
		case "CHECK": return 3
		case "PRIMARY KEY": return 4
		case "UNKNOWN_FROM_NAME": return 5
		default:
			s.logger.Debug("DROP CONSTRAINT DDL with unrecognized structure for priority sorting.", zap.String("ddl", ddl))
			return 6
		}
	}

	sorted := make([]string, len(ddls))
	copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		priI := priority(sorted[i]); priJ := priority(sorted[j])
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
		s.logger.Debug("ADD CONSTRAINT DDL with unknown priority category during sorting.", zap.String("ddl", ddl))
		return 5
	}
	sorted := make([]string, len(ddls))
	copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		priI := priority(sorted[i]); priJ := priority(sorted[j])
		if priI != priJ { return priI < priJ }
		return sorted[i] < sorted[j]
	})
	return sorted
}

func (s *SchemaSyncer) sortAlterColumns(ddls []string) []string {
	priority := func(ddl string) int {
		upperDDL := strings.ToUpper(ddl)
		if strings.Contains(upperDDL, "DROP COLUMN") { return 1 }
		if strings.Contains(upperDDL, "MODIFY COLUMN") || strings.Contains(upperDDL, "ALTER COLUMN") { return 2 }
		if strings.Contains(upperDDL, "ADD COLUMN") { return 3 }
		if !strings.EqualFold(upperDDL, "UNKNOWN DDL STATEMENT") { // Hanya log jika bukan kasus uji spesifik
			s.logger.Debug("ALTER DDL with unknown priority category during sorting.", zap.String("ddl", ddl))
		}
		return 4
	}
	sorted := make([]string, len(ddls))
	copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		priI := priority(sorted[i]); priJ := priority(sorted[j])
		if priI != priJ { return priI < priJ }
		return sorted[i] < sorted[j]
	})
	return sorted
}

func (s *SchemaSyncer) sortDropIndexes(ddls []string) []string {
	sorted := make([]string, len(ddls))
	copy(sorted, ddls)
	sort.Strings(sorted)
	return sorted
}

func (s *SchemaSyncer) sortAddIndexes(ddls []string) []string {
	sorted := make([]string, len(ddls))
	copy(sorted, ddls)
	sort.Strings(sorted)
	return sorted
}
