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

// categorizedDDLs (definisi ada di syncer_types.go atau file terpisah jika ada)
// type categorizedDDLs struct {
// 	CreateTableDDL     string
// 	AlterColumnDDLs    []string
// 	AddIndexDDLs       []string
// 	DropIndexDDLs      []string
// 	AddConstraintDDLs  []string
// 	DropConstraintDDLs []string
// }


// cleanSingleDDL membersihkan satu string DDL dari spasi berlebih dan titik koma akhir.
func cleanSingleDDL(ddl string) string {
	return strings.TrimRight(strings.TrimSpace(ddl), ";")
}

func (s *SchemaSyncer) parseAndCategorizeDDLs(ddls *SchemaExecutionResult, table string) (*categorizedDDLs, error) {
	log := s.logger.With(zap.String("table", table), zap.String("phase", "parse-ddls"))
	parsed := &categorizedDDLs{
		AlterColumnDDLs:    make([]string, 0),
		AddIndexDDLs:       make([]string, 0),
		DropIndexDDLs:      make([]string, 0),
		AddConstraintDDLs:  make([]string, 0),
		DropConstraintDDLs: make([]string, 0),
	}

	if ddls.TableDDL != "" {
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
				if i < len(potentialStatements)-1 {
					remainingStatementsInTableDDL := false
					for j := i + 1; j < len(potentialStatements); j++ {
						subsequentCleanedStmt := cleanSingleDDL(potentialStatements[j])
						if subsequentCleanedStmt == "" {
							continue
						}
						remainingStatementsInTableDDL = true
						if strings.HasPrefix(strings.ToUpper(subsequentCleanedStmt), "ALTER TABLE") {
							parsed.AlterColumnDDLs = append(parsed.AlterColumnDDLs, subsequentCleanedStmt)
							log.Debug("Categorized subsequent ALTER TABLE DDL from TableDDL.", zap.String("ddl", subsequentCleanedStmt))
						} else {
							log.Warn("Ignoring unrecognized subsequent statement in TableDDL after CREATE TABLE.", zap.String("statement", subsequentCleanedStmt))
						}
					}
					if remainingStatementsInTableDDL {
						log.Warn("TableDDL contained CREATE TABLE and was followed by other statements; these were processed or logged.",
							zap.String("create_table_part", parsed.CreateTableDDL),
							zap.String("full_table_ddl_input", ddls.TableDDL))
					}
				}
				break // Setelah CREATE TABLE, sisa dari TableDDL (jika ada) sudah diproses atau diabaikan.
			} else if strings.HasPrefix(upperCleanedStmt, "ALTER TABLE") {
				parsed.AlterColumnDDLs = append(parsed.AlterColumnDDLs, cleanedStmt)
				log.Debug("Categorized ALTER TABLE DDL from TableDDL.", zap.String("ddl", cleanedStmt))
			} else {
				// Jika statement pertama bukan CREATE dan bukan ALTER
				if i == 0 && !isCreateTableProcessed {
					log.Warn("TableDDL starts with an unrecognized statement. This statement will be categorized as a general column alteration, but review is advised.", zap.String("statement", cleanedStmt))
					parsed.AlterColumnDDLs = append(parsed.AlterColumnDDLs, cleanedStmt)
				} else if cleanedStmt != "" { // Statement berikutnya yang tidak dikenal (setelah yang pertama bukan CREATE/ALTER)
					log.Warn("Ignoring unrecognized statement in TableDDL.", zap.String("statement", cleanedStmt))
				}
			}
		}
	}

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

	for _, ddl := range ddls.ConstraintDDLs {
		cleanedDDL := cleanSingleDDL(ddl)
		if cleanedDDL == "" { continue }
		upperCleanedDDL := strings.ToUpper(cleanedDDL)
		// Perluas deteksi DROP untuk berbagai sintaks constraint
		if strings.Contains(upperCleanedDDL, "DROP CONSTRAINT") ||
			strings.Contains(upperCleanedDDL, "DROP FOREIGN KEY") || // MySQL specific
			strings.Contains(upperCleanedDDL, "DROP CHECK") ||       // MySQL specific
			strings.Contains(upperCleanedDDL, "DROP PRIMARY KEY") { // MySQL specific / general
			parsed.DropConstraintDDLs = append(parsed.DropConstraintDDLs, cleanedDDL)
		} else if strings.HasPrefix(upperCleanedDDL, "ALTER TABLE") && strings.Contains(upperCleanedDDL, "ADD CONSTRAINT") {
			parsed.AddConstraintDDLs = append(parsed.AddConstraintDDLs, cleanedDDL)
		} else if strings.HasPrefix(upperCleanedDDL, "ADD CONSTRAINT") { // Kasus tanpa ALTER TABLE prefix (kurang umum)
			log.Debug("Found 'ADD CONSTRAINT' DDL without 'ALTER TABLE' prefix. Categorizing as ADD.", zap.String("ddl", cleanedDDL))
			parsed.AddConstraintDDLs = append(parsed.AddConstraintDDLs, cleanedDDL)
		} else {
			log.Warn("Unknown DDL type in ConstraintDDLs, skipping categorization.", zap.String("ddl", cleanedDDL))
		}
	}

	// Urutkan DDLs untuk eksekusi yang lebih deterministik dan aman
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
		if ddl == "" { // Bisa terjadi jika hasil cleanSingleDDL adalah string kosong
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
					return currentErr // Kembalikan error pertama yang tidak bisa diabaikan
				}
			}
		}
	}
	if accumulatedErrors != nil {
		log.Warn("DDL phase completed with accumulated (ignorable or non-fatal) errors.",
			zap.String("phase_name", phaseName),
			zap.NamedError("accumulated_ddl_errors", accumulatedErrors))
		// Jika continueOnError true, kita kembalikan accumulatedErrors.
		// Jika false, seharusnya kita sudah return error pertama.
		// Namun, untuk konsistensi, jika ada accumulatedErrors, kembalikan itu.
		return accumulatedErrors
	}
	return nil // Tidak ada error fatal, atau semua error diabaikan/diakumulasi
}

func (s *SchemaSyncer) shouldIgnoreDDLError(err error, ddl string) bool {
	if err == nil {
		return false
	}

	errStrOriginal := err.Error()
	errStrLower := strings.ToLower(errStrOriginal)
	upperDDL := strings.ToUpper(ddl)

	sqlState := ""
	numericErrorCode := ""

	reSQLState := regexp.MustCompile(`\(sqlstate\s+([a-z0-9]{5})\)`)
	matchesSQLState := reSQLState.FindStringSubmatch(errStrLower)
	if len(matchesSQLState) > 1 {
		sqlState = strings.ToUpper(matchesSQLState[1])
	}

	// Regex yang lebih baik untuk menangkap "Error NNNN" dari MySQL
	reMySQLErrorCode := regexp.MustCompile(`error\s+(\d+)`)
	matchesMySQLErrorCode := reMySQLErrorCode.FindStringSubmatch(errStrLower)
	if len(matchesMySQLErrorCode) > 1 {
		numericErrorCode = matchesMySQLErrorCode[1]
	} else {
		// Coba tangkap "Error Code: NNNN" (digunakan oleh beberapa driver/versi)
		reMySQLErrorCodeInParen := regexp.MustCompile(`error\s+code:\s*(\d+)`)
		matchesMySQLErrorCodeInParen := reMySQLErrorCodeInParen.FindStringSubmatch(errStrLower)
		if len(matchesMySQLErrorCodeInParen) > 1 {
			numericErrorCode = matchesMySQLErrorCodeInParen[1]
		}
	}


	isDropTable := strings.HasPrefix(upperDDL, "DROP TABLE")
	isDropIndex := strings.HasPrefix(upperDDL, "DROP INDEX")
	isDropConstraint := strings.Contains(upperDDL, "DROP CONSTRAINT") ||
		strings.Contains(upperDDL, "DROP FOREIGN KEY") ||
		strings.Contains(upperDDL, "DROP CHECK") ||
		strings.Contains(upperDDL, "DROP PRIMARY KEY")
	isDropType := strings.HasPrefix(upperDDL, "DROP TYPE")
	isDropSchema := strings.HasPrefix(upperDDL, "DROP SCHEMA")

	isCreateTable := strings.HasPrefix(upperDDL, "CREATE TABLE")
	isCreateIndex := strings.HasPrefix(upperDDL, "CREATE INDEX") || strings.HasPrefix(upperDDL, "CREATE UNIQUE INDEX")
	isAddConstraint := strings.Contains(upperDDL, "ADD CONSTRAINT")
	isAddColumn := strings.Contains(upperDDL, "ADD COLUMN")

	currentDialectLog := s.logger.With(zap.String("dialect", s.dstDialect),
		zap.String("sqlstate_extracted", sqlState),
		zap.String("numeric_error_code_extracted", numericErrorCode),
		zap.String("ddl_preview", ddl[:min(len(ddl), 70)]+"..."), // Menggunakan helper min
		zap.String("error_original", errStrOriginal),
	)

	switch s.dstDialect {
	case "postgres":
		if (sqlState == "42P07" || sqlState == "42710") &&
			(isCreateTable || isCreateIndex || isAddConstraint || strings.HasPrefix(upperDDL, "CREATE TYPE") || strings.HasPrefix(upperDDL, "CREATE SCHEMA")) {
			currentDialectLog.Debug("Ignoring PostgreSQL duplicate object error (CREATE attempt).")
			return true
		}
		if (sqlState == "42704" || sqlState == "42P01") &&
			(isDropTable || isDropIndex || isDropConstraint || isDropType || isDropSchema) {
			if sqlState == "42P01" && isAddConstraint && strings.Contains(upperDDL, "FOREIGN KEY") {
				currentDialectLog.Debug("PostgreSQL error 42P01 (undefined_table) for ADD FOREIGN KEY will NOT be ignored.")
				return false
			}
			currentDialectLog.Debug("Ignoring PostgreSQL object does not exist error (DROP attempt).")
			return true
		}

	case "mysql":
		if numericErrorCode == "1050" && isCreateTable { currentDialectLog.Debug("MySQL: Ignoring Error 1050 (Table already exists)."); return true }
		if numericErrorCode == "1061" && isCreateIndex { currentDialectLog.Debug("MySQL: Ignoring Error 1061 (Duplicate key name for index)."); return true }
		if numericErrorCode == "1060" && isAddColumn { currentDialectLog.Debug("MySQL: Ignoring Error 1060 (Duplicate column name)."); return true }
		if numericErrorCode == "1826" && isAddConstraint && strings.Contains(upperDDL, "FOREIGN KEY") { currentDialectLog.Debug("MySQL: Ignoring Error 1826 (FK already exists)."); return true }
		if numericErrorCode == "3822" && isAddConstraint && strings.Contains(upperDDL, "CHECK") { currentDialectLog.Debug("MySQL: Ignoring Error 3822 (Check constraint already exists)."); return true }

		// Untuk UNIQUE/PK already exists, MySQL bisa menggunakan kode 1061 (jika didukung indeks) atau 1062 (jika data duplikat),
		// atau pesan string umum. Error 1062 (Duplicate entry) TIDAK BOLEH diabaikan karena itu masalah data, bukan DDL.
		// Kita akan fokus pada pesan string untuk constraint already exists jika kode tidak spesifik.
		if (strings.Contains(errStrLower, "unique constraint") || strings.Contains(errStrLower, "primary key constraint")) &&
		    strings.Contains(errStrLower, "already exists") && isAddConstraint {
			currentDialectLog.Debug("MySQL: Ignoring 'constraint already exists' message for ADD CONSTRAINT.")
			return true
		}


		if numericErrorCode == "1051" && isDropTable { currentDialectLog.Debug("MySQL: Ignoring Error 1051 (Unknown table) for DROP TABLE."); return true }
		if numericErrorCode == "1091" && (isDropIndex || isDropConstraint) {
			currentDialectLog.Debug("MySQL: Ignoring Error 1091 (Can't DROP; check that it exists) for DROP INDEX/CONSTRAINT.")
			return true
		}

		// Fallback ke pola pesan jika kode numerik tidak cocok atau tidak ada
		objectNamePattern := `(?:` + "`[^`]*`" + `|'[^']*'|[\w$.-]+)`
		tableNamePattern  := `(?:` + "`[^`]*`" + `|'[^']*'|[\w$.]+)`
		ignorableMySQLPatterns := []struct{Pattern string; ForCreate bool; ForDrop bool}{
			// Create/Add
			{Pattern: `duplicate key name\s+'?` + objectNamePattern + `'?`, ForCreate: true}, // Untuk indeks
			{Pattern: `table\s+'?` + tableNamePattern + `'?\s+already exists`, ForCreate: true},
			{Pattern: `duplicate column name\s+'?` + objectNamePattern + `'?`, ForCreate: true},
			{Pattern: `(?:foreign key\s+)?constraint(?:\s+'?`+objectNamePattern+`'?)?\s+already exists`, ForCreate: true},
			{Pattern: `check constraint\s+'?`+objectNamePattern+`'?\s+already exists`, ForCreate: true},
			{Pattern: `index\s+'?`+objectNamePattern+`'?\s+already exists on table\s+'?` + tableNamePattern + `'?`, ForCreate: true},
			// Drop
			{Pattern: `unknown table\s+'?` + tableNamePattern + `'?`, ForDrop: true},
			{Pattern: `can't drop (?:index|foreign key|constraint|check|primary key)?\s*'?"?` + objectNamePattern + `"?'.*check that it exists`, ForDrop: true}, // Lebih fleksibel dengan quote
			{Pattern: `constraint\s+'?`+objectNamePattern+`'?\s+does not exist`, ForDrop: true},
		}
		for _, p := range ignorableMySQLPatterns {
			if matched, _ := regexp.MatchString(`(?i)`+p.Pattern, errStrLower); matched {
				if (p.ForCreate && (isCreateTable || isCreateIndex || isAddConstraint || isAddColumn)) ||
				   (p.ForDrop && (isDropTable || isDropIndex || isDropConstraint)) {
					currentDialectLog.Debug("MySQL error matched ignorable message pattern with DDL context.", zap.String("matched_pattern", p.Pattern))
					return true
				}
			}
		}


	case "sqlite":
		if (strings.Contains(errStrLower, "already exists") || strings.Contains(errStrLower, "duplicate column name")) &&
			(isCreateTable || isCreateIndex || isAddConstraint || isAddColumn) {
			currentDialectLog.Debug("SQLite: Ignoring 'already exists' or 'duplicate column' error for CREATE/ADD DDL.")
			return true
		}
		if strings.Contains(errStrLower, "no such table:") && isDropTable { currentDialectLog.Debug("SQLite: Ignoring 'no such table' for DROP TABLE."); return true }
		if strings.Contains(errStrLower, "no such index:") && isDropIndex { currentDialectLog.Debug("SQLite: Ignoring 'no such index' for DROP INDEX."); return true }
		// Hati-hati dengan "constraint ... failed", itu BUKAN "already exists" atau "does not exist"
	}

	currentDialectLog.Debug("DDL error will NOT be ignored.")
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

	// Lebih baik deteksi tipe DDL dulu, baru nama.
	if strings.Contains(upperDDL, "DROP PRIMARY KEY") {
		constraintName = "PRIMARY_KEY_IMPLICIT_DROP_NAME"
		constraintType = "PRIMARY KEY"
	} else if strings.Contains(upperDDL, "DROP FOREIGN KEY") {
		re := regexp.MustCompile(`(?i)DROP\s+FOREIGN\s+KEY\s+("?` + `[^"` + "`" + `\s;']+)`)
		if matches := re.FindStringSubmatch(upperDDL); len(matches) > 1 {
			constraintName = strings.Trim(matches[1], "\"`[]")
		}
		constraintType = "FOREIGN KEY"
	} else if strings.Contains(upperDDL, "DROP CHECK") {
		re := regexp.MustCompile(`(?i)DROP\s+CHECK\s+("?` + `[^"` + "`" + `\s;']+)`)
		if matches := re.FindStringSubmatch(upperDDL); len(matches) > 1 {
			constraintName = strings.Trim(matches[1], "\"`[]")
		}
		constraintType = "CHECK"
	} else if strings.Contains(upperDDL, "DROP CONSTRAINT") {
		re := regexp.MustCompile(`(?i)DROP\s+CONSTRAINT\s+("?` + `[^"` + "`" + `\s;']+)`)
		if matches := re.FindStringSubmatch(upperDDL); len(matches) > 1 {
			constraintName = strings.Trim(matches[1], "\"`[]")
		}
		// Coba tebak tipe dari nama untuk DROP CONSTRAINT (jika nama ada)
		if constraintName != "" {
			lowerName := strings.ToLower(constraintName)
			if strings.HasPrefix(lowerName, "fk_") || strings.Contains(lowerName, "_fk_") { constraintType = "FOREIGN KEY"
			} else if strings.HasPrefix(lowerName, "uq_") || strings.Contains(lowerName, "_uq_") { constraintType = "UNIQUE"
			} else if strings.HasPrefix(lowerName, "pk_") || strings.Contains(lowerName, "_pk_") { constraintType = "PRIMARY KEY"
			} else if strings.HasPrefix(lowerName, "chk_") || strings.Contains(lowerName, "_chk_") { constraintType = "CHECK"
			} else { constraintType = "UNKNOWN_FROM_NAME" }
		} else {
			constraintType = "UNKNOWN_DROP_CONSTRAINT_NO_NAME"
		}
	} else if strings.Contains(upperDDL, "DROP INDEX") {
		re := regexp.MustCompile(`(?i)DROP\s+INDEX\s+("?` + `[^"` + "`" + `\s;']+)`)
		if matches := re.FindStringSubmatch(upperDDL); len(matches) > 1 {
			constraintName = strings.Trim(matches[1], "\"`[]")
		}
		constraintType = "INDEX"
	} else {
		logger.Warn("Could not parse DDL for DROP operation to determine constraint/index type.", zap.String("ddl_for_sort", ddl))
		return ddl, "UNKNOWN_DDL_STRUCTURE"
	}

	// Jika nama tidak berhasil diekstrak tapi tipe diketahui (misal DROP PRIMARY KEY tanpa nama)
	if constraintName == "" && constraintType != "UNKNOWN_DDL_STRUCTURE" && constraintType != "" {
	    // Untuk PRIMARY_KEY_IMPLICIT_DROP_NAME sudah ditangani.
	    // Untuk tipe lain, jika nama tidak ada, gunakan placeholder.
	    if constraintType != "PRIMARY KEY" { // PK sudah punya nama placeholder
	        constraintName = fmt.Sprintf("%s_IMPLICIT_DROP_NAME", strings.ReplaceAll(strings.ToLower(constraintType), " ", "_"))
	    }
	} else if constraintName == "" { // Jika nama masih kosong dan tipe juga tidak diketahui
	    constraintName = ddl // Fallback: gunakan DDL sebagai nama
	}

	return constraintName, constraintType
}


func (s *SchemaSyncer) sortConstraintsForDrop(ddls []string) []string {
	priority := func(ddl string) int {
		_, consType := extractConstraintNameAndTypeForDrop(ddl, s.logger)
		switch consType {
		case "FOREIGN KEY": return 1
		case "UNIQUE": return 2
		case "CHECK": return 3
		case "PRIMARY KEY": return 4
		case "INDEX": return 5
		case "UNKNOWN_FROM_NAME": return 6
		case "UNKNOWN_DROP_CONSTRAINT_NO_NAME": return 6 // Sama dengan UNKNOWN_FROM_NAME
		default: // UNKNOWN_DDL_STRUCTURE
			return 7
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
		if strings.Contains(upperDDL, "MODIFY COLUMN") || (strings.Contains(upperDDL, "ALTER COLUMN") && !strings.Contains(upperDDL, "ADD COLUMN") && !strings.Contains(upperDDL, "DROP COLUMN")) { return 2 }
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
	sort.Strings(sorted) // Indeks biasanya bisa di-drop dalam urutan apa pun
	return sorted
}

func (s *SchemaSyncer) sortAddIndexes(ddls []string) []string {
	sorted := make([]string, len(ddls))
	copy(sorted, ddls)
	sort.Strings(sorted) // Indeks biasanya bisa di-add dalam urutan apa pun relatif satu sama lain
	return sorted
}

// Helper min untuk logging
func min(a, b int) int {
	if a < b { return a }
	return b
}
