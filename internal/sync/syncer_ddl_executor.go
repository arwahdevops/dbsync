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

// cleanSingleDDL membersihkan satu DDL statement dari komentar dan spasi berlebih,
// serta menghapus titik koma di akhir.
func cleanSingleDDL(ddl string) string {
	// Hapus komentar satu baris (-- style)
	reCommentLine := regexp.MustCompile(`--.*`)
	cleaned := reCommentLine.ReplaceAllString(ddl, "")
	// Hapus komentar blok (/* ... */ style)
	reCommentBlock := regexp.MustCompile(`/\*.*?\*/`)
	cleaned = reCommentBlock.ReplaceAllString(cleaned, "")
	// Trim spasi di awal/akhir dan hapus titik koma di akhir jika ada
	return strings.TrimRight(strings.TrimSpace(cleaned), ";")
}

// parseAndCategorizeDDLs memecah dan mengkategorikan DDLs dari SchemaExecutionResult.
// DDLs juga diurutkan dalam setiap kategori untuk memastikan urutan eksekusi yang logis.
func (s *SchemaSyncer) parseAndCategorizeDDLs(ddls *SchemaExecutionResult, table string) (*categorizedDDLs, error) {
	log := s.logger.With(zap.String("table", table), zap.String("phase", "parse-ddls"))
	parsed := &categorizedDDLs{
		AlterColumnDDLs: make([]string, 0), AddIndexDDLs: make([]string, 0),
		DropIndexDDLs: make([]string, 0), AddConstraintDDLs: make([]string, 0),
		DropConstraintDDLs: make([]string, 0),
	}

	if ddls == nil {
		log.Debug("Input SchemaExecutionResult is nil, no DDLs to parse.")
		return parsed, nil
	}

	// Proses TableDDL: bisa berisi CREATE TABLE atau serangkaian ALTER TABLE untuk kolom
	if ddls.TableDDL != "" {
		potentialStatements := strings.Split(ddls.TableDDL, ";")
		isCreateTableProcessed := false

		for _, stmt := range potentialStatements {
			cleanedStmt := cleanSingleDDL(stmt)
			if cleanedStmt == "" {
				continue
			}
			upperCleanedStmt := strings.ToUpper(cleanedStmt)

			if !isCreateTableProcessed && strings.HasPrefix(upperCleanedStmt, "CREATE TABLE") {
				if parsed.CreateTableDDL != "" {
					// Ini seharusnya tidak terjadi jika DDL generator bekerja dengan benar
					log.Error("Multiple CREATE TABLE statements found in TableDDL input, this is unexpected.",
						zap.String("existing_create_ddl", parsed.CreateTableDDL),
						zap.String("new_create_ddl", cleanedStmt))
					return nil, fmt.Errorf("multiple CREATE TABLE statements found for table '%s'", table)
				}
				parsed.CreateTableDDL = cleanedStmt
				isCreateTableProcessed = true
				log.Debug("Categorized CREATE TABLE DDL.", zap.String("ddl_preview", truncateForLog(parsed.CreateTableDDL, 70)))
			} else if strings.HasPrefix(upperCleanedStmt, "ALTER TABLE") {
				parsed.AlterColumnDDLs = append(parsed.AlterColumnDDLs, cleanedStmt)
				log.Debug("Categorized ALTER TABLE (column modification) DDL from TableDDL input.", zap.String("ddl_preview", truncateForLog(cleanedStmt, 70)))
			} else if cleanedStmt != "" { // Jika ada statement lain yang tidak dikenal
				log.Warn("Unrecognized DDL statement in TableDDL input, categorizing as general column alteration. Review DDL.",
					zap.String("statement_preview", truncateForLog(cleanedStmt, 70)))
				parsed.AlterColumnDDLs = append(parsed.AlterColumnDDLs, cleanedStmt)
			}
		}
	}

	// Proses IndexDDLs
	for _, ddl := range ddls.IndexDDLs {
		cleanedDDL := cleanSingleDDL(ddl)
		if cleanedDDL == "" {
			continue
		}
		upperCleanedDDL := strings.ToUpper(cleanedDDL)
		if strings.HasPrefix(upperCleanedDDL, "DROP INDEX") {
			parsed.DropIndexDDLs = append(parsed.DropIndexDDLs, cleanedDDL)
		} else if strings.HasPrefix(upperCleanedDDL, "CREATE INDEX") || strings.HasPrefix(upperCleanedDDL, "CREATE UNIQUE INDEX") {
			parsed.AddIndexDDLs = append(parsed.AddIndexDDLs, cleanedDDL)
		} else {
			log.Warn("Unknown DDL type in IndexDDLs, skipping categorization for this DDL.", zap.String("ddl_preview", truncateForLog(cleanedDDL, 70)))
		}
	}

	// Proses ConstraintDDLs
	for _, ddl := range ddls.ConstraintDDLs {
		cleanedDDL := cleanSingleDDL(ddl)
		if cleanedDDL == "" {
			continue
		}
		upperCleanedDDL := strings.ToUpper(cleanedDDL)
		isDrop := false
		// Cek keyword DROP constraint yang umum
		if strings.Contains(upperCleanedDDL, "DROP CONSTRAINT") ||
			strings.Contains(upperCleanedDDL, "DROP FOREIGN KEY") || // MySQL specific
			strings.Contains(upperCleanedDDL, "DROP CHECK") || // MySQL specific
			strings.Contains(upperCleanedDDL, "DROP PRIMARY KEY") { // MySQL specific
			isDrop = true
		} else if strings.HasPrefix(upperCleanedDDL, "DROP CONSTRAINT") { // Fallback jika hanya "DROP CONSTRAINT nama"
			isDrop = true
		}

		if isDrop {
			parsed.DropConstraintDDLs = append(parsed.DropConstraintDDLs, cleanedDDL)
		} else if strings.HasPrefix(upperCleanedDDL, "ALTER TABLE") && strings.Contains(upperCleanedDDL, "ADD CONSTRAINT") {
			parsed.AddConstraintDDLs = append(parsed.AddConstraintDDLs, cleanedDDL)
		} else if strings.HasPrefix(upperCleanedDDL, "ADD CONSTRAINT") { // Jarang terjadi tanpa ALTER TABLE, tapi untuk kelengkapan
			log.Debug("Found 'ADD CONSTRAINT' DDL without 'ALTER TABLE' prefix. Categorizing as ADD.", zap.String("ddl_preview", truncateForLog(cleanedDDL, 70)))
			parsed.AddConstraintDDLs = append(parsed.AddConstraintDDLs, cleanedDDL)
		} else {
			log.Warn("Unknown DDL type in ConstraintDDLs, skipping categorization for this DDL.", zap.String("ddl_preview", truncateForLog(cleanedDDL, 70)))
		}
	}

	// Panggil method sorting sebagai bagian dari SchemaSyncer
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

// executeDDLPhase mengeksekusi daftar DDL dalam transaksi yang diberikan.
// Parameter continueOnError menentukan apakah akan melanjutkan jika satu DDL gagal (dan mengumpulkan error)
// atau langsung mengembalikan error.
func (s *SchemaSyncer) executeDDLPhase(tx *gorm.DB, phaseName string, ddlList []string, continueOnError bool, log *zap.Logger) error {
	if len(ddlList) == 0 {
		log.Debug("No DDLs to execute for phase.", zap.String("phase_name", phaseName))
		return nil
	}
	log.Info("Executing DDL phase.", zap.String("phase_name", phaseName), zap.Int("ddl_count", len(ddlList)))

	var accumulatedErrors error
	for _, ddl := range ddlList {
		if ddl == "" { // Skip DDL kosong yang mungkin hasil dari pembersihan
			continue
		}
		// Memastikan DDL diakhiri dengan titik koma bisa membantu beberapa driver DB, meskipun GORM Exec sering tidak memerlukannya.
		finalDDL := ddl
		if !strings.HasSuffix(finalDDL, ";") {
			finalDDL += ";"
		}

		log.Debug("Executing DDL.", zap.String("phase_name", phaseName), zap.String("ddl", finalDDL))
		if err := tx.Exec(finalDDL).Error; err != nil {
			execErrLog := log.With(zap.String("phase_name", phaseName), zap.String("failed_ddl", finalDDL), zap.Error(err))
			if s.shouldIgnoreDDLError(err, finalDDL) {
				execErrLog.Warn("DDL execution resulted in an ignorable error, continuing.")
			} else {
				currentErr := fmt.Errorf("failed DDL in phase '%s': [%s], error: %w", phaseName, finalDDL, err)
				if continueOnError {
					execErrLog.Error("Failed to execute DDL, but configured to continue. Error will be accumulated.")
					accumulatedErrors = multierr.Append(accumulatedErrors, currentErr)
				} else {
					execErrLog.Error("Failed to execute DDL, aborting phase and transaction.")
					return currentErr // Return error, akan menyebabkan rollback transaksi
				}
			}
		}
	}

	if accumulatedErrors != nil {
		log.Warn("DDL phase completed with accumulated (ignorable or non-fatal) errors.",
			zap.String("phase_name", phaseName),
			zap.NamedError("accumulated_ddl_errors", accumulatedErrors))
		// Jika continueOnError true, kita mengembalikan error gabungan agar pemanggil tahu ada masalah,
		// meskipun fase ini diizinkan untuk melanjutkan dan tidak menghentikan transaksi.
		return accumulatedErrors
	}
	log.Debug("DDL phase executed successfully.", zap.String("phase_name", phaseName))
	return nil
}

// shouldIgnoreDDLError menentukan apakah error DDL tertentu dapat diabaikan.
// Misalnya, error "object already exists" saat CREATE, atau "object does not exist" saat DROP.
func (s *SchemaSyncer) shouldIgnoreDDLError(err error, ddl string) bool {
	if err == nil {
		return false
	}

	errStrOriginal := err.Error()
	errStrLower := strings.ToLower(errStrOriginal)
	// DDL yang bersih (tanpa titik koma akhir) digunakan untuk analisis prefix
	upperDDL := strings.ToUpper(strings.TrimRight(ddl, ";"))
	sqlState, numericErrorCode := "", ""

	// Ekstraksi SQLState (umumnya PostgreSQL)
	if matches := regexp.MustCompile(`\(sqlstate\s+([a-z0-9]{5})\)`).FindStringSubmatch(errStrLower); len(matches) > 1 {
		sqlState = strings.ToUpper(matches[1])
	}
	// Ekstraksi numeric error code (umumnya MySQL, tapi bisa juga ada di dialek lain)
	if matches := regexp.MustCompile(`(?i)(?:error|code)[:\s]*(\d+)`).FindStringSubmatch(errStrLower); len(matches) > 1 {
		numericErrorCode = matches[1]
	}

	// Tentukan apakah DDL adalah operasi DROP atau CREATE/ADD
	isDrop := strings.HasPrefix(upperDDL, "DROP") ||
		(strings.HasPrefix(upperDDL, "ALTER TABLE") &&
			(strings.Contains(upperDDL, "DROP CONSTRAINT") ||
				strings.Contains(upperDDL, "DROP FOREIGN KEY") ||
				strings.Contains(upperDDL, "DROP CHECK") ||
				strings.Contains(upperDDL, "DROP PRIMARY KEY") ||
				strings.Contains(upperDDL, "DROP COLUMN")))

	isCreateOrAdd := strings.HasPrefix(upperDDL, "CREATE") ||
		(strings.HasPrefix(upperDDL, "ALTER TABLE") &&
			(strings.Contains(upperDDL, "ADD CONSTRAINT") ||
				strings.Contains(upperDDL, "ADD COLUMN")))

	logCtx := s.logger.With(zap.String("dialect", s.dstDialect), zap.String("sqlstate", sqlState),
		zap.String("error_code", numericErrorCode), zap.String("ddl_preview", truncateForLog(ddl, 70)),
		zap.String("original_error_msg", errStrOriginal))

	switch s.dstDialect {
	case "postgres":
		if isCreateOrAdd {
			// 42P07: DUPLICATE_TABLE, DUPLICATE_SCHEMA, DUPLICATE_TYPE, etc.
			// 42710: DUPLICATE_OBJECT (constraints, indexes)
			if sqlState == "42P07" || sqlState == "42710" {
				logCtx.Debug("PostgreSQL: Ignoring 'duplicate object' error (CREATE/ADD).", zap.String("sqlstate_matched", sqlState))
				return true
			}
		}
		if isDrop {
			// 42704: UNDEFINED_OBJECT (constraints, indexes)
			// 42P01: UNDEFINED_TABLE, UNDEFINED_SCHEMA, UNDEFINED_TYPE
			if sqlState == "42704" || sqlState == "42P01" {
				// PENTING: Jangan abaikan jika kita mencoba ADD FOREIGN KEY dan tabel referensi tidak ada (42P01).
				if sqlState == "42P01" && strings.Contains(upperDDL, "ADD CONSTRAINT") && strings.Contains(upperDDL, "FOREIGN KEY") {
					logCtx.Debug("PostgreSQL: SQLState 42P01 (undefined_table) for ADD FOREIGN KEY operation will NOT be ignored.")
					return false
				}
				logCtx.Debug("PostgreSQL: Ignoring 'object does not exist' error (DROP).", zap.String("sqlstate_matched", sqlState))
				return true
			}
		}
	case "mysql":
		if isCreateOrAdd {
			// Kode error MySQL untuk "sudah ada"
			// ER_TABLE_EXISTS_ERROR, ER_DUP_KEYNAME, ER_DUP_FIELDNAME, ER_FK_DUP_NAME, ER_CHECK_CONSTRAINT_DUP_NAME, ER_DUP_KEY
			if numericErrorCode == "1050" || numericErrorCode == "1061" || numericErrorCode == "1060" ||
				numericErrorCode == "1826" || numericErrorCode == "3822" || numericErrorCode == "1022" {
				logCtx.Debug("MySQL: Ignoring 'already exists'/'duplicate' error by specific code (CREATE/ADD).", zap.String("error_code_matched", numericErrorCode))
				return true
			}
			// Fallback regex untuk pesan error duplikasi
			if regexp.MustCompile(`(?i)(already exists|duplicate (key|column|constraint|entry)|constraint.*already exists)`).MatchString(errStrLower) {
				logCtx.Debug("MySQL: Ignoring 'already exists'/'duplicate' error by message pattern (CREATE/ADD).")
				return true
			}
		}
		if isDrop {
			// Kode error MySQL untuk "tidak ada"
			// ER_BAD_TABLE_ERROR, ER_BAD_FIELD_ERROR (juga untuk key/constraint pada DROP)
			if numericErrorCode == "1051" || numericErrorCode == "1091" {
				logCtx.Debug("MySQL: Ignoring 'does not exist' or 'can't drop' error by specific code (DROP).", zap.String("error_code_matched", numericErrorCode))
				return true
			}
			// Fallback regex untuk pesan error "tidak ada"
			if regexp.MustCompile(`(?i)(unknown table|can't drop.*?check that.*?exists|table.*doesn't exist|unknown column|unknown key|unknown constraint|constraint.*?does not exist)`).MatchString(errStrLower) {
				logCtx.Debug("MySQL: Ignoring 'does not exist' or 'can't drop' error by message pattern (DROP).")
				return true
			}
		}
	case "sqlite":
		if isCreateOrAdd {
			if strings.Contains(errStrLower, "already exists") || strings.Contains(errStrLower, "duplicate column name") {
				logCtx.Debug("SQLite: Ignoring 'already exists'/'duplicate column' error (CREATE/ADD).")
				return true
			}
			if strings.Contains(upperDDL, "ADD CONSTRAINT") && strings.Contains(upperDDL, "UNIQUE") &&
				strings.Contains(errStrLower, "constraint") && strings.Contains(errStrLower, "already exists") {
				logCtx.Debug("SQLite: Ignoring 'constraint already exists' for ADD CONSTRAINT UNIQUE.")
				return true
			}
			// JANGAN abaikan "UNIQUE constraint failed: table.column" karena ini error data, bukan skema duplikat.
			if strings.Contains(errStrLower, "unique constraint failed") && !strings.Contains(errStrLower, "already exists") {
				logCtx.Debug("SQLite: 'UNIQUE constraint failed' (data violation during DDL execution) will NOT be ignored.")
				return false
			}
		}
		if isDrop {
			if strings.Contains(errStrLower, "no such table") ||
				strings.Contains(errStrLower, "no such index") ||
				(strings.Contains(errStrLower, "no such column") && strings.Contains(upperDDL, "DROP COLUMN")) ||
				(strings.Contains(errStrLower, "constraint") && (strings.Contains(errStrLower, "not found") || strings.Contains(errStrLower, "no such constraint"))) {
				logCtx.Debug("SQLite: Ignoring 'no such ...' or 'constraint not found' error (DROP).")
				return true
			}
		}
	}
	logCtx.Debug("DDL error will NOT be ignored based on current rules.")
	return false
}

// extractConstraintNameAndTypeForDrop mengekstrak nama dan tipe constraint dari DDL DROP.
// Ini digunakan untuk membantu pengurutan DDL DROP.
func (s *SchemaSyncer) extractConstraintNameAndTypeForDrop(ddl string, logger *zap.Logger, dstDialect string) (string, string) {
	upperDDL := strings.ToUpper(ddl)
	var constraintName, constraintType string

	// Regex untuk menangkap nama objek (bisa di-quote atau tidak, bisa mengandung skema, bisa ada IF EXISTS)
	// Memperbolehkan tanda hubung, titik (untuk nama skema.objek), dan underscore dalam nama.
	// \p{L} untuk huruf unicode, \p{N} untuk angka unicode.
	nameRegex := `(?:IF\s+EXISTS\s+)?("?(?:[\w\p{L}\p{N}.-]|[\._-])+"?(?:\."?(?:[\w\p{L}\p{N}.-]|[\._-])+"?)?|[\w\p{L}\p{N}.-]+|\[(?:[\w\p{L}\p{N}.-]|[\._-])+\])`

	if strings.Contains(upperDDL, "DROP PRIMARY KEY") {
		constraintName = "PRIMARY_KEY_IMPLICIT_DROP_NAME_FOR_SORT"
		constraintType = "PRIMARY KEY"
	} else if matches := regexp.MustCompile(`(?i)DROP\s+FOREIGN\s+KEY\s+` + nameRegex).FindStringSubmatch(upperDDL); len(matches) > 1 {
		constraintName = strings.Trim(matches[len(matches)-1], "\"`[]") // Ambil grup terakhir yang cocok (nama sebenarnya)
		constraintType = "FOREIGN KEY"
	} else if matches := regexp.MustCompile(`(?i)DROP\s+CHECK\s+` + nameRegex).FindStringSubmatch(upperDDL); len(matches) > 1 {
		constraintName = strings.Trim(matches[len(matches)-1], "\"`[]")
		constraintType = "CHECK"
	} else if matches := regexp.MustCompile(`(?i)DROP\s+CONSTRAINT\s+` + nameRegex).FindStringSubmatch(upperDDL); len(matches) > 1 {
		constraintName = strings.Trim(matches[len(matches)-1], "\"`[]")
		if constraintName != "" {
			lowerName := strings.ToLower(constraintName)
			if strings.Contains(lowerName, "fk_") || strings.Contains(lowerName, "_fk") || strings.HasSuffix(lowerName, "fkey") {
				constraintType = "FOREIGN KEY"
			} else if strings.Contains(lowerName, "uq_") || strings.Contains(lowerName, "_uq") || (strings.HasSuffix(lowerName, "key") && !strings.Contains(lowerName, "primary") && !strings.Contains(lowerName, "foreign")) {
				constraintType = "UNIQUE"
			} else if strings.Contains(lowerName, "pk_") || strings.Contains(lowerName, "_pk") || strings.HasSuffix(lowerName, "pkey") {
				constraintType = "PRIMARY KEY"
			} else if strings.Contains(lowerName, "chk_") || strings.Contains(lowerName, "_chk") || strings.HasPrefix(lowerName, "check_") || strings.HasPrefix(lowerName, "ck_") {
				constraintType = "CHECK"
			} else {
				constraintType = "UNKNOWN_CONSTRAINT_TYPE_FROM_NAME"
			}
		} else {
			constraintType = "UNKNOWN_DROP_CONSTRAINT_NO_NAME_PARSED"
		}
	} else if matches := regexp.MustCompile(`(?i)DROP\s+INDEX\s+` + nameRegex).FindStringSubmatch(upperDDL); len(matches) > 1 {
		constraintName = strings.Trim(matches[len(matches)-1], "\"`[]")
		constraintType = "INDEX"
	} else {
		logger.Warn("Could not parse DDL for DROP operation to determine its type for sorting.", zap.String("ddl_for_sort_preview", truncateForLog(ddl, 70)))
		return ddl, "UNKNOWN_DDL_STRUCTURE_FOR_DROP_SORT"
	}

	if constraintName == "" && constraintType != "UNKNOWN_DDL_STRUCTURE_FOR_DROP_SORT" && constraintType != "" {
		if constraintType != "PRIMARY KEY" {
			constraintName = fmt.Sprintf("%s_IMPLICIT_DROP_NAME_FOR_SORT_FROM_DDL", strings.ReplaceAll(strings.ToLower(constraintType), " ", "_"))
		}
	} else if constraintName == "" {
		constraintName = fmt.Sprintf("UNNAMED_DROP_AT_%s", truncateForLog(ddl, 10))
	}
	return constraintName, constraintType
}

// sortConstraintsForDrop mengurutkan DDL DROP constraint berdasarkan prioritas tipe, lalu nama.
func (s *SchemaSyncer) sortConstraintsForDrop(ddls []string) []string {
	priority := func(ddl string) int {
		_, consType := s.extractConstraintNameAndTypeForDrop(ddl, s.logger, s.dstDialect)
		switch consType {
		case "FOREIGN KEY":
			return 1
		case "UNIQUE":
			return 2
		case "CHECK":
			return 3
		case "PRIMARY KEY":
			return 4
		case "INDEX":
			return 5
		case "UNKNOWN_CONSTRAINT_TYPE_FROM_NAME", "UNKNOWN_DROP_CONSTRAINT_NO_NAME_PARSED":
			return 6
		default:
			return 7 // UNKNOWN_DDL_STRUCTURE_FOR_DROP_SORT
		}
	}
	sorted := make([]string, len(ddls))
	copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		priI, priJ := priority(sorted[i]), priority(sorted[j])
		if priI != priJ {
			return priI < priJ
		}
		nameI, _ := s.extractConstraintNameAndTypeForDrop(sorted[i], s.logger, s.dstDialect)
		nameJ, _ := s.extractConstraintNameAndTypeForDrop(sorted[j], s.logger, s.dstDialect)
		return nameI < nameJ
	})
	return sorted
}

// sortConstraintsForAdd mengurutkan DDL ADD constraint berdasarkan prioritas tipe, lalu DDL string.
func (s *SchemaSyncer) sortConstraintsForAdd(ddls []string) []string {
	priority := func(ddl string) int {
		upperDDL := strings.ToUpper(ddl)
		if strings.Contains(upperDDL, "PRIMARY KEY") {
			return 1
		}
		if strings.Contains(upperDDL, "UNIQUE") {
			return 2
		}
		if strings.Contains(upperDDL, "CHECK") {
			return 3
		}
		if strings.Contains(upperDDL, "FOREIGN KEY") {
			return 4
		}
		s.logger.Debug("ADD CONSTRAINT DDL with unknown priority category for sorting.", zap.String("ddl_preview", truncateForLog(ddl, 70)))
		return 5
	}
	sorted := make([]string, len(ddls))
	copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		priI, priJ := priority(sorted[i]), priority(sorted[j])
		if priI != priJ {
			return priI < priJ
		}
		return sorted[i] < sorted[j]
	})
	return sorted
}

// sortAlterColumns mengurutkan DDL ALTER COLUMN berdasarkan jenis operasi, lalu DDL string.
func (s *SchemaSyncer) sortAlterColumns(ddls []string) []string {
	priority := func(ddl string) int {
		upperDDL := strings.ToUpper(ddl)
		if strings.Contains(upperDDL, "DROP COLUMN") {
			return 1
		}
		if strings.Contains(upperDDL, "MODIFY COLUMN") || // MySQL
			(strings.Contains(upperDDL, "ALTER COLUMN") && // PostgreSQL
				!strings.Contains(upperDDL, "ADD COLUMN") &&
				!strings.Contains(upperDDL, "DROP COLUMN")) {
			return 2
		}
		if strings.Contains(upperDDL, "ADD COLUMN") {
			return 3
		}
		if ddl != "" && !strings.Contains(upperDDL, "UNKNOWN DDL STATEMENT") {
			s.logger.Debug("ALTER DDL with unknown priority category for sorting.", zap.String("ddl_preview", truncateForLog(ddl, 70)))
		}
		return 4
	}
	sorted := make([]string, len(ddls))
	copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		priI, priJ := priority(sorted[i]), priority(sorted[j])
		if priI != priJ {
			return priI < priJ
		}
		return sorted[i] < sorted[j]
	})
	return sorted
}

// sortDropIndexes mengurutkan DDL DROP INDEX secara leksikografis.
func (s *SchemaSyncer) sortDropIndexes(ddls []string) []string {
	sorted := make([]string, len(ddls))
	copy(sorted, ddls)
	sort.Strings(sorted)
	return sorted
}

// sortAddIndexes mengurutkan DDL CREATE INDEX secara leksikografis.
func (s *SchemaSyncer) sortAddIndexes(ddls []string) []string {
	sorted := make([]string, len(ddls))
	copy(sorted, ddls)
	sort.Strings(sorted)
	return sorted
}

// splitPostgresFKsForDeferredExecution memisahkan DDL FK PostgreSQL yang didefinisikan
// sebagai DEFERRABLE INITIALLY DEFERRED.
func (s *SchemaSyncer) splitPostgresFKsForDeferredExecution(allConstraints []string) (deferredFKs []string, nonDeferredFKs []string) {
	for _, ddl := range allConstraints {
		upperDDL := strings.ToUpper(ddl)
		if strings.HasPrefix(upperDDL, "ALTER TABLE") &&
			strings.Contains(upperDDL, "ADD CONSTRAINT") &&
			strings.Contains(upperDDL, "FOREIGN KEY") &&
			strings.Contains(upperDDL, "DEFERRABLE") &&
			strings.Contains(upperDDL, "INITIALLY DEFERRED") {
			deferredFKs = append(deferredFKs, ddl)
		} else {
			nonDeferredFKs = append(nonDeferredFKs, ddl)
		}
	}
	return
}
