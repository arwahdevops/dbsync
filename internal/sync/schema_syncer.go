// internal/sync/schema_syncer.go
package sync

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strconv" // Dipertahankan karena digunakan oleh applyTypeModifiers
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/arwahdevops/dbsync/internal/config"
	"github.com/arwahdevops/dbsync/internal/utils"
)

// --- Structs (Definisi ColumnInfo, IndexInfo, ConstraintInfo) ---
type ColumnInfo struct {
	Name                 string
	Type                 string         // Tipe asli dari database sumber
	MappedType           string         // Tipe yang sudah dimapping ke dialek tujuan
	IsNullable           bool
	IsPrimary            bool
	IsGenerated          bool
	DefaultValue         sql.NullString
	AutoIncrement        bool
	OrdinalPosition      int
	Length               sql.NullInt64
	Precision            sql.NullInt64
	Scale                sql.NullInt64
	Collation            sql.NullString
	Comment              sql.NullString
	// GenerationExpression sql.NullString // Pertimbangkan untuk menambahkan ini jika detail ekspresi diperlukan untuk perbandingan atau DDL
}

type IndexInfo struct {
	Name      string
	Columns   []string
	IsUnique  bool
	IsPrimary bool
	RawDef    string // Definisi mentah jika ada (berguna untuk PostgreSQL)
	// IndexType string // Pertimbangkan menambahkan jika bisa diekstrak secara konsisten
}

type ConstraintInfo struct {
	Name           string
	Type           string
	Columns        []string
	Definition     string
	ForeignTable   string
	ForeignColumns []string
	OnDelete       string
	OnUpdate       string
}

// SchemaSyncer menangani logika sinkronisasi skema.
type SchemaSyncer struct {
	srcDB      *gorm.DB
	dstDB      *gorm.DB
	srcDialect string
	dstDialect string
	logger     *zap.Logger
	// typeMapper *config.TypeMapper // Pertimbangkan untuk menyuntikkan instance TypeMapper jika logikanya sangat kompleks dan terpisah
}

// Ensure SchemaSyncer implements the interface
var _ SchemaSyncerInterface = (*SchemaSyncer)(nil)

// --- Constructor ---
func NewSchemaSyncer(srcDB, dstDB *gorm.DB, srcDialect, dstDialect string, logger *zap.Logger) *SchemaSyncer {
	return &SchemaSyncer{
		srcDB:      srcDB,
		dstDB:      dstDB,
		srcDialect: strings.ToLower(srcDialect),
		dstDialect: strings.ToLower(dstDialect),
		logger:     logger.Named("schema-syncer"),
	}
}

// --- Interface Implementation (Dispatcher Methods) ---

// SyncTableSchema menganalisis skema sumber/tujuan dan menghasilkan DDLs berdasarkan strategi.
// Ini TIDAK mengeksekusi DDLs itu sendiri.
func (s *SchemaSyncer) SyncTableSchema(ctx context.Context, table string, strategy config.SchemaSyncStrategy) (*SchemaExecutionResult, error) {
	start := time.Now()
	log := s.logger.With(zap.String("table", table), zap.String("strategy", string(strategy)))
	log.Info("Starting schema analysis for table")

	result := &SchemaExecutionResult{
		PrimaryKeys: make([]string, 0), // Inisialisasi
	}

	if s.isSystemTable(table, s.srcDialect) {
		log.Debug("Skipping system table for schema sync.")
		return result, nil // Tidak ada DDL, PK kosong
	}

	// 1. Get Source Schema
	srcSchema, srcExists, err := s.fetchSchemaDetails(ctx, s.srcDB, s.srcDialect, table, "source")
	if err != nil {
		return nil, fmt.Errorf("failed to get source schema for table '%s': %w", table, err)
	}
	if !srcExists { // Jika tabel sumber tidak ada
		log.Warn("Source table does not exist. Skipping schema sync for this table.")
		return result, nil
	}
	if len(srcSchema.Columns) == 0 { // Tabel ada tapi tidak ada kolom (kasus aneh, tapi mungkin)
		log.Warn("Source table exists but appears to have no columns. Skipping schema sync for this table.")
		return result, nil
	}
	result.PrimaryKeys = getPKColumnNames(srcSchema.Columns) // Dapatkan PK dari kolom sumber

	// Memastikan MappedType untuk kolom sumber diisi.
	// Ini penting sebelum DDL generation.
	if err := s.populateMappedTypesForSourceColumns(srcSchema.Columns, table); err != nil {
		return nil, err // Error sudah termasuk konteks tabel dan kolom
	}

	// 2. Generate DDL based on strategy
	switch strategy {
	case config.SchemaSyncDropCreate:
		log.Warn("Using 'drop_create' strategy - THIS IS DESTRUCTIVE!")
		ddls, errGen := s.generateDDLsForDropCreate(table, srcSchema.Columns, srcSchema.Indexes, srcSchema.Constraints)
		if errGen != nil {
			return nil, errGen // Error sudah dibungkus
		}
		result.TableDDL = ddls.TableDDL
		result.IndexDDLs = ddls.IndexDDLs
		result.ConstraintDDLs = ddls.ConstraintDDLs

	case config.SchemaSyncAlter:
		log.Info("Attempting 'alter' schema synchronization strategy")
		dstSchema, dstExists, errGetDst := s.fetchSchemaDetails(ctx, s.dstDB, s.dstDialect, table, "destination")
		if errGetDst != nil {
			return nil, fmt.Errorf("failed to get destination schema for table '%s': %w", table, errGetDst)
		}

		if !dstExists {
			log.Info("Destination table not found, will perform CREATE TABLE operation instead of ALTER.")
			ddls, errGen := s.generateDDLsForDropCreate(table, srcSchema.Columns, srcSchema.Indexes, srcSchema.Constraints)
			if errGen != nil {
				return nil, errGen
			}
			result.TableDDL = ddls.TableDDL
			result.IndexDDLs = ddls.IndexDDLs
			result.ConstraintDDLs = ddls.ConstraintDDLs
		} else {
			log.Debug("Destination table exists, generating ALTER DDLs if necessary.")
			// Memastikan MappedType untuk kolom tujuan diisi (dengan Type-nya sendiri) untuk perbandingan.
			s.populateMappedTypesForDestinationColumns(dstSchema.Columns)

			joinedAlterColumnDDLs, indexDDLs, constraintDDLs, errGen := s.generateAlterDDLs(
				table,
				srcSchema.Columns, srcSchema.Indexes, srcSchema.Constraints,
				dstSchema.Columns, dstSchema.Indexes, dstSchema.Constraints,
			)
			if errGen != nil {
				return nil, fmt.Errorf("failed to generate ALTER DDLs for '%s': %w", table, errGen)
			}
			result.TableDDL = joinedAlterColumnDDLs
			result.IndexDDLs = indexDDLs
			result.ConstraintDDLs = constraintDDLs
		}

	case config.SchemaSyncNone:
		log.Info("Schema sync strategy is 'none'. No DDLs will be generated or executed for table structure.")
		// result.PrimaryKeys sudah di-set dari srcSchema.Columns di atas.

	default:
		return nil, fmt.Errorf("unknown schema sync strategy: %s for table '%s'", strategy, table)
	}

	log.Info("Schema analysis and DDL generation completed for table", zap.Duration("duration", time.Since(start)))
	return result, nil
}

// populateMappedTypesForSourceColumns mengisi field MappedType untuk kolom sumber.
func (s *SchemaSyncer) populateMappedTypesForSourceColumns(columns []ColumnInfo, tableName string) error {
	log := s.logger.With(zap.String("table", tableName), zap.String("phase", "populate-source-mapped-types"))
	for i := range columns {
		if columns[i].IsGenerated {
			columns[i].MappedType = columns[i].Type // Gunakan tipe asli, jangan mapping
			log.Debug("Skipping type mapping for generated source column", zap.String("column", columns[i].Name), zap.String("original_type", columns[i].Type))
			continue
		}
		mapped, mapErr := s.mapDataType(columns[i].Type) // mapDataType sudah menyertakan logging internal
		if mapErr != nil {
			log.Error("Fatal type mapping error for source column",
				zap.String("column", columns[i].Name), zap.String("original_type", columns[i].Type), zap.Error(mapErr))
			return fmt.Errorf("fatal type mapping error for column '%s' in table '%s': %w", columns[i].Name, tableName, mapErr)
		}
		columns[i].MappedType = mapped
	}
	return nil
}

// populateMappedTypesForDestinationColumns mengisi MappedType untuk kolom tujuan (biasanya sama dengan Type-nya).
func (s *SchemaSyncer) populateMappedTypesForDestinationColumns(columns []ColumnInfo) {
	for i := range columns {
		if columns[i].MappedType == "" { // Hanya jika belum diisi (seharusnya selalu kosong dari fetcher)
			columns[i].MappedType = columns[i].Type
		}
	}
}

// generateDDLsForDropCreate adalah helper untuk strategi drop_create atau ketika alter mendeteksi tabel tujuan tidak ada.
func (s *SchemaSyncer) generateDDLsForDropCreate(table string, srcColumns []ColumnInfo, srcIndexes []IndexInfo, srcConstraints []ConstraintInfo) (*SchemaExecutionResult, error) {
	result := &SchemaExecutionResult{}
	tableDDL, _, errGen := s.generateCreateTableDDL(table, srcColumns)
	if errGen != nil {
		return nil, fmt.Errorf("failed to generate CREATE TABLE DDL for '%s': %w", table, errGen)
	}
	result.TableDDL = tableDDL
	result.IndexDDLs = s.generateCreateIndexDDLs(table, srcIndexes)       // generateCreateIndexDDLs dari schema_ddl_create.go
	result.ConstraintDDLs = s.generateAddConstraintDDLs(table, srcConstraints) // generateAddConstraintDDLs dari schema_ddl_create.go
	return result, nil
}

// schemaDetails adalah struct internal untuk mengelompokkan hasil fetch skema.
type schemaDetails struct {
	Columns     []ColumnInfo
	Indexes     []IndexInfo
	Constraints []ConstraintInfo
}

// fetchSchemaDetails mengambil semua detail skema (kolom, indeks, constraint) untuk sebuah tabel.
// Parameter `dbType` bisa "source" atau "destination" untuk logging.
func (s *SchemaSyncer) fetchSchemaDetails(ctx context.Context, db *gorm.DB, dialect string, table string, dbType string) (*schemaDetails, bool, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", dialect), zap.String("database_role", dbType))
	details := &schemaDetails{}
	var exists bool

	if db.Migrator().HasTable(table) {
		exists = true
	} else {
		log.Debug("Table not found based on GORM Migrator, schema details will be empty.")
		return details, false, nil
	}

	var err error
	details.Columns, err = s.getColumns(ctx, db, dialect, table)
	if err != nil {
		return nil, exists, fmt.Errorf("failed to get columns for %s table '%s': %w", dbType, table, err)
	}
	if exists && len(details.Columns) == 0 {
		log.Warn("Table reported as existing by GORM Migrator, but no columns were fetched. This might indicate an issue or an empty table definition.",
			zap.String("table", table), zap.String("dialect", dialect), zap.String("database_role", dbType))
	}

	details.Indexes, err = s.getIndexes(ctx, db, dialect, table)
	if err != nil {
		log.Error("Failed to get indexes, proceeding with empty list.", zap.Error(err))
		details.Indexes = []IndexInfo{}
	}

	details.Constraints, err = s.getConstraints(ctx, db, dialect, table)
	if err != nil {
		log.Error("Failed to get constraints, proceeding with empty list.", zap.Error(err))
		details.Constraints = []ConstraintInfo{}
	}

	return details, exists, nil
}

// ExecuteDDLs menerapkan DDLs yang dihasilkan ke database tujuan.
func (s *SchemaSyncer) ExecuteDDLs(ctx context.Context, table string, ddls *SchemaExecutionResult) error {
	log := s.logger.With(zap.String("table", table), zap.String("database", "destination"))

	if ddls == nil || (ddls.TableDDL == "" && len(ddls.IndexDDLs) == 0 && len(ddls.ConstraintDDLs) == 0) {
		log.Debug("No DDLs to execute for table.")
		return nil
	}
	log.Info("Starting DDL execution for table.")

	parsedDDLs, errParse := s.parseAndCategorizeDDLs(ddls, table)
	if errParse != nil {
		return fmt.Errorf("error parsing DDLs for table '%s': %w", table, errParse)
	}

	return s.dstDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var revertFKChecks func() error
		isDropCreateStrategy := parsedDDLs.CreateTableDDL != ""

		if isDropCreateStrategy && s.dstDialect == "mysql" {
			log.Debug("Disabling FOREIGN_KEY_CHECKS for MySQL DROP/CREATE.")
			if err := tx.Exec("SET FOREIGN_KEY_CHECKS=0;").Error; err != nil {
				return fmt.Errorf("failed to disable foreign key checks on destination for table '%s': %w", table, err)
			}
			revertFKChecks = func() error {
				log.Debug("Re-enabling FOREIGN_KEY_CHECKS for MySQL for table.", zap.String("table", table))
				return tx.Exec("SET FOREIGN_KEY_CHECKS=1;").Error
			}
			defer func() {
				if revertFKChecks != nil {
					if rErr := revertFKChecks(); rErr != nil {
						log.Error("Failed to re-enable foreign key checks on destination for table", zap.String("table", table), zap.Error(rErr))
					}
				}
			}()
		}

		if !isDropCreateStrategy {
			if err := s.executeDDLPhase(tx, "Phase 1 (ALTER): Dropping existing constraints", parsedDDLs.DropConstraintDDLs, true, log); err != nil { return err }
			if err := s.executeDDLPhase(tx, "Phase 1 (ALTER): Dropping existing indexes", parsedDDLs.DropIndexDDLs, true, log); err != nil { return err }
		}

		if isDropCreateStrategy {
			log.Warn("Executing DROP TABLE IF EXISTS prior to CREATE (destructive operation).", zap.String("table", table))
			dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", utils.QuoteIdentifier(table, s.dstDialect))
			if s.dstDialect == "postgres" { dropSQL += " CASCADE" }
			if err := tx.Exec(dropSQL).Error; err != nil {
				return fmt.Errorf("failed to execute DROP TABLE IF EXISTS for '%s': %w", table, err)
			}
			if err := s.executeDDLPhase(tx, "Phase 2 (DROP/CREATE): Creating table", []string{parsedDDLs.CreateTableDDL}, false, log); err != nil { return err }
		} else if len(parsedDDLs.AlterColumnDDLs) > 0 {
			if err := s.executeDDLPhase(tx, "Phase 2 (ALTER): Altering table columns", parsedDDLs.AlterColumnDDLs, false, log); err != nil { return err }
		} else {
			log.Info("Phase 2: No table structure (CREATE/ALTER) changes needed.")
		}

		if err := s.executeDDLPhase(tx, "Phase 3: Creating new indexes", parsedDDLs.AddIndexDDLs, true, log); err != nil { return err }

		var deferredFKs []string
		var nonDeferredFKs []string
		if s.dstDialect == "postgres" {
			deferredFKs, nonDeferredFKs = s.splitPostgresFKsForDeferredExecution(parsedDDLs.AddConstraintDDLs)
		} else {
			nonDeferredFKs = parsedDDLs.AddConstraintDDLs
		}

		if err := s.executeDDLPhase(tx, "Phase 4: Adding new non-deferred constraints", nonDeferredFKs, false, log); err != nil { return err }

		if s.dstDialect == "postgres" && len(deferredFKs) > 0 {
			if err := s.executeDDLPhase(tx, "Phase 4: Adding new DEFERRED PostgreSQL FK constraints", deferredFKs, false, log); err != nil { return err }
		}

		log.Info("DDL execution transaction phase finished successfully for table.")
		return nil
	})
}

// categorizedDDLs adalah struct untuk menampung DDL yang sudah dikategorikan.
type categorizedDDLs struct {
	CreateTableDDL     string
	AlterColumnDDLs    []string
	AddIndexDDLs       []string
	DropIndexDDLs      []string
	AddConstraintDDLs  []string
	DropConstraintDDLs []string
}

// parseAndCategorizeDDLs mem-parse DDL dari SchemaExecutionResult menjadi kategori yang lebih spesifik.
func (s *SchemaSyncer) parseAndCategorizeDDLs(ddls *SchemaExecutionResult, table string) (*categorizedDDLs, error) {
	log := s.logger.With(zap.String("table", table), zap.String("phase", "parse-ddls"))
	parsed := &categorizedDDLs{}

	if ddls.TableDDL != "" {
		trimmedUpperTableDDL := strings.ToUpper(strings.TrimSpace(ddls.TableDDL))
		if strings.HasPrefix(trimmedUpperTableDDL, "CREATE TABLE") {
			parsed.CreateTableDDL = ddls.TableDDL
		} else {
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
		if trimmed == "" { continue }
		upperTrimmed := strings.ToUpper(trimmed)
		if strings.Contains(upperTrimmed, "DROP INDEX") {
			parsed.DropIndexDDLs = append(parsed.DropIndexDDLs, trimmed)
		} else if strings.HasPrefix(upperTrimmed, "CREATE INDEX") || strings.HasPrefix(upperTrimmed, "CREATE UNIQUE INDEX") {
			parsed.AddIndexDDLs = append(parsed.AddIndexDDLs, trimmed)
		} else {
			log.Warn("Unknown DDL type in IndexDDLs, skipping parsing", zap.String("ddl", trimmed))
		}
	}
	parsed.DropIndexDDLs = sortDropIndexes(parsed.DropIndexDDLs)
	parsed.AddIndexDDLs = sortAddIndexes(parsed.AddIndexDDLs)

	for _, ddl := range ddls.ConstraintDDLs {
		trimmed := strings.TrimSpace(ddl)
		if trimmed == "" { continue }
		upperTrimmed := strings.ToUpper(trimmed)
		if strings.Contains(upperTrimmed, "DROP CONSTRAINT") || strings.Contains(upperTrimmed, "DROP FOREIGN KEY") || strings.Contains(upperTrimmed, "DROP CHECK") || strings.Contains(upperTrimmed, "DROP PRIMARY KEY") {
			parsed.DropConstraintDDLs = append(parsed.DropConstraintDDLs, trimmed)
		} else if strings.HasPrefix(upperTrimmed, "ADD CONSTRAINT") {
			parsed.AddConstraintDDLs = append(parsed.AddConstraintDDLs, trimmed)
		} else {
			log.Warn("Unknown DDL type in ConstraintDDLs, skipping parsing", zap.String("ddl", trimmed))
		}
	}
	parsed.DropConstraintDDLs = sortConstraintsForDrop(parsed.DropConstraintDDLs)
	parsed.AddConstraintDDLs = sortConstraintsForAdd(parsed.AddConstraintDDLs)

	parsed.AlterColumnDDLs = sortAlterColumns(parsed.AlterColumnDDLs)

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
		if ddl == "" { continue }
		log.Debug("Executing DDL.", zap.String("phase_name", phaseName), zap.String("ddl", ddl))
		if err := tx.Exec(ddl).Error; err != nil {
			execErrLog := log.With(zap.String("phase_name", phaseName), zap.String("failed_ddl", ddl), zap.Error(err))
			if s.shouldIgnoreDDLError(err) {
				execErrLog.Warn("DDL execution resulted in an ignorable error, continuing.")
			} else if continueOnError {
				execErrLog.Error("Failed to execute DDL, but configured to continue.")
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
	if err == nil { return false }
	errStr := strings.ToLower(err.Error())

	ignorablePatterns := map[string][]string{
		"mysql": {
			"duplicate key name",
			"can't drop index",
			"check constraint .* already exists",
			"constraint .* does not exist",
		},
		"postgres": {
			"relation .* already exists",
			"index .* already exists",
			"constraint .* for relation .* already exists",
			"constraint .* on table .* does not exist",
			"index .* does not exist",
			"role .* already exists",
			"schema .* already exists",
		},
		"sqlite": {
			"index .* already exists",
			"constraint .* failed",
		},
	}

	if patterns, ok := ignorablePatterns[s.dstDialect]; ok {
		for _, pattern := range patterns {
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

// GetPrimaryKeys mengambil nama kolom kunci primer untuk sebuah tabel dari sumber.
func (s *SchemaSyncer) GetPrimaryKeys(ctx context.Context, table string) ([]string, error) {
	log := s.logger.With(zap.String("table", table), zap.String("database_role", "source"))
	log.Debug("Fetching primary keys for table.")

	srcSchema, srcExists, err := s.fetchSchemaDetails(ctx, s.srcDB, s.srcDialect, table, "source")
	if err != nil {
		return nil, fmt.Errorf("failed to get source schema for PK detection, table '%s': %w", table, err)
	}
	if !srcExists {
		log.Warn("Cannot get primary keys, source table does not exist.")
		return []string{}, nil // Kembalikan slice kosong jika tabel tidak ada
	}


	pks := getPKColumnNames(srcSchema.Columns)
	if len(pks) == 0 {
		log.Warn("No primary key detected for table via column information. Checking constraints...")
		for _, cons := range srcSchema.Constraints {
			if cons.Type == "PRIMARY KEY" {
				pks = cons.Columns
				log.Info("Found primary key via constraints.", zap.Strings("pk_columns", pks))
				break
			}
		}
		if len(pks) == 0 {
			log.Warn("Still no primary key found after checking constraints.")
		}
	} else {
		log.Debug("Primary keys detected via column information.", zap.Strings("pk_columns", pks))
	}
	return pks, nil
}

// --- Dispatcher Methods untuk getColumns, getIndexes, getConstraints ---
func (s *SchemaSyncer) getColumns(ctx context.Context, db *gorm.DB, dialect string, table string) ([]ColumnInfo, error) {
	log := s.logger.With(zap.String("dialect", dialect), zap.String("table", table))
	log.Debug("Fetching table columns.")
	var cols []ColumnInfo
	var err error

	switch dialect {
	case "mysql":
		cols, err = s.getMySQLColumns(ctx, db, table)
	case "postgres":
		cols, err = s.getPostgresColumns(ctx, db, table)
	case "sqlite":
		cols, err = s.getSQLiteColumns(ctx, db, table)
	default:
		return nil, fmt.Errorf("getColumns: unsupported dialect: %s for table '%s'", dialect, table)
	}
	if err != nil {
		return nil, err
	}
	return cols, nil
}

func (s *SchemaSyncer) getIndexes(ctx context.Context, db *gorm.DB, dialect, table string) ([]IndexInfo, error) {
	log := s.logger.With(zap.String("dialect", dialect), zap.String("table", table))
	log.Debug("Fetching table indexes.")
	switch dialect {
	case "mysql":
		return s.getMySQLIndexes(ctx, db, table)
	case "postgres":
		return s.getPostgresIndexes(ctx, db, table)
	case "sqlite":
		return s.getSQLiteIndexes(ctx, db, table)
	default:
		log.Warn("Index fetching not implemented for dialect, returning empty.", zap.String("dialect", dialect))
		return []IndexInfo{}, nil
	}
}
func (s *SchemaSyncer) getConstraints(ctx context.Context, db *gorm.DB, dialect, table string) ([]ConstraintInfo, error) {
	log := s.logger.With(zap.String("dialect", dialect), zap.String("table", table))
	log.Debug("Fetching table constraints.")
	switch dialect {
	case "mysql":
		return s.getMySQLConstraints(ctx, db, table)
	case "postgres":
		return s.getPostgresConstraints(ctx, db, table)
	case "sqlite":
		return s.getSQLiteConstraints(ctx, db, table)
	default:
		log.Warn("Constraint fetching not implemented for dialect, returning empty.", zap.String("dialect", dialect))
		return []ConstraintInfo{}, nil
	}
}

// --- General Helper Functions (Common across dialects) ---
func getPKColumnNames(columns []ColumnInfo) []string {
	var pks []string
	for _, c := range columns {
		if c.IsPrimary {
			pks = append(pks, c.Name)
		}
	}
	sort.Strings(pks)
	return pks
}

func (s *SchemaSyncer) isSystemTable(table, dialect string) bool {
	lT := strings.ToLower(table)
	if strings.HasPrefix(lT, "information_schema.") { return true }

	switch dialect {
	case "mysql":
		return strings.HasPrefix(lT, "performance_schema.") ||
			strings.HasPrefix(lT, "mysql.") ||
			strings.HasPrefix(lT, "sys.") ||
			lT == "performance_schema" || lT == "mysql" || lT == "sys"
	case "postgres":
		return strings.HasPrefix(lT, "pg_catalog.") ||
			strings.HasPrefix(lT, "pg_toast.") ||
			lT == "pg_catalog" || lT == "pg_toast"
	case "sqlite":
		return strings.HasPrefix(lT, "sqlite_")
	default:
		return false
	}
}

// --- Type Mapping Helpers ---
func (s *SchemaSyncer) mapDataType(srcType string) (string, error) {
	log := s.logger.With(zap.String("source_type_raw", srcType),
		zap.String("src_dialect", s.srcDialect), zap.String("dst_dialect", s.dstDialect))

	fullSrcTypeLower := strings.ToLower(strings.TrimSpace(srcType))
	normalizedSrcTypeKey := normalizeTypeName(fullSrcTypeLower)

	typeMappingConfigEntry := config.GetTypeMappingForDialects(s.srcDialect, s.dstDialect)

	if typeMappingConfigEntry != nil {
		log.Debug("Found external type mapping configuration for dialect pair.")
		for _, sm := range typeMappingConfigEntry.SpecialMappings {
			re, errComp := regexp.Compile(sm.SourceTypePattern)
			if errComp != nil {
				log.Error("Invalid regex pattern in special type mapping, skipping this pattern.",
					zap.String("pattern", sm.SourceTypePattern), zap.Error(errComp))
				continue
			}
			if re.MatchString(fullSrcTypeLower) {
				log.Info("Matched special type mapping",
					zap.String("pattern", sm.SourceTypePattern),
					zap.String("raw_source_type", fullSrcTypeLower),
					zap.String("target_type", sm.TargetType))
				return s.applyTypeModifiers(srcType, sm.TargetType), nil
			}
		}

		if mappedType, ok := typeMappingConfigEntry.Mappings[normalizedSrcTypeKey]; ok {
			log.Info("Matched standard type mapping",
				zap.String("normalized_source_key", normalizedSrcTypeKey),
				zap.String("raw_source_type", srcType),
				zap.String("target_type", mappedType))
			return s.applyTypeModifiers(srcType, mappedType), nil
		}
	} else {
		log.Debug("No external type mapping configuration found for current dialect pair. Will use internal fallbacks or direct type if dialects are same.")
	}

	if s.srcDialect == s.dstDialect {
		log.Info("Source and destination dialects are the same, using source type directly (with modifiers).",
			zap.String("source_type", srcType))
		return srcType, nil
	}

	fallbackType := s.getGenericFallbackType()
	log.Warn("No specific type mapping found through external config or direct match for different dialects. Using generic fallback type.",
		zap.String("normalized_source_key_used_for_lookup", normalizedSrcTypeKey),
		zap.String("original_source_type", srcType),
		zap.String("fallback_target_type", fallbackType))
	return s.applyTypeModifiers(srcType, fallbackType), nil
}

func (s *SchemaSyncer) applyTypeModifiers(srcTypeRaw, mappedBaseType string) string {
	log := s.logger.With(
		zap.String("src_type_raw_for_modifier", srcTypeRaw),
		zap.String("mapped_base_type_for_modifier", mappedBaseType),
		zap.String("dst_dialect_for_modifier", s.dstDialect),
	)

	re := regexp.MustCompile(`\((.+?)\)`)
	matches := re.FindStringSubmatch(srcTypeRaw)
	var modifierContent string
	if len(matches) > 1 {
		modifierContent = strings.TrimSpace(matches[1])
	}

	baseTypeWithoutExistingModifiers := strings.Split(mappedBaseType, "(")[0]
	normBaseType := normalizeTypeName(baseTypeWithoutExistingModifiers)

	if modifierContent == "" {
		log.Debug("No modifier found in source type or mapped base type does not support it, returning base type.")
		return baseTypeWithoutExistingModifiers
	}

	canHaveModifier := false
	switch {
	case isStringType(normBaseType):
		if !strings.Contains(normBaseType, "text") && !strings.Contains(normBaseType, "blob") &&
			normBaseType != "clob" && normBaseType != "json" && normBaseType != "xml" && normBaseType != "uuid" {
			canHaveModifier = true
		}
	case isBinaryType(normBaseType):
		if !strings.Contains(normBaseType, "blob") && normBaseType != "bytea" {
			canHaveModifier = true
		}
	case isScaleRelevant(normBaseType):
		canHaveModifier = true
	case isPrecisionRelevant(normBaseType) && (strings.Contains(normBaseType, "time") || strings.Contains(normBaseType, "timestamp")):
		parts := strings.Split(modifierContent, ",")
		if len(parts) == 1 {
			if _, err := strconv.Atoi(strings.TrimSpace(parts[0])); err == nil {
				modifierContent = strings.TrimSpace(parts[0])
				canHaveModifier = true
			}
		}
	}

	if canHaveModifier {
		log.Debug("Applying modifier to base type.", zap.String("modifier_content", modifierContent))
		return fmt.Sprintf("%s(%s)", baseTypeWithoutExistingModifiers, modifierContent)
	}

	log.Debug("Modifier found in source, but target base type does not typically support it or modifier format is incompatible; returning base type without modifier.", zap.String("modifier_content", modifierContent))
	return baseTypeWithoutExistingModifiers
}

func (s *SchemaSyncer) getGenericFallbackType() string {
	switch s.dstDialect {
	case "mysql": return "LONGTEXT"
	case "postgres": return "TEXT"
	case "sqlite": return "TEXT"
	default:
		s.logger.Error("Unknown destination dialect for generic fallback type, defaulting to TEXT.", zap.String("unknown_dialect", s.dstDialect))
		return "TEXT"
	}
}

// --- DDL Execution Helpers (Sorting) ---
func sortConstraintsForDrop(ddls []string) []string {
	sorted := make([]string, len(ddls)); copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		isFkI := strings.Contains(strings.ToUpper(sorted[i]), "FOREIGN KEY")
		isFkJ := strings.Contains(strings.ToUpper(sorted[j]), "FOREIGN KEY")
		if isFkI && !isFkJ { return true }
		if !isFkI && isFkJ { return false }
		return false
	})
	return sorted
}

func sortConstraintsForAdd(ddls []string) []string {
	sorted := make([]string, len(ddls)); copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		isFkI := strings.Contains(strings.ToUpper(sorted[i]), "FOREIGN KEY")
		isFkJ := strings.Contains(strings.ToUpper(sorted[j]), "FOREIGN KEY")
		if !isFkI && isFkJ { return true }
		if isFkI && !isFkJ { return false }
		return false
	})
	return sorted
}

func sortAlterColumns(ddls []string) []string {
	priority := func(ddl string) int {
		upperDDL := strings.ToUpper(ddl)
		if strings.Contains(upperDDL, "DROP COLUMN") { return 1 }
		if strings.Contains(upperDDL, "MODIFY COLUMN") || strings.Contains(upperDDL, "ALTER COLUMN") { return 2 }
		if strings.Contains(upperDDL, "ADD COLUMN") { return 3 }
		return 4
	}
	sorted := make([]string, len(ddls)); copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		return priority(sorted[i]) < priority(sorted[j])
	})
	return sorted
}

func sortDropIndexes(ddls []string) []string {
	return ddls
}

func sortAddIndexes(ddls []string) []string {
	return ddls
}
