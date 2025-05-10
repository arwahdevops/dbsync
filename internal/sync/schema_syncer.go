package sync

import (
	"context"
	// "database/sql" // Tidak dibutuhkan langsung di file ini jika tipe ada di syncer_types.go
	"fmt"
	// "regexp" // Tidak dibutuhkan langsung di file ini
	// "strconv" // Tidak dibutuhkan langsung di file ini
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/arwahdevops/dbsync/internal/config"
	"github.com/arwahdevops/dbsync/internal/utils" // Diperlukan untuk QuoteIdentifier di ExecuteDDLs
)

// SchemaSyncer menangani logika sinkronisasi skema.
type SchemaSyncer struct {
	srcDB      *gorm.DB
	dstDB      *gorm.DB
	srcDialect string
	dstDialect string
	logger     *zap.Logger
}

// Ensure SchemaSyncer implements the interface
var _ SchemaSyncerInterface = (*SchemaSyncer)(nil)

// NewSchemaSyncer membuat instance SchemaSyncer baru.
func NewSchemaSyncer(srcDB, dstDB *gorm.DB, srcDialect, dstDialect string, logger *zap.Logger) *SchemaSyncer {
	return &SchemaSyncer{
		srcDB:      srcDB,
		dstDB:      dstDB,
		srcDialect: strings.ToLower(srcDialect),
		dstDialect: strings.ToLower(dstDialect),
		logger:     logger.Named("schema-syncer"),
	}
}

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
	// fetchSchemaDetails adalah method dari SchemaSyncer (di syncer_fetch_dispatch.go)
	srcSchema, srcExists, err := s.fetchSchemaDetails(ctx, s.srcDB, s.srcDialect, table, "source")
	if err != nil {
		return nil, fmt.Errorf("failed to get source schema for table '%s': %w", table, err)
	}
	if !srcExists {
		log.Warn("Source table does not exist. Skipping schema sync for this table.")
		return result, nil
	}
	if len(srcSchema.Columns) == 0 {
		log.Warn("Source table exists but appears to have no columns. Skipping schema sync for this table.")
		return result, nil
	}
	result.PrimaryKeys = getPKColumnNames(srcSchema.Columns) // getPKColumnNames dari syncer_helpers.go

	// populateMappedTypesForSourceColumns adalah method dari SchemaSyncer (di syncer_type_mapper.go)
	if err := s.populateMappedTypesForSourceColumns(srcSchema.Columns, table); err != nil {
		return nil, err
	}

	// 2. Generate DDL based on strategy
	switch strategy {
	case config.SchemaSyncDropCreate:
		log.Warn("Using 'drop_create' strategy - THIS IS DESTRUCTIVE!")
		ddls, errGen := s.generateDDLsForDropCreate(table, srcSchema.Columns, srcSchema.Indexes, srcSchema.Constraints)
		if errGen != nil {
			return nil, errGen
		}
		result.TableDDL = ddls.TableDDL
		result.IndexDDLs = ddls.IndexDDLs
		result.ConstraintDDLs = ddls.ConstraintDDLs

	case config.SchemaSyncAlter:
		log.Info("Attempting 'alter' schema synchronization strategy")
		// fetchSchemaDetails untuk destinasi
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
			// populateMappedTypesForDestinationColumns dari syncer_type_mapper.go
			s.populateMappedTypesForDestinationColumns(dstSchema.Columns)

			// generateAlterDDLs dari schema_alter.go (ini adalah method SchemaSyncer)
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

// generateDDLsForDropCreate adalah helper untuk strategi drop_create.
// Ini adalah method dari SchemaSyncer.
func (s *SchemaSyncer) generateDDLsForDropCreate(table string, srcColumns []ColumnInfo, srcIndexes []IndexInfo, srcConstraints []ConstraintInfo) (*SchemaExecutionResult, error) {
	log := s.logger.With(zap.String("table", table), zap.String("strategy", "generate_drop_create_ddls"))
	result := &SchemaExecutionResult{}

	// generateCreateTableDDL dari schema_ddl_create.go (method SchemaSyncer)
	tableDDL, _, errGen := s.generateCreateTableDDL(table, srcColumns) // pkColumnNamesFromCreateTable tidak digunakan lebih lanjut di sini
	if errGen != nil {
		return nil, fmt.Errorf("failed to generate CREATE TABLE DDL for '%s': %w", table, errGen)
	}
	result.TableDDL = tableDDL

	// Filter indeks dan constraint untuk menghindari duplikasi
	inlineConstraintNames := make(map[string]bool)
	for _, cons := range srcConstraints {
		if cons.Type == "PRIMARY KEY" || cons.Type == "UNIQUE" {
			// Jika nama constraint ini (yang unik) juga digunakan sebagai nama indeks,
			// maka indeks itu mungkin sudah dibuat secara implisit.
			inlineConstraintNames[cons.Name] = true
		}
	}
	// Jika PK tidak punya nama eksplisit (misal "PRIMARY" di MySQL),
	// kita tidak bisa filter indeks PK berdasarkan nama constraint di sini.
	// Tapi generateCreateIndexDDLs sudah skip idx.IsPrimary.

	filteredIndexes := make([]IndexInfo, 0, len(srcIndexes))
	for _, idx := range srcIndexes {
		if idx.IsPrimary {
			log.Debug("Skipping explicit CREATE INDEX for PRIMARY KEY (handled by CREATE TABLE).", zap.String("index_name", idx.Name))
			continue
		}
		if idx.IsUnique && inlineConstraintNames[idx.Name] {
			log.Debug("Skipping explicit CREATE UNIQUE INDEX; likely covered by inline UNIQUE constraint from CREATE TABLE.",
				zap.String("index_name", idx.Name))
			continue
		}
		filteredIndexes = append(filteredIndexes, idx)
	}
	// generateCreateIndexDDLs dari schema_ddl_create.go (method SchemaSyncer)
	result.IndexDDLs = s.generateCreateIndexDDLs(table, filteredIndexes)

	filteredConstraints := make([]ConstraintInfo, 0, len(srcConstraints))
	for _, cons := range srcConstraints {
		if cons.Type == "PRIMARY KEY" {
			log.Debug("Skipping explicit ADD CONSTRAINT for PRIMARY KEY (handled by CREATE TABLE).", zap.String("constraint_name", cons.Name))
			continue
		}
		if cons.Type == "UNIQUE" && inlineConstraintNames[cons.Name] {
			log.Debug("Skipping explicit ADD CONSTRAINT UNIQUE; likely covered by inline UNIQUE constraint from CREATE TABLE.",
				zap.String("constraint_name", cons.Name))
			continue
		}
		filteredConstraints = append(filteredConstraints, cons)
	}
	// generateAddConstraintDDLs dari schema_ddl_create.go (method SchemaSyncer)
	result.ConstraintDDLs = s.generateAddConstraintDDLs(table, filteredConstraints)

	return result, nil
}


// ExecuteDDLs menerapkan DDLs yang dihasilkan ke database tujuan.
// Ini adalah method dari SchemaSyncer yang memenuhi SchemaSyncerInterface.
func (s *SchemaSyncer) ExecuteDDLs(ctx context.Context, table string, ddls *SchemaExecutionResult) error {
	log := s.logger.With(zap.String("table", table), zap.String("database_role", "destination"), zap.String("action", "ExecuteDDLs"))

	if ddls == nil || (ddls.TableDDL == "" && len(ddls.IndexDDLs) == 0 && len(ddls.ConstraintDDLs) == 0) {
		log.Debug("No DDLs to execute for table.")
		return nil
	}
	log.Info("Starting DDL execution for table.")

	// parseAndCategorizeDDLs adalah method dari SchemaSyncer (di syncer_ddl_executor.go)
	parsedDDLs, errParse := s.parseAndCategorizeDDLs(ddls, table)
	if errParse != nil {
		return fmt.Errorf("error parsing DDLs for table '%s': %w", table, errParse)
	}

	return s.dstDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var revertFKChecks func() error
		isDropCreateStrategy := parsedDDLs.CreateTableDDL != ""

		if isDropCreateStrategy && s.dstDialect == "mysql" {
			log.Debug("Disabling FOREIGN_KEY_CHECKS for MySQL DROP/CREATE operation.")
			if err := tx.Exec("SET SESSION foreign_key_checks = 0;").Error; err != nil { // Gunakan SET SESSION
				log.Error("Failed to disable foreign key checks on destination for table. DDL execution might fail.",
					zap.String("table", table), zap.Error(err))
				// Pertimbangkan untuk return error di sini agar transaksi gagal jika ini kritis
				// return fmt.Errorf("failed to disable foreign key checks on destination for table '%s': %w", table, err)
			} else {
				revertFKChecks = func() error {
					log.Debug("Re-enabling FOREIGN_KEY_CHECKS for MySQL after DDL execution.", zap.String("table", table))
					return tx.Exec("SET SESSION foreign_key_checks = 1;").Error // Gunakan SET SESSION
				}
				defer func() {
					if revertFKChecks != nil {
						if rErr := revertFKChecks(); rErr != nil {
							log.Error("Failed to re-enable foreign key checks on destination for table after DDL execution.",
								zap.String("table", table), zap.Error(rErr))
						}
					}
				}()
			}
		}

		// Fase 1 (ALTER): Drop constraint dan indeks yang ada (jika bukan drop_create)
		if !isDropCreateStrategy {
			// executeDDLPhase adalah method dari SchemaSyncer (di syncer_ddl_executor.go)
			if err := s.executeDDLPhase(tx, "Phase 1 (ALTER): Dropping existing constraints", parsedDDLs.DropConstraintDDLs, true, log); err != nil {
				log.Error("Error(s) occurred during dropping existing constraints. Transaction will be rolled back.", zap.Error(err))
				return err // Kembalikan error untuk rollback transaksi
			}
			if err := s.executeDDLPhase(tx, "Phase 1 (ALTER): Dropping existing indexes", parsedDDLs.DropIndexDDLs, true, log); err != nil {
				log.Error("Error(s) occurred during dropping existing indexes. Transaction will be rolled back.", zap.Error(err))
				return err
			}
		}

		// Fase 2: CREATE TABLE (jika drop_create) atau ALTER COLUMN (jika alter)
		if isDropCreateStrategy {
			log.Warn("Executing DROP TABLE IF EXISTS prior to CREATE (destructive operation).", zap.String("table", table))
			dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", utils.QuoteIdentifier(table, s.dstDialect)) // utils dari internal/utils
			if s.dstDialect == "postgres" {
				dropSQL += " CASCADE"
			}
			if err := tx.Exec(dropSQL).Error; err != nil {
				if !s.shouldIgnoreDDLError(err) { // shouldIgnoreDDLError dari syncer_ddl_executor.go
					log.Error("Failed to execute DROP TABLE IF EXISTS, aborting.", zap.String("table", table), zap.Error(err))
					return fmt.Errorf("failed to execute DROP TABLE IF EXISTS for '%s': %w", table, err)
				}
				log.Warn("DROP TABLE IF EXISTS resulted in an ignorable error.", zap.String("table", table), zap.Error(err))
			}
			if err := s.executeDDLPhase(tx, "Phase 2 (DROP/CREATE): Creating table", []string{parsedDDLs.CreateTableDDL}, false, log); err != nil {
				return err // CREATE TABLE gagal -> fatal
			}
		} else if len(parsedDDLs.AlterColumnDDLs) > 0 {
			if err := s.executeDDLPhase(tx, "Phase 2 (ALTER): Altering table columns", parsedDDLs.AlterColumnDDLs, false, log); err != nil {
				return err // ALTER COLUMN gagal (dan tidak diabaikan) -> fatal
			}
		} else {
			log.Info("Phase 2: No table structure (CREATE/ALTER) changes needed.")
		}

		// Fase 3: ADD INDEX
		if err := s.executeDDLPhase(tx, "Phase 3: Creating new indexes", parsedDDLs.AddIndexDDLs, true, log); err != nil {
			// Jika ada error yang tidak diabaikan saat membuat indeks, kembalikan untuk rollback
			log.Error("Error(s) occurred during creating new indexes. Transaction will be rolled back.", zap.Error(err))
			return err
		}

		// Fase 4: ADD CONSTRAINT
		var deferredFKs []string
		var nonDeferredFKs []string
		if s.dstDialect == "postgres" {
			// splitPostgresFKsForDeferredExecution dari syncer_ddl_executor.go
			deferredFKs, nonDeferredFKs = s.splitPostgresFKsForDeferredExecution(parsedDDLs.AddConstraintDDLs)
		} else {
			nonDeferredFKs = parsedDDLs.AddConstraintDDLs
		}

		if err := s.executeDDLPhase(tx, "Phase 4: Adding new non-deferred constraints", nonDeferredFKs, false, log); err != nil {
			return err // Gagal menambah constraint non-FK -> fatal
		}

		if s.dstDialect == "postgres" && len(deferredFKs) > 0 {
			if err := s.executeDDLPhase(tx, "Phase 4 (Postgres): Adding new DEFERRED FK constraints", deferredFKs, false, log); err != nil {
				return err // Gagal menambah FK -> fatal
			}
		}

		log.Info("DDL execution transaction phase finished successfully for table.")
		return nil
	})
}


// GetPrimaryKeys mengambil nama kolom kunci primer untuk sebuah tabel dari sumber.
func (s *SchemaSyncer) GetPrimaryKeys(ctx context.Context, table string) ([]string, error) {
	log := s.logger.With(zap.String("table", table), zap.String("database_role", "source"), zap.String("action", "GetSourcePrimaryKeys"))
	log.Debug("Fetching primary keys for source table.")

	// fetchSchemaDetails dari syncer_fetch_dispatch.go
	srcSchema, srcExists, err := s.fetchSchemaDetails(ctx, s.srcDB, s.srcDialect, table, "source")
	if err != nil {
		return nil, fmt.Errorf("failed to get source schema for PK detection, table '%s': %w", table, err)
	}
	if !srcExists {
		log.Warn("Cannot get primary keys, source table does not exist.")
		return []string{}, nil
	}
	if len(srcSchema.Columns) == 0 {
		log.Warn("Cannot get primary keys, source table has no columns.")
		return []string{}, nil
	}

	pks := getPKColumnNames(srcSchema.Columns) // getPKColumnNames dari syncer_helpers.go
	if len(pks) == 0 {
		log.Debug("No primary key detected for table via column information. Checking constraints...")
		// Cek dari daftar constraint sumber
		for _, cons := range srcSchema.Constraints {
			if cons.Type == "PRIMARY KEY" {
				pks = cons.Columns // Kolom di ConstraintInfo sudah seharusnya dalam urutan yang benar
				log.Info("Found primary key via constraints.", zap.Strings("pk_columns", pks))
				break
			}
		}
		if len(pks) == 0 {
			log.Warn("Still no primary key found after checking constraints for source table.")
		}
	} else {
		log.Debug("Primary keys detected via column information for source table.", zap.Strings("pk_columns", pks))
	}
	return pks, nil
}


// --- Helper Umum SchemaSyncer ---

// getPKColumnNames (ada di syncer_helpers.go)
// isSystemTable (ada di syncer_helpers.go)

// populateMappedTypesForSourceColumns (ada di syncer_type_mapper.go)
// populateMappedTypesForDestinationColumns (ada di syncer_type_mapper.go)

// fetchSchemaDetails (ada di syncer_fetch_dispatch.go)
// getColumns (ada di syncer_fetch_dispatch.go)
// getIndexes (ada di syncer_fetch_dispatch.go)
// getConstraints (ada di syncer_fetch_dispatch.go)

// mapDataType (ada di syncer_type_mapper.go)
// applyTypeModifiers (ada di syncer_type_mapper.go)
// getGenericFallbackType (ada di syncer_type_mapper.go)

// generateCreateTableDDL (ada di schema_ddl_create.go)
// mapColumnDefinition (ada di schema_ddl_create.go)
// generateCreateIndexDDLs (ada di schema_ddl_create.go)
// generateAddConstraintDDLs (ada di schema_ddl_create.go)
// getAutoIncrementSyntax (ada di schema_ddl_create.go)
// formatDefaultValue (ada di schema_ddl_create.go)
// getCollationSyntax (ada di schema_ddl_create.go)
// getCommentSyntax (ada di schema_ddl_create.go)

// generateAlterDDLs (ada di schema_alter.go)
// generateMySQLModifyColumnDDLs (ada di ddl_alter_column_mysql.go)
// generatePostgresModifyColumnDDLs (ada di ddl_alter_column_postgres.go)
// generateSQLiteModifyColumnDDLs (ada di ddl_alter_column_sqlite.go)
// generateAddColumnDDL (ada di schema_ddl_alter.go)
// generateDropColumnDDL (ada di schema_ddl_alter.go)
// generateDropIndexDDL (ada di schema_ddl_alter.go)
// generateDropConstraintDDL (ada di schema_ddl_alter.go)

// parseAndCategorizeDDLs (ada di syncer_ddl_executor.go)
// executeDDLPhase (ada di syncer_ddl_executor.go)
// shouldIgnoreDDLError (ada di syncer_ddl_executor.go)
// splitPostgresFKsForDeferredExecution (ada di syncer_ddl_executor.go)
// sort* (ada di syncer_ddl_executor.go)

// getColumnModifications (ada di compare_columns.go)
// areTypesEquivalent (ada di compare_columns.go)
// areDefaultsEquivalent (ada di compare_columns.go)
// extractBaseTypeFromGenerated (ada di compare_columns.go)

// needsColumnModification (ada di schema_compare.go)
// needsIndexModification (ada di schema_compare.go)
// needsConstraintModification (ada di schema_compare.go)
// normalizeDefaultValue (ada di schema_compare.go)
// normalizeFKAction (ada di schema_compare.go)
// normalizeCheckDefinition (ada di schema_compare.go)
// isDefaultNullOrFunction (ada di schema_compare.go)

// normalizeTypeName (ada di syncer_type_mapper.go)
// isStringType (ada di syncer_type_mapper.go)
// ... dan helper tipe lainnya ada di syncer_type_mapper.go
