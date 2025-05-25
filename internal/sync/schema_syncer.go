// internal/sync/schema_syncer.go
package sync

import (
	"context"
	"fmt"
	"sort" // <-- TAMBAHKAN IMPOR INI
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/arwahdevops/dbsync/internal/config"
	"github.com/arwahdevops/dbsync/internal/utils"
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

	if s.isSystemTable(table, s.srcDialect) { // from syncer_helpers.go
		log.Debug("Skipping system table for schema sync.")
		return result, nil // Tidak ada DDL, tidak ada PK yang relevan untuk sync
	}

	srcSchema, srcExists, err := s.fetchSchemaDetails(ctx, s.srcDB, s.srcDialect, table, "source")
	if err != nil {
		return nil, fmt.Errorf("failed to get source schema for table '%s': %w", table, err)
	}
	if !srcExists {
		log.Warn("Source table does not exist. Skipping schema sync for this table.")
		return result, nil // Tidak ada DDL, PK juga tidak ada
	}
	if len(srcSchema.Columns) == 0 {
		log.Warn("Source table exists but appears to have no columns. Skipping schema sync for this table.")
		return result, nil // Tidak ada DDL, PK juga tidak ada
	}

	// Dapatkan PK dari sumber, ini penting bahkan untuk strategi 'none' agar data sync tahu cara paginasi
	result.PrimaryKeys = getPKColumnNames(srcSchema.Columns) // from syncer_helpers.go
	if len(result.PrimaryKeys) == 0 {                        // Coba dari constraint jika kolom tidak menandai PK
		for _, cons := range srcSchema.Constraints {
			if cons.Type == "PRIMARY KEY" {
				result.PrimaryKeys = cons.Columns
				sort.Strings(result.PrimaryKeys) // Pastikan urutan konsisten
				break
			}
		}
	}
	log.Debug("Determined primary keys for source table (for data sync pagination).", zap.Strings("pks", result.PrimaryKeys))

	// Isi MappedType untuk kolom sumber. Ini diperlukan untuk SEMUA strategi
	// karena MappedType sumber akan menjadi dasar untuk definisi kolom tujuan.
	if err := s.populateMappedTypesForSourceColumns(srcSchema.Columns, table); err != nil { // from syncer_type_mapper.go
		return nil, fmt.Errorf("failed to populate mapped types for source columns of table '%s': %w", table, err)
	}

	switch strategy {
	case config.SchemaSyncDropCreate:
		log.Warn("Using 'drop_create' strategy - THIS IS DESTRUCTIVE!")
		// generateDDLsForDropCreate sudah menggunakan MappedType dari srcSchema.Columns
		ddls, errGen := s.generateDDLsForDropCreate(table, srcSchema.Columns, srcSchema.Indexes, srcSchema.Constraints)
		if errGen != nil {
			return nil, fmt.Errorf("failed to generate DDLs for drop_create strategy on table '%s': %w", table, errGen)
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
			// Sama seperti drop_create, tapi tanpa DROP eksplisit di awal (CREATE IF NOT EXISTS akan menangani)
			ddls, errGen := s.generateDDLsForDropCreate(table, srcSchema.Columns, srcSchema.Indexes, srcSchema.Constraints)
			if errGen != nil {
				return nil, fmt.Errorf("failed to generate DDLs for create (alter strategy, dst not exists) on table '%s': %w", table, errGen)
			}
			result.TableDDL = ddls.TableDDL
			result.IndexDDLs = ddls.IndexDDLs
			result.ConstraintDDLs = ddls.ConstraintDDLs
		} else {
			log.Debug("Destination table exists, generating ALTER DDLs if necessary.")
			// populateMappedTypesForDestinationColumns menggunakan tipe asli tujuan jika MappedType kosong.
			// Ini digunakan oleh getColumnModifications untuk perbandingan.
			s.populateMappedTypesForDestinationColumns(dstSchema.Columns) // from syncer_type_mapper.go

			// generateAlterDDLs menggunakan srcSchema.Columns (yang MappedType-nya sudah diisi untuk target)
			// dan dstSchema.Columns (yang MappedType-nya diisi dengan tipe asli tujuan)
			joinedAlterColumnDDLs, indexDDLs, constraintDDLs, errGen := s.generateAlterDDLs(
				table,
				srcSchema.Columns, srcSchema.Indexes, srcSchema.Constraints,
				dstSchema.Columns, dstSchema.Indexes, dstSchema.Constraints,
			) // from schema_alter.go
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
func (s *SchemaSyncer) generateDDLsForDropCreate(table string, srcColumns []ColumnInfo, srcIndexes []IndexInfo, srcConstraints []ConstraintInfo) (*SchemaExecutionResult, error) {
	log := s.logger.With(zap.String("table", table), zap.String("strategy", "generate_drop_create_ddls"))
	result := &SchemaExecutionResult{}

	// generateCreateTableDDL menggunakan MappedType dari srcColumns
	// dan juga mengembalikan PK (unquoted) untuk data sync, meskipun kita tidak langsung pakai di sini.
	tableDDL, _, errGen := s.generateCreateTableDDL(table, srcColumns) // from schema_ddl_create.go
	if errGen != nil {
		return nil, fmt.Errorf("failed to generate CREATE TABLE DDL for '%s': %w", table, errGen)
	}
	result.TableDDL = tableDDL

	finalIndexes := make([]IndexInfo, 0, len(srcIndexes))
	for _, idx := range srcIndexes {
		if idx.IsPrimary {
			log.Debug("Skipping explicit CREATE INDEX for PRIMARY KEY in drop_create (handled by CREATE TABLE).", zap.String("index_name", idx.Name))
			continue
		}
		finalIndexes = append(finalIndexes, idx)
	}
	result.IndexDDLs = s.generateCreateIndexDDLs(table, finalIndexes) // from schema_ddl_create.go

	finalConstraints := make([]ConstraintInfo, 0, len(srcConstraints))
	for _, cons := range srcConstraints {
		if cons.Type == "PRIMARY KEY" {
			log.Debug("Skipping explicit ADD CONSTRAINT for PRIMARY KEY in drop_create (handled by CREATE TABLE).", zap.String("constraint_name", cons.Name))
			continue
		}
		if cons.Type == "UNIQUE" {
			log.Debug("Skipping explicit ADD CONSTRAINT UNIQUE in drop_create (handled by CREATE UNIQUE INDEX or inline in CREATE TABLE).", zap.String("constraint_name", cons.Name))
			continue
		}
		finalConstraints = append(finalConstraints, cons)
	}
	result.ConstraintDDLs = s.generateAddConstraintDDLs(table, finalConstraints) // from schema_ddl_create.go

	return result, nil
}

// ExecuteDDLs menerapkan DDLs yang dihasilkan ke database tujuan.
func (s *SchemaSyncer) ExecuteDDLs(ctx context.Context, table string, ddls *SchemaExecutionResult) error {
	log := s.logger.With(zap.String("table", table), zap.String("database_role", "destination"), zap.String("action", "ExecuteDDLs"))

	if ddls == nil || (ddls.TableDDL == "" && len(ddls.IndexDDLs) == 0 && len(ddls.ConstraintDDLs) == 0) {
		log.Debug("No DDLs to execute for table.")
		return nil
	}
	log.Info("Starting DDL execution for table.")

	// parseAndCategorizeDDLs ada di syncer_ddl_executor.go
	parsedDDLs, errParse := s.parseAndCategorizeDDLs(ddls, table)
	if errParse != nil {
		return fmt.Errorf("error parsing DDLs for table '%s': %w", table, errParse)
	}

	return s.dstDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var revertFKChecks func() error // Untuk MySQL FK checks

		isTableCreation := parsedDDLs.CreateTableDDL != ""

		if isTableCreation && s.dstDialect == "mysql" {
			log.Debug("Disabling FOREIGN_KEY_CHECKS for MySQL table creation operation.")
			if err := tx.Exec("SET SESSION foreign_key_checks = 0;").Error; err != nil {
				log.Error("Failed to disable foreign key checks on destination for table. DDL execution might fail.", zap.String("table", table), zap.Error(err))
			} else {
				revertFKChecks = func() error {
					log.Debug("Re-enabling FOREIGN_KEY_CHECKS for MySQL after DDL execution.", zap.String("table", table))
					return tx.Exec("SET SESSION foreign_key_checks = 1;").Error
				}
				defer func() {
					if revertFKChecks != nil {
						if rErr := revertFKChecks(); rErr != nil {
							log.Error("Failed to re-enable foreign key checks on destination for table after DDL execution.", zap.String("table", table), zap.Error(rErr))
						}
					}
				}()
			}
		}

		// Urutan Eksekusi:
		if !isTableCreation { // Strategi ALTER murni
			if err := s.executeDDLPhase(tx, "Phase 1 (ALTER): Dropping existing constraints", parsedDDLs.DropConstraintDDLs, true, log); err != nil {
				return err
			}
			if err := s.executeDDLPhase(tx, "Phase 1 (ALTER): Dropping existing indexes", parsedDDLs.DropIndexDDLs, true, log); err != nil {
				return err
			}
		}

		if isTableCreation {
			log.Warn("Executing DROP TABLE IF EXISTS prior to CREATE (destructive operation).", zap.String("table", table))
			dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", utils.QuoteIdentifier(table, s.dstDialect))
			if s.dstDialect == "postgres" {
				dropSQL += " CASCADE"
			}
			dropSQL += ";"
			if err := tx.Exec(dropSQL).Error; err != nil {
				if !s.shouldIgnoreDDLError(err, dropSQL) { // from syncer_ddl_executor.go
					log.Error("Failed to execute DROP TABLE IF EXISTS, aborting table creation.", zap.String("table", table), zap.Error(err))
					return fmt.Errorf("failed to execute DROP TABLE IF EXISTS for '%s': %w", table, err)
				}
				log.Warn("DROP TABLE IF EXISTS resulted in an ignorable error.", zap.String("table", table), zap.Error(err))
			}
			if err := s.executeDDLPhase(tx, "Phase 2 (CREATE): Creating table", []string{parsedDDLs.CreateTableDDL}, false, log); err != nil {
				return err
			}
		} else if len(parsedDDLs.AlterColumnDDLs) > 0 {
			if err := s.executeDDLPhase(tx, "Phase 2 (ALTER): Altering table columns", parsedDDLs.AlterColumnDDLs, false, log); err != nil {
				return err
			}
		} else {
			log.Info("Phase 2: No table structure (CREATE/ALTER) changes needed.")
		}

		if err := s.executeDDLPhase(tx, "Phase 3: Creating new indexes", parsedDDLs.AddIndexDDLs, true, log); err != nil {
			return err
		}

		var deferredFKs, nonDeferredConstraints []string
		if s.dstDialect == "postgres" {
			deferredFKs, nonDeferredConstraints = s.splitPostgresFKsForDeferredExecution(parsedDDLs.AddConstraintDDLs) // from syncer_ddl_executor.go
		} else {
			nonDeferredConstraints = parsedDDLs.AddConstraintDDLs
		}

		if err := s.executeDDLPhase(tx, "Phase 4: Adding new non-deferred constraints", nonDeferredConstraints, false, log); err != nil {
			return err
		}
		if s.dstDialect == "postgres" && len(deferredFKs) > 0 {
			if err := s.executeDDLPhase(tx, "Phase 4 (Postgres): Adding new DEFERRED FK constraints", deferredFKs, false, log); err != nil {
				return err
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

	pks := getPKColumnNames(srcSchema.Columns) // from syncer_helpers.go
	if len(pks) == 0 {
		log.Debug("No primary key detected for table via column information. Checking constraints...")
		for _, cons := range srcSchema.Constraints {
			if cons.Type == "PRIMARY KEY" {
				pks = make([]string, len(cons.Columns))
				copy(pks, cons.Columns)
				sort.Strings(pks) // Pastikan urutan konsisten
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

// GetFKDependencies mengambil dependensi FK keluar untuk daftar tabel yang diberikan.
// Mengembalikan peta: nama tabel -> slice nama tabel yang dirujuk (hanya jika tabel referensi ada di tableNames).
func (s *SchemaSyncer) GetFKDependencies(ctx context.Context, db *gorm.DB, dialect string, tableNames []string) (map[string][]string, error) {
	log := s.logger.With(zap.String("dialect", dialect), zap.String("action", "GetFKDependencies"))
	log.Info("Fetching foreign key dependencies for tables.", zap.Strings("tables_to_query_for_fk_deps", tableNames))

	dependencies := make(map[string][]string)
	tableSet := make(map[string]bool, len(tableNames))
	for _, tn := range tableNames {
		dependencies[tn] = []string{}
		tableSet[tn] = true
	}

	for _, tableName := range tableNames {
		constraints, err := s.getConstraints(ctx, db, dialect, tableName) // from syncer_fetch_dispatch.go
		if err != nil {
			log.Error("Failed to get constraints for FK dependency analysis. This might affect table processing order.",
				zap.String("table_being_analyzed", tableName), zap.Error(err))
			continue
		}

		currentTableFkTargets := make(map[string]bool)
		for _, cons := range constraints {
			if cons.Type == "FOREIGN KEY" && cons.ForeignTable != "" && cons.ForeignTable != tableName {
				if _, isRelevantForeignTable := tableSet[cons.ForeignTable]; isRelevantForeignTable {
					if !currentTableFkTargets[cons.ForeignTable] {
						dependencies[tableName] = append(dependencies[tableName], cons.ForeignTable)
						currentTableFkTargets[cons.ForeignTable] = true
					}
				}
			}
		}
		if len(dependencies[tableName]) > 0 {
			sort.Strings(dependencies[tableName]) // Urutkan untuk konsistensi
		}
	}

	if log.Core().Enabled(zap.DebugLevel) {
		debugDeps := make(map[string][]string)
		for k, v := range dependencies {
			if len(v) > 0 {
				debugDeps[k] = v
			}
		}
		if len(debugDeps) > 0 {
			log.Debug("Foreign key dependencies (table -> its prerequisites within the sync set) fetched.", zap.Any("dependencies_map", debugDeps))
		} else {
			log.Debug("No relevant foreign key dependencies found among the set of tables being synced.")
		}
	}
	return dependencies, nil
}
