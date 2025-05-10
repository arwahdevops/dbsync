package sync

import (
	"context"
	"fmt"
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

	if s.isSystemTable(table, s.srcDialect) {
		log.Debug("Skipping system table for schema sync.")
		return result, nil // Tidak ada DDL, PK kosong
	}

	// 1. Get Source Schema
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
	result.PrimaryKeys = getPKColumnNames(srcSchema.Columns)

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
			s.populateMappedTypesForDestinationColumns(dstSchema.Columns)

			// generateAlterDDLs ada di schema_alter.go
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
		return []string{}, nil
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

// generateDDLsForDropCreate adalah helper untuk strategi drop_create.
func (s *SchemaSyncer) generateDDLsForDropCreate(table string, srcColumns []ColumnInfo, srcIndexes []IndexInfo, srcConstraints []ConstraintInfo) (*SchemaExecutionResult, error) {
	result := &SchemaExecutionResult{}
	// generateCreateTableDDL dari schema_ddl_create.go
	tableDDL, _, errGen := s.generateCreateTableDDL(table, srcColumns)
	if errGen != nil {
		return nil, fmt.Errorf("failed to generate CREATE TABLE DDL for '%s': %w", table, errGen)
	}
	result.TableDDL = tableDDL
	// generateCreateIndexDDLs dari schema_ddl_create.go
	result.IndexDDLs = s.generateCreateIndexDDLs(table, srcIndexes)
	// generateAddConstraintDDLs dari schema_ddl_create.go
	result.ConstraintDDLs = s.generateAddConstraintDDLs(table, srcConstraints)
	return result, nil
}
