// internal/sync/orchestrator.go
package sync

import (
	"context"
	"fmt"
	// "sort"    // HAPUS JIKA QUERY SUDAH ORDER BY
	// "strings" // HAPUS JIKA TIDAK ADA OPERASI STRING LAGI DI FILE INI
	"time"

	"go.uber.org/zap"

	"github.com/arwahdevops/dbsync/internal/config"
	"github.com/arwahdevops/dbsync/internal/db"
	"github.com/arwahdevops/dbsync/internal/metrics"
)

// Orchestrator mengelola keseluruhan proses sinkronisasi.
type Orchestrator struct {
	srcConn      *db.Connector
	dstConn      *db.Connector
	cfg          *config.Config
	logger       *zap.Logger
	schemaSyncer SchemaSyncerInterface
	metrics      *metrics.Store
}

// SyncResult menyimpan hasil sinkronisasi untuk satu tabel.
type SyncResult struct {
	Table                    string
	SchemaSyncSkipped        bool
	SchemaAnalysisError      error
	SchemaExecutionError     error
	DataError                error
	ConstraintExecutionError error
	SkipReason               string
	RowsSynced               int64
	Batches                  int
	Duration                 time.Duration
	Skipped                  bool
}

var _ OrchestratorInterface = (*Orchestrator)(nil)

func NewOrchestrator(srcConn, dstConn *db.Connector, cfg *config.Config, logger *zap.Logger, metricsStore *metrics.Store) *Orchestrator {
	return &Orchestrator{
		srcConn:      srcConn,
		dstConn:      dstConn,
		cfg:          cfg,
		logger:       logger.Named("full-sync"),
		schemaSyncer: NewSchemaSyncer(
			srcConn.DB,
			dstConn.DB,
			srcConn.Dialect,
			dstConn.Dialect,
			logger,
		),
		metrics: metricsStore,
	}
}

func (f *Orchestrator) Run(ctx context.Context) map[string]SyncResult {
	startTime := time.Now()
	f.logger.Info("Starting full database synchronization run",
		zap.String("direction", f.cfg.SyncDirection),
		zap.Int("workers", f.cfg.Workers),
		zap.Int("batch_size", f.cfg.BatchSize),
		zap.String("schema_strategy", string(f.cfg.SchemaSyncStrategy)),
	)
	f.metrics.SyncRunning.Set(1)
	defer f.metrics.SyncRunning.Set(0)

	results := make(map[string]SyncResult)
	tables, err := f.listTables(ctx)
	if err != nil {
		f.logger.Error("Failed to list source tables", zap.Error(err))
		f.metrics.SyncErrorsTotal.WithLabelValues("list_tables", "").Inc()
		f.metrics.SyncDuration.Observe(time.Since(startTime).Seconds())
		return results
	}

	if len(tables) == 0 {
		f.logger.Warn("No tables found in source database to synchronize")
		f.metrics.SyncDuration.Observe(time.Since(startTime).Seconds())
		return results
	}

	f.logger.Info("Found tables to synchronize", zap.Int("count", len(tables)), zap.Strings("tables", tables))

	results = f.runTableProcessingPool(ctx, tables)

	f.logger.Info("Full synchronization run finished",
		zap.Duration("total_duration", time.Since(startTime)),
		zap.Int("total_tables_processed_or_skipped", len(results)),
	)
	f.metrics.SyncDuration.Observe(time.Since(startTime).Seconds())

	return results
}

func (f *Orchestrator) listTables(ctx context.Context) ([]string, error) {
	var tables []string
	var err error
	dbCfg := f.cfg.SrcDB
	dialect := f.srcConn.Dialect
	log := f.logger.With(zap.String("dialect", dialect), zap.String("database", dbCfg.DBName), zap.String("action", "listTables"))
	log.Info("Listing user tables from source database")

	db := f.srcConn.DB.WithContext(ctx)

	switch dialect {
	case "mysql":
		// Query sudah menyertakan ORDER BY table_name
		// Jika ada filter tambahan menggunakan strings.Contains atau sejenisnya, `strings` perlu dipertahankan.
		// Contoh filter: `AND table_name NOT IN ('sys_config')` sudah ada di query.
		query := `SELECT table_name FROM information_schema.tables
				  WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'
				  AND table_name NOT IN ('sys_config')
				  ORDER BY table_name`
		err = db.Raw(query).Scan(&tables).Error
	case "postgres":
		// Query sudah menyertakan ORDER BY table_name
		// Filter `NOT LIKE 'pg_%'` juga sudah di query.
		query := `SELECT table_name FROM information_schema.tables
				  WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'
				  AND table_name NOT LIKE 'pg_%' AND table_name NOT LIKE 'sql_%'
				  ORDER BY table_name`
		err = db.Raw(query).Scan(&tables).Error
	case "sqlite":
		// Query sudah menyertakan ORDER BY name
		// Filter `NOT LIKE 'sqlite_%'` juga sudah di query.
		query := `SELECT name FROM sqlite_master
				  WHERE type='table' AND name NOT LIKE 'sqlite_%'
				  ORDER BY name`
		err = db.Raw(query).Scan(&tables).Error
	default:
		return nil, fmt.Errorf("unsupported dialect for listing tables: %s", dialect)
	}

	if err != nil {
		if ctx.Err() != nil {
			log.Error("Context cancelled during table listing", zap.Error(ctx.Err()), zap.NamedError("db_error", err))
			return nil, fmt.Errorf("context cancelled during table listing: %w; db error: %v", ctx.Err(), err)
		}
		log.Error("Failed to execute list tables query", zap.Error(err))
		return nil, fmt.Errorf("failed to list tables for database '%s' (%s): %w", dbCfg.DBName, dialect, err)
	}

	// Karena query sudah ORDER BY, baris `sort.Strings(tables)` ini tidak lagi diperlukan.
	// sort.Strings(tables)

	log.Debug("Table listing successful", zap.Int("table_count", len(tables)))
	return tables, nil
}
