package sync

import (
	"context"
	"fmt"
	//"sort" // Diperlukan jika kita sort PK dari DB secara manual, meskipun query biasanya sudah bisa ORDER BY
	// "strings" // Mungkin tidak diperlukan lagi di file ini
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm" // Diperlukan untuk method getDestinationTablePKs

	"github.com/arwahdevops/dbsync/internal/config"
	"github.com/arwahdevops/dbsync/internal/db"
	"github.com/arwahdevops/dbsync/internal/metrics"
	"github.com/arwahdevops/dbsync/internal/utils" // Untuk QuoteIdentifier di getDestinationTablePKs
)

// Orchestrator mengelola keseluruhan proses sinkronisasi.
type Orchestrator struct {
	srcConn      *db.Connector
	dstConn      *db.Connector
	cfg          *config.Config
	logger       *zap.Logger
	schemaSyncer SchemaSyncerInterface // Tetap menggunakan interface untuk schema syncer utama
	metrics      *metrics.Store
}

// SyncResult menyimpan hasil sinkronisasi untuk satu tabel.
// Definisi ini bisa dipindahkan ke syncer_types.go jika belum
/*
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
*/

var _ OrchestratorInterface = (*Orchestrator)(nil)

func NewOrchestrator(srcConn, dstConn *db.Connector, cfg *config.Config, logger *zap.Logger, metricsStore *metrics.Store) *Orchestrator {
	return &Orchestrator{
		srcConn:      srcConn,
		dstConn:      dstConn,
		cfg:          cfg,
		logger:       logger.Named("orchestrator"), // Diubah dari "full-sync" agar lebih spesifik ke peran
		schemaSyncer: NewSchemaSyncer( // schemaSyncer untuk operasi skema sumber -> tujuan
			srcConn.DB,
			dstConn.DB,
			srcConn.Dialect,
			dstConn.Dialect,
			logger, // Logger utama akan di-scope lebih lanjut oleh SchemaSyncer
		),
		metrics: metricsStore,
	}
}

func (f *Orchestrator) Run(ctx context.Context) map[string]SyncResult {
	startTime := time.Now()
	f.logger.Info("Starting database synchronization run",
		zap.String("direction", f.cfg.SyncDirection),
		zap.Int("workers", f.cfg.Workers),
		zap.Int("batch_size", f.cfg.BatchSize),
		zap.String("schema_strategy", string(f.cfg.SchemaSyncStrategy)),
	)
	f.metrics.SyncRunning.Set(1)
	defer f.metrics.SyncRunning.Set(0)

	results := make(map[string]SyncResult)
	tables, err := f.listSourceTables(ctx) // Mengganti nama agar lebih jelas
	if err != nil {
		f.logger.Error("Failed to list source tables", zap.Error(err))
		f.metrics.SyncErrorsTotal.WithLabelValues("list_tables", "").Inc()
		f.metrics.SyncDuration.Observe(time.Since(startTime).Seconds())
		// Kembalikan map kosong jika gagal list tabel, atau bisa juga error
		return results
	}

	if len(tables) == 0 {
		f.logger.Warn("No tables found in source database to synchronize")
		f.metrics.SyncDuration.Observe(time.Since(startTime).Seconds())
		return results
	}

	f.logger.Info("Found tables to synchronize", zap.Int("count", len(tables)), zap.Strings("tables", tables))

	// runTableProcessingPool sekarang menjadi method dari Orchestrator
	results = f.runTableProcessingPool(ctx, tables)

	totalDuration := time.Since(startTime)
	f.logger.Info("Synchronization run finished",
		zap.Duration("total_duration", totalDuration),
		zap.Int("total_tables_processed_or_skipped", len(results)),
	)
	f.metrics.SyncDuration.Observe(totalDuration.Seconds())

	return results
}

// listSourceTables mengambil daftar tabel dari database sumber.
func (f *Orchestrator) listSourceTables(ctx context.Context) ([]string, error) {
	var tables []string
	var err error
	dbCfg := f.cfg.SrcDB
	dialect := f.srcConn.Dialect
	log := f.logger.With(zap.String("dialect", dialect), zap.String("database", dbCfg.DBName), zap.String("action", "listSourceTables"))
	log.Info("Listing user tables from source database")

	db := f.srcConn.DB.WithContext(ctx) // Gunakan koneksi sumber

	switch dialect {
	case "mysql":
		query := `SELECT table_name FROM information_schema.tables
				  WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'
				  AND table_name NOT IN ('sys_config') -- Contoh filter
				  ORDER BY table_name;`
		err = db.Raw(query).Scan(&tables).Error
	case "postgres":
		query := `SELECT table_name FROM information_schema.tables
				  WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'
				  AND table_name NOT LIKE 'pg_%' AND table_name NOT LIKE 'sql_%'
				  ORDER BY table_name;`
		err = db.Raw(query).Scan(&tables).Error
	case "sqlite":
		query := `SELECT name FROM sqlite_master
				  WHERE type='table' AND name NOT LIKE 'sqlite_%'
				  ORDER BY name;`
		err = db.Raw(query).Scan(&tables).Error
	default:
		return nil, fmt.Errorf("unsupported dialect for listing tables: %s", dialect)
	}

	if err != nil {
		if ctx.Err() != nil { // Cek apakah context dibatalkan
			log.Error("Context cancelled during table listing", zap.Error(ctx.Err()), zap.NamedError("db_error", err))
			return nil, fmt.Errorf("context cancelled during table listing: %w (db error: %v)", ctx.Err(), err)
		}
		log.Error("Failed to execute list tables query", zap.Error(err))
		return nil, fmt.Errorf("failed to list tables for database '%s' (%s): %w", dbCfg.DBName, dialect, err)
	}

	log.Debug("Table listing successful", zap.Int("table_count", len(tables)))
	return tables, nil
}

// getDestinationTablePKs mengambil Primary Key untuk tabel di database tujuan.
// Ini adalah method privat dari Orchestrator.
func (f *Orchestrator) getDestinationTablePKs(ctx context.Context, tableName string) ([]string, error) {
	log := f.logger.With(zap.String("table", tableName), zap.String("dialect", f.dstConn.Dialect), zap.String("action", "getDestinationTablePKs"))
	log.Debug("Fetching primary keys for destination table")

	var pks []string
	var err error

	// Gunakan koneksi tujuan (f.dstConn.DB)
	db := f.dstConn.DB.WithContext(ctx)

	switch f.dstConn.Dialect {
	case "postgres":
		query := `
            SELECT kcu.column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = current_schema() -- Atau schema spesifik jika diperlukan
              AND tc.table_name = $1
            ORDER BY kcu.ordinal_position;` // Penting untuk urutan PK komposit
		err = db.Raw(query, tableName).Scan(&pks).Error
	case "mysql":
		query := `
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = ?
              AND COLUMN_KEY = 'PRI'
            ORDER BY ORDINAL_POSITION;` // Penting untuk urutan PK komposit
		err = db.Raw(query, tableName).Scan(&pks).Error
	case "sqlite":
		var columnsInfo []struct {
			Name string `gorm:"column:name"`
			Pk   int    `gorm:"column:pk"` // Urutan PK, 0 jika bukan PK, 1, 2, ... untuk PK komposit
		}
		// PRAGMA table_info mengembalikan kolom dalam urutan definisi, Pk > 0 menandakan PK.
		// Untuk PK komposit, Pk akan 1, 2, dst.
		err = db.Raw(fmt.Sprintf("PRAGMA table_info(%s);", utils.QuoteIdentifier(tableName, "sqlite"))).Scan(&columnsInfo).Error
		if err == nil {
			pkMap := make(map[int]string)
			maxPkOrder := 0
			for _, col := range columnsInfo {
				if col.Pk > 0 {
					pkMap[col.Pk] = col.Name
					if col.Pk > maxPkOrder {
						maxPkOrder = col.Pk
					}
				}
			}
			pks = make([]string, maxPkOrder)
			for i := 1; i <= maxPkOrder; i++ {
				pks[i-1] = pkMap[i] // Pk adalah 1-based index
			}
		}
	default:
		return nil, fmt.Errorf("getDestinationTablePKs: unsupported destination dialect %s", f.dstConn.Dialect)
	}

	if err != nil {
		if err == gorm.ErrRecordNotFound { // Jika query tidak menemukan baris (misal, tabel tidak ada PK)
			log.Warn("No primary key rows found by query for destination table.", zap.Error(err))
			return []string{}, nil // Kembalikan slice kosong, bukan error
		}
		return nil, fmt.Errorf("failed to query PKs for destination table '%s' (%s): %w", tableName, f.dstConn.Dialect, err)
	}
    if len(pks) == 0 {
        log.Warn("No primary key columns identified for destination table. Upsert behavior will be affected.", zap.String("table", tableName))
    } else {
        log.Debug("Destination primary keys fetched.", zap.Strings("pk_columns", pks))
    }
	return pks, nil
}
