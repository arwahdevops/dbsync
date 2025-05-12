// internal/sync/orchestrator.go
package sync

import (
	"context"
	"fmt"
	"sort"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm" // Diperlukan untuk gorm.ErrRecordNotFound di getDestinationTablePKs

	"github.com/arwahdevops/dbsync/internal/config"
	"github.com/arwahdevops/dbsync/internal/db"
	"github.com/arwahdevops/dbsync/internal/metrics"
	"github.com/arwahdevops/dbsync/internal/utils"
)

// Orchestrator mengelola keseluruhan proses sinkronisasi.
type Orchestrator struct {
	srcConn      *db.Connector
	dstConn      *db.Connector
	cfg          *config.Config
	logger       *zap.Logger
	schemaSyncer SchemaSyncerInterface // Menggunakan interface untuk schema syncer
	metrics      *metrics.Store
}

// Pastikan Orchestrator mengimplementasikan interface OrchestratorInterface.
var _ OrchestratorInterface = (*Orchestrator)(nil)

// NewOrchestrator membuat instance Orchestrator baru.
func NewOrchestrator(srcConn, dstConn *db.Connector, cfg *config.Config, logger *zap.Logger, metricsStore *metrics.Store) *Orchestrator {
	return &Orchestrator{
		srcConn:      srcConn,
		dstConn:      dstConn,
		cfg:          cfg,
		logger:       logger.Named("orchestrator"), // Memberi nama pada logger untuk konteks
		schemaSyncer: NewSchemaSyncer( // Inisialisasi SchemaSyncer
			srcConn.DB,
			dstConn.DB,
			srcConn.Dialect,
			dstConn.Dialect,
			logger, // Logger utama akan di-scope lebih lanjut oleh SchemaSyncer
		),
		metrics: metricsStore,
	}
}

// Run memulai dan mengelola keseluruhan proses sinkronisasi database.
// Ini adalah method utama yang dipanggil untuk menjalankan sinkronisasi.
func (f *Orchestrator) Run(ctx context.Context) map[string]SyncResult {
	startTime := time.Now()
	f.logger.Info("Starting database synchronization run",
		zap.String("direction", f.cfg.SyncDirection),
		zap.Int("workers", f.cfg.Workers),
		zap.Int("batch_size", f.cfg.BatchSize),
		zap.String("schema_strategy", string(f.cfg.SchemaSyncStrategy)),
	)
	f.metrics.SyncRunning.Set(1)       // Set metrik bahwa sinkronisasi sedang berjalan
	defer f.metrics.SyncRunning.Set(0) // Pastikan metrik direset saat fungsi selesai

	results := make(map[string]SyncResult) // Peta untuk menyimpan hasil sinkronisasi per tabel

	// 1. Ambil daftar tabel dari sumber, sudah diurutkan secara alfabetis
	allSourceTables, err := f.listSourceTables(ctx)
	if err != nil {
		f.logger.Error("Failed to list source tables, aborting synchronization.", zap.Error(err))
		f.metrics.SyncErrorsTotal.WithLabelValues("list_tables", "").Inc()
		f.metrics.SyncDuration.Observe(time.Since(startTime).Seconds())
		return results // Kembalikan hasil kosong jika tidak bisa mengambil daftar tabel
	}

	if len(allSourceTables) == 0 {
		f.logger.Warn("No tables found in source database to synchronize.")
		f.metrics.SyncDuration.Observe(time.Since(startTime).Seconds())
		return results // Tidak ada tabel, selesai
	}
	f.logger.Info("Found source tables (alphabetical order initially)",
		zap.Int("count", len(allSourceTables)),
		zap.Strings("tables", allSourceTables))

	// 2. Tentukan urutan pemrosesan tabel
	tablesToProcess := allSourceTables // Default ke urutan alfabetis
	// Pengurutan topologis berdasarkan FK hanya diperlukan jika strategi melibatkan perubahan skema
	// dan ada lebih dari satu tabel.
	if (f.cfg.SchemaSyncStrategy == config.SchemaSyncDropCreate || f.cfg.SchemaSyncStrategy == config.SchemaSyncAlter) && len(allSourceTables) > 1 {
		f.logger.Info("Attempting to determine table processing order based on FK dependencies from source DB.")
		// Menggunakan koneksi SUMBER (f.srcConn) untuk mendapatkan dependensi FK
		orderedTables, orderErr := f.getExecutionOrder(ctx, allSourceTables, f.srcConn)
		if orderErr != nil {
			f.logger.Error("Failed to determine FK-based table execution order. Proceeding with alphabetical order. This may cause FK creation errors if 'drop_create' or 'alter' (with new FKs) strategy is used.",
				zap.Error(orderErr),
				zap.Strings("final_processing_order_alphabetical", tablesToProcess)) // tablesToProcess di sini masih allSourceTables
		} else {
			tablesToProcess = orderedTables
			f.logger.Info("FK-based table execution order determined.",
				zap.Strings("final_processing_order_fk_based", tablesToProcess))
		}
	} else if len(allSourceTables) > 1 { // Jika ada >1 tabel tapi tidak perlu urutan FK
		f.logger.Info("Skipping FK-based table ordering (strategy is 'none' or only one table, or other reason).",
			zap.String("schema_strategy", string(f.cfg.SchemaSyncStrategy)),
			zap.Int("table_count", len(allSourceTables)))
	} else { // Jika hanya satu tabel
		f.logger.Info("Only one table found, no special ordering needed.", zap.Strings("table_to_process", tablesToProcess))
	}

	f.logger.Info("Final table processing order established.", zap.Strings("ordered_tables_for_pool", tablesToProcess))

	// 3. Proses tabel menggunakan worker pool
	// Method runTableProcessingPool sekarang ada di orchestrator_pool.go
	results = f.runTableProcessingPool(ctx, tablesToProcess)

	// Selesai
	totalDuration := time.Since(startTime)
	f.logger.Info("Synchronization run finished",
		zap.Duration("total_duration", totalDuration),
		zap.Int("total_tables_processed_or_skipped", len(results)),
	)
	f.metrics.SyncDuration.Observe(totalDuration.Seconds())

	return results
}

// getExecutionOrder mengurutkan tabel berdasarkan dependensi FK (topological sort).
// dbConn harus merupakan koneksi ke database SUMBER.
func (f *Orchestrator) getExecutionOrder(ctx context.Context, tableNames []string, dbConn *db.Connector) ([]string, error) {
	log := f.logger.Named("table-orderer").With(zap.String("dialect_for_fk_deps", dbConn.Dialect))
	log.Info("Fetching FK dependencies to determine execution order.", zap.Int("num_tables_in_scope", len(tableNames)))

	// fkSourceToTargets: map[table_V_yang_punya_FK] -> list dari [table_U_yang_direferensikan_oleh_V]
	// U adalah prasyarat untuk V.
	fkSourceToTargets, err := f.schemaSyncer.GetFKDependencies(ctx, dbConn.DB, dbConn.Dialect, tableNames)
	if err != nil {
		return nil, fmt.Errorf("failed to get FK dependencies for ordering: %w", err)
	}

	if log.Core().Enabled(zap.DebugLevel) {
		debugFkMap := make(map[string][]string)
		sortedTableNamesWithDeps := make([]string, 0, len(fkSourceToTargets))
		for k := range fkSourceToTargets {
			sortedTableNamesWithDeps = append(sortedTableNamesWithDeps, k)
		}
		sort.Strings(sortedTableNamesWithDeps)
		for _, k := range sortedTableNamesWithDeps {
			if len(fkSourceToTargets[k]) > 0 {
				debugFkMap[k] = fkSourceToTargets[k]
			}
		}
		if len(debugFkMap) > 0 {
			log.Debug("Raw FK dependencies (Table V -> [Tables U it references and are prerequisites for V]) within the sync set:",
				zap.Any("fk_source_to_targets_map", debugFkMap))
		} else {
			log.Debug("No FK dependencies found among the set of tables being synced.")
		}
	}

	adj := make(map[string][]string)    // Adjacency list: U_prereq -> [V_dependen]
	inDegree := make(map[string]int) // InDegree[V]: jumlah U yang menjadi prasyarat untuk V
	tableSet := make(map[string]bool)

	for _, tn := range tableNames {
		tableSet[tn] = true
		adj[tn] = []string{}
		inDegree[tn] = 0
	}

	for tableV, referencedTablesU_list := range fkSourceToTargets {
		if !tableSet[tableV] {
			log.Warn("TableV from dependency map not in initial table set, skipping its outgoing FKs for graph.", zap.String("tableV", tableV))
			continue
		}
		for _, tableU_prerequisite := range referencedTablesU_list {
			if !tableSet[tableU_prerequisite] {
				log.Debug("FK target is outside the set of tables being synced. This dependency won't influence internal ordering.",
					zap.String("table_v_with_fk", tableV),
					zap.String("fk_target_table_u_external", tableU_prerequisite))
				continue
			}
			adj[tableU_prerequisite] = append(adj[tableU_prerequisite], tableV)
			inDegree[tableV]++
			if log.Core().Enabled(zap.DebugLevel) {
				log.Debug("Added FK dependency edge for ordering.",
					zap.String("prerequisite_U", tableU_prerequisite),
					zap.String("dependent_V", tableV),
					zap.Int(fmt.Sprintf("new_inDegree_%s", tableV), inDegree[tableV]))
			}
		}
	}

	if log.Core().Enabled(zap.DebugLevel) {
		// Logging untuk In-Degree Map
		sortedInDegreeKeys := make([]string, 0, len(inDegree)); for k := range inDegree { sortedInDegreeKeys = append(sortedInDegreeKeys, k) }; sort.Strings(sortedInDegreeKeys)
		logInDegree := make(map[string]int); for _, k := range sortedInDegreeKeys { logInDegree[k] = inDegree[k] }

		// Logging untuk Adjacency List Map
		sortedAdjKeys := make([]string, 0, len(adj)); for k := range adj { sortedAdjKeys = append(sortedAdjKeys, k) }; sort.Strings(sortedAdjKeys)
		logAdj := make(map[string][]string)
		for _, k := range sortedAdjKeys {
			if len(adj[k]) > 0 { logAdj[k] = adj[k] } // Hanya log adj yang punya dependents
		}
		log.Debug("Constructed graph for topological sort:",
			zap.Any("in_degree_map (V -> count of U's that are prereq for V)", logInDegree),
			zap.Any("adjacency_list_map (U_prereq -> list of V's that depend on U)", logAdj),
		)
	}

	// Algoritma Kahn untuk topological sort
	queue := make([]string, 0)
	for _, tableName := range tableNames { // Iterasi dalam urutan alfabetis (dari listSourceTables)
		if deg, ok := inDegree[tableName]; ok && deg == 0 {
			queue = append(queue, tableName)
		} else if !ok {
			log.Error("Table missing from inDegree map during queue initialization. This indicates a bug in graph setup.", zap.String("table_missing_from_inDegree", tableName))
			// Ini kondisi error, bisa return error atau panic.
			// return nil, fmt.Errorf("table %s missing from inDegree map, graph setup error", tableName)
		}
	}

	if log.Core().Enabled(zap.DebugLevel) && len(queue) > 0 {
		log.Debug("Initial queue for Kahn's algorithm (nodes with in-degree 0, alphabetically sorted):", zap.Strings("initial_queue", queue))
	}

	var sortedOrder []string
	processedCount := 0
	for len(queue) > 0 {
		sort.Strings(queue) // Sort antrian untuk memastikan determinisme jika ada beberapa pilihan
		tableU_processed := queue[0]
		queue = queue[1:]

		sortedOrder = append(sortedOrder, tableU_processed)
		processedCount++

		if log.Core().Enabled(zap.DebugLevel) {
			log.Debug("Processing table from queue (Kahn's algorithm step).",
				zap.String("table_u_processed_from_queue", tableU_processed),
				zap.Int("processed_count_so_far", processedCount),
				zap.Int("current_queue_length_after_pop", len(queue)))
		}

		dependentsV_list := adj[tableU_processed]
		sort.Strings(dependentsV_list) // Sort tetangga untuk pemrosesan yang deterministik

		for _, tableV_dependent := range dependentsV_list {
			if _, ok := inDegree[tableV_dependent]; !ok {
				log.Error("Dependent tableV not found in inDegree map during graph traversal. Bug in setup or processing.",
					zap.String("tableU_prereq_just_processed", tableU_processed),
					zap.String("tableV_dependent_not_in_inDegree_map", tableV_dependent))
				continue
			}
			inDegree[tableV_dependent]--
			if log.Core().Enabled(zap.DebugLevel) {
				log.Debug("Decremented in-degree for dependent table.",
					zap.String("dependent_table_V", tableV_dependent),
					zap.Int("new_in_degree_of_V", inDegree[tableV_dependent]),
					zap.String("prerequisite_U_just_processed", tableU_processed))
			}
			if inDegree[tableV_dependent] == 0 {
				if log.Core().Enabled(zap.DebugLevel) {
					log.Debug("Adding table to queue as its in-degree is now 0.", zap.String("table_v_to_add_to_queue", tableV_dependent))
				}
				queue = append(queue, tableV_dependent)
			}
		}
	}

	if processedCount != len(tableNames) {
		var cycleTables []string
		for _, tableName := range tableNames { // Iterasi dalam urutan alfabetis
			if deg, ok := inDegree[tableName]; ok && deg > 0 {
				cycleTables = append(cycleTables, tableName)
			}
		}
		log.Error("Circular dependency detected among tables. Cannot determine a strict processing order. Will fallback to alphabetical order for remaining/all tables.",
			zap.Int("total_tables_in_scope", len(tableNames)),
			zap.Int("tables_successfully_ordered_topologically", processedCount),
			zap.Strings("tables_involved_in_or_affected_by_cycle (remaining_in_degree > 0, alphabetically sorted)", cycleTables))
		return nil, fmt.Errorf("circular FK dependency detected. Tables (or part of cycle) with remaining in-degrees > 0: %v", cycleTables)
	}

	log.Info("Successfully determined table execution order based on FK dependencies.", zap.Strings("topologically_sorted_tables", sortedOrder))
	return sortedOrder, nil
}

// listSourceTables mengambil daftar tabel dari database sumber.
// Query di dalam fungsi ini sudah mengurutkan tabel secara alfabetis.
func (f *Orchestrator) listSourceTables(ctx context.Context) ([]string, error) {
	var tables []string
	var err error
	dbCfg := f.cfg.SrcDB
	dialect := f.srcConn.Dialect
	log := f.logger.With(zap.String("dialect", dialect), zap.String("database", dbCfg.DBName), zap.String("action", "listSourceTables"))
	log.Info("Listing user tables from source database")

	dbGormInstance := f.srcConn.DB.WithContext(ctx)

	switch dialect {
	case "mysql":
		query := `SELECT table_name FROM information_schema.tables
				  WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'
				  ORDER BY table_name;`
		err = dbGormInstance.Raw(query).Scan(&tables).Error
	case "postgres":
		query := `SELECT table_name FROM information_schema.tables
				  WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'
				  AND table_name NOT LIKE 'pg_%' AND table_name NOT LIKE 'sql_%'
				  ORDER BY table_name;`
		err = dbGormInstance.Raw(query).Scan(&tables).Error
	case "sqlite":
		query := `SELECT name FROM sqlite_master
				  WHERE type='table' AND name NOT LIKE 'sqlite_%'
				  ORDER BY name;`
		err = dbGormInstance.Raw(query).Scan(&tables).Error
	default:
		return nil, fmt.Errorf("unsupported dialect for listing tables: %s", dialect)
	}

	if err != nil {
		if ctx.Err() != nil {
			log.Error("Context cancelled during table listing", zap.Error(ctx.Err()), zap.NamedError("db_error", err))
			return nil, fmt.Errorf("context cancelled during table listing: %w (db error: %v)", ctx.Err(), err)
		}
		log.Error("Failed to execute list tables query", zap.Error(err))
		return nil, fmt.Errorf("failed to list tables for database '%s' (%s): %w", dbCfg.DBName, dialect, err)
	}

	log.Debug("Table listing successful, already sorted alphabetically by query.", zap.Int("table_count", len(tables)))
	return tables, nil
}

// getDestinationTablePKs mengambil nama kolom kunci primer untuk tabel tujuan.
// Ini penting untuk operasi upsert (ON CONFLICT).
func (f *Orchestrator) getDestinationTablePKs(ctx context.Context, tableName string) ([]string, error) {
	log := f.logger.With(zap.String("table", tableName), zap.String("dialect", f.dstConn.Dialect), zap.String("action", "getDestinationTablePKs"))
	log.Debug("Fetching primary keys for destination table")

	var pks []string
	var err error

	dbGormInstance := f.dstConn.DB.WithContext(ctx)

	switch f.dstConn.Dialect {
	case "postgres":
		query := `
            SELECT kcu.column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = current_schema()
              AND tc.table_name = $1
            ORDER BY kcu.ordinal_position;`
		err = dbGormInstance.Raw(query, tableName).Scan(&pks).Error
	case "mysql":
		query := `
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = ?
              AND COLUMN_KEY = 'PRI'
            ORDER BY ORDINAL_POSITION;`
		err = dbGormInstance.Raw(query, tableName).Scan(&pks).Error
	case "sqlite":
		var columnsInfo []struct {
			Name string `gorm:"column:name"`
			Pk   int    `gorm:"column:pk"` // pk > 0 jika bagian dari PK, urutan 1-based
		}
		err = dbGormInstance.Raw(fmt.Sprintf("PRAGMA table_info(%s);", utils.QuoteIdentifier(tableName, "sqlite"))).Scan(&columnsInfo).Error
		if err == nil {
			pkMap := make(map[int]string)
			maxPkOrder := 0
			for _, col := range columnsInfo {
				if col.Pk > 0 {
					pkMap[col.Pk] = col.Name
					if col.Pk > maxPkOrder { maxPkOrder = col.Pk }
				}
			}
			if maxPkOrder > 0 {
				pks = make([]string, maxPkOrder)
				allPksFound := true
				for i := 1; i <= maxPkOrder; i++ {
					if pkName, ok := pkMap[i]; ok {
						pks[i-1] = pkName // Konversi ke 0-based index
					} else {
						log.Error("Inconsistency in SQLite PK ordinal numbers when constructing PK list. PK component missing.",
							zap.String("table", tableName),
							zap.Int("max_pk_order_found", maxPkOrder),
							zap.Int("missing_pk_order_in_map", i),
							zap.Any("pk_map_from_pragma", pkMap))
						allPksFound = false
						break
					}
				}
				if !allPksFound {
					return nil, fmt.Errorf("inconsistency in SQLite PK ordinal numbers for table %s, missing Pk order component", tableName)
				}
			} else {
				pks = []string{}
			}
		}
	default:
		return nil, fmt.Errorf("getDestinationTablePKs: unsupported destination dialect %s", f.dstConn.Dialect)
	}

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			log.Warn("No primary key definition rows found by query for destination table (or table does not exist/no PK).", zap.String("table", tableName))
			return []string{}, nil
		}
		return nil, fmt.Errorf("failed to query PKs for destination table '%s' (%s): %w", tableName, f.dstConn.Dialect, err)
	}

    if len(pks) == 0 {
        log.Warn("No primary key columns identified for destination table. Upsert behavior might be affected if PKs are expected for conflict resolution.", zap.String("table", tableName))
    } else {
        log.Debug("Destination primary keys fetched.", zap.String("table", tableName), zap.Strings("pk_columns_ordered", pks))
    }
	return pks, nil
}
