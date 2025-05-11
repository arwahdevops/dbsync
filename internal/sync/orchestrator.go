// internal/sync/orchestrator.go

package sync

import (
	"context"
	"fmt"
	"sort"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

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
	schemaSyncer SchemaSyncerInterface
	metrics      *metrics.Store
}

// Pastikan Orchestrator mengimplementasikan interface (jika ada)
var _ OrchestratorInterface = (*Orchestrator)(nil)

func NewOrchestrator(srcConn, dstConn *db.Connector, cfg *config.Config, logger *zap.Logger, metricsStore *metrics.Store) *Orchestrator {
	return &Orchestrator{
		srcConn:      srcConn,
		dstConn:      dstConn,
		cfg:          cfg,
		logger:       logger.Named("orchestrator"),
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
	// listSourceTables sudah mengurutkan tabel secara alfabetis
	allSourceTables, err := f.listSourceTables(ctx)
	if err != nil {
		f.logger.Error("Failed to list source tables", zap.Error(err))
		f.metrics.SyncErrorsTotal.WithLabelValues("list_tables", "").Inc()
		f.metrics.SyncDuration.Observe(time.Since(startTime).Seconds())
		return results
	}

	if len(allSourceTables) == 0 {
		f.logger.Warn("No tables found in source database to synchronize")
		f.metrics.SyncDuration.Observe(time.Since(startTime).Seconds())
		return results
	}
	f.logger.Info("Found source tables (alphabetical order initially)", zap.Int("count", len(allSourceTables)), zap.Strings("tables", allSourceTables))

	tablesToProcess := allSourceTables // Default ke urutan alfabetis
	// Hanya lakukan pengurutan topologis jika strategi memerlukannya dan ada >1 tabel
	if (f.cfg.SchemaSyncStrategy == config.SchemaSyncDropCreate || f.cfg.SchemaSyncStrategy == config.SchemaSyncAlter) && len(allSourceTables) > 1 {
		f.logger.Info("Attempting to determine table processing order based on FK dependencies from source DB.")
		// Menggunakan koneksi SUMBER (f.srcConn) untuk mendapatkan dependensi FK
		orderedTables, orderErr := f.getExecutionOrder(ctx, allSourceTables, f.srcConn)
		if orderErr != nil {
			// Jika gagal mengurutkan (misalnya karena siklus), log error dan putuskan bagaimana melanjutkan.
			// Untuk produksi, mungkin lebih aman gagal jika urutan sangat krusial.
			// Untuk saat ini, kita log error dan fallback ke urutan alfabetis dengan peringatan.
			f.logger.Error("Failed to determine FK-based table execution order. Proceeding with alphabetical order. This may cause FK creation errors if 'drop_create' or 'alter' (with new FKs) strategy is used.",
				zap.Error(orderErr),
				zap.Strings("fallback_alphabetical_order", allSourceTables))
			// tablesToProcess tetap allSourceTables (alphabetical)
		} else {
			tablesToProcess = orderedTables
			f.logger.Info("Table processing order determined by FK dependencies.", zap.Strings("ordered_tables", tablesToProcess))
		}
	} else if len(allSourceTables) > 1 { // Log jika ordering diskip padahal ada >1 tabel
		f.logger.Info("Skipping FK-based table ordering.",
			zap.String("reason", "Strategy is 'none' or other reason."),
			zap.String("schema_strategy", string(f.cfg.SchemaSyncStrategy)),
			zap.Int("table_count", len(allSourceTables)))
	}


	results = f.runTableProcessingPool(ctx, tablesToProcess)

	totalDuration := time.Since(startTime)
	f.logger.Info("Synchronization run finished",
		zap.Duration("total_duration", totalDuration),
		zap.Int("total_tables_processed_or_skipped", len(results)),
	)
	f.metrics.SyncDuration.Observe(totalDuration.Seconds())

	return results
}

// getExecutionOrder mengurutkan tabel berdasarkan dependensi FK (topological sort).
// Graf: node = tabel. Edge U -> V jika V memiliki FK ke U (U harus ada sebelum V).
// InDegree[V] = jumlah tabel U yang V punya FK ke (jumlah prasyarat untuk V).
func (f *Orchestrator) getExecutionOrder(ctx context.Context, tableNames []string, dbConn *db.Connector) ([]string, error) {
	log := f.logger.Named("table-orderer").With(zap.String("dialect", dbConn.Dialect))
	log.Info("Fetching FK dependencies to determine execution order.")

	// fkSourceToTargets: map[table_V_with_FK] -> list of tables U_referenced_by_V
	fkSourceToTargets, err := f.schemaSyncer.GetFKDependencies(ctx, dbConn.DB, dbConn.Dialect, tableNames)
	if err != nil {
		return nil, fmt.Errorf("failed to get FK dependencies for ordering: %w", err)
	}

	// Adjacency list untuk graf: map[table_U_prerequisite] -> list of tables V_that_depend_on_U
	adj := make(map[string][]string)
	inDegree := make(map[string]int) // inDegree[V] = jumlah U yang menjadi prasyarat V
	tableSet := make(map[string]bool)

	for _, tn := range tableNames {
		tableSet[tn] = true
		adj[tn] = []string{}
		inDegree[tn] = 0
	}

	if f.cfg.DebugMode {
		// Log fkSourceToTargets dengan key yang diurutkan untuk determinisme
		sortedFkKeys := make([]string, 0, len(fkSourceToTargets))
		for k := range fkSourceToTargets { sortedFkKeys = append(sortedFkKeys, k) }
		sort.Strings(sortedFkKeys)
		logFkDeps := make(map[string][]string)
		for _, k := range sortedFkKeys { logFkDeps[k] = fkSourceToTargets[k] }
		log.Debug("Raw FK dependencies (Table V -> [Tables U it references])", zap.Any("fk_source_to_targets", logFkDeps))
	}

	// Bangun graf dan hitung in-degree
	for tableV, referencedTablesU := range fkSourceToTargets {
		if !tableSet[tableV] { // Seharusnya selalu ada jika fkSourceToTargets dari tableNames
			log.Warn("TableV from dependency map not in initial table set, skipping its outgoing FKs for graph.", zap.String("tableV", tableV))
			continue
		}
		// tableV memiliki FK ke setiap tableU dalam referencedTablesU.
		// Ini berarti tableU adalah prasyarat untuk tableV.
		// Edge di graf dependensi proses adalah tableU -> tableV.
		for _, tableU := range referencedTablesU {
			if !tableSet[tableU] {
				// Jika tabel yang direferensikan (tableU) tidak ada dalam daftar sinkronisasi,
				// maka dependensi ini tidak relevan untuk pengurutan *internal* set tabel ini.
				log.Debug("FK target is outside the set of tables being synced. Ignoring for ordering graph construction.",
					zap.String("table_v_with_fk", tableV),
					zap.String("fk_target_table_u_external", tableU))
				continue // Abaikan edge ini
			}
			// Tambahkan edge: tableU -> tableV
			adj[tableU] = append(adj[tableU], tableV)
			inDegree[tableV]++ // Tambah in-degree untuk tableV
		}
	}

	if f.cfg.DebugMode {
		// Log inDegree dan adj dengan key yang diurutkan
		sortedInDegreeKeys := make([]string, 0, len(inDegree)); for k := range inDegree { sortedInDegreeKeys = append(sortedInDegreeKeys, k) }; sort.Strings(sortedInDegreeKeys)
		logInDegree := make(map[string]int); for _, k := range sortedInDegreeKeys { logInDegree[k] = inDegree[k] }

		sortedAdjKeys := make([]string, 0, len(adj)); for k := range adj { sortedAdjKeys = append(sortedAdjKeys, k) }; sort.Strings(sortedAdjKeys)
		logAdj := make(map[string][]string); for _, k := range sortedAdjKeys { logAdj[k] = adj[k] }

		log.Debug("Constructed graph for topological sort:",
			zap.Any("in_degree (V -> count of U's that are prereq for V)", logInDegree),
			zap.Any("adjacency_list (U -> list of V's that depend on U)", logAdj),
		)
	}

	// Inisialisasi antrian dengan node ber-in-degree 0
	// Iterasi tableNames (yang sudah diurutkan alfabetis) untuk antrian awal yang deterministik
	queue := make([]string, 0)
	for _, tableName := range tableNames {
		if deg, ok := inDegree[tableName]; ok && deg == 0 {
			queue = append(queue, tableName)
		} else if !ok {
			// Seharusnya semua tabel ada di inDegree map setelah inisialisasi
			log.Error("Table missing from inDegree map during queue initialization. This is a bug.", zap.String("table", tableName))
		}
	}
	// Antrian awal tidak perlu di-sort lagi karena sumbernya (tableNames) sudah di-sort.

	if f.cfg.DebugMode && len(queue) > 0 {
		log.Debug("Initial queue for Kahn's algorithm (nodes with in-degree 0):", zap.Strings("queue", queue))
	}


	var sortedOrder []string
	processedCount := 0
	for len(queue) > 0 {
		// Untuk konsistensi jika beberapa item di antrian bisa diproses,
		// dan untuk kasus di mana item ditambahkan ke antrian dalam urutan yang tidak terduga,
		// sortir antrian sebelum mengambil elemen.
		sort.Strings(queue)

		tableU := queue[0] // Ambil dari depan
		queue = queue[1:]  // Hapus dari depan

		sortedOrder = append(sortedOrder, tableU)
		processedCount++

		if f.cfg.DebugMode {
			log.Debug("Processing table from queue (Kahn's)", zap.String("table_u", tableU), zap.Int("processed_count", processedCount))
		}

		// Kurangi in-degree dari semua node yang bergantung pada tableU
		// Urutkan adj[tableU] untuk pemrosesan yang deterministik
		dependentsV := adj[tableU]
		sort.Strings(dependentsV) // Penting untuk determinisme jika beberapa jadi 0

		for _, tableV := range dependentsV {
			if _, ok := inDegree[tableV]; !ok {
				// Ini seharusnya tidak terjadi jika semua tabel sudah diinisialisasi di inDegree
				log.Error("Dependent tableV not found in inDegree map. This is a bug.", zap.String("tableU_prereq", tableU), zap.String("tableV_dependent", tableV))
				continue
			}
			inDegree[tableV]--
			if f.cfg.DebugMode {
				log.Debug("Decremented in-degree", zap.String("table_v_dependent_on_u", tableV), zap.Int("new_in_degree", inDegree[tableV]), zap.String("prereq_u_just_processed", tableU))
			}
			if inDegree[tableV] == 0 {
				if f.cfg.DebugMode {
					log.Debug("Adding to queue as in-degree is now 0", zap.String("table_v_to_add", tableV))
				}
				queue = append(queue, tableV)
			}
		}
	}

	if processedCount != len(tableNames) {
		var cycleTables []string
		// Iterasi tableNames (input asli) untuk menemukan yang tidak terproses
		for _, tableName := range tableNames {
			if deg, ok := inDegree[tableName]; ok && deg > 0 {
				cycleTables = append(cycleTables, tableName)
			} else if !ok {
				// Jika tabel tidak ada di inDegree, itu masalah inisialisasi.
				// Tapi jika ada dan degree bukan >0, dia seharusnya sudah diproses atau in-degree awalnya 0.
			}
		}
		sort.Strings(cycleTables) // Laporkan secara konsisten
		log.Error("Circular dependency detected among tables. Cannot determine a strict processing order.",
			zap.Int("total_tables_in_scope", len(tableNames)),
			zap.Int("tables_in_sorted_order", processedCount),
			zap.Strings("tables_involved_in_or_affected_by_cycle (remaining_in_degree > 0)", cycleTables))
		return nil, fmt.Errorf("circular FK dependency detected. Tables in/affected by cycle: %v", cycleTables)
	}

	log.Info("Successfully determined table execution order.", zap.Strings("ordered_tables", sortedOrder))
	return sortedOrder, nil
}


// listSourceTables mengambil daftar tabel dari database sumber.
// Query sudah mengurutkan berdasarkan nama tabel.
func (f *Orchestrator) listSourceTables(ctx context.Context) ([]string, error) {
	var tables []string
	var err error
	dbCfg := f.cfg.SrcDB
	dialect := f.srcConn.Dialect
	log := f.logger.With(zap.String("dialect", dialect), zap.String("database", dbCfg.DBName), zap.String("action", "listSourceTables"))
	log.Info("Listing user tables from source database")

	db := f.srcConn.DB.WithContext(ctx)

	switch dialect {
	case "mysql":
		query := `SELECT table_name FROM information_schema.tables
				  WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'
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

// getDestinationTablePKs (tetap sama seperti sebelumnya)
func (f *Orchestrator) getDestinationTablePKs(ctx context.Context, tableName string) ([]string, error) {
	log := f.logger.With(zap.String("table", tableName), zap.String("dialect", f.dstConn.Dialect), zap.String("action", "getDestinationTablePKs"))
	log.Debug("Fetching primary keys for destination table")

	var pks []string
	var err error

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
              AND tc.table_schema = current_schema()
              AND tc.table_name = $1
            ORDER BY kcu.ordinal_position;`
		err = db.Raw(query, tableName).Scan(&pks).Error
	case "mysql":
		query := `
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = ?
              AND COLUMN_KEY = 'PRI'
            ORDER BY ORDINAL_POSITION;`
		err = db.Raw(query, tableName).Scan(&pks).Error
	case "sqlite":
		var columnsInfo []struct {
			Name string `gorm:"column:name"`
			Pk   int    `gorm:"column:pk"`
		}
		err = db.Raw(fmt.Sprintf("PRAGMA table_info(%s);", utils.QuoteIdentifier(tableName, "sqlite"))).Scan(&columnsInfo).Error
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
						pks[i-1] = pkName
					} else {
						log.Error("Inconsistency in SQLite PK ordinal numbers", zap.String("table", tableName), zap.Int("missing_pk_order", i), zap.Any("pk_map", pkMap))
						allPksFound = false
						break
					}
				}
				if !allPksFound {
					return nil, fmt.Errorf("inconsistency in SQLite PK ordinal numbers for table %s, missing Pk order", tableName)
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
			log.Warn("No primary key rows found by query for destination table.", zap.Error(err))
			return []string{}, nil
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

// --- orchestrator_pool.go dan orchestrator_table_processor.go TIDAK PERLU DIUBAH ---
// --- orchestrator_data_sync.go TIDAK PERLU DIUBAH untuk logika pengurutan tabel ---
