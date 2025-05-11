package sync

import (
	"context"
	"fmt"
	"sort" // Diperlukan untuk mengurutkan map keys untuk logging deterministik (opsional)
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

// Pastikan Orchestrator mengimplementasikan interface (jika ada)
var _ OrchestratorInterface = (*Orchestrator)(nil)

func NewOrchestrator(srcConn, dstConn *db.Connector, cfg *config.Config, logger *zap.Logger, metricsStore *metrics.Store) *Orchestrator {
	return &Orchestrator{
		srcConn:      srcConn,
		dstConn:      dstConn,
		cfg:          cfg,
		logger:       logger.Named("orchestrator"),
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
	f.logger.Info("Starting database synchronization run",
		zap.String("direction", f.cfg.SyncDirection),
		zap.Int("workers", f.cfg.Workers),
		zap.Int("batch_size", f.cfg.BatchSize),
		zap.String("schema_strategy", string(f.cfg.SchemaSyncStrategy)),
	)
	f.metrics.SyncRunning.Set(1)
	defer f.metrics.SyncRunning.Set(0)

	results := make(map[string]SyncResult)
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
	f.logger.Info("Found source tables", zap.Int("count", len(allSourceTables)), zap.Strings("tables", allSourceTables))

	var tablesToProcess []string
	if f.cfg.SchemaSyncStrategy == config.SchemaSyncDropCreate || f.cfg.SchemaSyncStrategy == config.SchemaSyncAlter {
		// Untuk drop_create, urutan sangat penting. Untuk alter, juga bisa penting jika ada ADD FK baru.
		// Kita akan mengurutkan berdasarkan dependensi FK dari SUMBER.
		orderedTables, orderErr := f.getExecutionOrder(ctx, allSourceTables, f.srcConn)
		if orderErr != nil {
			f.logger.Error("Failed to determine table execution order due to FK dependencies. Proceeding with alphabetical order (THIS IS RISKY and may lead to errors).",
				zap.Error(orderErr))
			// Fallback: gunakan urutan alfabetis (seperti sebelumnya), tapi ini berisiko.
			// Atau, bisa juga return error di sini jika urutan sangat kritis.
			// Untuk saat ini, kita log error dan lanjutkan dengan alphabetical.
			tablesToProcess = allSourceTables // Sudah diurutkan alfabetis oleh listSourceTables
		} else {
			tablesToProcess = orderedTables
			f.logger.Info("Table processing order determined by FK dependencies.", zap.Strings("ordered_tables", tablesToProcess))
		}
	} else {
		// Untuk strategi 'none', urutan tabel mungkin tidak sekritis itu untuk DDL.
		tablesToProcess = allSourceTables
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
// Tabel yang tidak memiliki dependensi keluar (atau hanya ke dirinya sendiri) akan lebih dulu.
func (f *Orchestrator) getExecutionOrder(ctx context.Context, tableNames []string, dbConn *db.Connector) ([]string, error) {
	log := f.logger.Named("table-orderer")
	log.Info("Determining table execution order based on FK dependencies from source.", zap.String("dialect", dbConn.Dialect))

	// dependencies: map[table_name] -> list of tables it has FKs to (outgoing dependencies)
	dependencies, err := f.schemaSyncer.GetFKDependencies(ctx, dbConn.DB, dbConn.Dialect, tableNames)
	if err != nil {
		return nil, fmt.Errorf("failed to get FK dependencies: %w", err)
	}

	// Validasi bahwa semua tabel dari tableNames ada di map dependencies
	for _, tn := range tableNames {
		if _, ok := dependencies[tn]; !ok {
			// Ini seharusnya tidak terjadi jika GetFKDependencies menginisialisasi semua
			log.Warn("Table from initial list not found in dependency map, adding with no dependencies.", zap.String("table", tn))
			dependencies[tn] = []string{}
		}
	}


	// reverseDependencies: map[table_name] -> list of tables that have FKs to it (incoming dependencies for processing order)
	reverseDependencies := make(map[string][]string)
	inDegree := make(map[string]int)

	for _, tableName := range tableNames {
		// Inisialisasi untuk semua tabel, bahkan yang tidak punya FK keluar atau masuk
		if _, ok := reverseDependencies[tableName]; !ok {
			reverseDependencies[tableName] = []string{}
		}
		if _, ok := inDegree[tableName]; !ok {
			inDegree[tableName] = 0
		}
	}


	for depender, depList := range dependencies {
		// `depender` adalah tabel yang memiliki FK.
		// `dep` adalah tabel yang dirujuk oleh FK dari `depender`.
		// Artinya, `dep` harus ada sebelum `depender` bisa dibuat dengan FK-nya.
		// Jadi, edge di graf dependensi proses adalah `dep` -> `depender`.
		// `inDegree[depender]` adalah jumlah tabel yang `depender` rujuk.
		inDegree[depender] = len(depList)
		for _, dependedOn := range depList {
			// `dependedOn` adalah tabel yang menjadi prasyarat.
			// `depender` adalah tabel yang akan bergantung pada `dependedOn`.
			// Jadi, `dependents[dependedOn]` harus berisi `depender`.
			if _, ok := reverseDependencies[dependedOn]; !ok {
				// Jika dependedOn tidak ada di tableNames (FK ke tabel di luar scope), abaikan untuk reverse graph.
				// Tapi inDegree untuk depender tetap dihitung.
				log.Debug("FK target is outside the set of tables being synced. Ignoring for reverse dependency graph construction.",
					zap.String("depender_table", depender),
					zap.String("fk_target_table", dependedOn))
				continue
			}
			reverseDependencies[dependedOn] = append(reverseDependencies[dependedOn], depender)
		}
	}

	if f.cfg.DebugMode { // Log detail graf hanya jika debug
		// Urutkan key map untuk logging yang konsisten (opsional)
		sortedInDegreeKeys := make([]string, 0, len(inDegree))
		for k := range inDegree { sortedInDegreeKeys = append(sortedInDegreeKeys, k) }
		sort.Strings(sortedInDegreeKeys)
		logInDegree := make(map[string]int)
		for _, k := range sortedInDegreeKeys { logInDegree[k] = inDegree[k] }

		sortedRevDepKeys := make([]string, 0, len(reverseDependencies))
		for k := range reverseDependencies { sortedRevDepKeys = append(sortedRevDepKeys, k) }
		sort.Strings(sortedRevDepKeys)
		logRevDep := make(map[string][]string)
		for _, k := range sortedRevDepKeys { logRevDep[k] = reverseDependencies[k] }

		log.Debug("Constructed dependency graph details for topological sort:",
			zap.Any("outgoing_fk_dependencies (table_X -> [tables_it_refs])", dependencies),
			zap.Any("effective_in_degree_for_processing_order (table_X_needs_these_many_refs_to_be_processed_first)", logInDegree),
			zap.Any("reverse_dependencies (table_X_is_refd_by -> [these_tables])", logRevDep),
		)
	}


	queue := make([]string, 0)
	for tableName, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, tableName)
		}
	}
	// Urutkan antrian awal secara alfabetis untuk pemrosesan yang lebih deterministik jika ada banyak node dengan in-degree 0
	sort.Strings(queue)


	var sortedOrder []string
	for len(queue) > 0 {
		table := queue[0]
		queue = queue[1:]
		sortedOrder = append(sortedOrder, table)

		// Untuk setiap tabel `dependentTable` yang merujuk ke `table` yang baru saja diproses:
		dependentsOfTable := reverseDependencies[table]
		// Urutkan dependentsOfTable secara alfabetis untuk konsistensi jika ada beberapa yang in-degree nya jadi 0 bersamaan
		sort.Strings(dependentsOfTable)

		for _, dependentTable := range dependentsOfTable {
			inDegree[dependentTable]--
			if inDegree[dependentTable] == 0 {
				queue = append(queue, dependentTable)
				// Urutkan kembali antrian setiap kali item baru ditambahkan untuk menjaga determinisme
				// Ini bisa jadi mahal, alternatifnya adalah hanya mengurutkan di awal.
				// Untuk kebanyakan kasus, urutan antrian tidak krusial selama topologinya benar.
				// sort.Strings(queue) // Opsional, untuk determinisme absolut
			}
		}
	}

	if len(sortedOrder) != len(tableNames) {
		var cycleTables []string
		for tableName, degree := range inDegree {
			if degree > 0 {
				cycleTables = append(cycleTables, tableName)
			}
		}
		sort.Strings(cycleTables)
		log.Error("Circular dependency detected among tables. Cannot determine a strict processing order.",
			zap.Int("total_tables_in_scope", len(tableNames)),
			zap.Int("tables_in_sorted_order", len(sortedOrder)),
			zap.Strings("tables_involved_in_or_affected_by_cycle", cycleTables))
		return nil, fmt.Errorf("circular FK dependency detected, cannot establish processing order. Tables potentially in cycle: %v", cycleTables)
	}

	log.Info("Successfully determined table execution order.", zap.Strings("ordered_tables", sortedOrder))
	return sortedOrder, nil
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
				  ORDER BY table_name;` // MySQL sudah mengurutkan
		err = db.Raw(query).Scan(&tables).Error
	case "postgres":
		query := `SELECT table_name FROM information_schema.tables
				  WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'
				  AND table_name NOT LIKE 'pg_%' AND table_name NOT LIKE 'sql_%'
				  ORDER BY table_name;` // Postgres juga mengurutkan
		err = db.Raw(query).Scan(&tables).Error
	case "sqlite":
		query := `SELECT name FROM sqlite_master
				  WHERE type='table' AND name NOT LIKE 'sqlite_%'
				  ORDER BY name;` // SQLite juga mengurutkan
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

	// Tidak perlu sort lagi di sini karena query sudah ORDER BY table_name
	log.Debug("Table listing successful, already sorted alphabetically by query.", zap.Int("table_count", len(tables)))
	return tables, nil
}

// getDestinationTablePKs mengambil Primary Key untuk tabel di database tujuan.
// Ini adalah method privat dari Orchestrator.
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
			if maxPkOrder > 0 { // Hanya buat slice jika ada PK
				pks = make([]string, maxPkOrder)
				for i := 1; i <= maxPkOrder; i++ {
					if pkName, ok := pkMap[i]; ok {
						pks[i-1] = pkName
					} else {
						// Ini tidak seharusnya terjadi jika Pk continuous
						return nil, fmt.Errorf("inconsistency in SQLite PK ordinal numbers for table %s, missing Pk order %d", tableName, i)
					}
				}
			} else {
				pks = []string{} // Tidak ada PK
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
