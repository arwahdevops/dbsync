package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Store holds the Prometheus metrics collectors.
type Store struct {
	Registry                *prometheus.Registry // Use a custom registry
	SyncRunning             prometheus.Gauge
	SyncDuration            prometheus.Histogram
	TableSyncDuration       *prometheus.HistogramVec
	TableSyncSuccessTotal   *prometheus.CounterVec
	RowsSyncedTotal         *prometheus.CounterVec
	BatchesProcessedTotal   *prometheus.CounterVec
	BatchProcessingDuration *prometheus.HistogramVec
	BatchErrorsTotal        *prometheus.CounterVec
	SyncErrorsTotal         *prometheus.CounterVec
	DBConnections           *prometheus.GaugeVec // Example: Track connections per DB
}

// NewMetricsStore creates and registers Prometheus metrics.
func NewMetricsStore() *Store {
	registry := prometheus.NewRegistry() // Create a non-global registry

	store := &Store{
		Registry: registry,
		SyncRunning: promauto.With(registry).NewGauge(prometheus.GaugeOpts{
			Name: "dbsync_up",
			Help: "Indicates if the dbsync process is currently running (1 = running, 0 = not running).",
		}),
		SyncDuration: promauto.With(registry).NewHistogram(prometheus.HistogramOpts{
			Name:    "dbsync_run_duration_seconds",
			Help:    "Duration of the entire dbsync run.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 15), // Adjust buckets as needed (1s to ~9h)
		}),
		TableSyncDuration: promauto.With(registry).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "dbsync_table_sync_duration_seconds",
			Help:    "Duration histogram for synchronizing individual tables.",
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 16), // Buckets from 100ms to ~1.8 hours
		}, []string{"table"}), // Label by table name
		TableSyncSuccessTotal: promauto.With(registry).NewCounterVec(prometheus.CounterOpts{
			Name: "dbsync_table_sync_success_total",
			Help: "Total number of tables successfully synchronized (including data and constraints).",
		}, []string{"table"}),
		RowsSyncedTotal: promauto.With(registry).NewCounterVec(prometheus.CounterOpts{
			Name: "dbsync_rows_synced_total",
			Help: "Total number of rows synchronized, labeled by table.",
		}, []string{"table"}),
		BatchesProcessedTotal: promauto.With(registry).NewCounterVec(prometheus.CounterOpts{
			Name: "dbsync_batches_processed_total",
			Help: "Total number of batches processed, labeled by table.",
		}, []string{"table"}),
		BatchProcessingDuration: promauto.With(registry).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "dbsync_batch_processing_duration_seconds",
			Help:    "Duration histogram for processing individual batches (insert/upsert).",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 15), // Buckets from 10ms to ~5min
		}, []string{"table", "status"}), // Labels: table, status (success, success_retry, failure_*)
		BatchErrorsTotal: promauto.With(registry).NewCounterVec(prometheus.CounterOpts{
			Name: "dbsync_batch_errors_total",
			Help: "Total number of batch processing errors after retries.",
		}, []string{"table"}),
		SyncErrorsTotal: promauto.With(registry).NewCounterVec(prometheus.CounterOpts{
			Name: "dbsync_errors_total",
			Help: "Total number of errors encountered during sync, labeled by type and table.",
		}, []string{"type", "table"}), // Types: list_tables, schema_analysis, schema_execution, data_sync, constraint_apply, connection, connection_cancelled, connection_failed
		DBConnections: promauto.With(registry).NewGaugeVec(prometheus.GaugeOpts{
			Name: "dbsync_db_connections_active",
			Help: "Number of active database connections (requires integration with GORM stats).",
		}, []string{"db_alias"}), // Labels: source, destination
	}

	// TODO: Integrate DBConnections with GORM's DB.Stats() if needed periodically
	// Example: Periodically call updateDBStats(srcConn, "source", store)

	return store
}

// Example function to update DB stats (call this periodically if needed)
/*
func updateDBStats(conn *db.Connector, alias string, store *Store) {
	if conn == nil {
		return
	}
	stats, err := conn.DB.DB()
	if err != nil {
		logger.Log.Warn("Failed to get DB stats", zap.String("db_alias", alias), zap.Error(err))
		return
	}
	dbStats := stats.Stats()
	store.DBConnections.WithLabelValues(alias).Set(float64(dbStats.OpenConnections))
	// Could add gauges for Idle, WaitCount, WaitDuration etc.
}
*/