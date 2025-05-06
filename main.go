package main

import (
	"context"
	"fmt"
	stdlog "log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/caarlos0/env/v8"
	"github.com/joho/godotenv"
	"go.uber.org/zap"

	"github.com/arwahdevops/dbsync/internal/config"
	"github.com/arwahdevops/dbsync/internal/db"
	"github.com/arwahdevops/dbsync/internal/logger"
	"github.com/arwahdevops/dbsync/internal/metrics"
	"github.com/arwahdevops/dbsync/internal/server"
	projectSync "github.com/arwahdevops/dbsync/internal/sync" // Impor paket sync internal dengan alias 'projectSync'
)

func main() {
	// 1. Load environment variables (.env overrides)
	if err := godotenv.Overload(".env"); err != nil {
		// Gunakan standard log di sini karena Zap belum tentu siap
		stdlog.Printf("Warning: Could not load .env file: %v. Relying on environment variables.\n", err)
	}

	// 2. Initial config loading to get logging settings
	// Load minimal config just for logger setup first.
	preCfg := &struct {
		EnableJsonLogging bool `env:"ENABLE_JSON_LOGGING" envDefault:"false"`
		DebugMode         bool `env:"DEBUG_MODE" envDefault:"false"` // Add DEBUG_MODE for logger
	}{}
	// Gunakan Parse dari paket env yang diimpor
	// Ignore error here, use defaults if parsing fails for pre-config
	_ = env.Parse(preCfg)

	// 3. Initialize Zap logger based on pre-config
	if err := logger.Init(preCfg.DebugMode, preCfg.EnableJsonLogging); err != nil {
		stdlog.Fatalf("Failed to initialize logger: %v", err) // Use standard log
	}
	// Defer logger sync only if initialization was successful
	defer func() {
		_ = logger.Log.Sync() // Ignore sync error on exit
	}()

	// 4. Load and validate full configuration using Zap for logging errors
	cfg, err := config.Load()
	if err != nil {
		logger.Log.Fatal("Configuration loading error", zap.Error(err)) // Use Zap Fatal
	}
	logLoadedConfig(cfg) // Log config safely

	// 5. Setup context for graceful shutdown
	// Handle SIGINT (Ctrl+C) and SIGTERM (termination signal)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop() // Ensure stop() is called to release resources associated with the context

	// 6. Initialize Metrics Store
	metricsStore := metrics.NewMetricsStore()

	// 7. Initialize database connections with retry (concurrently)
	logger.Log.Info("Connecting to databases...")
	var srcConn, dstConn *db.Connector
	var dbWg sync.WaitGroup // Gunakan sync dari paket standar
	var srcErr, dstErr error
	dbWg.Add(2)

	go func() {
		defer dbWg.Done()
		srcConn, srcErr = connectDBWithRetry(ctx, cfg.SrcDB, cfg.MaxRetries, cfg.RetryInterval, "source", metricsStore)
	}()
	go func() {
		defer dbWg.Done()
		dstConn, dstErr = connectDBWithRetry(ctx, cfg.DstDB, cfg.MaxRetries, cfg.RetryInterval, "destination", metricsStore)
	}()
	dbWg.Wait() // Wait for both connections to establish or fail

	// Check for fatal connection errors
	if srcErr != nil { logger.Log.Fatal("Failed to establish source DB connection", zap.Error(srcErr)) }
	if dstErr != nil { logger.Log.Fatal("Failed to establish destination DB connection", zap.Error(dstErr)) }

	// Defer closing connections
	defer func() {
		logger.Log.Info("Closing database connections...")
		if srcConn != nil {
			if err := srcConn.Close(); err != nil {
				logger.Log.Error("Error closing source DB connection", zap.Error(err))
			}
		}
		if dstConn != nil {
			if err := dstConn.Close(); err != nil {
				logger.Log.Error("Error closing destination DB connection", zap.Error(err))
			}
		}
	}()

	// 8. Optimize connection pools (after successful connection)
	logger.Log.Info("Optimizing database connection pools")
	if err := srcConn.Optimize(cfg.ConnPoolSize, cfg.ConnMaxLifetime); err != nil {
		logger.Log.Warn("Failed to optimize source DB connection pool", zap.Error(err))
	}
	if err := dstConn.Optimize(cfg.ConnPoolSize, cfg.ConnMaxLifetime); err != nil {
		logger.Log.Warn("Failed to optimize destination DB connection pool", zap.Error(err))
	}

	// 9. Start HTTP Server for Metrics, Health, Pprof (runs in background)
	go server.RunHTTPServer(ctx, cfg, metricsStore, srcConn, dstConn, logger.Log)

	// 10. Create and run the synchronization process
	logger.Log.Info("Starting database synchronization process...")
	// Gunakan alias projectSync saat merujuk ke tipe/fungsi dari paket internal sync
	var syncer projectSync.FullSyncInterface = projectSync.NewFullSync(srcConn, dstConn, cfg, logger.Log, metricsStore)
	results := syncer.Run(ctx) // This blocks until sync is complete or ctx is cancelled

	// 11. Process and log results after Run finishes
	logger.Log.Info("Synchronization process finished. Processing results...")
	exitCode := processResults(results) // processResults sekarang menerima map[string]projectSync.SyncResult

	// 12. Wait for shutdown signal if Run finished before signal was received
	// This allows the HTTP server to keep running until explicitly stopped.
	logger.Log.Info("Waiting for shutdown signal (Ctrl+C or SIGTERM)...")
	<-ctx.Done() // Wait here until context is cancelled

	// Stop was deferred, context cancellation triggers cleanup via defer statements
	logger.Log.Info("Shutdown signal received or process completed. Exiting.", zap.Int("exit_code", exitCode))
	os.Exit(exitCode) // Exit with appropriate code
}


// --- Helper Functions ---

// logLoadedConfig safely logs configuration details, omitting sensitive info.
func logLoadedConfig(cfg *config.Config) {
	// Avoid logging passwords directly
	logger.Log.Info("Configuration loaded successfully",
		zap.String("sync_direction", cfg.SyncDirection),
		// Source DB Info
		zap.String("src_dialect", cfg.SrcDB.Dialect),
		zap.String("src_host", cfg.SrcDB.Host),
		zap.Int("src_port", cfg.SrcDB.Port),
		zap.String("src_user", cfg.SrcDB.User),
		zap.String("src_dbname", cfg.SrcDB.DBName),
		zap.String("src_sslmode", cfg.SrcDB.SSLMode),
		// Destination DB Info
		zap.String("dst_dialect", cfg.DstDB.Dialect),
		zap.String("dst_host", cfg.DstDB.Host),
		zap.Int("dst_port", cfg.DstDB.Port),
		zap.String("dst_user", cfg.DstDB.User),
		zap.String("dst_dbname", cfg.DstDB.DBName),
		zap.String("dst_sslmode", cfg.DstDB.SSLMode),
		// Sync Parameters
		zap.Int("workers", cfg.Workers),
		zap.Int("batch_size", cfg.BatchSize),
		zap.Duration("table_timeout", cfg.TableTimeout),
		zap.String("schema_strategy", string(cfg.SchemaSyncStrategy)),
		zap.Bool("skip_failed_tables", cfg.SkipFailedTables),
		zap.Bool("disable_fk_during_sync", cfg.DisableFKDuringSync),
		// Retry Parameters
		zap.Int("max_retries", cfg.MaxRetries),
		zap.Duration("retry_interval", cfg.RetryInterval),
		// Pool Parameters
		zap.Int("conn_pool_size", cfg.ConnPoolSize),
		zap.Duration("conn_max_lifetime", cfg.ConnMaxLifetime),
		// Observability
		zap.Bool("json_logging", cfg.EnableJsonLogging),
		zap.Bool("enable_pprof", cfg.EnablePprof),
		zap.Int("metrics_port", cfg.MetricsPort),
	)
}

// connectDBWithRetry attempts to connect and ping the database with retries.
func connectDBWithRetry(
	ctx context.Context,
	dbCfg config.DatabaseConfig,
	maxRetries int,
	retryInterval time.Duration,
	dbLabel string, // "source" or "destination"
	metricsStore *metrics.Store,
) (*db.Connector, error) {
	gl := logger.GetGormLogger() // Get the initialized GORM logger
	var lastErr error

	dsn := buildDSN(dbCfg) // Use buildDSN helper
	if dsn == "" {
		err := fmt.Errorf("could not build DSN for %s database (unsupported dialect: %s)", dbLabel, dbCfg.Dialect)
		metricsStore.SyncErrorsTotal.WithLabelValues("connection", dbLabel).Inc() // Increment specific error counter
		return nil, err
	}

	for i := 0; i <= maxRetries; i++ {
		attemptStartTime := time.Now()
		// Wait before retrying (only if not the first attempt)
		if i > 0 {
			logger.Log.Warn("Retrying database connection",
				zap.String("db", dbLabel),
				zap.Int("attempt", i+1), // Log as attempt #2, #3, etc.
				zap.Int("max_attempts", maxRetries+1),
				zap.Duration("wait_interval", retryInterval),
				zap.Error(lastErr), // Log the error from the previous attempt
			)
			// Use a timer to wait, allowing context cancellation
			timer := time.NewTimer(retryInterval)
			select {
			case <-timer.C:
				// Continue retry
			case <-ctx.Done():
				timer.Stop() // Clean up timer
				logger.Log.Warn("Context cancelled during connection retry wait", zap.String("db", dbLabel), zap.Error(ctx.Err()))
				errMsg := fmt.Errorf("connection cancelled for %s DB while waiting to retry (attempt %d): %w (last error: %v)", dbLabel, i+1, ctx.Err(), lastErr)
				metricsStore.SyncErrorsTotal.WithLabelValues("connection_cancelled", dbLabel).Inc()
				return nil, errMsg
			}
		}

		logger.Log.Info("Attempting to connect", zap.String("db", dbLabel), zap.String("dialect", dbCfg.Dialect), zap.Int("attempt", i+1))
		conn, err := db.New(dbCfg.Dialect, dsn, gl)
		if err != nil {
			lastErr = fmt.Errorf("connection attempt %d/%d failed: %w", i+1, maxRetries+1, err)
			continue // Go to next retry iteration
		}

		// Ping the database to verify connection is live
		pingErr := conn.Ping(ctx) // Ping uses its own internal timeout now
		if pingErr != nil {
			lastErr = fmt.Errorf("ping attempt %d/%d failed after successful connection: %w (connection error: %v)", i+1, maxRetries+1, pingErr, err)
			// Close the potentially bad connection before retrying
			_ = conn.Close() // Ignore close error here
			continue // Go to next retry iteration
		}

		// Connection and Ping successful
		logger.Log.Info("Database connection successful",
			zap.String("db", dbLabel),
			zap.String("dialect", dbCfg.Dialect),
			zap.Int("attempt", i+1),
			zap.Duration("connect_duration", time.Since(attemptStartTime)),
		)
		return conn, nil
	}

	// If loop finishes, all retries failed
	logger.Log.Error("Failed to connect to database after all retries",
		zap.String("db", dbLabel),
		zap.Int("attempts", maxRetries+1),
		zap.Error(lastErr), // Log the final error
	)
	metricsStore.SyncErrorsTotal.WithLabelValues("connection_failed", dbLabel).Inc()
	return nil, fmt.Errorf("failed to connect to %s DB (%s) after %d attempts: %w", dbLabel, dbCfg.Dialect, maxRetries+1, lastErr)
}


// buildDSN creates the Data Source Name string for connecting to the database.
func buildDSN(cfg config.DatabaseConfig) string {
	switch strings.ToLower(cfg.Dialect) {
	case "mysql":
		// Example: user:password@tcp(host:port)/dbname?charset=utf8mb4&parseTime=True&loc=Local&timeout=10s&readTimeout=60s&writeTimeout=60s
		return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local&timeout=10s&readTimeout=60s&writeTimeout=60s",
			cfg.User,
			cfg.Password, // WARNING: Password in DSN string
			cfg.Host,
			cfg.Port,
			cfg.DBName,
		)
	case "postgres":
		// Example: host=localhost port=5432 user=postgres password=secret dbname=test sslmode=disable connect_timeout=10
		return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s connect_timeout=10",
			cfg.Host,
			cfg.Port,
			cfg.User,
			cfg.Password, // WARNING: Password in DSN string
			cfg.DBName,
			strings.ToLower(cfg.SSLMode), // Ensure SSL mode is lowercase
		)
	case "sqlite":
		// Example: file:my_database.db?cache=shared&_foreign_keys=1&_journal_mode=WAL&_busy_timeout=5000
		return fmt.Sprintf("file:%s?cache=shared&_foreign_keys=1&_journal_mode=WAL&_busy_timeout=5000",
			cfg.DBName, // DBName should be the file path for SQLite
		)
	default:
		logger.Log.Error("Cannot build DSN: Unsupported database dialect", zap.String("dialect", cfg.Dialect))
		return ""
	}
}

// processResults analyzes the sync results and determines the exit code.
// Gunakan alias projectSync untuk SyncResult
func processResults(results map[string]projectSync.SyncResult) (exitCode int) {
	successCount := 0
	schemaFailCount := 0
	dataFailCount := 0
	constraintFailCount := 0
	skippedCount := 0
	totalTables := len(results)

	if totalTables == 0 {
		logger.Log.Warn("Synchronization finished, but no tables were processed.")
		return 0 // No tables found isn't necessarily an error state
	}


	for table, res := range results {
		// Log details for each table result
		fields := []zap.Field{
			zap.String("table", table),
			zap.Duration("duration", res.Duration),
			zap.Bool("skipped", res.Skipped),
			zap.Bool("schema_sync_skipped", res.SchemaSyncSkipped),
			zap.Int64("rows_synced", res.RowsSynced),
			zap.Int("batches", res.Batches),
			zap.NamedError("schema_error", res.SchemaError),
			zap.NamedError("data_error", res.DataError),
			zap.NamedError("constraint_error", res.ConstraintError),
		}

		level := zap.InfoLevel // Default to Info for successful tables
		status := "Success"

		if res.Skipped {
			skippedCount++
			level = zap.WarnLevel
			status = "Skipped"
			if res.SchemaError != nil { schemaFailCount++; status = "Skipped (Schema Error)" }
            if res.DataError != nil { dataFailCount++; status = "Skipped (Data Error)" } // Should not happen if schema failed
			if res.ConstraintError != nil { constraintFailCount++; status = "Skipped (Constraint Error)" } // Should not happen
		} else if res.SchemaError != nil {
			schemaFailCount++
			level = zap.ErrorLevel
			status = "Schema Failure"
		} else if res.DataError != nil {
			dataFailCount++
			level = zap.ErrorLevel
			status = "Data Failure"
		} else if res.ConstraintError != nil {
			constraintFailCount++
			level = zap.WarnLevel // Constraint failure is a warning, data sync succeeded
			status = "Constraint Failure"
			successCount++ // Count data sync as success even if constraints failed
		} else {
			successCount++
		}
		logger.Log.Check(level, "Table sync result: "+status).Write(fields...)
	}


	// Final Summary Log
	logger.Log.Info("Synchronization summary",
		zap.Int("total_tables_attempted", totalTables),
		zap.Int("successful_data_constraint_sync", successCount), // Renamed for clarity
		zap.Int("schema_failures", schemaFailCount),
		zap.Int("data_failures", dataFailCount),
		zap.Int("constraint_failures", constraintFailCount), // Only constraint step failed
		zap.Int("skipped", skippedCount),
	)

	// Determine overall exit code
	if schemaFailCount > 0 || dataFailCount > 0 {
		logger.Log.Error("Synchronization completed with critical errors (schema or data).")
		return 1 // Indicate critical failure
	}
	if constraintFailCount > 0 {
		logger.Log.Warn("Synchronization completed successfully for data, but with constraint application errors.")
		return 2 // Indicate partial success / warning
	}
	if skippedCount == totalTables && totalTables > 0 {
		logger.Log.Warn("Synchronization completed, but all tables were skipped.")
		return 3 // Indicate nothing was actually synced
	}
	logger.Log.Info("Synchronization completed successfully.")
	return 0 // Success
}
