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
	"github.com/arwahdevops/dbsync/internal/secrets" // <-- Impor paket baru
	"github.com/arwahdevops/dbsync/internal/server"
	projectSync "github.com/arwahdevops/dbsync/internal/sync" // Impor paket sync internal dengan alias 'projectSync'
)

func main() {
	// 1. Load environment variables (.env overrides)
	if err := godotenv.Overload(".env"); err != nil {
		stdlog.Printf("Warning: Could not load .env file: %v. Relying on environment variables.\n", err)
	}

	// 2. Initial config loading to get logging settings
	preCfg := &struct {
		EnableJsonLogging bool `env:"ENABLE_JSON_LOGGING" envDefault:"false"`
		DebugMode         bool `env:"DEBUG_MODE" envDefault:"false"`
	}{}
	_ = env.Parse(preCfg) // Ignore error for pre-config

	// 3. Initialize Zap logger based on pre-config
	if err := logger.Init(preCfg.DebugMode, preCfg.EnableJsonLogging); err != nil {
		stdlog.Fatalf("Failed to initialize logger: %v", err)
	}
	defer func() { _ = logger.Log.Sync() }()

	// 4. Load and validate full configuration using Zap for logging errors
	cfg, err := config.Load()
	if err != nil {
		logger.Log.Fatal("Configuration loading error", zap.Error(err))
	}
	logLoadedConfig(cfg) // Log config safely

	// 5. Setup context for graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 6. Initialize Metrics Store
	metricsStore := metrics.NewMetricsStore()

	// 7. Initialize Secret Managers (Contoh Vault)
	// Lakukan ini untuk setiap secret manager yang didukung
	vaultMgr, vaultErr := secrets.NewVaultManager(cfg, logger.Log)
	if vaultErr != nil {
		// Log error tapi jangan fatal jika Vault tidak di-enable
		if cfg.VaultEnabled { // Hanya fatal jika seharusnya aktif
			logger.Log.Fatal("Failed to initialize Vault secret manager", zap.Error(vaultErr))
		} else {
			logger.Log.Warn("Could not initialize Vault secret manager (Vault not enabled or config error)", zap.Error(vaultErr))
		}
	}
	// Buat slice dari semua secret manager yang aktif (atau bisa diinisialisasi)
	// Ini memungkinkan loadCredentials mencoba beberapa manager
	availableSecretManagers := make([]secrets.SecretManager, 0)
	if vaultMgr != nil { // Hanya tambahkan jika inisialisasi berhasil (meskipun mungkin disabled)
		availableSecretManagers = append(availableSecretManagers, vaultMgr)
	}
	// Tambahkan inisialisasi manager lain di sini dan append ke slice
	// awsMgr, awsErr := secrets.NewAwsSmManager(cfg, logger.Log) ... if awsMgr != nil { availableSecretManagers = append...}

	// 8. Load Credentials (Prioritaskan Env Var, lalu coba Secret Managers)
	logger.Log.Info("Loading database credentials...")
	srcCreds, srcCredsErr := loadCredentials(ctx, cfg, &cfg.SrcDB, "source", availableSecretManagers)
	if srcCredsErr != nil { logger.Log.Fatal("Failed to load source DB credentials", zap.Error(srcCredsErr)) }

	dstCreds, dstCredsErr := loadCredentials(ctx, cfg, &cfg.DstDB, "destination", availableSecretManagers)
	if dstCredsErr != nil { logger.Log.Fatal("Failed to load destination DB credentials", zap.Error(dstCredsErr)) }

	// 9. Initialize database connections with retry (menggunakan kredensial yang sudah didapat)
	logger.Log.Info("Connecting to databases...")
	var srcConn, dstConn *db.Connector
	var dbWg sync.WaitGroup
	var srcErr, dstErr error
	dbWg.Add(2)

	go func() {
		defer dbWg.Done()
		// Panggil connectDBWithRetry dengan kredensial eksplisit
		srcConn, srcErr = connectDBWithRetry(ctx, cfg.SrcDB, srcCreds.Username, srcCreds.Password, cfg.MaxRetries, cfg.RetryInterval, "source", metricsStore)
	}()
	go func() {
		defer dbWg.Done()
		// Panggil connectDBWithRetry dengan kredensial eksplisit
		dstConn, dstErr = connectDBWithRetry(ctx, cfg.DstDB, dstCreds.Username, dstCreds.Password, cfg.MaxRetries, cfg.RetryInterval, "destination", metricsStore)
	}()
	dbWg.Wait()

	if srcErr != nil { logger.Log.Fatal("Failed to establish source DB connection", zap.Error(srcErr)) }
	if dstErr != nil { logger.Log.Fatal("Failed to establish destination DB connection", zap.Error(dstErr)) }

	// Defer closing connections
	defer func() {
		logger.Log.Info("Closing database connections...")
		if srcConn != nil { if err := srcConn.Close(); err != nil { logger.Log.Error("Error closing source DB", zap.Error(err)) } }
		if dstConn != nil { if err := dstConn.Close(); err != nil { logger.Log.Error("Error closing destination DB", zap.Error(err)) } }
	}()

	// 10. Optimize connection pools
	logger.Log.Info("Optimizing database connection pools")
	if err := srcConn.Optimize(cfg.ConnPoolSize, cfg.ConnMaxLifetime); err != nil { logger.Log.Warn("Failed to optimize source DB pool", zap.Error(err)) }
	if err := dstConn.Optimize(cfg.ConnPoolSize, cfg.ConnMaxLifetime); err != nil { logger.Log.Warn("Failed to optimize destination DB pool", zap.Error(err)) }

	// 11. Start HTTP Server for Metrics, Health, Pprof (runs in background)
	go server.RunHTTPServer(ctx, cfg, metricsStore, srcConn, dstConn, logger.Log)

	// 12. Create and run the synchronization process
	logger.Log.Info("Starting database synchronization process...")
	var syncer projectSync.FullSyncInterface = projectSync.NewFullSync(srcConn, dstConn, cfg, logger.Log, metricsStore)
	results := syncer.Run(ctx)

	// 13. Process and log results after Run finishes
	logger.Log.Info("Synchronization process finished. Processing results...")
	exitCode := processResults(results)

	// 14. Wait for shutdown signal if Run finished before signal was received
	logger.Log.Info("Waiting for shutdown signal (Ctrl+C or SIGTERM)...")
	<-ctx.Done()

	logger.Log.Info("Shutdown signal received or process completed. Exiting.", zap.Int("exit_code", exitCode))
	os.Exit(exitCode)
}


// --- Helper Functions ---

// logLoadedConfig safely logs configuration details.
func logLoadedConfig(cfg *config.Config) {
	logger.Log.Info("Configuration loaded successfully",
		zap.String("sync_direction", cfg.SyncDirection),
		// Source DB (sensitive password omitted)
		zap.String("src_dialect", cfg.SrcDB.Dialect), zap.String("src_host", cfg.SrcDB.Host), zap.Int("src_port", cfg.SrcDB.Port), zap.String("src_user", cfg.SrcDB.User), zap.String("src_dbname", cfg.SrcDB.DBName), zap.String("src_sslmode", cfg.SrcDB.SSLMode),
		// Destination DB (sensitive password omitted)
		zap.String("dst_dialect", cfg.DstDB.Dialect), zap.String("dst_host", cfg.DstDB.Host), zap.Int("dst_port", cfg.DstDB.Port), zap.String("dst_user", cfg.DstDB.User), zap.String("dst_dbname", cfg.DstDB.DBName), zap.String("dst_sslmode", cfg.DstDB.SSLMode),
		// Sync Parameters
		zap.Int("workers", cfg.Workers), zap.Int("batch_size", cfg.BatchSize), zap.Duration("table_timeout", cfg.TableTimeout), zap.String("schema_strategy", string(cfg.SchemaSyncStrategy)), zap.Bool("skip_failed_tables", cfg.SkipFailedTables), zap.Bool("disable_fk_during_sync", cfg.DisableFKDuringSync),
		// Retry Parameters
		zap.Int("max_retries", cfg.MaxRetries), zap.Duration("retry_interval", cfg.RetryInterval),
		// Pool Parameters
		zap.Int("conn_pool_size", cfg.ConnPoolSize), zap.Duration("conn_max_lifetime", cfg.ConnMaxLifetime),
		// Observability
		zap.Bool("json_logging", cfg.EnableJsonLogging), zap.Bool("enable_pprof", cfg.EnablePprof), zap.Int("metrics_port", cfg.MetricsPort),
		// Vault Config (sensitive token omitted)
		zap.Bool("vault_enabled", cfg.VaultEnabled), zap.String("vault_addr", cfg.VaultAddr), zap.String("src_secret_path", cfg.SrcSecretPath), zap.String("dst_secret_path", cfg.DstSecretPath),
	)
}

// loadCredentials attempts to load DB credentials either directly from config or via secret managers.
func loadCredentials(
	ctx context.Context,
	cfg *config.Config,
	dbCfg *config.DatabaseConfig, // Pointer ke config DB spesifik (SRC/DST)
	dbLabel string, // "source" or "destination"
	secretManagers []secrets.SecretManager, // Terima slice dari semua manager yang diinisialisasi
) (*secrets.Credentials, error) {

	log := logger.Log.With(zap.String("db", dbLabel))

	// 1. Prioritaskan password dari environment variable langsung
	if dbCfg.Password != "" {
		log.Info("Using password directly from environment variable")
		if dbCfg.User == "" {
			return nil, fmt.Errorf("password provided for %s DB via env var, but username is missing", dbLabel)
		}
		return &secrets.Credentials{
			Username: dbCfg.User,
			Password: dbCfg.Password,
		}, nil
	}
	log.Info("Password not found in environment variable, checking secret managers...")

	// 2. Cek secret managers secara berurutan
	var secretPath string
	if dbLabel == "source" { secretPath = cfg.SrcSecretPath } else { secretPath = cfg.DstSecretPath }

	// Hanya coba secret manager jika path-nya ada
	if secretPath != "" {
		for _, sm := range secretManagers {
			if sm != nil && sm.IsEnabled() {
				log.Info("Attempting to retrieve credentials from configured secret manager", zap.String("manager_type", fmt.Sprintf("%T", sm)), zap.String("path_or_id", secretPath))
				// Beri timeout pada pengambilan secret?
				getCtx, cancel := context.WithTimeout(ctx, 15*time.Second) // Timeout 15 detik
				creds, err := sm.GetCredentials(getCtx, secretPath)
				cancel() // Selalu panggil cancel

				if err == nil && creds != nil {
					log.Info("Successfully retrieved credentials from secret manager")
					if creds.Password == "" { return nil, fmt.Errorf("retrieved credentials for %s, but password field is empty", dbLabel) }
					// Fallback username jika kosong di secret
					if creds.Username == "" {
						log.Warn("Username field empty in retrieved secret, falling back to environment variable username")
						creds.Username = dbCfg.User
						if creds.Username == "" { return nil, fmt.Errorf("password retrieved for %s, but username is missing in both secret and env var", dbLabel) }
					}
					return creds, nil
				} else {
					log.Warn("Failed to retrieve credentials from secret manager", zap.String("manager_type", fmt.Sprintf("%T", sm)), zap.Error(err))
				}
			}
		}
	} else {
		log.Warn("Secret path/ID is not configured, cannot use secret managers")
	}


	// 3. Jika semua gagal (password env kosong DAN secret manager gagal/tidak aktif/tidak dikonfigurasi pathnya)
	log.Error("Failed to load credentials: Password not found in env vars and no enabled secret manager provided valid credentials for the configured path.")
	return nil, fmt.Errorf("could not load credentials for %s DB", dbLabel)
}


// connectDBWithRetry perlu diubah untuk menerima username/password
func connectDBWithRetry(
	ctx context.Context,
	dbCfg config.DatabaseConfig, // Tetap terima config untuk host, port, dll.
	username string,
	password string,
	maxRetries int,
	retryInterval time.Duration,
	dbLabel string,
	metricsStore *metrics.Store,
) (*db.Connector, error) {
	gl := logger.GetGormLogger()
	var lastErr error

	// Bangun DSN menggunakan kredensial yang diterima
	dsn := buildDSN(dbCfg, username, password) // buildDSN diubah
	if dsn == "" {
		err := fmt.Errorf("could not build DSN for %s DB (unsupported dialect: %s)", dbLabel, dbCfg.Dialect)
		metricsStore.SyncErrorsTotal.WithLabelValues("connection", dbLabel).Inc()
		return nil, err
	}

	// Logika retry loop sama
	for i := 0; i <= maxRetries; i++ {
		attemptStartTime := time.Now()
		if i > 0 {
			logger.Log.Warn("Retrying database connection", zap.String("db", dbLabel), zap.Int("attempt", i+1), zap.Int("max_attempts", maxRetries+1), zap.Duration("wait_interval", retryInterval), zap.Error(lastErr))
			timer := time.NewTimer(retryInterval); select { case <-timer.C: case <-ctx.Done(): timer.Stop(); errMsg := fmt.Errorf("cancelled waiting retry %s DB: %w (last error: %v)", dbLabel, ctx.Err(), lastErr); metricsStore.SyncErrorsTotal.WithLabelValues("connection_cancelled", dbLabel).Inc(); return nil, errMsg }
		}

		logger.Log.Info("Attempting to connect", zap.String("db", dbLabel), zap.String("dialect", dbCfg.Dialect), zap.Int("attempt", i+1))
		conn, err := db.New(dbCfg.Dialect, dsn, gl) // Gunakan DSN yang sudah dibangun
		if err != nil { lastErr = fmt.Errorf("connect attempt %d/%d failed: %w", i+1, maxRetries+1, err); continue }

		pingErr := conn.Ping(ctx)
		if pingErr != nil { lastErr = fmt.Errorf("ping attempt %d/%d failed: %w (conn err: %v)", i+1, maxRetries+1, pingErr, err); _ = conn.Close(); continue }

		logger.Log.Info("Database connection successful", zap.String("db", dbLabel), zap.Duration("connect_duration", time.Since(attemptStartTime)))
		return conn, nil
	}

	logger.Log.Error("Failed to connect to database after all retries", zap.String("db", dbLabel), zap.Int("attempts", maxRetries+1), zap.Error(lastErr))
	metricsStore.SyncErrorsTotal.WithLabelValues("connection_failed", dbLabel).Inc()
	return nil, fmt.Errorf("failed to connect to %s DB (%s) after %d attempts: %w", dbLabel, dbCfg.Dialect, maxRetries+1, lastErr)
}


// buildDSN diubah untuk menerima kredensial
func buildDSN(cfg config.DatabaseConfig, username, password string) string {
	// Ambil detail lain dari cfg
	host := cfg.Host
	port := cfg.Port
	dbname := cfg.DBName
	sslmode := strings.ToLower(cfg.SSLMode)

	switch strings.ToLower(cfg.Dialect) {
	case "mysql":
		return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local&timeout=10s&readTimeout=60s&writeTimeout=60s",
			username, password, host, port, dbname)
	case "postgres":
		return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s connect_timeout=10",
			host, port, username, password, dbname, sslmode)
	case "sqlite":
		return fmt.Sprintf("file:%s?cache=shared&_foreign_keys=1&_journal_mode=WAL&_busy_timeout=5000", dbname)
	default:
		logger.Log.Error("Cannot build DSN: Unsupported database dialect", zap.String("dialect", cfg.Dialect))
		return ""
	}
}

// processResults (Tidak Berubah, tapi pastikan tipenya projectSync.SyncResult)
func processResults(results map[string]projectSync.SyncResult) (exitCode int) {
	successCount := 0; schemaFailCount := 0; dataFailCount := 0; constraintFailCount := 0; skippedCount := 0
	totalTables := len(results)
	if totalTables == 0 { logger.Log.Warn("Sync finished, no tables processed."); return 0 }

	for table, res := range results {
		fields := []zap.Field{ zap.String("table", table), zap.Duration("duration", res.Duration), zap.Bool("skipped", res.Skipped), zap.Bool("schema_sync_skipped", res.SchemaSyncSkipped), zap.Int64("rows_synced", res.RowsSynced), zap.Int("batches", res.Batches), zap.NamedError("schema_error", res.SchemaError), zap.NamedError("data_error", res.DataError), zap.NamedError("constraint_error", res.ConstraintError), }
		level := zap.InfoLevel; status := "Success"
		if res.Skipped { skippedCount++; level = zap.WarnLevel; status = "Skipped"; if res.SchemaError != nil { schemaFailCount++; status = "Skipped (Schema Error)" }; if res.DataError != nil { dataFailCount++; status = "Skipped (Data Error)" }; if res.ConstraintError != nil { constraintFailCount++; status = "Skipped (Constraint Error)" }
		} else if res.SchemaError != nil { schemaFailCount++; level = zap.ErrorLevel; status = "Schema Failure"
		} else if res.DataError != nil { dataFailCount++; level = zap.ErrorLevel; status = "Data Failure"
		} else if res.ConstraintError != nil { constraintFailCount++; level = zap.WarnLevel; status = "Constraint Failure"; successCount++
		} else { successCount++ }
		logger.Log.Check(level, "Table sync result: "+status).Write(fields...)
	}

	logger.Log.Info("Synchronization summary", zap.Int("total_tables_attempted", totalTables), zap.Int("successful_data_constraint_sync", successCount), zap.Int("schema_failures", schemaFailCount), zap.Int("data_failures", dataFailCount), zap.Int("constraint_failures", constraintFailCount), zap.Int("skipped", skippedCount))

	if schemaFailCount > 0 || dataFailCount > 0 { logger.Log.Error("Sync completed with critical errors."); return 1 }
	if constraintFailCount > 0 { logger.Log.Warn("Sync completed successfully for data, but with constraint errors."); return 2 }
	if skippedCount == totalTables && totalTables > 0 { logger.Log.Warn("Sync completed, but all tables were skipped."); return 3 }
	logger.Log.Info("Synchronization completed successfully."); return 0
}
