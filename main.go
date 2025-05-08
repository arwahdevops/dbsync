// main.go
package main

import (
	"context"
	"flag"
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
	"github.com/arwahdevops/dbsync/internal/logger" // Pastikan ini adalah import yang benar
	"github.com/arwahdevops/dbsync/internal/metrics"
	"github.com/arwahdevops/dbsync/internal/secrets"
	"github.com/arwahdevops/dbsync/internal/server"
	projectSync "github.com/arwahdevops/dbsync/internal/sync" // Alias untuk menghindari konflik nama
)

var (
	syncDirectionOverride       string
	batchSizeOverride           int
	workersOverride             int
	schemaSyncStrategyOverride  string
	typeMappingFilePathOverride string
	// Tambahkan flag lain yang ingin di-override di sini
)

func main() {
	// Definisikan flag CLI
	flag.StringVar(&syncDirectionOverride, "sync-direction", "", "Override SYNC_DIRECTION (e.g., mysql-to-postgres)")
	flag.IntVar(&batchSizeOverride, "batch-size", 0, "Override BATCH_SIZE (must be > 0)")
	flag.IntVar(&workersOverride, "workers", 0, "Override WORKERS (must be > 0)")
	flag.StringVar(&schemaSyncStrategyOverride, "schema-strategy", "", "Override SCHEMA_SYNC_STRATEGY (drop_create, alter, none)")
	flag.StringVar(&typeMappingFilePathOverride, "type-map-file", "", "Override TYPE_MAPPING_FILE_PATH")
	// ... definisi flag lainnya ...
	flag.Parse()

	// 1. Load environment variables (.env overrides)
	if err := godotenv.Overload(".env"); err != nil {
		stdlog.Printf("Warning: Could not load .env file: %v. Relying on environment variables.\n", err)
	}

	// 2. Initial config loading untuk mendapatkan setting logger
	preCfg := &struct {
		EnableJsonLogging bool `env:"ENABLE_JSON_LOGGING" envDefault:"false"`
		DebugMode         bool `env:"DEBUG_MODE" envDefault:"false"`
	}{}
	if err := env.Parse(preCfg); err != nil {
		stdlog.Fatalf("Failed to parse pre-configuration for logger: %v", err)
	}

	// 3. Initialize Zap logger
	if err := logger.Init(preCfg.DebugMode, preCfg.EnableJsonLogging); err != nil {
		stdlog.Fatalf("Failed to initialize logger: %v", err)
	}
	defer func() { _ = logger.Log.Sync() }()

	// 4. Load and validate full configuration dari environment variables
	cfg, err := config.Load()
	if err != nil {
		logger.Log.Fatal("Configuration loading error from environment", zap.Error(err))
	}

	// --- Terapkan Override dari Flag CLI SETELAH config.Load() ---
	applyCliOverrides(cfg)

	// Muat type mappings dari file SETELAH config utama (termasuk path dari CLI) dimuat
	if err := config.LoadTypeMappings(cfg.TypeMappingFilePath, logger.Log); err != nil {
		logger.Log.Warn("Proceeding with potentially limited or no external type mappings due to load error.", zap.Error(err))
	}

	logLoadedConfig(cfg) // Log konfigurasi final

	// 5. Setup context untuk graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 6. Initialize Metrics Store
	metricsStore := metrics.NewMetricsStore()

	// 7. Initialize Secret Managers
	vaultMgr, vaultErr := secrets.NewVaultManager(cfg, logger.Log)
	if vaultErr != nil {
		if cfg.VaultEnabled {
			logger.Log.Fatal("Failed to initialize Vault secret manager", zap.Error(vaultErr))
		} else {
			logger.Log.Warn("Could not initialize Vault secret manager (Vault not enabled or config error)", zap.Error(vaultErr))
		}
	}
	availableSecretManagers := make([]secrets.SecretManager, 0)
	if vaultMgr != nil && vaultMgr.IsEnabled() {
		availableSecretManagers = append(availableSecretManagers, vaultMgr)
	}

	// 8. Load Credentials
	logger.Log.Info("Loading database credentials...")
	srcCreds, srcCredsErr := loadCredentials(ctx, cfg, &cfg.SrcDB, "source", cfg.SrcSecretPath, cfg.SrcUsernameKey, cfg.SrcPasswordKey, availableSecretManagers)
	if srcCredsErr != nil {
		logger.Log.Fatal("Failed to load source DB credentials", zap.Error(srcCredsErr))
	}
	dstCreds, dstCredsErr := loadCredentials(ctx, cfg, &cfg.DstDB, "destination", cfg.DstSecretPath, cfg.DstUsernameKey, cfg.DstPasswordKey, availableSecretManagers)
	if dstCredsErr != nil {
		logger.Log.Fatal("Failed to load destination DB credentials", zap.Error(dstCredsErr))
	}

	// 9. Initialize database connections with retry
	logger.Log.Info("Connecting to databases...")
	var srcConn, dstConn *db.Connector
	var dbWg sync.WaitGroup
	var srcErrInner, dstErrInner error
	dbWg.Add(2)
	go func() {
		defer dbWg.Done()
		srcConn, srcErrInner = connectDBWithRetry(ctx, cfg.SrcDB, srcCreds.Username, srcCreds.Password, cfg.MaxRetries, cfg.RetryInterval, "source", metricsStore)
	}()
	go func() {
		defer dbWg.Done()
		dstConn, dstErrInner = connectDBWithRetry(ctx, cfg.DstDB, dstCreds.Username, dstCreds.Password, cfg.MaxRetries, cfg.RetryInterval, "destination", metricsStore)
	}()
	dbWg.Wait()
	if srcErrInner != nil {
		logger.Log.Fatal("Failed to establish source DB connection", zap.Error(srcErrInner))
	}
	if dstErrInner != nil {
		logger.Log.Fatal("Failed to establish destination DB connection", zap.Error(dstErrInner))
	}
	defer func() {
		logger.Log.Info("Closing database connections...")
		if srcConn != nil {
			if err := srcConn.Close(); err != nil {
				logger.Log.Error("Error closing source DB", zap.Error(err))
			}
		}
		if dstConn != nil {
			if err := dstConn.Close(); err != nil {
				logger.Log.Error("Error closing destination DB", zap.Error(err))
			}
		}
	}()

	// 10. Optimize connection pools
	logger.Log.Info("Optimizing database connection pools")
	if err := srcConn.Optimize(cfg.ConnPoolSize, cfg.ConnMaxLifetime); err != nil {
		logger.Log.Warn("Failed to optimize source DB pool", zap.Error(err))
	}
	if err := dstConn.Optimize(cfg.ConnPoolSize, cfg.ConnMaxLifetime); err != nil {
		logger.Log.Warn("Failed to optimize destination DB pool", zap.Error(err))
	}

	// 11. Start HTTP Server
	go server.RunHTTPServer(ctx, cfg, metricsStore, srcConn, dstConn, logger.Log)

	// 12. Create and run the synchronization process
	logger.Log.Info("Starting database synchronization process...")
	syncer := projectSync.NewFullSync(srcConn, dstConn, cfg, logger.Log, metricsStore)
	results := syncer.Run(ctx) // results is map[string]projectSync.SyncResult

	// 13. Process and log results
	logger.Log.Info("Synchronization process finished. Processing results...")
	exitCode := processResults(results) // Pass the results map

	// 14. Wait for shutdown or completion
	if ctx.Err() == nil {
		logger.Log.Info("Main synchronization logic completed. Waiting for shutdown signal (Ctrl+C or SIGTERM)...")
		<-ctx.Done() // Block until context is cancelled
	} else {
		logger.Log.Info("Shutdown signal received during synchronization. Proceeding with cleanup.")
	}

	logger.Log.Info("Shutdown complete. Exiting.", zap.Int("exit_code", exitCode))
	os.Exit(exitCode)
}

// applyCliOverrides menerapkan nilai dari flag CLI ke struct Config.
func applyCliOverrides(cfg *config.Config) {
	if syncDirectionOverride != "" {
		logger.Log.Info("Overriding SYNC_DIRECTION with CLI flag", zap.String("env_value", cfg.SyncDirection), zap.String("cli_value", syncDirectionOverride))
		cfg.SyncDirection = syncDirectionOverride
	}
	if batchSizeOverride > 0 {
		logger.Log.Info("Overriding BATCH_SIZE with CLI flag", zap.Int("env_value", cfg.BatchSize), zap.Int("cli_value", batchSizeOverride))
		cfg.BatchSize = batchSizeOverride
	}
	if workersOverride > 0 {
		logger.Log.Info("Overriding WORKERS with CLI flag", zap.Int("env_value", cfg.Workers), zap.Int("cli_value", workersOverride))
		cfg.Workers = workersOverride
	}
	if schemaSyncStrategyOverride != "" {
		logger.Log.Info("Overriding SCHEMA_SYNC_STRATEGY with CLI flag", zap.String("env_value", string(cfg.SchemaSyncStrategy)), zap.String("cli_value", schemaSyncStrategyOverride))
		// Pastikan value dari flag valid sebelum di-assign
		strategyValue := config.SchemaSyncStrategy(strings.ToLower(schemaSyncStrategyOverride))
		switch strategyValue {
		case config.SchemaSyncDropCreate, config.SchemaSyncAlter, config.SchemaSyncNone:
			cfg.SchemaSyncStrategy = strategyValue
		default:
			logger.Log.Warn("Invalid value provided for -schema-strategy flag, ignoring override.",
				zap.String("invalid_value", schemaSyncStrategyOverride),
				zap.String("allowed_values", fmt.Sprintf("%s, %s, %s", config.SchemaSyncDropCreate, config.SchemaSyncAlter, config.SchemaSyncNone)))
		}
	}
	if typeMappingFilePathOverride != "" {
		logger.Log.Info("Overriding TYPE_MAPPING_FILE_PATH with CLI flag", zap.String("env_value", cfg.TypeMappingFilePath), zap.String("cli_value", typeMappingFilePathOverride))
		cfg.TypeMappingFilePath = typeMappingFilePathOverride
	}
	// Tambahkan override flag lain di sini jika ada
}

// logLoadedConfig mencatat konfigurasi final yang digunakan.
func logLoadedConfig(cfg *config.Config) {
	// Ambil password dari config jika ada (untuk logging 'present' atau 'vault')
	srcPassSource := "not set"
	if cfg.SrcDB.Password != "" {
		srcPassSource = "env var"
	} else if cfg.VaultEnabled && cfg.SrcSecretPath != "" {
		srcPassSource = "vault"
	}
	dstPassSource := "not set"
	if cfg.DstDB.Password != "" {
		dstPassSource = "env var"
	} else if cfg.VaultEnabled && cfg.DstSecretPath != "" {
		dstPassSource = "vault"
	}

	logger.Log.Info("Final configuration in use",
		zap.String("sync_direction", cfg.SyncDirection),
		zap.Int("batch_size", cfg.BatchSize),
		zap.Int("workers", cfg.Workers),
		zap.String("schema_strategy", string(cfg.SchemaSyncStrategy)),
		zap.String("type_mapping_file_path", cfg.TypeMappingFilePath),
		zap.String("src_dialect", cfg.SrcDB.Dialect), zap.String("src_host", cfg.SrcDB.Host), zap.Int("src_port", cfg.SrcDB.Port), zap.String("src_user", cfg.SrcDB.User), zap.String("src_password_source", srcPassSource), zap.String("src_dbname", cfg.SrcDB.DBName), zap.String("src_sslmode", cfg.SrcDB.SSLMode),
		zap.String("dst_dialect", cfg.DstDB.Dialect), zap.String("dst_host", cfg.DstDB.Host), zap.Int("dst_port", cfg.DstDB.Port), zap.String("dst_user", cfg.DstDB.User), zap.String("dst_password_source", dstPassSource), zap.String("dst_dbname", cfg.DstDB.DBName), zap.String("dst_sslmode", cfg.DstDB.SSLMode),
		zap.Duration("table_timeout", cfg.TableTimeout), zap.Bool("skip_failed_tables", cfg.SkipFailedTables), zap.Bool("disable_fk_during_sync", cfg.DisableFKDuringSync),
		zap.Int("max_retries", cfg.MaxRetries), zap.Duration("retry_interval", cfg.RetryInterval),
		zap.Int("conn_pool_size", cfg.ConnPoolSize), zap.Duration("conn_max_lifetime", cfg.ConnMaxLifetime),
		zap.Bool("json_logging", cfg.EnableJsonLogging), zap.Bool("enable_pprof", cfg.EnablePprof), zap.Int("metrics_port", cfg.MetricsPort), zap.Bool("debug_mode", cfg.DebugMode),
		zap.Bool("vault_enabled", cfg.VaultEnabled), zap.String("vault_addr", cfg.VaultAddr), zap.Bool("vault_token_present", cfg.VaultToken != ""),
		zap.String("vault_cacert", cfg.VaultCACert), zap.Bool("vault_skip_verify", cfg.VaultSkipVerify),
		zap.String("src_secret_path", cfg.SrcSecretPath), zap.String("src_username_key", cfg.SrcUsernameKey), zap.String("src_password_key", cfg.SrcPasswordKey),
		zap.String("dst_secret_path", cfg.DstSecretPath), zap.String("dst_username_key", cfg.DstUsernameKey), zap.String("dst_password_key", cfg.DstPasswordKey),
	)
}

// loadCredentials memuat kredensial dari env var atau secret manager.
func loadCredentials(
	ctx context.Context,
	cfg *config.Config,
	dbCfg *config.DatabaseConfig,
	dbLabel string,
	secretPath string,
	usernameKey string,
	passwordKey string,
	secretManagers []secrets.SecretManager,
) (*secrets.Credentials, error) {
	log := logger.Log.With(zap.String("db", dbLabel))

	if dbCfg.Password != "" {
		log.Info("Using password directly from environment variable for DB.")
		if dbCfg.User == "" {
			return nil, fmt.Errorf("password provided for %s DB via env var, but username (e.g., %s_USER) is missing", dbLabel, strings.ToUpper(dbLabel))
		}
		return &secrets.Credentials{Username: dbCfg.User, Password: dbCfg.Password}, nil
	}
	log.Info("Password not found in direct environment variable for this DB. Checking secret managers...")

	if secretPath != "" {
		if len(secretManagers) == 0 {
			log.Warn("Secret path is configured, but no secret managers are active/enabled.") // Warning jika path ada tapi manager tidak aktif
		}
		for _, sm := range secretManagers {
			log.Info("Attempting to retrieve credentials from configured secret manager",
				zap.String("manager_type", fmt.Sprintf("%T", sm)),
				zap.String("path_or_id", secretPath),
			)
			getCtx, cancel := context.WithTimeout(ctx, 15*time.Second) // Timeout untuk ambil secret
			creds, err := sm.GetCredentials(getCtx, secretPath, usernameKey, passwordKey)
			cancel()

			if err == nil && creds != nil {
				log.Info("Successfully retrieved credentials from secret manager.")
				if creds.Password == "" {
					log.Error("Retrieved credentials, but password field is empty.", zap.String("manager_type", fmt.Sprintf("%T", sm)))
					return nil, fmt.Errorf("retrieved credentials for %s from %T, but password field is empty", dbLabel, sm)
				}
				// Jika username dari secret kosong, gunakan dari config DB (env var)
				if creds.Username == "" {
					log.Warn("Username field empty in retrieved secret. Falling back to DB config username.",
						zap.String("db_config_user", dbCfg.User))
					creds.Username = dbCfg.User
					if creds.Username == "" {
						log.Error("Password retrieved, but username is missing in both secret and DB config.", zap.String("required_env_var", fmt.Sprintf("%s_USER", strings.ToUpper(dbLabel))))
						return nil, fmt.Errorf("password retrieved for %s, but username is missing in both secret and DB config (e.g., %s_USER)", dbLabel, strings.ToUpper(dbLabel))
					}
				}
				return creds, nil
			}
			log.Warn("Failed to retrieve credentials from secret manager. Trying next if available.",
				zap.String("manager_type", fmt.Sprintf("%T", sm)),
				zap.Error(err), // Log error dari secret manager
			)
		}
		// Jika loop selesai tanpa hasil dari manager manapun
		log.Error("Failed to retrieve credentials from all configured/enabled secret managers for the specified path.", zap.String("path_or_id", secretPath))
	} else {
		log.Info("Secret path is not configured for this DB. Cannot use secret managers.")
	}

	log.Error("Failed to load credentials for DB: Password not found in env vars, and no enabled secret manager provided valid credentials for the configured path (if any).")
	return nil, fmt.Errorf("could not load credentials for %s DB. Ensure %s_PASSWORD or Vault (if enabled via VAULT_ENABLED=true and %s_SECRET_PATH is set) is configured correctly", dbLabel, strings.ToUpper(dbLabel), strings.ToUpper(dbLabel))
}

// connectDBWithRetry mencoba menghubungkan ke DB dengan logika retry.
func connectDBWithRetry(
	ctx context.Context,
	dbCfg config.DatabaseConfig,
	username string,
	password string,
	maxRetries int,
	retryInterval time.Duration,
	dbLabel string,
	metricsStore *metrics.Store,
) (*db.Connector, error) {
	gl := logger.GetGormLogger() // Dapatkan GORM logger wrapper
	var lastErr error

	dsn := buildDSN(dbCfg, username, password)
	if dsn == "" {
		err := fmt.Errorf("could not build DSN for %s DB (unsupported dialect: %s)", dbLabel, dbCfg.Dialect)
		metricsStore.SyncErrorsTotal.WithLabelValues("connection", dbLabel).Inc()
		return nil, err
	}

	for i := 0; i <= maxRetries; i++ {
		attemptStartTime := time.Now()
		// Tunggu sebelum retry (kecuali percobaan pertama)
		if i > 0 {
			logFields := []zap.Field{
				zap.String("db", dbLabel),
				zap.Int("attempt", i+1),
				zap.Int("max_attempts", maxRetries+1),
				zap.Duration("wait_interval", retryInterval),
				zap.NamedError("previous_error", lastErr),
			}
			logger.Log.Warn("Retrying database connection", logFields...)
			timer := time.NewTimer(retryInterval)
			select {
			case <-timer.C:
				// Lanjutkan retry
			case <-ctx.Done():
				timer.Stop()
				errMsg := fmt.Errorf("context cancelled while waiting to retry connection to %s DB (attempt %d): %w; last error: %v", dbLabel, i+1, ctx.Err(), lastErr)
				metricsStore.SyncErrorsTotal.WithLabelValues("connection_cancelled", dbLabel).Inc()
				return nil, errMsg
			}
		}

		// Coba hubungkan
		logger.Log.Info("Attempting to connect",
			zap.String("db", dbLabel),
			zap.String("dialect", dbCfg.Dialect),
			zap.String("host", dbCfg.Host),
			zap.Int("port", dbCfg.Port),
			zap.String("dbname", dbCfg.DBName),
			zap.String("user", username), // Log username juga
			zap.Int("attempt", i+1))

		conn, err := db.New(dbCfg.Dialect, dsn, gl) // Gunakan GORM logger
		if err != nil {
			lastErr = fmt.Errorf("connect attempt %d/%d failed for %s: %w", i+1, maxRetries+1, dbLabel, err)
			continue // Lanjut ke retry berikutnya
		}

		// Coba ping setelah koneksi berhasil dibuat
		pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second) // Timeout pendek untuk ping
		pingErr := conn.Ping(pingCtx)
		pingCancel()
		if pingErr != nil {
			lastErr = fmt.Errorf("ping attempt %d/%d failed for %s: %w (connection error: %v)", i+1, maxRetries+1, dbLabel, pingErr, err)
			_ = conn.Close() // Tutup koneksi yang gagal ping
			continue // Lanjut ke retry berikutnya
		}

		// Koneksi dan ping berhasil
		logger.Log.Info("Database connection successful",
			zap.String("db", dbLabel),
			zap.Duration("connect_duration", time.Since(attemptStartTime)))
		return conn, nil // Kembalikan koneksi yang berhasil
	}

	// Jika semua retry gagal
	logger.Log.Error("Failed to connect to database after all retries",
		zap.String("db", dbLabel),
		zap.Int("attempts", maxRetries+1),
		zap.NamedError("final_error", lastErr))
	metricsStore.SyncErrorsTotal.WithLabelValues("connection_failed", dbLabel).Inc()
	return nil, fmt.Errorf("failed to connect to %s DB (%s at %s:%d) after %d attempts: %w", dbLabel, dbCfg.Dialect, dbCfg.Host, dbCfg.Port, maxRetries+1, lastErr)
}

// buildDSN membangun Data Source Name (DSN) string.
func buildDSN(cfg config.DatabaseConfig, username, password string) string {
	host := cfg.Host
	port := cfg.Port
	dbname := cfg.DBName
	sslmode := strings.ToLower(cfg.SSLMode)

	switch strings.ToLower(cfg.Dialect) {
	case "mysql":
		// Referensi: https://github.com/go-sql-driver/mysql#dsn-data-source-name
		sslParam := "tls=false" // Default jika disable
		if sslmode != "disable" && sslmode != "" {
			// true: use TLS but skip verification (equiv to skip-verify)
			// skip-verify: use TLS, skip verification
			// preferred: use TLS if available, skip verification
			// required: use TLS, fail if not available (verification depends on config)
			// verify-ca: use TLS, verify CA cert
			// verify-full: use TLS, verify CA and hostname
			if sslmode == "skip-verify" || sslmode == "preferred" {
				sslParam = "tls=skip-verify"
			} else if sslmode == "require" || sslmode == "verify-ca" || sslmode == "verify-full" {
				// Untuk verify-ca/verify-full, perlu mendaftarkan config TLS via mysql.RegisterTLSConfig
				// DSN saja mungkin tidak cukup. Kita set ke true atau custom.
				// Jika custom CA/cert diperlukan, pengguna harus mengaturnya.
				// Untuk simple 'require', 'tls=true' mungkin cukup.
				// Untuk keamanan, anggap 'require' perlu verifikasi (lebih aman dari 'skip-verify').
				// Kita gunakan 'tls=true' sebagai representasi umum TLS aktif,
				// tapi beri peringatan jika verifikasi diperlukan.
				sslParam = "tls=true"
				if sslmode == "verify-ca" || sslmode == "verify-full" {
					logger.Log.Warn("MySQL SSL modes 'verify-ca' or 'verify-full' require additional TLS configuration (mysql.RegisterTLSConfig) beyond the DSN for proper verification. Using 'tls=true'.", zap.String("sslmode", sslmode))
				}
			} else {
				// Default ke true jika ada nilai sslmode lain yang tidak dikenal
				sslParam = "tls=true"
			}
		}
		// timeout=10s (connect), readTimeout=60s, writeTimeout=60s
		return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local&timeout=10s&readTimeout=60s&writeTimeout=60s&%s",
			username, password, host, port, dbname, sslParam)
	case "postgres":
		// Referensi: https://pkg.go.dev/github.com/lib/pq#hdr-Connection_String_Parameters
		// sslmode: disable, allow, prefer, require, verify-ca, verify-full
		return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s connect_timeout=10",
			host, port, username, password, dbname, sslmode)
	case "sqlite":
		// Referensi: https://github.com/mattn/go-sqlite3#connection-string
		// file:path?options...
		// cache=shared: bagus untuk konkurensi
		// _foreign_keys=1: aktifkan FK per koneksi (GORM mungkin sudah handle ini?)
		// _journal_mode=WAL: mode jurnal yang lebih baik untuk konkurensi baca/tulis
		// _busy_timeout=5000: tunggu 5 detik jika DB terkunci
		return fmt.Sprintf("file:%s?cache=shared&_foreign_keys=1&_journal_mode=WAL&_busy_timeout=5000", dbname)
	default:
		logger.Log.Error("Cannot build DSN: Unsupported database dialect", zap.String("dialect", cfg.Dialect))
		return ""
	}
}


// processResults memproses hasil sinkronisasi dan menentukan exit code.
// Menggunakan nama field yang sudah diperbaiki dari projectSync.SyncResult
func processResults(results map[string]projectSync.SyncResult) (exitCode int) {
	successCount := 0
	schemaFailCount := 0 // Gabungkan analysis & execution error
	dataFailCount := 0
	constraintFailCount := 0
	skippedCount := 0
	totalTables := len(results)

	if totalTables == 0 {
		logger.Log.Warn("Sync finished, but no tables were found in the source or all were filtered out.")
		return 0 // Exit code 0 karena tidak ada error, hanya tidak ada pekerjaan
	}

	var schemaFailedTables []string // Tabel dengan error analisis atau eksekusi skema
	var dataFailedTables []string   // Tabel dengan error data
	var constraintFailedTables []string // Tabel dengan error constraint saja

	for table, res := range results {
		fields := []zap.Field{
			zap.String("table", table),
			zap.Duration("duration", res.Duration),
			zap.Bool("processing_skipped", res.Skipped),
			zap.Bool("schema_sync_explicitly_disabled", res.SchemaSyncSkipped),
			zap.Int64("rows_synced", res.RowsSynced),
			zap.Int("batches_processed", res.Batches),
		}

		// Tambahkan detail error jika ada
		// PERBAIKAN: Gunakan nama field yang benar
		hasSchemaError := res.SchemaAnalysisError != nil || res.SchemaExecutionError != nil
		hasDataError := res.DataError != nil
		hasConstraintError := res.ConstraintExecutionError != nil

		if res.SchemaAnalysisError != nil { fields = append(fields, zap.NamedError("schema_analysis_error", res.SchemaAnalysisError)) }
		if res.SchemaExecutionError != nil { fields = append(fields, zap.NamedError("schema_execution_error", res.SchemaExecutionError)) }
		if res.DataError != nil { fields = append(fields, zap.NamedError("data_error", res.DataError)) }
		// PERBAIKAN: Gunakan nama field yang benar
		if res.ConstraintExecutionError != nil { fields = append(fields, zap.NamedError("constraint_execution_error", res.ConstraintExecutionError)) }
		if res.Skipped { fields = append(fields, zap.String("skip_reason", res.SkipReason)) }


		level := zap.InfoLevel
		statusMsg := "Table synchronization SUCCEEDED."

		if res.Skipped {
			skippedCount++
			level = zap.WarnLevel
			statusMsg = "Table processing SKIPPED."
		} else if hasSchemaError {
			schemaFailCount++
			schemaFailedTables = append(schemaFailedTables, table)
			level = zap.ErrorLevel
			statusMsg = "Table schema synchronization FAILED (analysis or execution)."
		} else if hasDataError {
			dataFailCount++
			dataFailedTables = append(dataFailedTables, table)
			level = zap.ErrorLevel
			statusMsg = "Table data synchronization FAILED."
		} else if hasConstraintError { // PERBAIKAN: Gunakan hasConstraintError
			constraintFailCount++
			constraintFailedTables = append(constraintFailedTables, table)
			level = zap.WarnLevel
			statusMsg = "Table data sync SUCCEEDED, but applying constraints/indexes FAILED."
			successCount++
		} else {
			successCount++
		}
		logger.Log.Check(level, statusMsg).Write(fields...)
	}

	logger.Log.Info("-------------------- Synchronization Summary --------------------",
		zap.Int("total_tables_evaluated", totalTables),
		zap.Int("tables_fully_successful", successCount),
		zap.Int("tables_with_schema_failures (analysis/execution)", schemaFailCount),
		zap.Int("tables_with_data_failures", dataFailCount),
		zap.Int("tables_with_constraint_failures_only", constraintFailCount),
		zap.Int("tables_skipped_processing", skippedCount),
	)
	if len(schemaFailedTables) > 0 {
		logger.Log.Error("Schema failures occurred for tables", zap.Strings("tables", schemaFailedTables))
	}
	if len(dataFailedTables) > 0 {
		logger.Log.Error("Data sync failures occurred for tables", zap.Strings("tables", dataFailedTables))
	}
	if len(constraintFailedTables) > 0 {
		logger.Log.Warn("Constraint/Index application failures occurred for tables (data was synced)", zap.Strings("tables", constraintFailedTables))
	}

	if schemaFailCount > 0 || dataFailCount > 0 {
		logger.Log.Error("Overall synchronization: COMPLETED WITH CRITICAL ERRORS (Schema or Data Failures).")
		return 1
	}
	if constraintFailCount > 0 {
		logger.Log.Warn("Overall synchronization: COMPLETED WITH CONSTRAINT/INDEX APPLICATION ERRORS (data was synced successfully).")
		return 2
	}
	if skippedCount == totalTables && totalTables > 0 {
		logger.Log.Warn("Overall synchronization: COMPLETED, BUT ALL TABLES WERE SKIPPED (check logs for reasons).")
		return 3
	}
	logger.Log.Info("Overall synchronization: COMPLETED SUCCESSFULLY.")
	return 0
}
