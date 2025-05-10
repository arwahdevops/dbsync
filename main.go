// main.go
package main

import (
	"context"
	"errors" // Untuk errors.Is
	"flag"
	"fmt"
	stdlog "log" // Logger standar Go, digunakan sebelum Zap diinisialisasi
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
	dbsync_logger "github.com/arwahdevops/dbsync/internal/logger" // Alias agar jelas
	"github.com/arwahdevops/dbsync/internal/metrics"
	"github.com/arwahdevops/dbsync/internal/secrets"
	"github.com/arwahdevops/dbsync/internal/server"
	projectSync "github.com/arwahdevops/dbsync/internal/sync" // Alias untuk menghindari konflik nama
)

var (
	// Variabel untuk flag CLI
	syncDirectionOverride       string
	batchSizeOverride           int
	workersOverride             int
	schemaSyncStrategyOverride  string
	typeMappingFilePathOverride string
	// Tambahkan flag lain yang ingin di-override di sini
)

const (
	// Konstanta untuk timeout yang lebih terstruktur
	vaultCredentialTimeout = 20 * time.Second
	dbConnectTimeout       = 15 * time.Second // Timeout per upaya koneksi DB
	dbPingTimeout          = 5 * time.Second
	httpServerShutdownTimeout = 10 * time.Second
)

func main() {
	// 0. Inisialisasi awal dan parsing flag
	initFlags() // Pindahkan definisi dan parsing flag ke fungsi terpisah

	// 1. Load .env file (jika ada, akan menimpa environment variables yang sudah ada)
	// `godotenv.Overload` lebih aman karena tidak akan menimpa var env yang sudah diset secara eksplisit.
	// Jika ingin .env menimpa var env sistem, gunakan `godotenv.Load`.
	// Untuk aplikasi CLI, `Overload` biasanya lebih intuitif.
	if err := godotenv.Overload(".env"); err != nil {
		stdlog.Printf("Info: Could not load .env file (this is optional): %v. Relying on explicit environment variables or defaults.\n", err)
	}

	// 2. Konfigurasi awal untuk logger
	// Parsing variabel env yang dibutuhkan untuk konfigurasi logger saja.
	preCfg := &struct {
		EnableJsonLogging bool `env:"ENABLE_JSON_LOGGING" envDefault:"false"`
		DebugMode         bool `env:"DEBUG_MODE" envDefault:"false"`
	}{}
	if err := env.Parse(preCfg); err != nil {
		stdlog.Fatalf("FATAL: Failed to parse pre-configuration for logger: %v", err)
	}

	// 3. Inisialisasi Zap logger global
	if err := dbsync_logger.Init(preCfg.DebugMode, preCfg.EnableJsonLogging); err != nil {
		stdlog.Fatalf("FATAL: Failed to initialize logger: %v", err)
	}
	// Defer sync logger untuk memastikan semua buffer log ditulis sebelum exit.
	defer func() {
		if err := dbsync_logger.Log.Sync(); err != nil {
			// Jangan Fatal di sini karena mungkin sudah dalam proses shutdown karena error lain.
			// Cukup log ke stderr jika Zap sync gagal.
			stdlog.Printf("Error syncing Zap logger: %v\n", err)
		}
	}()

	// Mulai menggunakan Zap logger dari sini
	appLogger := dbsync_logger.Log // Alias untuk kemudahan

	// 4. Load dan validasi konfigurasi aplikasi utama
	cfg, err := config.Load()
	if err != nil {
		appLogger.Fatal("Configuration loading error from environment", zap.Error(err))
	}

	// Terapkan override dari flag CLI setelah konfigurasi dari env dimuat
	applyCliOverrides(cfg, appLogger) // Berikan logger untuk mencatat override

	// Muat pemetaan tipe data dari file (path bisa di-override oleh CLI)
	if err := config.LoadTypeMappings(cfg.TypeMappingFilePath, appLogger); err != nil {
		// Jika LoadTypeMappings mengembalikan error, itu berarti file path dikonfigurasi
		// tapi ada masalah saat memuatnya. Ini harus dianggap fatal.
		appLogger.Fatal("Failed to load custom type mappings from configured file. Please check the file path and its content.",
			zap.String("configured_path", cfg.TypeMappingFilePath),
			zap.Error(err))
	}

	logFinalConfig(cfg, appLogger) // Log konfigurasi final yang akan digunakan

	// 5. Setup context untuk graceful shutdown
	// Context utama untuk seluruh aplikasi.
	// `stop` adalah fungsi yang akan dipanggil untuk membatalkan context ini.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop() // Pastikan stop dipanggil saat main() selesai, untuk membersihkan resources context.

	// 6. Initialize Metrics Store
	metricsStore := metrics.NewMetricsStore()

	// 7. Initialize Secret Manager (Vault)
	vaultMgr, vaultErr := secrets.NewVaultManager(cfg, appLogger)
	if vaultErr != nil {
		if cfg.VaultEnabled { // Hanya fatal jika Vault memang diaktifkan
			appLogger.Fatal("Failed to initialize Vault secret manager", zap.Error(vaultErr))
		} else {
			// Jika Vault tidak diaktifkan, error inisialisasi mungkin karena konfigurasi default yang salah,
			// tapi tidak fatal.
			appLogger.Warn("Could not initialize Vault secret manager (Vault is not enabled or config error)", zap.Error(vaultErr))
		}
	}
	availableSecretManagers := make([]secrets.SecretManager, 0, 1) // Kapasitas 1 jika hanya Vault
	if vaultMgr != nil && vaultMgr.IsEnabled() {
		availableSecretManagers = append(availableSecretManagers, vaultMgr)
		appLogger.Info("Vault secret manager is active.")
	} else {
		appLogger.Info("Vault secret manager is not active.")
	}

	// 8. Load Database Credentials
	appLogger.Info("Loading database credentials...")
	srcCreds, srcCredsErr := loadCredentials(ctx, cfg, &cfg.SrcDB, "source", cfg.SrcSecretPath, cfg.SrcUsernameKey, cfg.SrcPasswordKey, availableSecretManagers, appLogger)
	if srcCredsErr != nil {
		appLogger.Fatal("Failed to load source DB credentials", zap.Error(srcCredsErr))
	}
	dstCreds, dstCredsErr := loadCredentials(ctx, cfg, &cfg.DstDB, "destination", cfg.DstSecretPath, cfg.DstUsernameKey, cfg.DstPasswordKey, availableSecretManagers, appLogger)
	if dstCredsErr != nil {
		appLogger.Fatal("Failed to load destination DB credentials", zap.Error(dstCredsErr))
	}

	// 9. Initialize Database Connections (dengan retry)
	appLogger.Info("Connecting to databases...")
	var srcConn, dstConn *db.Connector
	var dbWg sync.WaitGroup // WaitGroup untuk menunggu koneksi selesai
	var srcErrDB, dstErrDB error // Error spesifik untuk setiap koneksi

	dbWg.Add(2)
	go func() {
		defer dbWg.Done()
		srcConn, srcErrDB = connectDBWithRetry(ctx, cfg.SrcDB, srcCreds.Username, srcCreds.Password, cfg, "source", metricsStore, appLogger)
	}()
	go func() {
		defer dbWg.Done()
		dstConn, dstErrDB = connectDBWithRetry(ctx, cfg.DstDB, dstCreds.Username, dstCreds.Password, cfg, "destination", metricsStore, appLogger)
	}()
	dbWg.Wait() // Tunggu kedua goroutine koneksi selesai

	if srcErrDB != nil {
		appLogger.Fatal("Failed to establish source DB connection", zap.Error(srcErrDB))
	}
	if dstErrDB != nil {
		appLogger.Fatal("Failed to establish destination DB connection", zap.Error(dstErrDB))
	}

	// Defer penutupan koneksi DB
	defer func() {
		appLogger.Info("Closing database connections...")
		if srcConn != nil {
			if err := srcConn.Close(); err != nil {
				appLogger.Error("Error closing source DB connection", zap.Error(err))
			} else {
				appLogger.Info("Source DB connection closed.")
			}
		}
		if dstConn != nil {
			if err := dstConn.Close(); err != nil {
				appLogger.Error("Error closing destination DB connection", zap.Error(err))
			} else {
				appLogger.Info("Destination DB connection closed.")
			}
		}
	}()

	// 10. Optimalkan connection pool untuk setiap koneksi
	appLogger.Info("Optimizing database connection pools.")
	if err := srcConn.Optimize(cfg.ConnPoolSize, cfg.ConnMaxLifetime); err != nil {
		appLogger.Warn("Failed to optimize source DB connection pool", zap.Error(err))
	}
	if err := dstConn.Optimize(cfg.ConnPoolSize, cfg.ConnMaxLifetime); err != nil {
		appLogger.Warn("Failed to optimize destination DB connection pool", zap.Error(err))
	}

	// 11. Start HTTP Server untuk metrik dan health checks
	// Jalankan di goroutine agar tidak memblokir proses sinkronisasi utama.
	// Context `ctx` akan digunakan oleh server HTTP untuk graceful shutdown.
	httpServerWg := &sync.WaitGroup{}
	httpServerWg.Add(1)
	go func() {
		defer httpServerWg.Done()
		server.RunHTTPServer(ctx, cfg, metricsStore, srcConn, dstConn, appLogger)
		appLogger.Info("HTTP server has shut down.")
	}()


	// 12. Buat dan jalankan proses sinkronisasi utama
	appLogger.Info("Initializing synchronization orchestrator...")
	syncer := projectSync.NewOrchestrator(srcConn, dstConn, cfg, appLogger, metricsStore)

	appLogger.Info("Starting main database synchronization process...")
	results := syncer.Run(ctx) // results adalah map[string]projectSync.SyncResult

	// 13. Proses dan log hasil sinkronisasi
	appLogger.Info("Main synchronization process finished. Processing results...")
	exitCode := processSyncResults(results, appLogger)

	// 14. Tunggu sinyal shutdown atau penyelesaian
	// Jika sinkronisasi selesai tanpa interupsi eksternal (Ctrl+C), kita masih ingin
	// menunggu sinyal tersebut untuk shutdown server HTTP, atau jika ada proses background lain.
	// Jika `syncer.Run(ctx)` selesai karena `ctx` dibatalkan, blok ini akan langsung dilewati.
	if ctx.Err() == nil { // Jika context *belum* dibatalkan
		appLogger.Info("Synchronization logic completed. Waiting for shutdown signal (Ctrl+C or SIGTERM) to stop HTTP server...")
		<-ctx.Done() // Blok hingga sinyal diterima atau `stop()` dipanggil secara manual
		appLogger.Info("Shutdown signal received after sync completion.")
	} else {
		appLogger.Info("Shutdown signal received during or after synchronization process. Proceeding with cleanup.", zap.Error(ctx.Err()))
	}

	// Panggil `stop()` secara eksplisit untuk memastikan semua turunan context juga dibatalkan,
	// meskipun `signal.NotifyContext` juga melakukannya. Ini juga berguna jika ada pembatalan manual.
	stop()

	// Tunggu server HTTP selesai shutdown (jika masih berjalan)
	appLogger.Info("Waiting for HTTP server to complete shutdown...")
	httpServerWg.Wait() // Tunggu goroutine server.RunHTTPServer selesai

	appLogger.Info("Application shutdown complete.", zap.Int("exit_code", exitCode))
	os.Exit(exitCode)
}

// initFlags mendefinisikan dan mem-parse flag command-line.
func initFlags() {
	flag.StringVar(&syncDirectionOverride, "sync-direction", "", "Override SYNC_DIRECTION (e.g., mysql-to-postgres)")
	flag.IntVar(&batchSizeOverride, "batch-size", 0, "Override BATCH_SIZE (must be > 0)")
	flag.IntVar(&workersOverride, "workers", 0, "Override WORKERS (must be > 0)")
	flag.StringVar(&schemaSyncStrategyOverride, "schema-strategy", "", "Override SCHEMA_SYNC_STRATEGY (drop_create, alter, none)")
	flag.StringVar(&typeMappingFilePathOverride, "type-map-file", "", "Override TYPE_MAPPING_FILE_PATH")
	// ... tambahkan definisi flag lainnya di sini ...
	flag.Parse()
}

// applyCliOverrides menerapkan nilai dari flag CLI ke struct Config.
// Menggunakan logger untuk mencatat override yang dilakukan.
func applyCliOverrides(cfg *config.Config, logger *zap.Logger) {
	if syncDirectionOverride != "" {
		logger.Info("Overriding SYNC_DIRECTION with CLI flag", zap.String("env_value", cfg.SyncDirection), zap.String("cli_value", syncDirectionOverride))
		cfg.SyncDirection = syncDirectionOverride
	}
	if batchSizeOverride > 0 {
		logger.Info("Overriding BATCH_SIZE with CLI flag", zap.Int("env_value", cfg.BatchSize), zap.Int("cli_value", batchSizeOverride))
		cfg.BatchSize = batchSizeOverride
	}
	if workersOverride > 0 {
		logger.Info("Overriding WORKERS with CLI flag", zap.Int("env_value", cfg.Workers), zap.Int("cli_value", workersOverride))
		cfg.Workers = workersOverride
	}
	if schemaSyncStrategyOverride != "" {
		logger.Info("Overriding SCHEMA_SYNC_STRATEGY with CLI flag", zap.String("env_value", string(cfg.SchemaSyncStrategy)), zap.String("cli_value", schemaSyncStrategyOverride))
		strategyValue := config.SchemaSyncStrategy(strings.ToLower(schemaSyncStrategyOverride))
		switch strategyValue {
		case config.SchemaSyncDropCreate, config.SchemaSyncAlter, config.SchemaSyncNone:
			cfg.SchemaSyncStrategy = strategyValue
		default:
			logger.Warn("Invalid value provided for -schema-strategy flag, ignoring override.",
				zap.String("invalid_value", schemaSyncStrategyOverride),
				zap.String("allowed_values", fmt.Sprintf("%s, %s, %s", config.SchemaSyncDropCreate, config.SchemaSyncAlter, config.SchemaSyncNone)))
		}
	}
	if typeMappingFilePathOverride != "" {
		logger.Info("Overriding TYPE_MAPPING_FILE_PATH with CLI flag", zap.String("env_value", cfg.TypeMappingFilePath), zap.String("cli_value", typeMappingFilePathOverride))
		cfg.TypeMappingFilePath = typeMappingFilePathOverride
	}
	// ... terapkan override flag lainnya ...
}

// logFinalConfig mencatat konfigurasi final yang digunakan.
func logFinalConfig(cfg *config.Config, logger *zap.Logger) {
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

	logger.Info("Final configuration in use",
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
	logger *zap.Logger,
) (*secrets.Credentials, error) {
	log := logger.With(zap.String("db_config_label", dbLabel))

	if dbCfg.User == "" {
		return nil, fmt.Errorf("username (e.g., %s_USER) for %s DB is missing in configuration", strings.ToUpper(dbLabel), dbLabel)
	}

	if dbCfg.Password != "" {
		log.Info("Using password directly from environment variable for DB.")
		return &secrets.Credentials{Username: dbCfg.User, Password: dbCfg.Password}, nil
	}
	log.Info("Password not found in direct environment variable for this DB. Checking secret managers...")

	if !cfg.VaultEnabled || secretPath == "" { // Periksa juga apakah Vault di-enable
		log.Info("Vault is not enabled or secret path is not configured for this DB. Cannot use Vault.", zap.Bool("vault_enabled_cfg", cfg.VaultEnabled), zap.String("secret_path_cfg", secretPath))
		// Jika tidak ada password & Vault tidak bisa digunakan, ini error.
		return nil, fmt.Errorf("password for %s DB not found in environment variables, and Vault is not enabled or secret path is not configured", dbLabel)
	}


	if len(secretManagers) == 0 {
		log.Error("Vault is enabled and secret path is configured, but no secret managers were successfully initialized or made available.")
		return nil, fmt.Errorf("vault is enabled for %s DB, but no active secret manager found (initialization might have failed)", dbLabel)
	}

	for _, sm := range secretManagers { // Harusnya hanya ada VaultManager di sini jika hanya Vault yang diimplementasikan
		log.Info("Attempting to retrieve credentials from configured secret manager.",
			zap.String("manager_type", fmt.Sprintf("%T", sm)),
			zap.String("path_or_id", secretPath),
		)
		getCtx, cancel := context.WithTimeout(ctx, vaultCredentialTimeout)
		creds, err := sm.GetCredentials(getCtx, secretPath, usernameKey, passwordKey)
		cancel() // Selalu panggil cancel

		if err == nil && creds != nil {
			if creds.Password == "" {
				log.Error("Retrieved credentials from secret manager, but password field is empty.")
				return nil, fmt.Errorf("password for %s DB from %T is empty", dbLabel, sm)
			}
			// Jika username dari secret kosong, gunakan dari config DB (env var)
			// Ini sudah dicek di awal bahwa dbCfg.User tidak kosong.
			if creds.Username == "" {
				log.Warn("Username field empty in retrieved secret. Falling back to DB config username from environment variable.",
					zap.String("db_config_user", dbCfg.User))
				creds.Username = dbCfg.User
			}
			log.Info("Successfully retrieved credentials from secret manager.")
			return creds, nil
		}
		log.Warn("Failed to retrieve credentials from a secret manager. Trying next if available.",
			zap.String("manager_type", fmt.Sprintf("%T", sm)),
			zap.Error(err),
		)
	}

	// Jika loop selesai tanpa hasil
	log.Error("Failed to retrieve credentials from all configured/enabled secret managers for the specified path.", zap.String("path_or_id", secretPath))
	return nil, fmt.Errorf("could not load credentials for %s DB using Vault. Path: '%s'", dbLabel, secretPath)
}

// connectDBWithRetry mencoba menghubungkan ke DB dengan logika retry.
func connectDBWithRetry(
	ctx context.Context,
	dbCfg config.DatabaseConfig,
	username string,
	password string,
	appCfg *config.Config, // Pass cfg aplikasi utama untuk MaxRetries, RetryInterval
	dbLabel string,
	metricsStore *metrics.Store,
	logger *zap.Logger,
) (*db.Connector, error) {
	gl := dbsync_logger.GetGormLogger() // Dapatkan GORM logger wrapper
	var lastErr error

	dsn := buildDSN(dbCfg, username, password, logger) // Berikan logger ke buildDSN
	if dsn == "" {
		err := fmt.Errorf("could not build DSN for %s DB (unsupported dialect: %s)", dbLabel, dbCfg.Dialect)
		metricsStore.SyncErrorsTotal.WithLabelValues("connection", dbLabel).Inc()
		return nil, err
	}

	for i := 0; i <= appCfg.MaxRetries; i++ {
		attemptStartTime := time.Now()
		// Tunggu sebelum retry (kecuali percobaan pertama)
		if i > 0 {
			logFields := []zap.Field{
				zap.String("db", dbLabel),
				zap.Int("attempt", i+1),
				zap.Int("max_attempts", appCfg.MaxRetries+1),
				zap.Duration("wait_interval", appCfg.RetryInterval),
				zap.NamedError("previous_error", lastErr),
			}
			logger.Warn("Retrying database connection", logFields...)
			timer := time.NewTimer(appCfg.RetryInterval)
			select {
			case <-timer.C:
			case <-ctx.Done(): // Periksa context utama
				timer.Stop()
				errMsg := fmt.Errorf("context cancelled while waiting to retry connection to %s DB (attempt %d/%d): %w; last error: %v", dbLabel, i+1, appCfg.MaxRetries+1, ctx.Err(), lastErr)
				metricsStore.SyncErrorsTotal.WithLabelValues("connection_cancelled", dbLabel).Inc()
				return nil, errMsg
			}
		}

		logger.Info("Attempting to connect to database.",
			zap.String("db", dbLabel),
			zap.String("dialect", dbCfg.Dialect),
			zap.String("host", dbCfg.Host),
			zap.Int("port", dbCfg.Port),
			zap.String("dbname", dbCfg.DBName),
			zap.String("user", username),
			zap.Int("attempt", i+1),
			zap.Int("max_attempts", appCfg.MaxRetries+1))

		// Context dengan timeout untuk satu upaya koneksi
		connectAttemptCtx, connectAttemptCancel := context.WithTimeout(ctx, dbConnectTimeout)
		conn, err := db.New(dbCfg.Dialect, dsn, gl) // db.New tidak menerima context, GORM handle di internal
		connectAttemptCancel() // Cancel setelah db.New selesai atau timeout

		if err != nil {
			if errors.Is(connectAttemptCtx.Err(), context.DeadlineExceeded) { // Cek apakah timeout upaya koneksi
				lastErr = fmt.Errorf("connect attempt %d for %s DB timed out after %v: %w", i+1, dbLabel, dbConnectTimeout, err)
			} else if errors.Is(ctx.Err(), context.Canceled) { // Cek apakah context utama dibatalkan
				lastErr = fmt.Errorf("context cancelled during connect attempt %d for %s DB: %w; underlying error: %v", i+1, dbLabel, ctx.Err(), err)
				metricsStore.SyncErrorsTotal.WithLabelValues("connection_cancelled", dbLabel).Inc()
				return nil, lastErr // Keluar segera jika context utama dibatalkan
			} else {
				lastErr = fmt.Errorf("connect attempt %d for %s DB failed: %w", i+1, dbLabel, err)
			}
			continue
		}

		pingAttemptCtx, pingAttemptCancel := context.WithTimeout(ctx, dbPingTimeout)
		pingErr := conn.Ping(pingAttemptCtx)
		pingAttemptCancel()

		if pingErr != nil {
			_ = conn.Close() // Tutup koneksi yang gagal ping
			if errors.Is(pingAttemptCtx.Err(), context.DeadlineExceeded) {
				lastErr = fmt.Errorf("ping attempt %d for %s DB timed out after %v: %w", i+1, dbLabel, dbPingTimeout, pingErr)
			} else if errors.Is(ctx.Err(), context.Canceled) {
				lastErr = fmt.Errorf("context cancelled during ping attempt %d for %s DB: %w; underlying error: %v", i+1, dbLabel, ctx.Err(), pingErr)
				metricsStore.SyncErrorsTotal.WithLabelValues("connection_cancelled", dbLabel).Inc()
				return nil, lastErr
			} else {
				lastErr = fmt.Errorf("ping attempt %d for %s DB failed: %w", i+1, dbLabel, pingErr)
			}
			continue
		}

		logger.Info("Database connection successful.",
			zap.String("db", dbLabel),
			zap.Duration("connect_duration", time.Since(attemptStartTime)))
		return conn, nil
	}

	logger.Error("Failed to connect to database after all retries.",
		zap.String("db", dbLabel),
		zap.Int("attempts_made", appCfg.MaxRetries+1),
		zap.NamedError("final_error", lastErr))
	metricsStore.SyncErrorsTotal.WithLabelValues("connection_failed", dbLabel).Inc()
	return nil, fmt.Errorf("failed to connect to %s DB (%s at %s:%d) after %d attempts: %w", dbLabel, dbCfg.Dialect, dbCfg.Host, dbCfg.Port, appCfg.MaxRetries+1, lastErr)
}


// buildDSN membangun Data Source Name (DSN) string.
func buildDSN(cfg config.DatabaseConfig, username, password string, logger *zap.Logger) string {
	host := cfg.Host
	port := cfg.Port
	dbname := cfg.DBName
	sslmode := strings.ToLower(cfg.SSLMode)

	switch strings.ToLower(cfg.Dialect) {
	case "mysql":
		sslParam := "tls=false"
		if sslmode != "disable" && sslmode != "" {
			// Mapping SSL mode ke parameter DSN MySQL
			// https://github.com/go-sql-driver/mysql#tls
			// "true", "false", "skip-verify", "preferred", atau nama profil TLS yang terdaftar.
			switch sslmode {
			case "require", "verify-ca", "verify-full": // Untuk ini, idealnya gunakan profil TLS kustom.
				sslParam = "tls=true"                 // Minimal TLS, verifikasi tergantung server & client config.
				if sslmode == "verify-ca" || sslmode == "verify-full" {
					logger.Warn("MySQL SSL modes 'verify-ca' or 'verify-full' might require a pre-registered TLS config name in the DSN for full effect (e.g., 'customSSLProfile') instead of just 'tls=true'. Ensure client and server are properly configured for verification.",
						zap.String("sslmode_used", sslmode))
				}
			case "allow", "prefer": // MySQL tidak punya 'allow'. 'preferred' paling dekat.
				sslParam = "tls=preferred" // Coba TLS, fallback ke non-TLS jika gagal.
			case "skip-verify":
				sslParam = "tls=skip-verify"
			default: // Jika ada nilai lain, default ke 'tls=true' sebagai tindakan aman.
				sslParam = "tls=true"
				logger.Warn("Unknown MySQL SSL mode, defaulting DSN tls parameter to 'true'.", zap.String("unknown_sslmode", sslmode))
			}
		}
		return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local&timeout=10s&readTimeout=60s&writeTimeout=60s&%s",
			username, password, host, port, dbname, sslParam)
	case "postgres":
		return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s connect_timeout=10",
			host, port, username, password, dbname, sslmode)
	case "sqlite":
		return fmt.Sprintf("file:%s?cache=shared&_foreign_keys=1&_journal_mode=WAL&_busy_timeout=5000", dbname)
	default:
		logger.Error("Cannot build DSN: Unsupported database dialect", zap.String("dialect", cfg.Dialect))
		return ""
	}
}

// processSyncResults memproses hasil sinkronisasi dan menentukan exit code.
func processSyncResults(results map[string]projectSync.SyncResult, logger *zap.Logger) (exitCode int) {
	successCount := 0
	schemaFailCount := 0
	dataFailCount := 0
	constraintFailCount := 0
	skippedCount := 0
	totalTables := len(results)

	if totalTables == 0 {
		logger.Warn("Sync finished, but no tables were found in the source or all were filtered out.")
		return 0 // Exit code 0 karena tidak ada error, hanya tidak ada pekerjaan
	}

	var schemaFailedTables, dataFailedTables, constraintFailedTables []string

	for table, res := range results {
		fields := []zap.Field{
			zap.String("table", table),
			zap.Duration("duration", res.Duration),
			zap.Bool("processing_skipped_overall", res.Skipped),
			zap.Bool("schema_sync_explicitly_disabled", res.SchemaSyncSkipped),
			zap.Int64("rows_synced", res.RowsSynced),
			zap.Int("batches_processed", res.Batches),
		}

		hasSchemaError := res.SchemaAnalysisError != nil || res.SchemaExecutionError != nil
		hasDataError := res.DataError != nil
		hasConstraintError := res.ConstraintExecutionError != nil

		if res.SchemaAnalysisError != nil { fields = append(fields, zap.NamedError("schema_analysis_error", res.SchemaAnalysisError)) }
		if res.SchemaExecutionError != nil { fields = append(fields, zap.NamedError("schema_execution_error", res.SchemaExecutionError)) }
		if res.DataError != nil { fields = append(fields, zap.NamedError("data_error", res.DataError)) }
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
		} else if hasConstraintError {
			constraintFailCount++
			constraintFailedTables = append(constraintFailedTables, table)
			level = zap.WarnLevel
			statusMsg = "Table data sync SUCCEEDED, but applying constraints/indexes FAILED."
			successCount++ // Tetap dihitung sukses jika hanya constraint yg gagal
		} else {
			successCount++
		}
		logger.Log(level, statusMsg, fields...)
	}

	logger.Info("-------------------- Synchronization Summary --------------------",
		zap.Int("total_tables_evaluated", totalTables),
		zap.Int("tables_fully_successful_or_data_success_with_constraint_fail", successCount),
		zap.Int("tables_with_schema_failures", schemaFailCount),
		zap.Int("tables_with_data_failures", dataFailCount),
		zap.Int("tables_with_only_constraint_failures", constraintFailCount), // Diperjelas
		zap.Int("tables_skipped_overall_processing", skippedCount),
	)
	if len(schemaFailedTables) > 0 {
		logger.Error("Schema failures (analysis/execution) occurred for tables:", zap.Strings("tables", schemaFailedTables))
	}
	if len(dataFailedTables) > 0 {
		logger.Error("Data sync failures occurred for tables:", zap.Strings("tables", dataFailedTables))
	}
	if len(constraintFailedTables) > 0 {
		logger.Warn("Constraint/Index application failures occurred for tables (data was synced successfully):", zap.Strings("tables", constraintFailedTables))
	}

	// Menentukan exit code berdasarkan tingkat keparahan error
	if schemaFailCount > 0 || dataFailCount > 0 {
		logger.Error("Overall synchronization: COMPLETED WITH CRITICAL ERRORS (Schema or Data Failures).")
		return 1 // Error paling parah
	}
	if constraintFailCount > 0 {
		logger.Warn("Overall synchronization: COMPLETED WITH CONSTRAINT/INDEX APPLICATION ERRORS (data was synced successfully).")
		return 2 // Error, tapi tidak se-kritis kegagalan data/skema
	}
	if skippedCount == totalTables && totalTables > 0 {
		logger.Warn("Overall synchronization: COMPLETED, BUT ALL TABLES WERE SKIPPED (check logs for reasons).")
		return 3 // Bukan error, tapi mungkin tidak sesuai harapan
	}
	if skippedCount > 0 { // Jika ada yang sukses dan ada yang diskip
		logger.Info("Overall synchronization: COMPLETED (some tables may have been skipped).")
		return 0 // Anggap sukses jika ada yang berhasil dan tidak ada error kritis
	}

	logger.Info("Overall synchronization: COMPLETED SUCCESSFULLY.")
	return 0
}
