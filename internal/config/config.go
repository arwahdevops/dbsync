package config

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/caarlos0/env/v8"
)

type SchemaSyncStrategy string

const (
	SchemaSyncDropCreate SchemaSyncStrategy = "drop_create"
	SchemaSyncAlter      SchemaSyncStrategy = "alter"
	SchemaSyncNone       SchemaSyncStrategy = "none"
)

type Config struct {
	// Sync Settings
	SyncDirection       string             `env:"SYNC_DIRECTION,required"`
	BatchSize           int                `env:"BATCH_SIZE" envDefault:"1000"`
	Workers             int                `env:"WORKERS" envDefault:"8"`
	TableTimeout        time.Duration      `env:"TABLE_TIMEOUT" envDefault:"5m"`
	SchemaSyncStrategy  SchemaSyncStrategy `env:"SCHEMA_SYNC_STRATEGY" envDefault:"drop_create"`
	SkipFailedTables    bool               `env:"SKIP_FAILED_TABLES" envDefault:"false"`
	DisableFKDuringSync bool               `env:"DISABLE_FK_DURING_SYNC" envDefault:"false"`

	// Retry Logic
	MaxRetries    int           `env:"MAX_RETRIES" envDefault:"3"`
	RetryInterval time.Duration `env:"RETRY_INTERVAL" envDefault:"5s"`

	// Connection Pool
	ConnPoolSize    int           `env:"CONN_POOL_SIZE" envDefault:"20"`
	ConnMaxLifetime time.Duration `env:"CONN_MAX_LIFETIME" envDefault:"1h"`

	// Observability & Debugging
	EnableJsonLogging bool `env:"ENABLE_JSON_LOGGING" envDefault:"false"`
	EnablePprof       bool `env:"ENABLE_PPROF" envDefault:"false"`
	MetricsPort       int  `env:"METRICS_PORT" envDefault:"9091"`
	DebugMode         bool `env:"DEBUG_MODE" envDefault:"false"` // Variabel untuk logger init

	// --- Database Configurations ---
	SrcDB DatabaseConfig `envPrefix:"SRC_"`
	DstDB DatabaseConfig `envPrefix:"DST_"`

	// --- Optional: Vault Secret Management ---
	VaultEnabled     bool   `env:"VAULT_ENABLED" envDefault:"false"`
	VaultAddr        string `env:"VAULT_ADDR" envDefault:"https://127.0.0.1:8200"` // Vault address
	VaultToken       string `env:"VAULT_TOKEN"`                                   // Vault token (jika menggunakan auth token)
	VaultCACert      string `env:"VAULT_CACERT"`                                  // Path to CA cert for Vault TLS
	VaultSkipVerify  bool   `env:"VAULT_SKIP_VERIFY" envDefault:"false"`          // Skip TLS verification for Vault (TIDAK AMAN)
	SrcSecretPath    string `env:"SRC_SECRET_PATH"`                               // Path to source secret in Vault (e.g., secret/data/dbsync/source)
	DstSecretPath    string `env:"DST_SECRET_PATH"`                               // Path to destination secret in Vault
	SrcUsernameKey   string `env:"SRC_USERNAME_KEY" envDefault:"username"`        // Key for username in source secret data
	SrcPasswordKey   string `env:"SRC_PASSWORD_KEY" envDefault:"password"`        // Key for password in source secret data
	DstUsernameKey   string `env:"DST_USERNAME_KEY" envDefault:"username"`        // Key for username in dest secret data
	DstPasswordKey   string `env:"DST_PASSWORD_KEY" envDefault:"password"`        // Key for password in dest secret data
	// TODO: Add support for other Vault auth methods (AppRole, K8s, etc.)
}

type DatabaseConfig struct {
	Dialect  string `env:"DIALECT,required"`
	Host     string `env:"HOST,required"`
	Port     int    `env:"PORT,required"`
	User     string `env:"USER,required"` // Username default, bisa di-override oleh secret manager
	Password string `env:"PASSWORD" envDefault:""` // Password dari env var (opsional, prioritas rendah)
	DBName   string `env:"DBNAME,required"`
	SSLMode  string `env:"SSLMODE" envDefault:"disable"`
}

func Load() (*Config, error) {
	cfg := &Config{}
	// Gunakan RequiredIfNoDef HANYA untuk field yang WAJIB diisi SELALU,
	// Password sekarang opsional di level ini.
	opts := env.Options{RequiredIfNoDef: true}
	if err := env.ParseWithOptions(cfg, opts); err != nil {
		return nil, fmt.Errorf("config parsing error: %w", err)
	}

	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	// Validasi tambahan jika Vault diaktifkan
	if cfg.VaultEnabled {
		if cfg.VaultToken == "" {
			// Ini mungkin OK jika menggunakan metode auth lain, perlu validasi lebih lanjut
			// return nil, fmt.Errorf("Vault is enabled but VAULT_TOKEN is not set")
			fmt.Println("WARN: Vault is enabled but VAULT_TOKEN is not set. Ensure another auth method is configured.")
		}
		if cfg.SrcSecretPath == "" {
			return nil, fmt.Errorf("Vault is enabled but SRC_SECRET_PATH is not set")
		}
		if cfg.DstSecretPath == "" {
			return nil, fmt.Errorf("Vault is enabled but DST_SECRET_PATH is not set")
		}
	}


	return cfg, nil
}

func validateConfig(cfg *Config) error {
	// Validasi SyncDirection
	allowedDirections := map[string]bool{ "mysql-to-mysql": true, "mysql-to-postgres": true, "postgres-to-mysql": true, "postgres-to-postgres": true, "sqlite-to-mysql": true, "sqlite-to-postgres": true, }
	dir := strings.ToLower(cfg.SyncDirection); if !allowedDirections[dir] { return fmt.Errorf("invalid sync direction: %s. Valid: %v", cfg.SyncDirection, getMapKeys(allowedDirections)) }
	// Validasi SchemaSyncStrategy
	strategy := strings.ToLower(string(cfg.SchemaSyncStrategy)); if strategy != string(SchemaSyncDropCreate) && strategy != string(SchemaSyncAlter) && strategy != string(SchemaSyncNone) { return fmt.Errorf("invalid schema sync strategy: %s. Valid: %s, %s, %s", cfg.SchemaSyncStrategy, SchemaSyncDropCreate, SchemaSyncAlter, SchemaSyncNone) }
	// Validasi port
	validatePort := func(port int, name string) error { if port < 1 || port > 65535 { return fmt.Errorf("invalid %s port: %d", name, port) }; return nil }; if err := validatePort(cfg.SrcDB.Port, "source"); err != nil { return err }; if err := validatePort(cfg.DstDB.Port, "destination"); err != nil { return err }; if err := validatePort(cfg.MetricsPort, "metrics"); err != nil { return err }
	// Validasi nilai numerik
	if cfg.BatchSize <= 0 { return fmt.Errorf("batch size must be positive") }; if cfg.Workers <= 0 { return fmt.Errorf("workers must be positive") }; if cfg.MaxRetries < 0 { return fmt.Errorf("max retries cannot be negative") }; if cfg.ConnPoolSize <= 0 { return fmt.Errorf("connection pool size must be positive") }
	// Validasi SSLMode
	validSSL := map[string]bool{ "disable": true, "allow": true, "prefer": true, "require": true, "verify-ca": true, "verify-full": true, }; if isSSLModeRelevant(cfg.SrcDB.Dialect) && !validSSL[strings.ToLower(cfg.SrcDB.SSLMode)] { return fmt.Errorf("invalid SSL mode for source DB: %s", cfg.SrcDB.SSLMode) }; if isSSLModeRelevant(cfg.DstDB.Dialect) && !validSSL[strings.ToLower(cfg.DstDB.SSLMode)] { return fmt.Errorf("invalid SSL mode for destination DB: %s", cfg.DstDB.SSLMode) }
	// Validasi kesesuaian SyncDirection dengan dialect
	parts := strings.Split(dir, "-to-"); srcDialect := strings.ToLower(cfg.SrcDB.Dialect); dstDialect := strings.ToLower(cfg.DstDB.Dialect); if parts[0] != srcDialect { return fmt.Errorf("sync direction source (%s) mismatch source dialect (%s)", parts[0], srcDialect) }; if parts[1] != dstDialect { return fmt.Errorf("sync direction destination (%s) mismatch destination dialect (%s)", parts[1], dstDialect) }

	return nil
}

func getMapKeys(m map[string]bool) []string { keys := make([]string, 0, len(m)); for k := range m { keys = append(keys, k) }; sort.Strings(keys); return keys }
func isSSLModeRelevant(dialect string) bool { switch strings.ToLower(dialect) { case "postgres", "mysql": return true; default: return false } }
