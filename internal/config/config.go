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
	SchemaSyncDropCreate SchemaSyncStrategy = "drop_create" // Default, destructive
	SchemaSyncAlter      SchemaSyncStrategy = "alter"       // Safer, attempts ALTER TABLE (WIP)
	SchemaSyncNone       SchemaSyncStrategy = "none"        // Do not sync schema
)

type Config struct {
	// Sync Settings
	SyncDirection       string             `env:"SYNC_DIRECTION,required"`
	BatchSize           int                `env:"BATCH_SIZE" envDefault:"1000"`
	Workers             int                `env:"WORKERS" envDefault:"8"`
	TableTimeout        time.Duration      `env:"TABLE_TIMEOUT" envDefault:"5m"` // Max time for *one* table (schema+data)
	SchemaSyncStrategy  SchemaSyncStrategy `env:"SCHEMA_SYNC_STRATEGY" envDefault:"drop_create"`
	SkipFailedTables    bool               `env:"SKIP_FAILED_TABLES" envDefault:"false"` // Continue sync if one table fails
	DisableFKDuringSync bool               `env:"DISABLE_FK_DURING_SYNC" envDefault:"false"` // Attempt to disable FKs during data load (MySQL/Postgres) - EXPERIMENTAL

	// Retry Logic (for recoverable errors like batch insert)
	MaxRetries    int           `env:"MAX_RETRIES" envDefault:"3"`
	RetryInterval time.Duration `env:"RETRY_INTERVAL" envDefault:"5s"`

	// Connection Pool
	ConnPoolSize    int           `env:"CONN_POOL_SIZE" envDefault:"20"`
	ConnMaxLifetime time.Duration `env:"CONN_MAX_LIFETIME" envDefault:"1h"`

	// Observability & Debugging
	EnableJsonLogging bool `env:"ENABLE_JSON_LOGGING" envDefault:"false"` // Log in JSON format
	EnablePprof       bool `env:"ENABLE_PPROF" envDefault:"false"`        // Enable pprof endpoints
	MetricsPort       int  `env:"METRICS_PORT" envDefault:"9091"`       // Port for /metrics, /healthz, /readyz, /debug/pprof
	//LogLevel        string `env:"LOG_LEVEL" envDefault:"info"` // TODO: Implement configurable log level

	// Database Configurations
	SrcDB DatabaseConfig `envPrefix:"SRC_"`
	DstDB DatabaseConfig `envPrefix:"DST_"`
}

type DatabaseConfig struct {
	Dialect  string `env:"DIALECT,required"`
	Host     string `env:"HOST,required"`
	Port     int    `env:"PORT,required"`
	User     string `env:"USER,required"`
	Password string `env:"PASSWORD,required"` // Consider external secret management in real prod
	DBName   string `env:"DBNAME,required"`
	SSLMode  string `env:"SSLMODE" envDefault:"disable"` // Use "require" or higher in prod
}

func Load() (*Config, error) {
	cfg := &Config{}
	opts := env.Options{RequiredIfNoDef: true}
	if err := env.ParseWithOptions(cfg, opts); err != nil {
		return nil, fmt.Errorf("config parsing error: %w", err)
	}

	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

func validateConfig(cfg *Config) error {
	// Validasi SyncDirection
	allowedDirections := map[string]bool{
		"mysql-to-mysql":       true,
		"mysql-to-postgres":    true,
		"postgres-to-mysql":    true,
		"postgres-to-postgres": true,
		"sqlite-to-mysql":      true,
		"sqlite-to-postgres":   true,
	}
	dir := strings.ToLower(cfg.SyncDirection)
	if !allowedDirections[dir] {
		return fmt.Errorf("invalid sync direction: %s. Valid options: %v",
			cfg.SyncDirection, getMapKeys(allowedDirections))
	}

	// Validasi SchemaSyncStrategy
	strategy := strings.ToLower(string(cfg.SchemaSyncStrategy))
	if strategy != string(SchemaSyncDropCreate) && strategy != string(SchemaSyncAlter) && strategy != string(SchemaSyncNone) {
		return fmt.Errorf("invalid schema sync strategy: %s. Valid options: %s, %s, %s",
			cfg.SchemaSyncStrategy, SchemaSyncDropCreate, SchemaSyncAlter, SchemaSyncNone)
	}

	// Validasi port
	validatePort := func(port int, name string) error {
		if port < 1 || port > 65535 {
			return fmt.Errorf("invalid %s port: %d", name, port)
		}
		return nil
	}
	if err := validatePort(cfg.SrcDB.Port, "source"); err != nil {
		return err
	}
	if err := validatePort(cfg.DstDB.Port, "destination"); err != nil {
		return err
	}
	if err := validatePort(cfg.MetricsPort, "metrics"); err != nil {
		return err
	}

	// Validasi nilai numerik
	if cfg.BatchSize <= 0 {
		return fmt.Errorf("batch size must be positive")
	}
	if cfg.Workers <= 0 {
		return fmt.Errorf("workers must be positive")
	}
	if cfg.MaxRetries < 0 {
		return fmt.Errorf("max retries cannot be negative")
	}
	if cfg.ConnPoolSize <= 0 {
		return fmt.Errorf("connection pool size must be positive")
	}

	// Validasi SSLMode
	validSSL := map[string]bool{
		"disable":     true,
		"allow":       true,
		"prefer":      true,
		"require":     true,
		"verify-ca":   true,
		"verify-full": true,
	}
	if isSSLModeRelevant(cfg.SrcDB.Dialect) && !validSSL[strings.ToLower(cfg.SrcDB.SSLMode)] {
		return fmt.Errorf("invalid SSL mode for source DB: %s", cfg.SrcDB.SSLMode)
	}
	if isSSLModeRelevant(cfg.DstDB.Dialect) && !validSSL[strings.ToLower(cfg.DstDB.SSLMode)] {
		return fmt.Errorf("invalid SSL mode for destination DB: %s", cfg.DstDB.SSLMode)
	}

	// Validasi kesesuaian SyncDirection dengan dialect
	parts := strings.Split(dir, "-to-")
	srcDialect := strings.ToLower(cfg.SrcDB.Dialect)
	dstDialect := strings.ToLower(cfg.DstDB.Dialect)
	if parts[0] != srcDialect {
		return fmt.Errorf("sync direction source (%s) in %s doesn't match source database dialect (%s)",
			parts[0], cfg.SyncDirection, srcDialect)
	}
	if parts[1] != dstDialect {
		return fmt.Errorf("sync direction destination (%s) in %s doesn't match destination database dialect (%s)",
			parts[1], cfg.SyncDirection, dstDialect)
	}

	return nil
}

func getMapKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys) // Sort for consistent error messages
	return keys
}

func isSSLModeRelevant(dialect string) bool {
	switch strings.ToLower(dialect) {
	case "postgres", "mysql": // MySQL also uses SSL parameters, though DSN format differs
		return true
	default:
		return false
	}
}