package config

import (
	// Impor yang tidak lagi dibutuhkan seperti "encoding/json", "os", "errors" sudah dihapus
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/caarlos0/env/v8"
	"go.uber.org/zap"
)

type SchemaSyncStrategy string

const (
	SchemaSyncDropCreate SchemaSyncStrategy = "drop_create"
	SchemaSyncAlter      SchemaSyncStrategy = "alter"
	SchemaSyncNone       SchemaSyncStrategy = "none"
)

type ModifierHandlingStrategy string

const (
	ModifierHandlingApplySource      ModifierHandlingStrategy = "apply_source"
	ModifierHandlingUseTargetDefined ModifierHandlingStrategy = "use_target_defined"
	ModifierHandlingIgnoreSource     ModifierHandlingStrategy = "ignore_source"
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
	DebugMode         bool `env:"DEBUG_MODE" envDefault:"false"`

	// --- Database Configurations ---
	SrcDB DatabaseConfig `envPrefix:"SRC_"`
	DstDB DatabaseConfig `envPrefix:"DST_"`

	// --- Optional: Vault Secret Management ---
	VaultEnabled    bool   `env:"VAULT_ENABLED" envDefault:"false"`
	VaultAddr       string `env:"VAULT_ADDR" envDefault:"https://127.0.0.1:8200"`
	VaultToken      string `env:"VAULT_TOKEN"`
	VaultCACert     string `env:"VAULT_CACERT"`
	VaultSkipVerify bool   `env:"VAULT_SKIP_VERIFY" envDefault:"false"`
	SrcSecretPath   string `env:"SRC_SECRET_PATH"`
	DstSecretPath   string `env:"DST_SECRET_PATH"`
	SrcUsernameKey  string `env:"SRC_USERNAME_KEY" envDefault:"username"`
	SrcPasswordKey  string `env:"SRC_PASSWORD_KEY" envDefault:"password"`
	DstUsernameKey  string `env:"DST_USERNAME_KEY" envDefault:"username"`
	DstPasswordKey  string `env:"DST_PASSWORD_KEY" envDefault:"password"`

	// TypeMappingFilePath string `env:"TYPE_MAPPING_FILE_PATH" envDefault:""` // DIHAPUS - Kustomisasi tipe eksternal dinonaktifkan sementara.
}

type DatabaseConfig struct {
	Dialect  string `env:"DIALECT,required"`
	Host     string `env:"HOST,required"`
	Port     int    `env:"PORT,required"`
	User     string `env:"USER,required"`
	Password string `env:"PASSWORD" envDefault:""`
	DBName   string `env:"DBNAME,required"`
	SSLMode  string `env:"SSLMODE" envDefault:"disable"`
}

type StandardTypeMapping struct {
	TargetType        string                   `json:"target_type"`
	ModifierHandling  ModifierHandlingStrategy `json:"modifier_handling,omitempty"`
	PostgresUsingExpr string                   `json:"postgres_using_expr,omitempty"`
}

type TypeMappingConfigEntry struct {
	SourceDialect   string                         `json:"source_dialect"`
	TargetDialect   string                         `json:"target_dialect"`
	Mappings        map[string]StandardTypeMapping `json:"mappings"`
	SpecialMappings []SpecialMapping               `json:"special_mappings"`
}

type SpecialMapping struct {
	SourceTypePattern string                   `json:"source_type_pattern"`
	TargetType        string                   `json:"target_type"`
	ModifierHandling  ModifierHandlingStrategy `json:"modifier_handling,omitempty"`
	PostgresUsingExpr string                   `json:"postgres_using_expr,omitempty"`
}

type AllTypeMappings struct {
	Mappings []TypeMappingConfigEntry `json:"type_mappings"`
}

var loadedTypeMappingsCache *AllTypeMappings

func Load() (*Config, error) {
	cfg := &Config{}
	opts := env.Options{RequiredIfNoDef: true}
	if err := env.ParseWithOptions(cfg, opts); err != nil {
		return nil, fmt.Errorf("config parsing error: %w", err)
	}

	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	if cfg.VaultEnabled {
		if cfg.VaultToken == "" {
			// Peringatan dipindahkan ke main.go setelah logger diinisialisasi
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

// LoadTypeMappings sekarang menginisialisasi dari default internal.
// Kustomisasi melalui file eksternal saat ini dinonaktifkan.
func LoadTypeMappings(logger *zap.Logger) error {
	if loadedTypeMappingsCache != nil {
		logger.Debug("Internal type mappings already initialized, skipping re-initialization.")
		return nil
	}

	logger.Info("Initializing internal default type mappings. External type mapping files are currently not supported.")
	mappings := defaultTypeMappingsProvider() // Panggil fungsi provider dari default_typemaps.go

	// Validasi dan set default untuk ModifierHandling
	for i, entry := range mappings.Mappings {
		for key, stdMap := range entry.Mappings {
			if stdMap.ModifierHandling == "" {
				stdMap.ModifierHandling = ModifierHandlingApplySource
			}
			isValidModifierHandling := false
			for _, validMH := range []ModifierHandlingStrategy{ModifierHandlingApplySource, ModifierHandlingUseTargetDefined, ModifierHandlingIgnoreSource} {
				if stdMap.ModifierHandling == validMH {
					isValidModifierHandling = true
					break
				}
			}
			if !isValidModifierHandling {
				logger.Warn("Invalid modifier_handling in internal standard mapping, defaulting to 'apply_source'",
					zap.String("source_dialect", entry.SourceDialect),
					zap.String("target_dialect", entry.TargetDialect),
					zap.String("source_type_key", key),
					zap.String("invalid_value", string(stdMap.ModifierHandling)))
				stdMap.ModifierHandling = ModifierHandlingApplySource
			}
			mappings.Mappings[i].Mappings[key] = stdMap
		}
		for j, spMap := range entry.SpecialMappings {
			if spMap.ModifierHandling == "" {
				spMap.ModifierHandling = ModifierHandlingApplySource
			}
			isValidModifierHandling := false
			for _, validMH := range []ModifierHandlingStrategy{ModifierHandlingApplySource, ModifierHandlingUseTargetDefined, ModifierHandlingIgnoreSource} {
				if spMap.ModifierHandling == validMH {
					isValidModifierHandling = true
					break
				}
			}
			if !isValidModifierHandling {
				logger.Warn("Invalid modifier_handling in internal special mapping, defaulting to 'apply_source'",
					zap.String("source_dialect", entry.SourceDialect),
					zap.String("target_dialect", entry.TargetDialect),
					zap.String("source_type_pattern", spMap.SourceTypePattern),
					zap.String("invalid_value", string(spMap.ModifierHandling)))
				spMap.ModifierHandling = ModifierHandlingApplySource
			}
			mappings.Mappings[i].SpecialMappings[j] = spMap
		}
	}

	loadedTypeMappingsCache = mappings
	logger.Info("Successfully initialized internal default type mappings.",
		zap.Int("configurations_loaded", len(loadedTypeMappingsCache.Mappings)))
	return nil
}

func GetTypeMappingForDialects(srcDialect, dstDialect string) *TypeMappingConfigEntry {
	if loadedTypeMappingsCache == nil {
		// Ini kondisi kritis. Seharusnya LoadTypeMappings sudah dipanggil.
		// Di aplikasi nyata, ini bisa jadi panic atau log fatal.
		// Untuk saat ini, kembalikan nil agar pemanggil bisa menangani.
		// fmt.Println("FATAL: GetTypeMappingForDialects called before LoadTypeMappings or cache is nil.")
		// os.Exit(1) // Atau panic()
		return nil
	}
	for _, cfgEntry := range loadedTypeMappingsCache.Mappings {
		if strings.EqualFold(cfgEntry.SourceDialect, srcDialect) && strings.EqualFold(cfgEntry.TargetDialect, dstDialect) {
			return &cfgEntry
		}
	}
	return nil
}

func validateConfig(cfg *Config) error {
	allowedDirections := map[string]bool{"mysql-to-mysql": true, "mysql-to-postgres": true, "postgres-to-mysql": true, "postgres-to-postgres": true, "sqlite-to-mysql": true, "sqlite-to-postgres": true}
	dir := strings.ToLower(cfg.SyncDirection)
	if !allowedDirections[dir] {
		return fmt.Errorf("invalid sync direction: %s. Valid: %v", cfg.SyncDirection, getMapKeys(allowedDirections))
	}
	strategy := strings.ToLower(string(cfg.SchemaSyncStrategy))
	if strategy != string(SchemaSyncDropCreate) && strategy != string(SchemaSyncAlter) && strategy != string(SchemaSyncNone) {
		return fmt.Errorf("invalid schema sync strategy: %s. Valid: %s, %s, %s", cfg.SchemaSyncStrategy, SchemaSyncDropCreate, SchemaSyncAlter, SchemaSyncNone)
	}
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

	validSSL := map[string]bool{"disable": true, "allow": true, "prefer": true, "require": true, "verify-ca": true, "verify-full": true}
	if isSSLModeRelevant(cfg.SrcDB.Dialect) && !validSSL[strings.ToLower(cfg.SrcDB.SSLMode)] {
		return fmt.Errorf("invalid SSL mode for source DB: %s", cfg.SrcDB.SSLMode)
	}
	if isSSLModeRelevant(cfg.DstDB.Dialect) && !validSSL[strings.ToLower(cfg.DstDB.SSLMode)] {
		return fmt.Errorf("invalid SSL mode for destination DB: %s", cfg.DstDB.SSLMode)
	}
	parts := strings.Split(dir, "-to-")
	if len(parts) != 2 {
		return fmt.Errorf("invalid SYNC_DIRECTION format: %s. Expected format 'source-to-destination'", dir)
	}
	srcDialect := strings.ToLower(cfg.SrcDB.Dialect)
	dstDialect := strings.ToLower(cfg.DstDB.Dialect)
	if parts[0] != srcDialect {
		return fmt.Errorf("sync direction source (%s) mismatch source dialect (%s)", parts[0], srcDialect)
	}
	if parts[1] != dstDialect {
		return fmt.Errorf("sync direction destination (%s) mismatch destination dialect (%s)", parts[1], dstDialect)
	}

	return nil
}

func getMapKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func isSSLModeRelevant(dialect string) bool {
	switch strings.ToLower(dialect) {
	case "postgres", "mysql":
		return true
	default:
		return false
	}
}
