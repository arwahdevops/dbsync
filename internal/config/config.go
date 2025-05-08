package config

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/caarlos0/env/v8"
	"go.uber.org/zap" // Diperlukan untuk logging di LoadTypeMappings
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
	DebugMode         bool `env:"DEBUG_MODE" envDefault:"false"`

	// --- Database Configurations ---
	SrcDB DatabaseConfig `envPrefix:"SRC_"`
	DstDB DatabaseConfig `envPrefix:"DST_"`

	// --- Optional: Vault Secret Management ---
	VaultEnabled     bool   `env:"VAULT_ENABLED" envDefault:"false"`
	VaultAddr        string `env:"VAULT_ADDR" envDefault:"https://127.0.0.1:8200"`
	VaultToken       string `env:"VAULT_TOKEN"`
	VaultCACert      string `env:"VAULT_CACERT"`
	VaultSkipVerify  bool   `env:"VAULT_SKIP_VERIFY" envDefault:"false"`
	SrcSecretPath    string `env:"SRC_SECRET_PATH"`
	DstSecretPath    string `env:"DST_SECRET_PATH"`
	SrcUsernameKey   string `env:"SRC_USERNAME_KEY" envDefault:"username"`
	SrcPasswordKey   string `env:"SRC_PASSWORD_KEY" envDefault:"password"`
	DstUsernameKey   string `env:"DST_USERNAME_KEY" envDefault:"username"`
	DstPasswordKey   string `env:"DST_PASSWORD_KEY" envDefault:"password"`

	// --- Type Mapping Configuration ---
	TypeMappingFilePath string `env:"TYPE_MAPPING_FILE_PATH" envDefault:"./typemap.json"`
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

// --- Type Mapping Structures (pindahkan ke sini atau file terpisah jika besar) ---
type TypeMappingConfigEntry struct { // Ubah nama agar tidak konflik dengan struct Config
	SourceDialect   string            `json:"source_dialect"`
	TargetDialect   string            `json:"target_dialect"`
	Mappings        map[string]string `json:"mappings"`
	SpecialMappings []SpecialMapping  `json:"special_mappings"`
}

type SpecialMapping struct {
	SourceTypePattern string `json:"source_type_pattern"` // Regex
	TargetType        string `json:"target_type"`
	// Bisa ditambahkan field lain seperti 'priority' jika ada overlap pola
}

type AllTypeMappings struct {
	Mappings []TypeMappingConfigEntry `json:"type_mappings"`
}

var loadedTypeMappingsCache *AllTypeMappings // Cache mapping yang sudah dimuat

// Load loads the main application configuration.
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
			fmt.Println("WARN: Vault is enabled but VAULT_TOKEN is not set. Ensure another auth method is configured or will be implemented.")
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

// LoadTypeMappings memuat mapping tipe data dari file JSON.
// `logger` harus sudah diinisialisasi sebelum memanggil fungsi ini.
func LoadTypeMappings(filePath string, logger *zap.Logger) error {
	if loadedTypeMappingsCache != nil && filePath == "" { // Jika sudah dimuat dan path kosong, jangan proses lagi
		logger.Debug("External type mappings already processed or path is empty, skipping reload.")
		return nil
	}
	if filePath == "" {
		logger.Info("Type mapping file path is not configured. No external type mappings will be loaded. The application will rely on internal fallbacks or direct type usage.")
		loadedTypeMappingsCache = &AllTypeMappings{Mappings: []TypeMappingConfigEntry{}} // Inisialisasi kosong
		return nil
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		logger.Error("Failed to read type mapping file. Proceeding without external type mappings.", zap.String("path", filePath), zap.Error(err))
		loadedTypeMappingsCache = &AllTypeMappings{Mappings: []TypeMappingConfigEntry{}} // Inisialisasi kosong sebagai fallback
		// Tidak mengembalikan error di sini agar aplikasi tetap bisa berjalan dengan fallback
		return nil // Atau return fmt.Errorf(...) jika ingin fatal
	}

	var mappings AllTypeMappings
	if err := json.Unmarshal(data, &mappings); err != nil {
		logger.Error("Failed to unmarshal type mapping file. Proceeding without external type mappings.", zap.String("path", filePath), zap.Error(err))
		loadedTypeMappingsCache = &AllTypeMappings{Mappings: []TypeMappingConfigEntry{}} // Inisialisasi kosong sebagai fallback
		return nil // Atau return fmt.Errorf(...) jika ingin fatal
	}

	loadedTypeMappingsCache = &mappings
	logger.Info("Successfully loaded type mappings from file.",
		zap.String("path", filePath),
		zap.Int("configurations_loaded", len(loadedTypeMappingsCache.Mappings)))
	return nil
}

// GetTypeMappingForDialects mengambil konfigurasi mapping spesifik untuk pasangan dialek.
func GetTypeMappingForDialects(srcDialect, dstDialect string) *TypeMappingConfigEntry {
	if loadedTypeMappingsCache == nil {
		// Ini seharusnya tidak terjadi jika LoadTypeMappings dipanggil, bahkan jika file tidak ada.
		// Namun, sebagai pengaman:
		return nil
	}
	for _, cfgEntry := range loadedTypeMappingsCache.Mappings {
		if strings.EqualFold(cfgEntry.SourceDialect, srcDialect) && strings.EqualFold(cfgEntry.TargetDialect, dstDialect) {
			return &cfgEntry
		}
	}
	return nil // Tidak ada mapping yang ditemukan untuk pasangan dialek ini
}

func validateConfig(cfg *Config) error {
	// Validasi SyncDirection
	allowedDirections := map[string]bool{"mysql-to-mysql": true, "mysql-to-postgres": true, "postgres-to-mysql": true, "postgres-to-postgres": true, "sqlite-to-mysql": true, "sqlite-to-postgres": true}
	dir := strings.ToLower(cfg.SyncDirection)
	if !allowedDirections[dir] {
		return fmt.Errorf("invalid sync direction: %s. Valid: %v", cfg.SyncDirection, getMapKeys(allowedDirections))
	}
	// Validasi SchemaSyncStrategy
	strategy := strings.ToLower(string(cfg.SchemaSyncStrategy))
	if strategy != string(SchemaSyncDropCreate) && strategy != string(SchemaSyncAlter) && strategy != string(SchemaSyncNone) {
		return fmt.Errorf("invalid schema sync strategy: %s. Valid: %s, %s, %s", cfg.SchemaSyncStrategy, SchemaSyncDropCreate, SchemaSyncAlter, SchemaSyncNone)
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
	validSSL := map[string]bool{"disable": true, "allow": true, "prefer": true, "require": true, "verify-ca": true, "verify-full": true}
	if isSSLModeRelevant(cfg.SrcDB.Dialect) && !validSSL[strings.ToLower(cfg.SrcDB.SSLMode)] {
		return fmt.Errorf("invalid SSL mode for source DB: %s", cfg.SrcDB.SSLMode)
	}
	if isSSLModeRelevant(cfg.DstDB.Dialect) && !validSSL[strings.ToLower(cfg.DstDB.SSLMode)] {
		return fmt.Errorf("invalid SSL mode for destination DB: %s", cfg.DstDB.SSLMode)
	}
	// Validasi kesesuaian SyncDirection dengan dialect
	parts := strings.Split(dir, "-to-")
	if len(parts) != 2 { // Pastikan formatnya benar
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
