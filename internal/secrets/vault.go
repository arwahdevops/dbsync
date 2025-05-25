package secrets

import (
	"context"
	"fmt"
	"net/http"
	// "strings" // Tidak dibutuhkan lagi di sini
	"time"

	vault "github.com/hashicorp/vault/api"
	"go.uber.org/zap"

	"github.com/arwahdevops/dbsync/internal/config" // Masih dibutuhkan untuk cfg.VaultEnabled dll
)

// VaultManager implements the SecretManager interface for HashiCorp Vault.
type VaultManager struct {
	client *vault.Client
	cfg    *config.Config // Masih butuh cfg untuk IsEnabled, VaultAddr, dll.
	logger *zap.Logger
}

// NewVaultManager (tetap sama)
func NewVaultManager(cfg *config.Config, baseLogger *zap.Logger) (*VaultManager, error) {
	// ... (implementasi sama)
	log := baseLogger.Named("vault-manager")
	if !cfg.VaultEnabled {
		log.Info("Vault secret manager is disabled via configuration.")
		return &VaultManager{cfg: cfg, logger: log}, nil
	}

	log.Info("Initializing Vault secret manager", zap.String("address", cfg.VaultAddr))

	vConfig := vault.DefaultConfig()
	vConfig.Address = cfg.VaultAddr
	vConfig.Timeout = 10 * time.Second

	tlsConfig := &vault.TLSConfig{
		CACert:   cfg.VaultCACert,
		Insecure: cfg.VaultSkipVerify,
	}
	err := vConfig.ConfigureTLS(tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to configure Vault TLS: %w", err)
	}

	client, err := vault.NewClient(vConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Vault client: %w", err)
	}

	if cfg.VaultToken != "" {
		log.Info("Using Vault token authentication")
		client.SetToken(cfg.VaultToken)
	} else {
		log.Warn("Vault is enabled, but no VAULT_TOKEN provided and other auth methods are not implemented yet.")
	}

	return &VaultManager{
		client: client,
		cfg:    cfg,
		logger: log,
	}, nil
}

// IsEnabled (tetap sama)
func (m *VaultManager) IsEnabled() bool {
	return m.cfg != nil && m.cfg.VaultEnabled && m.client != nil
}

// GetCredentials retrieves secrets from Vault KV v2 engine.
// Menerima usernameKey dan passwordKey secara eksplisit.
func (m *VaultManager) GetCredentials(ctx context.Context, path, usernameKey, passwordKey string) (*Credentials, error) {
	if !m.IsEnabled() {
		return nil, fmt.Errorf("Vault manager is not enabled or not initialized")
	}
	if path == "" {
		return nil, fmt.Errorf("Vault secret path cannot be empty")
	}
	if usernameKey == "" { // Default jika tidak disediakan (seharusnya dari config)
		usernameKey = "username"
	}
	if passwordKey == "" { // Default jika tidak disediakan
		passwordKey = "password"
	}

	log := m.logger.With(zap.String("vault_path", path))
	log.Info("Attempting to read secret from Vault KV v2", zap.String("username_key", usernameKey), zap.String("password_key", passwordKey))

	secret, err := m.client.KVv2("secret").Get(ctx, path)
	if err != nil {
		if vaultErr, ok := err.(*vault.ResponseError); ok && vaultErr.StatusCode == http.StatusNotFound {
			log.Error("Secret not found in Vault", zap.Error(err))
			return nil, fmt.Errorf("secret '%s' not found in Vault: %w", path, err)
		}
		log.Error("Failed to read secret from Vault", zap.Error(err))
		return nil, fmt.Errorf("failed to read secret '%s' from Vault: %w", path, err)
	}

	if secret == nil || secret.Data == nil || secret.Data["data"] == nil {
		log.Error("Vault secret data is empty or malformed (expected KV v2 format with 'data' subkey)")
		return nil, fmt.Errorf("secret data for '%s' is empty or not in expected KV v2 format", path)
	}

	secretData, ok := secret.Data["data"].(map[string]interface{})
	if !ok {
		log.Error("Could not assert Vault secret 'data' subkey to map[string]interface{}")
		return nil, fmt.Errorf("unexpected format for secret data at '%s'", path)
	}

	usernameVal, uOk := secretData[usernameKey]
	passwordVal, pOk := secretData[passwordKey]

	if !pOk || passwordVal == nil {
		log.Error("Password key not found or is null in Vault secret data", zap.String("key_used", passwordKey))
		return nil, fmt.Errorf("password key '%s' not found or is null in secret '%s'", passwordKey, path)
	}
	password, pStrOk := passwordVal.(string)
	if !pStrOk || password == "" {
		log.Error("Password value in Vault secret is not a non-empty string", zap.String("key_used", passwordKey))
		return nil, fmt.Errorf("password value for key '%s' in secret '%s' is not a non-empty string", passwordKey, path)
	}

	username := ""
	if uOk && usernameVal != nil {
		username, _ = usernameVal.(string)
	}

	log.Info("Successfully retrieved credentials from Vault")
	return &Credentials{
		Username: username,
		Password: password,
	}, nil
}
