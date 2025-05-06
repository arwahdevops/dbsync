package secrets

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	vault "github.com/hashicorp/vault/api"
	"go.uber.org/zap"

	"github.com/arwahdevops/dbsync/internal/config"
)

// VaultManager implements the SecretManager interface for HashiCorp Vault.
type VaultManager struct {
	client *vault.Client
	cfg    *config.Config
	logger *zap.Logger
}

// NewVaultManager creates and configures a Vault client.
func NewVaultManager(cfg *config.Config, baseLogger *zap.Logger) (*VaultManager, error) {
	log := baseLogger.Named("vault-manager")
	if !cfg.VaultEnabled {
		log.Info("Vault secret manager is disabled via configuration.")
		// Return a disabled manager, not an error
		return &VaultManager{cfg: cfg, logger: log}, nil
	}

	log.Info("Initializing Vault secret manager", zap.String("address", cfg.VaultAddr))

	vConfig := vault.DefaultConfig()
	vConfig.Address = cfg.VaultAddr
	vConfig.Timeout = 10 * time.Second // Set a reasonable timeout

	// Configure TLS
	tlsConfig := &vault.TLSConfig{
		CACert:     cfg.VaultCACert,
		Insecure:   cfg.VaultSkipVerify, // Use with caution!
	}
	err := vConfig.ConfigureTLS(tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to configure Vault TLS: %w", err)
	}

	client, err := vault.NewClient(vConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Vault client: %w", err)
	}

	// --- Authentication ---
	// Currently only supports token auth. Extend this for other methods.
	if cfg.VaultToken != "" {
		log.Info("Using Vault token authentication")
		client.SetToken(cfg.VaultToken)
		// Optionally verify token validity here?
		// _, err := client.Auth().Token().LookupSelf()
		// if err != nil {
		//     log.Error("Vault token seems invalid", zap.Error(err))
		//     // Decide if this should be fatal? Maybe only if Vault is mandatory.
		// }
	} else {
		// TODO: Implement other auth methods (AppRole, K8s, etc.) based on config flags
		log.Warn("Vault is enabled, but no VAULT_TOKEN provided and other auth methods are not implemented yet.")
		// Depending on requirements, you might return an error here if no auth method is configured.
	}

	return &VaultManager{
		client: client,
		cfg:    cfg,
		logger: log,
	}, nil
}

// IsEnabled checks if Vault integration is enabled in the config.
func (m *VaultManager) IsEnabled() bool {
	// Check cfg exists and VaultEnabled flag is true
	return m.cfg != nil && m.cfg.VaultEnabled && m.client != nil // Ensure client was created
}

// GetCredentials retrieves secrets from Vault KV v2 engine.
func (m *VaultManager) GetCredentials(ctx context.Context, path string) (*Credentials, error) {
	if !m.IsEnabled() {
		return nil, fmt.Errorf("Vault manager is not enabled or not initialized")
	}
	if path == "" {
		return nil, fmt.Errorf("Vault secret path cannot be empty")
	}

	log := m.logger.With(zap.String("vault_path", path))
	log.Info("Attempting to read secret from Vault KV v2")

	// Assume KV v2, adjust mount path if necessary
	// The path provided in config should be like 'secret/data/myapp/db'
	secret, err := m.client.KVv2("secret").Get(ctx, path) // Mount path "secret" is common default
	if err != nil {
		// Check if it's a 404 Not Found vs other errors
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

	// Vault KV v2 stores data under a 'data' subkey
	secretData, ok := secret.Data["data"].(map[string]interface{})
	if !ok {
		log.Error("Could not assert Vault secret 'data' subkey to map[string]interface{}")
		return nil, fmt.Errorf("unexpected format for secret data at '%s'", path)
	}


	// Determine username and password keys based on config (SRC vs DST)
	usernameKey := m.cfg.SrcUsernameKey
	passwordKey := m.cfg.SrcPasswordKey
	// Basic check to see if the path looks like a destination path
	// This is fragile, better to pass a label ("source"/"destination") to GetCredentials if possible
	if strings.Contains(path, "dst") || strings.Contains(path, "dest") || path == m.cfg.DstSecretPath {
		usernameKey = m.cfg.DstUsernameKey
		passwordKey = m.cfg.DstPasswordKey
	}


	// Extract username and password
	usernameVal, uOk := secretData[usernameKey]
	passwordVal, pOk := secretData[passwordKey]

	if !pOk || passwordVal == nil {
		log.Error("Password key not found or is null in Vault secret data", zap.String("password_key", passwordKey))
		return nil, fmt.Errorf("password key '%s' not found or is null in secret '%s'", passwordKey, path)
	}
	password, pStrOk := passwordVal.(string)
	if !pStrOk || password == "" {
		log.Error("Password value in Vault secret is not a non-empty string", zap.String("password_key", passwordKey))
		return nil, fmt.Errorf("password value for key '%s' in secret '%s' is not a non-empty string", passwordKey, path)
	}

	username := "" // Default to empty if not found
	if uOk && usernameVal != nil {
		username, _ = usernameVal.(string) // Ignore error if not string, username becomes ""
	}

	log.Info("Successfully retrieved credentials from Vault")
	return &Credentials{
		Username: username, // Might be empty if key not found or value not string
		Password: password,
	}, nil
}
