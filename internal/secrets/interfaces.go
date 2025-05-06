package secrets

import "context"

// Credentials holds the retrieved username and password.
type Credentials struct {
	Username string
	Password string
	// Add other fields if needed by specific managers or DSN builders
}

// SecretManager defines the interface for interacting with different secret backends.
type SecretManager interface {
	// GetCredentials retrieves database credentials from the secret manager.
	// pathOrID specifies the location of the secret (e.g., Vault path, AWS ARN, GCP ID).
	GetCredentials(ctx context.Context, pathOrID string) (*Credentials, error)

	// IsEnabled checks if this specific secret manager is configured and enabled.
	IsEnabled() bool
}
