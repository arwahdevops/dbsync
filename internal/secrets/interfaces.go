package secrets

import "context"

// Credentials holds the retrieved username and password.
type Credentials struct {
	Username string
	Password string
}

// SecretManager defines the interface for interacting with different secret backends.
type SecretManager interface {
	// GetCredentials retrieves database credentials from the secret manager.
	// pathOrID specifies the location of the secret.
	// usernameKey and passwordKey specify the keys within the secret data.
	GetCredentials(ctx context.Context, pathOrID string, usernameKey string, passwordKey string) (*Credentials, error)

	// IsEnabled checks if this specific secret manager is configured and enabled.
	IsEnabled() bool
}
