package sync

import (
	"context"

	"github.com/arwahdevops/dbsync/internal/config"
	"gorm.io/gorm" // Tambahkan impor GORM
)

// SchemaExecutionResult holds the DDLs generated for execution.
type SchemaExecutionResult struct {
	TableDDL       string   // DDL for CREATE/ALTER TABLE (can be multiple ALTERs joined by ';')
	IndexDDLs      []string // DDLs for CREATE INDEX / DROP INDEX
	ConstraintDDLs []string // DDLs for ADD CONSTRAINT / DROP CONSTRAINT (UNIQUE, FK)
	PrimaryKeys    []string // Detected primary key columns (unquoted)
}

// SchemaSyncerInterface defines the operations for synchronizing schema.
type SchemaSyncerInterface interface {
	// SyncTableSchema analyzes source/destination and generates DDLs based on the strategy.
	// It does NOT execute the DDLs itself.
	SyncTableSchema(ctx context.Context, table string, strategy config.SchemaSyncStrategy) (*SchemaExecutionResult, error)

	// ExecuteDDLs applies the generated DDLs to the destination database.
	// Implementation should handle execution order (e.g., drops first, FKs last).
	ExecuteDDLs(ctx context.Context, table string, ddls *SchemaExecutionResult) error

	// GetPrimaryKeys explicitly fetches primary keys (can be part of SyncTableSchema result).
	GetPrimaryKeys(ctx context.Context, table string) ([]string, error)

	// GetFKDependencies fetches outgoing foreign key dependencies for a list of tables.
	// Returns a map where key is a table name and value is a slice of table names it has FKs to.
	GetFKDependencies(ctx context.Context, db *gorm.DB, dialect string, tableNames []string) (map[string][]string, error)
}

// OrchestratorInterface defines the main synchronization runner.
type OrchestratorInterface interface {
	Run(ctx context.Context) map[string]SyncResult // Returns results per table
}
