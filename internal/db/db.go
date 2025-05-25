package db

import (
	"context"
	// "database/sql" // Not needed directly
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap" // Needed for logging fields
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/arwahdevops/dbsync/internal/logger"
)

type Connector struct {
	DB      *gorm.DB
	Dialect string
}

func New(dialect, dsn string, gl logger.GormLoggerInterface) (*Connector, error) { // Use interface
	var dialector gorm.Dialector

	lcDialect := strings.ToLower(dialect)
	switch lcDialect {
	case "mysql":
		dialector = mysql.Open(dsn)
	case "postgres":
		dialector = postgres.Open(dsn)
	case "sqlite":
		dialector = sqlite.Open(dsn)
	default:
		return nil, fmt.Errorf("unsupported dialect: %s", dialect)
	}

	db, err := gorm.Open(dialector, &gorm.Config{
		Logger: gl,
		// Add other GORM config if needed (e.g., NamingStrategy, PrepareStmt)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect database (%s): %w", lcDialect, err)
	}

	return &Connector{
		DB:      db,
		Dialect: lcDialect, // Store lowercase dialect consistently
	}, nil
}

// Optimize configures the underlying connection pool.
func (c *Connector) Optimize(poolSize int, maxLifetime time.Duration) error {
	sqlDB, err := c.DB.DB()
	if err != nil {
		return fmt.Errorf("failed to get sql.DB for optimization: %w", err)
	}

	// Set sensible defaults if config values are not positive
	if poolSize <= 0 {
		poolSize = 10 // Default pool size
	}
	if maxLifetime <= 0 {
		maxLifetime = time.Hour // Default max lifetime
	}

	switch c.Dialect {
	case "mysql", "postgres":
		// Apply configured pool settings for MySQL and Postgres
		sqlDB.SetMaxIdleConns(poolSize / 2) // General recommendation
		sqlDB.SetMaxOpenConns(poolSize)
		sqlDB.SetConnMaxLifetime(maxLifetime)
	case "sqlite":
		// SQLite typically works best with a single connection
		sqlDB.SetMaxOpenConns(1)
		sqlDB.SetConnMaxLifetime(0) // No expiration needed for file-based DB typically
	}
	return nil
}

func (c *Connector) Ping(ctx context.Context) error {
	sqlDB, err := c.DB.DB()
	if err != nil {
		return fmt.Errorf("failed to get sql.DB for ping: %w", err)
	}
	// Use a context with timeout specifically for the ping operation
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second) // Example: 5 second timeout for ping
	defer cancel()
	return sqlDB.PingContext(pingCtx)
}

func (c *Connector) Close() error {
	sqlDB, err := c.DB.DB()
	if err != nil {
		// Log the error but don't prevent potential closure if DB handle is valid
		logger.Log.Warn("Failed to get sql.DB for closing, attempting close anyway", zap.Error(err))
		// If gorm.DB itself has a Close method, use it as fallback? (Check GORM docs)
		// Currently, gorm doesn't expose a direct Close, relies on sql.DB.
		return fmt.Errorf("failed to get sql.DB handle to close: %w", err)
	}
	logger.Log.Info("Closing database connection pool", zap.String("dialect", c.Dialect))
	return sqlDB.Close()
}
