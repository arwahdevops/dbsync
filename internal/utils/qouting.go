package utils

import (
	"fmt"
	"strings"
)

// QuoteIdentifier quotes an identifier based on the specified SQL dialect.
// Handles basic escaping for the quote character itself within the name.
func QuoteIdentifier(name, dialect string) string {
	dialect = strings.ToLower(dialect)
	switch dialect {
	case "mysql":
		// Escape backticks within the name
		return fmt.Sprintf("`%s`", strings.ReplaceAll(name, "`", "``"))
	case "postgres":
		// Escape double quotes within the name
		return fmt.Sprintf("\"%s\"", strings.ReplaceAll(name, "\"", "\"\""))
	case "sqlite":
		// SQLite accepts double quotes, backticks, or square brackets. Double quotes are safest.
		return fmt.Sprintf("\"%s\"", strings.ReplaceAll(name, "\"", "\"\""))
	default:
		// Fallback for unknown dialects: Try double quotes (ANSI SQL standard)
		// This might fail on some databases or for reserved words.
		// Log a warning if this happens?
		return fmt.Sprintf("\"%s\"", strings.ReplaceAll(name, "\"", "\"\""))
	}
}