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
		// GORM juga cenderung menggunakan double quotes untuk SQLite.
		return fmt.Sprintf("\"%s\"", strings.ReplaceAll(name, "\"", "\"\""))
	default:
		// Fallback for unknown dialects: Try double quotes (ANSI SQL standard)
		// This might fail on some databases or for reserved words.
		// Pertimbangkan untuk log warning jika ini terjadi di konteks non-test.
		return fmt.Sprintf("\"%s\"", strings.ReplaceAll(name, "\"", "\"\""))
	}
}

// UnquoteIdentifier removes dialect-specific quotes from an identifier
// and unescapes quote characters within the name.
// Jika string input tidak di-quote dengan cara yang diharapkan untuk dialek tersebut,
// string akan dikembalikan apa adanya.
func UnquoteIdentifier(quotedName, dialect string) string {
	name := strings.TrimSpace(quotedName)
	if len(name) < 2 {
		return name // Not quoted (or empty string) or too short to be quoted
	}

	firstChar := name[0]
	lastChar := name[len(name)-1]
	var quoteChar byte
	var escapeSequence, originalChar string

	switch strings.ToLower(dialect) {
	case "mysql":
		if firstChar == '`' && lastChar == '`' {
			quoteChar = '`'
			escapeSequence = "``"
			originalChar = "`"
		}
	case "postgres", "sqlite": // SQLite juga umum dengan double quotes
		if firstChar == '"' && lastChar == '"' {
			quoteChar = '"'
			escapeSequence = "\"\""
			originalChar = "\""
		}
	default:
		// Untuk dialek yang tidak diketahui, coba unquote jika menggunakan double quotes (ANSI standard)
		if firstChar == '"' && lastChar == '"' {
			quoteChar = '"'
			escapeSequence = "\"\""
			originalChar = "\""
		} else {
			return name // Tidak ada pola quote yang dikenali untuk dialek ini atau fallback
		}
	}

	if quoteChar != 0 { // Jika quoteChar di-set, berarti kita menemukan pola quote yang valid
		// Hapus quote terluar
		unquotedContent := name[1 : len(name)-1]
		// Unescape karakter quote di dalam nama
		return strings.ReplaceAll(unquotedContent, escapeSequence, originalChar)
	}

	return name // Jika tidak ada pola quote yang cocok
}
