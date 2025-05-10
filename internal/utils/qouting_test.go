package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuoteIdentifier(t *testing.T) {
	testCases := []struct {
		name      string
		inputName string
		dialect   string
		expected  string
	}{
		// MySQL Test Cases
		{"MySQL Basic", "my_table", "mysql", "`my_table`"},
		{"MySQL With Space", "my table", "mysql", "`my table`"},
		{"MySQL With Backtick", "my`table", "mysql", "`my``table`"},
		{"MySQL Reserved Word", "select", "mysql", "`select`"},
		{"MySQL Empty", "", "mysql", "``"},
		{"MySQL Already Quoted (Incorrectly)", "`already_quoted`", "mysql", "```already_quoted```"}, // Double quoting
		{"MySQL With Special Chars", "table-name.col", "mysql", "`table-name.col`"},

		// PostgreSQL Test Cases
		{"PostgreSQL Basic", "MyTable", "postgres", `"MyTable"`}, // Case sensitive
		{"PostgreSQL With Space", "My Table", "postgres", `"My Table"`},
		{"PostgreSQL With Quote", `My"Table`, "postgres", `"My""Table"`},
		{"PostgreSQL Reserved Word", "user", "postgres", `"user"`},
		{"PostgreSQL Empty", "", "postgres", `""`},
		{"PostgreSQL Already Quoted (Incorrectly)", `"already_quoted"`, "postgres", `"""already_quoted"""`},
		{"PostgreSQL With Special Chars", "table-name.col", "postgres", `"table-name.col"`},

		// SQLite Test Cases
		{"SQLite Basic", "some_column", "sqlite", `"some_column"`},
		{"SQLite With Space", "some column", "sqlite", `"some column"`},
		{"SQLite With Quote", `another"column`, "sqlite", `"another""column"`},
		{"SQLite Reserved Word", "table", "sqlite", `"table"`},
		{"SQLite Empty", "", "sqlite", `""`},
		{"SQLite Already Quoted (Incorrectly)", `"already_quoted"`, "sqlite", `"""already_quoted"""`},

		// Unknown Dialect Test Cases (Fallback to double quotes)
		{"Unknown Dialect Basic", "fallback_id", "unknown", `"fallback_id"`},
		{"Unknown Dialect With Space", "fallback id", "unknown", `"fallback id"`},
		{"Unknown Dialect With Quote", `fallback"id`, "unknown", `"fallback""id"`},
		{"Unknown Dialect Empty", "", "unknown", `""`},

		// Case insensitivity for dialect string
		{"MySQL Uppercase Dialect", "my_table", "MYSQL", "`my_table`"},
		{"PostgreSQL MixedCase Dialect", "MyTable", "PostgreSQL", `"MyTable"`},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := QuoteIdentifier(tc.inputName, tc.dialect)
			assert.Equal(t, tc.expected, actual, "Test Case: %s, Input: '%s', Dialect: %s", tc.name, tc.inputName, tc.dialect)
		})
	}
}

func TestUnquoteIdentifier(t *testing.T) {
	testCases := []struct {
		name        string
		inputQuoted string
		dialect     string
		expected    string
	}{
		// MySQL Test Cases
		{"MySQL Basic Unquote", "`my_table`", "mysql", "my_table"},
		{"MySQL With Space Unquote", "`my table`", "mysql", "my table"},
		{"MySQL With Escaped Backtick Unquote", "`my``table`", "mysql", "my`table"},
		{"MySQL Reserved Word Unquote", "`select`", "mysql", "select"},
		{"MySQL Empty Quoted Unquote", "``", "mysql", ""},
		{"MySQL Not Quoted", "my_table_no_quotes", "mysql", "my_table_no_quotes"},
		{"MySQL Incorrectly Quoted (Single Quotes)", "'my_table_single'", "mysql", "'my_table_single'"},
		{"MySQL Trim Spaces", "  `trimmed_id`  ", "mysql", "trimmed_id"},

		// PostgreSQL Test Cases
		{"PostgreSQL Basic Unquote", `"MyTable"`, "postgres", "MyTable"},
		{"PostgreSQL With Space Unquote", `"My Table"`, "postgres", "My Table"},
		{"PostgreSQL With Escaped Quote Unquote", `"My""Table"`, "postgres", `My"Table`},
		{"PostgreSQL Reserved Word Unquote", `"user"`, "postgres", "user"},
		{"PostgreSQL Empty Quoted Unquote", `""`, "postgres", ""},
		{"PostgreSQL Not Quoted", "MyTableNoQuotes", "postgres", "MyTableNoQuotes"},
		{"PostgreSQL Incorrectly Quoted (Backticks)", "`pg_table`", "postgres", "`pg_table`"},
		{"PostgreSQL Trim Spaces", `  "trimmed_id"  `, "postgres", "trimmed_id"},


		// SQLite Test Cases
		{"SQLite Basic Unquote", `"some_column"`, "sqlite", "some_column"},
		{"SQLite With Space Unquote", `"some column"`, "sqlite", "some column"},
		{"SQLite With Escaped Quote Unquote", `"another""column"`, "sqlite", `another"column`},
		{"SQLite Reserved Word Unquote", `"table"`, "sqlite", "table"},
		{"SQLite Empty Quoted Unquote", `""`, "sqlite", ""},
		{"SQLite Not Quoted", "sqlite_no_quotes", "sqlite", "sqlite_no_quotes"},
		{"SQLite Incorrectly Quoted (Backticks)", "`sqlite_table`", "sqlite", "`sqlite_table`"},


		// Unknown Dialect Test Cases (Fallback to double quotes for unquoting)
		{"Unknown Basic Unquote", `"fallback_id"`, "unknown", "fallback_id"},
		{"Unknown With Space Unquote", `"fallback id"`, "unknown", "fallback id"},
		{"Unknown With Escaped Quote Unquote", `"fallback""id"`, "unknown", `fallback"id`},
		{"Unknown Empty Quoted Unquote", `""`, "unknown", ""},
		{"Unknown Not Quoted", "unknown_no_quotes", "unknown", "unknown_no_quotes"},
		{"Unknown Incorrectly Quoted (Backticks)", "`unknown_fallback`", "unknown", "`unknown_fallback`"},

		// Edge cases
		{"Too Short To Be Quoted 1", `"`, "postgres", `"`},
		{"Too Short To Be Quoted 2", "`", "mysql", "`"},
		{"Too Short To Be Quoted 3", "a", "postgres", "a"},

		// Case insensitivity for dialect string
		{"MySQL Uppercase Dialect Unquote", "`my_table`", "MYSQL", "my_table"},
		{"PostgreSQL MixedCase Dialect Unquote", `"MyTable"`, "PostgreSQL", "MyTable"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := UnquoteIdentifier(tc.inputQuoted, tc.dialect)
			assert.Equal(t, tc.expected, actual, "Test Case: %s, Input: '%s', Dialect: %s", tc.name, tc.inputQuoted, tc.dialect)
		})
	}
}
