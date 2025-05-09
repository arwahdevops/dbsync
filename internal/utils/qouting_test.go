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
		{"MySQL Basic", "my_table", "mysql", "`my_table`"},
		{"MySQL With Backtick", "my`table", "mysql", "`my``table`"},
		{"MySQL Empty", "", "mysql", "``"},
		{"PostgreSQL Basic", "MyTable", "postgres", `"MyTable"`},
		{"PostgreSQL With Quote", `My"Table`, "postgres", `"My""Table"`},
		{"PostgreSQL Empty", "", "postgres", `""`},
		{"SQLite Basic", "some_column", "sqlite", `"some_column"`},
		{"SQLite With Quote", `another"column`, "sqlite", `"another""column"`},
		{"SQLite Empty", "", "sqlite", `""`},
		{"Unknown Dialect Fallback", "fallback_id", "unknown", `"fallback_id"`},
		{"Unknown Dialect Empty", "", "unknown", `""`},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := QuoteIdentifier(tc.inputName, tc.dialect)
			assert.Equal(t, tc.expected, actual, "Test Case: %s", tc.name)
		})
	}
}
