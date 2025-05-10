package sync

import (
	"testing"

	"github.com/stretchr/testify/assert"
	// "go.uber.org/zap" // Jika Anda ingin menggunakan logger di test, uncomment dan inisialisasi.
	// "go.uber.org/zap/zaptest"
)

func TestNormalizeDefaultValue(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		dialect  string
		expected string
	}{
		// General
		{"Empty String", "", "mysql", ""},
		{"Lowercase", "hello", "mysql", "hello"},
		{"Uppercase", "HELLO", "mysql", "hello"},
		{"Single Quoted", "'world'", "mysql", "world"},
		{"Double Quoted", `"world"`, "mysql", "world"},
		{"Backtick Quoted", "`world`", "mysql", "world"},
		{"NULL Literal lower", "null", "mysql", "null"},
		{"NULL Literal upper", "NULL", "postgres", "null"},
		{"Number String", "123", "mysql", "123"},
		{"Boolean True Forms 1", "true", "mysql", "1"},
		{"Boolean True Forms 2", "T", "postgres", "1"},
		{"Boolean True Forms 3", "YES", "mysql", "1"},
		{"Boolean True Forms 4", "on", "sqlite", "1"},
		{"Boolean True Forms 5", "1", "mysql", "1"},
		{"Boolean False Forms 1", "false", "postgres", "0"},
		{"Boolean False Forms 2", "F", "mysql", "0"},
		{"Boolean False Forms 3", "no", "sqlite", "0"},
		{"Boolean False Forms 4", "OFF", "mysql", "0"},
		{"Boolean False Forms 5", "0", "postgres", "0"},

		// Time functions
		{"NOW()", "now()", "mysql", "current_timestamp"},
		{"CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP", "postgres", "current_timestamp"},
		{"CURRENT_TIMESTAMP()", "current_timestamp()", "mysql", "current_timestamp"},
		{"GETDATE()", "getdate()", "sqlserver", "current_timestamp"}, // sqlserver tidak didukung, tapi tes normalisasi umum
		{"SYSDATETIME()", "sysdatetime()", "sqlserver", "current_timestamp"},
		{"CURRENT_DATE", "CURRENT_DATE", "postgres", "current_date"},
		{"CURRENT_TIME", "current_time()", "mysql", "current_time"},

		// UUID functions
		{"UUID()", "uuid()", "mysql", "uuid_function"},
		{"GEN_RANDOM_UUID()", "gen_random_uuid()", "postgres", "uuid_function"},
		{"NEWID()", "newid()", "sqlserver", "uuid_function"},

		// MySQL Specific
		{"MySQL Boolean b'1'", "b'1'", "mysql", "1"},
		{"MySQL Boolean b'0'", "b'0'", "mysql", "0"},
		{"MySQL CURRENT_TIMESTAMP ON UPDATE", "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP", "mysql", "current_timestamp"},
		{"MySQL Value ON UPDATE", "'default_val' ON UPDATE CURRENT_TIMESTAMP", "mysql", "default_val"},

		// PostgreSQL Specific
		{"Postgres Cast Text", "'hello'::character varying", "postgres", "hello"},
		{"Postgres Cast Text with space", "'hello world'::text", "postgres", "hello world"},
		{"Postgres Cast Numeric", "123::integer", "postgres", "123"},
		{"Postgres Cast Function", "now()::timestamp without time zone", "postgres", "current_timestamp"},
		{"Postgres Nextval", "nextval('my_seq'::regclass)", "postgres", "nextval"},
		{"Postgres Boolean true cast", "true::boolean", "postgres", "1"},
		{"Postgres Quoted text in cast", "('hello world')::text", "postgres", "hello world"}, // Diperbaiki ekspektasinya
		{"Postgres Nested Cast", "('123'::text)::integer", "postgres", "123"},              // Diperbaiki ekspektasinya
		{"Postgres Array Literal Cast", "ARRAY['foo','bar']::text[]", "postgres", "array['foo','bar']"}, // Tanda kurung array harus tetap
		{"Postgres Row Constructor Cast", "ROW(1, 'foo')::myrowtype", "postgres", "row(1, 'foo')"}, // Tanda kurung row harus tetap
		{"Postgres Default with space and quote", "  ' default value '  ", "postgres", " default value "},


		// SQLite Specific
		{"SQLite NULL case-insensitive", "NuLl", "sqlite", "null"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := normalizeDefaultValue(tc.input, tc.dialect)
			assert.Equal(t, tc.expected, actual, "Test Case: %s", tc.name)
		})
	}
}

func TestNormalizeFKAction(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{"Empty", "", "NO ACTION"},
		{"No Action Explicit", "NO ACTION", "NO ACTION"},
		{"No Action Lower", "no action", "NO ACTION"},
		{"Cascade", "CASCADE", "CASCADE"},
		{"Cascade Lower", "cascade", "CASCADE"},
		{"Restrict", "RESTRICT", "RESTRICT"},
		{"Restrict Lower", "restrict", "RESTRICT"},
		{"Set Null", "SET NULL", "SET NULL"},
		{"Set Null Lower", "set null", "SET NULL"},
		{"Set Default", "SET DEFAULT", "SET DEFAULT"},
		{"Set Default Lower", "set default", "SET DEFAULT"},
		{"With Spaces", "  CASCADE  ", "CASCADE"},
		{"Unknown Action", "DO NOTHING", "DO NOTHING"}, // Harus tetap uppercase
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := normalizeFKAction(tc.input)
			assert.Equal(t, tc.expected, actual, "Test Case: %s", tc.name)
		})
	}
}

func TestNormalizeCheckDefinition(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{"Simple Check", "col > 0", "col > 0"},
		{"With Parens", "(col > 0)", "col > 0"}, // Diperbaiki ekspektasinya, kurung terluar sederhana dihilangkan
		{"Extra Spaces", "  col   >  0  ", "col > 0"},
		{"Mixed Case", "CoL > 0", "col > 0"},
		{"With Line Comment", "col > 0 -- must be positive", "col > 0"},
		{"With Block Comment", "col > 0 /* must be positive */", "col > 0"},
		{"Block Comment Mid", "col /* comment */ > 0", "col > 0"},
		// Ekspektasi diubah: satu pasang kurung terluar yang valid akan dihilangkan.
		{"Complex With Parens", "( (col1 > 0 AND col2 < 10) OR col3 = 'test' )", "(col1 > 0 and col2 < 10) or col3 = 'test'"},
		{"Empty String", "", ""},
		{"Only Comment", "-- only comment", ""},
		{"Check with string literal", "name = 'Test User'", "name = 'test user'"}, // Literal juga jadi lowercase, ini batasan dari strings.ToLower()
		{"Check with function", "LENGTH(name) > 3", "length(name) > 3"},
		{"Only Outer Parens", "(col1 > 0)", "col1 > 0"},
		{"Parens Not Outer Simple", "func((col1 > 0))", "func((col1 > 0))"}, // Tidak boleh strip jika ada fungsi
		{"Parens Not Outer Complex", "(a > 0) AND (b < 0)", "(a > 0) and (b < 0)"}, // Tidak boleh strip
		{"Nested Parens Simplifiable", "(((a > 0)))", "a > 0"}, // Akan distrip iteratif
		{"No Strip Needed", "col1 > 0 and col2 < 10", "col1 > 0 and col2 < 10"},
		{"Check with double parens no space", "((col1 > 5))", "col1 > 5"},
		{"Check with function and parens", "(length(name) > 5)", "length(name) > 5"},
		{"Check already normalized", "col > 0 and price < 100", "col > 0 and price < 100"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := normalizeCheckDefinition(tc.input)
			assert.Equal(t, tc.expected, actual, "Test Case: %s, Input: '%s'", tc.name, tc.input)
		})
	}
}

func TestIsDefaultNullOrFunction(t *testing.T) {
	testCases := []struct {
		name          string
		normalizedDef string
		expected      bool
	}{
		{"Empty", "", true},
		{"Null Literal", "null", true},
		{"Current Timestamp", "current_timestamp", true},
		{"Current Date", "current_date", true},
		{"Current Time", "current_time", true},
		{"Nextval", "nextval", true},
		{"UUID Function", "uuid_function", true},
		{"Literal String", "hello", false},
		{"Literal Number 0", "0", false},
		{"Literal Number 1", "1", false},
		{"Literal Number 123", "123", false},
		{"Literal True (normalized)", "1", false},
		{"Literal False (normalized)", "0", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := isDefaultNullOrFunction(tc.normalizedDef)
			assert.Equal(t, tc.expected, actual, "Test Case: %s, Input: %s", tc.name, tc.normalizedDef)
		})
	}
}
