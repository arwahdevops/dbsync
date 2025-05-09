package sync

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestNormalizeTypeName(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{"Lowercase", "varchar", "varchar"},
		{"Uppercase", "VARCHAR", "varchar"},
		{"Mixed Case", "VarChar", "varchar"},
		{"With Size", "VARCHAR(255)", "varchar"},
		{"With Precision Scale", "DECIMAL(10,2)", "decimal"},
		{"With Spaces", "  varchar( 50 )  ", "varchar"},
		{"MySQL Unsigned", "INT unsigned", "int"},
		{"MySQL Unsigned Zerofill", "INT(11) unsigned zerofill", "int"},
		{"MySQL Unsigned With Size", "BIGINT(20) UNSIGNED", "bigint"},
		{"PostgreSQL Character Varying", "character varying(100)", "varchar"},
		{"PostgreSQL Double Precision", "double precision", "double"},
		{"PostgreSQL Timestamp With Time Zone", "timestamp with time zone", "timestamptz"},
		{"PostgreSQL Timestamp Without Time Zone", "timestamp without time zone", "timestamp"},
		{"PostgreSQL Integer Alias", "INT4", "int"},
		{"PostgreSQL BigInt Alias", "INT8", "bigint"},
		{"PostgreSQL Serial Alias", "SERIAL4", "serial"},
		{"PostgreSQL BigSerial Alias", "BIGSERIAL", "bigserial"},
		{"Boolean", "BOOLEAN", "bool"},
		{"No Modifier", "TEXT", "text"},
		{"Empty String", "", ""},
		{"Only Spaces", "   ", ""},
		{"Type with space in name", "double precision", "double"}, // sudah ada
		{"Type with (nonsense) modifier", "mytype(foo bar)", "mytype"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := normalizeTypeName(tc.input)
			assert.Equal(t, tc.expected, actual, "Test Case: %s", tc.name)
		})
	}
}

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
		{"Postgres Cast Numeric", "123::integer", "postgres", "123"}, // Angka tidak di-quote setelah cast
		{"Postgres Cast Function", "now()::timestamp without time zone", "postgres", "current_timestamp"},
		{"Postgres Nextval", "nextval('my_seq'::regclass)", "postgres", "nextval"},
		{"Postgres Boolean true cast", "true::boolean", "postgres", "1"},

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

// Anda bisa menambahkan lebih banyak test case untuk isStringType, isNumericType, dll.
// Contoh untuk isStringType:
func TestIsStringType(t *testing.T) {
	stringTypes := []string{"char", "varchar", "nchar", "nvarchar", "text", "tinytext", "mediumtext", "longtext", "clob", "enum", "set", "uuid", "json", "xml"}
	nonStringTypes := []string{"int", "decimal", "bool", "date", "timestamp", "binary"}

	for _, sType := range stringTypes {
		t.Run(sType, func(t *testing.T) {
			assert.True(t, isStringType(sType), "Expected '%s' to be a string type", sType)
		})
	}
	for _, nsType := range nonStringTypes {
		t.Run(nsType, func(t *testing.T) {
			assert.False(t, isStringType(nsType), "Expected '%s' NOT to be a string type", nsType)
		})
	}
}
