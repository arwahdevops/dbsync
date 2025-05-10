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
		{"Type with space in name", "double precision", "double"},
		{"Type with (nonsense) modifier", "mytype(foo bar)", "mytype"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := normalizeTypeName(tc.input)
			assert.Equal(t, tc.expected, actual, "Test Case: %s", tc.name)
		})
	}
}

func TestIsStringType(t *testing.T) {
	stringTypes := []string{
		"char", "varchar", "nchar", "nvarchar",
		"text", "tinytext", "mediumtext", "longtext",
		"clob", "enum", "set", "uuid", "json", "xml",
	}
	nonStringTypes := []string{
		"int", "decimal", "bool", "date", "timestamp", "binary", "blob", "bytea",
	}

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

func TestIsBinaryType(t *testing.T) {
	binaryTypes := []string{"binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob", "bytea"}
	nonBinaryTypes := []string{"char", "text", "int", "json"}

	for _, bType := range binaryTypes {
		t.Run(bType, func(t *testing.T) {
			assert.True(t, isBinaryType(bType), "Expected '%s' to be a binary type", bType)
		})
	}
	for _, nbType := range nonBinaryTypes {
		t.Run(nbType, func(t *testing.T) {
			assert.False(t, isBinaryType(nbType), "Expected '%s' NOT to be a binary type", nbType)
		})
	}
}

func TestIsNumericType(t *testing.T) {
	numericTypes := []string{
		"tinyint", "smallint", "mediumint", "int", "integer", "bigint", // integer types
		"serial", "bigserial", // serial types
		"decimal", "numeric",                                // fixed-point
		"float", "double", "real",                           // floating-point
		"money", // monetary type
	}
	nonNumericTypes := []string{"char", "varchar", "text", "date", "timestamp", "bool", "blob", "json"}

	for _, numType := range numericTypes {
		t.Run(numType, func(t *testing.T) {
			assert.True(t, isNumericType(numType), "Expected '%s' to be a numeric type", numType)
		})
	}
	for _, nonNumType := range nonNumericTypes {
		t.Run(nonNumType, func(t *testing.T) {
			assert.False(t, isNumericType(nonNumType), "Expected '%s' NOT to be a numeric type", nonNumType)
		})
	}
}

func TestIsIntegerType(t *testing.T) {
	integerTypes := []string{"tinyint", "smallint", "mediumint", "int", "integer", "bigint", "serial", "bigserial"}
	nonIntegerTypes := []string{"decimal", "float", "varchar", "text", "date"}

	for _, intType := range integerTypes {
		t.Run(intType, func(t *testing.T) {
			assert.True(t, isIntegerType(intType), "Expected '%s' to be an integer type", intType)
		})
	}
	for _, nonIntType := range nonIntegerTypes {
		t.Run(nonIntType, func(t *testing.T) {
			assert.False(t, isIntegerType(nonIntType), "Expected '%s' NOT to be an integer type", nonIntType)
		})
	}
}

func TestIsPrecisionRelevant(t *testing.T) {
	relevantTypes := []string{"decimal", "numeric", "time", "timestamp", "timestamptz", "datetimeoffset"}
	nonRelevantTypes := []string{"int", "varchar", "text", "date", "bool"}

	for _, relType := range relevantTypes {
		t.Run(relType, func(t *testing.T) {
			assert.True(t, isPrecisionRelevant(relType), "Expected precision to be relevant for '%s'", relType)
		})
	}
	for _, nonRelType := range nonRelevantTypes {
		t.Run(nonRelType, func(t *testing.T) {
			assert.False(t, isPrecisionRelevant(nonRelType), "Expected precision NOT to be relevant for '%s'", nonRelType)
		})
	}
}

func TestIsScaleRelevant(t *testing.T) {
	relevantTypes := []string{"decimal", "numeric"}
	nonRelevantTypes := []string{"int", "varchar", "text", "date", "bool", "time", "timestamp"}

	for _, relType := range relevantTypes {
		t.Run(relType, func(t *testing.T) {
			assert.True(t, isScaleRelevant(relType), "Expected scale to be relevant for '%s'", relType)
		})
	}
	for _, nonRelType := range nonRelevantTypes {
		t.Run(nonRelType, func(t *testing.T) {
			assert.False(t, isScaleRelevant(nonRelType), "Expected scale NOT to be relevant for '%s'", nonRelType)
		})
	}
}
