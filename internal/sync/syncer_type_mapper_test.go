package sync

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// Helper untuk membuat instance SchemaSyncer untuk pengujian applyTypeModifiers
func newTestTypeMapperSyncer(srcDialect, dstDialect string, logger *zap.Logger) *SchemaSyncer {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &SchemaSyncer{
		srcDialect: srcDialect,
		dstDialect: dstDialect,
		logger:     logger,
	}
}


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
		{"With Spaces and Numeric Mod", "  varchar( 50 )  ", "varchar"},
		{"MySQL Unsigned", "INT unsigned", "int"},
		{"MySQL Unsigned Zerofill with Numeric Mod", "INT(11) unsigned zerofill", "int"},
		{"PostgreSQL Character Varying with Numeric Mod", "character varying(100)", "varchar"},
		{"Boolean", "BOOLEAN", "bool"},
		{"No Modifier", "TEXT", "text"},
		{"Type with (nonsense) modifier - NOT stripped", "mytype(foo bar)", "mytype(foo bar)"},
		{"Type with complex modifier - NOT stripped", "GEOMETRY(Point,4326)", "geometry(point,4326)"},
		{"Type with no modifier but parens in name - NOT stripped", "func_returns_type()", "func_returns_type()"},
		{"Empty String", "", ""},
		{"Only Spaces", "   ", ""},
		{"Type with trailing space before numeric paren", "VARCHAR (255)", "varchar"},
		{"Type with no space before numeric paren", "INT(11)", "int"},
		{"Timestamp with time zone PG", "timestamp with time zone", "timestamptz"},
		{"Timestamp (3) with time zone PG", "timestamp(3) with time zone", "timestamptz"}, // Expected setelah normalisasi & alias
		{"bit varying with numeric mod", "bit varying(10)", "varbit"},                   // Expected setelah normalisasi & alias
		{"character with numeric mod", "character(5)", "char"},                         // Expected setelah normalisasi & alias
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := normalizeTypeName(tc.input)
			assert.Equal(t, tc.expected, actual, "Test Case: %s, Input: '%s'", tc.name, tc.input)
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
		t.Run("IsString_"+sType, func(t *testing.T) {
			assert.True(t, isStringType(sType), "Expected '%s' to be a string type", sType)
		})
	}
	for _, nsType := range nonStringTypes {
		t.Run("IsNotString_"+nsType, func(t *testing.T) {
			assert.False(t, isStringType(nsType), "Expected '%s' NOT to be a string type", nsType)
		})
	}
}

func TestIsBinaryType(t *testing.T) {
	binaryTypes := []string{"binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob", "bytea", "bit", "varbit"}
	nonBinaryTypes := []string{"char", "text", "int", "json"}

	for _, bType := range binaryTypes {
		t.Run("IsBinary_"+bType, func(t *testing.T) {
			assert.True(t, isBinaryType(bType), "Expected '%s' to be a binary type", bType)
		})
	}
	for _, nbType := range nonBinaryTypes {
		t.Run("IsNotBinary_"+nbType, func(t *testing.T) {
			assert.False(t, isBinaryType(nbType), "Expected '%s' NOT to be a binary type", nbType)
		})
	}
}

func TestIsNumericType(t *testing.T) {
	numericTypes := []string{
		"tinyint", "smallint", "mediumint", "int", "integer", "bigint",
		"serial", "bigserial", "decimal", "numeric",
		"float", "double", "real", "money",
	}
	nonNumericTypes := []string{"char", "varchar", "text", "date", "timestamp", "bool", "blob", "json"}

	for _, numType := range numericTypes {
		t.Run("IsNumeric_"+numType, func(t *testing.T) {
			assert.True(t, isNumericType(numType), "Expected '%s' to be a numeric type", numType)
		})
	}
	for _, nonNumType := range nonNumericTypes {
		t.Run("IsNotNumeric_"+nonNumType, func(t *testing.T) {
			assert.False(t, isNumericType(nonNumType), "Expected '%s' NOT to be a numeric type", nonNumType)
		})
	}
}

func TestIsIntegerType(t *testing.T) {
	integerTypes := []string{"tinyint", "smallint", "mediumint", "int", "integer", "bigint", "serial", "bigserial"}
	nonIntegerTypes := []string{"decimal", "float", "varchar", "text", "date"}

	for _, intType := range integerTypes {
		t.Run("IsInteger_"+intType, func(t *testing.T) {
			assert.True(t, isIntegerType(intType), "Expected '%s' to be an integer type", intType)
		})
	}
	for _, nonIntType := range nonIntegerTypes {
		t.Run("IsNotInteger_"+nonIntType, func(t *testing.T) {
			assert.False(t, isIntegerType(nonIntType), "Expected '%s' NOT to be an integer type", nonIntType)
		})
	}
}

func TestIsPrecisionRelevant(t *testing.T) {
	relevantTypes := []string{"decimal", "numeric", "time", "timetz", "timestamp", "timestamptz", "datetime"}
	nonRelevantTypes := []string{"int", "varchar", "text", "date", "bool"}

	for _, relType := range relevantTypes {
		t.Run("IsPrecisionRelevant_"+relType, func(t *testing.T) {
			assert.True(t, isPrecisionRelevant(relType), "Expected precision to be relevant for '%s'", relType)
		})
	}
	for _, nonRelType := range nonRelevantTypes {
		t.Run("IsNotPrecisionRelevant_"+nonRelType, func(t *testing.T) {
			assert.False(t, isPrecisionRelevant(nonRelType), "Expected precision NOT to be relevant for '%s'", nonRelType)
		})
	}
}

func TestIsScaleRelevant(t *testing.T) {
	relevantTypes := []string{"decimal", "numeric"}
	nonRelevantTypes := []string{"int", "varchar", "text", "date", "bool", "time", "timestamp"}

	for _, relType := range relevantTypes {
		t.Run("IsScaleRelevant_"+relType, func(t *testing.T) {
			assert.True(t, isScaleRelevant(relType), "Expected scale to be relevant for '%s'", relType)
		})
	}
	for _, nonRelType := range nonRelevantTypes {
		t.Run("IsNotScaleRelevant_"+nonRelType, func(t *testing.T) {
			assert.False(t, isScaleRelevant(nonRelType), "Expected scale NOT to be relevant for '%s'", nonRelType)
		})
	}
}


func TestApplyTypeModifiers(t *testing.T) {
	logger := zaptest.NewLogger(t)

	testCases := []struct {
		name           string
		srcTypeRaw     string
		mappedBaseType string
		dstDialect     string
		expected       string
	}{
		// Sumber tanpa modifier
		{"SrcNoMod_MappedNoMod", "TEXT", "TEXT", "postgres", "TEXT"},
		{"SrcNoMod_MappedWithMod", "TEXT", "VARCHAR(255)", "postgres", "VARCHAR(255)"},
		{"SrcNoMod_MappedNoMod_Time", "TIMESTAMP", "TIMESTAMP", "mysql", "TIMESTAMP"},

		// Sumber dengan modifier, Mapped tanpa modifier
		{"SrcVARCHAR_MappedVARCHAR", "VARCHAR(100)", "VARCHAR", "postgres", "VARCHAR(100)"},
		{"SrcCHAR_MappedCHAR", "CHAR(10)", "CHAR", "mysql", "CHAR(10)"},
		{"SrcDECIMAL_MappedNUMERIC", "DECIMAL(12,4)", "NUMERIC", "postgres", "NUMERIC(12,4)"},
		{"SrcNUMERIC_MappedDECIMAL", "NUMERIC(8)", "DECIMAL", "mysql", "DECIMAL(8)"},
		{"SrcFLOAT_MappedREAL_NoMod", "FLOAT(10,2)", "REAL", "postgres", "REAL"},
		{"SrcDOUBLE_MappedDOUBLE_NoMod", "DOUBLE(15,4)", "DOUBLE", "mysql", "DOUBLE"},
		{"SrcTIMESTAMP_MappedTIMESTAMP_PG_WithPrec", "TIMESTAMP(3) WITH TIME ZONE", "TIMESTAMP WITH TIME ZONE", "postgres", "TIMESTAMP WITH TIME ZONE(3)"},
		{"SrcDATETIME_MappedDATETIME_MySQL_WithPrec", "DATETIME(6)", "DATETIME", "mysql", "DATETIME(6)"},
		{"SrcTIME_MappedTIME_MySQL_WithPrec", "TIME(2)", "TIME", "mysql", "TIME(2)"},
		{"SrcTIME_MappedTIME_PG_WithPrec", "TIME(4) WITHOUT TIME ZONE", "TIME WITHOUT TIME ZONE", "postgres", "TIME WITHOUT TIME ZONE(4)"},

		// Sumber dengan modifier, Mapped DENGAN modifier (prioritas)
		{"SrcENUM_MappedVARCHARWithMod", "ENUM('a','b')", "VARCHAR(50)", "postgres", "VARCHAR(50)"},
		{"SrcSET_MappedTEXT", "SET('x','y')", "TEXT", "postgres", "TEXT"},
		{"SrcDECIMAL_MappedNUMERICWithMod_SrcModWins", "DECIMAL(10,2)", "NUMERIC(20,5)", "postgres", "NUMERIC(10,2)"},

		// Modifier sumber tidak valid atau tidak relevan / tidak transferable
		{"SrcVARCHAR_MappedINT_ModIgnored", "VARCHAR(50)", "INTEGER", "postgres", "INTEGER"},
		{"SrcTEXT_NonNumericMod_MappedVARCHAR_ModIgnored", "TEXT(MAX)", "VARCHAR", "postgres", "VARCHAR"},
		{"SrcTEXT_NumericMod_MappedVARCHAR_ModIgnored", "TEXT(100)", "VARCHAR", "postgres", "VARCHAR"},
		{"SrcDECIMAL_MappedVARCHAR_ModIgnored", "DECIMAL(8,2)", "VARCHAR", "postgres", "VARCHAR"},
		{"SrcINT_DisplayWidth_MappedVARCHAR_ModIgnored", "INT(11)", "VARCHAR", "mysql", "VARCHAR"},

		// Validasi range presisi waktu MySQL
		{"SrcDATETIME_MySQL_PrecInRange", "DATETIME(3)", "DATETIME", "mysql", "DATETIME(3)"},
		{"SrcDATETIME_MySQL_PrecOutOfRangeHi", "DATETIME(7)", "DATETIME", "mysql", "DATETIME"},
		{"SrcDATETIME_MySQL_PrecOutOfRangeLow_Invalid", "DATETIME(-1)", "DATETIME", "mysql", "DATETIME"},
		{"SrcTIMESTAMP_MySQL_PrecValid", "TIMESTAMP(0)", "TIMESTAMP", "mysql", "TIMESTAMP(0)"},
		
		// Validasi presisi/skala DECIMAL/NUMERIC
		{"SrcDECIMAL_InvalidPrec_Char", "DECIMAL(A,2)", "NUMERIC", "postgres", "NUMERIC"},
		{"SrcDECIMAL_InvalidScale_Char", "DECIMAL(10,B)", "NUMERIC", "postgres", "NUMERIC"},
		{"SrcDECIMAL_InvalidPrec_Zero", "DECIMAL(0,0)", "NUMERIC", "postgres", "NUMERIC"},
		{"SrcDECIMAL_InvalidScale_Negative", "DECIMAL(10,-1)", "NUMERIC", "postgres", "NUMERIC"},
		{"SrcDECIMAL_InvalidScale_GtPrec", "DECIMAL(5,6)", "NUMERIC", "postgres", "NUMERIC"},

		// Kasus SQLite dengan presisi waktu (harus diabaikan)
		{"SrcTIMESTAMP_SQLite_PrecIgnored", "TIMESTAMP(3)", "TIMESTAMP", "sqlite", "TIMESTAMP"},
		{"SrcDATETIME_SQLite_PrecIgnored", "DATETIME(6)", "DATETIME", "sqlite", "DATETIME"},

		// MappedBaseType dari pemetaan internal (contoh)
		{"MySQL_INT_to_PG_INTEGER_SrcModIgnored", "INT(11)", "INTEGER", "postgres", "INTEGER"},
		{"MySQL_DECIMAL_to_PG_NUMERIC_SrcModApplied", "DECIMAL(15,5)", "NUMERIC", "postgres", "NUMERIC(15,5)"},
		{"PG_TIMESTAMP_to_MySQL_DATETIME_SrcModApplied", "TIMESTAMP(3)", "DATETIME(6)", "mysql", "DATETIME(3)"},
		{"PG_TIMESTAMP_to_MySQL_DATETIME_SrcModOutOfRange", "TIMESTAMP(7)", "DATETIME(6)", "mysql", "DATETIME"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			syncer := newTestTypeMapperSyncer("any_src_dialect_for_test", tc.dstDialect, logger.Named(tc.name))
			actual := syncer.applyTypeModifiers(tc.srcTypeRaw, tc.mappedBaseType)
			assert.Equal(t, tc.expected, actual)
		})
	}
}
