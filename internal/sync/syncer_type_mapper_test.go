// internal/sync/syncer_type_mapper_test.go
package sync

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/arwahdevops/dbsync/internal/config" // Impor config untuk ModifierHandlingStrategy
)

// Helper newTestTypeMapperSyncer untuk membuat instance SchemaSyncer untuk pengujian.
func newTestTypeMapperSyncer(srcDialect, dstDialect string, logger *zap.Logger) *SchemaSyncer {
	if logger == nil {
		logger = zap.NewNop() // Default ke No-Op logger jika tidak disediakan
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
		{"Timestamp (3) with time zone PG", "timestamp(3) with time zone", "timestamptz"},
		{"bit varying with numeric mod", "bit varying(10)", "varbit"},
		{"character with numeric mod", "character(5)", "char"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Asumsi normalizeTypeName ada di compare_helpers.go (package sync)
			actual := normalizeTypeName(tc.input)
			assert.Equal(t, tc.expected, actual, "Test Case: %s, Input: '%s'", tc.name, tc.input)
		})
	}
}

func TestIsStringType(t *testing.T) {
	stringTypes := []string{"char", "varchar", "nchar", "nvarchar", "text", "tinytext", "mediumtext", "longtext", "clob", "enum", "set", "uuid", "json", "xml"}
	nonStringTypes := []string{"int", "decimal", "bool", "date", "timestamp", "binary", "blob", "bytea"}
	for _, sType := range stringTypes {
		t.Run("IsString_"+sType, func(t *testing.T) { assert.True(t, isStringType(sType), "Expected '%s' to be a string type", sType) })
	}
	for _, nsType := range nonStringTypes {
		t.Run("IsNotString_"+nsType, func(t *testing.T) { assert.False(t, isStringType(nsType), "Expected '%s' NOT to be a string type", nsType) })
	}
}

func TestIsBinaryType(t *testing.T) {
	binaryTypes := []string{"binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob", "bytea", "bit", "varbit"}
	nonBinaryTypes := []string{"char", "text", "int", "json"}
	for _, bType := range binaryTypes {
		t.Run("IsBinary_"+bType, func(t *testing.T) { assert.True(t, isBinaryType(bType), "Expected '%s' to be a binary type", bType) })
	}
	for _, nbType := range nonBinaryTypes {
		t.Run("IsNotBinary_"+nbType, func(t *testing.T) { assert.False(t, isBinaryType(nbType), "Expected '%s' NOT to be a binary type", nbType) })
	}
}

func TestIsNumericType(t *testing.T) {
	numericTypes := []string{"tinyint", "smallint", "mediumint", "int", "integer", "bigint", "serial", "bigserial", "decimal", "numeric", "float", "double", "real", "money"}
	nonNumericTypes := []string{"char", "varchar", "text", "date", "timestamp", "bool", "blob", "json"}
	for _, numType := range numericTypes {
		t.Run("IsNumeric_"+numType, func(t *testing.T) { assert.True(t, isNumericType(numType), "Expected '%s' to be a numeric type", numType) })
	}
	for _, nonNumType := range nonNumericTypes {
		t.Run("IsNotNumeric_"+nonNumType, func(t *testing.T) { assert.False(t, isNumericType(nonNumType), "Expected '%s' NOT to be a numeric type", nonNumType) })
	}
}

func TestIsIntegerType(t *testing.T) {
	integerTypes := []string{"tinyint", "smallint", "mediumint", "int", "integer", "bigint", "serial", "bigserial"}
	nonIntegerTypes := []string{"decimal", "float", "varchar", "text", "date"}
	for _, intType := range integerTypes {
		t.Run("IsInteger_"+intType, func(t *testing.T) { assert.True(t, isIntegerType(intType), "Expected '%s' to be an integer type", intType) })
	}
	for _, nonIntType := range nonIntegerTypes {
		t.Run("IsNotInteger_"+nonIntType, func(t *testing.T) { assert.False(t, isIntegerType(nonIntType), "Expected '%s' NOT to be an integer type", nonIntType) })
	}
}

func TestIsPrecisionRelevant(t *testing.T) {
	relevantTypes := []string{"decimal", "numeric", "time", "timetz", "timestamp", "timestamptz", "datetime"}
	nonRelevantTypes := []string{"int", "varchar", "text", "date", "bool"}
	for _, relType := range relevantTypes {
		t.Run("IsPrecisionRelevant_"+relType, func(t *testing.T) { assert.True(t, isPrecisionRelevant(relType), "Expected precision to be relevant for '%s'", relType) })
	}
	for _, nonRelType := range nonRelevantTypes {
		t.Run("IsNotPrecisionRelevant_"+nonRelType, func(t *testing.T) { assert.False(t, isPrecisionRelevant(nonRelType), "Expected precision NOT to be relevant for '%s'", nonRelType) })
	}
}

func TestIsScaleRelevant(t *testing.T) {
	relevantTypes := []string{"decimal", "numeric"}
	nonRelevantTypes := []string{"int", "varchar", "text", "date", "bool", "time", "timestamp"}
	for _, relType := range relevantTypes {
		t.Run("IsScaleRelevant_"+relType, func(t *testing.T) { assert.True(t, isScaleRelevant(relType), "Expected scale to be relevant for '%s'", relType) })
	}
	for _, nonRelType := range nonRelevantTypes {
		t.Run("IsNotScaleRelevant_"+nonRelType, func(t *testing.T) { assert.False(t, isScaleRelevant(nonRelType), "Expected scale NOT to be relevant for '%s'", nonRelType) })
	}
}

func TestApplyTypeModifiers(t *testing.T) {
	logger := zaptest.NewLogger(t)

	testCases := []struct {
		name                 string
		srcTypeRaw           string
		mappedTypeFromConfig string
		dstDialect           string
		modHandling          config.ModifierHandlingStrategy
		expected             string
	}{
		// --- Kasus untuk ModifierHandlingApplySource ---
		{"ApplySrc_SrcNoMod_MappedNoMod", "TEXT", "TEXT", "postgres", config.ModifierHandlingApplySource, "TEXT"},
		{"ApplySrc_SrcNoMod_MappedWithMod", "TEXT", "VARCHAR(255)", "postgres", config.ModifierHandlingApplySource, "VARCHAR(255)"}, // mappedTypeFromConfig (target) punya modifier, sumber tidak
		{"ApplySrc_SrcVARCHAR_MappedVARCHAR_NoModInTarget", "VARCHAR(100)", "VARCHAR", "postgres", config.ModifierHandlingApplySource, "VARCHAR(100)"},
		{"ApplySrc_SrcCHAR_MappedCHAR_NoModInTarget", "CHAR(10)", "CHAR", "mysql", config.ModifierHandlingApplySource, "CHAR(10)"},
		{"ApplySrc_SrcDECIMAL_MappedNUMERIC_NoModInTarget", "DECIMAL(12,4)", "NUMERIC", "postgres", config.ModifierHandlingApplySource, "NUMERIC(12,4)"},
		{"ApplySrc_SrcTIMESTAMP_MappedTIMESTAMP_PG_WithPrec", "TIMESTAMP(3) WITH TIME ZONE", "TIMESTAMP WITH TIME ZONE", "postgres", config.ModifierHandlingApplySource, "TIMESTAMP WITH TIME ZONE(3)"},
		{"ApplySrc_SrcDATETIME_MappedDATETIME_MySQL_WithPrec", "DATETIME(6)", "DATETIME", "mysql", config.ModifierHandlingApplySource, "DATETIME(6)"},
		{"ApplySrc_SrcTIME_MappedTIME_MySQL_WithPrec", "TIME(2)", "TIME", "mysql", config.ModifierHandlingApplySource, "TIME(2)"},
		{"ApplySrc_SrcTIME_MappedTIME_PG_WithPrec", "TIME(4) WITHOUT TIME ZONE", "TIME WITHOUT TIME ZONE", "postgres", config.ModifierHandlingApplySource, "TIME WITHOUT TIME ZONE(4)"},
		{"ApplySrc_MySQL_INT_to_PG_INTEGER_SrcModIgnored", "INT(11)", "INTEGER", "postgres", config.ModifierHandlingApplySource, "INTEGER"},
		{"ApplySrc_PG_TIMESTAMP_to_MySQL_DATETIME_SrcModApplied", "TIMESTAMP(3)", "DATETIME(6)", "mysql", config.ModifierHandlingApplySource, "DATETIME(3)"}, // mappedTypeFromConfig punya modifier, tapi apply_source akan mencoba menerapkan modifier sumber ke tipe dasar target
		{"ApplySrc_PG_TIMESTAMP_to_MySQL_DATETIME_SrcModOutOfRange", "TIMESTAMP(7)", "DATETIME(6)", "mysql", config.ModifierHandlingApplySource, "DATETIME"}, // presisi sumber (7) invalid untuk DATETIME MySQL, jadi modifier sumber diabaikan, dan karena DATETIME(6) punya modifier sendiri, itu yang dipakai
		{"ApplySrc_SrcDATETIME_MySQL_PrecInRange", "DATETIME(3)", "DATETIME", "mysql", config.ModifierHandlingApplySource, "DATETIME(3)"},
		{"ApplySrc_SrcDATETIME_MySQL_PrecOutOfRangeHi", "DATETIME(7)", "DATETIME", "mysql", config.ModifierHandlingApplySource, "DATETIME"},
		{"ApplySrc_SrcDATETIME_MySQL_PrecOutOfRangeLow_InvalidSrcMod", "DATETIME(-1)", "DATETIME", "mysql", config.ModifierHandlingApplySource, "DATETIME"}, // Modifier sumber (-1) tidak valid
		{"ApplySrc_SrcTIMESTAMP_SQLite_PrecIgnored", "TIMESTAMP(3)", "TIMESTAMP", "sqlite", config.ModifierHandlingApplySource, "TIMESTAMP"},
		{"ApplySrc_SrcVARBINARY_MappedBYTEA", "VARBINARY(100)", "BYTEA", "postgres", config.ModifierHandlingApplySource, "BYTEA"}, // BYTEA PG tidak pakai panjang eksplisit
		{"ApplySrc_SrcBINARY_MappedBINARYMySQL", "BINARY(10)", "BINARY", "mysql", config.ModifierHandlingApplySource, "BINARY(10)"},
		{"ApplySrc_SrcBIT_MappedBITMySQL", "BIT(8)", "BIT", "mysql", config.ModifierHandlingApplySource, "BIT(8)"},
		{"ApplySrc_SrcBIT_MappedVARBITPG", "BIT(64)", "VARBIT", "postgres", config.ModifierHandlingApplySource, "VARBIT(64)"},

		// --- Kasus untuk ModifierHandlingUseTargetDefined ---
		{"UseTargetDef_SrcVARCHAR_MappedVARCHARWithMod", "VARCHAR(100)", "VARCHAR(50)", "postgres", config.ModifierHandlingUseTargetDefined, "VARCHAR(50)"},
		{"UseTargetDef_SrcDECIMAL_MappedNUMERICNoMod", "DECIMAL(10,2)", "NUMERIC", "postgres", config.ModifierHandlingUseTargetDefined, "NUMERIC"},
		{"UseTargetDef_SrcINT_MappedSpecificTarget", "INT(11)", "INTEGER /* target specific */ (123)", "postgres", config.ModifierHandlingUseTargetDefined, "INTEGER /* target specific */ (123)"},
		{"UseTargetDef_SrcTEXT_MappedTargetWithLength", "TEXT", "VARCHAR(MAX)", "mysql", config.ModifierHandlingUseTargetDefined, "VARCHAR(MAX)"}, // VARCHAR(MAX) tidak ada di MySQL, ini contoh saja, seharusnya LONGTEXT

		// --- Kasus untuk ModifierHandlingIgnoreSource ---
		{"IgnoreSrc_SrcVARCHAR_MappedVARCHARWithMod", "VARCHAR(100)", "VARCHAR(30)", "postgres", config.ModifierHandlingIgnoreSource, "VARCHAR(30)"}, // Menggunakan tipe target apa adanya
		{"IgnoreSrc_SrcVARCHAR_MappedTEXTNoMod", "VARCHAR(255)", "TEXT", "postgres", config.ModifierHandlingIgnoreSource, "TEXT"},                     // Menggunakan tipe target apa adanya
		{"IgnoreSrc_SrcINT_MappedBIGINT", "INT(11)", "BIGINT", "mysql", config.ModifierHandlingIgnoreSource, "BIGINT"},
		{"IgnoreSrc_SrcDECIMAL_MappedNUMERICNoMod", "DECIMAL(10,2)", "NUMERIC", "postgres", config.ModifierHandlingIgnoreSource, "NUMERIC"},

		// --- Kasus Khusus dan Tepi ---
		{"ApplySrc_SrcENUM_MappedVARCHARWithMod_TargetModWinsForEnum", "ENUM('a','b')", "VARCHAR(50)", "postgres", config.ModifierHandlingApplySource, "VARCHAR(50)"}, // ENUM adalah kasus khusus
		{"ApplySrc_SrcWithNonNumericMod_TargetNoMod", "GEOMETRY(POINT, 4326)", "GEOMETRY", "postgres", config.ModifierHandlingApplySource, "GEOMETRY"},                  // Modifier sumber tidak transferable, target tidak punya modifier
		{"ApplySrc_SrcWithNonNumericMod_TargetWithMod", "GEOMETRY(POINT, 4326)", "GEOMETRY(Point)", "postgres", config.ModifierHandlingApplySource, "GEOMETRY(Point)"}, // Modifier sumber tidak transferable, target punya modifiernya sendiri
		{"ApplySrc_SrcINT_Unsigned_MappedINT_PG", "INT(10) UNSIGNED", "INTEGER", "postgres", config.ModifierHandlingApplySource, "INTEGER"},                                // UNSIGNED diabaikan, (10) diabaikan untuk INTEGER PG
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// srcDialect dalam newTestTypeMapperSyncer tidak terlalu penting untuk unit test applyTypeModifiers ini,
			// karena fungsi ini lebih fokus pada dstDialect dan bagaimana modifier diterapkan.
			// Namun, jika ada logika di applyTypeModifiers yang bergantung pada srcDialect, ini perlu disesuaikan.
			syncer := newTestTypeMapperSyncer("any_src_for_test", tc.dstDialect, logger.Named(tc.name))
			actual := syncer.applyTypeModifiers(tc.srcTypeRaw, tc.mappedTypeFromConfig, tc.modHandling)
			assert.Equal(t, tc.expected, actual, "Test Case: %s\nSrcType: %s\nMappedTypeFromCfg: %s\nDstDialect: %s\nModHandling: %s", tc.name, tc.srcTypeRaw, tc.mappedTypeFromConfig, tc.dstDialect, tc.modHandling)
		})
	}
}
