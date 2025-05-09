// internal/sync/compare_columns_test.go
package sync

import (
	"database/sql"
	// "strings" // Tidak digunakan secara langsung di file test ini
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// Helper untuk membuat ColumnInfo dengan mudah
func newColInfo(name, typ string, nullable bool, isPrimary bool, isGenerated bool, defaultValue sql.NullString, autoIncrement bool, length sql.NullInt64, precision sql.NullInt64, scale sql.NullInt64, collation sql.NullString, comment sql.NullString) ColumnInfo {
	return ColumnInfo{
		Name:            name,
		Type:            typ,
		MappedType:      "", // Akan diisi secara manual oleh test atau helper
		IsNullable:      nullable,
		IsPrimary:       isPrimary,
		IsGenerated:     isGenerated,
		DefaultValue:    defaultValue,
		AutoIncrement:   autoIncrement,
		Length:          length,
		Precision:       precision,
		Scale:           scale,
		Collation:       collation,
		Comment:         comment,
		OrdinalPosition: 1,
	}
}

// Helper untuk membuat sql.NullInt64
func nullInt(val int64) sql.NullInt64 {
	return sql.NullInt64{Int64: val, Valid: true}
}

// Helper untuk membuat sql.NullString
func nullStr(val string) sql.NullString {
	return sql.NullString{String: val, Valid: true}
}

var noInt = sql.NullInt64{}
var noStr = sql.NullString{}

// --- Test untuk areDefaultsEquivalent ---

func TestAreDefaultsEquivalent(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defaultSyncer := &SchemaSyncer{srcDialect: "mysql", dstDialect: "postgres", logger: logger}

	testCases := []struct {
		name                     string
		srcDefRaw                string
		dstDefRaw                string
		typeForDefaultComparison string
		expected                 bool
		overrideSyncer           *SchemaSyncer
	}{
		{"Identical Strings", "'hello'", "'hello'", "varchar", true, nil},
		{"Identical Strings Different Quotes", "'hello'", `"hello"`, "varchar", true, nil},
		{"Different Strings", "'hello'", "'world'", "varchar", false, nil},
		{"Identical Numbers", "123", "123", "int", true, nil},
		{"Different Numbers", "123", "124", "int", false, nil},
		{"Identical Floats", "1.23", "1.23", "float", true, nil},
		{"Different Floats", "1.23", "1.24", "float", false, nil},
		{"Float Equiv 0", "0", "0.0", "float", true, nil},
		{"Float Equiv 5", "5", "5.00", "decimal(10,2)", true, nil},
		{"Number vs String", "123", "'123'", "int", false, nil},
		{"String vs Number", "'123'", "123", "varchar", false, nil},
		{"NULL vs Empty (Normalized)", "null", "", "varchar", true, nil},
		{"Empty vs NULL (Normalized)", "", "NULL", "int", true, nil},
		{"NULL vs Literal", "NULL", "'hello'", "varchar", false, nil},
		{"Literal vs NULL", "'0'", "null", "int", false, nil},
		{"Both Empty", "", "", "varchar", true, nil},
		{"Both NULL (Normalized)", "null", "NULL", "varchar", true, nil},
		{"Identical Functions (Normalized)", "CURRENT_TIMESTAMP", "now()", "timestamp", true, nil},
		{"Different Functions", "CURRENT_DATE", "now()", "date", false, nil},
		{"Function vs Literal", "now()", "'2025-01-01'", "timestamp", false, nil},
		{"Boolean True Equiv (Normalized)", "1", "true", "bool", true, nil},
		{"Boolean False Equiv (Normalized)", "0", "false", "bool", true, nil},
		{"Boolean True vs False (Normalized)", "1", "false", "bool", false, nil},
		{"MySQL Bool TinyInt Equiv (Normalized)", "'1'", "true", "tinyint(1)", true, &SchemaSyncer{srcDialect: "mysql", dstDialect: "mysql", logger: logger}},
		{"MySQL Bool TinyInt Diff (Normalized)", "'1'", "false", "tinyint(1)", false, &SchemaSyncer{srcDialect: "mysql", dstDialect: "mysql", logger: logger}},
		{"Postgres Cast vs Literal (Normalized)", "'hello'::text", "'hello'", "text", true, &SchemaSyncer{srcDialect: "postgres", dstDialect: "postgres", logger: logger}},
		{"Postgres Cast vs Diff Literal (Normalized)", "'hello'::text", "'world'", "text", false, &SchemaSyncer{srcDialect: "postgres", dstDialect: "postgres", logger: logger}},
		{"Postgres Cast Func vs Func (Normalized)", "now()::timestamp", "current_timestamp", "timestamp", true, &SchemaSyncer{srcDialect: "postgres", dstDialect: "postgres", logger: logger}},
		{"Decimal Comparison with APD (same)", "10.000", "10", "decimal(10,3)", true, nil},
		{"Decimal Comparison with APD (different)", "10.001", "10", "decimal(10,3)", false, nil},
		{"Quoted Numeric vs Numeric", "'123'", "123", "int", true, nil},
		{"Quoted Numeric vs Quoted Numeric", "'123'", "'123.0'", "float", true, nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			currentSyncer := defaultSyncer
			if tc.overrideSyncer != nil {
				currentSyncer = tc.overrideSyncer
			}
			subTestLogger := logger.Named(t.Name())
			actual := currentSyncer.areDefaultsEquivalent(tc.srcDefRaw, tc.dstDefRaw, tc.typeForDefaultComparison, subTestLogger)
			assert.Equal(t, tc.expected, actual, "Test Case: %s", tc.name)
		})
	}
}

// --- Test untuk getColumnModifications ---

func TestGetColumnModifications(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defaultSyncer := &SchemaSyncer{srcDialect: "mysql", dstDialect: "postgres", logger: logger}

	testCases := []struct {
		name           string
		src            ColumnInfo
		dst            ColumnInfo
		srcMappedType  string
		expected       []string
		overrideSyncer *SchemaSyncer
	}{
		{
			name:          "No Difference",
			src:           newColInfo("col1", "INT", false, true, false, noStr, true, noInt, noInt, noInt, noStr, noStr),
			dst:           newColInfo("col1", "INTEGER", false, true, false, noStr, true, noInt, noInt, noInt, noStr, noStr),
			srcMappedType: "INTEGER",
			expected:      []string{},
		},
		{
			name:          "Type Difference (Simple, INT -> VARCHAR)",
			src:           newColInfo("col_type_simple", "INT", false, false, false, noStr, false, noInt, noInt, noInt, noStr, noStr),
			dst:           newColInfo("col_type_simple", "INTEGER", false, false, false, noStr, false, noInt, noInt, noInt, noStr, noStr),
			srcMappedType: "VARCHAR",
			expected:      []string{"type (src: INT, dst: INTEGER)"},
		},
		{
			name:          "Type Difference (Length)",
			src:           newColInfo("col_text_len", "VARCHAR(100)", true, false, false, noStr, false, nullInt(100), noInt, noInt, noStr, noStr),
			dst:           newColInfo("col_text_len", "VARCHAR(50)", true, false, false, noStr, false, nullInt(50), noInt, noInt, noStr, noStr),
			srcMappedType: "VARCHAR(100)",
			expected:      []string{"type (src: VARCHAR(100), dst: VARCHAR(50))"},
		},
		{
			name:          "Type Difference (Fixed vs Var)",
			src:           newColInfo("col_char_var", "CHAR(10)", true, false, false, noStr, false, nullInt(10), noInt, noInt, noStr, noStr),
			dst:           newColInfo("col_char_var", "VARCHAR(10)", true, false, false, noStr, false, nullInt(10), noInt, noInt, noStr, noStr),
			srcMappedType: "CHAR(10)",
			expected:      []string{"type (src: CHAR(10), dst: VARCHAR(10))"},
		},
		{
			name:          "Type Difference (Precision/Scale)",
			src:           newColInfo("col_num_prec", "DECIMAL(12,4)", true, false, false, noStr, false, noInt, nullInt(12), nullInt(4), noStr, noStr),
			dst:           newColInfo("col_num_prec", "NUMERIC(10,2)", true, false, false, noStr, false, noInt, nullInt(10), nullInt(2), noStr, noStr),
			srcMappedType: "NUMERIC(12,4)",
			expected:      []string{"type (src: DECIMAL(12,4), dst: NUMERIC(10,2))"},
		},
		{
			name:          "Type Equivalence (MySQL TINYINT(1) vs PG BOOLEAN)",
			src:           newColInfo("col_bool_equiv", "TINYINT(1)", true, false, false, noStr, false, nullInt(1), noInt, noInt, noStr, noStr),
			dst:           newColInfo("col_bool_equiv", "BOOLEAN", true, false, false, noStr, false, noInt, noInt, noInt, noStr, noStr),
			srcMappedType: "BOOLEAN",
			overrideSyncer: &SchemaSyncer{srcDialect: "mysql", dstDialect: "postgres", logger: logger},
			expected:      []string{},
		},
		{
			name:          "Nullability Difference (Not Null -> Null)",
			src:           newColInfo("col_null_to_not", "INT", true, false, false, noStr, false, noInt, noInt, noInt, noStr, noStr),
			dst:           newColInfo("col_null_to_not", "INTEGER", false, false, false, noStr, false, noInt, noInt, noInt, noStr, noStr),
			srcMappedType: "INTEGER",
			expected:      []string{"nullability (src: true, dst: false)"},
		},
		{
			name:          "Nullability Difference (Null -> Not Null)",
			src:           newColInfo("col_notnull_to_null", "INT", false, false, false, noStr, false, noInt, noInt, noInt, noStr, noStr),
			dst:           newColInfo("col_notnull_to_null", "INTEGER", true, false, false, noStr, false, noInt, noInt, noInt, noStr, noStr),
			srcMappedType: "INTEGER",
			expected:      []string{"nullability (src: false, dst: true)"},
		},
		{
			name:          "Default Difference (Src has, Dst NULL)",
			src:           newColInfo("col_def_src", "VARCHAR(10)", true, false, false, nullStr("def_val"), false, nullInt(10), noInt, noInt, noStr, noStr),
			dst:           newColInfo("col_def_src", "VARCHAR(10)", true, false, false, noStr, false, nullInt(10), noInt, noInt, noStr, noStr),
			srcMappedType: "VARCHAR(10)",
			expected:      []string{"default (src: 'def_val', dst: NULL)"},
		},
		{
			name:          "Default Difference (Src NULL, Dst has)",
			src:           newColInfo("col_def_dst", "VARCHAR(10)", true, false, false, noStr, false, nullInt(10), noInt, noInt, noStr, noStr),
			dst:           newColInfo("col_def_dst", "VARCHAR(10)", true, false, false, nullStr("old_val"), false, nullInt(10), noInt, noInt, noStr, noStr),
			srcMappedType: "VARCHAR(10)",
			expected:      []string{"default (src: NULL, dst: 'old_val')"},
		},
		{
			name:          "Default Difference (Literal -> Literal)",
			src:           newColInfo("col_def_lit", "INT", true, false, false, nullStr("10"), false, noInt, noInt, noInt, noStr, noStr),
			dst:           newColInfo("col_def_lit", "INTEGER", true, false, false, nullStr("5"), false, noInt, noInt, noInt, noStr, noStr),
			srcMappedType: "INTEGER",
			expected:      []string{"default (src: '10', dst: '5')"},
		},
		{
			name:          "Default Difference (Function vs Literal with internal quotes)",
			src:           newColInfo("col_def_func_lit", "TIMESTAMP", true, false, false, nullStr("CURRENT_TIMESTAMP"), false, noInt, noInt, noInt, noStr, noStr),
			dst:           newColInfo("col_def_func_lit", "TIMESTAMP WITHOUT TIME ZONE", true, false, false, nullStr("'2024-01-01'::timestamp"), false, noInt, noInt, noInt, noStr, noStr),
			srcMappedType: "TIMESTAMP WITHOUT TIME ZONE",
			expected:      []string{"default (src: 'CURRENT_TIMESTAMP', dst: ''2024-01-01'::timestamp')"}, // Ini adalah ekspektasi yang benar
		},
		{
			name:          "Default Ignored for AutoIncrement",
			src:           newColInfo("id_auto_ign", "INT", false, true, false, nullStr("1"), true, noInt, noInt, noInt, noStr, noStr),
			dst:           newColInfo("id_auto_ign", "INTEGER", false, true, false, noStr, true, noInt, noInt, noInt, noStr, noStr),
			srcMappedType: "INTEGER",
			expected:      []string{},
		},
		{
			name:          "Default Ignored for Generated Column",
			src:           newColInfo("gen_col_def_ign", "INT AS (id+1)", true, false, true, nullStr("expression"), false, noInt, noInt, noInt, noStr, noStr),
			dst:           newColInfo("gen_col_def_ign", "INTEGER", true, false, true, nullStr("other expr"), false, noInt, noInt, noInt, noStr, noStr),
			srcMappedType: "INT",
			expected:      []string{},
		},
		{
			name:          "AutoIncrement Difference",
			src:           newColInfo("id_auto_change", "INT", false, true, false, noStr, true, noInt, noInt, noInt, noStr, noStr),
			dst:           newColInfo("id_auto_change", "INTEGER", false, true, false, noStr, false, noInt, noInt, noInt, noStr, noStr),
			srcMappedType: "INTEGER",
			expected:      []string{"auto_increment (src: true, dst: false)"},
		},
		{
			name:          "Generated Status Difference",
			src:           newColInfo("gen_stat_change", "INT AS (id+1)", true, false, true, noStr, false, noInt, noInt, noInt, noStr, noStr),
			dst:           newColInfo("gen_stat_change", "INTEGER", true, false, false, noStr, false, noInt, noInt, noInt, noStr, noStr),
			srcMappedType: "INT",
			expected:      []string{"generated_status (src: true, dst: false)"},
		},
		{
			name:          "Collation Difference (Different)",
			src:           newColInfo("col_coll_diff", "VARCHAR(50)", true, false, false, noStr, false, nullInt(50), noInt, noInt, nullStr("utf8mb4_unicode_ci"), noStr),
			dst:           newColInfo("col_coll_diff", "VARCHAR(50)", true, false, false, noStr, false, nullInt(50), noInt, noInt, nullStr("utf8mb4_general_ci"), noStr),
			srcMappedType: "VARCHAR(50)",
			expected:      []string{"collation (src: utf8mb4_unicode_ci, dst: utf8mb4_general_ci)"},
		},
		{
			name:          "Collation Difference (Src has, Dst empty/default)",
			src:           newColInfo("col_coll_src", "VARCHAR(50)", true, false, false, noStr, false, nullInt(50), noInt, noInt, nullStr("latin1_swedish_ci"), noStr),
			dst:           newColInfo("col_coll_src", "VARCHAR(50)", true, false, false, noStr, false, nullInt(50), noInt, noInt, noStr, noStr),
			srcMappedType: "VARCHAR(50)",
			expected:      []string{"collation (src: latin1_swedish_ci, dst: )"},
		},
		{
			name:          "Collation Difference (Dst has, Src empty/default)",
			src:           newColInfo("col_coll_dst", "VARCHAR(50)", true, false, false, noStr, false, nullInt(50), noInt, noInt, noStr, noStr),
			dst:           newColInfo("col_coll_dst", "VARCHAR(50)", true, false, false, noStr, false, nullInt(50), noInt, noInt, nullStr("en_US.UTF-8"), noStr),
			srcMappedType: "VARCHAR(50)",
			expected:      []string{"collation (src: , dst: en_US.UTF-8)"},
		},
		{
			name:          "Collation Ignored for Non-String Type",
			src:           newColInfo("col_int_coll_ign", "INT", true, false, false, noStr, false, noInt, noInt, noInt, nullStr("some_collation"), noStr),
			dst:           newColInfo("col_int_coll_ign", "INTEGER", true, false, false, noStr, false, noInt, noInt, noInt, noStr, noStr),
			srcMappedType: "INTEGER",
			expected:      []string{},
		},
		{
			name:          "Comment Difference (Ignored for DDL diff)",
			src:           newColInfo("col_comm_ign", "INT", false, false, false, noStr, false, noInt, noInt, noInt, noStr, nullStr("Source comment")),
			dst:           newColInfo("col_comm_ign", "INTEGER", false, false, false, noStr, false, noInt, noInt, noInt, noStr, nullStr("Dest comment")),
			srcMappedType: "INTEGER",
			expected:      []string{},
		},
		{
			name:          "Multiple Differences (Type, Nullability, Default)",
			src:           newColInfo("col_multi_diff", "TEXT", false, false, false, nullStr("new default"), false, noInt, noInt, noInt, noStr, noStr),
			dst:           newColInfo("col_multi_diff", "VARCHAR(255)", true, false, false, nullStr("old default"), false, nullInt(255), noInt, noInt, noStr, noStr),
			srcMappedType: "TEXT",
			expected: []string{
				"type (src: TEXT, dst: VARCHAR(255))",
				"nullability (src: false, dst: true)",
				"default (src: 'new default', dst: 'old default')",
			},
		},
		{
			name:          "Type Difference, src.MappedType empty (non-generated)",
			src:           newColInfo("col_empty_map_type", "RARE_TYPE_SRC", false, false, false, noStr, false, noInt, noInt, noInt, noStr, noStr),
			dst:           newColInfo("col_empty_map_type", "RARE_TYPE_DST", false, false, false, noStr, false, noInt, noInt, noInt, noStr, noStr),
			srcMappedType: "", // MappedType kosong, akan fallback ke src.Type untuk perbandingan
			expected:      []string{"type (src: RARE_TYPE_SRC, dst: RARE_TYPE_DST)"},
		},
		{
			name:          "Type Difference, Generated Column (INT AS (...) vs OTHER_INT_TYPE)",
			src:           newColInfo("gen_col_type_diff", "INT AS (id+1) STORED", true, false, true, noStr, false, noInt, noInt, noInt, noStr, noStr),
			dst:           newColInfo("gen_col_type_diff", "OTHER_INT_TYPE", true, false, true, noStr, false, noInt, noInt, noInt, noStr, noStr),
			srcMappedType: "INT", // Hasil dari extractBaseTypeFromGenerated("INT AS (id+1) STORED") adalah "INT"
			expected:      []string{"type (src: INT AS (id+1) STORED, dst: OTHER_INT_TYPE)"},
		},
		{
			name:          "Type Equivalence, Generated Column (INT AS (...) vs INTEGER)",
			src:           newColInfo("gen_col_type_equiv", "INT AS (id+1) VIRTUAL", true, false, true, noStr, false, noInt, noInt, noInt, noStr, noStr),
			dst:           newColInfo("gen_col_type_equiv", "INTEGER", true, false, true, noStr, false, noInt, noInt, noInt, noStr, noStr),
			srcMappedType: "INT", // Hasil dari extractBaseTypeFromGenerated("INT AS (id+1) VIRTUAL") adalah "INT"
			expected:      []string{},
		},
		{
			name:          "MySQL TEXT to PG VARCHAR (Type Difference)",
			src:           newColInfo("mysql_text_pg_varchar", "TEXT", true, false, false, noStr, false, noInt, noInt, noInt, noStr, noStr),
			dst:           newColInfo("mysql_text_pg_varchar", "VARCHAR(1000)", true, false, false, noStr, false, nullInt(1000), noInt, noInt, noStr, noStr),
			srcMappedType: "TEXT",
			overrideSyncer: &SchemaSyncer{srcDialect: "mysql", dstDialect: "postgres", logger: logger},
			expected:      []string{"type (src: TEXT, dst: VARCHAR(1000))"},
		},
		{
			name:          "PG TEXT to MySQL VARCHAR (Type Difference)",
			src:           newColInfo("pg_text_mysql_varchar", "TEXT", true, false, false, noStr, false, noInt, noInt, noInt, noStr, noStr),
			dst:           newColInfo("pg_text_mysql_varchar", "VARCHAR(255)", true, false, false, noStr, false, nullInt(255), noInt, noInt, noStr, noStr),
			srcMappedType: "LONGTEXT",
			overrideSyncer: &SchemaSyncer{srcDialect: "postgres", dstDialect: "mysql", logger: logger},
			expected:      []string{"type (src: TEXT, dst: VARCHAR(255))"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			currentSyncer := defaultSyncer
			if tc.overrideSyncer != nil {
				currentSyncer = tc.overrideSyncer
			}

			tc.src.MappedType = tc.srcMappedType

			subTestLogger := logger.Named(tc.name)
			actual := currentSyncer.getColumnModifications(tc.src, tc.dst, subTestLogger)
			assert.ElementsMatch(t, tc.expected, actual, "Test Case: %s\nSRC: %+v\nDST: %+v", tc.name, tc.src, tc.dst)
		})
	}
}
