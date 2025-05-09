// internal/sync/compare_columns_test.go
package sync

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// Helper untuk membuat ColumnInfo dengan mudah
func newColInfo(name, typ string, nullable bool, isPrimary bool, isGenerated bool, defaultValue sql.NullString, autoIncrement bool, length sql.NullInt64, precision sql.NullInt64, scale sql.NullInt64, collation sql.NullString, comment sql.NullString) ColumnInfo {
	return ColumnInfo{
		Name:            name,
		Type:            typ,
		MappedType:      "", // Akan diisi secara manual oleh test
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

func nullInt(val int64) sql.NullInt64 { return sql.NullInt64{Int64: val, Valid: true} }
func nullStr(val string) sql.NullString { return sql.NullString{String: val, Valid: true} }

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
		expected                 bool // Ekspektasi disesuaikan dengan logika "presisi DDL"
		overrideSyncer           *SchemaSyncer
	}{
		{"Identical Strings", "'hello'", "'hello'", "varchar", true, nil},
		{"Identical Strings Different Quotes", "'hello'", `"hello"`, "varchar", true, nil},
		{"Different Strings", "'hello'", "'world'", "varchar", false, nil},
		{"Identical Numbers (Raw)", "123", "123", "int", true, nil},
		{"Different Numbers (Raw)", "123", "124", "int", false, nil},
		{"Identical Floats (Raw)", "1.23", "1.23", "float", true, nil},
		{"Different Floats (Raw)", "1.23", "1.24", "float", false, nil},
		{"Float Equiv 0 (Raw)", "0", "0.0", "float", true, nil}, // "0" dan "0.0" setelah normalisasi akan dibandingkan sebagai float
		{"Float Equiv 5 (Raw)", "5", "5.00", "decimal(10,2)", true, nil}, // "5" dan "5.00" setelah normalisasi akan dibandingkan numerik

		// Kasus di mana DDL asli berbeda (angka vs. string angka)
		{"Number vs String (type int)", "123", "'123'", "int", false, nil},         // Diharapkan false karena DDL asli beda
		{"String vs Number (type varchar)", "'123'", "123", "varchar", false, nil}, // Diharapkan false karena DDL asli beda

		{"NULL vs Empty (Normalized)", "null", "", "varchar", true, nil},
		{"Empty vs NULL (Normalized)", "", "NULL", "int", true, nil},
		{"NULL vs Literal", "NULL", "'hello'", "varchar", false, nil},
		{"Literal vs NULL", "'0'", "null", "int", false, nil}, // '0' adalah literal, NULL adalah null
		{"Both Empty", "", "", "varchar", true, nil},
		{"Both NULL (Normalized)", "null", "NULL", "varchar", true, nil},
		{"Identical Functions (Normalized)", "CURRENT_TIMESTAMP", "now()", "timestamp", true, nil},
		{"Different Functions", "CURRENT_DATE", "now()", "date", false, nil},
		{"Function vs Literal", "now()", "'2025-01-01'", "timestamp", false, nil},
		{"Boolean True Equiv (Normalized)", "1", "true", "bool", true, nil}, // true -> "1"
		{"Boolean False Equiv (Normalized)", "0", "false", "bool", true, nil},// false -> "0"
		{"Boolean True vs False (Normalized)", "1", "false", "bool", false, nil}, // "1" vs "0"
		{"MySQL Bool TinyInt Equiv (Normalized)", "'1'", "true", "tinyint(1)", true, &SchemaSyncer{srcDialect: "mysql", dstDialect: "mysql", logger: logger}}, // true -> "1"
		{"MySQL Bool TinyInt Diff (Normalized)", "'1'", "false", "tinyint(1)", false, &SchemaSyncer{srcDialect: "mysql", dstDialect: "mysql", logger: logger}}, // "1" vs "0"
		{"Postgres Cast vs Literal (Normalized)", "'hello'::text", "'hello'", "text", true, &SchemaSyncer{srcDialect: "postgres", dstDialect: "postgres", logger: logger}},
		{"Postgres Cast vs Diff Literal (Normalized)", "'hello'::text", "'world'", "text", false, &SchemaSyncer{srcDialect: "postgres", dstDialect: "postgres", logger: logger}},
		{"Postgres Cast Func vs Func (Normalized)", "now()::timestamp", "current_timestamp", "timestamp", true, &SchemaSyncer{srcDialect: "postgres", dstDialect: "postgres", logger: logger}},
		{"Decimal Comparison with APD (same, normalized)", "10.000", "10", "decimal(10,3)", true, nil}, // "10.000" vs "10" -> APD(10.000) vs APD(10) -> sama
		{"Decimal Comparison with APD (different, normalized)", "10.001", "10", "decimal(10,3)", false, nil}, // "10.001" vs "10" -> APD(10.001) vs APD(10) -> beda

		// Kasus yang menguji logika "quoted vs raw" vs "nilai setelah normalisasi"
		{"Quoted Numeric vs Numeric (type int)", "'123'", "123", "int", false, nil}, // Diharapkan false karena DDL asli beda (ekspektasi dari diskusi)
		{"Quoted Numeric vs Quoted Numeric (type float, same value)", "'123'", "'123.0'", "float", true, nil}, // Setelah normalisasi: "123" vs "123.0", lalu float(123) vs float(123.0) -> true
		{"Quoted Numeric vs Quoted Numeric (type float, diff value)", "'123.1'", "'123.0'", "float", false, nil}, // Setelah normalisasi: "123.1" vs "123.0", lalu float(123.1) vs float(123.0) -> false
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			currentSyncer := defaultSyncer
			if tc.overrideSyncer != nil {
				currentSyncer = tc.overrideSyncer
			}
			subTestLogger := logger.Named(tc.name)
			actual := currentSyncer.areDefaultsEquivalent(tc.srcDefRaw, tc.dstDefRaw, tc.typeForDefaultComparison, subTestLogger)
			if tc.expected != actual {
				// Tambahkan log lebih detail jika gagal
				t.Logf("Debug Info for: %s", tc.name)
				t.Logf("  srcDefRaw: '%s', dstDefRaw: '%s', typeForDefaultComparison: '%s'", tc.srcDefRaw, tc.dstDefRaw, tc.typeForDefaultComparison)
				// Anda bisa memanggil normalizeDefaultValue di sini untuk melihat hasilnya jika perlu
				normSrc := normalizeDefaultValue(tc.srcDefRaw, currentSyncer.srcDialect)
				normDst := normalizeDefaultValue(tc.dstDefRaw, currentSyncer.dstDialect)
				t.Logf("  normSrcDef: '%s', normDstDef: '%s'", normSrc, normDst)
			}
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
			expected:      []string{"default (src: 'CURRENT_TIMESTAMP', dst: ''2024-01-01'::timestamp')"},
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
			if !assert.ElementsMatch(t, tc.expected, actual) {
				// Tambahkan log lebih detail jika assert gagal
				t.Logf("Test Case: %s FAILED", tc.name)
				t.Logf("  SRC ColumnInfo: %+v", tc.src)
				t.Logf("  DST ColumnInfo: %+v", tc.dst)
				t.Logf("  Expected Diffs: %v", tc.expected)
				t.Logf("  Actual Diffs  : %v", actual)
			}
		})
	}
}
