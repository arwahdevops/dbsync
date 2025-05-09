// internal/sync/compare_columns_test.go
package sync

import (
	"database/sql"
	"strings" // <- Pastikan ini di-import di atas
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// --- Helper dari test sebelumnya ---

// Helper untuk membuat ColumnInfo dengan mudah
func newColInfo(name, typ string, nullable bool, isPrimary bool, isGenerated bool, defaultValue sql.NullString, autoIncrement bool, length sql.NullInt64, precision sql.NullInt64, scale sql.NullInt64, collation sql.NullString, comment sql.NullString) ColumnInfo {
	return ColumnInfo{
		Name:            name,
		Type:            typ,
		MappedType:      "", // Akan diisi manual dalam test getColumnModifications
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
		OrdinalPosition: 1, // Posisi tidak terlalu relevan untuk tes ini
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
	syncer := &SchemaSyncer{srcDialect: "mysql", dstDialect: "postgres", logger: logger} // Contoh dialek

	testCases := []struct {
		name                   string
		srcDefRaw              string
		dstDefRaw              string
		typeForDefaultComparison string // Tipe *sumber* (bisa MappedType)
		expected               bool
	}{
		{"Identical Strings", "'hello'", "'hello'", "varchar", true},
		{"Identical Strings Different Quotes", "'hello'", `"hello"`, "varchar", true}, // Normalisasi harus menangani ini
		{"Different Strings", "'hello'", "'world'", "varchar", false},
		{"Identical Numbers", "123", "123", "int", true},
		{"Different Numbers", "123", "124", "int", false},
		{"Identical Floats", "1.23", "1.23", "float", true},
		{"Different Floats", "1.23", "1.24", "float", false},
		{"Float Equiv 0", "0", "0.0", "float", true},
		{"Float Equiv 5", "5", "5.00", "decimal", true},
		{"Number vs String", "123", "'123'", "int", false}, // Tipe berbeda cara penanganan defaultnya
		{"String vs Number", "'123'", "123", "varchar", false},
		{"NULL vs Empty", "null", "", "varchar", true},
		{"Empty vs NULL", "", "NULL", "int", true},
		{"NULL vs Literal", "NULL", "'hello'", "varchar", false},
		{"Literal vs NULL", "'0'", "null", "int", false},
		{"Both Empty", "", "", "varchar", true},
		{"Both NULL", "null", "NULL", "varchar", true},
		{"Identical Functions", "CURRENT_TIMESTAMP", "now()", "timestamp", true}, // Normalisasi fungsi
		{"Different Functions", "CURRENT_DATE", "now()", "date", false},
		{"Function vs Literal", "now()", "'2025-01-01'", "timestamp", false},
		{"Boolean True Equiv", "1", "true", "bool", true},
		{"Boolean False Equiv", "0", "false", "bool", true},
		{"Boolean True vs False", "1", "false", "bool", false},
		{"MySQL Bool TinyInt Equiv", "'1'", "true", "tinyint(1)", true}, // Untuk MySQL tinyint(1)
		{"MySQL Bool TinyInt Diff", "'1'", "false", "tinyint(1)", false},
		{"Postgres Cast vs Literal", "'hello'::text", "'hello'", "text", true},
		{"Postgres Cast vs Diff Literal", "'hello'::text", "'world'", "text", false},
		{"Postgres Cast Func vs Func", "now()::timestamp", "current_timestamp", "timestamp", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Gunakan logger sub-test
			subTestLogger := logger.Named(t.Name())
			actual := syncer.areDefaultsEquivalent(tc.srcDefRaw, tc.dstDefRaw, tc.typeForDefaultComparison, subTestLogger)
			assert.Equal(t, tc.expected, actual, "Test Case: %s", tc.name)
		})
	}
}

// --- Test untuk getColumnModifications ---

func TestGetColumnModifications(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	// Buat instance syncer dasar. Dialek bisa diubah per test case jika perlu.
	syncer := &SchemaSyncer{srcDialect: "mysql", dstDialect: "postgres", logger: logger}

	testCases := []struct {
		name     string
		src      ColumnInfo
		dst      ColumnInfo
		expected []string // Daftar perbedaan yang diharapkan
	}{
		{
			name: "No Difference",
			src:  newColInfo("col1", "INT", false, true, false, noStr, true, noInt, noInt, noInt, noStr, noStr), // MappedType akan diisi di bawah
			dst:  newColInfo("col1", "INTEGER", false, true, false, noStr, true, noInt, noInt, noInt, noStr, noStr),
			expected: []string{},
		},
		{
			name: "Type Difference (Simple)",
			src:  newColInfo("col_type_simple", "INT", false, false, false, noStr, false, noInt, noInt, noInt, noStr, noStr), // MappedType: VARCHAR
			dst:  newColInfo("col_type_simple", "INTEGER", false, false, false, noStr, false, noInt, noInt, noInt, noStr, noStr),
			expected: []string{"type (src: INT [mapped to: VARCHAR], dst: INTEGER)"},
		},
		{
			name: "Type Difference (Length)",
			src:  newColInfo("col_text", "VARCHAR(100)", true, false, false, noStr, false, nullInt(100), noInt, noInt, noStr, noStr), // MappedType: VARCHAR(100)
			dst:  newColInfo("col_text", "VARCHAR(50)", true, false, false, noStr, false, nullInt(50), noInt, noInt, noStr, noStr),
			expected: []string{"type (src: VARCHAR(100) [mapped to: VARCHAR(100)], dst: VARCHAR(50))"},
		},
        {
			name: "Type Difference (Fixed vs Var)",
			src:  newColInfo("col_char", "CHAR(10)", true, false, false, noStr, false, nullInt(10), noInt, noInt, noStr, noStr), // MappedType: CHAR(10)
			dst:  newColInfo("col_char", "VARCHAR(10)", true, false, false, noStr, false, nullInt(10), noInt, noInt, noStr, noStr),
			expected: []string{"type (src: CHAR(10) [mapped to: CHAR(10)], dst: VARCHAR(10))"},
		},
		{
			name: "Type Difference (Precision/Scale)",
			src:  newColInfo("col_num", "DECIMAL(12,4)", true, false, false, noStr, false, noInt, nullInt(12), nullInt(4), noStr, noStr), // MappedType: NUMERIC(12,4)
			dst:  newColInfo("col_num", "NUMERIC(10,2)", true, false, false, noStr, false, noInt, nullInt(10), nullInt(2), noStr, noStr),
			expected: []string{"type (src: DECIMAL(12,4) [mapped to: NUMERIC(12,4)], dst: NUMERIC(10,2))"},
		},
		{
			name: "Type Equivalence (TINYINT(1) vs BOOLEAN)",
			src:  newColInfo("col_bool", "TINYINT(1)", true, false, false, noStr, false, nullInt(1), noInt, noInt, noStr, noStr), // MappedType: BOOLEAN
			dst:  newColInfo("col_bool", "BOOLEAN", true, false, false, noStr, false, noInt, noInt, noInt, noStr, noStr),
			expected: []string{}, // Tidak ada perbedaan tipe yang dilaporkan
		},
		{
			name: "Nullability Difference (Not Null -> Null)",
			src:  newColInfo("col1", "INT", true, false, false, noStr, false, noInt, noInt, noInt, noStr, noStr), // MappedType: INTEGER
			dst:  newColInfo("col1", "INTEGER", false, false, false, noStr, false, noInt, noInt, noInt, noStr, noStr),
			expected: []string{"nullability (src: true, dst: false)"},
		},
		{
			name: "Nullability Difference (Null -> Not Null)",
			src:  newColInfo("col1", "INT", false, false, false, noStr, false, noInt, noInt, noInt, noStr, noStr), // MappedType: INTEGER
			dst:  newColInfo("col1", "INTEGER", true, false, false, noStr, false, noInt, noInt, noInt, noStr, noStr),
			expected: []string{"nullability (src: false, dst: true)"},
		},
		{
			name: "Default Difference (NULL -> Literal)",
			src:  newColInfo("col_def", "VARCHAR(10)", true, false, false, nullStr("def"), false, nullInt(10), noInt, noInt, noStr, noStr), // MappedType: VARCHAR(10)
			dst:  newColInfo("col_def", "VARCHAR(10)", true, false, false, noStr, false, nullInt(10), noInt, noInt, noStr, noStr),
			expected: []string{"default (src: ''def'', dst: NULL)"},
		},
		{
			name: "Default Difference (Literal -> NULL)",
			src:  newColInfo("col_def", "VARCHAR(10)", true, false, false, noStr, false, nullInt(10), noInt, noInt, noStr, noStr), // MappedType: VARCHAR(10)
			dst:  newColInfo("col_def", "VARCHAR(10)", true, false, false, nullStr("old"), false, nullInt(10), noInt, noInt, noStr, noStr),
			expected: []string{"default (src: NULL, dst: ''old'')"},
		},
		{
			name: "Default Difference (Literal -> Literal)",
			src:  newColInfo("col_def", "INT", true, false, false, nullStr("10"), false, noInt, noInt, noInt, noStr, noStr), // MappedType: INTEGER
			dst:  newColInfo("col_def", "INTEGER", true, false, false, nullStr("5"), false, noInt, noInt, noInt, noStr, noStr),
			expected: []string{"default (src: ''10'', dst: ''5'')"},
		},
		{
			name: "Default Difference (Function vs Literal)",
			src:  newColInfo("col_time", "TIMESTAMP", true, false, false, nullStr("CURRENT_TIMESTAMP"), false, noInt, noInt, noInt, noStr, noStr), // MappedType: TIMESTAMP...
			dst:  newColInfo("col_time", "TIMESTAMP WITHOUT TIME ZONE", true, false, false, nullStr("'2024-01-01'::timestamp"), false, noInt, noInt, noInt, noStr, noStr),
			expected: []string{"default (src: ''CURRENT_TIMESTAMP'', dst: '''2024-01-01''::timestamp')"}, // Default mentah ditampilkan di pesan
		},
		{
			name: "Default Ignored for AutoIncrement",
			src:  newColInfo("id", "INT", false, true, false, nullStr("1"), true, noInt, noInt, noInt, noStr, noStr), // MappedType: INTEGER
			dst:  newColInfo("id", "INTEGER", false, true, false, noStr, true, noInt, noInt, noInt, noStr, noStr),
			expected: []string{},
		},
        {
			name: "Default Ignored for Generated",
			src:  newColInfo("gen_col", "INT", true, false, true, nullStr("expression"), false, noInt, noInt, noInt, noStr, noStr), // MappedType: INT
			dst:  newColInfo("gen_col", "INTEGER", true, false, true, nullStr("other expr"), false, noInt, noInt, noInt, noStr, noStr),
			expected: []string{}, // Hanya status generated yg dibandingkan, bukan default
		},
		{
			name: "AutoIncrement Difference",
			src:  newColInfo("id", "INT", false, true, false, noStr, true, noInt, noInt, noInt, noStr, noStr), // MappedType: INTEGER
			dst:  newColInfo("id", "INTEGER", false, true, false, noStr, false, noInt, noInt, noInt, noStr, noStr),
			expected: []string{"auto_increment (src: true, dst: false)"},
		},
		{
			name: "Generated Status Difference",
			src:  newColInfo("gen_col", "INT", true, false, true, noStr, false, noInt, noInt, noInt, noStr, noStr), // MappedType: INT
			dst:  newColInfo("gen_col", "INTEGER", true, false, false, noStr, false, noInt, noInt, noInt, noStr, noStr),
			expected: []string{"generated_status (src: true, dst: false)"},
		},
        {
			name: "Collation Difference (Different)",
			src:  newColInfo("col_text", "VARCHAR(50)", true, false, false, noStr, false, nullInt(50), noInt, noInt, nullStr("utf8mb4_unicode_ci"), noStr), // MappedType: VARCHAR(50)
			dst:  newColInfo("col_text", "VARCHAR(50)", true, false, false, noStr, false, nullInt(50), noInt, noInt, nullStr("utf8mb4_general_ci"), noStr),
			expected: []string{"collation (src: utf8mb4_unicode_ci, dst: utf8mb4_general_ci)"},
		},
        {
			name: "Collation Difference (Src has, Dst empty/default)",
			src:  newColInfo("col_text", "VARCHAR(50)", true, false, false, noStr, false, nullInt(50), noInt, noInt, nullStr("latin1_swedish_ci"), noStr), // MappedType: VARCHAR(50)
			dst:  newColInfo("col_text", "VARCHAR(50)", true, false, false, noStr, false, nullInt(50), noInt, noInt, noStr, noStr), // Dst no collation
			expected: []string{"collation (src: latin1_swedish_ci, dst: )"},
		},
        {
			name: "Collation Difference (Dst has, Src empty/default)",
			src:  newColInfo("col_text", "VARCHAR(50)", true, false, false, noStr, false, nullInt(50), noInt, noInt, noStr, noStr), // MappedType: VARCHAR(50)
			dst:  newColInfo("col_text", "VARCHAR(50)", true, false, false, noStr, false, nullInt(50), noInt, noInt, nullStr("en_US.UTF-8"), noStr), // Dst has collation
			expected: []string{"collation (src: , dst: en_US.UTF-8)"},
		},
        {
			name: "Collation Ignored for Non-String Type",
			src:  newColInfo("col_int", "INT", true, false, false, noStr, false, noInt, noInt, noInt, nullStr("some_collation"), noStr), // MappedType: INTEGER
			dst:  newColInfo("col_int", "INTEGER", true, false, false, noStr, false, noInt, noInt, noInt, noStr, noStr),
			expected: []string{}, // Collation diff ignored
		},
		{
			name: "Comment Difference",
			src:  newColInfo("col1", "INT", false, false, false, noStr, false, noInt, noInt, noInt, noStr, nullStr("Source comment")),
			dst:  newColInfo("col1", "INTEGER", false, false, false, noStr, false, noInt, noInt, noInt, noStr, nullStr("Dest comment")),
			expected: []string{}, // Comment diff tidak termasuk dalam hasil standar
		},
		{
			name: "Multiple Differences (Type, Nullability, Default)",
			src:  newColInfo("col_multi", "TEXT", false, false, false, nullStr("new default"), false, noInt, noInt, noInt, noStr, noStr), // MappedType: TEXT
			dst:  newColInfo("col_multi", "VARCHAR(255)", true, false, false, nullStr("old default"), false, nullInt(255), noInt, noInt, noStr, noStr),
			expected: []string{
				"type (src: TEXT [mapped to: TEXT], dst: VARCHAR(255))",
				"nullability (src: false, dst: true)",
				"default (src: ''new default'', dst: ''old default'')",
			},
		},
	}

	// Pre-fill MappedType based on src.Type or test case needs
	// Ini adalah penyederhanaan; dalam aplikasi nyata, mapDataType akan dipanggil.
	mapSrcType := func(src ColumnInfo) string {
		// Logika mapping sederhana untuk tes
		if src.IsGenerated { return src.Type } // Tipe asli untuk generated
		switch src.Name {
		case "col1": return "INTEGER"
		case "col_type_simple": return "VARCHAR" // Simulasi mapping INT ke VARCHAR untuk tes
        case "col_num": return "NUMERIC(12,4)" // Mapping dari DECIMAL(12,4)
        case "col_bool": return "BOOLEAN"      // Mapping dari TINYINT(1)
        case "col_time": return "TIMESTAMP WITHOUT TIME ZONE" // Mapping dari TIMESTAMP
		case "col_text", "col_multi":
            if strings.HasPrefix(src.Type, "VARCHAR") { return src.Type } // PERBAIKAN: Hapus 'strings.'
            return "TEXT"
        case "col_char": return src.Type
        case "col_def":
             if strings.HasPrefix(src.Type, "VARCHAR") { return src.Type } // PERBAIKAN: Hapus 'strings.'
             return "INTEGER"
        case "id": return "INTEGER"
        case "gen_col": return "INT" // Gunakan tipe dasar untuk generated
		default: return src.Type // Fallback
		}
	}

	for i := range testCases {
        // Isi MappedType untuk sumber secara manual berdasarkan logika tes
        testCases[i].src.MappedType = mapSrcType(testCases[i].src)
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			subTestLogger := logger.Named(t.Name())
			actual := syncer.getColumnModifications(tc.src, tc.dst, subTestLogger)
            // Gunakan ElementsMatch karena urutan diffs mungkin tidak penting
			assert.ElementsMatch(t, tc.expected, actual, "Test Case: %s", tc.name)
		})
	}
}
