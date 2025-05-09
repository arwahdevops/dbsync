// internal/sync/syncer_ddl_executor_test.go
package sync

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func newTestSchemaSyncer(dstDialect string, logger *zap.Logger) *SchemaSyncer {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &SchemaSyncer{
		dstDialect: dstDialect,
		logger:     logger,
	}
}

func TestParseAndCategorizeDDLs(t *testing.T) {
	logger := zaptest.NewLogger(t)
	syncerPostgres := newTestSchemaSyncer("postgres", logger)

	testCases := []struct {
		name           string
		syncer         *SchemaSyncer
		inputDDLs      *SchemaExecutionResult
		inputTable     string
		expectedParsed *categorizedDDLs
		expectedError  bool
	}{
		{
			name:   "Empty Input",
			syncer: syncerPostgres,
			inputDDLs: &SchemaExecutionResult{},
			inputTable: "test_table_empty",
			expectedParsed: &categorizedDDLs{
				AlterColumnDDLs:    []string{}, AddIndexDDLs: []string{}, DropIndexDDLs: []string{},
				AddConstraintDDLs:  []string{}, DropConstraintDDLs: []string{},
			},
			expectedError: false,
		},
		{
			name:   "Only Create Table",
			syncer: syncerPostgres,
			inputDDLs: &SchemaExecutionResult{
				TableDDL: "CREATE TABLE \"test_table1\" (id INT PRIMARY KEY);",
			},
			inputTable: "test_table1",
			expectedParsed: &categorizedDDLs{
				CreateTableDDL:     "CREATE TABLE \"test_table1\" (id INT PRIMARY KEY)",
				AlterColumnDDLs:    []string{}, AddIndexDDLs: []string{}, DropIndexDDLs: []string{},
				AddConstraintDDLs:  []string{}, DropConstraintDDLs: []string{},
			},
			expectedError: false,
		},
		{
			name:   "Create Table with Trailing Semicolon and Space",
			syncer: syncerPostgres,
			inputDDLs: &SchemaExecutionResult{
				TableDDL: "CREATE TABLE \"test_table_space\" (name VARCHAR(50));  ",
			},
			inputTable: "test_table_space",
			expectedParsed: &categorizedDDLs{
				CreateTableDDL:     "CREATE TABLE \"test_table_space\" (name VARCHAR(50))",
				AlterColumnDDLs:    []string{}, AddIndexDDLs: []string{}, DropIndexDDLs: []string{},
				AddConstraintDDLs:  []string{}, DropConstraintDDLs: []string{},
			},
			expectedError: false,
		},
		{
			name:   "Only Alter Column DDLs",
			syncer: syncerPostgres,
			inputDDLs: &SchemaExecutionResult{
				TableDDL: "ALTER TABLE \"test_alter\" ADD COLUMN new_col INT; ALTER TABLE \"test_alter\" DROP COLUMN old_col",
			},
			inputTable: "test_alter",
			expectedParsed: &categorizedDDLs{
				CreateTableDDL:     "",
				AlterColumnDDLs:    []string{"ALTER TABLE \"test_alter\" DROP COLUMN old_col", "ALTER TABLE \"test_alter\" ADD COLUMN new_col INT"}, // Sorted
				AddIndexDDLs:       []string{}, DropIndexDDLs: []string{}, AddConstraintDDLs: []string{}, DropConstraintDDLs: []string{},
			},
			expectedError: false,
		},
		{
			name:   "Mixed DDLs",
			syncer: syncerPostgres,
			inputDDLs: &SchemaExecutionResult{
				TableDDL: "ALTER TABLE \"mixed_table\" MODIFY COLUMN status VARCHAR(20);",
				IndexDDLs: []string{
					"CREATE UNIQUE INDEX idx_mixed_unique ON \"mixed_table\" (email);",
					"DROP INDEX idx_mixed_old_b;", // Diurutkan jadi setelah _old
					"DROP INDEX idx_mixed_old;",
				},
				ConstraintDDLs: []string{
					"ALTER TABLE \"mixed_table\" ADD CONSTRAINT uq_mixed_name UNIQUE (name);",
					"ALTER TABLE \"mixed_table\" DROP CONSTRAINT fk_mixed_obsolete;",
					"ALTER TABLE \"mixed_table\" ADD CONSTRAINT fk_mixed_new FOREIGN KEY (user_id) REFERENCES users(id);",
					"ALTER TABLE \"mixed_table\" DROP CONSTRAINT pk_to_drop;",
				},
			},
			inputTable: "mixed_table",
			expectedParsed: &categorizedDDLs{
				CreateTableDDL:  "",
				AlterColumnDDLs: []string{"ALTER TABLE \"mixed_table\" MODIFY COLUMN status VARCHAR(20)"},
				AddIndexDDLs:    []string{"CREATE UNIQUE INDEX idx_mixed_unique ON \"mixed_table\" (email)"}, // Diurutkan
				DropIndexDDLs:   []string{"DROP INDEX idx_mixed_old", "DROP INDEX idx_mixed_old_b"}, // Diurutkan
				AddConstraintDDLs: []string{
					"ALTER TABLE \"mixed_table\" ADD CONSTRAINT uq_mixed_name UNIQUE (name)",
					"ALTER TABLE \"mixed_table\" ADD CONSTRAINT fk_mixed_new FOREIGN KEY (user_id) REFERENCES users(id)",
				}, // Diurutkan: UNIQUE, FK
				DropConstraintDDLs: []string{
					"ALTER TABLE \"mixed_table\" DROP CONSTRAINT fk_mixed_obsolete",
					"ALTER TABLE \"mixed_table\" DROP CONSTRAINT pk_to_drop",
				}, // Diurutkan: FK, PK
			},
			expectedError: false,
		},
		{
			name:   "Unrecognized DDL in TableDDL (starts with unknown)",
			syncer: syncerPostgres,
			inputDDLs: &SchemaExecutionResult{
				TableDDL: "UNKNOWN DDL STATEMENT; ALTER TABLE tbl ADD col1 INT;",
			},
			inputTable: "unrec_table",
			expectedParsed: &categorizedDDLs{
				CreateTableDDL:     "",
				AlterColumnDDLs:    []string{"ALTER TABLE tbl ADD col1 INT", "UNKNOWN DDL STATEMENT"}, // Sorted
				AddIndexDDLs:       []string{}, DropIndexDDLs: []string{}, AddConstraintDDLs: []string{}, DropConstraintDDLs: []string{},
			},
			expectedError: false,
		},
		{
			name:   "Create Table Followed by Alter and other DDL",
			syncer: syncerPostgres,
			inputDDLs: &SchemaExecutionResult{
				TableDDL: "CREATE TABLE create_then_alter (id INT); ALTER TABLE create_then_alter ADD COLUMN name VARCHAR(10); SELECT 1;",
			},
			inputTable: "create_then_alter",
			expectedParsed: &categorizedDDLs{
				CreateTableDDL:     "CREATE TABLE create_then_alter (id INT)",
				AlterColumnDDLs:    []string{"ALTER TABLE create_then_alter ADD COLUMN name VARCHAR(10)"},
				AddIndexDDLs:       []string{}, DropIndexDDLs: []string{}, AddConstraintDDLs: []string{}, DropConstraintDDLs: []string{},
			}, // "SELECT 1" akan diabaikan dengan warning
			expectedError: false,
		},
		{
			name:   "Empty statements and only semicolons",
			syncer: syncerPostgres,
			inputDDLs: &SchemaExecutionResult{
				TableDDL: "; ; ALTER TABLE t1 ADD c1 INT; ;",
				IndexDDLs: []string{"", " ; ", "DROP INDEX idx1;", "  "},
			},
			inputTable: "t1",
			expectedParsed: &categorizedDDLs{
				AlterColumnDDLs:    []string{"ALTER TABLE t1 ADD c1 INT"},
				DropIndexDDLs:      []string{"DROP INDEX idx1"},
				AddIndexDDLs:       []string{}, AddConstraintDDLs: []string{}, DropConstraintDDLs: []string{},
			},
			expectedError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			currentSyncer := tc.syncer
			if currentSyncer == nil { // Fallback jika tidak diset di test case
				currentSyncer = newTestSchemaSyncer("postgres", logger.Named(tc.name))
			} else {
				// Re-assign logger untuk test case ini jika syncer sudah ada
				currentSyncer.logger = logger.Named(tc.name)
			}

			actualParsed, err := currentSyncer.parseAndCategorizeDDLs(tc.inputDDLs, tc.inputTable)

			if tc.expectedError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedParsed.CreateTableDDL, actualParsed.CreateTableDDL, "CreateTableDDL mismatch")
				assert.ElementsMatch(t, tc.expectedParsed.AlterColumnDDLs, actualParsed.AlterColumnDDLs, "AlterColumnDDLs mismatch")
				assert.ElementsMatch(t, tc.expectedParsed.AddIndexDDLs, actualParsed.AddIndexDDLs, "AddIndexDDLs mismatch")
				assert.ElementsMatch(t, tc.expectedParsed.DropIndexDDLs, actualParsed.DropIndexDDLs, "DropIndexDDLs mismatch")
				assert.ElementsMatch(t, tc.expectedParsed.AddConstraintDDLs, actualParsed.AddConstraintDDLs, "AddConstraintDDLs mismatch")
				assert.ElementsMatch(t, tc.expectedParsed.DropConstraintDDLs, actualParsed.DropConstraintDDLs, "DropConstraintDDLs mismatch")
			}
		})
	}
}

func TestShouldIgnoreDDLError(t *testing.T) {
	logger := zaptest.NewLogger(t)

	testCases := []struct {
		name     string
		dialect  string
		err      error
		expected bool
	}{
		{"PG: Duplicate Table (CREATE)", "postgres", errors.New(`ERROR: relation "my_table" already exists (sqlstate 42P07)`), true},
		{"PG: Duplicate Index (CREATE)", "postgres", errors.New(`ERROR: relation "my_index" already exists (sqlstate 42P07)`), true},
		{"PG: Duplicate Constraint", "postgres", errors.New(`ERROR: constraint "my_constraint" for relation "my_table" already exists (sqlstate 42710)`), true},
		{"PG: Index Does Not Exist (DROP IF EXISTS)", "postgres", errors.New(`ERROR: index "non_existent_index" does not exist (sqlstate 42704)`), true},
		{"PG: Table Does Not Exist (DROP IF EXISTS)", "postgres", errors.New(`ERROR: table "non_existent_table" does not exist (sqlstate 42P01)`), true},
		{"PG: Constraint Does Not Exist (DROP IF EXISTS)", "postgres", errors.New(`ERROR: constraint "non_existent_constraint" on table "some_table" does not exist (sqlstate 42704)`), true},
		{"PG: Type Already Exists", "postgres", errors.New(`ERROR: type "my_enum" already exists (sqlstate 42P07)`), true},
		{"PG: Schema Already Exists", "postgres", errors.New(`ERROR: schema "my_schema" already exists (sqlstate 42P07)`), true},
		{"PG: Real Error", "postgres", errors.New("ERROR: syntax error at or near \"INVALID\" (sqlstate 42601)"), false},
		{"PG: Nil Error", "postgres", nil, false},

		{"MySQL: Duplicate Key Name (Index)", "mysql", errors.New("Error 1061 (42000): Duplicate key name 'idx_name'"), true},
		{"MySQL: Can't Drop Index (Not Exists)", "mysql", errors.New("Error 1091 (42000): Can't DROP INDEX `idx_does_not_exist`; check that it exists"), true},
		{"MySQL: Can't Drop Index with single quotes", "mysql", errors.New("Error 1091 (42000): Can't DROP 'idx_does_not_exist'; check that it exists"), true},
		{"MySQL: Can't Drop Index without quotes", "mysql", errors.New("Error 1091 (42000): Can't DROP idx_no_quote; check that it exists"), true},
		{"MySQL: Table Already Exists", "mysql", errors.New("Error 1050 (42S01): Table 'my_table' already exists"), true},
		{"MySQL: Unknown Table (DROP)", "mysql", errors.New("Error 1051 (42S02): Unknown table 'my_db.my_table'"), true},
		{"MySQL: Duplicate Column Name (ADD)", "mysql", errors.New("Error 1060 (42S21): Duplicate column name 'col1'"), true},
		{"MySQL: FK Already Exists", "mysql", errors.New("Error 1826 (HY000): Foreign key constraint for key 'fk_name' already exists."), true},
		{"MySQL: FK Already Exists (Varian 2 - nama constraint)", "mysql", errors.New("ERROR 1826 (HY000): Foreign key constraint 'my_fk_constraint_name' already exists."), true},
		{"MySQL: Check Constraint Already Exists", "mysql", errors.New("Error 3822 (HY000): Check constraint 'chk_name' already exists."), true},
		{"MySQL: Constraint Does Not Exist (DROP)", "mysql", errors.New("Error 1091 (42000): Can't DROP CONSTRAINT `non_existent_constraint`; check that it exists"), true},
		{"MySQL: Real Error", "mysql", errors.New("Error 1064 (42000): You have an error in your SQL syntax..."), false},
		{"MySQL: Nil Error", "mysql", nil, false},
		{"MySQL: Index already exists (MariaDB variant)", "mysql", errors.New("Index `idx_some_name` already exists on table `my_table`"), true},

		{"SQLite: Index Already Exists", "sqlite", errors.New("index idx_test already exists"), true},
		{"SQLite: Table Already Exists", "sqlite", errors.New("table my_table already exists"), true},
		{"SQLite: No Such Index (DROP)", "sqlite", errors.New("no such index: idx_gone"), true},
		{"SQLite: No Such Table (DROP)", "sqlite", errors.New("no such table: table_gone"), true},
		{"SQLite: Constraint Already Exists (UNIQUE)", "sqlite", errors.New("constraint uq_col already exists"), true}, // Pesan generik
		{"SQLite: Constraint Failed (CHECK)", "sqlite", errors.New("constraint ck_val failed"), true},
		{"SQLite: Column Already Exists (ADD COLUMN)", "sqlite", errors.New("column new_col already exists"), true},
		{"SQLite: Real Error", "sqlite", errors.New("near \"SLECT\": syntax error"), false},
		{"SQLite: Nil Error", "sqlite", nil, false},

		{"Unknown Dialect: Real Error", "oracle", errors.New("ORA-00942: table or view does not exist"), false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			currentSyncer := newTestSchemaSyncer(tc.dialect, logger.Named(tc.name))
			actual := currentSyncer.shouldIgnoreDDLError(tc.err)
			assert.Equal(t, tc.expected, actual, "Error for: %s", tc.err)
		})
	}
}

func TestSortConstraintsForDrop(t *testing.T) {
	logger := zaptest.NewLogger(t)
	syncer := newTestSchemaSyncer("mysql", logger)

	input := []string{
		"ALTER TABLE t1 DROP CONSTRAINT uq_1",
		"ALTER TABLE t1 DROP CONSTRAINT fk_1",
		"ALTER TABLE t1 DROP CONSTRAINT pk_constraint_name",
		"ALTER TABLE t1 DROP CONSTRAINT fk_2",
		"ALTER TABLE t1 DROP CONSTRAINT chk_1",
		"ALTER TABLE t1 DROP FOREIGN KEY mysql_fk_direct",
		"ALTER TABLE t1 DROP PRIMARY KEY",
		"ALTER TABLE t1 DROP CHECK mysql_check_direct",
		"DROP CONSTRAINT unknown_type_cons",
		"ALTER TABLE t1 DROP CONSTRAINT some_other_constraint", // Tipe tidak diketahui dari nama
	}
	// Urutan Drop: FKs, UNIQUEs, CHECKs, PKs, Others
	expected := []string{
		"ALTER TABLE t1 DROP CONSTRAINT fk_1",                 // FK by name
		"ALTER TABLE t1 DROP CONSTRAINT fk_2",                 // FK by name
		"ALTER TABLE t1 DROP FOREIGN KEY mysql_fk_direct",     // FK by keyword
		"ALTER TABLE t1 DROP CONSTRAINT uq_1",                 // UNIQUE by name
		"ALTER TABLE t1 DROP CONSTRAINT chk_1",                // CHECK by name
		"ALTER TABLE t1 DROP CHECK mysql_check_direct",        // CHECK by keyword
		"ALTER TABLE t1 DROP CONSTRAINT pk_constraint_name",   // PK by name
		"ALTER TABLE t1 DROP PRIMARY KEY",                     // PK by keyword
		"ALTER TABLE t1 DROP CONSTRAINT some_other_constraint", // Unknown, sorted alphabetically
		"DROP CONSTRAINT unknown_type_cons",                   // Unknown, sorted alphabetically
	}
	actual := syncer.sortConstraintsForDrop(input)
	assert.Equal(t, expected, actual)
}

func TestSortConstraintsForAdd(t *testing.T) {
	logger := zaptest.NewLogger(t)
	syncer := newTestSchemaSyncer("postgres", logger)

	input := []string{
		"ALTER TABLE t1 ADD CONSTRAINT fk_1 FOREIGN KEY (col) REFERENCES t2(id)",
		"ALTER TABLE t1 ADD CONSTRAINT uq_1 UNIQUE (col_u)",
		"ALTER TABLE t1 ADD CONSTRAINT pk_1 PRIMARY KEY (id)",
		"ALTER TABLE t1 ADD CONSTRAINT chk_1 CHECK (col > 0)",
		"ALTER TABLE t1 ADD CONSTRAINT fk_2 FOREIGN KEY (col_b) REFERENCES t3(id)",
		"ADD CONSTRAINT unknown_add_cons TYPELESS",
	}
	// Urutan Add: PK, Unique, Check, FK, Others
	expected := []string{
		"ALTER TABLE t1 ADD CONSTRAINT pk_1 PRIMARY KEY (id)",
		"ALTER TABLE t1 ADD CONSTRAINT uq_1 UNIQUE (col_u)",
		"ALTER TABLE t1 ADD CONSTRAINT chk_1 CHECK (col > 0)",
		"ALTER TABLE t1 ADD CONSTRAINT fk_1 FOREIGN KEY (col) REFERENCES t2(id)",
		"ALTER TABLE t1 ADD CONSTRAINT fk_2 FOREIGN KEY (col_b) REFERENCES t3(id)",
		"ADD CONSTRAINT unknown_add_cons TYPELESS",
	}
	actual := syncer.sortConstraintsForAdd(input)
	assert.Equal(t, expected, actual)
}

func TestSortAlterColumns(t *testing.T) {
	logger := zaptest.NewLogger(t)
	syncer := newTestSchemaSyncer("postgres", logger)

	input := []string{
		"ALTER TABLE t1 ADD COLUMN new_col3 INT",
		"ALTER TABLE t1 MODIFY COLUMN existing_col VARCHAR(50)", // MySQL
		"ALTER TABLE t1 ALTER COLUMN pg_col TYPE VARCHAR(50)",  // PostgreSQL
		"ALTER TABLE t1 DROP COLUMN old_col1",
		"ALTER TABLE t1 ADD COLUMN new_col1 TEXT",
		"ALTER TABLE t1 ALTER COLUMN type_change TYPE BIGINT",
		"ALTER TABLE t1 DROP COLUMN old_col2",
		"UNKNOWN DDL STATEMENT",
	}
	// Urutan: DROP, MODIFY/ALTER, ADD, Unknown
	expected := []string{
		"ALTER TABLE t1 DROP COLUMN old_col1",
		"ALTER TABLE t1 DROP COLUMN old_col2",
		"ALTER TABLE t1 MODIFY COLUMN existing_col VARCHAR(50)",
		"ALTER TABLE t1 ALTER COLUMN pg_col TYPE VARCHAR(50)",
		"ALTER TABLE t1 ALTER COLUMN type_change TYPE BIGINT",
		"ALTER TABLE t1 ADD COLUMN new_col3 INT",
		"ALTER TABLE t1 ADD COLUMN new_col1 TEXT",
		"UNKNOWN DDL STATEMENT",
	}
	actual := syncer.sortAlterColumns(input)
	assert.Equal(t, expected, actual)
}

func TestSortIndexes(t *testing.T) {
	logger := zaptest.NewLogger(t)
	syncer := newTestSchemaSyncer("postgres", logger)

	dropInput := []string{"DROP INDEX b", "DROP INDEX a", "DROP INDEX c"}
	dropExpected := []string{"DROP INDEX a", "DROP INDEX b", "DROP INDEX c"}
	assert.Equal(t, dropExpected, syncer.sortDropIndexes(dropInput))

	addInput := []string{"CREATE INDEX b_idx ON t(b)", "CREATE INDEX a_idx ON t(a)", "CREATE UNIQUE INDEX c_idx ON t(c)"}
	addExpected := []string{"CREATE INDEX a_idx ON t(a)", "CREATE INDEX b_idx ON t(b)", "CREATE UNIQUE INDEX c_idx ON t(c)"}
	assert.Equal(t, addExpected, syncer.sortAddIndexes(addInput))
}

func TestSplitPostgresFKsForDeferredExecution(t *testing.T) {
	logger := zaptest.NewLogger(t)
	syncer := newTestSchemaSyncer("postgres", logger)

	inputConstraints := []string{
		"ALTER TABLE orders ADD CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(id) DEFERRABLE INITIALLY DEFERRED",
		"ALTER TABLE products ADD CONSTRAINT uq_sku UNIQUE (sku)",
		"ALTER TABLE order_items ADD CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES orders(id)",
		"ALTER TABLE payment_details ADD CONSTRAINT fk_payment_order FOREIGN KEY (order_id) REFERENCES orders(id) DEFERRABLE INITIALLY IMMEDIATE",
		"ALTER TABLE users ADD CONSTRAINT chk_email CHECK (email LIKE '%@%')",
		"ALTER TABLE self_ref ADD CONSTRAINT fk_self FOREIGN KEY (parent_id) REFERENCES self_ref(id) DEFERRABLE INITIALLY DEFERRED",
	}

	expectedDeferredFKs := []string{
		"ALTER TABLE orders ADD CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(id) DEFERRABLE INITIALLY DEFERRED",
		"ALTER TABLE self_ref ADD CONSTRAINT fk_self FOREIGN KEY (parent_id) REFERENCES self_ref(id) DEFERRABLE INITIALLY DEFERRED",
	}
	expectedNonDeferredFKs := []string{
		"ALTER TABLE products ADD CONSTRAINT uq_sku UNIQUE (sku)",
		"ALTER TABLE order_items ADD CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES orders(id)",
		"ALTER TABLE payment_details ADD CONSTRAINT fk_payment_order FOREIGN KEY (order_id) REFERENCES orders(id) DEFERRABLE INITIALLY IMMEDIATE",
		"ALTER TABLE users ADD CONSTRAINT chk_email CHECK (email LIKE '%@%')",
	}

	actualDeferred, actualNonDeferred := syncer.splitPostgresFKsForDeferredExecution(inputConstraints)

	assert.ElementsMatch(t, expectedDeferredFKs, actualDeferred, "Deferred FKs mismatch")
	assert.ElementsMatch(t, expectedNonDeferredFKs, actualNonDeferred, "Non-deferred FKs (and other constraints) mismatch")
}
