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

// newTestSchemaSyncer adalah helper untuk membuat instance SchemaSyncer untuk pengujian.
// Ini dibutuhkan karena fungsi sorting adalah method dari SchemaSyncer.
func newTestSchemaSyncer(dstDialect string, logger *zap.Logger) *SchemaSyncer {
	if logger == nil {
		logger = zap.NewNop() // Default ke No-Op logger jika tidak disediakan
	}
	return &SchemaSyncer{
		dstDialect: dstDialect,
		logger:     logger,
	}
}

func TestParseAndCategorizeDDLs(t *testing.T) {
	logger := zaptest.NewLogger(t)
	// Gunakan syncer dengan dialek tertentu jika logika parsing bergantung padanya,
	// namun untuk kategorisasi umum, dialek mungkin tidak terlalu berpengaruh.
	// Kita akan set logger per test case jika perlu.
	syncerPostgres := newTestSchemaSyncer("postgres", logger) // Default syncer

	testCases := []struct {
		name           string
		syncer         *SchemaSyncer // Untuk kasus yang mungkin memerlukan dialek spesifik di syncer
		inputDDLs      *SchemaExecutionResult
		inputTable     string
		expectedParsed *categorizedDDLs
		expectedError  bool
	}{
		{
			name:       "Empty Input",
			syncer:     syncerPostgres,
			inputDDLs:  &SchemaExecutionResult{},
			inputTable: "test_table_empty",
			expectedParsed: &categorizedDDLs{
				CreateTableDDL:  "", // Eksplisitkan
				AlterColumnDDLs: []string{}, AddIndexDDLs: []string{}, DropIndexDDLs: []string{},
				AddConstraintDDLs: []string{}, DropConstraintDDLs: []string{},
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
				CreateTableDDL:  "CREATE TABLE \"test_table1\" (id INT PRIMARY KEY)",
				AlterColumnDDLs: []string{}, AddIndexDDLs: []string{}, DropIndexDDLs: []string{},
				AddConstraintDDLs: []string{}, DropConstraintDDLs: []string{},
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
				CreateTableDDL:  "CREATE TABLE \"test_table_space\" (name VARCHAR(50))",
				AlterColumnDDLs: []string{}, AddIndexDDLs: []string{}, DropIndexDDLs: []string{},
				AddConstraintDDLs: []string{}, DropConstraintDDLs: []string{},
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
				CreateTableDDL: "",
				// Urutan setelah sorting: DROP dulu, baru ADD
				AlterColumnDDLs: []string{"ALTER TABLE \"test_alter\" DROP COLUMN old_col", "ALTER TABLE \"test_alter\" ADD COLUMN new_col INT"},
				AddIndexDDLs:    []string{}, DropIndexDDLs: []string{}, AddConstraintDDLs: []string{}, DropConstraintDDLs: []string{},
			},
			expectedError: false,
		},
		{
			name: "Mixed DDLs with Sorting Applied",
			// Gunakan syncer MySQL karena input DDL ada yang gaya MySQL (MODIFY COLUMN)
			// dan logika sorting bisa dipengaruhi oleh dialek di extractConstraintNameAndTypeForDrop
			syncer: newTestSchemaSyncer("mysql", logger),
			inputDDLs: &SchemaExecutionResult{
				TableDDL: "ALTER TABLE \"mixed_table\" MODIFY COLUMN status VARCHAR(20);",
				IndexDDLs: []string{
					"CREATE UNIQUE INDEX idx_mixed_unique ON \"mixed_table\" (email);",
					"DROP INDEX idx_mixed_old_b;",
					"DROP INDEX idx_mixed_old_a;",
				},
				ConstraintDDLs: []string{
					"ALTER TABLE \"mixed_table\" ADD CONSTRAINT uq_mixed_name UNIQUE (name);",
					"ALTER TABLE \"mixed_table\" DROP CONSTRAINT fk_mixed_obsolete;", // FK
					"ALTER TABLE \"mixed_table\" ADD CONSTRAINT fk_mixed_new FOREIGN KEY (user_id) REFERENCES users(id);",
					"ALTER TABLE \"mixed_table\" DROP CONSTRAINT pk_to_drop;", // PK
				},
			},
			inputTable: "mixed_table",
			expectedParsed: &categorizedDDLs{
				CreateTableDDL:  "",
				AlterColumnDDLs: []string{"ALTER TABLE \"mixed_table\" MODIFY COLUMN status VARCHAR(20)"},
				AddIndexDDLs:    []string{"CREATE UNIQUE INDEX idx_mixed_unique ON \"mixed_table\" (email)"},
				DropIndexDDLs:   []string{"DROP INDEX idx_mixed_old_a", "DROP INDEX idx_mixed_old_b"},
				AddConstraintDDLs: []string{ // PK (jika ada ADD PK), UQ, CHECK, FK
					"ALTER TABLE \"mixed_table\" ADD CONSTRAINT uq_mixed_name UNIQUE (name)",
					"ALTER TABLE \"mixed_table\" ADD CONSTRAINT fk_mixed_new FOREIGN KEY (user_id) REFERENCES users(id)",
				},
				DropConstraintDDLs: []string{ // FK, UQ, CHK, PK (lalu nama)
					"ALTER TABLE \"mixed_table\" DROP CONSTRAINT fk_mixed_obsolete",
					"ALTER TABLE \"mixed_table\" DROP CONSTRAINT pk_to_drop",
				},
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
				CreateTableDDL: "",
				// Urutan setelah sorting: ALTER dulu baru UNKNOWN
				AlterColumnDDLs: []string{"ALTER TABLE tbl ADD col1 INT", "UNKNOWN DDL STATEMENT"},
				AddIndexDDLs:    []string{}, DropIndexDDLs: []string{}, AddConstraintDDLs: []string{}, DropConstraintDDLs: []string{},
			},
			expectedError: false,
		},
		{
			name:   "Create Table Followed by Alter in TableDDL",
			syncer: syncerPostgres,
			inputDDLs: &SchemaExecutionResult{
				TableDDL: "CREATE TABLE create_then_alter (id INT); ALTER TABLE create_then_alter ADD COLUMN name VARCHAR(10);",
			},
			inputTable: "create_then_alter",
			expectedParsed: &categorizedDDLs{
				CreateTableDDL:  "CREATE TABLE create_then_alter (id INT)",
				AlterColumnDDLs: []string{"ALTER TABLE create_then_alter ADD COLUMN name VARCHAR(10)"},
				AddIndexDDLs:    []string{}, DropIndexDDLs: []string{}, AddConstraintDDLs: []string{}, DropConstraintDDLs: []string{},
			},
			expectedError: false,
		},
		{
			name:   "Empty statements and only semicolons",
			syncer: syncerPostgres,
			inputDDLs: &SchemaExecutionResult{
				TableDDL:  "; ; ALTER TABLE t1 ADD c1 INT; ;",
				IndexDDLs: []string{"", " ; ", "DROP INDEX idx1;", "  "},
			},
			inputTable: "t1",
			expectedParsed: &categorizedDDLs{
				CreateTableDDL:  "",
				AlterColumnDDLs: []string{"ALTER TABLE t1 ADD c1 INT"},
				DropIndexDDLs:   []string{"DROP INDEX idx1"},
				AddIndexDDLs:    []string{}, AddConstraintDDLs: []string{}, DropConstraintDDLs: []string{},
			},
			expectedError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			currentSyncer := tc.syncer
			if currentSyncer == nil { // Jika tc.syncer tidak diset, gunakan default
				currentSyncer = newTestSchemaSyncer("postgres", logger.Named(tc.name))
			} else {
				// Jika tc.syncer diset, pastikan logger-nya juga di-scope untuk test case ini
				currentSyncer.logger = logger.Named(tc.name)
			}

			actualParsed, err := currentSyncer.parseAndCategorizeDDLs(tc.inputDDLs, tc.inputTable)

			if tc.expectedError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedParsed.CreateTableDDL, actualParsed.CreateTableDDL, "CreateTableDDL mismatch for %s", tc.name)
				assert.Equal(t, tc.expectedParsed.AlterColumnDDLs, actualParsed.AlterColumnDDLs, "AlterColumnDDLs mismatch for %s", tc.name)
				assert.Equal(t, tc.expectedParsed.AddIndexDDLs, actualParsed.AddIndexDDLs, "AddIndexDDLs mismatch for %s", tc.name)
				assert.Equal(t, tc.expectedParsed.DropIndexDDLs, actualParsed.DropIndexDDLs, "DropIndexDDLs mismatch for %s", tc.name)
				assert.Equal(t, tc.expectedParsed.AddConstraintDDLs, actualParsed.AddConstraintDDLs, "AddConstraintDDLs mismatch for %s", tc.name)
				assert.Equal(t, tc.expectedParsed.DropConstraintDDLs, actualParsed.DropConstraintDDLs, "DropConstraintDDLs mismatch for %s", tc.name)
			}
		})
	}
}

func TestShouldIgnoreDDLError(t *testing.T) {
	logger := zaptest.NewLogger(t) // Logger untuk debugging test

	// DDL examples for context, diakhiri titik koma agar sesuai input dari executeDDLPhase
	ddlCreateTable := "CREATE TABLE my_table (id INT);"
	ddlCreateIndex := "CREATE INDEX my_index ON my_table(id);"
	ddlAddConstraintFK := "ALTER TABLE child ADD CONSTRAINT fk_child_parent FOREIGN KEY (parent_id) REFERENCES parent(id);"
	ddlAddConstraintUQ := "ALTER TABLE my_table ADD CONSTRAINT uq_col UNIQUE (my_col);"
	ddlAddColumn := "ALTER TABLE my_table ADD COLUMN new_col INT;"

	ddlDropTable := "DROP TABLE my_table;"
	ddlDropIndex := "DROP INDEX my_index;"
	ddlDropConstraint := "ALTER TABLE my_table DROP CONSTRAINT my_constraint;"
	ddlDropType := "DROP TYPE my_type;"
	ddlDropSchema := "DROP SCHEMA my_schema;"

	genericDDL := "SOME GENERIC DDL;"

	testCases := []struct {
		name     string
		dialect  string
		err      error
		ddl      string
		expected bool
	}{
		// --- PostgreSQL ---
		{"PG: Duplicate Table (CREATE)", "postgres", errors.New(`ERROR: relation "my_table" already exists (sqlstate 42P07)`), ddlCreateTable, true},
		{"PG: Duplicate Index (CREATE)", "postgres", errors.New(`ERROR: relation "my_index" already exists (sqlstate 42P07)`), ddlCreateIndex, true},
		{"PG: Duplicate Constraint (ADD)", "postgres", errors.New(`ERROR: constraint "my_constraint" for relation "my_table" already exists (sqlstate 42710)`), ddlAddConstraintUQ, true},
		{"PG: Index Does Not Exist (DROP)", "postgres", errors.New(`ERROR: index "non_existent_index" does not exist (sqlstate 42704)`), "DROP INDEX non_existent_index;", true},
		{"PG: Table Does Not Exist (DROP)", "postgres", errors.New(`ERROR: table "non_existent_table" does not exist (sqlstate 42P01)`), "DROP TABLE non_existent_table;", true},
		{"PG: Type Does Not Exist (DROP)", "postgres", errors.New(`ERROR: type "non_existent_type" does not exist (sqlstate 42P01)`), ddlDropType, true},
		{"PG: Schema Does Not Exist (DROP)", "postgres", errors.New(`ERROR: schema "non_existent_schema" does not exist (sqlstate 42P01)`), ddlDropSchema, true},
		{"PG: Table Referenced by FK Does Not Exist (ADD FK) - NOT IGNORED", "postgres", errors.New(`ERROR: relation "parent_table" does not exist (sqlstate 42P01)`), ddlAddConstraintFK, false},
		{"PG: Constraint Does Not Exist (DROP)", "postgres", errors.New(`ERROR: constraint "non_existent_constraint" on table "some_table" does not exist (sqlstate 42704)`), "ALTER TABLE some_table DROP CONSTRAINT non_existent_constraint;", true},
		{"PG: Type Already Exists (CREATE)", "postgres", errors.New(`ERROR: type "my_enum" already exists (sqlstate 42P07)`), "CREATE TYPE my_enum AS ENUM ('a');", true},
		{"PG: Schema Already Exists (CREATE)", "postgres", errors.New(`ERROR: schema "my_schema" already exists (sqlstate 42P07)`), "CREATE SCHEMA my_schema;", true},
		{"PG: Real Syntax Error - NOT IGNORED", "postgres", errors.New("ERROR: syntax error at or near \"INVALID\" (sqlstate 42601)"), genericDDL, false},
		{"PG: Unique Violation on Create Index - NOT IGNORED", "postgres", errors.New("ERROR: could not create unique index \"my_unique_idx\" (sqlstate 23505) DETAIL: Key (column)=(value) is duplicated."), ddlCreateIndex, false},
		{"PG: Nil Error", "postgres", nil, genericDDL, false},

		// --- MySQL ---
		{"MySQL: Table Already Exists (CREATE, Code 1050)", "mysql", errors.New("Error 1050 (42S01): Table 'my_table' already exists"), ddlCreateTable, true},
		{"MySQL: Duplicate Key Name (Index, Code 1061)", "mysql", errors.New("Error 1061 (42000): Duplicate key name 'idx_name'"), ddlCreateIndex, true},
		{"MySQL: Duplicate Column Name (ADD, Code 1060)", "mysql", errors.New("Error 1060 (42S21): Duplicate column name 'col1'"), ddlAddColumn, true},
		{"MySQL: FK Already Exists (ADD, Code 1826)", "mysql", errors.New("Error 1826 (HY000): Foreign key constraint for key 'fk_name' already exists."), ddlAddConstraintFK, true},
		{"MySQL: FK Already Exists (ADD, Message)", "mysql", errors.New("Error HY000: Foreign key constraint 'my_fk_constraint_name' already exists."), ddlAddConstraintFK, true},
		{"MySQL: Check Constraint Already Exists (ADD, Code 3822)", "mysql", errors.New("Error 3822 (HY000): Check constraint 'chk_name' already exists."), "ALTER TABLE tbl ADD CONSTRAINT chk_name CHECK (col > 0);", true},
		{"MySQL: Unique Constraint Already Exists (ADD, Message)", "mysql", errors.New("Error HY000: Unique constraint 'uq_name' already exists."), ddlAddConstraintUQ, true},
		{"MySQL: PK Constraint Already Exists (ADD, Message)", "mysql", errors.New("Error HY000: Primary key constraint 'pk_name' already exists."), "ALTER TABLE tbl ADD CONSTRAINT pk_name PRIMARY KEY(id);", true},
		{"MySQL: Unknown Table (DROP, Code 1051)", "mysql", errors.New("Error 1051 (42S02): Unknown table 'my_db.my_table'"), ddlDropTable, true},
		{"MySQL: Can't Drop Index (Not Exists, Code 1091)", "mysql", errors.New("Error 1091 (42000): Can't DROP INDEX `idx_does_not_exist`; check that it exists"), "DROP INDEX `idx_does_not_exist` ON `some_table`;", true},
		{"MySQL: Can't Drop FK (Not Exists, Code 1091)", "mysql", errors.New("Error 1091 (42000): Can't DROP FOREIGN KEY `fk_non_existent`; check that it exists"), "ALTER TABLE tbl DROP FOREIGN KEY `fk_non_existent`;", true},
		{"MySQL: Can't Drop Constraint (Not Exists, Code 1091)", "mysql", errors.New("Error 1091 (42000): Can't DROP CONSTRAINT `uq_non_existent`; check that it exists"), "ALTER TABLE tbl DROP CONSTRAINT `uq_non_existent`;", true},
		{"MySQL: Can't Drop Check (Not Exists, Code 1091)", "mysql", errors.New("Error 1091 (42000): Can't DROP CHECK `chk_non_existent`; check that it exists"), "ALTER TABLE tbl DROP CHECK `chk_non_existent`;", true},
		{"MySQL: Can't Drop PK (Not Exists, Code 1091)", "mysql", errors.New("Error 1091 (42000): Can't DROP PRIMARY KEY; check that it exists"), "ALTER TABLE tbl DROP PRIMARY KEY;", true},
		{"MySQL: Index Already Exists (Message)", "mysql", errors.New("Index `idx_some_name` already exists on table `my_table`"), ddlCreateIndex, true},
		{"MySQL: Constraint Does Not Exist (DROP, Message)", "mysql", errors.New("CONSTRAINT `my_cons` does not exist"), ddlDropConstraint, true},
		{"MySQL: Real Syntax Error - NOT IGNORED", "mysql", errors.New("Error 1064 (42000): You have an error in your SQL syntax..."), genericDDL, false},
		{"MySQL: Unknown Table (CREATE FK to non-existent table) - NOT IGNORED", "mysql", errors.New("Error 1005 (HY000): Can't create table 'db'.'child' (errno: 150 \"Foreign key constraint is incorrectly formed\")"), ddlAddConstraintFK, false},
		{"MySQL: Nil Error", "mysql", nil, genericDDL, false},

		// --- SQLite ---
		{"SQLite: Index Already Exists (CREATE)", "sqlite", errors.New("index idx_test already exists"), ddlCreateIndex, true},
		{"SQLite: Table Already Exists (CREATE)", "sqlite", errors.New("table my_table already exists"), ddlCreateTable, true},
		{"SQLite: Column Already Exists (ADD)", "sqlite", errors.New("duplicate column name: new_col"), ddlAddColumn, true},
		{"SQLite: No Such Index (DROP)", "sqlite", errors.New("no such index: idx_gone"), ddlDropIndex, true},
		{"SQLite: No Such Table (DROP)", "sqlite", errors.New("no such table: table_gone"), ddlDropTable, true},
		{"SQLite: Unique Constraint Failed on Create Index - NOT IGNORED", "sqlite", errors.New("UNIQUE constraint failed: my_table.my_col"), ddlCreateIndex, false},
		{"SQLite: Constraint Already Exists (ADD)", "sqlite", errors.New("constraint uq_col already exists"), ddlAddConstraintUQ, true},
		{"SQLite: Real Syntax Error - NOT IGNORED", "sqlite", errors.New("near \"SLECT\": syntax error"), genericDDL, false},
		{"SQLite: Nil Error", "sqlite", nil, genericDDL, false},

		// --- Unknown Dialect ---
		{"Unknown Dialect: Real Error - NOT IGNORED", "oracle", errors.New("ORA-00942: table or view does not exist"), genericDDL, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			currentSyncer := newTestSchemaSyncer(tc.dialect, logger.Named(tc.name))
			actual := currentSyncer.shouldIgnoreDDLError(tc.err, tc.ddl)
			assert.Equal(t, tc.expected, actual, "Test: %s\nError: %v\nDDL: %s", tc.name, tc.err, tc.ddl)
		})
	}
}

func TestSortConstraintsForDrop(t *testing.T) {
	logger := zaptest.NewLogger(t)
	syncer := newTestSchemaSyncer("mysql", logger)

	input := []string{
		"ALTER TABLE t1 DROP CONSTRAINT uq_1;",
		"ALTER TABLE t1 DROP CONSTRAINT fk_1;",
		"ALTER TABLE t1 DROP CONSTRAINT pk_constraint_name;",
		"ALTER TABLE t1 DROP CONSTRAINT fk_2;",
		"ALTER TABLE t1 DROP CONSTRAINT chk_1;",
		"ALTER TABLE t1 DROP FOREIGN KEY mysql_fk_direct;",
		"ALTER TABLE t1 DROP PRIMARY KEY;",
		"ALTER TABLE t1 DROP CHECK mysql_check_direct;",
		"DROP CONSTRAINT unknown_type_cons;", // DDL ini akan menghasilkan nama 'DROP CONSTRAINT unknown_type_cons'
		"ALTER TABLE t1 DROP CONSTRAINT some_other_constraint;",
	}

	// Ekspektasi setelah membersihkan titik koma dari input dan mengurutkan
	// Sesuai logika: FKs, UNIQUE, CHECKs, PKs, UNKNOWN_FROM_NAME, UNKNOWN_DDL_STRUCTURE
	// Dalam setiap grup, diurutkan berdasarkan nama constraint yang diekstrak.
	expected := []string{
		"ALTER TABLE t1 DROP CONSTRAINT fk_1",                  // Nama: fk_1
		"ALTER TABLE t1 DROP CONSTRAINT fk_2",                  // Nama: fk_2
		"ALTER TABLE t1 DROP FOREIGN KEY mysql_fk_direct",      // Nama: mysql_fk_direct
		"ALTER TABLE t1 DROP CONSTRAINT uq_1",                  // Nama: uq_1
		"ALTER TABLE t1 DROP CONSTRAINT chk_1",                 // Nama: chk_1
		"ALTER TABLE t1 DROP CHECK mysql_check_direct",         // Nama: mysql_check_direct
		"ALTER TABLE t1 DROP CONSTRAINT pk_constraint_name",    // Nama: pk_constraint_name
		"ALTER TABLE t1 DROP PRIMARY KEY",                      // Nama: PRIMARY_KEY_IMPLICIT_DROP_NAME_FOR_SORT
		"ALTER TABLE t1 DROP CONSTRAINT some_other_constraint", // Nama: some_other_constraint
		"DROP CONSTRAINT unknown_type_cons",                    // Nama: DROP CONSTRAINT unknown_type_cons
	}

	// Bersihkan input DDLs dari titik koma untuk konsistensi dengan hasil cleanSingleDDL
	cleanedInput := make([]string, len(input))
	for i, ddl := range input {
		cleanedInput[i] = cleanSingleDDL(ddl)
	}

	actual := syncer.sortConstraintsForDrop(cleanedInput)
	assert.Equal(t, expected, actual, "TestSortConstraintsForDrop failed")
}

func TestSortConstraintsForAdd(t *testing.T) {
	logger := zaptest.NewLogger(t)
	syncer := newTestSchemaSyncer("postgres", logger)

	input := []string{
		"ALTER TABLE t1 ADD CONSTRAINT fk_1 FOREIGN KEY (col) REFERENCES t2(id);",
		"ALTER TABLE t1 ADD CONSTRAINT uq_1 UNIQUE (col_u);",
		"ALTER TABLE t1 ADD CONSTRAINT pk_1 PRIMARY KEY (id);",
		"ALTER TABLE t1 ADD CONSTRAINT chk_1 CHECK (col > 0);",
		"ALTER TABLE t1 ADD CONSTRAINT fk_2 FOREIGN KEY (col_b) REFERENCES t3(id);",
		"ADD CONSTRAINT unknown_add_cons TYPELESS;",
	}
	expected := []string{
		"ALTER TABLE t1 ADD CONSTRAINT pk_1 PRIMARY KEY (id)",
		"ALTER TABLE t1 ADD CONSTRAINT uq_1 UNIQUE (col_u)",
		"ALTER TABLE t1 ADD CONSTRAINT chk_1 CHECK (col > 0)",
		"ALTER TABLE t1 ADD CONSTRAINT fk_1 FOREIGN KEY (col) REFERENCES t2(id)",
		"ALTER TABLE t1 ADD CONSTRAINT fk_2 FOREIGN KEY (col_b) REFERENCES t3(id)",
		"ADD CONSTRAINT unknown_add_cons TYPELESS",
	}

	cleanedInput := make([]string, len(input))
	for i, ddl := range input {
		cleanedInput[i] = cleanSingleDDL(ddl)
	}
	actual := syncer.sortConstraintsForAdd(cleanedInput)
	assert.Equal(t, expected, actual)
}

func TestSortAlterColumns(t *testing.T) {
	logger := zaptest.NewLogger(t)
	syncer := newTestSchemaSyncer("postgres", logger)

	input := []string{
		"ALTER TABLE t1 ADD COLUMN new_col3 INT;",
		"ALTER TABLE t1 MODIFY COLUMN existing_col VARCHAR(50);",
		"ALTER TABLE t1 ALTER COLUMN pg_col TYPE VARCHAR(50);",
		"ALTER TABLE t1 DROP COLUMN old_col1;",
		"ALTER TABLE t1 ADD COLUMN new_col1 TEXT;",
		"ALTER TABLE t1 ALTER COLUMN type_change TYPE BIGINT;",
		"ALTER TABLE t1 DROP COLUMN old_col2;",
		"UNKNOWN DDL STATEMENT;", // Placeholder
	}
	expected := []string{
		"ALTER TABLE t1 DROP COLUMN old_col1",
		"ALTER TABLE t1 DROP COLUMN old_col2",
		"ALTER TABLE t1 ALTER COLUMN pg_col TYPE VARCHAR(50)",
		"ALTER TABLE t1 ALTER COLUMN type_change TYPE BIGINT",
		"ALTER TABLE t1 MODIFY COLUMN existing_col VARCHAR(50)",
		"ALTER TABLE t1 ADD COLUMN new_col1 TEXT",
		"ALTER TABLE t1 ADD COLUMN new_col3 INT",
		"UNKNOWN DDL STATEMENT",
	}
	cleanedInput := make([]string, len(input))
	for i, ddl := range input {
		cleanedInput[i] = cleanSingleDDL(ddl)
	}
	actual := syncer.sortAlterColumns(cleanedInput)
	assert.Equal(t, expected, actual)
}

func TestSortIndexes(t *testing.T) {
	logger := zaptest.NewLogger(t)
	syncer := newTestSchemaSyncer("postgres", logger)

	dropInput := []string{"DROP INDEX b;", "DROP INDEX a;", "DROP INDEX c;"}
	dropExpected := []string{"DROP INDEX a", "DROP INDEX b", "DROP INDEX c"}

	cleanedDropInput := make([]string, len(dropInput))
	for i, ddl := range dropInput {
		cleanedDropInput[i] = cleanSingleDDL(ddl)
	}
	actualDrop := syncer.sortDropIndexes(cleanedDropInput)
	assert.Equal(t, dropExpected, actualDrop)

	addInput := []string{"CREATE INDEX b_idx ON t(b);", "CREATE INDEX a_idx ON t(t.a);", "CREATE UNIQUE INDEX c_idx ON t(c);"}
	addExpected := []string{"CREATE INDEX a_idx ON t(t.a)", "CREATE INDEX b_idx ON t(b)", "CREATE UNIQUE INDEX c_idx ON t(c)"}

	cleanedAddInput := make([]string, len(addInput))
	for i, ddl := range addInput {
		cleanedAddInput[i] = cleanSingleDDL(ddl)
	}
	actualAdd := syncer.sortAddIndexes(cleanedAddInput)
	assert.Equal(t, addExpected, actualAdd)
}

func TestSplitPostgresFKsForDeferredExecution(t *testing.T) {
	logger := zaptest.NewLogger(t)
	syncer := newTestSchemaSyncer("postgres", logger)

	inputConstraints := []string{
		"ALTER TABLE orders ADD CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(id) DEFERRABLE INITIALLY DEFERRED;",
		"ALTER TABLE products ADD CONSTRAINT uq_sku UNIQUE (sku);",
		"ALTER TABLE order_items ADD CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES orders(id);",
		"ALTER TABLE payment_details ADD CONSTRAINT fk_payment_order FOREIGN KEY (order_id) REFERENCES orders(id) DEFERRABLE INITIALLY IMMEDIATE;",
		"ALTER TABLE users ADD CONSTRAINT chk_email CHECK (email LIKE '%@%');",
		"ALTER TABLE self_ref ADD CONSTRAINT fk_self FOREIGN KEY (parent_id) REFERENCES self_ref(id) DEFERRABLE INITIALLY DEFERRED;",
		"ALTER TABLE another_table ADD CONSTRAINT another_fk FOREIGN KEY (col) REFERENCES other(id) DEFERRABLE;",
	}

	expectedDeferredFKs := []string{
		"ALTER TABLE orders ADD CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(id) DEFERRABLE INITIALLY DEFERRED",
		"ALTER TABLE self_ref ADD CONSTRAINT fk_self FOREIGN KEY (parent_id) REFERENCES self_ref(id) DEFERRABLE INITIALLY DEFERRED",
	}
	expectedNonDeferredItems := []string{
		"ALTER TABLE products ADD CONSTRAINT uq_sku UNIQUE (sku)",
		"ALTER TABLE order_items ADD CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES orders(id)",
		"ALTER TABLE payment_details ADD CONSTRAINT fk_payment_order FOREIGN KEY (order_id) REFERENCES orders(id) DEFERRABLE INITIALLY IMMEDIATE",
		"ALTER TABLE users ADD CONSTRAINT chk_email CHECK (email LIKE '%@%')",
		"ALTER TABLE another_table ADD CONSTRAINT another_fk FOREIGN KEY (col) REFERENCES other(id) DEFERRABLE",
	}

	cleanedInputConstraints := make([]string, len(inputConstraints))
	for i, ddl := range inputConstraints {
		cleanedInputConstraints[i] = cleanSingleDDL(ddl)
	}

	actualDeferred, actualNonDeferred := syncer.splitPostgresFKsForDeferredExecution(cleanedInputConstraints)

	assert.ElementsMatch(t, expectedDeferredFKs, actualDeferred, "Deferred FKs mismatch")
	assert.ElementsMatch(t, expectedNonDeferredItems, actualNonDeferred, "Non-deferred items (FKs and other constraints) mismatch")
}
