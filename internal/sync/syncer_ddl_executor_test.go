package sync

import (
	"errors"
	"fmt"
	"testing"

	"github.com/arwahdevops/dbsync/internal/logger" // Untuk inisialisasi logger jika SchemaSyncer membutuhkannya
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type pgErrorWithSQLState struct {
	msg      string
	sqlState string
}

func (e *pgErrorWithSQLState) Error() string {
	return fmt.Sprintf("ERROR: %s (SQLSTATE %s)", e.msg, e.sqlState)
}

type mysqlError struct {
	msg string
	// number int // Error number bisa ditambahkan jika perlu untuk test yang lebih spesifik
}

func (e *mysqlError) Error() string {
	return e.msg
}

func TestShouldIgnoreDDLError(t *testing.T) {
	err := logger.Init(true, false) // Inisialisasi logger global dbsync
	if err != nil {
		t.Fatalf("Failed to init logger for test: %v", err)
	}

	syncerPostgres := &SchemaSyncer{dstDialect: "postgres", logger: zap.L()}
	syncerMySQL := &SchemaSyncer{dstDialect: "mysql", logger: zap.L()}
	syncerSQLite := &SchemaSyncer{dstDialect: "sqlite", logger: zap.L()}

	testCases := []struct {
		name        string
		syncer      *SchemaSyncer
		err         error
		expected    bool
		description string
	}{
		// PostgreSQL Cases
		{"PG: Ignorable - Relation Already Exists (SQLSTATE)", syncerPostgres, &pgErrorWithSQLState{msg: `relation "my_table" already exists`, sqlState: "42P07"}, true, "PG relation already exists by SQLSTATE"},
		{"PG: Ignorable - Index Already Exists (SQLSTATE)", syncerPostgres, &pgErrorWithSQLState{msg: `index "my_index" already exists`, sqlState: "42P07"}, true, "PG index already exists (shares 42P07 with table)"},
		{"PG: Ignorable - Constraint Already Exists (SQLSTATE)", syncerPostgres, &pgErrorWithSQLState{msg: `constraint "my_constraint" for relation "my_table" already exists`, sqlState: "42710"}, true, "PG constraint already exists by SQLSTATE"},
		{"PG: Ignorable - Table Does Not Exist (DROP IF EXISTS) (SQLSTATE)", syncerPostgres, &pgErrorWithSQLState{msg: `table "non_existent_table" does not exist`, sqlState: "42P01"}, true, "PG table does not exist (42P01) should be ignored for DROP IF EXISTS"},
		{"PG: Ignorable - Index Does Not Exist (DROP IF EXISTS) (SQLSTATE)", syncerPostgres, &pgErrorWithSQLState{msg: `index "non_existent_index" does not exist`, sqlState: "42704"}, true, "PG index does not exist (42704 UNDEFINED_OBJECT) should be ignored for DROP IF EXISTS"},
		{"PG: Ignorable - Relation Already Exists (Pesan)", syncerPostgres, errors.New(`ERROR: relation "users_pkey" already exists`), true, "PG relation already exists by message"},
		{"PG: Ignorable - Index Already Exists (Pesan)", syncerPostgres, errors.New(`ERROR: index "my_idx" already exists`), true, "PG index already exists by message"},
		{"PG: Not Ignorable - Generic Error", syncerPostgres, errors.New("ERROR: syntax error at or near \"INVALID\""), false, "PG generic syntax error"},
		{"PG: Not Ignorable - Unique Violation (Data Error)", syncerPostgres, &pgErrorWithSQLState{msg: `duplicate key value violates unique constraint "users_email_key"`, sqlState: "23505"}, false, "PG unique violation (data, not DDL ignore)"},

		// MySQL Cases
		{"MySQL: Ignorable - Table Already Exists", syncerMySQL, &mysqlError{msg: "Error 1050: Table 'my_table' already exists"}, true, "MySQL table already exists"},
		{"MySQL: Ignorable - Duplicate Key Name", syncerMySQL, &mysqlError{msg: "Error 1061: Duplicate key name 'my_index'"}, true, "MySQL duplicate key name for index"},
		{"MySQL: Ignorable - Cant DROP Index (single quote)", syncerMySQL, &mysqlError{msg: "Error 1091: Can't DROP INDEX 'my_index'; check that it exists"}, true, "MySQL cant drop index (single quote)"},
		{"MySQL: Ignorable - Cant DROP Index (backtick)", syncerMySQL, &mysqlError{msg: "Error 1091: Can't DROP INDEX `my_index`; check that it exists"}, true, "MySQL cant drop index (backtick)"},
		{"MySQL: Ignorable - FK Already Exists", syncerMySQL, &mysqlError{msg: "Error 1826: Foreign key constraint 'fk_name' already exists."}, true, "MySQL FK already exists"},
		{"MySQL: Ignorable - Duplicate Column", syncerMySQL, &mysqlError{msg: "Error 1060: Duplicate column name 'col'"}, true, "MySQL duplicate column (ignorable for ALTER TABLE ADD IF NOT EXISTS scenarios if we had such)"},
		{"MySQL: Not Ignorable - Generic Error", syncerMySQL, &mysqlError{msg: "Error 1064: You have an error in your SQL syntax"}, false, "MySQL generic syntax error"},

		// SQLite Cases
		{"SQLite: Ignorable - Table Already Exists", syncerSQLite, errors.New("table my_table already exists"), true, "SQLite table already exists"},
		{"SQLite: Ignorable - Index Already Exists", syncerSQLite, errors.New("index my_index already exists"), true, "SQLite index already exists"},
		{"SQLite: Ignorable - No Such Table (DROP)", syncerSQLite, errors.New("no such table: my_table"), true, "SQLite no such table (for DROP)"},
		{"SQLite: Not Ignorable - Generic Error", syncerSQLite, errors.New("near \"INVALID\": syntax error"), false, "SQLite generic syntax error"},
		// {"SQLite: Ignorable - Unique Constraint Failed", syncerSQLite, errors.New("UNIQUE constraint failed: my_table.my_column"), true, "SQLite unique constraint (could be data or DDL if ADDing constraint to existing data)"}, // Mungkin tidak selalu diabaikan

		// General
		{"Nil Error", syncerPostgres, nil, false, "Nil error should not be ignored"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := tc.syncer.shouldIgnoreDDLError(tc.err)
			assert.Equal(t, tc.expected, actual, tc.description)
		})
	}
}
