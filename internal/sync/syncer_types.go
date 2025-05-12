// internal/sync/syncer_types.go
package sync

import (
	"context" // Diperlukan untuk processTableInput
	"database/sql"
	"time"

	// Impor yang diperlukan untuk processTableInput
	"github.com/arwahdevops/dbsync/internal/config"
	"github.com/arwahdevops/dbsync/internal/db"
	"github.com/arwahdevops/dbsync/internal/metrics"
	"go.uber.org/zap" // Diperlukan untuk processTableInput
)

// ColumnInfo menyimpan detail tentang sebuah kolom.
type ColumnInfo struct {
	Name                 string
	Type                 string
	MappedType           string
	IsNullable           bool
	IsPrimary            bool
	IsGenerated          bool
	DefaultValue         sql.NullString
	AutoIncrement        bool
	OrdinalPosition      int
	Length               sql.NullInt64
	Precision            sql.NullInt64
	Scale                sql.NullInt64
	Collation            sql.NullString
	Comment              sql.NullString
	GenerationExpression sql.NullString
}

// IndexInfo menyimpan detail tentang sebuah indeks.
type IndexInfo struct {
	Name      string
	Columns   []string
	IsUnique  bool
	IsPrimary bool
	RawDef    string
}

// ConstraintInfo menyimpan detail tentang sebuah constraint.
type ConstraintInfo struct {
	Name           string
	Type           string
	Columns        []string
	Definition     string
	ForeignTable   string
	ForeignColumns []string
	OnDelete       string
	OnUpdate       string
}

// schemaDetails adalah struct internal untuk mengelompokkan hasil fetch skema.
type schemaDetails struct {
	Columns     []ColumnInfo
	Indexes     []IndexInfo
	Constraints []ConstraintInfo
}

// categorizedDDLs adalah struct untuk menampung DDL yang sudah dikategorikan untuk eksekusi.
type categorizedDDLs struct {
	CreateTableDDL     string
	AlterColumnDDLs    []string
	AddIndexDDLs       []string
	DropIndexDDLs      []string
	AddConstraintDDLs  []string
	DropConstraintDDLs []string
}

// SyncResult menyimpan hasil sinkronisasi untuk satu tabel.
type SyncResult struct {
	Table                    string
	SchemaSyncSkipped        bool
	SchemaAnalysisError      error
	SchemaExecutionError     error
	DataError                error
	ConstraintExecutionError error
	SkipReason               string
	RowsSynced               int64
	Batches                  int
	Duration                 time.Duration
	Skipped                  bool
}

// processTableInput adalah argumen untuk fungsi pemrosesan tabel.
// Didefinisikan di sini untuk sentralisasi.
type processTableInput struct {
	ctx          context.Context
	tableName    string
	cfg          *config.Config
	logger       *zap.Logger // Logger utama (akan di-scope lebih lanjut)
	metrics      *metrics.Store
	schemaSyncer SchemaSyncerInterface
	srcConn      *db.Connector // Koneksi sumber
	dstConn      *db.Connector // Koneksi tujuan
}

// processTableResult adalah alias untuk SyncResult, digunakan sebagai tipe return dari goroutine.
// Didefinisikan di sini untuk sentralisasi.
type processTableResult SyncResult
