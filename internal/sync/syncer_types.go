package sync

import (
	"database/sql"
	"time"
)

// ColumnInfo menyimpan detail tentang sebuah kolom.
type ColumnInfo struct {
	Name                 string
	Type                 string         // Tipe asli dari database sumber
	MappedType           string         // Tipe yang sudah dimapping ke dialek tujuan
	IsNullable           bool
	IsPrimary            bool
	IsGenerated          bool
	DefaultValue         sql.NullString
	AutoIncrement        bool
	OrdinalPosition      int
	Length               sql.NullInt64  // Untuk tipe string/biner
	Precision            sql.NullInt64  // Untuk tipe numerik/waktu
	Scale                sql.NullInt64  // Untuk tipe numerik
	Collation            sql.NullString
	Comment              sql.NullString
	GenerationExpression sql.NullString // Ekspresi untuk kolom generated (jika bisa diambil)
}

// IndexInfo menyimpan detail tentang sebuah indeks.
type IndexInfo struct {
	Name      string
	Columns   []string // Kolom dalam urutan definisi indeks
	IsUnique  bool
	IsPrimary bool
	RawDef    string // Definisi mentah jika ada (berguna untuk PostgreSQL, misal USING GIN)
	// IndexType string // Opsional: BTREE, HASH, GIN, GIST (mungkin bisa diekstrak dari RawDef)
}

// ConstraintInfo menyimpan detail tentang sebuah constraint.
type ConstraintInfo struct {
	Name           string
	Type           string   // PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK
	Columns        []string // Kolom yang terlibat dalam constraint (dalam urutan definisi)
	Definition     string   // Untuk CHECK constraints: ekspresi check
	ForeignTable   string   // Untuk FOREIGN KEY: nama tabel referensi
	ForeignColumns []string // Untuk FOREIGN KEY: kolom di tabel referensi (dalam urutan definisi)
	OnDelete       string   // Untuk FOREIGN KEY: aksi ON DELETE (NO ACTION, CASCADE, SET NULL, SET DEFAULT, RESTRICT)
	OnUpdate       string   // Untuk FOREIGN KEY: aksi ON UPDATE
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
	AlterColumnDDLs    []string // Bisa berisi multiple ALTER statement untuk satu tabel (ADD, DROP, MODIFY)
	AddIndexDDLs       []string
	DropIndexDDLs      []string
	AddConstraintDDLs  []string
	DropConstraintDDLs []string
}

// SyncResult menyimpan hasil sinkronisasi untuk satu tabel.
type SyncResult struct {
	Table                    string
	SchemaSyncSkipped        bool // True jika schema sync dilewati karena strategi 'none' atau tabel sistem
	SchemaAnalysisError      error
	SchemaExecutionError     error // Bisa jadi multierr jika ada beberapa DDL yang gagal dan continueOnError
	DataError                error
	ConstraintExecutionError error // Bisa jadi multierr
	SkipReason               string
	RowsSynced               int64
	Batches                  int
	Duration                 time.Duration
	Skipped                  bool // True jika keseluruhan pemrosesan tabel ini dilewati karena error fatal sebelumnya
}
