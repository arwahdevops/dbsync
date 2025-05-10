package sync

import "database/sql"

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
	Length               sql.NullInt64
	Precision            sql.NullInt64
	Scale                sql.NullInt64
	Collation            sql.NullString
	Comment              sql.NullString
	// GenerationExpression sql.NullString // Pertimbangkan untuk menambahkan ini jika detail ekspresi diperlukan untuk perbandingan atau DDL
}

// IndexInfo menyimpan detail tentang sebuah indeks.
type IndexInfo struct {
	Name      string
	Columns   []string
	IsUnique  bool
	IsPrimary bool
	RawDef    string // Definisi mentah jika ada (berguna untuk PostgreSQL)
	// IndexType string // Pertimbangkan menambahkan jika bisa diekstrak secara konsisten
}

// ConstraintInfo menyimpan detail tentang sebuah constraint.
type ConstraintInfo struct {
	Name           string
	Type           string
	Columns        []string
	Definition     string // Untuk CHECK constraints
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
