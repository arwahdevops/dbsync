package sync

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// fetchSchemaDetails mengambil semua detail skema (kolom, indeks, constraint) untuk sebuah tabel.
// Parameter `dbType` bisa "source" atau "destination" untuk logging.
func (s *SchemaSyncer) fetchSchemaDetails(ctx context.Context, db *gorm.DB, dialect string, table string, dbType string) (*schemaDetails, bool, error) {
	log := s.logger.With(zap.String("table", table), zap.String("dialect", dialect), zap.String("database_role", dbType))
	details := &schemaDetails{}
	var exists bool

	// GORM Migrator HasTable mungkin tidak selalu akurat atau berperilaku sama di semua dialek
	// untuk tabel yang sangat baru dibuat atau dalam transaksi.
	// Mengandalkan hasil dari getColumns untuk menentukan keberadaan tabel lebih aman.
	var err error
	details.Columns, err = s.getColumns(ctx, db, dialect, table)
	if err != nil {
		// Jika error adalah karena tabel tidak ada (berdasarkan pesan error yang umum),
		// itu bukan error fatal untuk fetchSchemaDetails, anggap saja tabel tidak ada.
		if strings.Contains(strings.ToLower(err.Error()), "table") &&
			(strings.Contains(strings.ToLower(err.Error()), "not found") || strings.Contains(strings.ToLower(err.Error()), "does not exist")) {
			log.Debug("Table not found during column fetch.", zap.Error(err))
			return details, false, nil
		}
		return nil, false, fmt.Errorf("failed to get columns for %s table '%s': %w", dbType, table, err)
	}

	if len(details.Columns) > 0 {
		exists = true
	} else {
		// Jika getColumns mengembalikan slice kosong tanpa error, bisa berarti tabel tidak ada
		// atau tabel ada tapi tidak punya kolom (kasus aneh).
		log.Debug("No columns fetched, assuming table does not exist or is empty.")
		return details, false, nil
	}

	details.Indexes, err = s.getIndexes(ctx, db, dialect, table)
	if err != nil {
		log.Error("Failed to get indexes, proceeding with empty list.", zap.Error(err))
		details.Indexes = []IndexInfo{} // Jangan return error, lanjutkan dengan list kosong
	}

	details.Constraints, err = s.getConstraints(ctx, db, dialect, table)
	if err != nil {
		log.Error("Failed to get constraints, proceeding with empty list.", zap.Error(err))
		details.Constraints = []ConstraintInfo{} // Jangan return error, lanjutkan dengan list kosong
	}

	return details, exists, nil
}

// getColumns adalah dispatcher untuk mengambil informasi kolom.
func (s *SchemaSyncer) getColumns(ctx context.Context, db *gorm.DB, dialect string, table string) ([]ColumnInfo, error) {
	log := s.logger.With(zap.String("dialect", dialect), zap.String("table", table))
	log.Debug("Dispatching to fetch table columns.")
	var cols []ColumnInfo
	var err error

	switch dialect {
	case "mysql":
		cols, err = s.getMySQLColumns(ctx, db, table)
	case "postgres":
		cols, err = s.getPostgresColumns(ctx, db, table)
	case "sqlite":
		cols, err = s.getSQLiteColumns(ctx, db, table)
	default:
		return nil, fmt.Errorf("getColumns: unsupported dialect: %s for table '%s'", dialect, table)
	}
	if err != nil {
		// Error sudah ditangani di fungsi spesifik dialek jika tabel tidak ada.
		// Di sini kita hanya meneruskan error yang lebih umum.
		return nil, err
	}
	return cols, nil
}

// getIndexes adalah dispatcher untuk mengambil informasi indeks.
func (s *SchemaSyncer) getIndexes(ctx context.Context, db *gorm.DB, dialect, table string) ([]IndexInfo, error) {
	log := s.logger.With(zap.String("dialect", dialect), zap.String("table", table))
	log.Debug("Dispatching to fetch table indexes.")
	switch dialect {
	case "mysql":
		return s.getMySQLIndexes(ctx, db, table)
	case "postgres":
		return s.getPostgresIndexes(ctx, db, table)
	case "sqlite":
		return s.getSQLiteIndexes(ctx, db, table)
	default:
		log.Warn("Index fetching not implemented for dialect, returning empty.", zap.String("dialect", dialect))
		return []IndexInfo{}, nil // Kembalikan slice kosong, bukan error
	}
}

// getConstraints adalah dispatcher untuk mengambil informasi constraint.
func (s *SchemaSyncer) getConstraints(ctx context.Context, db *gorm.DB, dialect, table string) ([]ConstraintInfo, error) {
	log := s.logger.With(zap.String("dialect", dialect), zap.String("table", table))
	log.Debug("Dispatching to fetch table constraints.")
	switch dialect {
	case "mysql":
		return s.getMySQLConstraints(ctx, db, table)
	case "postgres":
		return s.getPostgresConstraints(ctx, db, table)
	case "sqlite":
		return s.getSQLiteConstraints(ctx, db, table)
	default:
		log.Warn("Constraint fetching not implemented for dialect, returning empty.", zap.String("dialect", dialect))
		return []ConstraintInfo{}, nil // Kembalikan slice kosong, bukan error
	}
}
