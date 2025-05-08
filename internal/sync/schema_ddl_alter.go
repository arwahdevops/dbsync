// internal/sync/schema_ddl_alter.go
package sync

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
	"github.com/arwahdevops/dbsync/internal/utils"
)

// generateModifyColumnDDLs generates one or more ALTER statements for column modification.
// Ini adalah dispatcher ke fungsi spesifik dialek.
// (Implementasi ini sudah kita review sebelumnya dan tampak baik)
func (s *SchemaSyncer) generateModifyColumnDDLs(table string, src, dst ColumnInfo, log *zap.Logger) []string {
	// Dapatkan perbedaan dulu (menggunakan fungsi dari compare_columns.go)
	diffs := s.getColumnModifications(src, dst, log) // `s` sudah menjadi receiver di sini

	if len(diffs) == 0 {
		return []string{}
	}

	// Gunakan logger dari parameter fungsi, yang sudah di-scope
	log.Info("Generating ALTER COLUMN DDL(s) due to detected differences.",
		zap.String("dst_dialect", s.dstDialect),
		zap.Strings("modifications_detected", diffs),
	)

	switch s.dstDialect {
	case "mysql":
		return s.generateMySQLModifyColumnDDLs(table, src, dst, diffs, log)
	case "postgres":
		return s.generatePostgresModifyColumnDDLs(table, src, dst, diffs, log)
	case "sqlite":
		return s.generateSQLiteModifyColumnDDLs(table, src, dst, diffs, log)
	default:
		log.Error("Unsupported destination dialect for ALTER COLUMN modifications", zap.String("dialect", s.dstDialect))
		return []string{}
	}
}

// --- Fungsi DDL ALTER lainnya (ADD COLUMN, DROP COLUMN, DROP INDEX, DROP CONSTRAINT) ---

// generateAddColumnDDL generates ALTER TABLE ... ADD COLUMN ...
func (s *SchemaSyncer) generateAddColumnDDL(table string, col ColumnInfo) (string, error) {
	// Logger sudah di-scope oleh pemanggil (generateAlterDDLs)
	log := s.logger.With(zap.String("table", table), zap.String("column", col.Name), zap.String("action", "ADD COLUMN"))

	// MappedType seharusnya sudah diisi oleh pemanggil (populateMappedTypesForSourceColumns)
	if col.MappedType == "" && !col.IsGenerated {
		errMsg := fmt.Sprintf("cannot generate ADD COLUMN DDL for non-generated column '%s': MappedType is missing", col.Name)
		log.Error(errMsg)
		return "", fmt.Errorf(errMsg)
	}
	if col.IsGenerated {
		// Perlu penanganan spesifik dialek untuk sintaks GENERATED AS
		log.Warn("Generating ADD COLUMN DDL for a generated column. Dialect-specific syntax for GENERATED AS expression is complex and might require manual adjustment or is not fully supported by dbsync.",
			zap.String("target_type", col.MappedType))
		// mapColumnDefinition akan mencoba membuat definisi dasar, tapi mungkin perlu disesuaikan.
	}

	// Dapatkan definisi kolom lengkap dari mapColumnDefinition
	// (mapColumnDefinition ada di schema_ddl_create.go atau file ini jika dipindah)
	colDef, err := s.mapColumnDefinition(col)
	if err != nil {
		errMsg := fmt.Sprintf("failed to map column definition for ADD COLUMN '%s': %v", col.Name, err)
		log.Error(errMsg)
		return "", fmt.Errorf(errMsg)
	}

	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)

	// Sintaks ADD COLUMN umumnya sama untuk dialek yang didukung
	switch s.dstDialect {
	case "mysql", "postgres", "sqlite":
		// SQLite mendukung ADD COLUMN.
		// IF NOT EXISTS tidak standar untuk ADD COLUMN.
		ddl := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", quotedTable, colDef)
		log.Debug("Generated ADD COLUMN DDL.", zap.String("ddl", ddl))
		return ddl, nil
	default:
		errMsg := fmt.Sprintf("unsupported destination dialect '%s' for ADD COLUMN", s.dstDialect)
		log.Error(errMsg)
		return "", fmt.Errorf(errMsg)
	}
}

// generateDropColumnDDL generates ALTER TABLE ... DROP COLUMN ...
func (s *SchemaSyncer) generateDropColumnDDL(table string, col ColumnInfo) (string, error) {
	log := s.logger.With(zap.String("table", table), zap.String("column", col.Name), zap.String("action", "DROP COLUMN"))
	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)
	quotedColName := utils.QuoteIdentifier(col.Name, s.dstDialect)

	var ddl string
	switch s.dstDialect {
	case "mysql":
		// MySQL tidak mendukung IF EXISTS untuk DROP COLUMN
		ddl = fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", quotedTable, quotedColName)
	case "postgres":
		// PostgreSQL mendukung IF EXISTS
		ddl = fmt.Sprintf("ALTER TABLE %s DROP COLUMN IF EXISTS %s;", quotedTable, quotedColName)
	case "sqlite":
		// SQLite mendukung DROP COLUMN sejak 3.35.0. Tidak mendukung IF EXISTS.
		log.Warn("DROP COLUMN support in SQLite depends on version (>= 3.35.0). DDL will be generated without IF EXISTS.",
			zap.String("table", table), zap.String("column", col.Name))
		ddl = fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", quotedTable, quotedColName)
	default:
		errMsg := fmt.Sprintf("unsupported destination dialect '%s' for DROP COLUMN", s.dstDialect)
		log.Error(errMsg)
		return "", fmt.Errorf(errMsg)
	}
	log.Debug("Generated DROP COLUMN DDL.", zap.String("ddl", ddl))
	return ddl, nil
}

// generateDropIndexDDL generates DROP INDEX statement.
func (s *SchemaSyncer) generateDropIndexDDL(table string, idx IndexInfo) (string, error) {
	log := s.logger.With(zap.String("table", table), zap.String("index", idx.Name), zap.String("action", "DROP INDEX"))
	quotedIndexName := utils.QuoteIdentifier(idx.Name, s.dstDialect)

	var ddl string
	switch s.dstDialect {
	case "mysql":
		// DROP INDEX di MySQL memerlukan nama tabel. Tidak mendukung IF EXISTS.
		quotedTableName := utils.QuoteIdentifier(table, s.dstDialect)
		ddl = fmt.Sprintf("DROP INDEX %s ON %s;", quotedIndexName, quotedTableName)
	case "postgres", "sqlite":
		// PostgreSQL dan SQLite (sejak 3.34.0 untuk DROP INDEX) mendukung IF EXISTS.
		ddl = fmt.Sprintf("DROP INDEX IF EXISTS %s;", quotedIndexName)
	default:
		errMsg := fmt.Sprintf("unsupported destination dialect '%s' for DROP INDEX", s.dstDialect)
		log.Error(errMsg)
		return "", fmt.Errorf(errMsg)
	}
	log.Debug("Generated DROP INDEX DDL.", zap.String("ddl", ddl))
	return ddl, nil
}

// generateDropConstraintDDL generates DROP CONSTRAINT statement.
func (s *SchemaSyncer) generateDropConstraintDDL(table string, constraint ConstraintInfo) (string, error) {
	log := s.logger.With(zap.String("table", table), zap.String("constraint", constraint.Name), zap.String("type", constraint.Type), zap.String("action", "DROP CONSTRAINT"))
	quotedTable := utils.QuoteIdentifier(table, s.dstDialect)
	quotedConstraintName := utils.QuoteIdentifier(constraint.Name, s.dstDialect)

	var ddl string
	switch s.dstDialect {
	case "mysql":
		// MySQL memiliki sintaks berbeda untuk jenis constraint yang berbeda
		// dan umumnya tidak mendukung IF EXISTS untuk DROP CONSTRAINT.
		switch strings.ToUpper(constraint.Type) {
		case "FOREIGN KEY":
			ddl = fmt.Sprintf("ALTER TABLE %s DROP FOREIGN KEY %s;", quotedTable, quotedConstraintName)
		case "UNIQUE":
			// Unik constraint di MySQL bisa jadi constraint atau index.
			// Jika dibuat sebagai constraint (misalnya, `ADD CONSTRAINT ... UNIQUE`), gunakan DROP CONSTRAINT.
			// Jika dibuat sebagai `CREATE UNIQUE INDEX`, gunakan `DROP INDEX`.
			// Logika pengambilan skema (`getMySQLConstraints`, `getMySQLIndexes`) harus konsisten.
			// Kita asumsikan `getMySQLConstraints` hanya mengembalikan constraint eksplisit.
			ddl = fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s;", quotedTable, quotedConstraintName)
			// Alternatif jika itu adalah indeks:
			// ddl = fmt.Sprintf("ALTER TABLE %s DROP INDEX %s;", quotedTable, quotedConstraintName)
			// Perlu cara untuk membedakan UNIQUE constraint vs UNIQUE index di MySQL.
			log.Debug("Generating DROP CONSTRAINT for MySQL UNIQUE. Ensure this was created as a CONSTRAINT, not just a UNIQUE INDEX.", zap.String("constraint_name", constraint.Name))
		case "CHECK":
			// CHECK didukung sejak MySQL 8.0.16
			ddl = fmt.Sprintf("ALTER TABLE %s DROP CHECK %s;", quotedTable, quotedConstraintName)
		case "PRIMARY KEY":
			// Hanya ada satu PK, tidak perlu nama.
			ddl = fmt.Sprintf("ALTER TABLE %s DROP PRIMARY KEY;", quotedTable)
		default:
			errMsg := fmt.Sprintf("unsupported constraint type '%s' for DROP CONSTRAINT in MySQL for constraint '%s'", constraint.Type, constraint.Name)
			log.Error(errMsg)
			return "", fmt.Errorf(errMsg)
		}
	case "postgres", "sqlite":
		// PostgreSQL dan SQLite menggunakan sintaks umum dan mendukung IF EXISTS.
		ddl = fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s;", quotedTable, quotedConstraintName)
	default:
		errMsg := fmt.Sprintf("unsupported destination dialect '%s' for DROP CONSTRAINT", s.dstDialect)
		log.Error(errMsg)
		return "", fmt.Errorf(errMsg)
	}
	log.Debug("Generated DROP CONSTRAINT DDL.", zap.String("ddl", ddl))
	return ddl, nil
}
