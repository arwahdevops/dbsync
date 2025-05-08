// internal/sync/schema_syncer.go
package sync

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strconv" // Diimpor karena digunakan oleh applyTypeModifiers
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/arwahdevops/dbsync/internal/config"
	"github.com/arwahdevops/dbsync/internal/utils"
)

// --- Structs (Definisi ColumnInfo, IndexInfo, ConstraintInfo) ---
// ColumnInfo menyimpan detail tentang kolom database.
type ColumnInfo struct {
	Name            string
	Type            string         // Tipe asli dari database sumber
	MappedType      string         // Tipe yang sudah dimapping ke dialek tujuan
	IsNullable      bool
	IsPrimary       bool
	IsGenerated     bool           // Apakah kolom ini GENERATED (misal, MySQL GENERATED ALWAYS, PG GENERATED)
	DefaultValue    sql.NullString // Nilai default mentah
	AutoIncrement   bool           // Apakah kolom ini auto_increment (MySQL) atau IDENTITY (PG) atau INTEGER PRIMARY KEY (SQLite)
	OrdinalPosition int            // Posisi kolom dalam tabel
	Length          sql.NullInt64  // Untuk tipe string/binary
	Precision       sql.NullInt64  // Untuk tipe numerik/waktu
	Scale           sql.NullInt64  // Untuk tipe numerik
	Collation       sql.NullString
	Comment         sql.NullString
	// GenerationExpression sql.NullString // Jika ingin mengambil dan membandingkan ekspresi generated column
}

// IndexInfo menyimpan detail tentang indeks.
type IndexInfo struct {
	Name      string
	Columns   []string // Nama kolom yang diindeks, diurutkan
	IsUnique  bool
	IsPrimary bool   // Apakah ini indeks yang dibuat untuk PRIMARY KEY
	RawDef    string // Definisi mentah jika ada (berguna untuk PostgreSQL)
	// IndexType string // BTREE, HASH, GIN, GIST (jika bisa didapatkan secara konsisten)
}

// ConstraintInfo menyimpan detail tentang constraint.
type ConstraintInfo struct {
	Name           string
	Type           string   // PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK
	Columns        []string // Nama kolom yang terlibat, diurutkan
	Definition     string   // Untuk CHECK constraint (ekspresi)
	ForeignTable   string   // Untuk FOREIGN KEY: nama tabel referensi
	ForeignColumns []string // Untuk FOREIGN KEY: nama kolom referensi, diurutkan
	OnDelete       string   // Untuk FOREIGN KEY: aksi ON DELETE (CASCADE, SET NULL, dll.)
	OnUpdate       string   // Untuk FOREIGN KEY: aksi ON UPDATE
}

// SchemaSyncer menangani logika sinkronisasi skema.
type SchemaSyncer struct {
	srcDB      *gorm.DB
	dstDB      *gorm.DB
	srcDialect string
	dstDialect string
	logger     *zap.Logger
	// Cache untuk informasi skema mentah, jika diperlukan untuk detail seperti GenerationExpression
	// Ini perlu diisi saat getColumns jika ingin digunakan.
	// rawSchemaInfo map[string]map[string]interface{} // map[tableName][columnName]map[attribute]value
}

// Ensure SchemaSyncer implements the interface
var _ SchemaSyncerInterface = (*SchemaSyncer)(nil)

// --- Constructor ---
func NewSchemaSyncer(srcDB, dstDB *gorm.DB, srcDialect, dstDialect string, logger *zap.Logger) *SchemaSyncer {
	return &SchemaSyncer{
		srcDB:      srcDB,
		dstDB:      dstDB,
		srcDialect: strings.ToLower(srcDialect),
		dstDialect: strings.ToLower(dstDialect),
		logger:     logger.Named("schema-syncer"),
		// rawSchemaInfo: make(map[string]map[string]interface{}), // Inisialisasi jika digunakan
	}
}

// --- Interface Implementation (Dispatcher Methods) ---

// SyncTableSchema menganalisis skema sumber/tujuan dan menghasilkan DDLs berdasarkan strategi.
// Ini TIDAK mengeksekusi DDLs itu sendiri.
func (s *SchemaSyncer) SyncTableSchema(ctx context.Context, table string, strategy config.SchemaSyncStrategy) (*SchemaExecutionResult, error) {
	start := time.Now()
	log := s.logger.With(zap.String("table", table), zap.String("strategy", string(strategy)))
	log.Info("Starting schema analysis for table")

	result := &SchemaExecutionResult{
		PrimaryKeys: make([]string, 0), // Inisialisasi
	}

	if s.isSystemTable(table, s.srcDialect) {
		log.Debug("Skipping system table for schema sync.")
		return result, nil // Tidak ada DDL, PK kosong
	}

	// 1. Get Source Schema
	srcColumns, err := s.getColumns(ctx, s.srcDB, s.srcDialect, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get source columns for table '%s': %w", table, err)
	}
	if len(srcColumns) == 0 {
		log.Warn("Source table appears to have no columns or does not exist. Skipping schema sync for this table.")
		return result, nil // Tidak ada yang bisa disinkronkan
	}
	result.PrimaryKeys = getPKColumnNames(srcColumns) // Dapatkan PK dari kolom sumber

	srcIndexes, err := s.getIndexes(ctx, s.srcDB, s.srcDialect, table)
	if err != nil {
		log.Error("Failed to get source indexes for table", zap.Error(err))
		srcIndexes = []IndexInfo{} // Lanjutkan dengan array kosong
	}
	srcConstraints, err := s.getConstraints(ctx, s.srcDB, s.srcDialect, table)
	if err != nil {
		log.Error("Failed to get source constraints for table", zap.Error(err))
		srcConstraints = []ConstraintInfo{} // Lanjutkan dengan array kosong
	}

	// Pastikan MappedType di srcColumns sudah terisi SEBELUM generate DDL.
	// Fungsi getColumns sudah melakukan ini sekarang.
	// (Loop mapping tipe di sini tidak diperlukan lagi jika getColumns sudah benar)


	// 2. Generate DDL based on strategy
	switch strategy {
	case config.SchemaSyncDropCreate:
		log.Warn("Using 'drop_create' strategy - THIS IS DESTRUCTIVE!")
		tableDDL, _, errGen := s.generateCreateTableDDL(table, srcColumns)
		if errGen != nil {
			return nil, fmt.Errorf("failed to generate CREATE TABLE DDL for '%s': %w", table, errGen)
		}
		result.TableDDL = tableDDL
		result.IndexDDLs = s.generateCreateIndexDDLs(table, srcIndexes)
		result.ConstraintDDLs = s.generateAddConstraintDDLs(table, srcConstraints)

	case config.SchemaSyncAlter:
		log.Info("Attempting 'alter' schema synchronization strategy")
		dstColumns, dstIndexes, dstConstraints, dstExists, errGetDst := s.getDestinationSchema(ctx, table)
		if errGetDst != nil {
			return nil, fmt.Errorf("failed to get destination schema for table '%s': %w", table, errGetDst)
		}

		if !dstExists {
			log.Info("Destination table not found, will perform CREATE TABLE operation instead of ALTER.")
			tableDDL, _, errGen := s.generateCreateTableDDL(table, srcColumns)
			if errGen != nil {
				return nil, fmt.Errorf("failed to generate CREATE TABLE DDL (alter mode, table not exists) for '%s': %w", table, errGen)
			}
			result.TableDDL = tableDDL
			result.IndexDDLs = s.generateCreateIndexDDLs(table, srcIndexes)
			result.ConstraintDDLs = s.generateAddConstraintDDLs(table, srcConstraints)
		} else {
			log.Debug("Destination table exists, generating ALTER DDLs if necessary.")
			// Panggil s.generateAlterDDLs (dari schema_alter.go)
			joinedAlterColumnDDLs, indexDDLs, constraintDDLs, errGen := s.generateAlterDDLs(table, srcColumns, srcIndexes, srcConstraints, dstColumns, dstIndexes, dstConstraints)
			if errGen != nil {
				return nil, fmt.Errorf("failed to generate ALTER DDLs for '%s': %w", table, errGen)
			}

			result.TableDDL = joinedAlterColumnDDLs
			result.IndexDDLs = indexDDLs
			result.ConstraintDDLs = constraintDDLs
		}

	case config.SchemaSyncNone:
		log.Info("Schema sync strategy is 'none'. No DDLs will be generated or executed for table structure.")
		// result.PrimaryKeys sudah di-set dari srcColumns di atas.

	default:
		return nil, fmt.Errorf("unknown schema sync strategy: %s", strategy)
	}

	log.Info("Schema analysis and DDL generation completed for table", zap.Duration("duration", time.Since(start)))
	return result, nil
}

// getDestinationSchema mengambil skema (kolom, indeks, constraint) dari tabel tujuan.
// Mengembalikan `exists = false` jika tabel tidak ada.
func (s *SchemaSyncer) getDestinationSchema(ctx context.Context, table string) (
	columns []ColumnInfo, indexes []IndexInfo, constraints []ConstraintInfo, exists bool, err error) {

	log := s.logger.With(zap.String("table", table), zap.String("database", "destination"))

	// Cek apakah tabel ada
	if !s.dstDB.Migrator().HasTable(table) {
		log.Debug("Destination table does not exist based on GORM Migrator.")
		exists = false
		return
	}
	exists = true
	log.Debug("Destination table exists. Fetching schema details.")

	columns, errC := s.getColumns(ctx, s.dstDB, s.dstDialect, table)
	if errC != nil {
		err = fmt.Errorf("get destination columns for table '%s': %w", table, errC)
		return
	}
	// Pastikan MappedType untuk kolom tujuan diisi (dengan Type-nya sendiri)
	for i := range columns {
		if columns[i].MappedType == "" { columns[i].MappedType = columns[i].Type }
	}


	indexes, errI := s.getIndexes(ctx, s.dstDB, s.dstDialect, table)
	if errI != nil {
		log.Error("Failed to get destination indexes", zap.Error(errI))
		indexes = []IndexInfo{} // Lanjutkan dengan slice kosong
	}

	constraints, errCo := s.getConstraints(ctx, s.dstDB, s.dstDialect, table)
	if errCo != nil {
		log.Error("Failed to get destination constraints", zap.Error(errCo))
		constraints = []ConstraintInfo{} // Lanjutkan dengan slice kosong
	}
	return
}


// ExecuteDDLs menerapkan DDLs yang dihasilkan ke database tujuan.
// DDLs dieksekusi dalam urutan yang logis (misalnya, DROP dulu, FKs terakhir).
func (s *SchemaSyncer) ExecuteDDLs(ctx context.Context, table string, ddls *SchemaExecutionResult) error {
	log := s.logger.With(zap.String("table", table), zap.String("database", "destination"))

	if ddls == nil || (ddls.TableDDL == "" && len(ddls.IndexDDLs) == 0 && len(ddls.ConstraintDDLs) == 0) {
		log.Debug("No DDLs to execute for table.")
		return nil
	}
	log.Info("Starting DDL execution for table.")

	// Pisahkan DDLs berdasarkan jenis operasi
	var createTableDDL string
	alterColumnDDLs := []string{}
	addIndexDDLs := []string{}
	dropIndexDDLs := []string{}
	addConstraintDDLs := []string{}
	dropConstraintDDLs := []string{}

	if ddls.TableDDL != "" {
		trimmedUpperTableDDL := strings.ToUpper(strings.TrimSpace(ddls.TableDDL))
		if strings.HasPrefix(trimmedUpperTableDDL, "CREATE TABLE") {
			createTableDDL = ddls.TableDDL
		} else {
			potentialAlters := strings.Split(ddls.TableDDL, ";")
			for _, ddl := range potentialAlters {
				trimmed := strings.TrimSpace(ddl)
				if trimmed != "" {
					alterColumnDDLs = append(alterColumnDDLs, trimmed)
				}
			}
		}
	}


	for _, ddl := range ddls.IndexDDLs {
		trimmed := strings.TrimSpace(ddl)
		if trimmed == "" { continue }
		upperTrimmed := strings.ToUpper(trimmed)
		if strings.Contains(upperTrimmed, "DROP INDEX") {
			dropIndexDDLs = append(dropIndexDDLs, trimmed)
		} else if strings.Contains(upperTrimmed, "CREATE INDEX") || strings.Contains(upperTrimmed, "CREATE UNIQUE INDEX") {
			addIndexDDLs = append(addIndexDDLs, trimmed)
		} else {
			log.Warn("Unknown DDL type in IndexDDLs, skipping execution", zap.String("ddl", trimmed))
		}
	}

	for _, ddl := range ddls.ConstraintDDLs {
		trimmed := strings.TrimSpace(ddl)
		if trimmed == "" { continue }
		upperTrimmed := strings.ToUpper(trimmed)
		if strings.Contains(upperTrimmed, "DROP CONSTRAINT") || strings.Contains(upperTrimmed, "DROP FOREIGN KEY") || strings.Contains(upperTrimmed, "DROP CHECK") || strings.Contains(upperTrimmed, "DROP PRIMARY KEY") {
			dropConstraintDDLs = append(dropConstraintDDLs, trimmed)
		} else if strings.Contains(upperTrimmed, "ADD CONSTRAINT") {
			addConstraintDDLs = append(addConstraintDDLs, trimmed)
		} else {
			log.Warn("Unknown DDL type in ConstraintDDLs, skipping execution", zap.String("ddl", trimmed))
		}
	}

	// Eksekusi DDLs dalam transaksi
	return s.dstDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var revertFKChecks func() error
		isDropCreateStrategy := createTableDDL != ""

		if isDropCreateStrategy && s.dstDialect == "mysql" {
			log.Debug("Disabling FOREIGN_KEY_CHECKS for MySQL DROP/CREATE.")
			if err := tx.Exec("SET FOREIGN_KEY_CHECKS=0;").Error; err != nil {
				return fmt.Errorf("failed to disable foreign key checks on destination for table '%s': %w", table, err)
			}
			revertFKChecks = func() error {
				log.Debug("Re-enabling FOREIGN_KEY_CHECKS for MySQL for table.", zap.String("table", table))
				return tx.Exec("SET FOREIGN_KEY_CHECKS=1;").Error
			}
			defer func() {
				if revertFKChecks != nil {
					if rErr := revertFKChecks(); rErr != nil {
						log.Error("Failed to re-enable foreign key checks on destination for table", zap.String("table", table), zap.Error(rErr))
					}
				}
			}()
		}

		// Fase 1: DROP Constraints & Indexes (Hanya untuk strategi ALTER)
		if !isDropCreateStrategy {
			sortedDropConstraints := sortConstraintsForDrop(dropConstraintDDLs)
			if len(sortedDropConstraints) > 0 || len(dropIndexDDLs) > 0 {
				log.Info("Executing Phase 1 (ALTER): Dropping existing constraints and/or indexes.")
				for _, ddl := range sortedDropConstraints {
					log.Debug("Executing DROP CONSTRAINT DDL", zap.String("ddl", ddl))
					if err := tx.Exec(ddl).Error; err != nil {
						log.Error("Failed to execute DROP CONSTRAINT DDL (continuing if possible)", zap.String("ddl", ddl), zap.Error(err))
					}
				}
				for _, ddl := range dropIndexDDLs {
					log.Debug("Executing DROP INDEX DDL", zap.String("ddl", ddl))
					if err := tx.Exec(ddl).Error; err != nil {
						log.Error("Failed to execute DROP INDEX DDL (continuing if possible)", zap.String("ddl", ddl), zap.Error(err))
					}
				}
			}
		}

		// Fase 2: Table Structure (CREATE atau ALTER)
		if isDropCreateStrategy {
			log.Warn("Executing DROP TABLE IF EXISTS prior to CREATE (destructive operation).", zap.String("table", table))
			dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", utils.QuoteIdentifier(table, s.dstDialect))
			if s.dstDialect == "postgres" { dropSQL += " CASCADE" }
			if err := tx.Exec(dropSQL).Error; err != nil {
				return fmt.Errorf("failed to execute DROP TABLE IF EXISTS for '%s': %w", table, err)
			}
			log.Info("Executing Phase 2 (DROP/CREATE): Creating table.")
			log.Debug("Executing CREATE TABLE DDL", zap.String("ddl", createTableDDL))
			if err := tx.Exec(createTableDDL).Error; err != nil {
				return fmt.Errorf("failed to execute CREATE TABLE DDL for '%s': %w", table, err)
			}
		} else if len(alterColumnDDLs) > 0 {
			log.Info("Executing Phase 2 (ALTER): Altering table columns.")
			sortedAlters := sortAlterColumns(alterColumnDDLs)
			for _, ddl := range sortedAlters {
				log.Debug("Executing ALTER COLUMN DDL", zap.String("ddl", ddl))
				if err := tx.Exec(ddl).Error; err != nil {
					return fmt.Errorf("failed to execute ALTER COLUMN DDL '%s': %w", ddl, err)
				}
			}
		} else {
			log.Info("Phase 2: No table structure (CREATE/ALTER) changes needed.")
		}

		// Fase 3: ADD Indexes (Setelah tabel dibuat/diubah)
		if len(addIndexDDLs) > 0 {
			log.Info("Executing Phase 3: Creating new indexes.")
			for _, ddl := range addIndexDDLs {
				log.Debug("Executing CREATE INDEX DDL", zap.String("ddl", ddl))
				if err := tx.Exec(ddl).Error; err != nil {
					log.Error("Failed to execute CREATE INDEX DDL (continuing if possible)", zap.String("ddl", ddl), zap.Error(err))
				}
			}
		}

		// Fase 4: ADD Constraints
		if len(addConstraintDDLs) > 0 {
			log.Info("Executing Phase 4: Adding new constraints.")
			sortedAddConstraints := sortConstraintsForAdd(addConstraintDDLs)
			for _, ddl := range sortedAddConstraints {
				log.Debug("Executing ADD CONSTRAINT DDL", zap.String("ddl", ddl))
				if err := tx.Exec(ddl).Error; err != nil {
					return fmt.Errorf("failed to execute ADD CONSTRAINT DDL '%s': %w", ddl, err)
				}
			}
		}

		log.Info("DDL execution transaction phase finished successfully for table.")
		return nil
	})
}


// GetPrimaryKeys mengambil nama kolom kunci primer untuk sebuah tabel dari sumber.
func (s *SchemaSyncer) GetPrimaryKeys(ctx context.Context, table string) ([]string, error) {
	log := s.logger.With(zap.String("table", table), zap.String("database", "source"))
	log.Debug("Fetching primary keys for table.")

	columns, err := s.getColumns(ctx, s.srcDB, s.srcDialect, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns for PK detection, table '%s': %w", table, err)
	}

	pks := getPKColumnNames(columns) // Helper untuk ekstrak nama PK dari []ColumnInfo
	if len(pks) == 0 {
		log.Warn("No primary key detected for table via column information.")
		// Coba fallback ke constraints jika perlu
		constraints, errCons := s.getConstraints(ctx, s.srcDB, s.srcDialect, table)
		if errCons == nil {
			for _, cons := range constraints {
				if cons.Type == "PRIMARY KEY" {
					pks = cons.Columns // Kolom di ConstraintInfo harusnya sudah diurutkan
					log.Info("Found primary key via constraints.", zap.Strings("pk_columns", pks))
					break
				}
			}
		} else {
			log.Error("Failed to get constraints for PK fallback detection", zap.Error(errCons))
		}
	} else {
		log.Debug("Primary keys detected via column information.", zap.Strings("pk_columns", pks))
	}
	return pks, nil
}

// --- Dispatcher Methods untuk getColumns, getIndexes, getConstraints ---
func (s *SchemaSyncer) getColumns(ctx context.Context, db *gorm.DB, dialect string, table string) ([]ColumnInfo, error) {
	log := s.logger.With(zap.String("dialect", dialect), zap.String("table", table))
	log.Debug("Fetching table columns.")
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
		return nil, fmt.Errorf("getColumns: unsupported dialect: %s", dialect)
	}
	if err != nil {
		return nil, err // Error sudah dibungkus oleh fungsi spesifik dialek
	}

	// Lakukan mapping tipe data HANYA jika mengambil kolom dari SUMBER (s.srcDB)
	if db == s.srcDB {
		for i := range cols {
			if cols[i].IsGenerated {
				cols[i].MappedType = cols[i].Type // Gunakan tipe asli, jangan mapping
				log.Debug("Skipping type mapping for generated source column", zap.String("column", cols[i].Name), zap.String("original_type", cols[i].Type))
				continue
			}
			mapped, mapErr := s.mapDataType(cols[i].Type)
			if mapErr != nil {
				log.Warn("Type mapping failed for source column, using original or generic fallback.",
					zap.String("column", cols[i].Name), zap.String("original_type", cols[i].Type), zap.Error(mapErr))
				// mapDataType sekarang mengembalikan fallback, jadi mapErr mungkin nil
				// tapi kita tetap set MappedType
			}
			cols[i].MappedType = mapped
		}
	} else { // Jika ini kolom dari TUJUAN (dstDB)
		for i := range cols {
			// Set MappedType sama dengan Type asli untuk konsistensi struct
			cols[i].MappedType = cols[i].Type
		}
	}
	return cols, nil
}

func (s *SchemaSyncer) getIndexes(ctx context.Context, db *gorm.DB, dialect, table string) ([]IndexInfo, error) {
	s.logger.Debug("Fetching table indexes.", zap.String("dialect", dialect), zap.String("table", table))
	switch dialect {
	case "mysql":
		return s.getMySQLIndexes(ctx, db, table)
	case "postgres":
		return s.getPostgresIndexes(ctx, db, table)
	case "sqlite":
		return s.getSQLiteIndexes(ctx, db, table)
	default:
		s.logger.Warn("Index fetching not implemented for dialect, returning empty.", zap.String("dialect", dialect))
		return []IndexInfo{}, nil
	}
}
func (s *SchemaSyncer) getConstraints(ctx context.Context, db *gorm.DB, dialect, table string) ([]ConstraintInfo, error) {
	s.logger.Debug("Fetching table constraints.", zap.String("dialect", dialect), zap.String("table", table))
	switch dialect {
	case "mysql":
		return s.getMySQLConstraints(ctx, db, table)
	case "postgres":
		return s.getPostgresConstraints(ctx, db, table)
	case "sqlite":
		return s.getSQLiteConstraints(ctx, db, table)
	default:
		s.logger.Warn("Constraint fetching not implemented for dialect, returning empty.", zap.String("dialect", dialect))
		return []ConstraintInfo{}, nil
	}
}

// --- General Helper Functions (Common across dialects) ---

// getPKColumnNames mengambil nama kolom Primary Key dari slice ColumnInfo.
// Mengembalikan slice nama PK yang sudah diurutkan.
func getPKColumnNames(columns []ColumnInfo) []string {
	var pks []string
	for _, c := range columns {
		if c.IsPrimary {
			pks = append(pks, c.Name)
		}
	}
	sort.Strings(pks) // Pastikan urutan konsisten
	return pks
}

// isSystemTable memeriksa apakah nama tabel adalah tabel sistem untuk dialek tertentu.
func (s *SchemaSyncer) isSystemTable(table, dialect string) bool {
	lT := strings.ToLower(table)
	switch dialect {
	case "mysql":
		return strings.HasPrefix(lT, "information_schema.") ||
			strings.HasPrefix(lT, "performance_schema.") ||
			strings.HasPrefix(lT, "mysql.") ||
			strings.HasPrefix(lT, "sys.") ||
			lT == "information_schema" || lT == "performance_schema" || lT == "mysql" || lT == "sys"
	case "postgres":
		return strings.HasPrefix(lT, "pg_") ||
			strings.HasPrefix(lT, "information_schema.") ||
			lT == "information_schema"
	case "sqlite":
		return strings.HasPrefix(lT, "sqlite_")
	default:
		return false
	}
}

// --- Type Mapping Helpers ---

// mapDataType memetakan tipe data dari dialek sumber ke dialek tujuan.
func (s *SchemaSyncer) mapDataType(srcType string) (string, error) {
	log := s.logger.With(zap.String("source_type_raw", srcType),
		zap.String("src_dialect", s.srcDialect), zap.String("dst_dialect", s.dstDialect))

	fullSrcTypeLower := strings.ToLower(strings.TrimSpace(srcType))
	normalizedSrcTypeKey := normalizeTypeName(fullSrcTypeLower)

	typeMappingConfigEntry := config.GetTypeMappingForDialects(s.srcDialect, s.dstDialect)

	if typeMappingConfigEntry != nil {
		log.Debug("Found external type mapping configuration for dialect pair.")
		// 1. Cek Special Mappings
		for _, sm := range typeMappingConfigEntry.SpecialMappings {
			re, errComp := regexp.Compile(sm.SourceTypePattern)
			if errComp != nil {
				log.Error("Invalid regex pattern in special type mapping, skipping.",
					zap.String("pattern", sm.SourceTypePattern), zap.Error(errComp))
				continue
			}
			if re.MatchString(fullSrcTypeLower) {
				log.Debug("Matched special type mapping",
					zap.String("pattern", sm.SourceTypePattern),
					zap.String("target_type", sm.TargetType))
				return s.applyTypeModifiers(srcType, sm.TargetType), nil
			}
		}

		// 2. Cek Mappings standar
		if mappedType, ok := typeMappingConfigEntry.Mappings[normalizedSrcTypeKey]; ok {
			log.Debug("Matched standard type mapping",
				zap.String("normalized_source_key", normalizedSrcTypeKey),
				zap.String("target_type", mappedType))
			return s.applyTypeModifiers(srcType, mappedType), nil
		}
	} else {
		log.Debug("No external type mapping configuration found for dialect pair. Will use generic fallback or direct type if dialects are same.")
	}

	// 4. Jika dialek sumber dan tujuan sama, gunakan tipe sumber apa adanya
	if s.srcDialect == s.dstDialect {
		log.Debug("Source and destination dialects are the same, using source type directly (with modifiers).")
		return s.applyTypeModifiers(srcType, srcType), nil
	}

	// 5. Fallback ke tipe generik
	fallbackType := s.getGenericFallbackType()
	log.Warn("No specific type mapping found, using generic fallback type.",
		zap.String("normalized_source_key_used_for_lookup", normalizedSrcTypeKey),
		zap.String("original_source_type", srcType),
		zap.String("fallback_target_type", fallbackType))
	return fallbackType, nil
}

// applyTypeModifiers mencoba menerapkan modifier (panjang, presisi, skala) dari tipe sumber
// ke tipe dasar tujuan yang sudah dimapped, jika relevan.
func (s *SchemaSyncer) applyTypeModifiers(srcTypeRaw, mappedBaseType string) string {
	re := regexp.MustCompile(`\((.+?)\)`)
	matches := re.FindStringSubmatch(srcTypeRaw)
	var modifierContent string
	if len(matches) > 1 { modifierContent = matches[1] }

	baseTypeWithoutExistingModifiers := strings.Split(mappedBaseType, "(")[0]
	normBaseType := normalizeTypeName(baseTypeWithoutExistingModifiers)

	if modifierContent != "" {
		canHaveModifier := false
		switch {
		case isStringType(normBaseType) && s.dstDialect != "sqlite":
			if !strings.Contains(normBaseType, "text") && !strings.Contains(normBaseType, "blob") && normBaseType != "clob" && normBaseType != "json" && normBaseType != "xml" {
				canHaveModifier = true
			}
		case isBinaryType(normBaseType) && s.dstDialect != "sqlite":
			if !strings.Contains(normBaseType, "blob") && normBaseType != "bytea" {
				canHaveModifier = true
			}
		case isScaleRelevant(normBaseType): // DECIMAL, NUMERIC
			canHaveModifier = true
		case isPrecisionRelevant(normBaseType) && (strings.Contains(normBaseType, "time") || strings.Contains(normBaseType, "timestamp")):
			// Untuk tipe waktu, modifier biasanya hanya presisi (angka tunggal)
			parts := strings.Split(modifierContent, ",")
			if len(parts) == 1 {
				if _, err := strconv.Atoi(parts[0]); err == nil {
					modifierContent = parts[0] // Gunakan hanya angka presisi
					canHaveModifier = true
				}
			}
		}

		if canHaveModifier {
			return fmt.Sprintf("%s(%s)", baseTypeWithoutExistingModifiers, modifierContent)
		}
	}
	return baseTypeWithoutExistingModifiers
}


// getGenericFallbackType mengembalikan tipe data fallback generik untuk dialek tujuan.
func (s *SchemaSyncer) getGenericFallbackType() string {
	switch s.dstDialect {
	case "mysql": return "LONGTEXT"
	case "postgres": return "TEXT"
	case "sqlite": return "TEXT"
	default:
		s.logger.Error("Unknown destination dialect for generic fallback type, defaulting to TEXT.", zap.String("unknown_dialect", s.dstDialect))
		return "TEXT"
	}
}

// --- DDL Execution Helpers (Sorting) ---

// sortConstraintsForDrop mengurutkan DDL DROP CONSTRAINT. FKs didahulukan.
func sortConstraintsForDrop(ddls []string) []string {
	sorted := make([]string, len(ddls)); copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		isFkI := strings.Contains(strings.ToUpper(sorted[i]), "FOREIGN KEY")
		isFkJ := strings.Contains(strings.ToUpper(sorted[j]), "FOREIGN KEY")
		if isFkI && !isFkJ { return true }
		if !isFkI && isFkJ { return false }
		// Jika tipe sama, atau keduanya bukan FK, urutan tidak diubah
		return false // Preserve relative order of non-FKs or FKs themselves
	})
	return sorted
}

// sortConstraintsForAdd mengurutkan DDL ADD CONSTRAINT. UNIQUE/CHECK didahulukan sebelum FK.
func sortConstraintsForAdd(ddls []string) []string {
	sorted := make([]string, len(ddls)); copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		isFkI := strings.Contains(strings.ToUpper(sorted[i]), "FOREIGN KEY")
		isFkJ := strings.Contains(strings.ToUpper(sorted[j]), "FOREIGN KEY")
		if !isFkI && isFkJ { return true } // Non-FKs add first
		if isFkI && !isFkJ { return false }
		return false // Preserve relative order
	})
	return sorted
}

// sortAlterColumns mengurutkan DDL ALTER COLUMN.
// Urutan ideal: DROP dulu, baru MODIFY, baru ADD.
func sortAlterColumns(ddls []string) []string {
	priority := func(ddl string) int {
		upperDDL := strings.ToUpper(ddl)
		if strings.Contains(upperDDL, "DROP COLUMN") { return 1 }
		if strings.Contains(upperDDL, "MODIFY COLUMN") || strings.Contains(upperDDL, "ALTER COLUMN") { return 2 }
		if strings.Contains(upperDDL, "ADD COLUMN") { return 3 }
		return 4
	}
	sorted := make([]string, len(ddls)); copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		return priority(sorted[i]) < priority(sorted[j])
	})
	return sorted
}
