package sync

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/arwahdevops/dbsync/internal/config" // Untuk GetTypeMappingForDialects
	"github.com/arwahdevops/dbsync/internal/utils"
)

// --- Structs ---
type SchemaSyncer struct {
	srcDB      *gorm.DB
	dstDB      *gorm.DB
	srcDialect string
	dstDialect string
	logger     *zap.Logger
	// Tambahkan cache untuk compiled regex dari special mappings jika diperlukan untuk performa
	// compiledSpecialPatterns map[string]*regexp.Regexp
}
type ColumnInfo struct {
	Name            string
	Type            string // Tipe asli dari database sumber
	MappedType      string // Tipe yang sudah dimapping ke dialek tujuan
	IsNullable      bool
	IsPrimary       bool
	IsGenerated     bool
	DefaultValue    sql.NullString
	AutoIncrement   bool
	OrdinalPosition int
	Length          sql.NullInt64
	Precision       sql.NullInt64
	Scale           sql.NullInt64
	Collation       sql.NullString
	Comment         sql.NullString
}
type IndexInfo struct {
	Name      string
	Columns   []string
	IsUnique  bool
	IsPrimary bool
	RawDef    string // Definisi mentah jika ada (berguna untuk PostgreSQL)
}
type ConstraintInfo struct {
	Name           string
	Type           string // PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK
	Columns        []string
	Definition     string // Untuk CHECK constraint
	ForeignTable   string // Untuk FOREIGN KEY
	ForeignColumns []string // Untuk FOREIGN KEY
	OnDelete       string // Untuk FOREIGN KEY
	OnUpdate       string // Untuk FOREIGN KEY
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
	}
}

// --- Interface Implementation (Dispatcher Methods) ---

func (s *SchemaSyncer) SyncTableSchema(ctx context.Context, table string, strategy config.SchemaSyncStrategy) (*SchemaExecutionResult, error) {
	start := time.Now()
	log := s.logger.With(zap.String("table", table), zap.String("strategy", string(strategy)))
	log.Info("Starting schema analysis for table")
	result := &SchemaExecutionResult{}

	if s.isSystemTable(table, s.srcDialect) {
		log.Debug("Skipping system table")
		return result, nil
	}

	// Get Source Schema
	srcColumns, err := s.getColumns(ctx, s.srcDB, s.srcDialect, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get source columns for table %s: %w", table, err)
	}
	if len(srcColumns) == 0 {
		log.Warn("Source table appears to be empty or does not exist (no columns found). Skipping schema sync for this table.")
		return result, nil // Tidak ada yang bisa disinkronkan jika tidak ada kolom
	}
	result.PrimaryKeys = getPKColumnNames(srcColumns) // Unquoted PK names

	srcIndexes, err := s.getIndexes(ctx, s.srcDB, s.srcDialect, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get source indexes for table %s: %w", table, err)
	}
	srcConstraints, err := s.getConstraints(ctx, s.srcDB, s.srcDialect, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get source constraints for table %s: %w", table, err)
	}

	// Generate DDL based on strategy
	switch strategy {
	case config.SchemaSyncDropCreate:
		log.Warn("Using 'drop_create' strategy - THIS IS DESTRUCTIVE!")
		tableDDL, _, errGen := s.generateCreateTableDDL(table, srcColumns) // PK sudah dihandle di DDL create table
		if errGen != nil {
			return nil, fmt.Errorf("failed to generate CREATE TABLE DDL for %s: %w", table, errGen)
		}
		result.TableDDL = tableDDL
		// Indeks dan constraint (selain PK) dibuat setelah tabel
		result.IndexDDLs = s.generateCreateIndexDDLs(table, srcIndexes)
		result.ConstraintDDLs = s.generateAddConstraintDDLs(table, srcConstraints)

	case config.SchemaSyncAlter:
		log.Info("Attempting 'alter' schema synchronization strategy")
		dstColumns, dstIndexes, dstConstraints, dstExists, errGetDst := s.getDestinationSchema(ctx, table)
		if errGetDst != nil {
			return nil, fmt.Errorf("failed to get destination schema for table %s: %w", table, errGetDst)
		}

		if !dstExists {
			log.Info("Destination table not found, will perform CREATE TABLE operation instead of ALTER.")
			tableDDL, _, errGen := s.generateCreateTableDDL(table, srcColumns)
			if errGen != nil {
				return nil, fmt.Errorf("failed to generate CREATE TABLE DDL (alter mode, table not exists) for %s: %w", table, errGen)
			}
			result.TableDDL = tableDDL
			result.IndexDDLs = s.generateCreateIndexDDLs(table, srcIndexes)
			result.ConstraintDDLs = s.generateAddConstraintDDLs(table, srcConstraints)
		} else {
			log.Debug("Destination table exists, generating ALTER DDLs if necessary.")
			alterColumnDDLs, indexDDLs, constraintDDLs, errGen := s.generateAlterDDLs(table, srcColumns, srcIndexes, srcConstraints, dstColumns, dstIndexes, dstConstraints)
			if errGen != nil {
				return nil, fmt.Errorf("failed to generate ALTER DDLs for %s: %w", table, errGen)
			}
			if len(alterColumnDDLs) > 0 {
				result.TableDDL = strings.Join(alterColumnDDLs, ";\n") // Gabungkan beberapa ALTER COLUMN
			}
			result.IndexDDLs = indexDDLs         // Ini sudah termasuk DROP dan CREATE
			result.ConstraintDDLs = constraintDDLs // Ini sudah termasuk DROP dan ADD
		}

	case config.SchemaSyncNone:
		log.Info("Schema sync strategy is 'none'. No DDLs will be generated or executed for table structure.")
		// Primary keys masih diperlukan untuk sinkronisasi data
		// result.PrimaryKeys sudah di-set di atas.

	default:
		return nil, fmt.Errorf("unknown schema sync strategy: %s", strategy)
	}

	log.Info("Schema analysis and DDL generation completed for table", zap.Duration("duration", time.Since(start)))
	return result, nil
}

// getDestinationSchema (tetap sama)
func (s *SchemaSyncer) getDestinationSchema(ctx context.Context, table string) (columns []ColumnInfo, indexes []IndexInfo, constraints []ConstraintInfo, exists bool, err error) {
	migrator := s.dstDB.Migrator()
	if !migrator.HasTable(table) {
		s.logger.Debug("Destination table does not exist", zap.String("table", table))
		exists = false
		return
	}
	s.logger.Debug("Fetching destination schema", zap.String("table", table))
	columns, errC := s.getColumns(ctx, s.dstDB, s.dstDialect, table)
	if errC != nil {
		err = fmt.Errorf("get dest columns: %w", errC)
		exists = true
		return
	}
	indexes, errI := s.getIndexes(ctx, s.dstDB, s.dstDialect, table)
	if errI != nil {
		err = fmt.Errorf("get dest indexes: %w", errI)
		exists = true
		return
	}
	constraints, errCo := s.getConstraints(ctx, s.dstDB, s.dstDialect, table)
	if errCo != nil {
		err = fmt.Errorf("get dest constraints: %w", errCo)
		exists = true
		return
	}
	exists = true
	return
}

// ExecuteDDLs (tetap sama)
func (s *SchemaSyncer) ExecuteDDLs(ctx context.Context, table string, ddls *SchemaExecutionResult) error {
	log := s.logger.With(zap.String("table", table))
	if ddls == nil || (ddls.TableDDL == "" && len(ddls.IndexDDLs) == 0 && len(ddls.ConstraintDDLs) == 0) {
		log.Debug("No DDLs to execute for table.")
		return nil
	}

	// Pisahkan DDLs berdasarkan jenis operasi untuk urutan eksekusi yang benar
	var createTableDDL string
	alterColumnDDLs := []string{}
	addIndexDDLs := []string{}
	dropIndexDDLs := []string{}
	addConstraintDDLs := []string{}
	dropConstraintDDLs := []string{}

	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(ddls.TableDDL)), "CREATE TABLE") {
		createTableDDL = ddls.TableDDL
	} else if ddls.TableDDL != "" {
		// TableDDL untuk ALTER bisa berisi beberapa statement dipisah semicolon
		potentialAlters := strings.Split(ddls.TableDDL, ";")
		for _, ddl := range potentialAlters {
			trimmed := strings.TrimSpace(ddl)
			if trimmed != "" {
				alterColumnDDLs = append(alterColumnDDLs, trimmed)
			}
		}
	}

	for _, ddl := range ddls.IndexDDLs {
		trimmed := strings.TrimSpace(ddl)
		if trimmed == "" {
			continue
		}
		if strings.Contains(strings.ToUpper(trimmed), "DROP INDEX") {
			dropIndexDDLs = append(dropIndexDDLs, trimmed)
		} else {
			addIndexDDLs = append(addIndexDDLs, trimmed)
		}
	}
	for _, ddl := range ddls.ConstraintDDLs {
		trimmed := strings.TrimSpace(ddl)
		if trimmed == "" {
			continue
		}
		// Perlu lebih hati-hati membedakan DROP CONSTRAINT untuk FK vs UNIQUE/CHECK
		if strings.Contains(strings.ToUpper(trimmed), "DROP CONSTRAINT") || strings.Contains(strings.ToUpper(trimmed), "DROP FOREIGN KEY") || strings.Contains(strings.ToUpper(trimmed), "DROP CHECK") {
			dropConstraintDDLs = append(dropConstraintDDLs, trimmed)
		} else {
			addConstraintDDLs = append(addConstraintDDLs, trimmed)
		}
	}

	// Eksekusi DDLs dalam transaksi di database tujuan
	return s.dstDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var revertFKChecks func() error // Fungsi untuk mengembalikan FOREIGN_KEY_CHECKS
		isDropCreateStrategy := createTableDDL != ""

		// Khusus MySQL, nonaktifkan FK checks untuk operasi DROP TABLE IF EXISTS
		if isDropCreateStrategy && s.dstDialect == "mysql" {
			log.Debug("Disabling FOREIGN_KEY_CHECKS for MySQL DROP/CREATE.")
			if err := tx.Exec("SET FOREIGN_KEY_CHECKS=0;").Error; err != nil {
				return fmt.Errorf("failed to disable foreign key checks on destination: %w", err)
			}
			revertFKChecks = func() error {
				log.Debug("Re-enabling FOREIGN_KEY_CHECKS for MySQL.")
				return tx.Exec("SET FOREIGN_KEY_CHECKS=1;").Error
			}
			defer func() {
				if rErr := revertFKChecks(); rErr != nil {
					log.Error("Failed to re-enable foreign key checks on destination", zap.Error(rErr))
					// Jangan return error dari defer karena bisa menutupi error asli transaksi
				}
			}()
		}

		// Fase 1: DROP Constraints & Indexes (Hanya untuk strategi ALTER, karena DROP/CREATE akan drop tabel)
		if !isDropCreateStrategy {
			// Urutkan: FK drops dulu, baru Unique/Check, baru Indeks
			sortedDropConstraints := sortConstraintsForDrop(dropConstraintDDLs)
			if len(sortedDropConstraints) > 0 || len(dropIndexDDLs) > 0 {
				log.Info("Executing Phase 1: Dropping existing constraints and/or indexes.")
				for _, ddl := range sortedDropConstraints {
					log.Debug("Executing DROP CONSTRAINT DDL", zap.String("ddl", ddl))
					if err := tx.Exec(ddl).Error; err != nil {
						// Beberapa error drop bisa diabaikan jika objek tidak ada (IF EXISTS menangani ini)
						// Tapi log error jika terjadi untuk investigasi
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
		if createTableDDL != "" {
			log.Warn("Executing DROP TABLE IF EXISTS prior to CREATE (destructive operation).", zap.String("table", table))
			dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", utils.QuoteIdentifier(table, s.dstDialect))
			if s.dstDialect == "postgres" { // PostgreSQL mendukung CASCADE
				dropSQL += " CASCADE"
			}
			if err := tx.Exec(dropSQL).Error; err != nil {
				return fmt.Errorf("failed to execute DROP TABLE IF EXISTS for %s: %w", table, err)
			}
			log.Info("Executing Phase 2: Creating table.")
			log.Debug("Executing CREATE TABLE DDL", zap.String("ddl", createTableDDL))
			if err := tx.Exec(createTableDDL).Error; err != nil {
				return fmt.Errorf("failed to execute CREATE TABLE DDL for %s: %w", table, err)
			}
		} else if len(alterColumnDDLs) > 0 {
			log.Info("Executing Phase 2: Altering table columns.")
			sortedAlters := sortAlterColumns(alterColumnDDLs) // Urutkan ALTER (misal drop dulu baru add)
			for _, ddl := range sortedAlters {
				log.Debug("Executing ALTER COLUMN DDL", zap.String("ddl", ddl))
				if err := tx.Exec(ddl).Error; err != nil {
					// Untuk ALTER, error lebih mungkin fatal untuk statement berikutnya.
					// Namun, tergantung SKIP_FAILED_TABLES, kita mungkin mau log dan lanjut
					// atau return error. Untuk sekarang, return error agar transaksi gagal.
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
					// Error membuat indeks bisa jadi karena nama duplikat (jika IF NOT EXISTS tidak dipakai/didukung)
					// atau data tidak valid. Log dan pertimbangkan apakah fatal.
					log.Error("Failed to execute CREATE INDEX DDL (continuing if possible)", zap.String("ddl", ddl), zap.Error(err))
				}
			}
		}

		// Fase 4: ADD Constraints (Setelah data disinkronkan nanti, tapi DDL disiapkan di sini)
		// Di sini kita hanya mengeksekusi DDL yang sudah diberikan.
		// Sinkronisasi data terjadi di luar fungsi ExecuteDDLs.
		// Jadi, DDL ADD CONSTRAINT dieksekusi setelah CREATE/ALTER dan ADD INDEX.
		if len(addConstraintDDLs) > 0 {
			log.Info("Executing Phase 4: Adding new constraints.")
			sortedAddConstraints := sortConstraintsForAdd(addConstraintDDLs) // Unique/Check dulu baru FK
			for _, ddl := range sortedAddConstraints {
				log.Debug("Executing ADD CONSTRAINT DDL", zap.String("ddl", ddl))
				if err := tx.Exec(ddl).Error; err != nil {
					// Ini bisa gagal jika data melanggar constraint (misal FK).
					// Ini adalah tempat di mana error constraint biasanya muncul.
					return fmt.Errorf("failed to execute ADD CONSTRAINT DDL '%s': %w", ddl, err)
				}
			}
		}

		log.Info("DDL execution transaction finished successfully for table.")
		return nil
	})
}

// GetPrimaryKeys (tetap sama)
func (s *SchemaSyncer) GetPrimaryKeys(ctx context.Context, table string) ([]string, error) {
	columns, err := s.getColumns(ctx, s.srcDB, s.srcDialect, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns for PKs, table %s: %w", table, err)
	}
	pks := getPKColumnNames(columns)
	if len(pks) == 0 {
		s.logger.Debug("No primary key detected for table via column info.", zap.String("table", table))
	}
	return pks, nil
}

// --- Dispatcher Methods untuk getColumns, getIndexes, getConstraints (tetap sama) ---
func (s *SchemaSyncer) getColumns(ctx context.Context, db *gorm.DB, dialect string, table string) ([]ColumnInfo, error) {
	s.logger.Debug("Fetching columns", zap.String("dialect", dialect), zap.String("table", table))
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
		return nil, err
	}
	// Penting: Panggil mapDataType untuk setiap kolom sumber di sini
	// agar MappedType terisi sebelum generateCreateTableDDL atau generateAlterDDLs.
	for i := range cols {
		if cols[i].IsGenerated { // Jangan petakan tipe untuk kolom generated
			cols[i].MappedType = cols[i].Type // Gunakan tipe asli
			s.logger.Debug("Skipping type mapping for generated column", zap.String("column", cols[i].Name), zap.String("original_type", cols[i].Type))
			continue
		}
		mapped, mapErr := s.mapDataType(cols[i].Type)
		if mapErr != nil {
			// mapDataType sudah melakukan logging, jadi di sini kita bisa memutuskan
			// apakah akan return error atau menggunakan tipe asli sebagai fallback.
			// Untuk saat ini, mapDataType mengembalikan fallback, jadi tidak perlu error di sini.
			s.logger.Warn("Type mapping failed for column, using original or generic fallback.",
				zap.String("column", cols[i].Name),
				zap.String("original_type", cols[i].Type),
				zap.Error(mapErr), // mapErr akan nil jika mapDataType mengembalikan fallback
			)
		}
		cols[i].MappedType = mapped
	}
	return cols, nil
}

func (s *SchemaSyncer) getIndexes(ctx context.Context, db *gorm.DB, dialect, table string) ([]IndexInfo, error) {
	s.logger.Debug("Fetching indexes", zap.String("dialect", dialect), zap.String("table", table))
	switch dialect {
	case "mysql":
		return s.getMySQLIndexes(ctx, db, table)
	case "postgres":
		return s.getPostgresIndexes(ctx, db, table)
	case "sqlite":
		return s.getSQLiteIndexes(ctx, db, table)
	default:
		s.logger.Warn("Index fetching not implemented for dialect", zap.String("dialect", dialect))
		return []IndexInfo{}, nil
	}
}
func (s *SchemaSyncer) getConstraints(ctx context.Context, db *gorm.DB, dialect, table string) ([]ConstraintInfo, error) {
	s.logger.Debug("Fetching constraints", zap.String("dialect", dialect), zap.String("table", table))
	switch dialect {
	case "mysql":
		return s.getMySQLConstraints(ctx, db, table)
	case "postgres":
		return s.getPostgresConstraints(ctx, db, table)
	case "sqlite":
		return s.getSQLiteConstraints(ctx, db, table)
	default:
		s.logger.Warn("Constraint fetching not implemented for dialect", zap.String("dialect", dialect))
		return []ConstraintInfo{}, nil
	}
}

// --- General Helper Functions (Common across dialects) ---
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
func (s *SchemaSyncer) isSystemTable(table, dialect string) bool {
	lT := strings.ToLower(table)
	lD := strings.ToLower(dialect)
	switch lD {
	case "mysql":
		return lT == "information_schema" || lT == "performance_schema" || lT == "mysql" || lT == "sys"
	case "postgres":
		return strings.HasPrefix(lT, "pg_") || lT == "information_schema"
	case "sqlite":
		return strings.HasPrefix(lT, "sqlite_")
	default:
		return false
	}
}

// --- Type Mapping Helpers ---
func (s *SchemaSyncer) mapDataType(srcType string) (string, error) {
	log := s.logger.With(zap.String("source_type_raw", srcType), zap.String("src_dialect", s.srcDialect), zap.String("dst_dialect", s.dstDialect))
	fullSrcTypeLower := strings.ToLower(strings.TrimSpace(srcType))

	// Normalisasi dasar: hapus ukuran numerik dan string untuk pencocokan kunci dasar
	// Contoh: "varchar(255)" menjadi "varchar", "decimal(10,2)" menjadi "decimal"
	normalizedSrcTypeKey := regexp.MustCompile(`\s*\(\s*\d+\s*(,\s*\d+\s*)?\)`).ReplaceAllString(fullSrcTypeLower, "")
	normalizedSrcTypeKey = strings.TrimSpace(normalizedSrcTypeKey)

	typeMappingConfigEntry := config.GetTypeMappingForDialects(s.srcDialect, s.dstDialect)

	if typeMappingConfigEntry != nil {
		// 1. Cek Special Mappings (menggunakan regex pada tipe sumber LENGKAP lowercase)
		for _, sm := range typeMappingConfigEntry.SpecialMappings {
			// TODO: Pertimbangkan untuk meng-compile regex sekali saat LoadTypeMappings untuk performa
			// jika ada banyak special mappings dan dipanggil sangat sering.
			// Untuk sekarang, compile on-the-fly.
			re, err := regexp.Compile(sm.SourceTypePattern) // Pola harus regex yang valid
			if err != nil {
				log.Error("Invalid regex pattern in special type mapping",
					zap.String("pattern", sm.SourceTypePattern), zap.Error(err))
				continue // Lewati pola yang tidak valid
			}
			if re.MatchString(fullSrcTypeLower) {
				log.Debug("Matched special type mapping",
					zap.String("pattern", sm.SourceTypePattern),
					zap.String("matched_source_type", fullSrcTypeLower),
					zap.String("target_type", sm.TargetType))
				// `applyTypeModifiers` mungkin masih dibutuhkan jika `sm.TargetType` adalah tipe dasar
				// dan kita ingin mempertahankan modifier dari `srcType` jika relevan.
				return s.applyTypeModifiers(srcType, sm.TargetType), nil
			}
		}

		// 2. Cek Mappings standar (menggunakan kunci yang sudah dinormalisasi)
		if mappedType, ok := typeMappingConfigEntry.Mappings[normalizedSrcTypeKey]; ok {
			log.Debug("Matched standard type mapping",
				zap.String("normalized_source_key", normalizedSrcTypeKey),
				zap.String("matched_source_type", fullSrcTypeLower),
				zap.String("target_type", mappedType))
			return s.applyTypeModifiers(srcType, mappedType), nil
		}

		// 3. Fallback jika tipe dasar (sebelum modifier dan spasi) ada di map
		// Contoh: "character varying" dinormalisasi menjadi "character varying", lalu base type-nya "character"
		baseType := strings.Fields(normalizedSrcTypeKey)[0] // Ambil kata pertama
		if baseType != normalizedSrcTypeKey {               // Hanya jika berbeda dari kunci yang sudah dinormalisasi
			if mappedType, ok := typeMappingConfigEntry.Mappings[baseType]; ok {
				log.Debug("Matched base type mapping (fallback)",
					zap.String("base_source_type", baseType),
					zap.String("matched_source_type", fullSrcTypeLower),
					zap.String("target_type", mappedType))
				return s.applyTypeModifiers(srcType, mappedType), nil
			}
		}
	} else {
		log.Debug("No external type mapping configuration found for dialect pair. Will use generic fallback.",
			zap.String("source_dialect", s.srcDialect),
			zap.String("target_dialect", s.dstDialect))
	}

	// 4. Fallback ke tipe generik jika tidak ada mapping ditemukan
	log.Warn("No specific type mapping found, using generic fallback type.",
		zap.String("normalized_source_key_used", normalizedSrcTypeKey),
		zap.String("original_source_type", srcType))
	return s.getGenericFallbackType(), nil
	// Mengembalikan error di sini jika tidak ada mapping sama sekali bisa jadi opsi,
	// tapi fallback ke tipe teks generik mungkin lebih aman untuk kelangsungan proses.
	// return "", fmt.Errorf("no type mapping found for source type '%s' from %s to %s", srcType, s.srcDialect, s.dstDialect)
}

// applyTypeModifiers (tetap sama)
func (s *SchemaSyncer) applyTypeModifiers(srcType, mappedType string) string {
	re := regexp.MustCompile(`\((.+?)\)`) // (size) or (precision,scale)
	matches := re.FindStringSubmatch(srcType)
	if len(matches) > 1 { // Jika ada modifier (angka dalam kurung)
		modifierContent := matches[1]
		baseMappedType := strings.Split(mappedType, "(")[0] // Ambil nama tipe dasar dari mappedType
		// Daftar tipe yang umumnya menerima modifier panjang/presisi
		// Ini mungkin perlu disesuaikan per dialek tujuan jika lebih detail.
		switch strings.ToUpper(baseMappedType) {
		case "VARCHAR", "CHAR", "NVARCHAR", "NCHAR", // Tipe string dengan panjang
			"DECIMAL", "NUMERIC", // Tipe numerik dengan presisi dan skala
			"VARBINARY", "BINARY", // Tipe biner dengan panjang
			"TIME", "DATETIME", "TIMESTAMP": // Beberapa dialek mendukung presisi untuk tipe waktu
			// Untuk tipe seperti ENUM atau SET di MySQL, modifier-nya adalah list of values, bukan numerik.
			// Logika ini tidak menangani itu, dan sebaiknya ENUM/SET ditangani via special mappings.
			if !strings.Contains(mappedType, "(") { // Hanya tambahkan modifier jika mappedType belum punya
				return fmt.Sprintf("%s(%s)", baseMappedType, modifierContent)
			}
			return mappedType // mappedType sudah punya modifier, jangan timpa
		default:
			return mappedType // Tipe lain umumnya tidak mengambil modifier numerik ini
		}
	}
	return mappedType // Tidak ada modifier di srcType atau mappedType tidak mendukungnya
}

// getGenericFallbackType (tetap sama)
func (s *SchemaSyncer) getGenericFallbackType() string {
	switch s.dstDialect {
	case "mysql":
		return "LONGTEXT"
	case "postgres":
		return "TEXT"
	case "sqlite":
		return "TEXT" // SQLite sering menggunakan TEXT secara fleksibel
	default:
		s.logger.Warn("Unknown destination dialect for generic fallback type", zap.String("dialect", s.dstDialect))
		return "TEXT" // Default paling aman
	}
}

// --- DDL Execution Helpers (Sorting - tetap sama) ---
func sortConstraintsForDrop(ddls []string) []string {
	sorted := make([]string, len(ddls))
	copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		isFkI := strings.Contains(strings.ToUpper(sorted[i]), "DROP FOREIGN KEY") ||
			(strings.Contains(strings.ToUpper(sorted[i]), "DROP CONSTRAINT") && strings.Contains(strings.ToUpper(sorted[i]), "FK_")) // Asumsi FK mengandung FK_
		isFkJ := strings.Contains(strings.ToUpper(sorted[j]), "DROP FOREIGN KEY") ||
			(strings.Contains(strings.ToUpper(sorted[j]), "DROP CONSTRAINT") && strings.Contains(strings.ToUpper(sorted[j]), "FK_"))
		if isFkI && !isFkJ {
			return true
		} // FK drops first
		if !isFkI && isFkJ {
			return false
		}
		return false // Maintain original order for non-FKs or same types
	})
	return sorted
}

func sortConstraintsForAdd(ddls []string) []string {
	sorted := make([]string, len(ddls))
	copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		isFkI := strings.Contains(strings.ToUpper(sorted[i]), "ADD CONSTRAINT") && strings.Contains(strings.ToUpper(sorted[i]), "FOREIGN KEY")
		isFkJ := strings.Contains(strings.ToUpper(sorted[j]), "ADD CONSTRAINT") && strings.Contains(strings.ToUpper(sorted[j]), "FOREIGN KEY")
		if !isFkI && isFkJ {
			return true
		} // Non-FKs (UNIQUE, CHECK) add first
		if isFkI && !isFkJ {
			return false
		}
		return false // Maintain original order for same types
	})
	return sorted
}

func sortAlterColumns(ddls []string) []string {
	sorted := make([]string, len(ddls))
	copy(sorted, ddls)
	sort.SliceStable(sorted, func(i, j int) bool {
		// Prioritaskan DROP COLUMN
		isDropI := strings.Contains(strings.ToUpper(sorted[i]), "DROP COLUMN")
		isDropJ := strings.Contains(strings.ToUpper(sorted[j]), "DROP COLUMN")
		if isDropI && !isDropJ {
			return true
		}
		if !isDropI && isDropJ {
			return false
		}
		// Kemudian ADD COLUMN
		isAddI := strings.Contains(strings.ToUpper(sorted[i]), "ADD COLUMN")
		isAddJ := strings.Contains(strings.ToUpper(sorted[j]), "ADD COLUMN")
		if isAddI && !isAddJ { // Jika I adalah ADD dan J bukan (dan bukan DROP), I setelah J
			return false // ADD harus setelah MODIFY/DROP (jika bukan DROP vs ADD)
		}
		if !isAddI && isAddJ { // Jika J adalah ADD dan I bukan, I sebelum J
			return true
		}
		// MODIFY COLUMN umumnya di tengah atau tidak terlalu berpengaruh urutannya terhadap ADD/DROP
		return false // Maintain original order for other cases (e.g. multiple MODIFY)
	})
	return sorted
}
