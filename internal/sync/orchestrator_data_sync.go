package sync

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	// "github.com/arwahdevops/dbsync/internal/config" // Tidak dibutuhkan langsung di file ini
	"github.com/arwahdevops/dbsync/internal/db"
	"github.com/arwahdevops/dbsync/internal/utils"
)

// getDestinationColumnTypes mengambil tipe data kolom dari tabel tujuan.
// Ini adalah method privat dari Orchestrator, bisa juga dipindah ke schema_fetcher khusus destinasi.
func (f *Orchestrator) getDestinationColumnTypes(ctx context.Context, tableName string) (map[string]string, error) {
	log := f.logger.With(zap.String("table", tableName), zap.String("dialect", f.dstConn.Dialect), zap.String("action", "getDestinationColumnTypes"))
	colTypes := make(map[string]string)

	var columnsData []struct {
		ColumnName string `gorm:"column:column_name"`
		DataType   string `gorm:"column:data_type"`
		UdtName    string `gorm:"column:udt_name"` // Untuk PostgreSQL
	}

	// Query spesifik per dialek untuk mendapatkan tipe data
	switch f.dstConn.Dialect {
	case "postgres":
		query := `
            SELECT column_name, data_type, udt_name
            FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = $1;`
		if err := f.dstConn.DB.WithContext(ctx).Raw(query, tableName).Scan(&columnsData).Error; err != nil {
			log.Error("Failed to fetch destination column types for PostgreSQL", zap.Error(err))
			return nil, fmt.Errorf("fetch postgres column types for %s: %w", tableName, err)
		}
		for _, c := range columnsData {
			normType := normalizeTypeName(c.DataType) // normalizeTypeName dari syncer_type_mapper.go
			if normType == "boolean" || strings.ToLower(c.UdtName) == "bool" {
				colTypes[c.ColumnName] = "boolean" // Penanda internal
			} else if normType == "array" && c.UdtName != "" { // Handle array types
				baseUdt := strings.TrimPrefix(c.UdtName, "_")
				colTypes[c.ColumnName] = normalizeTypeName(baseUdt) + "[]"
			} else if normType == "user-defined" && c.UdtName != "" {
				colTypes[c.ColumnName] = normalizeTypeName(c.UdtName)
			} else {
				colTypes[c.ColumnName] = normType
			}
		}
	case "mysql":
		// Untuk MySQL, kita perlu mengetahui MappedType dari sumber ke tujuan.
		// Ini lebih kompleks karena tipe asli MySQL bisa beragam (TINYINT(1), BOOLEAN alias).
		// Transformasi boolean untuk MySQL->MySQL biasanya tidak diperlukan karena 0/1 sudah valid.
		// Jika MySQL adalah tujuan dari PG, maka PG 'boolean' akan dipetakan ke 'tinyint(1)' oleh typemap.
		// Data dari PG (true/false) perlu dikonversi ke 1/0 untuk MySQL tinyint(1).
		// Logika ini akan ditambahkan jika diperlukan. Untuk sekarang, fokus pada PG sebagai tujuan.
		log.Debug("MySQL destination column type fetching for data transformation (e.g. bool) not fully implemented here yet. Relies on direct GORM handling.")

		// Namun, kita tetap bisa mengambil tipe data dasar MySQL jika diperlukan untuk transformasi lain.
		query := `SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS
				  WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?;`
		if err := f.dstConn.DB.WithContext(ctx).Raw(query, tableName).Scan(&columnsData).Error; err != nil {
			log.Error("Failed to fetch destination column types for MySQL", zap.Error(err))
			return nil, fmt.Errorf("fetch mysql column types for %s: %w", tableName, err)
		}
		for _, c := range columnsData {
			colTypes[c.ColumnName] = normalizeTypeName(c.DataType)
		}

	default:
		log.Debug("Destination column type fetching not implemented for dialect", zap.String("dialect", f.dstConn.Dialect))
	}
	log.Debug("Fetched destination column types.", zap.Any("types", colTypes))
	return colTypes, nil
}


// syncData performs the actual data transfer using pagination.
func (f *Orchestrator) syncData(ctx context.Context, table string, srcPKColumns []string) (totalRowsSynced int64, batches int, err error) {
	log := f.logger.With(zap.String("table", table), zap.Strings("source_pk_columns_for_pagination", srcPKColumns))

	var totalRows int64 = -1
	countCtx, countCancel := context.WithTimeout(ctx, 15*time.Second)
	countErr := f.srcConn.DB.WithContext(countCtx).Table(table).Count(&totalRows).Error
	countCancel()

	if countErr != nil {
		logFields := []zap.Field{zap.Error(countErr)}
		if errors.Is(countErr, context.DeadlineExceeded) {
			logFields = append(logFields, zap.Duration("count_query_timeout", 15*time.Second))
		} else if errors.Is(ctx.Err(), context.Canceled) {
			logFields = append(logFields, zap.NamedError("main_context_error", ctx.Err()))
		}
		log.Warn("Could not count total source rows for progress tracking. Progress percentage will not be available.", logFields...)
	} else if totalRows == 0 {
		log.Info("Source table is empty. No data to synchronize.")
		return 0, 0, nil // Tidak ada data, tidak ada error
	} else {
		log.Info("Approximate total source rows to synchronize", zap.Int64("count", totalRows))
	}

	// Dapatkan Primary Key dan Tipe Kolom dari tabel TUJUAN
	var dstPKColumns []string
	var dstColumnTypes map[string]string
	var errSchemaInfo error

	schemaInfoCtx, schemaInfoCancel := context.WithTimeout(ctx, 25*time.Second) // Timeout gabungan
	defer schemaInfoCancel()

	dstPKColumns, errSchemaInfo = f.getDestinationTablePKs(schemaInfoCtx, table)
	if errSchemaInfo != nil {
		log.Error("Failed to get destination primary keys for upsert. Upsert might default to UpdateAll or fail.", zap.Error(errSchemaInfo))
		// Tidak menggagalkan, biarkan syncBatchWithRetry mencoba fallback
	} else if len(dstPKColumns) == 0 {
		log.Warn("No primary key found for destination table. ON CONFLICT upsert will use GORM's UpdateAll or default behavior, which might be inefficient or fail if table has no other updatable columns.", zap.String("table", table))
	}

	// Ambil tipe kolom tujuan untuk transformasi data (misalnya, boolean)
	// Hanya relevan jika tujuan adalah PostgreSQL untuk konversi boolean dari MySQL 0/1.
	// Atau jika MySQL adalah tujuan dan sumbernya PG boolean (true/false -> 1/0).
	if f.dstConn.Dialect == "postgres" || f.dstConn.Dialect == "mysql" { // Perlu untuk kedua arah jika ada boolean
		dstColumnTypes, errSchemaInfo = f.getDestinationColumnTypes(schemaInfoCtx, table)
		if errSchemaInfo != nil {
			log.Error("Failed to get destination column types. Data transformation (e.g., boolean) might not occur as expected.", zap.Error(errSchemaInfo))
		}
	}


	var orderByClause string
	var quotedSrcPKColumns, srcPKPlaceholders []string
	canPaginate := len(srcPKColumns) > 0

	if canPaginate {
		var sortErr error
		orderByClause, quotedSrcPKColumns, srcPKPlaceholders, sortErr = f.buildPaginationClauses(srcPKColumns, f.srcConn.Dialect)
		if sortErr != nil {
			return 0, 0, fmt.Errorf("failed to build source pagination clauses for table '%s': %w", table, sortErr)
		}
		log.Debug("Using pagination for data sync.", zap.String("order_by_clause", orderByClause))
	} else {
		log.Warn("No source primary key provided/found for pagination. Attempting full table load in batches without explicit ordering for pagination.")
		orderByClause = ""
	}

	var revertFKsFunc func() error
	if f.cfg.DisableFKDuringSync && (f.dstConn.Dialect == "mysql" || f.dstConn.Dialect == "postgres") {
		var disableErr error
		revertFKsFunc, disableErr = f.toggleForeignKeys(ctx, f.dstConn, false, log)
		if disableErr != nil {
			log.Error("Attempt to disable foreign keys before data sync failed. Proceeding with FKs in current state.", zap.Error(disableErr))
		}
		if revertFKsFunc != nil {
			defer func() {
				log.Info("Attempting to re-enable/revert foreign keys after data sync completion or failure.")
				revertCtx, revertCancel := context.WithTimeout(context.Background(), 45*time.Second)
				defer revertCancel()
				if enableErr := f.revertFKsWithContext(revertCtx, revertFKsFunc); enableErr != nil {
					log.Error("Failed to re-enable/revert foreign keys after data sync.", zap.Error(enableErr))
				} else {
					log.Info("Foreign keys re-enabled/reverted successfully.")
				}
			}()
		}
	}

	var lastSrcPKValues []interface{}
	totalRowsSynced = 0
	batches = 0
	progressLogThreshold := 100
	noDataStreak := 0

	for {
		if errCtx := ctx.Err(); errCtx != nil {
			log.Warn("Context cancelled or timed out during data sync loop for table.", zap.Error(errCtx))
			return totalRowsSynced, batches, errCtx
		}

		var batchData []map[string]interface{}
		query := f.srcConn.DB.WithContext(ctx).Table(table)

		if canPaginate && lastSrcPKValues != nil {
			if len(quotedSrcPKColumns) == 0 || len(srcPKPlaceholders) == 0 {
				return totalRowsSynced, batches, fmt.Errorf("internal error: source pagination columns/placeholders not initialized for table '%s'", table)
			}
			whereClause, args := f.buildWhereClause(quotedSrcPKColumns, srcPKPlaceholders, lastSrcPKValues)
			query = query.Where(whereClause, args...)
		}

		if orderByClause != "" { query = query.Order(orderByClause) }
		query = query.Limit(f.cfg.BatchSize)

		log.Debug("Fetching next batch from source database.", zap.Int("batch_limit", f.cfg.BatchSize), zap.Any("last_pk_values_for_query", lastSrcPKValues))
		fetchStartTime := time.Now()
		fetchErr := query.Find(&batchData).Error
		fetchDuration := time.Since(fetchStartTime)

		if fetchErr != nil {
			log.Error("Failed to fetch data batch from source database.", zap.Error(fetchErr))
			return totalRowsSynced, batches, fmt.Errorf("fetch data batch from source table '%s': %w", table, fetchErr)
		}

		rowsInCurrentBatch := len(batchData)
		log.Debug("Data batch fetched from source.", zap.Duration("fetch_duration", fetchDuration), zap.Int("rows_in_batch", rowsInCurrentBatch))

		if rowsInCurrentBatch == 0 {
			if totalRowsSynced == 0 && lastSrcPKValues == nil { log.Info("Source table is empty or first fetch returned no data.") } else { log.Info("No more data found in source for this page, or sync complete.") }
			if canPaginate && lastSrcPKValues != nil {
				noDataStreak++
				if noDataStreak >= 3 {
					log.Error("Potential pagination issue: Multiple consecutive empty batches.", zap.Any("last_src_pk_values", lastSrcPKValues))
					return totalRowsSynced, batches, fmt.Errorf("pagination issue: empty batches for table '%s'", table)
				}
			}
			break
		}
		noDataStreak = 0

		// Transformasi data sebelum dikirim ke tujuan
		if dstColumnTypes != nil {
			for i, row := range batchData {
				transformedRow := make(map[string]interface{}, len(row))
				for key, val := range row {
					targetType, typeKnown := dstColumnTypes[key]
					if typeKnown {
						if f.dstConn.Dialect == "postgres" && targetType == "boolean" {
							// Konversi MySQL 0/1 (int/float) ke PG boolean (true/false)
							switch v := val.(type) {
							case int: transformedRow[key] = (v == 1)
							case int8: transformedRow[key] = (v == 1)
							case int16: transformedRow[key] = (v == 1)
							case int32: transformedRow[key] = (v == 1)
							case int64: transformedRow[key] = (v == 1)
							case float32: transformedRow[key] = (v == 1.0)
							case float64: transformedRow[key] = (v == 1.0)
							// Jika sudah bool, tidak perlu konversi
							case bool: transformedRow[key] = v
							default: transformedRow[key] = val // Tipe lain, biarkan
							}
							continue
						} else if f.dstConn.Dialect == "mysql" && targetType == "tinyint" { // Asumsi tinyint(1) untuk boolean
							// Konversi PG boolean (true/false) ke MySQL tinyint(1) (1/0)
							if boolVal, isBool := val.(bool); isBool {
								if boolVal { transformedRow[key] = 1 } else { transformedRow[key] = 0 }
								continue
							}
						}
					}
					transformedRow[key] = val // Salin jika tidak ada transformasi
				}
				batchData[i] = transformedRow
			}
		}

		log.Debug("Attempting to sync batch to destination database.", zap.Int("rows_to_sync", rowsInCurrentBatch))
		syncBatchStartTime := time.Now()
		batchSyncErr := f.syncBatchWithRetry(ctx, table, batchData, dstPKColumns)
		syncBatchDuration := time.Since(syncBatchStartTime)
		log.Debug("Batch synchronization attempt to destination complete.", zap.Duration("sync_duration", syncBatchDuration))

		if batchSyncErr != nil {
			return totalRowsSynced, batches, fmt.Errorf("sync batch to destination table '%s': %w", table, batchSyncErr)
		}

		batches++
		totalRowsSynced += int64(rowsInCurrentBatch)
		f.metrics.RowsSyncedTotal.WithLabelValues(table).Add(float64(rowsInCurrentBatch))

		if canPaginate {
			lastRowInBatch := batchData[rowsInCurrentBatch-1]
			sortedSrcPKNames, pkSortErr := f.getSortedPKNames(srcPKColumns)
			if pkSortErr != nil { return totalRowsSynced, batches, fmt.Errorf("get sorted source PK names for table '%s': %w", table, pkSortErr) }

			newLastSrcPKValues := make([]interface{}, len(sortedSrcPKNames))
			allPKsFound := true
			for i, pkName := range sortedSrcPKNames {
				val, ok := lastRowInBatch[pkName]
				if !ok {
					log.Error("Source PK column missing in fetched data, pagination cannot continue.", zap.String("missing_pk", pkName))
					allPKsFound = false; break
				}
				newLastSrcPKValues[i] = val
			}
			if !allPKsFound { return totalRowsSynced, batches, fmt.Errorf("source PK column missing in fetched data for table '%s'", table) }
			lastSrcPKValues = newLastSrcPKValues
		} else {
			if rowsInCurrentBatch < f.cfg.BatchSize { break } // Jika tidak paginasi dan batch tidak penuh, anggap selesai
		}

		if batches%progressLogThreshold == 0 || (totalRows > 0 && totalRowsSynced >= totalRows && totalRowsSynced > 0) {
			progressPercent := -1.0
			if totalRows > 0 { progressPercent = (float64(totalRowsSynced) / float64(totalRows)) * 100 }
			log.Info("Data sync in progress...", zap.Int("batch_num", batches), zap.Int64("rows_synced_so_far", totalRowsSynced), zap.Int64("total_src_rows", totalRows), zap.Float64("progress_pct", progressPercent))
		}
	}

	log.Info("Data synchronization finished for table.", zap.Int64("total_rows_synced", totalRowsSynced), zap.Int("total_batches", batches))
	return totalRowsSynced, batches, nil
}

// syncBatchWithRetry melakukan upsert batch data ke tabel tujuan dengan retry.
func (f *Orchestrator) syncBatchWithRetry(ctx context.Context, table string, batch []map[string]interface{}, dstPKColumns []string) error {
	log := f.logger.With(zap.String("table", table), zap.Int("batch_size", len(batch)))
	if len(batch) == 0 {
		log.Debug("Empty batch received, skipping upsert.")
		return nil
	}

	var lastErr error
	startTime := time.Now()
	metricStatus := "failure_unknown"
	defer func() {
		f.metrics.BatchProcessingDuration.WithLabelValues(table, metricStatus).Observe(time.Since(startTime).Seconds())
		if strings.HasPrefix(metricStatus, "success") { f.metrics.BatchesProcessedTotal.WithLabelValues(table).Inc()
		} else if !strings.Contains(metricStatus, "context_cancelled") { f.metrics.BatchErrorsTotal.WithLabelValues(table).Inc() }
	}()

	var conflictClause clause.Expression
	if len(dstPKColumns) > 0 { // Jika PK tujuan diketahui
		conflictTargetCols := make([]clause.Column, len(dstPKColumns))
		for i, pk := range dstPKColumns { conflictTargetCols[i] = clause.Column{Name: pk} }

		// Tentukan kolom yang akan diupdate: semua kolom di batch kecuali PK tujuan
		var colsToUpdateOnConflict []string
		if len(batch) > 0 {
			firstRow := batch[0]
			isDstPKMap := make(map[string]bool, len(dstPKColumns))
			for _, pk := range dstPKColumns { isDstPKMap[pk] = true }
			for colName := range firstRow {
				if !isDstPKMap[colName] { colsToUpdateOnConflict = append(colsToUpdateOnConflict, colName) }
			}
		}

		if len(colsToUpdateOnConflict) > 0 {
			log.Debug("Building ON CONFLICT DO UPDATE SET for specified non-PK columns.", zap.Strings("conflict_target_cols", dstPKColumns), zap.Strings("update_cols", colsToUpdateOnConflict))
			conflictClause = clause.OnConflict{ Columns: conflictTargetCols, DoUpdates: clause.AssignmentColumns(colsToUpdateOnConflict) }
		} else {
			log.Info("No non-PK columns found in batch to update on conflict. Using ON CONFLICT DO NOTHING.", zap.Strings("conflict_target_cols", dstPKColumns))
			conflictClause = clause.OnConflict{ Columns: conflictTargetCols, DoNothing: true }
		}
	} else { // Jika PK tujuan tidak diketahui atau untuk dialek seperti SQLite yang mungkin tidak memerlukan kolom konflik eksplisit
		log.Warn("Destination PKs not specified for ON CONFLICT clause; falling back to GORM's UpdateAll or dialect default behavior. This might be inefficient or cause 'model value required' if no updatable columns found by GORM.",
			zap.String("dialect", f.dstConn.Dialect))
		conflictClause = clause.OnConflict{UpdateAll: true} // Perilaku GORM default
	}


	for attempt := 0; attempt <= f.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			log.Warn("Retrying batch insert/upsert.", zap.Int("attempt", attempt+1), zap.NamedError("previous_error", lastErr))
			select {
			case <-time.After(f.cfg.RetryInterval):
			case <-ctx.Done():
				log.Error("Context cancelled while waiting for batch retry.", zap.Error(ctx.Err()))
				metricStatus = "failure_context_cancelled_retry"
				return fmt.Errorf("context cancelled on retry for table '%s': %w; last DB error: %v", table, ctx.Err(), lastErr)
			}
		}

		txErr := f.dstConn.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			return tx.Table(table).Clauses(conflictClause).CreateInBatches(batch, len(batch)).Error
		})

		if txErr == nil {
			metricStatus = "success"; if attempt > 0 { metricStatus = "success_retry" }
			log.Debug("Batch upsert successful.", zap.Int("attempt", attempt+1))
			return nil
		}
		lastErr = txErr

		if ctx.Err() != nil {
			log.Error("Context cancelled during/after batch transaction.", zap.Error(ctx.Err()), zap.NamedError("transaction_error", lastErr))
			metricStatus = "failure_context_cancelled_tx"
			return fmt.Errorf("context cancelled during batch tx for table '%s': %w; last DB error: %v", table, ctx.Err(), lastErr)
		}
		log.Warn("Batch upsert attempt failed.", zap.Int("attempt", attempt+1), zap.Error(lastErr))
	}

	log.Error("Batch upsert failed after all retries.", zap.NamedError("final_error", lastErr))
	metricStatus = "failure_max_retries"
	return fmt.Errorf("upsert batch to table '%s' failed after %d retries: %w", table, f.cfg.MaxRetries, lastErr)
}

// getSortedPKNames mengurutkan nama kolom PK (dari sumber) untuk konsistensi paginasi.
func (f *Orchestrator) getSortedPKNames(pkColumns []string) ([]string, error) {
	if len(pkColumns) == 0 {
		return []string{}, nil
	}
	sortedPKs := make([]string, len(pkColumns))
	copy(sortedPKs, pkColumns)
	sort.Strings(sortedPKs)
	return sortedPKs, nil
}

// buildPaginationClauses membangun klausa ORDER BY dan informasi placeholder untuk paginasi sumber.
func (f *Orchestrator) buildPaginationClauses(srcPKColumns []string, dialect string) (orderBy string, quotedPKs []string, placeholders []string, err error) {
	if len(srcPKColumns) == 0 {
		err = fmt.Errorf("no source primary keys provided for pagination")
		return
	}
	// Menggunakan urutan PK dari srcPKColumns apa adanya jika sudah diurutkan berdasarkan ordinal position.
	// Jika tidak, sorting berdasarkan nama adalah fallback untuk determinisme.
	// Saat ini, SchemaSyncer.GetPrimaryKeys (yang mengisi srcPKColumns) sudah sort by name.

	// Untuk konsistensi dengan WHERE (...tuple...) > (...tuple...), kita sort by name.
	// Jika urutan asli PK komposit dari DB penting dan berbeda, ini perlu disesuaikan.
	pksToUse, _ := f.getSortedPKNames(srcPKColumns) // Ini akan sort by name

	quotedPKs = make([]string, len(pksToUse))
	placeholders = make([]string, len(pksToUse))
	orderByParts := make([]string, len(pksToUse))

	for i, pk := range pksToUse {
		quotedPKs[i] = utils.QuoteIdentifier(pk, dialect)
		placeholders[i] = "?"
		orderByParts[i] = quotedPKs[i] + " ASC"
	}
	orderBy = strings.Join(orderByParts, ", ")
	return
}

// buildWhereClause membangun klausa WHERE untuk paginasi sumber.
func (f *Orchestrator) buildWhereClause(quotedSortedSrcPKs []string, placeholders []string, lastSrcPKValues []interface{}) (string, []interface{}) {
	if len(quotedSortedSrcPKs) == 0 {
		return "1=1", []interface{}{}
	}
	if len(quotedSortedSrcPKs) == 1 {
		return fmt.Sprintf("%s > ?", quotedSortedSrcPKs[0]), lastSrcPKValues
	}
	if len(quotedSortedSrcPKs) != len(placeholders) || len(quotedSortedSrcPKs) != len(lastSrcPKValues) {
		f.logger.Error("Mismatch in counts for building composite PK WHERE clause.",
			zap.Int("quoted_pks", len(quotedSortedSrcPKs)),
			zap.Int("placeholders", len(placeholders)),
			zap.Int("values", len(lastSrcPKValues)))
		if len(lastSrcPKValues) > 0 && len(quotedSortedSrcPKs) > 0 { // Fallback aman
			f.logger.Warn("Falling back to single PK condition for WHERE clause.")
			return fmt.Sprintf("%s > ?", quotedSortedSrcPKs[0]), []interface{}{lastSrcPKValues[0]}
		}
		return "1=0", []interface{}{} // Tidak akan cocok
	}
	whereTuple := fmt.Sprintf("(%s)", strings.Join(quotedSortedSrcPKs, ", "))
	placeholderTuple := fmt.Sprintf("(%s)", strings.Join(placeholders, ", "))
	return fmt.Sprintf("%s > %s", whereTuple, placeholderTuple), lastSrcPKValues
}


// toggleForeignKeys dan revertFKsWithContext sekarang adalah method Orchestrator
// (Definisi mereka dari jawaban sebelumnya bisa disalin ke sini atau ke file helper Orchestrator)
func (f *Orchestrator) toggleForeignKeys(ctx context.Context, conn *db.Connector, enable bool, log *zap.Logger) (revertFunc func() error, err error) {
	if conn.Dialect != "mysql" && conn.Dialect != "postgres" {
		log.Debug("Foreign key toggling skipped: not supported for dialect or not needed.", zap.String("dialect", conn.Dialect))
		return nil, nil
	}

	var cmdToExecute, cmdToRevert, initialStateCmd string
	var initialState interface{}
	var defaultInitialState interface{}

	dialect := conn.Dialect
	scopedLog := log.With(zap.String("target_dialect_for_fk_toggle", dialect))

	switch dialect {
	case "mysql":
		initialStateCmd = "SELECT @@SESSION.foreign_key_checks;"
		cmdToExecute = "SET SESSION foreign_key_checks = 0;"    // Disable
		cmdToRevert = "SET SESSION foreign_key_checks = %v;" // Placeholder untuk state awal
		defaultInitialState = 1                                // MySQL default FKs on
	case "postgres":
		initialStateCmd = "SHOW session_replication_role;"
		cmdToExecute = "SET session_replication_role = replica;" // Disable (efektifnya)
		cmdToRevert = "SET session_replication_role = '%s';"  // Placeholder untuk state awal
		defaultInitialState = "origin"                         // PG default
		scopedLog.Warn("Using SET session_replication_role for PostgreSQL FK toggling. User needs superuser or REPLICATION attribute.")
	default:
		return nil, nil
	}

	// Baca state awal
	var stateStr string
	readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
	errRead := conn.DB.WithContext(readCtx).Raw(initialStateCmd).Scan(&stateStr).Error
	readCancel()

	if errRead != nil {
		scopedLog.Warn("Could not read initial FK/replication state. Assuming default for revert.", zap.Error(errRead), zap.Any("assumed_default", defaultInitialState))
		initialState = defaultInitialState
	} else {
		scopedLog.Info("Successfully read initial FK/replication state.", zap.String("retrieved_state", stateStr))
		if dialect == "mysql" {
			if val, convErr := strconv.Atoi(stateStr); convErr == nil {
				initialState = val
			} else {
				scopedLog.Warn("Could not convert MySQL FK state to int. Assuming default.", zap.Error(convErr))
				initialState = defaultInitialState
			}
		} else { // postgres
			initialState = strings.TrimSpace(stateStr)
		}
	}

	// Format perintah revert dengan state awal
	if dialect == "mysql" {
		cmdToRevert = fmt.Sprintf(cmdToRevert, initialState.(int))
	} else { // postgres
		cmdToRevert = fmt.Sprintf(cmdToRevert, initialState.(string))
	}

	finalCmdToExecute := cmdToExecute // Perintah untuk menonaktifkan
	if enable {                     // Jika 'enable' true, berarti kita ingin MENGAKTIFKAN (revert)
		finalCmdToExecute = cmdToRevert
	}

	description := "Disabling FKs/triggers"
	if enable {
		description = fmt.Sprintf("Re-enabling/Reverting FKs/triggers to '%v'", initialState)
	}

	scopedLog.Info(description, zap.String("command", finalCmdToExecute))
	execCtx, execCancel := context.WithTimeout(ctx, 10*time.Second)
	executionError := conn.DB.WithContext(execCtx).Exec(finalCmdToExecute).Error
	execCancel()

	if executionError != nil {
		scopedLog.Error("Failed to execute command for toggling FKs.", zap.Error(executionError))
		return nil, fmt.Errorf("FK toggle command '%s' failed: %w", finalCmdToExecute, executionError)
	}

	// Fungsi revert yang akan dipanggil oleh defer
	revertFunc = func() error {
		rCtx, rCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer rCancel()
		scopedLog.Info("Executing revert command for FKs/triggers.", zap.String("revert_command", cmdToRevert))
		if rErr := conn.DB.WithContext(rCtx).Exec(cmdToRevert).Error; rErr != nil {
			scopedLog.Error("Failed to execute revert command for FKs.", zap.Error(rErr))
			return fmt.Errorf("FK revert command '%s' failed: %w", cmdToRevert, rErr)
		}
		return nil
	}
	return revertFunc, nil
}

func (f *Orchestrator) revertFKsWithContext(ctx context.Context, revertFunc func() error) error {
	if revertFunc == nil { return nil }
	doneChan := make(chan error, 1)
	go func() { doneChan <- revertFunc() }()
	select {
	case err := <-doneChan: return err
	case <-ctx.Done():
		f.logger.Error("Context timed out/cancelled while waiting for FK revert.", zap.Error(ctx.Err()))
		return fmt.Errorf("reverting FKs cancelled by context: %w", ctx.Err())
	}
}
