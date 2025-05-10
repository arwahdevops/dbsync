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

	"github.com/arwahdevops/dbsync/internal/db" // Diperlukan
	"github.com/arwahdevops/dbsync/internal/utils"
)

// getDestinationColumnTypes (tetap sama seperti sebelumnya)
func (f *Orchestrator) getDestinationColumnTypes(ctx context.Context, tableName string) (map[string]string, error) {
	log := f.logger.With(zap.String("table", tableName), zap.String("dialect", f.dstConn.Dialect), zap.String("action", "getDestinationColumnTypes"))
	colTypes := make(map[string]string)

	var columnsData []struct {
		ColumnName string `gorm:"column:column_name"`
		DataType   string `gorm:"column:data_type"`
		UdtName    string `gorm:"column:udt_name"` // Untuk PostgreSQL
	}

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
			normType := normalizeTypeName(c.DataType)
			if normType == "boolean" || strings.ToLower(c.UdtName) == "bool" {
				colTypes[c.ColumnName] = "boolean"
			} else if normType == "array" && c.UdtName != "" {
				baseUdt := strings.TrimPrefix(c.UdtName, "_")
				colTypes[c.ColumnName] = normalizeTypeName(baseUdt) + "[]"
			} else if normType == "user-defined" && c.UdtName != "" {
				colTypes[c.ColumnName] = normalizeTypeName(c.UdtName)
			} else {
				colTypes[c.ColumnName] = normType
			}
		}
	case "mysql":
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
		return 0, 0, nil
	} else {
		log.Info("Approximate total source rows to synchronize", zap.Int64("count", totalRows))
	}

	var dstPKColumns []string
	var dstColumnTypes map[string]string
	var errSchemaInfo error

	schemaInfoCtx, schemaInfoCancel := context.WithTimeout(ctx, 25*time.Second)
	defer schemaInfoCancel()

	dstPKColumns, errSchemaInfo = f.getDestinationTablePKs(schemaInfoCtx, table)
	if errSchemaInfo != nil {
		log.Error("Failed to get destination primary keys for upsert. Upsert might default to UpdateAll or fail.", zap.Error(errSchemaInfo))
	} else if len(dstPKColumns) == 0 {
		log.Warn("No primary key found for destination table. ON CONFLICT upsert will use GORM's UpdateAll or default behavior, which might be inefficient or fail if table has no other updatable columns.", zap.String("table", table))
	}

	if f.dstConn.Dialect == "postgres" || f.dstConn.Dialect == "mysql" {
		dstColumnTypes, errSchemaInfo = f.getDestinationColumnTypes(schemaInfoCtx, table)
		if errSchemaInfo != nil {
			log.Error("Failed to get destination column types. Data transformation (e.g., boolean) might not occur as expected.", zap.Error(errSchemaInfo))
		}
	}

	var orderByClause string
	var quotedSrcPKColumns []string // Tidak perlu placeholders di sini lagi
	canPaginate := len(srcPKColumns) > 0

	if canPaginate {
		var sortErr error
		// buildPaginationClauses sekarang hanya mengembalikan orderBy dan quotedPKs
		orderByClause, quotedSrcPKColumns, sortErr = f.buildPaginationOrderBy(srcPKColumns, f.srcConn.Dialect)
		if sortErr != nil {
			return 0, 0, fmt.Errorf("failed to build source pagination order by clause for table '%s': %w", table, sortErr)
		}
		log.Debug("Using pagination for data sync.", zap.String("order_by_clause", orderByClause), zap.Strings("quoted_src_pk_cols", quotedSrcPKColumns))
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

	var lastSrcPKValues []interface{} // Ini akan menjadi nilai-nilai PK dari baris terakhir batch sebelumnya
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
			if len(quotedSrcPKColumns) == 0 { // quotedSrcPKColumns didapat dari buildPaginationOrderBy
				return totalRowsSynced, batches, fmt.Errorf("internal error: source pagination columns not initialized for table '%s'", table)
			}
			// buildWhereClause sekarang juga menerima dialek sumber
			whereClause, args, whereErr := f.buildWhereClause(quotedSrcPKColumns, lastSrcPKValues, f.srcConn.Dialect)
			if whereErr != nil {
				return totalRowsSynced, batches, fmt.Errorf("failed to build WHERE clause for pagination on table '%s': %w", table, whereErr)
			}
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
				if noDataStreak >= 3 { // Meningkatkan toleransi sedikit, bisa jadi ada gap kecil di data
					log.Error("Potential pagination issue: Multiple consecutive empty batches despite having previous PK values. This might indicate inconsistent data sorting or a bug.", zap.Any("last_src_pk_values", lastSrcPKValues))
					// Tidak return error fatal di sini, biarkan loop berakhir secara alami.
					// Namun, ini adalah warning keras.
				}
			}
			break
		}
		noDataStreak = 0

		// Transformasi data (tetap sama)
		if dstColumnTypes != nil {
			for i, row := range batchData {
				transformedRow := make(map[string]interface{}, len(row))
				for key, val := range row {
					targetType, typeKnown := dstColumnTypes[key]
					if typeKnown {
						if f.dstConn.Dialect == "postgres" && targetType == "boolean" {
							switch v := val.(type) {
							case int: transformedRow[key] = (v == 1)
							case int8: transformedRow[key] = (v == 1)
							case int16: transformedRow[key] = (v == 1)
							case int32: transformedRow[key] = (v == 1)
							case int64: transformedRow[key] = (v == 1)
							case float32: transformedRow[key] = (v == 1.0)
							case float64: transformedRow[key] = (v == 1.0)
							case bool: transformedRow[key] = v
							default: transformedRow[key] = val
							}
							continue
						} else if f.dstConn.Dialect == "mysql" && targetType == "tinyint" {
							if boolVal, isBool := val.(bool); isBool {
								if boolVal { transformedRow[key] = 1 } else { transformedRow[key] = 0 }
								continue
							}
						}
					}
					transformedRow[key] = val
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
			// srcPKColumns adalah nama-nama PK sumber yang tidak di-quote, sesuai urutan dari schema fetch
			// yang mana idealnya sudah sesuai urutan ordinal PK jika dari DB, atau di-sort by name sebagai fallback.
			// Untuk paginasi, urutan yang digunakan di ORDER BY (dari buildPaginationOrderBy) adalah yang relevan.
			// QuotedSrcPKColumns sudah di-sort by name di buildPaginationOrderBy.
			// Kita perlu mengambil nilai PK dari lastRowInBatch sesuai urutan quotedSrcPKColumns.

			// Dapatkan nama PK yang tidak di-quote dari quotedSrcPKColumns (yang sudah di-sort by name)
			// agar bisa lookup di lastRowInBatch.
			unquotedSortedSrcPKNames := make([]string, len(quotedSrcPKColumns))
			for i, qpk := range quotedSrcPKColumns {
				unquotedSortedSrcPKNames[i] = utils.UnquoteIdentifier(qpk, f.srcConn.Dialect) // Perlu helper Unquote
			}


			newLastSrcPKValues := make([]interface{}, len(unquotedSortedSrcPKNames))
			allPKsFound := true
			for i, pkName := range unquotedSortedSrcPKNames {
				val, ok := lastRowInBatch[pkName]
				if !ok {
					log.Error("Source PK column missing in fetched data, pagination cannot continue.", zap.String("missing_pk_unquoted", pkName), zap.String("corresponding_quoted_pk", quotedSrcPKColumns[i]))
					allPKsFound = false; break
				}
				newLastSrcPKValues[i] = val
			}
			if !allPKsFound { return totalRowsSynced, batches, fmt.Errorf("source PK column missing in fetched data for table '%s'", table) }
			lastSrcPKValues = newLastSrcPKValues
		} else {
			if rowsInCurrentBatch < f.cfg.BatchSize { break }
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


// syncBatchWithRetry (tetap sama seperti sebelumnya)
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
	if len(dstPKColumns) > 0 {
		conflictTargetCols := make([]clause.Column, len(dstPKColumns))
		for i, pk := range dstPKColumns { conflictTargetCols[i] = clause.Column{Name: pk} }

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
	} else {
		log.Warn("Destination PKs not specified for ON CONFLICT clause; falling back to GORM's UpdateAll or dialect default behavior. This might be inefficient or cause 'model value required' if no updatable columns found by GORM.",
			zap.String("dialect", f.dstConn.Dialect))
		conflictClause = clause.OnConflict{UpdateAll: true}
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


// buildPaginationOrderBy membangun klausa ORDER BY dan mengembalikan nama PK yang di-quote dan diurutkan.
// Kolom PK diurutkan berdasarkan nama untuk konsistensi.
func (f *Orchestrator) buildPaginationOrderBy(srcPKColumns []string, dialect string) (orderBy string, quotedSortedPKs []string, err error) {
	if len(srcPKColumns) == 0 {
		err = fmt.Errorf("no source primary keys provided for pagination")
		return
	}

	// Urutkan nama PK berdasarkan abjad untuk memastikan urutan yang konsisten dalam ORDER BY dan WHERE
	// Ini penting jika urutan asli dari skema tidak dijamin konsisten.
	pksToUse := make([]string, len(srcPKColumns))
	copy(pksToUse, srcPKColumns)
	sort.Strings(pksToUse) // Urutkan berdasarkan nama

	quotedSortedPKs = make([]string, len(pksToUse))
	orderByParts := make([]string, len(pksToUse))

	for i, pk := range pksToUse {
		quotedSortedPKs[i] = utils.QuoteIdentifier(pk, dialect)
		orderByParts[i] = quotedSortedPKs[i] + " ASC" // Selalu ASC untuk paginasi standar
	}
	orderBy = strings.Join(orderByParts, ", ")
	return
}

// buildWhereClause membangun klausa WHERE untuk paginasi sumber.
// `quotedSortedSrcPKs` harus merupakan nama kolom PK yang sudah di-quote dan diurutkan (sama dengan urutan di ORDER BY).
// `lastSrcPKValues` harus berisi nilai-nilai yang sesuai dengan urutan `quotedSortedSrcPKs`.
func (f *Orchestrator) buildWhereClause(quotedSortedSrcPKs []string, lastSrcPKValues []interface{}, srcDialect string) (string, []interface{}, error) {
	numPKs := len(quotedSortedSrcPKs)
	if numPKs == 0 {
		return "1=1", []interface{}{}, nil // Tidak ada PK, tidak ada paginasi
	}
	if numPKs != len(lastSrcPKValues) {
		return "", nil, fmt.Errorf("mismatch between number of PK columns (%d) and PK values (%d)", numPKs, len(lastSrcPKValues))
	}

	if srcDialect == "sqlite" && numPKs > 1 {
		// Logika "Seek Method" untuk SQLite Composite PK
		// WHERE (pk1 > val1) OR
		//       (pk1 = val1 AND pk2 > val2) OR
		//       (pk1 = val1 AND pk2 = val2 AND pk3 > val3) ...
		var conditions []string
		var args []interface{}

		for i := 0; i < numPKs; i++ {
			var currentLevelConditions []string
			// Tambahkan kondisi pk_prev = val_prev untuk semua PK sebelumnya
			for j := 0; j < i; j++ {
				currentLevelConditions = append(currentLevelConditions, fmt.Sprintf("%s = ?", quotedSortedSrcPKs[j]))
				args = append(args, lastSrcPKValues[j])
			}
			// Tambahkan kondisi pk_current > val_current
			currentLevelConditions = append(currentLevelConditions, fmt.Sprintf("%s > ?", quotedSortedSrcPKs[i]))
			args = append(args, lastSrcPKValues[i])

			conditions = append(conditions, "("+strings.Join(currentLevelConditions, " AND ")+")")
		}
		return strings.Join(conditions, " OR "), args, nil
	}

	// Untuk dialek lain (MySQL, PostgreSQL) yang mendukung row-value constructor
	if numPKs == 1 {
		return fmt.Sprintf("%s > ?", quotedSortedSrcPKs[0]), lastSrcPKValues, nil
	}

	// Row-value constructor untuk >1 PK
	whereTuple := fmt.Sprintf("(%s)", strings.Join(quotedSortedSrcPKs, ", "))
	placeholders := make([]string, numPKs)
	for i := range placeholders {
		placeholders[i] = "?"
	}
	placeholderTuple := fmt.Sprintf("(%s)", strings.Join(placeholders, ", "))
	return fmt.Sprintf("%s > %s", whereTuple, placeholderTuple), lastSrcPKValues, nil
}


// toggleForeignKeys dan revertFKsWithContext (tetap sama seperti sebelumnya)
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
		cmdToExecute = "SET SESSION foreign_key_checks = 0;"
		cmdToRevert = "SET SESSION foreign_key_checks = %v;"
		defaultInitialState = 1
	case "postgres":
		initialStateCmd = "SHOW session_replication_role;"
		cmdToExecute = "SET session_replication_role = replica;"
		cmdToRevert = "SET session_replication_role = '%s';"
		defaultInitialState = "origin"
		scopedLog.Warn("Using SET session_replication_role for PostgreSQL FK toggling. User needs superuser or REPLICATION attribute.")
	default:
		return nil, nil
	}

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
		} else {
			initialState = strings.TrimSpace(stateStr)
		}
	}

	if dialect == "mysql" {
		cmdToRevert = fmt.Sprintf(cmdToRevert, initialState.(int))
	} else {
		cmdToRevert = fmt.Sprintf(cmdToRevert, initialState.(string))
	}

	finalCmdToExecute := cmdToExecute
	if enable {
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

// Helper UnquoteIdentifier (perlu ditambahkan ke utils/quoting.go jika belum ada)
// Jika sudah ada, ini hanya contoh.
/*
func UnquoteIdentifier(name, dialect string) string {
    name = strings.TrimSpace(name)
    if len(name) < 2 {
        return name
    }
    first, last := name[0], name[len(name)-1]

    switch strings.ToLower(dialect) {
    case "mysql":
        if first == '`' && last == '`' {
            return strings.ReplaceAll(name[1:len(name)-1], "``", "`")
        }
    case "postgres", "sqlite": // SQLite juga sering menggunakan double quotes
        if first == '"' && last == '"' {
            return strings.ReplaceAll(name[1:len(name)-1], "\"\"", "\"")
        }
    }
    return name // Jika tidak di-quote dengan karakter yang dikenal untuk dialek itu
}
*/
