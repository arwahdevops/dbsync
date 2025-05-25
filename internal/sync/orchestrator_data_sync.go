// internal/sync/orchestrator_data_sync.go
package sync

import (
	"context"
	"errors" // Diperlukan untuk errors.Is
	"fmt"
	"sort" // Diperlukan untuk buildPaginationOrderBy
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"        // Diperlukan untuk gorm.ErrRecordNotFound jika digunakan, atau operasi Transaction
	"gorm.io/gorm/clause" // Diperlukan untuk clause.OnConflict dll.

	"github.com/arwahdevops/dbsync/internal/db"    // Diperlukan untuk tipe db.Connector
	"github.com/arwahdevops/dbsync/internal/utils" // Diperlukan untuk QuoteIdentifier
)

// Definisi processTableInput dan processTableResult ada di syncer_types.go
// Definisi Orchestrator struct ada di orchestrator.go

// getDestinationColumnTypes mengambil tipe kolom dari tabel tujuan.
// Ini adalah method dari Orchestrator.
func (f *Orchestrator) getDestinationColumnTypes(ctx context.Context, tableName string) (map[string]string, error) {
	log := f.logger.With(zap.String("table", tableName), zap.String("dialect", f.dstConn.Dialect), zap.String("action", "getDestinationColumnTypes"))
	colTypes := make(map[string]string)

	var columnsData []struct {
		ColumnName string `gorm:"column:column_name"`
		DataType   string `gorm:"column:data_type"`
		UdtName    string `gorm:"column:udt_name"` // Untuk PostgreSQL
	}

	// Gunakan f.dstConn.DB untuk query
	dbGorm := f.dstConn.DB.WithContext(ctx)

	switch f.dstConn.Dialect {
	case "postgres":
		query := `
            SELECT column_name, data_type, udt_name
            FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = $1;`
		if err := dbGorm.Raw(query, tableName).Scan(&columnsData).Error; err != nil {
			log.Error("Failed to fetch destination column types for PostgreSQL", zap.Error(err))
			return nil, fmt.Errorf("fetch postgres column types for %s: %w", tableName, err)
		}
		for _, c := range columnsData {
			normType := normalizeTypeName(c.DataType) // from compare_helpers.go
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
		if err := dbGorm.Raw(query, tableName).Scan(&columnsData).Error; err != nil {
			log.Error("Failed to fetch destination column types for MySQL", zap.Error(err))
			return nil, fmt.Errorf("fetch mysql column types for %s: %w", tableName, err)
		}
		for _, c := range columnsData {
			colTypes[c.ColumnName] = normalizeTypeName(c.DataType) // from compare_helpers.go
		}
	default:
		log.Debug("Destination column type fetching not implemented for dialect, data transformation for booleans/specific types might not occur.",
			zap.String("dialect", f.dstConn.Dialect))
	}
	if len(colTypes) > 0 {
		log.Debug("Fetched destination column types.", zap.Any("types_map", colTypes))
	} else {
		log.Debug("No destination column types fetched (table might not exist yet or dialect not supported for this specific fetch).")
	}
	return colTypes, nil
}

// syncData melakukan transfer data dari sumber ke tujuan menggunakan paginasi.
// Ini adalah method dari Orchestrator.
func (f *Orchestrator) syncData(ctx context.Context, table string, srcPKColumns []string) (totalRowsSynced int64, batches int, err error) {
	log := f.logger.With(zap.String("table", table), zap.Strings("source_pk_columns_for_pagination", srcPKColumns))
	log.Info("Starting data synchronization for table.")

	var totalRows int64 = -1
	countCtx, countCancel := context.WithTimeout(ctx, 15*time.Second)
	countErr := f.srcConn.DB.WithContext(countCtx).Table(table).Count(&totalRows).Error
	countCancel()

	if countErr != nil {
		logFields := []zap.Field{zap.Error(countErr)}
		if errors.Is(ctx.Err(), context.Canceled) { // Periksa context utama
			logFields = append(logFields, zap.NamedError("main_context_error", ctx.Err()))
		}
		if errors.Is(countCtx.Err(), context.DeadlineExceeded) {
			logFields = append(logFields, zap.NamedError("count_context_error_deadline", countCtx.Err()))
		} else if countCtx.Err() != nil { // Error context lain
			logFields = append(logFields, zap.NamedError("count_context_error_other", countCtx.Err()))
		}
		log.Warn("Could not count total source rows for progress tracking. Progress percentage will not be available.", logFields...)
	} else if totalRows == 0 {
		log.Info("Source table is empty. No data to synchronize.")
		return 0, 0, nil
	} else {
		log.Info("Approximate total source rows to synchronize for progress display.", zap.Int64("approx_source_row_count", totalRows))
	}

	var dstPKColumns []string
	var dstColumnTypes map[string]string
	var errSchemaInfo error

	schemaInfoCtx, schemaInfoCancel := context.WithTimeout(ctx, 25*time.Second)
	defer schemaInfoCancel()

	dstPKColumns, errSchemaInfo = f.getDestinationTablePKs(schemaInfoCtx, table) // Method orchestrator
	if errSchemaInfo != nil {
		log.Error("Failed to get destination primary keys for upsert. Upsert might default to UpdateAll or fail if PKs are required by dialect.", zap.Error(errSchemaInfo))
	} else if len(dstPKColumns) == 0 {
		log.Warn("No primary key found for destination table. ON CONFLICT upsert will use GORM's UpdateAll or dialect's default behavior for tables without PKs, which might be inefficient or fail if the table has no other updatable columns.",
			zap.String("table", table))
	}

	if f.dstConn.Dialect == "postgres" || f.dstConn.Dialect == "mysql" {
		dstColumnTypes, errSchemaInfo = f.getDestinationColumnTypes(schemaInfoCtx, table) // Method orchestrator
		if errSchemaInfo != nil {
			log.Error("Failed to get destination column types. Data transformation (e.g., boolean) might not occur as expected.", zap.Error(errSchemaInfo))
		}
	}

	var orderByClause string
	var quotedSrcPKColumnsForOrderBy []string
	canPaginate := len(srcPKColumns) > 0

	if canPaginate {
		var sortErr error
		orderByClause, quotedSrcPKColumnsForOrderBy, sortErr = f.buildPaginationOrderBy(srcPKColumns, f.srcConn.Dialect) // Method orchestrator
		if sortErr != nil {
			return 0, 0, fmt.Errorf("failed to build source pagination order by clause for table '%s': %w", table, sortErr)
		}
		log.Debug("Using pagination for data sync.",
			zap.String("order_by_clause", orderByClause),
			zap.Strings("quoted_src_pk_cols_for_order_by", quotedSrcPKColumnsForOrderBy))
	} else {
		log.Warn("No source primary key provided/found for pagination. Attempting full table load in batches without explicit ordering for pagination. This is unsafe for large tables and may lead to missed or duplicated rows if data changes during sync.")
		orderByClause = ""
	}

	var revertFKsFunc func() error
	if f.cfg.DisableFKDuringSync && (f.dstConn.Dialect == "mysql" || f.dstConn.Dialect == "postgres") {
		var disableErr error
		revertFKsFunc, disableErr = f.toggleForeignKeys(ctx, f.dstConn, false, log) // Method orchestrator
		if disableErr != nil {
			log.Error("Attempt to disable foreign keys before data sync failed. Proceeding with FKs in current state (this might slow down inserts or cause FK violation errors).", zap.Error(disableErr))
		} else if revertFKsFunc != nil {
			log.Info("Foreign keys/triggers successfully disabled/set to replica mode on destination.")
			defer func() {
				log.Info("Attempting to re-enable/revert foreign keys/triggers on destination after data sync completion or failure.")
				revertCtx, revertCancel := context.WithTimeout(context.Background(), 45*time.Second)
				defer revertCancel()
				if enableErr := f.revertFKsWithContext(revertCtx, revertFKsFunc); enableErr != nil { // Method orchestrator
					log.Error("Failed to re-enable/revert foreign keys/triggers on destination after data sync. Manual check might be required.", zap.Error(enableErr))
				} else {
					log.Info("Foreign keys/triggers re-enabled/reverted successfully on destination.")
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
			log.Warn("Context cancelled or timed out during data sync loop for table. Aborting data sync for this table.", zap.Error(errCtx))
			return totalRowsSynced, batches, errCtx
		}

		var batchData []map[string]interface{}
		query := f.srcConn.DB.WithContext(ctx).Table(table)

		if canPaginate && lastSrcPKValues != nil {
			whereClause, args, whereErr := f.buildWhereClause(quotedSrcPKColumnsForOrderBy, lastSrcPKValues, f.srcConn.Dialect) // Method orchestrator
			if whereErr != nil {
				return totalRowsSynced, batches, fmt.Errorf("failed to build WHERE clause for pagination on table '%s': %w", table, whereErr)
			}
			query = query.Where(whereClause, args...)
		}

		if orderByClause != "" {
			query = query.Order(orderByClause)
		}
		query = query.Limit(f.cfg.BatchSize)

		log.Debug("Fetching next batch from source database.",
			zap.Int("batch_limit", f.cfg.BatchSize),
			zap.Any("last_pk_values_for_where_clause", lastSrcPKValues))

		fetchStartTime := time.Now()
		fetchErr := query.Find(&batchData).Error
		fetchDuration := time.Since(fetchStartTime)

		if fetchErr != nil {
			log.Error("Failed to fetch data batch from source database.", zap.Error(fetchErr))
			return totalRowsSynced, batches, fmt.Errorf("fetch data batch from source table '%s': %w", table, fetchErr)
		}

		rowsInCurrentBatch := len(batchData)
		log.Debug("Data batch fetched from source.",
			zap.Duration("fetch_duration_ms", fetchDuration.Round(time.Millisecond)),
			zap.Int("rows_in_batch", rowsInCurrentBatch))

		if rowsInCurrentBatch == 0 {
			if totalRowsSynced == 0 && lastSrcPKValues == nil {
				log.Info("Source table appears to be empty, or first fetch returned no data. Data sync for this table complete.")
			} else {
				log.Info("No more data found in source for this pagination page, or sync complete for table.")
			}
			if canPaginate && lastSrcPKValues != nil {
				noDataStreak++
				if noDataStreak >= 2 {
					log.Warn("Potential pagination issue: Multiple consecutive empty batches despite having previous PK values. This might indicate inconsistent data sorting, data deletion during sync, or a bug. Assuming sync is complete for this table.",
						zap.Any("last_src_pk_values_before_empty_batches", lastSrcPKValues))
				}
			}
			break
		}
		noDataStreak = 0

		if (f.dstConn.Dialect == "postgres" || f.dstConn.Dialect == "mysql") && dstColumnTypes != nil && len(dstColumnTypes) > 0 {
			for i, row := range batchData {
				transformedRow := make(map[string]interface{}, len(row))
				for key, val := range row {
					targetType, typeKnown := dstColumnTypes[key]
					if typeKnown {
						if f.dstConn.Dialect == "postgres" && targetType == "boolean" {
							switch v := val.(type) {
							case int, int8, int16, int32, int64:
								numVal, _ := strconv.ParseInt(fmt.Sprintf("%d", v), 10, 64)
								transformedRow[key] = (numVal == 1)
							case float32, float64:
								numVal, _ := strconv.ParseFloat(fmt.Sprintf("%f", v), 64)
								transformedRow[key] = (numVal == 1.0)
							case bool:
								transformedRow[key] = v
							case string:
								sVal := strings.ToLower(strings.TrimSpace(v))
								if sVal == "1" || sVal == "true" || sVal == "t" || sVal == "yes" || sVal == "on" {
									transformedRow[key] = true
								} else if sVal == "0" || sVal == "false" || sVal == "f" || sVal == "no" || sVal == "off" {
									transformedRow[key] = false
								} else {
									transformedRow[key] = val
								}
							default:
								transformedRow[key] = val
							}
							continue
						} else if f.dstConn.Dialect == "mysql" && targetType == "tinyint" {
							if boolVal, isBool := val.(bool); isBool {
								if boolVal {
									transformedRow[key] = 1
								} else {
									transformedRow[key] = 0
								}
								continue
							}
						}
					}
					transformedRow[key] = val
				}
				batchData[i] = transformedRow
			}
		}

		log.Debug("Attempting to sync batch to destination database.", zap.Int("rows_to_sync_in_batch", rowsInCurrentBatch))
		syncBatchStartTime := time.Now()
		batchSyncErr := f.syncBatchWithRetry(ctx, table, batchData, dstPKColumns) // Method orchestrator
		syncBatchDuration := time.Since(syncBatchStartTime)
		log.Debug("Batch synchronization attempt to destination complete.",
			zap.Duration("sync_duration_ms", syncBatchDuration.Round(time.Millisecond)))

		if batchSyncErr != nil {
			return totalRowsSynced, batches, fmt.Errorf("sync batch to destination table '%s': %w", table, batchSyncErr)
		}

		batches++
		totalRowsSynced += int64(rowsInCurrentBatch)
		f.metrics.RowsSyncedTotal.WithLabelValues(table).Add(float64(rowsInCurrentBatch))

		if canPaginate {
			lastRowInBatch := batchData[rowsInCurrentBatch-1]
			newLastSrcPKValues := make([]interface{}, len(srcPKColumns))
			allPKsFound := true
			for i, pkNameUnquoted := range srcPKColumns {
				val, ok := lastRowInBatch[pkNameUnquoted]
				if !ok {
					log.Error("Source PK column missing in fetched data from source, pagination cannot continue reliably. This is unexpected.",
						zap.String("missing_pk_unquoted", pkNameUnquoted),
						zap.Strings("all_source_pks_expected_in_order", srcPKColumns))
					allPKsFound = false
					break
				}
				newLastSrcPKValues[i] = val
			}
			if !allPKsFound {
				return totalRowsSynced, batches, fmt.Errorf("source PK column ('%s') missing in fetched data for table '%s', pagination failed", strings.Join(srcPKColumns, ","), table)
			}
			lastSrcPKValues = newLastSrcPKValues
		} else {
			if rowsInCurrentBatch < f.cfg.BatchSize {
				break
			}
		}

		if batches%progressLogThreshold == 0 || (totalRows > 0 && totalRowsSynced >= totalRows && totalRowsSynced > 0) {
			progressPercent := -1.0
			if totalRows > 0 {
				progressPercent = (float64(totalRowsSynced) / float64(totalRows)) * 100
			}
			log.Info("Data sync in progress for table...",
				zap.Int("batch_number_completed", batches),
				zap.Int64("rows_synced_so_far_for_table", totalRowsSynced),
				zap.Int64("approx_total_source_rows_for_table", totalRows),
				zap.Float64("progress_percentage", progressPercent))
		}
	}

	log.Info("Data synchronization finished for table.",
		zap.Int64("total_rows_synced_for_table", totalRowsSynced),
		zap.Int("total_batches_processed_for_table", batches))
	return totalRowsSynced, batches, nil
}

// syncBatchWithRetry adalah method dari Orchestrator.
func (f *Orchestrator) syncBatchWithRetry(ctx context.Context, table string, batch []map[string]interface{}, dstPKColumns []string) error {
	log := f.logger.With(zap.String("table", table), zap.Int("batch_size_for_upsert", len(batch)))
	if len(batch) == 0 {
		log.Debug("Empty batch received for upsert, skipping.")
		return nil
	}

	var lastErr error
	startTime := time.Now()
	metricStatus := "failure_unknown"

	defer func() {
		f.metrics.BatchProcessingDuration.WithLabelValues(table, metricStatus).Observe(time.Since(startTime).Seconds())
		if strings.HasPrefix(metricStatus, "success") {
			f.metrics.BatchesProcessedTotal.WithLabelValues(table).Inc()
		} else if !strings.Contains(metricStatus, "context_cancelled") {
			f.metrics.BatchErrorsTotal.WithLabelValues(table).Inc()
		}
	}()

	var conflictClause clause.Expression
	if len(dstPKColumns) > 0 {
		conflictTargetCols := make([]clause.Column, len(dstPKColumns))
		for i, pk := range dstPKColumns {
			conflictTargetCols[i] = clause.Column{Name: pk}
		}

		var colsToUpdateOnConflict []string
		if len(batch) > 0 {
			firstRow := batch[0]
			isDstPKMap := make(map[string]bool, len(dstPKColumns))
			for _, pk := range dstPKColumns {
				isDstPKMap[pk] = true
			}
			for colName := range firstRow {
				if !isDstPKMap[colName] {
					colsToUpdateOnConflict = append(colsToUpdateOnConflict, colName)
				}
			}
		}

		if len(colsToUpdateOnConflict) > 0 {
			log.Debug("Building ON CONFLICT DO UPDATE SET for specified non-PK columns.",
				zap.Strings("conflict_target_pk_cols", dstPKColumns),
				zap.Strings("update_cols_on_conflict", colsToUpdateOnConflict))
			conflictClause = clause.OnConflict{
				Columns:   conflictTargetCols,
				DoUpdates: clause.AssignmentColumns(colsToUpdateOnConflict),
			}
		} else {
			log.Info("No non-PK columns found in batch to update on conflict, or only PKs are being synced. Using ON CONFLICT DO NOTHING.",
				zap.Strings("conflict_target_pk_cols", dstPKColumns))
			conflictClause = clause.OnConflict{
				Columns:   conflictTargetCols,
				DoNothing: true,
			}
		}
	} else {
		log.Warn("Destination PKs not specified for ON CONFLICT clause; GORM will use UpdateAll or dialect default behavior. This might be inefficient, cause 'model value required' errors if no updatable columns are found by GORM, or insert duplicates if the table truly has no PK/UQ.",
			zap.String("dialect", f.dstConn.Dialect))
		conflictClause = clause.OnConflict{UpdateAll: true}
	}

	for attempt := 0; attempt <= f.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			log.Warn("Retrying batch insert/upsert.",
				zap.Int("attempt_number", attempt+1),
				zap.Int("max_attempts", f.cfg.MaxRetries+1),
				zap.NamedError("previous_batch_error", lastErr))
			select {
			case <-time.After(f.cfg.RetryInterval):
			case <-ctx.Done():
				log.Error("Context cancelled while waiting for batch retry.", zap.Error(ctx.Err()))
				metricStatus = "failure_context_cancelled_retry_wait"
				return fmt.Errorf("context cancelled on retry wait for table '%s': %w; last DB error: %v", table, ctx.Err(), lastErr)
			}
		}

		txErr := f.dstConn.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			return tx.Table(table).Clauses(conflictClause).CreateInBatches(batch, len(batch)).Error
		})

		if txErr == nil {
			metricStatus = "success"
			if attempt > 0 {
				metricStatus = "success_retry"
			}
			log.Debug("Batch upsert successful.", zap.Int("attempt_number", attempt+1))
			return nil
		}
		lastErr = txErr

		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			log.Error("Context cancelled or timed out during/after batch transaction attempt.",
				zap.Error(ctx.Err()), zap.NamedError("transaction_error_before_context_check", lastErr))
			metricStatus = "failure_context_cancelled_tx_attempt"
			return fmt.Errorf("context cancelled/timed out during batch tx for table '%s': %w; last DB error: %v", table, ctx.Err(), lastErr)
		}
		log.Warn("Batch upsert attempt failed.", zap.Int("attempt_number", attempt+1), zap.Error(lastErr))
	}

	log.Error("Batch upsert failed after all retries.", zap.NamedError("final_batch_error", lastErr))
	metricStatus = "failure_max_retries"
	return fmt.Errorf("upsert batch to table '%s' failed after %d retries: %w", table, f.cfg.MaxRetries+1, lastErr)
}

// buildPaginationOrderBy adalah method dari Orchestrator.
func (f *Orchestrator) buildPaginationOrderBy(srcPKColumnsUnsorted []string, dialect string) (orderByClause string, quotedSortedPKs []string, err error) {
	if len(srcPKColumnsUnsorted) == 0 {
		err = fmt.Errorf("no source primary keys provided for pagination")
		return
	}

	pksToUse := make([]string, len(srcPKColumnsUnsorted))
	copy(pksToUse, srcPKColumnsUnsorted)
	sort.Strings(pksToUse)

	quotedSortedPKs = make([]string, len(pksToUse))
	orderByParts := make([]string, len(pksToUse))

	for i, pk := range pksToUse {
		quotedSortedPKs[i] = utils.QuoteIdentifier(pk, dialect)
		orderByParts[i] = quotedSortedPKs[i] + " ASC"
	}
	orderByClause = strings.Join(orderByParts, ", ")
	return
}

// buildWhereClause adalah method dari Orchestrator.
func (f *Orchestrator) buildWhereClause(quotedSortedSrcPKs []string, lastSrcPKValues []interface{}, srcDialect string) (string, []interface{}, error) {
	numPKs := len(quotedSortedSrcPKs)
	if numPKs == 0 {
		return "1=1", []interface{}{}, nil
	}
	if numPKs != len(lastSrcPKValues) {
		return "", nil, fmt.Errorf("mismatch between number of PK columns (%d) and PK values (%d) for WHERE clause", numPKs, len(lastSrcPKValues))
	}

	if srcDialect == "sqlite" && numPKs > 1 {
		var conditions []string
		var args []interface{}
		for i := 0; i < numPKs; i++ {
			var currentLevelConditions []string
			for j := 0; j < i; j++ {
				currentLevelConditions = append(currentLevelConditions, fmt.Sprintf("%s = ?", quotedSortedSrcPKs[j]))
				args = append(args, lastSrcPKValues[j])
			}
			currentLevelConditions = append(currentLevelConditions, fmt.Sprintf("%s > ?", quotedSortedSrcPKs[i]))
			args = append(args, lastSrcPKValues[i])
			conditions = append(conditions, "("+strings.Join(currentLevelConditions, " AND ")+")")
		}
		return strings.Join(conditions, " OR "), args, nil
	}

	if numPKs == 1 {
		return fmt.Sprintf("%s > ?", quotedSortedSrcPKs[0]), lastSrcPKValues, nil
	}

	whereTuple := fmt.Sprintf("(%s)", strings.Join(quotedSortedSrcPKs, ", "))
	placeholders := make([]string, numPKs)
	for i := range placeholders {
		placeholders[i] = "?"
	}
	placeholderTuple := fmt.Sprintf("(%s)", strings.Join(placeholders, ", "))
	return fmt.Sprintf("%s > %s", whereTuple, placeholderTuple), lastSrcPKValues, nil
}

// toggleForeignKeys adalah method dari Orchestrator.
func (f *Orchestrator) toggleForeignKeys(ctx context.Context, conn *db.Connector, enable bool, log *zap.Logger) (revertFunc func() error, err error) {
	scopedLog := log.With(zap.String("target_dialect_for_fk_toggle", conn.Dialect))

	if conn.Dialect != "mysql" && conn.Dialect != "postgres" {
		scopedLog.Debug("Foreign key toggling skipped: not supported for dialect or not needed.", zap.String("dialect", conn.Dialect))
		return nil, nil
	}

	var cmdToExecute, cmdToRevert, initialStateCmd string
	var initialStateReadStr string
	var defaultInitialStateString string

	switch conn.Dialect {
	case "mysql":
		initialStateCmd = "SELECT @@SESSION.foreign_key_checks;"
		cmdToExecute = "SET SESSION foreign_key_checks = 0;"
		cmdToRevert = "SET SESSION foreign_key_checks = %s;"
		defaultInitialStateString = "1"
	case "postgres":
		initialStateCmd = "SHOW session_replication_role;"
		cmdToExecute = "SET session_replication_role = replica;"
		cmdToRevert = "SET session_replication_role = '%s';"
		defaultInitialStateString = "origin"
		scopedLog.Warn("Using SET session_replication_role for PostgreSQL FK toggling. User needs superuser or REPLICATION attribute for this operation.")
	default: // Should be caught by the initial check
		return nil, fmt.Errorf("toggleForeignKeys called with unsupported dialect: %s", conn.Dialect)
	}

	// Baca state awal menggunakan conn.DB
	readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
	// Penting: Gunakan conn.DB di sini, bukan f.dstConn.DB atau f.srcConn.DB secara langsung
	// karena `conn` adalah parameter yang dilewatkan yang bisa jadi src atau dst.
	errRead := conn.DB.WithContext(readCtx).Raw(initialStateCmd).Scan(&initialStateReadStr).Error
	readCancel()

	actualInitialStateString := defaultInitialStateString
	if errRead != nil {
		scopedLog.Warn("Could not read initial FK/replication state. Assuming default for revert.",
			zap.Error(errRead), zap.String("assumed_default_state", defaultInitialStateString))
	} else {
		actualInitialStateString = strings.TrimSpace(initialStateReadStr)
		scopedLog.Info("Successfully read initial FK/replication state.", zap.String("retrieved_state", actualInitialStateString))
	}

	finalCmdToRevert := fmt.Sprintf(cmdToRevert, actualInitialStateString)
	finalCmdToExecute := cmdToExecute
	description := "Disabling FKs/triggers"
	if enable {
		finalCmdToExecute = finalCmdToRevert
		description = fmt.Sprintf("Re-enabling/Reverting FKs/triggers to state '%s'", actualInitialStateString)
	}

	scopedLog.Info(description, zap.String("command_to_execute", finalCmdToExecute))
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
		scopedLog.Info("Executing revert command for FKs/triggers.", zap.String("revert_command_to_execute", finalCmdToRevert))
		if rErr := conn.DB.WithContext(rCtx).Exec(finalCmdToRevert).Error; rErr != nil {
			scopedLog.Error("Failed to execute revert command for FKs.", zap.Error(rErr))
			return fmt.Errorf("FK revert command '%s' failed: %w", finalCmdToRevert, rErr)
		}
		return nil
	}
	return revertFunc, nil
}

// revertFKsWithContext adalah method dari Orchestrator.
func (f *Orchestrator) revertFKsWithContext(ctx context.Context, revertFunc func() error) error {
	if revertFunc == nil {
		return nil
	}
	doneChan := make(chan error, 1)
	go func() {
		doneChan <- revertFunc()
	}()

	select {
	case err := <-doneChan:
		return err
	case <-ctx.Done():
		f.logger.Error("Context timed out/cancelled while waiting for FK revert operation to complete.", zap.Error(ctx.Err()))
		return fmt.Errorf("reverting FKs cancelled/timed out by context: %w", ctx.Err())
	}
}
