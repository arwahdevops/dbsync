// internal/sync/orchestrator_data_sync.go
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

	"github.com/arwahdevops/dbsync/internal/db"     // Diperlukan untuk db.Connector
	"github.com/arwahdevops/dbsync/internal/utils" // Diperlukan untuk utils.QuoteIdentifier
)

// syncData performs the actual data transfer using pagination.
// Ini adalah method dari Orchestrator.
func (f *Orchestrator) syncData(ctx context.Context, table string, pkColumns []string) (totalRowsSynced int64, batches int, err error) {
	log := f.logger.With(zap.String("table", table), zap.Strings("pk_columns_for_pagination", pkColumns))

	var totalRows int64 = -1
	countCtx, countCancel := context.WithTimeout(ctx, 15*time.Second)
	countErr := f.srcConn.DB.WithContext(countCtx).Table(table).Count(&totalRows).Error
	countCancel()

	if countErr != nil {
		logFields := []zap.Field{zap.Error(countErr)}
		if errors.Is(countErr, context.DeadlineExceeded) {
			logFields = append(logFields, zap.Duration("count_query_timeout", 15*time.Second))
			log.Warn("Counting total source rows timed out. Progress percentage will not be available.", logFields...)
		} else if errors.Is(ctx.Err(), context.Canceled) {
			logFields = append(logFields, zap.NamedError("main_context_error", ctx.Err()))
			log.Warn("Counting total source rows cancelled by main table context.", logFields...)
		} else if errors.Is(countErr, gorm.ErrRecordNotFound) {
			log.Debug("Source table appears empty based on Count() result.")
			totalRows = 0
		} else {
			log.Warn("Could not count total source rows for progress tracking due to DB error.", logFields...)
		}
	} else {
		log.Info("Approximate total source rows to synchronize", zap.Int64("count", totalRows))
	}

	var orderByClause string
	var quotedPKColumns, pkPlaceholders []string
	canPaginate := len(pkColumns) > 0

	if canPaginate {
		var sortErr error
		orderByClause, quotedPKColumns, pkPlaceholders, sortErr = f.buildPaginationClauses(pkColumns, f.srcConn.Dialect)
		if sortErr != nil {
			return 0, 0, fmt.Errorf("failed to build pagination clauses for table '%s': %w", table, sortErr)
		}
		log.Debug("Using pagination for data sync.", zap.String("order_by_clause", orderByClause))
	} else {
		log.Warn("No primary key provided/found for pagination. Attempting full table load in batches without explicit ordering for pagination. This is potentially unsafe and slow for very large tables, and data consistency during sync is not guaranteed if source data changes.")
		orderByClause = ""
	}

	var revertFKsFunc func() error
	if f.cfg.DisableFKDuringSync {
		var disableErr error
		revertFKsFunc, disableErr = f.toggleForeignKeys(ctx, f.dstConn, false, log)
		if disableErr != nil {
			log.Error("Attempt to disable foreign keys before data sync failed. Proceeding with FKs in current state.", zap.Error(disableErr))
		}
		if revertFKsFunc != nil {
			defer func() {
				log.Info("Attempting to re-enable/revert foreign keys after data sync completion/failure.")
				revertCtx, revertCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer revertCancel()
				if enableErr := f.revertFKsWithContext(revertCtx, revertFKsFunc); enableErr != nil {
					log.Error("Failed to re-enable/revert foreign keys after data sync.", zap.Error(enableErr))
				} else {
					log.Info("Foreign keys re-enabled/reverted successfully.")
				}
			}()
		}
	}

	var lastPKValues []interface{}
	totalRowsSynced = 0
	batches = 0
	progressLogThreshold := 100
	noDataStreak := 0

	for {
		if err := ctx.Err(); err != nil {
			log.Warn("Context cancelled or timed out during data sync loop for table.", zap.Error(err))
			return totalRowsSynced, batches, err
		}

		var batchData []map[string]interface{}
		query := f.srcConn.DB.WithContext(ctx).Table(table)

		if canPaginate && lastPKValues != nil {
			if len(quotedPKColumns) == 0 || len(pkPlaceholders) == 0 {
				return totalRowsSynced, batches, fmt.Errorf("internal error: pagination columns/placeholders not initialized correctly for table '%s'", table)
			}
			whereClause, args := f.buildWhereClause(quotedPKColumns, pkPlaceholders, lastPKValues)
			log.Debug("Applying WHERE clause for next page", zap.String("where_clause", whereClause), zap.Any("args", args))
			query = query.Where(whereClause, args...)
		}

		if orderByClause != "" {
			query = query.Order(orderByClause)
		}
		query = query.Limit(f.cfg.BatchSize)

		log.Debug("Fetching next batch from source database.", zap.Int("batch_limit", f.cfg.BatchSize))
		fetchStartTime := time.Now()
		fetchErr := query.Find(&batchData).Error
		fetchDuration := time.Since(fetchStartTime)

		if fetchErr != nil {
			log.Error("Failed to fetch data batch from source database.", zap.Error(fetchErr))
			return totalRowsSynced, batches, fmt.Errorf("failed to fetch data batch from source table '%s': %w", table, fetchErr)
		}

		rowsInCurrentBatch := len(batchData)
		log.Debug("Data batch fetched from source.", zap.Duration("fetch_duration_ms", fetchDuration.Round(time.Millisecond)), zap.Int("rows_in_batch", rowsInCurrentBatch))

		if rowsInCurrentBatch == 0 {
			if lastPKValues == nil && totalRowsSynced == 0 {
				log.Info("Source table appears to be empty or first fetch returned no data.")
			} else {
				log.Info("No more data found in source table for this page, or table synchronization complete.")
			}
			if canPaginate && lastPKValues != nil {
				noDataStreak++
				if noDataStreak >= 3 {
					log.Error("Potential pagination issue: Multiple consecutive empty batches received after processing some data. This might indicate an unstable sort order or issues with PK values if they are being modified during sync.",
						zap.Any("last_pk_values_before_empty_streak", lastPKValues))
					return totalRowsSynced, batches, fmt.Errorf("potential pagination issue: multiple empty batches for table '%s' after %d rows", table, totalRowsSynced)
				}
			}
			break
		}
		noDataStreak = 0

		log.Debug("Attempting to sync batch to destination database.", zap.Int("rows_to_sync_in_batch", rowsInCurrentBatch))
		syncBatchStartTime := time.Now()
		batchSyncErr := f.syncBatchWithRetry(ctx, table, batchData)
		syncBatchDuration := time.Since(syncBatchStartTime)
		log.Debug("Batch synchronization attempt to destination complete.", zap.Duration("sync_duration_ms", syncBatchDuration.Round(time.Millisecond)))

		if batchSyncErr != nil {
			return totalRowsSynced, batches, fmt.Errorf("failed to synchronize batch to destination table '%s': %w", table, batchSyncErr)
		}

		batches++
		totalRowsSynced += int64(rowsInCurrentBatch)
		f.metrics.RowsSyncedTotal.WithLabelValues(table).Add(float64(rowsInCurrentBatch))

		if canPaginate {
			lastRowInBatch := batchData[rowsInCurrentBatch-1]
			sortedPKNames, pkSortErr := f.getSortedPKNames(pkColumns)
			if pkSortErr != nil {
				return totalRowsSynced, batches, fmt.Errorf("failed to get sorted PK names for pagination update in table '%s': %w", table, pkSortErr)
			}

			newLastPKValues := make([]interface{}, len(sortedPKNames))
			pkValueFound := true
			missingPKName := ""
			for i, pkName := range sortedPKNames {
				val, ok := lastRowInBatch[pkName]
				if !ok {
					log.Error("Primary key column missing in fetched source data. Cannot reliably continue pagination.",
						zap.String("missing_pk_column_name", pkName),
						zap.Any("last_row_keys", func() []string {
							keys := make([]string, 0, len(lastRowInBatch))
							for k := range lastRowInBatch { keys = append(keys, k) }
							return keys
						}()),
					)
					pkValueFound = false
					missingPKName = pkName
					break
				}
				newLastPKValues[i] = val
			}
			if !pkValueFound {
				return totalRowsSynced, batches, fmt.Errorf("primary key column '%s' missing in fetched data for table '%s', pagination cannot continue", missingPKName, table)
			}
			lastPKValues = newLastPKValues
		} else {
			if totalRows > 0 && totalRowsSynced < totalRows {
				log.Warn("Full table load was expected (no pagination), but rows synced is less than total count. This might happen if source table changed during sync.",
					zap.Int64("rows_synced_no_pagination", totalRowsSynced),
					zap.Int64("expected_total_rows", totalRows))
			}
			break
		}

		if batches%progressLogThreshold == 0 || (totalRows >= 0 && totalRowsSynced >= totalRows && rowsInCurrentBatch > 0) {
			progressPercent := -1.0
			if totalRows > 0 {
				progressPercent = (float64(totalRowsSynced) / float64(totalRows)) * 100
			}
			log.Info("Data synchronization in progress...",
				zap.Int("current_batch_number", batches),
				zap.Int64("cumulative_rows_synced_so_far", totalRowsSynced),
				zap.Int64("approx_total_rows_in_source", totalRows),
				zap.Float64("approx_progress_percentage", progressPercent),
			)
		}
	}

	log.Info("Data synchronization finished for table.",
		zap.Int64("total_rows_successfully_synced", totalRowsSynced),
		zap.Int("total_batches_processed", batches),
	)
	return totalRowsSynced, batches, nil
}

// syncBatchWithRetry (Implementasi tetap sama seperti sebelumnya, pastikan menggunakan f.dstConn, f.cfg, f.logger, f.metrics)
func (f *Orchestrator) syncBatchWithRetry(ctx context.Context, table string, batch []map[string]interface{}) error {
	log := f.logger.With(zap.String("table", table), zap.Int("current_batch_size", len(batch)))
	if len(batch) == 0 {
		log.Debug("syncBatchWithRetry called with an empty batch, skipping operation.")
		return nil
	}

	var lastErrorEncountered error
	batchOpStartTime := time.Now()
	metricStatusLabel := "failure_unknown"

	defer func() {
		f.metrics.BatchProcessingDuration.WithLabelValues(table, metricStatusLabel).Observe(time.Since(batchOpStartTime).Seconds())
		if strings.HasPrefix(metricStatusLabel, "success") {
			f.metrics.BatchesProcessedTotal.WithLabelValues(table).Inc()
		} else {
			f.metrics.BatchErrorsTotal.WithLabelValues(table).Inc()
		}
	}()

	for attempt := 0; attempt <= f.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			log.Warn("Retrying batch insert/upsert operation.",
				zap.Int("attempt_number", attempt+1),
				zap.Int("max_attempts", f.cfg.MaxRetries+1),
				zap.Duration("wait_interval_before_retry", f.cfg.RetryInterval),
				zap.NamedError("previous_attempt_error", lastErrorEncountered),
			)
			retryTimer := time.NewTimer(f.cfg.RetryInterval)
			select {
			case <-retryTimer.C:
			case <-ctx.Done():
				retryTimer.Stop()
				log.Error("Context cancelled while waiting for batch insert/upsert retry.", zap.Error(ctx.Err()))
				metricStatusLabel = "failure_context_cancelled_retry"
				return fmt.Errorf("context cancelled waiting to retry batch insert for table '%s' (attempt %d): %w; last DB error: %v", table, attempt+1, ctx.Err(), lastErrorEncountered)
			}
		}

		transactionErr := f.dstConn.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			return tx.Table(table).Clauses(clause.OnConflict{UpdateAll: true}).CreateInBatches(batch, len(batch)).Error
		})

		if transactionErr == nil {
			metricStatusLabel = "success"
			if attempt > 0 {
				metricStatusLabel = "success_retry"
				log.Info("Batch insert/upsert succeeded after retry.", zap.Int("successful_attempt_number", attempt+1))
			} else {
				log.Debug("Batch insert/upsert succeeded on the first attempt.")
			}
			return nil
		}
		lastErrorEncountered = transactionErr

		if ctxErr := ctx.Err(); ctxErr != nil {
			log.Error("Context cancelled or timed out during/after batch transaction attempt.",
				zap.Error(ctxErr),
				zap.Int("attempt_number", attempt+1),
				zap.NamedError("transaction_db_error", lastErrorEncountered),
			)
			metricStatusLabel = "failure_context_cancelled_tx"
			return fmt.Errorf("context cancelled during batch insert for table '%s' (attempt %d): %w; last DB error: %v", table, attempt+1, ctxErr, lastErrorEncountered)
		}

		log.Warn("Batch insert/upsert attempt failed.",
			zap.Int("attempt_number", attempt+1),
			zap.Error(lastErrorEncountered),
		)
	}

	log.Error("Batch insert/upsert failed after exhausting all retry attempts.",
		zap.Int("total_attempts_made", f.cfg.MaxRetries+1),
		zap.NamedError("final_db_error", lastErrorEncountered),
	)
	metricStatusLabel = "failure_max_retries"
	return fmt.Errorf("failed to insert/upsert batch into table '%s' after %d retries: %w", table, f.cfg.MaxRetries, lastErrorEncountered)
}


// --- Helper Pagination (milik Orchestrator) ---
// (getSortedPKNames, buildPaginationClauses, buildWhereClause - implementasi sama seperti sebelumnya)
func (f *Orchestrator) getSortedPKNames(pkColumns []string) ([]string, error) {
	if len(pkColumns) == 0 {
		return []string{}, nil
	}
	sortedPKs := make([]string, len(pkColumns))
	copy(sortedPKs, pkColumns)
	sort.Strings(sortedPKs)
	return sortedPKs, nil
}

func (f *Orchestrator) buildPaginationClauses(pkColumns []string, dialect string) (orderBy string, quotedPKs []string, placeholders []string, err error) {
	if len(pkColumns) == 0 {
		err = fmt.Errorf("cannot build pagination clauses: no primary keys provided")
		return
	}
	sortedPKs, _ := f.getSortedPKNames(pkColumns)

	quotedPKs = make([]string, len(sortedPKs))
	placeholders = make([]string, len(sortedPKs))
	orderByParts := make([]string, len(sortedPKs))
	for i, pk := range sortedPKs {
		quotedPKs[i] = utils.QuoteIdentifier(pk, dialect)
		placeholders[i] = "?"
		orderByParts[i] = quotedPKs[i] + " ASC"
	}
	orderBy = strings.Join(orderByParts, ", ")
	return
}

func (f *Orchestrator) buildWhereClause(quotedSortedPKs []string, placeholders []string, lastPKValues []interface{}) (string, []interface{}) {
	if len(quotedSortedPKs) == 1 {
		return fmt.Sprintf("%s > ?", quotedSortedPKs[0]), lastPKValues
	}
	if len(quotedSortedPKs) != len(placeholders) || len(quotedSortedPKs) != len(lastPKValues) {
		f.logger.Error("Mismatch in counts for building composite PK WHERE clause. This indicates an internal logic error.",
			zap.Int("quoted_pk_count", len(quotedSortedPKs)),
			zap.Int("placeholders_count", len(placeholders)),
			zap.Int("values_count", len(lastPKValues)))
		if len(lastPKValues) > 0 && len(quotedSortedPKs) > 0 {
			return fmt.Sprintf("%s > ?", quotedSortedPKs[0]), []interface{}{lastPKValues[0]}
		}
		return "1=0", []interface{}{}
	}
	whereTuple := fmt.Sprintf("(%s)", strings.Join(quotedSortedPKs, ", "))
	placeholderTuple := fmt.Sprintf("(%s)", strings.Join(placeholders, ", "))
	whereClause := fmt.Sprintf("%s > %s", whereTuple, placeholderTuple)
	return whereClause, lastPKValues
}

// --- Helper FK Toggling (milik Orchestrator) ---
// (toggleForeignKeys, revertFKsWithContext - implementasi sama seperti sebelumnya)
func (f *Orchestrator) toggleForeignKeys(ctx context.Context, conn *db.Connector, enable bool, log *zap.Logger) (revertFunc func() error, err error) {
	if !f.cfg.DisableFKDuringSync && !enable {
		return nil, nil
	}

	var disableCmd, enableCmd, initialStateCmd, revertCmd string
	var initialState interface{} = nil
	var stateStr string
	var requiresTx bool = false

	dialect := conn.Dialect
	scopedLog := log.With(zap.String("target_dialect_for_fk_toggle", dialect))

	switch dialect {
	case "mysql":
		initialStateCmd = "SELECT @@FOREIGN_KEY_CHECKS"
		disableCmd = "SET FOREIGN_KEY_CHECKS = 0;"
		enableCmd = "SET FOREIGN_KEY_CHECKS = %v;"
	case "postgres":
		initialStateCmd = "SHOW session_replication_role"
		disableCmd = "SET session_replication_role = replica;"
		enableCmd = "SET session_replication_role = '%s';"
		scopedLog.Warn("Using SET session_replication_role for PostgreSQL FK toggling. Ensure the database user has appropriate permissions (superuser or REPLICATION attribute).")
	case "sqlite":
		initialStateCmd = "PRAGMA foreign_keys;"
		disableCmd = "PRAGMA foreign_keys = OFF;"
		enableCmd = "PRAGMA foreign_keys = %v;"
		scopedLog.Warn("Toggling foreign_keys via PRAGMA in SQLite affects only the current connection. Effectiveness with GORM's connection pool is NOT guaranteed and this feature is highly EXPERIMENTAL for SQLite.")
	default:
		scopedLog.Debug("Foreign key toggling is not supported for this dialect.")
		return nil, nil
	}

	if initialStateCmd != "" {
		readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
		errRead := conn.DB.WithContext(readCtx).Raw(initialStateCmd).Scan(&stateStr).Error
		readCancel()
		if errRead != nil {
			scopedLog.Warn("Could not read initial FK/replication state from database. Assuming default state for revert operation.", zap.Error(errRead))
			if dialect == "mysql" { initialState = 1 }
			if dialect == "postgres" { initialState = "origin" }
			if dialect == "sqlite" { initialState = 1 }
		} else {
			scopedLog.Info("Successfully read initial FK/replication state.", zap.String("retrieved_state", stateStr))
			if dialect == "mysql" || dialect == "sqlite" {
				val, convErr := strconv.Atoi(stateStr)
				if convErr == nil { initialState = val } else {
					scopedLog.Warn("Could not convert initial state string to integer, will use raw string for revert if possible or default.", zap.String("state_string", stateStr), zap.Error(convErr))
					initialState = stateStr
				}
			} else {
				initialState = stateStr
			}
		}
	} else {
		scopedLog.Warn("No command defined to read initial FK/replication state. Assuming default state for revert operation.")
		if dialect == "mysql" { initialState = 1 }
		if dialect == "postgres" { initialState = "origin" }
		if dialect == "sqlite" { initialState = 1 }
	}

	if dialect == "mysql" || dialect == "sqlite" {
		stateToUse := 1
		if stateInt, ok := initialState.(int); ok {
			stateToUse = stateInt
		} else if stateStrConv, ok := initialState.(string); ok {
			parsedInt, errConv := strconv.Atoi(stateStrConv)
			if errConv == nil { stateToUse = parsedInt }
		}
		enableCmd = fmt.Sprintf(enableCmd, stateToUse)
	} else {
		stateToUse := "origin"
		if stateStrConv, ok := initialState.(string); ok && stateStrConv != "" {
			stateToUse = stateStrConv
		}
		enableCmd = fmt.Sprintf(enableCmd, stateToUse)
	}

	var commandToExecute string
	var actionDescription string
	if enable {
		commandToExecute = enableCmd
		revertCmd = disableCmd
		actionDescription = "Enabling (reverting) foreign key checks/triggers"
	} else {
		commandToExecute = disableCmd
		revertCmd = enableCmd
		actionDescription = "Disabling foreign key checks/triggers"
	}

	scopedLog.Info(actionDescription, zap.String("command_to_execute", commandToExecute))
	execCtx, execCancel := context.WithTimeout(ctx, 10*time.Second)
	var executionError error
	if requiresTx {
		executionError = conn.DB.WithContext(execCtx).Transaction(func(tx *gorm.DB) error {
			return tx.Exec(commandToExecute).Error
		})
	} else {
		executionError = conn.DB.WithContext(execCtx).Exec(commandToExecute).Error
	}
	execCancel()

	if executionError != nil {
		scopedLog.Error("Failed to execute command for toggling FKs. Data sync will proceed with FKs in their current (potentially problematic) state.",
			zap.String("executed_command", commandToExecute),
			zap.Error(executionError))
		return nil, fmt.Errorf("failed to execute FK toggle command '%s': %w", commandToExecute, executionError)
	}

	revertFunc = func() error {
		revertInternalCtx, revertInternalCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer revertInternalCancel()
		scopedLog.Info("Executing revert command for foreign key checks/triggers.", zap.String("revert_command_to_execute", revertCmd))
		var revertExecutionError error
		if requiresTx {
			revertExecutionError = conn.DB.WithContext(revertInternalCtx).Transaction(func(tx *gorm.DB) error {
				return tx.Exec(revertCmd).Error
			})
		} else {
			revertExecutionError = conn.DB.WithContext(revertInternalCtx).Exec(revertCmd).Error
		}
		if revertExecutionError != nil {
			scopedLog.Error("Failed to execute revert command for FKs. Database FK state might be inconsistent.",
				zap.String("failed_revert_command", revertCmd),
				zap.Error(revertExecutionError))
			return fmt.Errorf("failed to execute FK revert command '%s': %w", revertCmd, revertExecutionError)
		}
		return nil
	}
	return revertFunc, nil
}

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
		f.logger.Error("Context timed out or cancelled while waiting for FK revert function to complete. FK state might be inconsistent.", zap.Error(ctx.Err()))
		return fmt.Errorf("reverting FKs was cancelled by external context: %w", ctx.Err())
	}
}
