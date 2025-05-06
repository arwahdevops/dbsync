package sync

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/arwahdevops/dbsync/internal/config"
	"github.com/arwahdevops/dbsync/internal/db"
	"github.com/arwahdevops/dbsync/internal/metrics" // Import metrics
	"github.com/arwahdevops/dbsync/internal/utils"   // Import utils
)

type FullSync struct {
	srcConn      *db.Connector
	dstConn      *db.Connector
	cfg          *config.Config
	logger       *zap.Logger
	schemaSyncer SchemaSyncerInterface // Use interface
	metrics      *metrics.Store        // Add metrics store
}

type SyncResult struct {
	Table             string
	SchemaSyncSkipped bool // True if schema sync was skipped (none strategy or other reason)
	SchemaError       error // Error during schema analysis or DDL execution
	DataError         error // Error during data sync
	ConstraintError   error // Error applying constraints/indexes post-data-sync
	RowsSynced        int64
	Batches           int
	Duration          time.Duration
	Skipped           bool // True if table processing was skipped entirely (system table, no PK, config skip)
}

// Ensure FullSync implements the interface
var _ FullSyncInterface = (*FullSync)(nil)

func NewFullSync(srcConn, dstConn *db.Connector, cfg *config.Config, logger *zap.Logger, metricsStore *metrics.Store) *FullSync {
	return &FullSync{
		srcConn:     srcConn,
		dstConn:     dstConn,
		cfg:         cfg,
		logger:      logger.Named("full-sync"),
		schemaSyncer: NewSchemaSyncer( // Inject concrete implementation
			srcConn.DB,
			dstConn.DB,
			srcConn.Dialect,
			dstConn.Dialect,
			logger,
		),
		metrics: metricsStore, // Inject metrics store
	}
}

func (f *FullSync) Run(ctx context.Context) map[string]SyncResult {
	startTime := time.Now()
	f.logger.Info("Starting full database synchronization run",
		zap.String("direction", f.cfg.SyncDirection),
		zap.Int("workers", f.cfg.Workers),
		zap.Int("batch_size", f.cfg.BatchSize),
		zap.String("schema_strategy", string(f.cfg.SchemaSyncStrategy)),
	)
	f.metrics.SyncRunning.Set(1) // Indicate sync is running
	defer f.metrics.SyncRunning.Set(0) // Indicate sync finished

	results := make(map[string]SyncResult)
	tables, err := f.listTables(ctx)
	if err != nil {
		f.logger.Error("Failed to list source tables", zap.Error(err))
		f.metrics.SyncErrorsTotal.WithLabelValues("list_tables", "").Inc()
		return results
	}

	if len(tables) == 0 {
		f.logger.Warn("No tables found in source database to synchronize")
		return results
	}

	f.logger.Info("Found tables to synchronize", zap.Int("count", len(tables)), zap.Strings("tables", tables))

	var wg sync.WaitGroup
	resultChan := make(chan SyncResult, len(tables))
	sem := make(chan struct{}, f.cfg.Workers) // Semaphore for workers

	for i, tableName := range tables {
		select {
		case <-ctx.Done():
			remainingTables := tables[i:]
			f.logger.Warn("Context cancelled before starting sync for remaining tables",
				zap.String("first_skipped_table", tableName),
				zap.Int("remaining_count", len(remainingTables)),
			)
			// Fill results for remaining tables as skipped
			for _, tbl := range remainingTables {
				results[tbl] = SyncResult{Table: tbl, Skipped: true, SchemaError: ctx.Err()} // Use SchemaError to indicate cancel reason
			}
			goto endloop // Use goto to break outer loop cleanly
		default:
			// Proceed
		}

		wg.Add(1)
		go func(tbl string) {
			defer wg.Done() // Ensure Done is called

			// Acquire worker slot or handle cancellation
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }() // Release slot when done
			case <-ctx.Done():
				f.logger.Warn("Context cancelled while waiting for worker slot", zap.String("table", tbl))
				resultChan <- SyncResult{Table: tbl, Skipped: true, SchemaError: ctx.Err()}
				return
			}

			log := f.logger.With(zap.String("table", tbl))
			start := time.Now()
			result := SyncResult{Table: tbl}
			syncCompletedWithoutError := false // Track if all steps finished ok

			// Defer sending result and recording metrics
			defer func() {
				result.Duration = time.Since(start)
				// Observe duration regardless of success/failure
				f.metrics.TableSyncDuration.WithLabelValues(tbl).Observe(result.Duration.Seconds())

				// Log final status for the table
				if syncCompletedWithoutError {
					log.Info("Table sync finished successfully",
						zap.Duration("duration", result.Duration),
						zap.Int64("rows_synced", result.RowsSynced),
						zap.Int("batches", result.Batches),
					)
					f.metrics.TableSyncSuccessTotal.WithLabelValues(tbl).Inc()
				} else if !result.Skipped { // Log failure details if not skipped for other reasons
					log.Error("Table sync finished with errors",
						zap.Duration("duration", result.Duration),
						zap.NamedError("schema_error", result.SchemaError),
						zap.NamedError("data_error", result.DataError),
						zap.NamedError("constraint_error", result.ConstraintError),
					)
				} else {
					log.Warn("Table sync was skipped",
						zap.Duration("duration", result.Duration),
						zap.NamedError("reason_schema_error", result.SchemaError), // Reason might be in SchemaError
						zap.NamedError("reason_data_error", result.DataError),     // Or DataError (like context cancel)
						zap.Bool("schema_sync_skipped_explicitly", result.SchemaSyncSkipped),
					)
				}
				resultChan <- result // Send result regardless of cancellation status now
			}()

			// --- Table Sync Logic ---
			tableCtx, cancel := context.WithTimeout(ctx, f.cfg.TableTimeout)
			defer cancel() // Ensure context is cancelled for the table

			// 1. Schema Analysis & DDL Generation
			log.Info("Starting schema analysis/generation")
			schemaResult, schemaErr := f.schemaSyncer.SyncTableSchema(tableCtx, tbl, f.cfg.SchemaSyncStrategy)
			if schemaErr != nil {
				log.Error("Schema analysis/generation failed", zap.Error(schemaErr))
				result.SchemaError = schemaErr
				f.metrics.SyncErrorsTotal.WithLabelValues("schema_analysis", tbl).Inc()
				if !f.cfg.SkipFailedTables { return } // Stop processing this goroutine if SkipFailedTables is false
				result.Skipped = true // Mark as skipped due to schema error if continuing
				return                // Stop processing this table
			}
			if f.cfg.SchemaSyncStrategy == config.SchemaSyncNone {
				result.SchemaSyncSkipped = true
				log.Info("Schema sync skipped due to 'none' strategy")
			} else {
				log.Info("Schema analysis/generation complete",
					zap.String("table_ddl_present", fmt.Sprintf("%t", schemaResult.TableDDL != "")),
					zap.Int("index_ddls", len(schemaResult.IndexDDLs)),
					zap.Int("constraint_ddls", len(schemaResult.ConstraintDDLs)),
				)
			}


			// Check cancellation after schema analysis
			if tableCtx.Err() != nil {
				log.Error("Context cancelled/timed out after schema analysis", zap.Error(tableCtx.Err()))
				result.SchemaError = tableCtx.Err() // Report timeout/cancel as schema error
				result.Skipped = true              // Mark as skipped
				return
			}

			// 2. Execute Table DDL (if generated) - CREATE TABLE or ALTER COLUMNs
			// Indexes and Constraints are applied AFTER data sync
			// Filter DDLs to execute only table structure changes here
			tableStructureDDLs := &SchemaExecutionResult{
				TableDDL: schemaResult.TableDDL,
				// Filter out ADD INDEX/CONSTRAINT from potential ALTER results for now
				// IndexDDLs: filterDDLs(schemaResult.IndexDDLs, "CREATE"),
				// ConstraintDDLs: filterDDLs(schemaResult.ConstraintDDLs, "ADD CONSTRAINT"),
			}
			// Only execute if there's actual table DDL
			if !result.SchemaSyncSkipped && tableStructureDDLs.TableDDL != "" {
				log.Info("Starting table DDL execution (CREATE/ALTER)")
				schemaErr = f.schemaSyncer.ExecuteDDLs(tableCtx, tbl, tableStructureDDLs)
				if schemaErr != nil {
					log.Error("Table DDL execution failed", zap.Error(schemaErr))
					result.SchemaError = schemaErr
					f.metrics.SyncErrorsTotal.WithLabelValues("schema_execution", tbl).Inc()
					if !f.cfg.SkipFailedTables { return }
					result.Skipped = true // Mark as skipped due to DDL error if continuing
					return
				}
				log.Info("Table DDL execution complete")
			}

			// Check cancellation after DDL execution
			if tableCtx.Err() != nil {
				log.Error("Context cancelled/timed out after DDL execution", zap.Error(tableCtx.Err()))
				result.SchemaError = tableCtx.Err()
				result.Skipped = true
				return
			}

			// 3. Data Synchronization
			pkColumns := schemaResult.PrimaryKeys // Get PKs from analysis result
			// Check if data sync is possible
			if len(pkColumns) == 0 && f.cfg.SchemaSyncStrategy != config.SchemaSyncNone {
				log.Warn("Skipping data sync because no primary key was found and schema sync is enabled.")
				result.Skipped = true // Mark as skipped due to no PK
				// Set syncCompletedWithoutError to true if schema part was ok?
				// No, let's consider skipping data a non-complete sync for this table.
				return
			}
			if len(pkColumns) == 0 && f.cfg.SchemaSyncStrategy == config.SchemaSyncNone {
				log.Warn("Schema sync is 'none' and no primary key detected. Data sync will attempt full table load without pagination, which is unsafe for large tables!")
				// Proceed without PKs only if schema sync is explicitly off - DANGEROUS
			}

			log.Info("Starting data synchronization")
			rows, batches, dataErr := f.syncData(tableCtx, tbl, pkColumns)
			result.RowsSynced = rows
			result.Batches = batches
			if dataErr != nil {
				log.Error("Data sync failed", zap.Error(dataErr))
				result.DataError = dataErr
				f.metrics.SyncErrorsTotal.WithLabelValues("data_sync", tbl).Inc()
				if !f.cfg.SkipFailedTables { return }
				// Don't mark as skipped, some data might have synced before error
				return // Stop processing this table if skipping failures is off
			}
			log.Info("Data synchronization complete")

			// Check cancellation after data sync
			if tableCtx.Err() != nil {
				log.Error("Context cancelled or timed out during/after data sync", zap.Error(tableCtx.Err()))
				result.DataError = tableCtx.Err()
				return
			}

			// 4. Execute Index & Constraint DDLs (if generated) - AFTER data sync
			// Filter DDLs to execute only ADD operations here (DROPs were handled by ExecuteDDLs in ALTER)
			indexAndConstraintDDLs := &SchemaExecutionResult{
				IndexDDLs:      schemaResult.IndexDDLs,      // Pass all index DDLs (includes drops from ALTER)
				ConstraintDDLs: schemaResult.ConstraintDDLs, // Pass all constraint DDLs (includes drops from ALTER)
			}
			if !result.SchemaSyncSkipped && (len(indexAndConstraintDDLs.IndexDDLs) > 0 || len(indexAndConstraintDDLs.ConstraintDDLs) > 0) {
				log.Info("Applying indexes and constraints after data sync")
				// Use a separate context? TableTimeout might be too short for large tables + index creation.
				// Let's use the original tableCtx for now, but this might need adjustment.
				constraintErr := f.schemaSyncer.ExecuteDDLs(tableCtx, tbl, indexAndConstraintDDLs)
				if constraintErr != nil {
					log.Error("Failed to apply indexes/constraints after data sync", zap.Error(constraintErr))
					result.ConstraintError = constraintErr // Separate error field
					f.metrics.SyncErrorsTotal.WithLabelValues("constraint_apply", tbl).Inc()
					if !f.cfg.SkipFailedTables { return }
					// Don't mark as skipped, data sync succeeded
					return // Stop if not skipping failures
				}
				log.Info("Indexes and constraints applied successfully")
			}

			// Check cancellation after constraints
			if tableCtx.Err() != nil {
				log.Error("Context cancelled or timed out during constraint application", zap.Error(tableCtx.Err()))
				if result.ConstraintError == nil { // Don't overwrite existing constraint error
					result.ConstraintError = tableCtx.Err()
				}
				return // Stop processing
			}

			// If we reach here without returning due to errors or cancellation
			syncCompletedWithoutError = true
			// --- End Table Sync Logic ---

		}(tableName)
	}

endloop: // Label for goto break

	// Wait for all active table sync goroutines to finish
	go func() {
		wg.Wait()
		close(resultChan) // Close channel once all goroutines are done
		close(sem)        // Close semaphore channel
	}()

	// Collect results from the channel
	for res := range resultChan {
		results[res.Table] = res
	}

	f.logger.Info("Full synchronization run finished",
		zap.Duration("total_duration", time.Since(startTime)),
		zap.Int("total_tables_processed", len(results)), // Includes tables attempted
	)
	f.metrics.SyncDuration.Observe(time.Since(startTime).Seconds())

	return results
}


// syncData performs the actual data transfer using pagination.
func (f *FullSync) syncData(ctx context.Context, table string, pkColumns []string) (totalRowsSynced int64, batches int, err error) {
	log := f.logger.With(zap.String("table", table), zap.Strings("pk_columns", pkColumns))
	// Log message moved to caller

	// --- Get Total Rows (for progress logging, optional but recommended) ---
	var totalRows int64 = -1
	countCtx, countCancel := context.WithTimeout(ctx, 15*time.Second) // Timeout for count query
	countErr := f.srcConn.DB.WithContext(countCtx).Table(table).Count(&totalRows).Error
	countCancel()
	if countErr != nil {
		log.Warn("Could not count total source rows for progress tracking", zap.Error(countErr))
	} else {
		log.Debug("Total source rows to sync", zap.Int64("count", totalRows))
	}

	// --- Determine Order By Clause ---
	var orderByClause string
	var quotedPKColumns, pkPlaceholders []string
	var sortErr error
	canPaginate := len(pkColumns) > 0
	if canPaginate {
		orderByClause, quotedPKColumns, pkPlaceholders, sortErr = f.buildPaginationClauses(pkColumns, f.srcConn.Dialect)
		if sortErr != nil { return 0, 0, fmt.Errorf("failed to build pagination clauses: %w", sortErr) }
		log.Debug("Using pagination", zap.String("order_by", orderByClause))
	} else {
		log.Warn("No primary key found - attempting full table load without pagination (unsafe for large tables)")
		orderByClause = "" // No order needed if loading all at once
	}


	// --- Pagination/Load Loop ---
	var lastPKValues []interface{}
	totalRowsSynced = 0
	batches = 0
	progressLogThreshold := 100 // Log progress every N batches
	noDataCounter := 0          // Counter for consecutive empty batches (safety break)

	// --- Attempt to Disable FKs during data load (EXPERIMENTAL) ---
	fkReEnableFunc, disableErr := f.toggleForeignKeys(ctx, f.dstConn, false, log) // Disable FKs
	if disableErr != nil {
		log.Error("Attempt to disable foreign keys before data sync failed, proceeding anyway...", zap.Error(disableErr))
		// Consider if this should be fatal based on configuration or needs.
	}
	// Ensure FKs are re-enabled afterwards, even on error/panic
	if fkReEnableFunc != nil {
		defer func() {
			log.Info("Attempting to re-enable foreign keys after data sync attempt")
			// Use a background context for the revert function to ensure it runs
			revertCtx, revertCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer revertCancel()
			// Call the revert function using the new context
			enableErr := f.revertFKsWithContext(revertCtx, fkReEnableFunc) // Wrapper needed if revertFunc doesn't take context
			if enableErr != nil {
				log.Error("Failed to re-enable foreign keys after data sync", zap.Error(enableErr))
			} else {
				log.Info("Foreign keys re-enabled successfully")
			}
		}()
	}


	for {
		// Check context cancellation before each iteration
		if ctx.Err() != nil { log.Warn("Context cancelled during data sync loop", zap.Error(ctx.Err())); return totalRowsSynced, batches, ctx.Err() }

		var batch []map[string]interface{}
		query := f.srcConn.DB.WithContext(ctx).Table(table)

		// Apply WHERE clause for pagination if possible and needed
		if canPaginate && lastPKValues != nil {
			whereClause, args := f.buildWhereClause(quotedPKColumns, pkPlaceholders, lastPKValues)
			log.Debug("Applying WHERE clause for next page", zap.String("clause", whereClause), zap.Any("args", args))
			query = query.Where(whereClause, args...)
		}

		// Apply Order By (if paginating) and Limit (always limit for safety, even if not paginating?)
		if orderByClause != "" { query = query.Order(orderByClause) }
		query = query.Limit(f.cfg.BatchSize) // Always limit to batch size

		log.Debug("Fetching next batch")
		fetchStartTime := time.Now()
		fetchErr := query.Find(&batch).Error
		fetchDuration := time.Since(fetchStartTime)
		log.Debug("Batch fetch complete", zap.Duration("fetch_duration", fetchDuration), zap.Int("rows_fetched", len(batch)))

		if fetchErr != nil {
			log.Error("Failed to fetch data batch from source", zap.Error(fetchErr))
			return totalRowsSynced, batches, fmt.Errorf("failed to fetch batch from %s: %w", table, fetchErr)
		}

		// Break loop if no more data
		if len(batch) == 0 {
			if lastPKValues == nil && totalRowsSynced == 0 { log.Info("Source table is empty or first fetch returned no data.") } else { log.Debug("No more data found in source table for this page.") }
			// Safety break for pagination issues
			if canPaginate && lastPKValues != nil {
				noDataCounter++; if noDataCounter > 3 {
					log.Error("Potential pagination issue: Multiple empty batches received consecutively.", zap.Any("last_pk_values", lastPKValues));
					return totalRowsSynced, batches, fmt.Errorf("potential pagination issue: multiple empty batches received for table %s", table)
				}
			}
			break // Normal exit from loop
		}
		noDataCounter = 0 // Reset counter if data was found


		// --- Sync Batch with Retry ---
		log.Debug("Syncing batch to destination", zap.Int("rows_in_batch", len(batch)))
		syncBatchStartTime := time.Now()
		batchErr := f.syncBatchWithRetry(ctx, table, batch)
		syncDuration := time.Since(syncBatchStartTime)
		log.Debug("Batch sync attempt complete", zap.Duration("sync_duration", syncDuration))

		if batchErr != nil {
			// Error is logged within syncBatchWithRetry
			return totalRowsSynced, batches, fmt.Errorf("failed to sync batch to %s: %w", table, batchErr)
		}

		// --- Update State for Next Iteration ---
		batches++
		batchSize := int64(len(batch))
		totalRowsSynced += batchSize
		// Metrics are updated inside syncBatchWithRetry now

		// Update last PK values if paginating
		if canPaginate {
			lastRow := batch[len(batch)-1]
			sortedPKNames, _ := f.getSortedPKNames(pkColumns) // Use consistent sorted order
			newLastPKValues := make([]interface{}, len(sortedPKNames))
			pkFound := true
			for i, pkName := range sortedPKNames {
				val, ok := lastRow[pkName]
				if !ok { log.Error("PK column missing in fetched data", zap.String("missing_pk", pkName)); pkFound = false; break }
				newLastPKValues[i] = val
			}
			if !pkFound { return totalRowsSynced, batches, fmt.Errorf("PK column missing in fetched data for %s", table) }
			lastPKValues = newLastPKValues
		} else {
			// If not paginating, we should have loaded all data, break the loop.
			log.Info("Full table load complete (no pagination).")
			break
		}

		// Log progress periodically
		if batches%progressLogThreshold == 0 || (totalRows > 0 && totalRowsSynced >= totalRows) { // Log on threshold or completion
			progressPercent := -1.0
			if totalRows > 0 { progressPercent = (float64(totalRowsSynced) / float64(totalRows)) * 100 }
			log.Info("Data sync progress",
				zap.Int("batch_num", batches),
				zap.Int64("rows_synced_cumulative", totalRowsSynced),
				zap.Int64("total_rows_approx", totalRows), // May be -1 if count failed
				zap.Float64("progress_percent_approx", progressPercent),
			)
		}

	} // End data fetch loop

	log.Info("Data sync finished for table", zap.Int64("total_rows_synced", totalRowsSynced), zap.Int("batches_processed", batches))
	return totalRowsSynced, batches, nil
}


// syncBatchWithRetry attempts to insert a batch with retry logic.
func (f *FullSync) syncBatchWithRetry(ctx context.Context, table string, batch []map[string]interface{}) error {
	log := f.logger.With(zap.String("table", table), zap.Int("batch_size", len(batch)))
	var lastErr error
	batchStartTime := time.Now()
	statusLabel := "failure_unknown" // Default status for metrics

	defer func() {
		// Record duration metric with the final status
		f.metrics.BatchProcessingDuration.WithLabelValues(table, statusLabel).Observe(time.Since(batchStartTime).Seconds())
		// Record row/batch count based on final status
		if strings.HasPrefix(statusLabel, "success") {
			f.metrics.RowsSyncedTotal.WithLabelValues(table).Add(float64(len(batch)))
			f.metrics.BatchesProcessedTotal.WithLabelValues(table).Inc()
		} else {
			f.metrics.BatchErrorsTotal.WithLabelValues(table).Inc()
		}
	}()

	for attempt := 0; attempt <= f.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			log.Warn("Retrying batch insert", zap.Int("attempt", attempt+1), zap.Int("max_attempts", f.cfg.MaxRetries+1), zap.Duration("retry_interval", f.cfg.RetryInterval), zap.Error(lastErr))
			timer := time.NewTimer(f.cfg.RetryInterval)
			select {
			case <-timer.C: // Wait complete
			case <-ctx.Done(): // Context cancelled during wait
				timer.Stop()
				log.Error("Context cancelled during batch insert retry wait", zap.Error(ctx.Err()))
				statusLabel = "failure_context_cancelled"
				return fmt.Errorf("context cancelled waiting to retry batch insert for %s (attempt %d): %w; last error: %v", table, attempt+1, ctx.Err(), lastErr)
			}
		}

		// Execute batch insert within a transaction
		txErr := f.dstConn.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			return tx.Clauses(clause.OnConflict{ UpdateAll: true }).Table(table).CreateInBatches(batch, len(batch)).Error
		})

		if txErr == nil {
			statusLabel = "success"
			if attempt > 0 { statusLabel = "success_retry" }
			if attempt > 0 { log.Info("Batch insert succeeded after retry", zap.Int("attempt", attempt+1)) }
			return nil // Success
		}

		lastErr = txErr // Store the error

		// Check context cancellation immediately after the failed attempt
		if ctx.Err() != nil {
			log.Error("Context cancelled during/after batch transaction attempt", zap.Error(ctx.Err()), zap.Int("attempt", attempt+1), zap.NamedError("transaction_error", lastErr))
			statusLabel = "failure_context_cancelled"
			return fmt.Errorf("context cancelled during batch insert for %s (attempt %d): %w; last db error: %v", table, attempt+1, ctx.Err(), lastErr)
		}

		log.Warn("Batch insert attempt failed", zap.Int("attempt", attempt+1), zap.Error(lastErr))
		// Continue loop to retry if max retries not reached

	} // End retry loop

	log.Error("Batch insert failed after maximum retries", zap.Int("max_retries", f.cfg.MaxRetries), zap.Error(lastErr))
	statusLabel = "failure_max_retries"
	return fmt.Errorf("failed to insert batch into %s after %d retries: %w", table, f.cfg.MaxRetries, lastErr)
}


// listTables retrieves a list of non-system tables from the source database.
func (f *FullSync) listTables(ctx context.Context) ([]string, error) {
	var tables []string
	var err error
	dbName := f.cfg.SrcDB.DBName
	dialect := f.srcConn.Dialect
	log := f.logger.With(zap.String("dialect", dialect), zap.String("database", dbName))
	log.Info("Listing user tables")

	switch dialect {
	case "mysql":
		query := `SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name`
		err = f.srcConn.DB.WithContext(ctx).Raw(query, dbName).Scan(&tables).Error
	case "postgres":
		query := `SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() AND table_type = 'BASE TABLE' ORDER BY table_name`
		err = f.srcConn.DB.WithContext(ctx).Raw(query).Scan(&tables).Error
	case "sqlite":
		query := `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`
		err = f.srcConn.DB.WithContext(ctx).Raw(query).Scan(&tables).Error
	default:
		return nil, fmt.Errorf("unsupported dialect for listing tables: %s", dialect)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to list tables for %s (%s): %w", dbName, dialect, err)
	}

	// Use schema syncer's logic to filter system tables reliably
	tempSchemaSyncer := NewSchemaSyncer(nil, nil, dialect, "", log) // Pass logger
	filteredTables := make([]string, 0, len(tables))
	for _, tbl := range tables {
		if !tempSchemaSyncer.isSystemTable(tbl, dialect) {
			filteredTables = append(filteredTables, tbl)
		} else {
			log.Debug("Filtered out potential system table", zap.String("table", tbl))
		}
	}

	return filteredTables, nil
}


// --- Pagination Helpers ---

func (f *FullSync) getSortedPKNames(pkColumns []string) ([]string, error) {
	if len(pkColumns) == 0 { return nil, fmt.Errorf("no primary key columns provided for sorting") }
	sortedPKs := make([]string, len(pkColumns)); copy(sortedPKs, pkColumns); sort.Strings(sortedPKs)
	return sortedPKs, nil
}

func (f *FullSync) buildPaginationClauses(pkColumns []string, dialect string) (orderBy string, quotedPKs []string, placeholders []string, err error) {
	if len(pkColumns) == 0 { err = fmt.Errorf("cannot build pagination clauses without primary keys"); return }
	sortedPKs, _ := f.getSortedPKNames(pkColumns) // Use sorted names for consistency
	quotedPKs = make([]string, len(sortedPKs)); placeholders = make([]string, len(sortedPKs)); orderByParts := make([]string, len(sortedPKs))
	for i, pk := range sortedPKs {
		quotedPKs[i] = utils.QuoteIdentifier(pk, dialect); placeholders[i] = "?"; orderByParts[i] = quotedPKs[i] + " ASC"
	}
	orderBy = strings.Join(orderByParts, ", "); return
}

func (f *FullSync) buildWhereClause(quotedSortedPKs []string, placeholders []string, lastPKValues []interface{}) (string, []interface{}) {
	if len(quotedSortedPKs) == 1 { // Simple PK
		return fmt.Sprintf("%s > ?", quotedSortedPKs[0]), lastPKValues
	}
	// Composite PK: WHERE (pk1, pk2, ...) > (?, ?, ...)
	whereTuple := fmt.Sprintf("(%s)", strings.Join(quotedSortedPKs, ", "))
	placeholderTuple := fmt.Sprintf("(%s)", strings.Join(placeholders, ", "))
	whereClause := fmt.Sprintf("%s > %s", whereTuple, placeholderTuple)
	return whereClause, lastPKValues // Args are already ordered correctly based on sorted PKs
}


// --- Foreign Key Toggling (Experimental) ---

// revertFKsWithContext is a helper to call the revert function with a specific context.
func (f *FullSync) revertFKsWithContext(ctx context.Context, revertFunc func() error) error {
	// This wrapper isn't strictly necessary if revertFunc itself handles context,
	// but added for clarity if revertFunc was simpler.
	// In our current implementation, revertFunc already uses a background context.
	// Calling it directly is fine.
	if revertFunc != nil {
		return revertFunc()
	}
	return nil
}


// toggleForeignKeys attempts to disable or enable FK checks. Returns a function to revert the state.
func (f *FullSync) toggleForeignKeys(ctx context.Context, conn *db.Connector, enable bool, log *zap.Logger) (revertFunc func() error, err error) {
	if !f.cfg.DisableFKDuringSync { return nil, nil } // Feature disabled

	var disableCmd, enableCmd, initialStateCmd, revertCmd string
	var initialState interface{} = nil
	var stateStr string // Intermediate string for Scan

	switch conn.Dialect {
	case "mysql":
		initialStateCmd = "SELECT @@FOREIGN_KEY_CHECKS"
		disableCmd = "SET FOREIGN_KEY_CHECKS = 0;"
		// enableCmd defined later based on initialState
	case "postgres":
		initialStateCmd = "SHOW session_replication_role"
		disableCmd = "SET session_replication_role = replica;"
		// enableCmd defined later based on initialState
	default:
		log.Debug("Foreign key toggling not supported for dialect", zap.String("dialect", conn.Dialect))
		return nil, nil
	}

	// Get initial state
	if initialStateCmd != "" {
		// Use a short timeout for reading the initial state
		readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
		err = conn.DB.WithContext(readCtx).Raw(initialStateCmd).Scan(&stateStr).Error
		readCancel() // Release context early

		if err != nil {
			log.Warn("Could not read initial FK/replication state, assuming default", zap.Error(err))
			if conn.Dialect == "mysql" { initialState = "1" } else if conn.Dialect == "postgres" { initialState = "origin" }
		} else {
			initialState = stateStr // Store the actual initial state
			log.Debug("Read initial FK/replication state", zap.String("state", stateStr), zap.String("dialect", conn.Dialect))
		}
	} else { // Set defaults if no command to read state
        if conn.Dialect == "mysql" { initialState = "1" } else if conn.Dialect == "postgres" { initialState = "origin" }
    }


	// Define enable/revert commands using the potentially defaulted initial state
	if conn.Dialect == "mysql" {
		enableCmd = fmt.Sprintf("SET FOREIGN_KEY_CHECKS = %v;", initialState) // %v handles string '1' or int 1
	} else if conn.Dialect == "postgres" {
		enableCmd = fmt.Sprintf("SET session_replication_role = '%v';", initialState) // Quote role name
	}


	targetCmd := disableCmd
	revertCmd = enableCmd
	action := "Disabling"
	if enable { // If the goal is to enable (less likely needed here)
		targetCmd = enableCmd
		revertCmd = disableCmd
		action = "Enabling"
	}

	log.Info(action+" foreign key checks/triggers", zap.String("dialect", conn.Dialect), zap.String("command", targetCmd))
	// Use a short timeout for executing the toggle command
	execCtx, execCancel := context.WithTimeout(ctx, 5*time.Second)
	execErr := conn.DB.WithContext(execCtx).Exec(targetCmd).Error
	execCancel()
	if execErr != nil {
		return nil, fmt.Errorf("failed to execute '%s': %w", targetCmd, execErr)
	}

	// Return a function that executes the revert command
	revertFunc = func() error {
		// Use a background context with timeout for revert robustness
		revertCtxInternal, revertCancelInternal := context.WithTimeout(context.Background(), 10*time.Second)
		defer revertCancelInternal()
		log.Info("Reverting foreign key checks/triggers state", zap.String("dialect", conn.Dialect), zap.String("command", revertCmd))
		revertErr := conn.DB.WithContext(revertCtxInternal).Exec(revertCmd).Error
		if revertErr != nil {
			return fmt.Errorf("failed to execute revert command '%s': %w", revertCmd, revertErr)
		}
		return nil
	}

	return revertFunc, nil
}