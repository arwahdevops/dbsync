// internal/sync/orchestrator.go
package sync

import (
	"context"
	"errors" // Digunakan untuk context.DeadlineExceeded
	"fmt"
	"sort"
	"strconv" // <-- Import yang ditambahkan
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/arwahdevops/dbsync/internal/config"
	"github.com/arwahdevops/dbsync/internal/db"
	"github.com/arwahdevops/dbsync/internal/metrics"
	"github.com/arwahdevops/dbsync/internal/utils"
)

type Orchestrator struct {
	srcConn      *db.Connector
	dstConn      *db.Connector
	cfg          *config.Config
	logger       *zap.Logger
	schemaSyncer SchemaSyncerInterface
	metrics      *metrics.Store
}

// SyncResult menyimpan hasil sinkronisasi untuk satu tabel.
type SyncResult struct {
	Table                    string
	SchemaSyncSkipped        bool          // Apakah sync skema dilewati karena strategy=none
	SchemaAnalysisError      error         // Error saat analisis/generasi DDL
	SchemaExecutionError     error         // Error saat eksekusi DDL tabel (CREATE/ALTER)
	DataError                error         // Error saat sinkronisasi data
	ConstraintExecutionError error         // Error saat eksekusi DDL indeks/constraint (setelah data)
	SkipReason               string        // Alasan mengapa pemrosesan tabel dilewati
	RowsSynced               int64
	Batches                  int
	Duration                 time.Duration
	Skipped                  bool          // Apakah pemrosesan tabel ini dilewati sepenuhnya (karena error fatal + SkipFailedTables=false, atau context cancelled)
}


var _ OrchestratorInterface = (*Orchestrator)(nil)

func NewOrchestrator(srcConn, dstConn *db.Connector, cfg *config.Config, logger *zap.Logger, metricsStore *metrics.Store) *Orchestrator {
	return &Orchestrator{
		srcConn:      srcConn,
		dstConn:      dstConn,
		cfg:          cfg,
		logger:       logger.Named("full-sync"),
		schemaSyncer: NewSchemaSyncer(
			srcConn.DB,
			dstConn.DB,
			srcConn.Dialect,
			dstConn.Dialect,
			logger,
		),
		metrics: metricsStore,
	}
}

func (f *Orchestrator) Run(ctx context.Context) map[string]SyncResult {
	startTime := time.Now()
	f.logger.Info("Starting full database synchronization run",
		zap.String("direction", f.cfg.SyncDirection),
		zap.Int("workers", f.cfg.Workers),
		zap.Int("batch_size", f.cfg.BatchSize),
		zap.String("schema_strategy", string(f.cfg.SchemaSyncStrategy)),
	)
	f.metrics.SyncRunning.Set(1)
	defer f.metrics.SyncRunning.Set(0) // Set ke 0 saat Run selesai

	results := make(map[string]SyncResult)
	tables, err := f.listTables(ctx) // Gunakan context di listTables
	if err != nil {
		f.logger.Error("Failed to list source tables", zap.Error(err))
		f.metrics.SyncErrorsTotal.WithLabelValues("list_tables", "").Inc()
		return results
	}

	if len(tables) == 0 {
		f.logger.Warn("No tables found in source database to synchronize")
		f.metrics.SyncDuration.Observe(time.Since(startTime).Seconds())
		return results
	}

	f.logger.Info("Found tables to synchronize", zap.Int("count", len(tables)), zap.Strings("tables", tables))

	var wg sync.WaitGroup
	resultChan := make(chan SyncResult, len(tables))
	sem := make(chan struct{}, f.cfg.Workers)

	for i, tableName := range tables {
		select {
		case <-ctx.Done():
			remainingTables := tables[i:]
			f.logger.Warn("Context cancelled before starting sync for remaining tables",
				zap.String("first_skipped_table", tableName),
				zap.Int("remaining_count", len(remainingTables)),
				zap.Error(ctx.Err()),
			)
			for _, tbl := range remainingTables {
				results[tbl] = SyncResult{Table: tbl, Skipped: true, SkipReason: "Context cancelled before start", DataError: ctx.Err()}
			}
			goto endloop
		default:
		}

		wg.Add(1)
		go func(tbl string) {
			defer wg.Done()

			log := f.logger.With(zap.String("table", tbl))
			start := time.Now()
			result := SyncResult{Table: tbl}

			defer func() {
				result.Duration = time.Since(start)
				f.metrics.TableSyncDuration.WithLabelValues(tbl).Observe(result.Duration.Seconds())

				logFields := []zap.Field{
					zap.Duration("duration", result.Duration),
					zap.Int64("rows_synced", result.RowsSynced),
					zap.Int("batches_processed", result.Batches),
					zap.Bool("schema_sync_skipped_explicitly", result.SchemaSyncSkipped),
				}

				if result.Skipped {
					logFields = append(logFields, zap.String("skip_reason", result.SkipReason))
					if result.SchemaAnalysisError != nil { logFields = append(logFields, zap.NamedError("cause_schema_analysis_error", result.SchemaAnalysisError)) }
					if result.SchemaExecutionError != nil { logFields = append(logFields, zap.NamedError("cause_schema_execution_error", result.SchemaExecutionError)) }
					if result.DataError != nil { logFields = append(logFields, zap.NamedError("cause_data_error", result.DataError)) }
					log.Warn("Table processing was skipped", logFields...)
				} else if result.SchemaAnalysisError != nil || result.SchemaExecutionError != nil || result.DataError != nil {
					if result.SchemaAnalysisError != nil { logFields = append(logFields, zap.NamedError("schema_analysis_error", result.SchemaAnalysisError)) }
					if result.SchemaExecutionError != nil { logFields = append(logFields, zap.NamedError("schema_execution_error", result.SchemaExecutionError)) }
					if result.DataError != nil { logFields = append(logFields, zap.NamedError("data_error", result.DataError)) }
					if result.ConstraintExecutionError != nil { logFields = append(logFields, zap.NamedError("constraint_execution_error", result.ConstraintExecutionError)) }
					log.Error("Table sync finished with critical errors", logFields...)
				} else if result.ConstraintExecutionError != nil {
					logFields = append(logFields, zap.NamedError("constraint_execution_error", result.ConstraintExecutionError))
					log.Warn("Table sync data SUCCEEDED, but applying constraints/indexes FAILED", logFields...)
					f.metrics.TableSyncSuccessTotal.WithLabelValues(tbl).Inc()
				} else {
					log.Info("Table sync finished successfully", logFields...)
					f.metrics.TableSyncSuccessTotal.WithLabelValues(tbl).Inc()
				}
				resultChan <- result
			}()

			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				log.Warn("Context cancelled while waiting for worker slot", zap.Error(ctx.Err()))
				result.Skipped = true
				result.SkipReason = "Context cancelled while waiting for worker"
				result.DataError = ctx.Err()
				return
			}

			tableCtx, cancel := context.WithTimeout(ctx, f.cfg.TableTimeout)
			defer cancel()

			// --- Tahap 1: Analisis Skema & Pembuatan DDL ---
			log.Info("Starting schema analysis/generation")
			schemaResult, schemaErr := f.schemaSyncer.SyncTableSchema(tableCtx, tbl, f.cfg.SchemaSyncStrategy)
			if schemaErr != nil {
				log.Error("Schema analysis/generation failed", zap.Error(schemaErr))
				result.SchemaAnalysisError = schemaErr
				f.metrics.SyncErrorsTotal.WithLabelValues("schema_analysis", tbl).Inc()
				if !f.cfg.SkipFailedTables {
					result.Skipped = true
					result.SkipReason = "Schema analysis failed (SkipFailedTables=false)"
					return
				}
				result.Skipped = true
				result.SkipReason = "Schema analysis failed (SkipFailedTables=true)"
				return
			}
			if f.cfg.SchemaSyncStrategy == config.SchemaSyncNone {
				result.SchemaSyncSkipped = true
				log.Info("Schema sync skipped due to 'none' strategy")
			} else if schemaResult != nil {
				log.Info("Schema analysis/generation complete",
					zap.Bool("table_ddl_present", schemaResult.TableDDL != ""),
					zap.Int("index_ddls_count", len(schemaResult.IndexDDLs)),
					zap.Int("constraint_ddls_count", len(schemaResult.ConstraintDDLs)),
					zap.Strings("detected_pks", schemaResult.PrimaryKeys),
				)
			}
			if tableCtx.Err() != nil {
				log.Error("Context cancelled or timed out after schema analysis", zap.Error(tableCtx.Err()))
				result.SchemaAnalysisError = fmt.Errorf("context error after schema analysis: %w", tableCtx.Err())
				result.Skipped = true
				result.SkipReason = "Context cancelled/timed out after schema analysis"
				return
			}

			// --- Tahap 2: Eksekusi DDL Struktur Tabel (CREATE/ALTER) ---
			if !result.SchemaSyncSkipped && schemaResult != nil && schemaResult.TableDDL != "" {
				log.Info("Starting table DDL execution (CREATE/ALTER)")
				tableStructureDDLs := &SchemaExecutionResult{ TableDDL: schemaResult.TableDDL }
				schemaExecErr := f.schemaSyncer.ExecuteDDLs(tableCtx, tbl, tableStructureDDLs)
				if schemaExecErr != nil {
					log.Error("Table DDL execution failed", zap.Error(schemaExecErr))
					result.SchemaExecutionError = schemaExecErr
					f.metrics.SyncErrorsTotal.WithLabelValues("schema_execution", tbl).Inc()
					if !f.cfg.SkipFailedTables {
						result.Skipped = true
						result.SkipReason = "Table DDL execution failed (SkipFailedTables=false)"
						return
					}
					log.Warn("Table DDL execution failed (SkipFailedTables=true), attempting to continue with data sync. This might lead to unexpected results if schema is incorrect.")
				} else {
					log.Info("Table DDL execution complete")
				}
			} else if !result.SchemaSyncSkipped {
				log.Info("No table structure DDL (CREATE/ALTER) to execute.")
			}
			if tableCtx.Err() != nil {
				log.Error("Context cancelled or timed out after DDL execution", zap.Error(tableCtx.Err()))
				result.SchemaExecutionError = fmt.Errorf("context error after DDL execution: %w", tableCtx.Err())
				result.Skipped = true
				result.SkipReason = "Context cancelled/timed out after DDL execution"
				return
			}

			// --- Tahap 3: Sinkronisasi Data ---
			pkColumns := []string{}
			if schemaResult != nil {
				pkColumns = schemaResult.PrimaryKeys
			}
			shouldSkipData := (result.SchemaExecutionError != nil && !f.cfg.SkipFailedTables)

			if !shouldSkipData {
				if len(pkColumns) == 0 && f.cfg.SchemaSyncStrategy != config.SchemaSyncNone {
					log.Warn("Data sync requires primary keys for reliable pagination, especially with ALTER/DROP_CREATE strategies. No primary key found/generated.", zap.Strings("retrieved_pks", pkColumns))
					log.Warn("Proceeding with data sync without primary key (schema strategy is not 'none'). This is risky for large tables or ALTER strategy.")
				} else if len(pkColumns) == 0 && f.cfg.SchemaSyncStrategy == config.SchemaSyncNone {
					log.Warn("Schema sync is 'none' and no primary key detected. Data sync will attempt full table load without pagination, which is unsafe for large tables!")
				}

				log.Info("Starting data synchronization")
				rows, batches, dataErr := f.syncData(tableCtx, tbl, pkColumns)
				result.RowsSynced = rows
				result.Batches = batches
				if dataErr != nil {
					log.Error("Data sync failed", zap.Error(dataErr))
					result.DataError = dataErr
					f.metrics.SyncErrorsTotal.WithLabelValues("data_sync", tbl).Inc()
					if !f.cfg.SkipFailedTables {
						result.Skipped = true
						result.SkipReason = "Data sync failed (SkipFailedTables=false)"
						return
					}
					log.Warn("Data sync failed (SkipFailedTables=true), attempting to continue with constraint/index application.")
				} else {
					log.Info("Data synchronization complete")
				}
			} else {
				log.Warn("Skipping data synchronization for this table due to previous critical schema errors.", zap.NamedError("schema_exec_error", result.SchemaExecutionError))
			}

			if tableCtx.Err() != nil {
				log.Error("Context cancelled or timed out during/after data sync phase", zap.Error(tableCtx.Err()))
				if result.DataError == nil { result.DataError = fmt.Errorf("context error after data sync phase: %w", tableCtx.Err()) }
				result.Skipped = true
				result.SkipReason = "Context cancelled/timed out during/after data sync phase"
				return
			}

			// --- Tahap 4: Eksekusi DDL Indeks & Constraint ---
			shouldSkipConstraints := ( (result.SchemaExecutionError != nil || result.DataError != nil) && !f.cfg.SkipFailedTables )

			if !result.SchemaSyncSkipped && schemaResult != nil && (len(schemaResult.IndexDDLs) > 0 || len(schemaResult.ConstraintDDLs) > 0) {
				if shouldSkipConstraints {
					log.Warn("Skipping constraint/index application due to previous critical schema/data errors.")
				} else {
					log.Info("Applying indexes and constraints after data sync")
					indexAndConstraintDDLs := &SchemaExecutionResult{
						IndexDDLs:      schemaResult.IndexDDLs,
						ConstraintDDLs: schemaResult.ConstraintDDLs,
					}
					constraintErr := f.schemaSyncer.ExecuteDDLs(tableCtx, tbl, indexAndConstraintDDLs)
					if constraintErr != nil {
						log.Error("Failed to apply indexes/constraints after data sync", zap.Error(constraintErr))
						result.ConstraintExecutionError = constraintErr
						f.metrics.SyncErrorsTotal.WithLabelValues("constraint_apply", tbl).Inc()
					} else {
						log.Info("Indexes and constraints applied successfully")
					}
				}
			} else if !result.SchemaSyncSkipped {
				log.Info("No index or constraint DDLs to execute.")
			}

			if tableCtx.Err() != nil {
				log.Error("Context cancelled or timed out during constraint application phase", zap.Error(tableCtx.Err()))
				if result.ConstraintExecutionError == nil { result.ConstraintExecutionError = fmt.Errorf("context error during constraint application: %w", tableCtx.Err()) }
			}

		}(tableName)
	}

endloop:

	go func() {
		wg.Wait()
		close(resultChan)
		close(sem)
		// PERBAIKAN: Gunakan f.logger
		f.logger.Debug("All table processing goroutines finished.")
	}()

	for res := range resultChan {
		results[res.Table] = res
	}

	f.logger.Info("Full synchronization run finished",
		zap.Duration("total_duration", time.Since(startTime)),
		zap.Int("total_tables_processed_or_skipped", len(results)),
	)
	f.metrics.SyncDuration.Observe(time.Since(startTime).Seconds())

	return results
}

// syncData performs the actual data transfer using pagination.
func (f *Orchestrator) syncData(ctx context.Context, table string, pkColumns []string) (totalRowsSynced int64, batches int, err error) {
	log := f.logger.With(zap.String("table", table), zap.Strings("pk_columns", pkColumns))

	var totalRows int64 = -1
	countCtx, countCancel := context.WithTimeout(ctx, 15*time.Second)
	countErr := f.srcConn.DB.WithContext(countCtx).Table(table).Count(&totalRows).Error
	countCancel()

	if countErr != nil {
		logFields := []zap.Field{zap.Error(countErr)}
		if errors.Is(countErr, context.DeadlineExceeded) {
			logFields = append(logFields, zap.Duration("count_query_timeout", 15*time.Second))
			log.Warn("Counting total source rows timed out.", logFields...)
		} else if errors.Is(ctx.Err(), context.Canceled) {
			logFields = append(logFields, zap.NamedError("main_context_error", ctx.Err()))
			log.Warn("Counting total source rows cancelled by main table context.", logFields...)
		} else if countErr == gorm.ErrRecordNotFound {
			log.Debug("Source table appears empty based on Count().")
			totalRows = 0
		} else {
			log.Warn("Could not count total source rows for progress tracking due to DB error.", logFields...)
		}
	} else {
		log.Info("Approximate total source rows to sync", zap.Int64("count", totalRows))
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
		log.Debug("Using pagination", zap.String("order_by", orderByClause))
	} else {
		log.Warn("No primary key provided/found - attempting full table load without pagination. This is potentially unsafe and slow for large tables!")
		orderByClause = ""
	}

	var revertFunc func() error
	if f.cfg.DisableFKDuringSync {
		var disableErr error
		revertFunc, disableErr = f.toggleForeignKeys(ctx, f.dstConn, false, log)
		if disableErr != nil {
			log.Error("Attempt to disable foreign keys before data sync failed, proceeding anyway...", zap.Error(disableErr))
		}
		if revertFunc != nil {
			defer func() {
				log.Info("Attempting to re-enable foreign keys after data sync attempt")
				revertCtx, revertCancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer revertCancel()
				enableErr := f.revertFKsWithContext(revertCtx, revertFunc)
				if enableErr != nil {
					log.Error("Failed to re-enable foreign keys after data sync", zap.Error(enableErr))
				} else {
					log.Info("Foreign keys re-enabled/reverted successfully")
				}
			}()
		}
	}

	var lastPKValues []interface{}
	totalRowsSynced = 0
	batches = 0
	progressLogThreshold := 100
	noDataCounter := 0

	for {
		if err := ctx.Err(); err != nil {
			log.Warn("Context cancelled during data sync loop", zap.Error(err))
			return totalRowsSynced, batches, err
		}

		var batch []map[string]interface{}
		query := f.srcConn.DB.WithContext(ctx).Table(table)

		if canPaginate && lastPKValues != nil {
			if len(quotedPKColumns) == 0 || len(pkPlaceholders) == 0 {
				return totalRowsSynced, batches, fmt.Errorf("internal error: pagination columns/placeholders not initialized for table '%s'", table)
			}
			whereClause, args := f.buildWhereClause(quotedPKColumns, pkPlaceholders, lastPKValues)
			log.Debug("Applying WHERE clause for next page", zap.String("clause", whereClause), zap.Any("args", args))
			query = query.Where(whereClause, args...)
		}

		if orderByClause != "" {
			query = query.Order(orderByClause)
		}
		query = query.Limit(f.cfg.BatchSize)

		log.Debug("Fetching next batch from source", zap.Int("limit", f.cfg.BatchSize))
		fetchStartTime := time.Now()
		fetchErr := query.Find(&batch).Error
		fetchDuration := time.Since(fetchStartTime)

		if fetchErr != nil {
			log.Error("Failed to fetch data batch from source", zap.Error(fetchErr))
			return totalRowsSynced, batches, fmt.Errorf("failed to fetch batch from '%s': %w", table, fetchErr)
		}

		rowsFetched := len(batch)
		log.Debug("Batch fetch complete", zap.Duration("fetch_duration", fetchDuration), zap.Int("rows_fetched", rowsFetched))


		if rowsFetched == 0 {
			if lastPKValues == nil && totalRowsSynced == 0 {
				log.Info("Source table is empty or first fetch returned no data.")
			} else {
				log.Info("No more data found in source table for this page or table completed.")
			}
			if canPaginate && lastPKValues != nil {
				noDataCounter++
				if noDataCounter >= 3 {
					log.Error("Potential pagination issue: Multiple consecutive empty batches received.",
						zap.Any("last_pk_values", lastPKValues))
					return totalRowsSynced, batches, fmt.Errorf("potential pagination issue: multiple empty batches received for table '%s' after syncing %d rows", table, totalRowsSynced)
				}
			}
			break
		}
		noDataCounter = 0

		log.Debug("Syncing batch to destination", zap.Int("rows_in_batch", rowsFetched))
		syncBatchStartTime := time.Now()
		batchErr := f.syncBatchWithRetry(ctx, table, batch)
		syncDuration := time.Since(syncBatchStartTime)
		log.Debug("Batch sync attempt complete", zap.Duration("sync_duration", syncDuration))

		if batchErr != nil {
			return totalRowsSynced, batches, fmt.Errorf("failed to sync batch to table '%s': %w", table, batchErr)
		}

		batches++
		batchSizeSynced := int64(rowsFetched)
		totalRowsSynced += batchSizeSynced
		f.metrics.RowsSyncedTotal.WithLabelValues(table).Add(float64(batchSizeSynced))

		if canPaginate {
			lastRow := batch[rowsFetched-1]
			sortedPKNames, sortErr := f.getSortedPKNames(pkColumns)
			if sortErr != nil {
				return totalRowsSynced, batches, fmt.Errorf("failed to get sorted PK names for pagination update in table '%s': %w", table, sortErr)
			}

			newLastPKValues := make([]interface{}, len(sortedPKNames))
			pkFound := true
			missingPK := "" // Simpan nama PK yang hilang untuk logging
			for i, pkName := range sortedPKNames {
				val, ok := lastRow[pkName]
				if !ok {
					log.Error("Primary key column missing in fetched source data, cannot continue pagination reliably.", zap.String("missing_pk_column", pkName))
					pkFound = false
					missingPK = pkName // Simpan nama yang hilang
					break
				}
				newLastPKValues[i] = val
			}
			if !pkFound {
				return totalRowsSynced, batches, fmt.Errorf("PK column '%s' missing in fetched data for table '%s', stopping sync", missingPK, table)
			}
			lastPKValues = newLastPKValues
		} else {
			log.Info("Full table load completed in a single batch (no pagination).")
			break
		}

		if batches%progressLogThreshold == 0 || (totalRows >= 0 && totalRowsSynced >= totalRows && rowsFetched > 0) {
			progressPercent := -1.0
			if totalRows > 0 {
				progressPercent = (float64(totalRowsSynced) / float64(totalRows)) * 100
			}
			log.Info("Data sync progress",
				zap.Int("batch_number", batches),
				zap.Int64("cumulative_rows_synced", totalRowsSynced),
				zap.Int64("approx_total_rows", totalRows),
				zap.Float64("approx_progress_percent", progressPercent),
			)
		}
	}

	log.Info("Data sync finished for table", zap.Int64("total_rows_synced", totalRowsSynced), zap.Int("batches_processed", batches))
	return totalRowsSynced, batches, nil
}

// syncBatchWithRetry attempts to insert/upsert a batch with retry logic.
func (f *Orchestrator) syncBatchWithRetry(ctx context.Context, table string, batch []map[string]interface{}) error {
	log := f.logger.With(zap.String("table", table), zap.Int("batch_size", len(batch)))
	if len(batch) == 0 {
		log.Debug("syncBatchWithRetry called with empty batch, skipping.")
		return nil
	}
	var lastErr error
	batchStartTime := time.Now()
	statusLabel := "failure_unknown"

	defer func() {
		f.metrics.BatchProcessingDuration.WithLabelValues(table, statusLabel).Observe(time.Since(batchStartTime).Seconds())
		if strings.HasPrefix(statusLabel, "success") {
			f.metrics.BatchesProcessedTotal.WithLabelValues(table).Inc()
		} else {
			f.metrics.BatchErrorsTotal.WithLabelValues(table).Inc()
		}
	}()

	for attempt := 0; attempt <= f.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			log.Warn("Retrying batch insert/upsert",
				zap.Int("attempt", attempt+1),
				zap.Int("max_attempts", f.cfg.MaxRetries+1),
				zap.Duration("retry_interval", f.cfg.RetryInterval),
				zap.NamedError("previous_error", lastErr))

			timer := time.NewTimer(f.cfg.RetryInterval)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				log.Error("Context cancelled during batch insert retry wait", zap.Error(ctx.Err()))
				statusLabel = "failure_context_cancelled_retry"
				return fmt.Errorf("context cancelled waiting to retry batch insert for table '%s' (attempt %d): %w; last db error: %v", table, attempt+1, ctx.Err(), lastErr)
			}
		}

		txErr := f.dstConn.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			return tx.Table(table).Clauses(clause.OnConflict{UpdateAll: true}).CreateInBatches(batch, len(batch)).Error
		})

		if txErr == nil {
			statusLabel = "success"
			if attempt > 0 {
				statusLabel = "success_retry"
				log.Info("Batch insert/upsert succeeded after retry", zap.Int("attempt", attempt+1))
			} else {
				log.Debug("Batch insert/upsert succeeded on first attempt.")
			}
			return nil
		}
		lastErr = txErr

		if ctxErr := ctx.Err(); ctxErr != nil {
			log.Error("Context cancelled during/after batch transaction attempt",
				zap.Error(ctxErr),
				zap.Int("attempt", attempt+1),
				zap.NamedError("transaction_error", lastErr))
			statusLabel = "failure_context_cancelled_tx"
			return fmt.Errorf("context cancelled during batch insert for table '%s' (attempt %d): %w; last db error: %v", table, attempt+1, ctxErr, lastErr)
		}
		log.Warn("Batch insert/upsert attempt failed", zap.Int("attempt", attempt+1), zap.Error(lastErr))

	}

	log.Error("Batch insert/upsert failed after maximum retries",
		zap.Int("max_retries", f.cfg.MaxRetries),
		zap.NamedError("final_error", lastErr))
	statusLabel = "failure_max_retries"
	return fmt.Errorf("failed to insert/upsert batch into table '%s' after %d retries: %w", table, f.cfg.MaxRetries, lastErr)
}


// listTables retrieves a list of non-system tables from the source database.
func (f *Orchestrator) listTables(ctx context.Context) ([]string, error) {
	var tables []string
	var err error
	dbCfg := f.cfg.SrcDB
	dialect := f.srcConn.Dialect
	log := f.logger.With(zap.String("dialect", dialect), zap.String("database", dbCfg.DBName), zap.String("action", "listTables"))
	log.Info("Listing user tables from source database")

	db := f.srcConn.DB.WithContext(ctx)

	switch dialect {
	case "mysql":
		query := `SELECT table_name FROM information_schema.tables
				  WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'
				  AND table_name NOT IN ('sys_config')
				  ORDER BY table_name`
		err = db.Raw(query).Scan(&tables).Error
	case "postgres":
		query := `SELECT table_name FROM information_schema.tables
				  WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'
				  AND table_name NOT LIKE 'pg_%'
				  AND table_name NOT LIKE 'sql_%'
				  ORDER BY table_name`
		err = db.Raw(query).Scan(&tables).Error
	case "sqlite":
		query := `SELECT name FROM sqlite_master
				  WHERE type='table' AND name NOT LIKE 'sqlite_%'
				  ORDER BY name`
		err = db.Raw(query).Scan(&tables).Error
	default:
		return nil, fmt.Errorf("unsupported dialect for listing tables: %s", dialect)
	}

	if err != nil {
		if ctx.Err() != nil {
			log.Error("Context cancelled during table listing", zap.Error(ctx.Err()), zap.NamedError("db_error", err))
			return nil, ctx.Err()
		}
		log.Error("Failed to execute list tables query", zap.Error(err))
		return nil, fmt.Errorf("failed to list tables for database '%s' (%s): %w", dbCfg.DBName, dialect, err)
	}

	log.Debug("Table listing successful", zap.Int("table_count", len(tables)))
	return tables, nil
}

// getSortedPKNames mengurutkan nama kolom PK secara alfabetis.
func (f *Orchestrator) getSortedPKNames(pkColumns []string) ([]string, error) {
	if len(pkColumns) == 0 {
		return []string{}, nil
	}
	sortedPKs := make([]string, len(pkColumns))
	copy(sortedPKs, pkColumns)
	sort.Strings(sortedPKs)
	return sortedPKs, nil
}

// buildPaginationClauses membangun klausa ORDER BY dan placeholder WHERE.
func (f *Orchestrator) buildPaginationClauses(pkColumns []string, dialect string) (orderBy string, quotedPKs []string, placeholders []string, err error) {
	if len(pkColumns) == 0 {
		err = fmt.Errorf("cannot build pagination clauses without primary keys")
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

// buildWhereClause membangun klausa WHERE untuk keyset pagination.
func (f *Orchestrator) buildWhereClause(quotedSortedPKs []string, placeholders []string, lastPKValues []interface{}) (string, []interface{}) {
	if len(quotedSortedPKs) == 1 {
		return fmt.Sprintf("%s > ?", quotedSortedPKs[0]), lastPKValues
	}
	if len(quotedSortedPKs) != len(placeholders) || len(quotedSortedPKs) != len(lastPKValues) {
		f.logger.Error("Mismatch in PK count for building composite WHERE clause",
			zap.Int("pk_count", len(quotedSortedPKs)),
			zap.Int("placeholder_count", len(placeholders)),
			zap.Int("value_count", len(lastPKValues)))
		// Fallback - ini kemungkinan besar akan salah
		if len(lastPKValues) > 0 {
			return fmt.Sprintf("%s > ?", quotedSortedPKs[0]), []interface{}{lastPKValues[0]}
		}
		return "1=0", []interface{}{} // Klausa yang selalu false
	}
	whereTuple := fmt.Sprintf("(%s)", strings.Join(quotedSortedPKs, ", "))
	placeholderTuple := fmt.Sprintf("(%s)", strings.Join(placeholders, ", "))
	whereClause := fmt.Sprintf("%s > %s", whereTuple, placeholderTuple)
	return whereClause, lastPKValues
}

// revertFKsWithContext memanggil fungsi revert dengan context.
func (f *Orchestrator) revertFKsWithContext(ctx context.Context, revertFunc func() error) error {
	if revertFunc == nil {
		return nil
	}
	done := make(chan error, 1)
	go func() {
		done <- revertFunc()
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		f.logger.Error("Context cancelled while waiting for FK revert function to complete.", zap.Error(ctx.Err()))
		return fmt.Errorf("revert FKs cancelled by external context: %w", ctx.Err())
	}
}


// toggleForeignKeys menonaktifkan atau mengaktifkan kembali foreign key checks/triggers.
func (f *Orchestrator) toggleForeignKeys(ctx context.Context, conn *db.Connector, enable bool, log *zap.Logger) (revertFunc func() error, err error) {
	if !f.cfg.DisableFKDuringSync && !enable {
		return nil, nil
	}
	if enable && !f.cfg.DisableFKDuringSync {
		return nil, nil
	}

	var disableCmd, enableCmd, initialStateCmd, revertCmd string
	var initialState interface{} = nil
	var stateStr string
	var requiresTx bool = false

	dialect := conn.Dialect
	log = log.With(zap.String("dialect", dialect))

	switch dialect {
	case "mysql":
		initialStateCmd = "SELECT @@FOREIGN_KEY_CHECKS"
		disableCmd = "SET FOREIGN_KEY_CHECKS = 0;"
		enableCmd = "SET FOREIGN_KEY_CHECKS = %v;"
	case "postgres":
		initialStateCmd = "SHOW session_replication_role"
		disableCmd = "SET session_replication_role = replica;"
		enableCmd = "SET session_replication_role = '%s';"
	case "sqlite":
		initialStateCmd = "PRAGMA foreign_keys;"
		disableCmd = "PRAGMA foreign_keys = OFF;"
		enableCmd = "PRAGMA foreign_keys = %v;"
		log.Warn("Toggling foreign keys via PRAGMA in SQLite might only affect the current connection, its effectiveness with connection pools needs verification.")
	default:
		log.Debug("Foreign key toggling not supported for dialect.")
		return nil, nil
	}

	if initialStateCmd != "" {
		readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
		errRead := conn.DB.WithContext(readCtx).Raw(initialStateCmd).Scan(&stateStr).Error
		readCancel()
		if errRead != nil {
			log.Warn("Could not read initial FK/replication state, assuming default for revert.", zap.Error(errRead))
			if dialect == "mysql" { initialState = 1 }
			if dialect == "postgres" { initialState = "origin" }
			if dialect == "sqlite" { initialState = 1 }
		} else {
			log.Info("Read initial FK/replication state.", zap.String("state", stateStr))
			if dialect == "mysql" || dialect == "sqlite" {
				// PERBAIKAN: Gunakan strconv
				initialStateInt, convErr := strconv.Atoi(stateStr)
				if convErr == nil {
					initialState = initialStateInt
				} else {
					log.Warn("Could not convert initial state to int, using raw string for revert.", zap.String("state", stateStr), zap.Error(convErr))
					initialState = stateStr
				}
			} else {
				initialState = stateStr
			}
		}
	} else {
		if dialect == "mysql" { initialState = 1 }
		if dialect == "postgres" { initialState = "origin" }
		if dialect == "sqlite" { initialState = 1 }
		log.Warn("No command to read initial FK state, assuming default for revert.", zap.Any("assumed_initial_state", initialState))
	}

	if dialect == "mysql" || dialect == "sqlite" {
		stateVal := 1
		if stateInt, ok := initialState.(int); ok {
			stateVal = stateInt
		} else if stateStr, ok := initialState.(string); ok {
			// PERBAIKAN: Gunakan strconv
			stateIntConv, convErr := strconv.Atoi(stateStr)
			if convErr == nil { stateVal = stateIntConv }
		}
		enableCmd = fmt.Sprintf(enableCmd, stateVal)
	} else {
		stateVal := "origin"
		if stateStr, ok := initialState.(string); ok {
			stateVal = stateStr
		}
		enableCmd = fmt.Sprintf(enableCmd, stateVal)
	}

	var targetCmd string
	action := "Disabling"
	if enable {
		targetCmd = enableCmd
		revertCmd = disableCmd
		action = "Enabling (Reverting)"
	} else {
		targetCmd = disableCmd
		revertCmd = enableCmd
		action = "Disabling"
	}

	log.Info(action+" foreign key checks/triggers", zap.String("command_being_executed", targetCmd))
	execCtx, execCancel := context.WithTimeout(ctx, 10*time.Second)
	var execErr error
	if requiresTx {
		execErr = conn.DB.WithContext(execCtx).Transaction(func(tx *gorm.DB) error {
			return tx.Exec(targetCmd).Error
		})
	} else {
		execErr = conn.DB.WithContext(execCtx).Exec(targetCmd).Error
	}
	execCancel()

	if execErr != nil {
		return nil, fmt.Errorf("failed to execute '%s': %w", targetCmd, execErr)
	}

	revertFunc = func() error {
		revertCtxInternal, revertCancelInternal := context.WithTimeout(context.Background(), 15*time.Second)
		defer revertCancelInternal()
		log.Info("Executing revert command for foreign key checks/triggers", zap.String("revert_command", revertCmd))
		var revertExecErr error
		if requiresTx {
			revertExecErr = conn.DB.WithContext(revertCtxInternal).Transaction(func(tx *gorm.DB) error {
				return tx.Exec(revertCmd).Error
			})
		} else {
			revertExecErr = conn.DB.WithContext(revertCtxInternal).Exec(revertCmd).Error
		}
		if revertExecErr != nil {
			return fmt.Errorf("failed to execute revert command '%s': %w", revertCmd, revertExecErr)
		}
		return nil
	}

	return revertFunc, nil
}
