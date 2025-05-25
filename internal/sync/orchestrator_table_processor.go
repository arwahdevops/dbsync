// internal/sync/orchestrator_table_processor.go
package sync

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/arwahdevops/dbsync/internal/config"  // Diperlukan untuk config.SchemaSyncNone
	"github.com/arwahdevops/dbsync/internal/metrics" // Diperlukan untuk metrics.Store
)

// processTableInput dan processTableResult didefinisikan di syncer_types.go

// processSingleTable menangani seluruh siklus hidup sinkronisasi untuk satu tabel.
// Ini adalah method dari Orchestrator.
func (f *Orchestrator) processSingleTable(input processTableInput) processTableResult {
	log := input.logger.With(zap.String("table", input.tableName))
	startTime := time.Now()
	result := processTableResult{Table: input.tableName} // processTableResult adalah alias SyncResult

	// Defer untuk logging hasil akhir dan metrik durasi tabel
	defer func() {
		result.Duration = time.Since(startTime)
		input.metrics.TableSyncDuration.WithLabelValues(input.tableName).Observe(result.Duration.Seconds())
		f.logFinalTableResult(log, result, input.metrics) // Panggil sebagai method f
	}()

	// Context dengan timeout untuk seluruh pemrosesan tabel ini
	tableCtx, cancelTableCtx := context.WithTimeout(input.ctx, input.cfg.TableTimeout)
	defer cancelTableCtx()

	// --- Tahap 1: Analisis Skema & Pembuatan DDL ---
	log.Info("Starting schema analysis and DDL generation phase.")
	schemaResult, schemaErr := input.schemaSyncer.SyncTableSchema(tableCtx, input.tableName, input.cfg.SchemaSyncStrategy)

	if schemaErr != nil {
		log.Error("Schema analysis/generation failed", zap.Error(schemaErr))
		result.SchemaAnalysisError = schemaErr
		input.metrics.SyncErrorsTotal.WithLabelValues("schema_analysis", input.tableName).Inc()
		result.Skipped = true
		result.SkipReason = fmt.Sprintf("Schema analysis/generation failed (SkipFailedTables=%t)", input.cfg.SkipFailedTables)
		return result
	}

	if input.cfg.SchemaSyncStrategy == config.SchemaSyncNone {
		result.SchemaSyncSkipped = true
		log.Info("Schema synchronization explicitly skipped due to 'none' strategy.")
	} else if schemaResult != nil {
		log.Info("Schema analysis/generation phase complete.",
			zap.Bool("table_ddl_present", schemaResult.TableDDL != ""),
			zap.Int("index_ddls_count", len(schemaResult.IndexDDLs)),
			zap.Int("constraint_ddls_count", len(schemaResult.ConstraintDDLs)),
			zap.Strings("detected_pks_for_data_sync", schemaResult.PrimaryKeys),
		)
	}

	if err := tableCtx.Err(); err != nil {
		log.Error("Context cancelled or timed out after schema analysis/generation phase", zap.Error(err))
		if result.SchemaAnalysisError == nil {
			result.SchemaAnalysisError = fmt.Errorf("context error after schema analysis: %w", err)
		}
		result.Skipped = true
		result.SkipReason = "Context cancelled/timed out after schema analysis/generation"
		return result
	}

	// --- Tahap 2: Eksekusi DDL Struktur Tabel (CREATE/ALTER) ---
	if !result.SchemaSyncSkipped && schemaResult != nil && schemaResult.TableDDL != "" {
		log.Info("Starting table DDL (CREATE/ALTER) execution phase.")
		tableStructureDDLs := &SchemaExecutionResult{TableDDL: schemaResult.TableDDL}
		schemaExecErr := input.schemaSyncer.ExecuteDDLs(tableCtx, input.tableName, tableStructureDDLs)

		if schemaExecErr != nil {
			log.Error("Table DDL (CREATE/ALTER) execution failed", zap.Error(schemaExecErr))
			result.SchemaExecutionError = schemaExecErr
			input.metrics.SyncErrorsTotal.WithLabelValues("schema_execution", input.tableName).Inc()
			result.Skipped = true
			result.SkipReason = fmt.Sprintf("Table DDL execution failed (SkipFailedTables=%t)", input.cfg.SkipFailedTables)
			return result
		}
		log.Info("Table DDL (CREATE/ALTER) execution phase complete.")
	} else if !result.SchemaSyncSkipped {
		log.Info("No table structure DDL (CREATE/ALTER) to execute for this table.")
	}

	if err := tableCtx.Err(); err != nil {
		log.Error("Context cancelled or timed out after table DDL execution phase", zap.Error(err))
		if result.SchemaExecutionError == nil {
			result.SchemaExecutionError = fmt.Errorf("context error after DDL execution: %w", err)
		}
		result.Skipped = true
		result.SkipReason = "Context cancelled/timed out after table DDL execution"
		return result
	}

	// --- Tahap 3: Sinkronisasi Data ---
	if result.Skipped {
		log.Warn("Skipping data synchronization phase due to prior critical schema errors.",
			zap.NamedError("schema_analysis_err_cause", result.SchemaAnalysisError),
			zap.NamedError("schema_execution_err_cause", result.SchemaExecutionError))
	} else {
		pkColumnsForDataSync := []string{}
		if schemaResult != nil {
			pkColumnsForDataSync = schemaResult.PrimaryKeys
		}

		if len(pkColumnsForDataSync) == 0 && input.cfg.SchemaSyncStrategy != config.SchemaSyncNone {
			log.Warn("Data sync: No primary key identified for table after schema operations. Pagination will not be used, which is unsafe and slow for large tables. Data sync might also fail if upsert requires PKs.",
				zap.Strings("pks_from_schema_result", pkColumnsForDataSync))
		} else if len(pkColumnsForDataSync) == 0 && input.cfg.SchemaSyncStrategy == config.SchemaSyncNone {
			log.Warn("Data sync: Schema strategy is 'none' and no primary key detected via source schema analysis. Attempting full table load without pagination (unsafe for large tables). Data sync might also fail if upsert requires PKs.")
		}

		log.Info("Starting data synchronization phase.")
		rowsSynced, batchesProcessed, dataSyncErr := f.syncData(tableCtx, input.tableName, pkColumnsForDataSync)
		result.RowsSynced = rowsSynced
		result.Batches = batchesProcessed

		if dataSyncErr != nil {
			log.Error("Data synchronization failed", zap.Error(dataSyncErr))
			result.DataError = dataSyncErr
			input.metrics.SyncErrorsTotal.WithLabelValues("data_sync", input.tableName).Inc()
			if !input.cfg.SkipFailedTables {
				log.Warn("Data synchronization failed for table. Since SkipFailedTables is false, this will be treated as a critical failure for the overall sync if not handled at a higher level, but constraint application for this table will still be attempted.")
			} else {
				log.Warn("Data synchronization failed (SkipFailedTables=true). Attempting to apply constraints/indexes if any.")
			}
		} else {
			log.Info("Data synchronization phase complete.")
		}
	}

	if err := tableCtx.Err(); err != nil {
		log.Error("Context cancelled or timed out during/after data synchronization phase", zap.Error(err))
		if result.DataError == nil {
			result.DataError = fmt.Errorf("context error during/after data sync: %w", err)
		}
		if !result.Skipped {
			result.Skipped = true
			result.SkipReason = "Context cancelled/timed out during/after data synchronization"
		}
		return result
	}

	// --- Tahap 4: Eksekusi DDL Indeks & Constraint ---
	if result.Skipped {
		log.Warn("Skipping index and constraint application phase due to prior critical errors or context cancellation for this table.")
	} else if !result.SchemaSyncSkipped && schemaResult != nil && (len(schemaResult.IndexDDLs) > 0 || len(schemaResult.ConstraintDDLs) > 0) {
		log.Info("Starting index and constraint application phase.")
		indexAndConstraintDDLs := &SchemaExecutionResult{
			IndexDDLs:      schemaResult.IndexDDLs,
			ConstraintDDLs: schemaResult.ConstraintDDLs,
		}
		constraintErr := input.schemaSyncer.ExecuteDDLs(tableCtx, input.tableName, indexAndConstraintDDLs)

		if constraintErr != nil {
			log.Error("Failed to apply indexes/constraints after data sync", zap.Error(constraintErr))
			result.ConstraintExecutionError = constraintErr
			input.metrics.SyncErrorsTotal.WithLabelValues("constraint_apply", input.tableName).Inc()
		} else {
			log.Info("Index and constraint application phase complete.")
		}
	} else if !result.SchemaSyncSkipped {
		log.Info("No index or constraint DDLs to execute for this table.")
	}

	if err := tableCtx.Err(); err != nil {
		log.Error("Context cancelled or timed out during/after index/constraint application phase", zap.Error(err))
		if result.ConstraintExecutionError == nil {
			result.ConstraintExecutionError = fmt.Errorf("context error during constraint application: %w", err)
		}
		if !result.Skipped {
			result.Skipped = true
			result.SkipReason = "Context cancelled/timed out during/after index/constraint application"
		}
	}
	return result
}

// logFinalTableResult mencatat hasil akhir pemrosesan tabel.
// Ini adalah method dari Orchestrator.
func (f *Orchestrator) logFinalTableResult(log *zap.Logger, result processTableResult, metricsStore *metrics.Store) {
	logFields := []zap.Field{
		zap.Duration("duration", result.Duration),
		zap.Int64("rows_synced", result.RowsSynced),
		zap.Int("batches_processed", result.Batches),
		zap.Bool("schema_sync_explicitly_disabled", result.SchemaSyncSkipped),
	}

	hasCriticalErrorExcludingConstraints := result.SchemaAnalysisError != nil || result.SchemaExecutionError != nil || result.DataError != nil
	hasOnlyConstraintError := !hasCriticalErrorExcludingConstraints && result.ConstraintExecutionError != nil

	if result.Skipped {
		logFields = append(logFields, zap.String("skip_reason", result.SkipReason))
		if result.SchemaAnalysisError != nil {
			logFields = append(logFields, zap.NamedError("schema_analysis_error_cause", result.SchemaAnalysisError))
		}
		if result.SchemaExecutionError != nil {
			logFields = append(logFields, zap.NamedError("schema_execution_error_cause", result.SchemaExecutionError))
		}
		if result.DataError != nil {
			logFields = append(logFields, zap.NamedError("data_error_cause", result.DataError))
		}
		if result.ConstraintExecutionError != nil && (result.SchemaAnalysisError != nil || result.SchemaExecutionError != nil || result.DataError != nil) {
			logFields = append(logFields, zap.NamedError("constraint_execution_error_after_skip_trigger", result.ConstraintExecutionError))
		}
		log.Warn("Table processing was SKIPPED or INTERRUPTED by error/cancellation", logFields...)
	} else if hasCriticalErrorExcludingConstraints {
		if result.SchemaAnalysisError != nil {
			logFields = append(logFields, zap.NamedError("schema_analysis_error", result.SchemaAnalysisError))
		}
		if result.SchemaExecutionError != nil {
			logFields = append(logFields, zap.NamedError("schema_execution_error", result.SchemaExecutionError))
		}
		if result.DataError != nil {
			logFields = append(logFields, zap.NamedError("data_error", result.DataError))
		}
		if result.ConstraintExecutionError != nil {
			logFields = append(logFields, zap.NamedError("constraint_execution_error_concurrent_with_critical", result.ConstraintExecutionError))
		}
		log.Error("Table synchronization finished with CRITICAL ERRORS", logFields...)
	} else if hasOnlyConstraintError {
		logFields = append(logFields, zap.NamedError("constraint_execution_error", result.ConstraintExecutionError))
		log.Warn("Table data synchronization SUCCEEDED, but applying constraints/indexes FAILED", logFields...)
		metricsStore.TableSyncSuccessTotal.WithLabelValues(result.Table).Inc()
	} else {
		log.Info("Table synchronization finished SUCCESSFULLY", logFields...)
		metricsStore.TableSyncSuccessTotal.WithLabelValues(result.Table).Inc()
	}
}
