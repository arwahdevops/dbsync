// internal/sync/orchestrator_table_processor.go
package sync

import (
	"context"
	"fmt" // Dipertahankan untuk fmt.Errorf
	"time"

	"go.uber.org/zap"
	// "gorm.io/gorm" // Tidak dibutuhkan langsung di sini jika data sync dipisah
	// "gorm.io/gorm/clause" // Tidak dibutuhkan langsung di sini

	"github.com/arwahdevops/dbsync/internal/config"
	// "github.com/arwahdevops/dbsync/internal/db" // Tidak dibutuhkan jika koneksi dilewatkan
	"github.com/arwahdevops/dbsync/internal/metrics"
	// "github.com/arwahdevops/dbsync/internal/utils" // Tidak dibutuhkan jika pagination dipisah
)

// processSingleTable menangani seluruh siklus hidup sinkronisasi untuk satu tabel.
// Ini adalah method dari Orchestrator.
func (f *Orchestrator) processSingleTable(input processTableInput) processTableResult {
	log := input.logger.With(zap.String("table", input.tableName))
	startTime := time.Now()
	result := processTableResult{Table: input.tableName}

	defer func() {
		result.Duration = time.Since(startTime)
		input.metrics.TableSyncDuration.WithLabelValues(input.tableName).Observe(result.Duration.Seconds())
		logFinalTableResult(log, result, input.metrics)
	}()

	tableCtx, cancelTableCtx := context.WithTimeout(input.ctx, input.cfg.TableTimeout)
	defer cancelTableCtx()

	// --- Tahap 1: Analisis Skema & Pembuatan DDL ---
	log.Info("Starting schema analysis and DDL generation phase.")
	schemaResult, schemaErr := input.schemaSyncer.SyncTableSchema(tableCtx, input.tableName, input.cfg.SchemaSyncStrategy)
	// ... (logika penanganan error schemaErr seperti sebelumnya) ...
	if schemaErr != nil {
		log.Error("Schema analysis/generation failed", zap.Error(schemaErr))
		result.SchemaAnalysisError = schemaErr
		input.metrics.SyncErrorsTotal.WithLabelValues("schema_analysis", input.tableName).Inc()
		if !input.cfg.SkipFailedTables {
			result.Skipped = true
			result.SkipReason = "Schema analysis/generation failed (SkipFailedTables=false)"
			return result
		}
		result.Skipped = true
		result.SkipReason = "Schema analysis/generation failed (SkipFailedTables=true)"
		return result
	}
	// ... (logika handle strategi 'none' dan logging schemaResult seperti sebelumnya) ...
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
	// ... (cek context error setelah tahap 1 seperti sebelumnya) ...
	if err := tableCtx.Err(); err != nil {
		log.Error("Context cancelled or timed out after schema analysis/generation phase", zap.Error(err))
		result.SchemaAnalysisError = fmt.Errorf("context error after schema analysis: %w", err)
		result.Skipped = true
		result.SkipReason = "Context cancelled/timed out after schema analysis/generation"
		return result
	}


	// --- Tahap 2: Eksekusi DDL Struktur Tabel (CREATE/ALTER) ---
	// ... (logika tahap 2 seperti sebelumnya) ...
	if !result.SchemaSyncSkipped && schemaResult != nil && schemaResult.TableDDL != "" {
		log.Info("Starting table DDL (CREATE/ALTER) execution phase.")
		tableStructureDDLs := &SchemaExecutionResult{TableDDL: schemaResult.TableDDL}
		schemaExecErr := input.schemaSyncer.ExecuteDDLs(tableCtx, input.tableName, tableStructureDDLs)
		if schemaExecErr != nil {
			log.Error("Table DDL (CREATE/ALTER) execution failed", zap.Error(schemaExecErr))
			result.SchemaExecutionError = schemaExecErr
			input.metrics.SyncErrorsTotal.WithLabelValues("schema_execution", input.tableName).Inc()
			if !input.cfg.SkipFailedTables {
				result.Skipped = true
				result.SkipReason = "Table DDL execution failed (SkipFailedTables=false)"
				return result
			}
			log.Warn("Table DDL execution failed (SkipFailedTables=true). Data sync might proceed on an incorrect schema.")
		} else {
			log.Info("Table DDL (CREATE/ALTER) execution phase complete.")
		}
	} else if !result.SchemaSyncSkipped {
		log.Info("No table structure DDL (CREATE/ALTER) to execute for this table.")
	}
    // ... (cek context error setelah tahap 2 seperti sebelumnya) ...
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
	pkColumnsForDataSync := []string{}
	if schemaResult != nil {
		pkColumnsForDataSync = schemaResult.PrimaryKeys
	}
	shouldSkipDataSync := (result.SchemaExecutionError != nil && !input.cfg.SkipFailedTables)

	if !shouldSkipDataSync {
		// ... (logika peringatan PK tetap sama) ...
		if len(pkColumnsForDataSync) == 0 {
			if input.cfg.SchemaSyncStrategy == config.SchemaSyncNone {
				log.Warn("Data sync: Schema strategy is 'none' and no primary key detected. Attempting full table load without pagination (unsafe for large tables).")
			} else {
				log.Warn("Data sync: No primary key found/generated for table. Pagination will not be used, which is unsafe and slow for large tables.")
			}
		}
		log.Info("Starting data synchronization phase.")
		// Panggil f.syncData dari file orchestrator_data_sync.go
		rowsSynced, batchesProcessed, dataSyncErr := f.syncData(tableCtx, input.tableName, pkColumnsForDataSync)
		result.RowsSynced = rowsSynced
		result.Batches = batchesProcessed
		if dataSyncErr != nil {
			// ... (logika penanganan error dataSyncErr seperti sebelumnya) ...
			log.Error("Data synchronization failed", zap.Error(dataSyncErr))
			result.DataError = dataSyncErr
			input.metrics.SyncErrorsTotal.WithLabelValues("data_sync", input.tableName).Inc()
			if !input.cfg.SkipFailedTables {
				result.Skipped = true
				result.SkipReason = "Data synchronization failed (SkipFailedTables=false)"
				return result
			}
			log.Warn("Data synchronization failed (SkipFailedTables=true). Attempting to apply constraints/indexes if any.")
		} else {
			log.Info("Data synchronization phase complete.")
		}
	} else {
		log.Warn("Skipping data synchronization phase due to prior critical schema DDL execution errors and SkipFailedTables=false.",
			zap.NamedError("triggering_schema_error", result.SchemaExecutionError))
	}
	// ... (cek context error setelah tahap 3 seperti sebelumnya) ...
	if err := tableCtx.Err(); err != nil {
		log.Error("Context cancelled or timed out during/after data synchronization phase", zap.Error(err))
		if result.DataError == nil {
			result.DataError = fmt.Errorf("context error during/after data sync: %w", err)
		}
		result.Skipped = true
		result.SkipReason = "Context cancelled/timed out during/after data synchronization"
		return result
	}


	// --- Tahap 4: Eksekusi DDL Indeks & Constraint ---
	// ... (logika tahap 4 seperti sebelumnya) ...
	shouldSkipConstraints := ( (result.SchemaExecutionError != nil || result.DataError != nil) && !input.cfg.SkipFailedTables )
	if !result.SchemaSyncSkipped && schemaResult != nil && (len(schemaResult.IndexDDLs) > 0 || len(schemaResult.ConstraintDDLs) > 0) {
		if shouldSkipConstraints {
			log.Warn("Skipping index/constraint application due to prior critical schema/data errors and SkipFailedTables=false.")
		} else {
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
		}
	} else if !result.SchemaSyncSkipped {
		log.Info("No index or constraint DDLs to execute for this table.")
	}
    // ... (cek context error setelah tahap 4 seperti sebelumnya) ...
	if err := tableCtx.Err(); err != nil {
		log.Error("Context cancelled or timed out during index/constraint application phase", zap.Error(err))
		if result.ConstraintExecutionError == nil {
			result.ConstraintExecutionError = fmt.Errorf("context error during constraint application: %w", err)
		}
	}

	return result
}

// logFinalTableResult adalah helper untuk mencatat hasil akhir pemrosesan tabel.
// (Implementasi tetap sama seperti sebelumnya)
func logFinalTableResult(log *zap.Logger, result processTableResult, metricsStore *metrics.Store) {
	logFields := []zap.Field{
		zap.Duration("duration", result.Duration),
		zap.Int64("rows_synced", result.RowsSynced),
		zap.Int("batches_processed", result.Batches),
		zap.Bool("schema_sync_explicitly_disabled", result.SchemaSyncSkipped),
	}

	hasCriticalError := result.SchemaAnalysisError != nil || result.SchemaExecutionError != nil || result.DataError != nil
	hasConstraintErrorOnly := !hasCriticalError && result.ConstraintExecutionError != nil

	if result.Skipped {
		logFields = append(logFields, zap.String("skip_reason", result.SkipReason))
		if result.SchemaAnalysisError != nil { logFields = append(logFields, zap.NamedError("schema_analysis_error_cause", result.SchemaAnalysisError)) }
		if result.SchemaExecutionError != nil { logFields = append(logFields, zap.NamedError("schema_execution_error_cause", result.SchemaExecutionError)) }
		if result.DataError != nil { logFields = append(logFields, zap.NamedError("data_error_cause", result.DataError)) }
		log.Warn("Table processing was skipped or interrupted by error/cancellation", logFields...)
	} else if hasCriticalError {
		if result.SchemaAnalysisError != nil { logFields = append(logFields, zap.NamedError("schema_analysis_error", result.SchemaAnalysisError)) }
		if result.SchemaExecutionError != nil { logFields = append(logFields, zap.NamedError("schema_execution_error", result.SchemaExecutionError)) }
		if result.DataError != nil { logFields = append(logFields, zap.NamedError("data_error", result.DataError)) }
		if result.ConstraintExecutionError != nil { logFields = append(logFields, zap.NamedError("constraint_execution_error_concurrent", result.ConstraintExecutionError)) }
		log.Error("Table synchronization finished with critical errors", logFields...)
	} else if hasConstraintErrorOnly {
		logFields = append(logFields, zap.NamedError("constraint_execution_error", result.ConstraintExecutionError))
		log.Warn("Table data synchronization SUCCEEDED, but applying constraints/indexes FAILED", logFields...)
		metricsStore.TableSyncSuccessTotal.WithLabelValues(result.Table).Inc()
	} else {
		log.Info("Table synchronization finished successfully", logFields...)
		metricsStore.TableSyncSuccessTotal.WithLabelValues(result.Table).Inc()
	}
}
