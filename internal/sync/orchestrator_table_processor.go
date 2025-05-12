// internal/sync/orchestrator_table_processor.go
package sync

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/arwahdevops/dbsync/internal/config"
	"github.com/arwahdevops/dbsync/internal/metrics"
)

// processSingleTable menangani seluruh siklus hidup sinkronisasi untuk satu tabel.
func (f *Orchestrator) processSingleTable(input processTableInput) processTableResult {
	log := input.logger.With(zap.String("table", input.tableName))
	startTime := time.Now()
	result := processTableResult{Table: input.tableName}

	// Defer untuk logging hasil akhir dan metrik durasi tabel
	defer func() {
		result.Duration = time.Since(startTime)
		input.metrics.TableSyncDuration.WithLabelValues(input.tableName).Observe(result.Duration.Seconds())
		logFinalTableResult(log, result, input.metrics) // logFinalTableResult tidak berubah
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
		// Jika analisis skema gagal, tidak ada gunanya melanjutkan untuk tabel ini, bahkan jika SkipFailedTables=true.
		// SkipFailedTables=true berarti kita lanjut ke *tabel berikutnya*, bukan tahap berikutnya untuk tabel yang gagal ini.
		result.Skipped = true
		result.SkipReason = fmt.Sprintf("Schema analysis/generation failed (SkipFailedTables=%t)", input.cfg.SkipFailedTables)
		return result // Langsung keluar untuk tabel ini
	}

	if input.cfg.SchemaSyncStrategy == config.SchemaSyncNone {
		result.SchemaSyncSkipped = true
		log.Info("Schema synchronization explicitly skipped due to 'none' strategy.")
	} else if schemaResult != nil { // schemaResult tidak akan nil jika schemaErr nil
		log.Info("Schema analysis/generation phase complete.",
			zap.Bool("table_ddl_present", schemaResult.TableDDL != ""),
			zap.Int("index_ddls_count", len(schemaResult.IndexDDLs)),
			zap.Int("constraint_ddls_count", len(schemaResult.ConstraintDDLs)),
			zap.Strings("detected_pks_for_data_sync", schemaResult.PrimaryKeys),
		)
	}

	// Cek context setelah tahap 1
	if err := tableCtx.Err(); err != nil {
		log.Error("Context cancelled or timed out after schema analysis/generation phase", zap.Error(err))
		if result.SchemaAnalysisError == nil { // Hanya set jika belum ada error dari tahap ini
			result.SchemaAnalysisError = fmt.Errorf("context error after schema analysis: %w", err)
		}
		result.Skipped = true
		result.SkipReason = "Context cancelled/timed out after schema analysis/generation"
		return result
	}

	// --- Tahap 2: Eksekusi DDL Struktur Tabel (CREATE/ALTER) ---
	// Hanya jalankan jika skema tidak diskip dan ada DDL tabel
	if !result.SchemaSyncSkipped && schemaResult != nil && schemaResult.TableDDL != "" {
		log.Info("Starting table DDL (CREATE/ALTER) execution phase.")
		tableStructureDDLs := &SchemaExecutionResult{TableDDL: schemaResult.TableDDL}
		// schemaSyncer.ExecuteDDLs akan menjalankan dalam transaksi (jika didukung DB)
		schemaExecErr := input.schemaSyncer.ExecuteDDLs(tableCtx, input.tableName, tableStructureDDLs)
		
		if schemaExecErr != nil {
			log.Error("Table DDL (CREATE/ALTER) execution failed", zap.Error(schemaExecErr))
			result.SchemaExecutionError = schemaExecErr
			input.metrics.SyncErrorsTotal.WithLabelValues("schema_execution", input.tableName).Inc()
			// Jika eksekusi DDL struktur tabel gagal, kita tidak boleh melanjutkan ke data sync untuk tabel ini.
			result.Skipped = true
			result.SkipReason = fmt.Sprintf("Table DDL execution failed (SkipFailedTables=%t)", input.cfg.SkipFailedTables)
			return result // Langsung keluar untuk tabel ini
		}
		log.Info("Table DDL (CREATE/ALTER) execution phase complete.")
	} else if !result.SchemaSyncSkipped {
		log.Info("No table structure DDL (CREATE/ALTER) to execute for this table.")
	}

	// Cek context setelah tahap 2
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
	// Lanjutkan ke data sync hanya jika tidak ada error skema sebelumnya (analysis atau execution).
	// `result.Skipped` akan true jika ada error skema sebelumnya dan kita sudah return.
	// Kondisi ini sebagai double check.
	if result.Skipped {
		log.Warn("Skipping data synchronization phase due to prior critical schema errors.",
			zap.NamedError("schema_analysis_err_cause", result.SchemaAnalysisError),
			zap.NamedError("schema_execution_err_cause", result.SchemaExecutionError))
	} else {
		pkColumnsForDataSync := []string{}
		if schemaResult != nil { // schemaResult pasti tidak nil di sini jika tidak ada error skema
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
			// Jika data sync gagal, kita mungkin masih ingin mencoba menerapkan constraint/indeks,
			// tergantung pada cfg.SkipFailedTables.
			// Jika !cfg.SkipFailedTables, Orchestrator akan menghentikan pemrosesan tabel *lain*.
			// Untuk tabel *ini*, kita catat errornya dan biarkan tahap constraint/index berjalan jika diinginkan.
			// Keputusan untuk `result.Skipped = true` di sini bergantung apakah kita mau menghentikan
			// tahap constraint jika data gagal. Saat ini, tidak diset true, jadi constraint akan dicoba.
			if !input.cfg.SkipFailedTables {
				log.Warn("Data synchronization failed for table. Since SkipFailedTables is false, this will be treated as a critical failure for the overall sync if not handled at a higher level, but constraint application for this table will still be attempted.")
				// Tidak set result.Skipped = true di sini agar constraint tetap dicoba.
				// Exit code di main.go akan menangani ini sebagai error.
			} else {
				log.Warn("Data synchronization failed (SkipFailedTables=true). Attempting to apply constraints/indexes if any.")
			}
		} else {
			log.Info("Data synchronization phase complete.")
		}
	}

	// Cek context setelah tahap 3
	if err := tableCtx.Err(); err != nil {
		log.Error("Context cancelled or timed out during/after data synchronization phase", zap.Error(err))
		if result.DataError == nil {
			result.DataError = fmt.Errorf("context error during/after data sync: %w", err)
		}
		result.Skipped = true // Jika context batal di sini, skip sisa tahap untuk tabel ini
		result.SkipReason = "Context cancelled/timed out during/after data synchronization"
		return result
	}

	// --- Tahap 4: Eksekusi DDL Indeks & Constraint ---
	// Lanjutkan ke tahap ini meskipun ada DataError (jika SkipFailedTables=true),
	// tapi jangan jika Skema gagal atau context sudah batal.
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
			// Error di sini tidak akan membuat result.Skipped = true, karena data mungkin sudah masuk.
			// Ini akan dilaporkan sebagai warning/error di summary akhir.
		} else {
			log.Info("Index and constraint application phase complete.")
		}
	} else if !result.SchemaSyncSkipped {
		log.Info("No index or constraint DDLs to execute for this table.")
	}

    // Cek context terakhir kali sebelum return
	if err := tableCtx.Err(); err != nil {
		log.Error("Context cancelled or timed out during/after index/constraint application phase", zap.Error(err))
		if result.ConstraintExecutionError == nil {
			result.ConstraintExecutionError = fmt.Errorf("context error during constraint application: %w", err)
		}
		// result.Skipped sudah mungkin true dari tahap sebelumnya jika context error.
		// Jika belum, set sekarang.
		if !result.Skipped {
			result.Skipped = true
			result.SkipReason = "Context cancelled/timed out during/after index/constraint application"
		}
	}

	return result
}

// logFinalTableResult (tetap sama)
func logFinalTableResult(log *zap.Logger, result processTableResult, metricsStore *metrics.Store) {
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
		// Tambahkan error penyebab skip jika ada
		if result.SchemaAnalysisError != nil { logFields = append(logFields, zap.NamedError("schema_analysis_error_cause", result.SchemaAnalysisError)) }
		if result.SchemaExecutionError != nil { logFields = append(logFields, zap.NamedError("schema_execution_error_cause", result.SchemaExecutionError)) }
		if result.DataError != nil { logFields = append(logFields, zap.NamedError("data_error_cause", result.DataError)) }
		if result.ConstraintExecutionError != nil && (result.SchemaAnalysisError != nil || result.SchemaExecutionError != nil || result.DataError != nil) {
		    // Hanya log constraint error sebagai 'cause' jika ada error lain yang menyebabkan skip
		    logFields = append(logFields, zap.NamedError("constraint_execution_error_after_skip_trigger", result.ConstraintExecutionError))
		}
		log.Warn("Table processing was SKIPPED or INTERRUPTED by error/cancellation", logFields...)
	} else if hasCriticalErrorExcludingConstraints {
		if result.SchemaAnalysisError != nil { logFields = append(logFields, zap.NamedError("schema_analysis_error", result.SchemaAnalysisError)) }
		if result.SchemaExecutionError != nil { logFields = append(logFields, zap.NamedError("schema_execution_error", result.SchemaExecutionError)) }
		if result.DataError != nil { logFields = append(logFields, zap.NamedError("data_error", result.DataError)) }
		// Jika ada error kritis, constraint error (jika ada) adalah sekunder.
		if result.ConstraintExecutionError != nil { logFields = append(logFields, zap.NamedError("constraint_execution_error_concurrent_with_critical", result.ConstraintExecutionError)) }
		log.Error("Table synchronization finished with CRITICAL ERRORS", logFields...)
	} else if hasOnlyConstraintError { // Tidak ada error kritis, hanya error constraint
		logFields = append(logFields, zap.NamedError("constraint_execution_error", result.ConstraintExecutionError))
		log.Warn("Table data synchronization SUCCEEDED, but applying constraints/indexes FAILED", logFields...)
		metricsStore.TableSyncSuccessTotal.WithLabelValues(result.Table).Inc() // Anggap data sukses
	} else { // Sukses penuh
		log.Info("Table synchronization finished SUCCESSFULLY", logFields...)
		metricsStore.TableSyncSuccessTotal.WithLabelValues(result.Table).Inc()
	}
}
