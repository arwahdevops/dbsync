package sync

import (
	"context"
	"errors"
	"fmt"
	"sort" // Tetap diperlukan untuk getSortedPKNames (jika PK sumber > 1)
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/arwahdevops/dbsync/internal/db"
	"github.com/arwahdevops/dbsync/internal/utils"
)

// syncData performs the actual data transfer using pagination.
func (f *Orchestrator) syncData(ctx context.Context, table string, srcPKColumns []string) (totalRowsSynced int64, batches int, err error) {
	log := f.logger.With(zap.String("table", table), zap.Strings("source_pk_columns_for_pagination", srcPKColumns))

	// Hitung total baris di sumber untuk logging progress (opsional)
	var totalRows int64 = -1 // Default -1 jika tidak bisa dihitung
	countCtx, countCancel := context.WithTimeout(ctx, 15*time.Second)
	countErr := f.srcConn.DB.WithContext(countCtx).Table(table).Count(&totalRows).Error
	countCancel()

	if countErr != nil {
		// Log error penghitungan, tapi jangan gagalkan sinkronisasi data
		logFields := []zap.Field{zap.Error(countErr)}
		if errors.Is(countErr, context.DeadlineExceeded) {
			logFields = append(logFields, zap.Duration("count_query_timeout", 15*time.Second))
		} else if errors.Is(ctx.Err(), context.Canceled) {
			logFields = append(logFields, zap.NamedError("main_context_error", ctx.Err()))
		}
		log.Warn("Could not count total source rows for progress tracking. Progress percentage will not be available.", logFields...)
	} else {
		log.Info("Approximate total source rows to synchronize", zap.Int64("count", totalRows))
	}

	// Dapatkan Primary Key dari tabel TUJUAN untuk klausa ON CONFLICT
	// Ini dilakukan sekali per tabel.
	var dstPKColumns []string
	var errDstPK error
	if f.dstConn.Dialect != "sqlite" { // SQLite ON CONFLICT tidak selalu butuh nama kolom PK eksplisit jika ROWID
		dstPKCtx, dstPKCancel := context.WithTimeout(ctx, 10*time.Second)
		dstPKColumns, errDstPK = f.getDestinationTablePKs(dstPKCtx, table) // Panggil method Orchestrator
		dstPKCancel()
		if errDstPK != nil {
			log.Error("Failed to get destination primary keys for upsert. Upsert might fail or be inefficient.", zap.Error(errDstPK))
			// Pertimbangkan untuk error di sini atau fallback ke UpdateAll
			// return 0, 0, fmt.Errorf("failed to get destination PKs for table '%s': %w", table, errDstPK)
		} else if len(dstPKColumns) == 0 {
			log.Warn("No primary key found for destination table. ON CONFLICT upsert will fallback to simple inserts or GORM default behavior.", zap.String("table", table))
		}
	} else {
		log.Debug("Skipping explicit destination PK fetch for SQLite ON CONFLICT.")
	}


	// Persiapan Paginasi Sumber
	var orderByClause string
	var quotedSrcPKColumns, srcPKPlaceholders []string
	canPaginate := len(srcPKColumns) > 0

	if canPaginate {
		var sortErr error
		// buildPaginationClauses menggunakan srcPKColumns
		orderByClause, quotedSrcPKColumns, srcPKPlaceholders, sortErr = f.buildPaginationClauses(srcPKColumns, f.srcConn.Dialect)
		if sortErr != nil {
			return 0, 0, fmt.Errorf("failed to build source pagination clauses for table '%s': %w", table, sortErr)
		}
		log.Debug("Using pagination for data sync.", zap.String("order_by_clause", orderByClause))
	} else {
		log.Warn("No source primary key provided/found for pagination. Attempting full table load in batches without explicit ordering for pagination. This is potentially unsafe and slow for very large tables.")
		orderByClause = "" // Tidak ada order by jika tidak ada PK sumber
	}

	// Toggle Foreign Keys (jika dikonfigurasi)
	var revertFKsFunc func() error
	if f.cfg.DisableFKDuringSync && (f.dstConn.Dialect == "mysql" || f.dstConn.Dialect == "postgres") { // SQLite PRAGMA FK lebih rumit dengan pool
		var disableErr error
		// toggleForeignKeys sekarang menjadi method Orchestrator
		revertFKsFunc, disableErr = f.toggleForeignKeys(ctx, f.dstConn, false, log) // false untuk disable
		if disableErr != nil {
			log.Error("Attempt to disable foreign keys before data sync failed. Proceeding with FKs in current state.", zap.Error(disableErr))
		}
		if revertFKsFunc != nil {
			defer func() { // Pastikan revert dipanggil
				log.Info("Attempting to re-enable/revert foreign keys after data sync completion or failure.")
				revertCtx, revertCancel := context.WithTimeout(context.Background(), 30*time.Second) // Context baru untuk revert
				defer revertCancel()
				// revertFKsWithContext sekarang menjadi method Orchestrator
				if enableErr := f.revertFKsWithContext(revertCtx, revertFKsFunc); enableErr != nil {
					log.Error("Failed to re-enable/revert foreign keys after data sync.", zap.Error(enableErr))
				} else {
					log.Info("Foreign keys re-enabled/reverted successfully.")
				}
			}()
		}
	}

	var lastSrcPKValues []interface{} // Untuk paginasi sumber
	totalRowsSynced = 0
	batches = 0
	progressLogThreshold := 100 // Log progress setiap X batch
	noDataStreak := 0           // Untuk deteksi masalah paginasi

	for {
		if err := ctx.Err(); err != nil {
			log.Warn("Context cancelled or timed out during data sync loop for table.", zap.Error(err))
			return totalRowsSynced, batches, err
		}

		var batchData []map[string]interface{}
		query := f.srcConn.DB.WithContext(ctx).Table(table)

		if canPaginate && lastSrcPKValues != nil {
			// Pastikan quotedSrcPKColumns dan srcPKPlaceholders sudah diinisialisasi dengan benar
			if len(quotedSrcPKColumns) == 0 || len(srcPKPlaceholders) == 0 {
				return totalRowsSynced, batches, fmt.Errorf("internal error: source pagination columns/placeholders not initialized correctly for table '%s'", table)
			}
			// buildWhereClause menggunakan quotedSrcPKColumns dan srcPKPlaceholders
			whereClause, args := f.buildWhereClause(quotedSrcPKColumns, srcPKPlaceholders, lastSrcPKValues)
			log.Debug("Applying WHERE clause for next source page", zap.String("where_clause", whereClause), zap.Any("args", args))
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
			if lastSrcPKValues == nil && totalRowsSynced == 0 { // Batch pertama kosong
				log.Info("Source table appears to be empty or first fetch returned no data.")
			} else {
				log.Info("No more data found in source table for this page, or table synchronization complete.")
			}
			if canPaginate && lastSrcPKValues != nil { // Jika sudah ada data, dan batch berikutnya kosong
				noDataStreak++
				if noDataStreak >= 3 { // Batas untuk deteksi masalah
					log.Error("Potential pagination issue: Multiple consecutive empty batches received after processing some data. This might indicate an unstable sort order or issues with PK values if they are being modified during sync.",
						zap.Any("last_src_pk_values_before_empty_streak", lastSrcPKValues))
					return totalRowsSynced, batches, fmt.Errorf("potential pagination issue: multiple empty batches for table '%s' after %d rows", table, totalRowsSynced)
				}
			}
			break // Keluar dari loop for jika tidak ada data lagi
		}
		noDataStreak = 0 // Reset jika ada data

		log.Debug("Attempting to sync batch to destination database.", zap.Int("rows_to_sync_in_batch", rowsInCurrentBatch))
		syncBatchStartTime := time.Now()
		// Kirim dstPKColumns ke syncBatchWithRetry
		batchSyncErr := f.syncBatchWithRetry(ctx, table, batchData, dstPKColumns)
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
			// getSortedPKNames menggunakan srcPKColumns
			sortedSrcPKNames, pkSortErr := f.getSortedPKNames(srcPKColumns)
			if pkSortErr != nil {
				return totalRowsSynced, batches, fmt.Errorf("failed to get sorted source PK names for pagination update in table '%s': %w", table, pkSortErr)
			}

			newLastSrcPKValues := make([]interface{}, len(sortedSrcPKNames))
			pkValueFound := true
			missingPKName := ""
			for i, pkName := range sortedSrcPKNames {
				val, ok := lastRowInBatch[pkName]
				if !ok {
					log.Error("Source primary key column missing in fetched source data. Cannot reliably continue pagination.",
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
				newLastSrcPKValues[i] = val
			}
			if !pkValueFound {
				return totalRowsSynced, batches, fmt.Errorf("source primary key column '%s' missing in fetched data for table '%s', pagination cannot continue", missingPKName, table)
			}
			lastSrcPKValues = newLastSrcPKValues
		} else { // Tidak ada paginasi, satu batch besar
			if totalRows > 0 && totalRowsSynced < totalRows {
				log.Warn("Full table load was expected (no pagination), but rows synced is less than total count. This might happen if source table changed during sync.",
					zap.Int64("rows_synced_no_pagination", totalRowsSynced),
					zap.Int64("expected_total_rows", totalRows))
			}
			// Jika tidak ada paginasi, setelah satu batch (atau batch pertama jika tabel besar), kita selesai.
			// Namun, query.Limit(f.cfg.BatchSize) tetap berlaku, jadi ini akan memproses per batch
			// tapi tanpa kriteria > lastPK. Jika tabel sangat besar, ini akan terus mengambil batch
			// berikutnya sampai kosong.
			// Jika rowsInCurrentBatch < f.cfg.BatchSize, itu juga indikasi akhir data.
			if rowsInCurrentBatch < f.cfg.BatchSize {
				log.Info("Last batch fetched for non-paginated table or table smaller than batch size.")
				break
			}
		}

		// Logging Progress
		if batches%progressLogThreshold == 0 || (totalRows >= 0 && totalRowsSynced >= totalRows && rowsInCurrentBatch > 0 && totalRowsSynced > 0 /* hanya log jika ada progress */) {
			progressPercent := float64(-1) // Default jika totalRows tidak diketahui
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

// syncBatchWithRetry melakukan upsert batch data ke tabel tujuan dengan retry.
// Sekarang menerima dstPKColumns.
func (f *Orchestrator) syncBatchWithRetry(ctx context.Context, table string, batch []map[string]interface{}, dstPKColumns []string) error {
	log := f.logger.With(zap.String("table", table), zap.Int("current_batch_size", len(batch)))
	if len(batch) == 0 {
		log.Debug("syncBatchWithRetry called with an empty batch, skipping operation.")
		return nil
	}

	var lastErrorEncountered error
	batchOpStartTime := time.Now()
	metricStatusLabel := "failure_unknown" // Default status untuk metrik

	// Defer untuk mencatat metrik durasi dan status batch
	defer func() {
		f.metrics.BatchProcessingDuration.WithLabelValues(table, metricStatusLabel).Observe(time.Since(batchOpStartTime).Seconds())
		if strings.HasPrefix(metricStatusLabel, "success") {
			f.metrics.BatchesProcessedTotal.WithLabelValues(table).Inc()
		} else if metricStatusLabel != "failure_context_cancelled_retry" && metricStatusLabel != "failure_context_cancelled_tx" {
			// Jangan double count error jika sudah dicatat oleh context cancellation
			f.metrics.BatchErrorsTotal.WithLabelValues(table).Inc()
		}
	}()

	// Tentukan kolom yang akan diupdate (semua kolom di batch kecuali PK destinasi)
	var columnsToUpdateClause []string
	var updateAllNonPK bool = true // Default behavior jika tidak ada kolom spesifik untuk diupdate

	if len(batch) > 0 && len(dstPKColumns) > 0 { // Hanya jika ada PK destinasi dan data
		firstRow := batch[0]
		isDstPK := make(map[string]bool)
		for _, pk := range dstPKColumns {
			isDstPK[pk] = true
		}
		tempColsToUpdate := make([]string, 0, len(firstRow))
		for colName := range firstRow {
			if !isDstPK[colName] {
				tempColsToUpdate = append(tempColsToUpdate, colName)
			}
		}
		if len(tempColsToUpdate) > 0 {
			columnsToUpdateClause = tempColsToUpdate
			updateAllNonPK = false // Kita punya daftar kolom spesifik
		} else {
			// Hanya ada PK, atau semua kolom di batch adalah PK. Tidak ada yang diupdate.
			log.Info("Batch contains only destination PK columns or no other columns to update. Will use ON CONFLICT DO NOTHING if PKs are specified.", zap.Strings("dst_pk_columns", dstPKColumns))
			// updateAllNonPK tetap true, GORM akan mencoba UpdateAll, tapi jika dstPKColumns ada, kita akan switch ke DO NOTHING di bawah
		}
	}


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
				// Kembalikan error gabungan dari context dan error DB terakhir
				return fmt.Errorf("context cancelled waiting to retry batch insert for table '%s' (attempt %d): %w; last DB error: %v", table, attempt+1, ctx.Err(), lastErrorEncountered)
			}
		}

		// Membangun klausa ON CONFLICT
		var conflictClause clause.Expression
		if len(dstPKColumns) > 0 { // Jika kita tahu PK destinasi
			conflictCols := make([]clause.Column, len(dstPKColumns))
			for i, pk := range dstPKColumns {
				conflictCols[i] = clause.Column{Name: pk}
			}
			if !updateAllNonPK && len(columnsToUpdateClause) > 0 { // Ada kolom spesifik untuk diupdate
				log.Debug("Building ON CONFLICT with specific columns to update.", zap.Strings("update_columns", columnsToUpdateClause))
				conflictClause = clause.OnConflict{
					Columns:   conflictCols,
					DoUpdates: clause.AssignmentColumns(columnsToUpdateClause), // SET col1=EXCLUDED.col1, ...
				}
			} else { // Tidak ada kolom non-PK untuk diupdate, atau fallback jika columnsToUpdateClause kosong
				log.Debug("Building ON CONFLICT DO NOTHING as no non-PK columns to update or fallback.", zap.Strings("pk_columns", dstPKColumns))
				conflictClause = clause.OnConflict{
					Columns:   conflictCols,
					DoNothing: true,
				}
			}
		} else { // Tidak tahu PK destinasi atau SQLite, gunakan UpdateAll atau biarkan GORM default
			if f.dstConn.Dialect == "sqlite" && len(batch) > 0 {
                 // Untuk SQLite, jika kita tidak tahu PK eksplisit, kita bisa coba dengan ROWID
                 // atau biarkan UpdateAll, tapi UpdateAll mungkin tidak selalu robust tanpa PK
                 // ON CONFLICT (ROWID) DO UPDATE ...
                 // Namun, GORM mungkin tidak langsung mendukung ini.
                 // clause.OnConflict{UpdateAll: true} adalah fallback yang paling umum untuk GORM.
                 log.Debug("Using ON CONFLICT with UpdateAll=true for SQLite or when destination PKs are unknown.")
                 conflictClause = clause.OnConflict{UpdateAll: true}
            } else if len(dstPKColumns) == 0 && f.dstConn.Dialect != "sqlite" {
                 log.Warn("No destination PKs known and not SQLite. Upsert might fail or lead to duplicates. Falling back to GORM default UpdateAll.")
                 conflictClause = clause.OnConflict{UpdateAll: true}
            } else {
                 // Seharusnya sudah ditangani oleh kondisi len(dstPKColumns) > 0 di atas.
                 // Ini sebagai fallback jika ada kasus yang terlewat.
                 log.Debug("Using default GORM ON CONFLICT (UpdateAll=true) due to unhandled PK/dialect combination.")
                 conflictClause = clause.OnConflict{UpdateAll: true}
            }
		}


		transactionErr := f.dstConn.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			return tx.Table(table).Clauses(conflictClause).CreateInBatches(batch, len(batch)).Error
		})

		if transactionErr == nil {
			metricStatusLabel = "success"
			if attempt > 0 {
				metricStatusLabel = "success_retry"
				log.Info("Batch insert/upsert succeeded after retry.", zap.Int("successful_attempt_number", attempt+1))
			} else {
				log.Debug("Batch insert/upsert succeeded on the first attempt.")
			}
			return nil // Sukses
		}
		lastErrorEncountered = transactionErr // Simpan error untuk retry berikutnya atau laporan final

		// Cek apakah context dibatalkan selama atau setelah transaksi
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
	} // Akhir loop retry

	// Jika semua retry gagal
	log.Error("Batch insert/upsert failed after exhausting all retry attempts.",
		zap.Int("total_attempts_made", f.cfg.MaxRetries+1),
		zap.NamedError("final_db_error", lastErrorEncountered),
	)
	metricStatusLabel = "failure_max_retries"
	return fmt.Errorf("failed to insert/upsert batch into table '%s' after %d retries: %w", table, f.cfg.MaxRetries, lastErrorEncountered)
}


// --- Helper Pagination (milik Orchestrator) ---
// Diasumsikan pkColumns adalah untuk SUMBER DB
func (f *Orchestrator) getSortedPKNames(pkColumns []string) ([]string, error) {
	if len(pkColumns) == 0 {
		return []string{}, nil // Tidak ada PK, tidak ada yang diurutkan
	}
	// Buat salinan untuk menghindari modifikasi slice asli
	sortedPKs := make([]string, len(pkColumns))
	copy(sortedPKs, pkColumns)
	sort.Strings(sortedPKs) // Urutkan nama kolom PK secara alfabetis
	return sortedPKs, nil
}

// Diasumsikan pkColumns adalah untuk SUMBER DB
func (f *Orchestrator) buildPaginationClauses(pkColumns []string, dialect string) (orderBy string, quotedPKs []string, placeholders []string, err error) {
	if len(pkColumns) == 0 {
		err = fmt.Errorf("cannot build pagination clauses: no primary keys provided")
		return
	}
	// Urutan kolom dalam ORDER BY harus konsisten. Jika pkColumns dari GetPrimaryKeys sudah terurut (misal oleh ORDINAL_POSITION),
	// maka sorting berdasarkan nama di sini mungkin mengubahnya.
	// Untuk konsistensi, kita gunakan urutan dari pkColumns apa adanya, atau urutkan secara eksplisit jika perlu.
	// Saat ini, GetPrimaryKeys di SchemaSyncer sudah mengurutkan PK berdasarkan nama.
	// Jika GetPrimaryKeys di Orchestrator (getDestinationTablePKs) mengambil berdasarkan ORDINAL_POSITION, itu lebih baik.
	// Untuk paginasi SUMBER, kita gunakan urutan apa adanya dari srcPKColumns (yang mungkin sudah diurutkan oleh SchemaSyncer.GetPrimaryKeys)

	// Kita akan mengurutkan pkColumns berdasarkan nama untuk determinisme jika urutan aslinya tidak dijamin/penting.
	// Namun, untuk PK komposit, urutan dalam ORDER BY PENTING dan harus sama dengan urutan di WHERE (...pk_cols...) > (...last_pk_values...).
	// Fungsi getSortedPKNames akan mengurutkan berdasarkan nama. Ini mungkin bukan yang ideal untuk ORDER BY jika PK komposit punya urutan spesifik.
	// Untuk saat ini, kita pakai sorted by name.
	sortedPKsForPagination, _ := f.getSortedPKNames(pkColumns)


	quotedPKs = make([]string, len(sortedPKsForPagination))
	placeholders = make([]string, len(sortedPKsForPagination))
	orderByParts := make([]string, len(sortedPKsForPagination))

	for i, pk := range sortedPKsForPagination {
		quotedPKs[i] = utils.QuoteIdentifier(pk, dialect)
		placeholders[i] = "?"
		orderByParts[i] = quotedPKs[i] + " ASC" // Selalu ASC untuk paginasi >
	}
	orderBy = strings.Join(orderByParts, ", ")
	return
}

// quotedSortedPKs dan placeholders harus berasal dari urutan yang sama (idealya dari buildPaginationClauses).
// lastPKValues juga harus dalam urutan yang sama.
func (f *Orchestrator) buildWhereClause(quotedSortedPKs []string, placeholders []string, lastPKValues []interface{}) (string, []interface{}) {
	if len(quotedSortedPKs) == 0 { // Seharusnya tidak terjadi jika canPaginate true
		return "1=1", []interface{}{} // Tidak ada filter
	}
	if len(quotedSortedPKs) == 1 {
		// Kasus PK tunggal: "pk_col > ?"
		return fmt.Sprintf("%s > ?", quotedSortedPKs[0]), lastPKValues
	}

	// Kasus PK komposit: (pk1, pk2, ...) > (?, ?, ...)
	// Pastikan jumlahnya cocok
	if len(quotedSortedPKs) != len(placeholders) || len(quotedSortedPKs) != len(lastPKValues) {
		f.logger.Error("Mismatch in counts for building composite PK WHERE clause. This indicates an internal logic error.",
			zap.Int("quoted_pk_count", len(quotedSortedPKs)),
			zap.Int("placeholders_count", len(placeholders)),
			zap.Int("values_count", len(lastPKValues)))
		// Fallback ke kondisi yang tidak akan pernah true, atau error.
		// Atau, fallback ke kondisi PK pertama saja jika darurat (tapi ini bisa salah).
		if len(lastPKValues) > 0 && len(quotedSortedPKs) > 0 {
			f.logger.Warn("Falling back to single PK condition due to count mismatch in composite PK WHERE clause.")
			return fmt.Sprintf("%s > ?", quotedSortedPKs[0]), []interface{}{lastPKValues[0]}
		}
		return "1=0", []interface{}{} // Kondisi yang tidak akan pernah cocok
	}

	whereTuple := fmt.Sprintf("(%s)", strings.Join(quotedSortedPKs, ", "))
	placeholderTuple := fmt.Sprintf("(%s)", strings.Join(placeholders, ", "))
	whereClause := fmt.Sprintf("%s > %s", whereTuple, placeholderTuple)
	return whereClause, lastPKValues
}


// --- Helper FK Toggling (milik Orchestrator) ---
// Ini bisa dipindahkan ke file terpisah jika orchestrator_data_sync.go terlalu besar
func (f *Orchestrator) toggleForeignKeys(ctx context.Context, conn *db.Connector, enable bool, log *zap.Logger) (revertFunc func() error, err error) {
	// Hanya untuk MySQL dan PostgreSQL untuk saat ini
	if conn.Dialect != "mysql" && conn.Dialect != "postgres" {
		log.Debug("Foreign key toggling skipped: not supported for dialect or not needed.", zap.String("dialect", conn.Dialect))
		return nil, nil
	}

	var disableCmd, enableCmd, initialStateCmd, revertCmd string
	var initialState interface{} = nil // Untuk menyimpan state awal (string atau int)
	var stateStr string                // Untuk membaca hasil query state awal
	requiresTx := false                // Beberapa pragma SQLite mungkin perlu transaksi

	dialect := conn.Dialect
	scopedLog := log.With(zap.String("target_dialect_for_fk_toggle", dialect))

	switch dialect {
	case "mysql":
		initialStateCmd = "SELECT @@SESSION.foreign_key_checks;" // Gunakan @@SESSION
		disableCmd = "SET SESSION foreign_key_checks = 0;"
		enableCmd = "SET SESSION foreign_key_checks = %v;" // Akan diisi dengan 0 atau 1
		initialState = 1 // Default MySQL adalah FK aktif (1)
	case "postgres":
		initialStateCmd = "SHOW session_replication_role;"
		disableCmd = "SET session_replication_role = replica;"
		enableCmd = "SET session_replication_role = '%s';" // Akan diisi dengan 'origin' atau state awal lainnya
		scopedLog.Warn("Using SET session_replication_role for PostgreSQL FK toggling. Ensure the database user has appropriate permissions (superuser or REPLICATION attribute). This affects trigger firing, not just FKs.")
		initialState = "origin" // Default PG adalah 'origin' atau 'local'
	default: // Seharusnya tidak sampai sini karena sudah difilter di atas
		return nil, nil
	}

	// Baca state awal
	if initialStateCmd != "" {
		readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
		errRead := conn.DB.WithContext(readCtx).Raw(initialStateCmd).Scan(&stateStr).Error
		readCancel()
		if errRead != nil {
			scopedLog.Warn("Could not read initial FK/replication state from database. Assuming default state for revert operation.", zap.Error(errRead), zap.Any("assumed_default", initialState))
			// initialState sudah di-set ke default di atas
		} else {
			scopedLog.Info("Successfully read initial FK/replication state.", zap.String("retrieved_state", stateStr))
			if dialect == "mysql" { // MySQL @@foreign_key_checks mengembalikan 0 atau 1
				val, convErr := strconv.Atoi(stateStr)
				if convErr == nil {
					initialState = val
				} else {
					scopedLog.Warn("Could not convert initial MySQL foreign_key_checks state string to integer. Using assumed default.", zap.String("state_string", stateStr), zap.Error(convErr), zap.Any("assumed_default", initialState))
				}
			} else { // PostgreSQL session_replication_role adalah string
				initialState = strings.TrimSpace(stateStr)
			}
		}
	}

	// Format perintah enable/revert dengan state awal yang benar
	if dialect == "mysql" {
		// Pastikan initialState adalah int 0 atau 1
		stateToRevertTo := 1 // Default ke enable jika ada masalah konversi
		if stateInt, ok := initialState.(int); ok && (stateInt == 0 || stateInt == 1) {
			stateToRevertTo = stateInt
		}
		revertCmd = fmt.Sprintf(enableCmd, stateToRevertTo) // enableCmd di sini adalah template untuk SET ... = %v
		if enable { // Jika kita ingin mengaktifkan (revert)
		    enableCmd = revertCmd // Perintah untuk enable adalah perintah revert
		} else { // Jika kita ingin menonaktifkan
		    // disableCmd sudah benar (SET ... = 0)
		}

	} else if dialect == "postgres" {
		stateToRevertTo := "origin" // Default revert ke 'origin'
		if stateStrConv, ok := initialState.(string); ok && stateStrConv != "" {
			stateToRevertTo = stateStrConv
		}
		revertCmd = fmt.Sprintf(enableCmd, stateToRevertTo) // enableCmd di sini adalah template untuk SET ... = '%s'
        if enable {
            enableCmd = revertCmd
        }
	}


	var commandToExecute string
	var actionDescription string

	if enable { // Jika parameter `enable` adalah true, berarti kita sedang dalam proses REVERT (mengaktifkan kembali)
		commandToExecute = revertCmd // Gunakan revertCmd yang sudah diformat dengan state awal
		actionDescription = fmt.Sprintf("Re-enabling/Reverting foreign key checks/triggers to initial state ('%v')", initialState)
	} else { // Jika `enable` false, berarti kita sedang MENONAKTIFKAN
		commandToExecute = disableCmd
		actionDescription = "Disabling foreign key checks/triggers"
		// revertCmd sudah disiapkan di atas untuk digunakan oleh fungsi defer
	}


	scopedLog.Info(actionDescription, zap.String("command_to_execute", commandToExecute))
	execCtx, execCancel := context.WithTimeout(ctx, 10*time.Second)
	var executionError error

	if requiresTx { // Tidak ada yang requiresTx saat ini untuk MySQL/PG
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

	// Fungsi revert yang akan dipanggil oleh defer di syncData
	revertFunc = func() error {
		revertInternalCtx, revertInternalCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer revertInternalCancel()
		scopedLog.Info("Executing revert command for foreign key checks/triggers.", zap.String("revert_command_to_execute", revertCmd), zap.Any("target_revert_state", initialState))
		var revertExecErr error
		if requiresTx {
			revertExecErr = conn.DB.WithContext(revertInternalCtx).Transaction(func(tx *gorm.DB) error { return tx.Exec(revertCmd).Error })
		} else {
			revertExecErr = conn.DB.WithContext(revertInternalCtx).Exec(revertCmd).Error
		}

		if revertExecErr != nil {
			scopedLog.Error("Failed to execute revert command for FKs. Database FK state might be inconsistent.",
				zap.String("failed_revert_command", revertCmd),
				zap.Error(revertExecErr))
			return fmt.Errorf("failed to execute FK revert command '%s': %w", revertCmd, revertExecErr)
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
