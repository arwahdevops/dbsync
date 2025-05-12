// internal/sync/orchestrator_pool.go
package sync

import (
	"context"
	"fmt"
	"sync" // Diperlukan untuk WaitGroup

	"go.uber.org/zap"
	// Tidak perlu impor config, db, metrics, dll. secara langsung jika sudah ada di processTableInput
	// yang didefinisikan di syncer_types.go
)

// Definisi processTableInput dan processTableResult sekarang ada di syncer_types.go

// runTableProcessingPool mengelola worker pool untuk memproses tabel.
// Ini adalah method dari Orchestrator.
func (f *Orchestrator) runTableProcessingPool(ctx context.Context, tables []string) map[string]SyncResult {
	results := make(map[string]SyncResult)
	var wg sync.WaitGroup
	// Buffer channel sejumlah tabel untuk menghindari deadlock jika goroutine exit lebih dulu
	// atau jika context dibatalkan dan kita ingin mengirim hasil skip.
	resultChan := make(chan processTableResult, len(tables))
	// Semaphore untuk membatasi jumlah goroutine yang berjalan secara bersamaan
	sem := make(chan struct{}, f.cfg.Workers)

	for i, tableName := range tables {
		// Cek apakah context utama sudah dibatalkan sebelum memulai goroutine baru
		select {
		case <-ctx.Done():
			f.logger.Warn("Main context cancelled. Halting scheduling of new table processing tasks.",
				zap.String("current_table_not_started", tableName),
				zap.Int("index_of_table", i),
				zap.Error(ctx.Err()))
			f.handleRemainingTablesOnCancel(ctx, tables[i:], results)
			goto endloop // Langsung ke akhir loop jika dibatalkan
		default:
			// Lanjutkan jika context belum dibatalkan
		}

		wg.Add(1)
		go func(tblName string) {
			defer wg.Done()

			// Mencoba mendapatkan slot dari semaphore
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }() // Pastikan slot dilepaskan setelah selesai
			case <-ctx.Done(): // Jika context dibatalkan saat menunggu slot
				f.logger.Warn("Context cancelled while waiting for worker slot to process table.",
					zap.String("table", tblName), zap.Error(ctx.Err()))
				resultChan <- processTableResult{
					Table:      tblName,
					Skipped:    true,
					SkipReason: "Context cancelled while waiting for worker slot",
					DataError:  fmt.Errorf("context cancelled: %w", ctx.Err()),
				}
				return
			}

			// Siapkan input untuk processSingleTable
			// processTableInput didefinisikan di syncer_types.go
			input := processTableInput{
				ctx:          ctx, // Teruskan context utama (processSingleTable akan membuat sub-context dengan timeout)
				tableName:    tblName,
				cfg:          f.cfg,
				logger:       f.logger, // Logger Orchestrator utama
				metrics:      f.metrics,
				schemaSyncer: f.schemaSyncer,
				srcConn:      f.srcConn,
				dstConn:      f.dstConn,
			}
			// Panggil method processSingleTable dari Orchestrator (f)
			// processSingleTable didefinisikan di orchestrator_table_processor.go
			resultChan <- f.processSingleTable(input)
		}(tableName)
	}

endloop:
	// Goroutine untuk menunggu semua worker selesai dan menutup resultChan
	go func() {
		wg.Wait()
		close(resultChan)
		// Tidak perlu close(sem) karena sem digunakan sebagai counter, bukan untuk komunikasi data
		f.logger.Debug("All table processing goroutines in pool have completed their work.")
	}()

	// Kumpulkan hasil dari channel
	for res := range resultChan {
		results[res.Table] = SyncResult(res) // Konversi kembali ke SyncResult
	}
	return results
}

// handleRemainingTablesOnCancel mengisi hasil untuk tabel yang belum sempat diproses
// ketika context utama dibatalkan.
// Ini adalah method dari Orchestrator.
func (f *Orchestrator) handleRemainingTablesOnCancel(ctx context.Context, remainingTables []string, results map[string]SyncResult) {
	if len(remainingTables) > 0 {
		f.logger.Warn("Context cancelled; marking remaining tables as skipped before processing could start.",
			zap.String("first_remaining_table_not_started", remainingTables[0]),
			zap.Int("count_of_remaining_tables_not_started", len(remainingTables)),
			zap.Error(ctx.Err()),
		)
		for _, tbl := range remainingTables {
			if _, exists := results[tbl]; !exists { // Hanya jika belum ada hasil (misalnya dari worker yang sudah jalan)
				results[tbl] = SyncResult{
					Table:      tbl,
					Skipped:    true,
					SkipReason: "Context cancelled before processing could start for this table",
					DataError:  fmt.Errorf("context cancelled: %w", ctx.Err()), // Atau error yang lebih spesifik
				}
			}
		}
	}
}
