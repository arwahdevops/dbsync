package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof" // Import pprof
	"sync" // Needed for waitgroup in readyz
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/arwahdevops/dbsync/internal/config"
	"github.com/arwahdevops/dbsync/internal/db"
	"github.com/arwahdevops/dbsync/internal/metrics"
)

// RunHTTPServer starts the HTTP server for metrics, health checks, and pprof.
func RunHTTPServer(
	ctx context.Context,
	cfg *config.Config,
	metricsStore *metrics.Store,
	srcConn *db.Connector, // Pass connections for readiness checks
	dstConn *db.Connector,
	logger *zap.Logger,
) {
	log := logger.Named("http-server")
	mux := http.NewServeMux()

	// Metrics endpoint using the custom registry
	mux.Handle("/metrics", promhttp.HandlerFor(metricsStore.Registry, promhttp.HandlerOpts{
		// Opts can be used to customize timeout, error handling etc.
	}))

	// Liveness endpoint
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		// Simple check: if the server is running, it's live
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "OK")
	})

	// Readiness endpoint
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		// Check DB connectivity concurrently
		pingCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second) // Short timeout for pings
		defer cancel()

		var srcErr, dstErr error
		var wg sync.WaitGroup

		// Check connections only if they were successfully established
		if srcConn != nil {
			wg.Add(1)
			go func() { defer wg.Done(); srcErr = srcConn.Ping(pingCtx) }()
		} else {
			srcErr = fmt.Errorf("source connection not established")
		}

		if dstConn != nil {
			wg.Add(1)
			go func() { defer wg.Done(); dstErr = dstConn.Ping(pingCtx) }()
		} else {
			dstErr = fmt.Errorf("destination connection not established")
		}

		wg.Wait() // Wait for pings to complete

		if srcErr == nil && dstErr == nil {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintln(w, "Ready")
		} else {
			log.Warn("Readiness check failed", zap.NamedError("src_ping_error", srcErr), zap.NamedError("dst_ping_error", dstErr))
			w.WriteHeader(http.StatusServiceUnavailable)
			// Provide more context in the error response
			errMsg := fmt.Sprintf("Not Ready: source_db_status=%v, destination_db_status=%v",
				formatPingError(srcErr), formatPingError(dstErr))
			fmt.Fprintln(w, errMsg)
		}
	})


	// Pprof endpoints (conditionally enabled)
	if cfg.EnablePprof {
		log.Info("Enabling pprof endpoints on /debug/pprof/")
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		// Consider adding authentication/authorization layer in front of pprof in real production
	} else {
		log.Info("Pprof endpoints are disabled.")
	}

	addr := fmt.Sprintf(":%d", cfg.MetricsPort)
	server := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Run server in a goroutine so it doesn't block the main sync process
	go func() {
		log.Info("Starting HTTP server", zap.String("address", addr))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("HTTP server ListenAndServe error", zap.Error(err))
			// Potentially trigger a shutdown or alert here
		}
		log.Info("HTTP server stopped listening")
	}()

	// Wait for context cancellation (sent from main) to initiate shutdown
	<-ctx.Done()
	log.Info("Shutting down HTTP server due to context cancellation...")

	// Attempt graceful shutdown with a timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error("HTTP server graceful shutdown failed", zap.Error(err))
	} else {
		log.Info("HTTP server gracefully stopped")
	}
}

// formatPingError provides a user-friendly status string.
func formatPingError(err error) string {
	if err == nil {
		return "OK"
	}
	// You could check for specific error types (e.g., context.DeadlineExceeded)
	return fmt.Sprintf("Error (%v)", err)
}