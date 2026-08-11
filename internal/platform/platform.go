// Package platform holds process-level plumbing shared by every binary:
// logging setup, signal handling and the HTTP server lifecycle.
package platform

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// NewLogger returns a JSON logger (for log aggregation in CloudWatch) unless
// LOG_FORMAT=text, at the level given by LOG_LEVEL.
func NewLogger(service string) *slog.Logger {
	var level slog.Level
	if err := level.UnmarshalText([]byte(os.Getenv("LOG_LEVEL"))); err != nil {
		level = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	if strings.EqualFold(os.Getenv("LOG_FORMAT"), "text") {
		h = slog.NewTextHandler(os.Stdout, opts)
	} else {
		h = slog.NewJSONHandler(os.Stdout, opts)
	}
	l := slog.New(h).With("service", service)
	slog.SetDefault(l)
	return l
}

// SignalContext returns a context cancelled on SIGINT or SIGTERM. Kubernetes
// sends SIGTERM before killing a pod.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
}

// Serve runs srv until ctx is cancelled, then shuts it down gracefully,
// giving in-flight requests up to shutdownTimeout to finish.
func Serve(ctx context.Context, srv *http.Server, shutdownTimeout time.Duration, log *slog.Logger) error {
	errCh := make(chan error, 1)
	go func() {
		log.Info("http server listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	log.Info("shutting down http server", "addr", srv.Addr)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	return <-errCh
}

// NewHTTPServer returns an http.Server with conservative timeouts so slow
// clients cannot pin connections forever.
func NewHTTPServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
}
