// Command api serves the public REST API.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/guangjiandong/cold-chain-monitor/internal/api"
	"github.com/guangjiandong/cold-chain-monitor/internal/config"
	"github.com/guangjiandong/cold-chain-monitor/internal/platform"
	"github.com/guangjiandong/cold-chain-monitor/internal/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "api:", err)
		os.Exit(1)
	}
}

func run() error {
	migrate := flag.Bool("migrate", false, "apply database migrations before serving")
	migrateOnly := flag.Bool("migrate-only", false, "apply database migrations and exit (Kubernetes Job / init container)")
	flag.Parse()

	log := platform.NewLogger("api")
	cfg, err := config.LoadAPI()
	if err != nil {
		return err
	}

	ctx, stop := platform.SignalContext()
	defer stop()

	pool, err := store.Connect(ctx, cfg.Postgres.DSN, cfg.Postgres.MaxConns)
	if err != nil {
		return err
	}
	defer pool.Close()

	if *migrate || *migrateOnly {
		if err := store.Migrate(ctx, pool, log); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
		if *migrateOnly {
			return nil
		}
	}

	srv := api.NewServer(store.New(pool), log)
	httpSrv := platform.NewHTTPServer(cfg.ListenAddr, srv.Handler())
	return platform.Serve(ctx, httpSrv, cfg.ShutdownTimeout, log)
}
