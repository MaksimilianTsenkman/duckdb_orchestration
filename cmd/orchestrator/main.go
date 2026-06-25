package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/maksimilian/duckdb-orchestrator/internal/app"
	"github.com/maksimilian/duckdb-orchestrator/internal/config"
	_ "github.com/marcboeker/go-duckdb"
)

func main() {
	start := time.Now()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(logger)
	slog.Info("program started")

	profile, err := config.LoadProfile(app.ProfilePath)
	if err != nil {
		slog.Error("load profile", "error", err)
		os.Exit(1)
	}

	application, err := app.Build(profile)
	if err != nil {
		slog.Error("build application", "error", err)
		os.Exit(1)
	}

	db, err := app.SetupDB(ctx, profile.DuckDBFile)
	if err != nil {
		slog.Error("setup duckdb", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := application.Run(ctx, db); err != nil {
		slog.Error("pipeline execution failed", "error", err)
		os.Exit(1)
	}

	slog.Info("pipeline completed", "duration", time.Since(start).String())
}
