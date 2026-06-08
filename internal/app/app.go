package app

import (
	"context"
	"database/sql"
	"path/filepath"
	"time"

	"github.com/maksimilian/duckdb-orchestrator/internal/config"
	"github.com/maksimilian/duckdb-orchestrator/internal/dag"
	"github.com/maksimilian/duckdb-orchestrator/internal/pipeline"
	"github.com/maksimilian/duckdb-orchestrator/internal/sqlrender"
)

type Application struct {
	runConfig       pipeline.RunConfig
	modelsToProcess [][]config.ModelConfig
}

const (
	SetupDir    = "setup"
	ProfilePath = SetupDir + "/profile.yml"
	SourcesPath = SetupDir + "/sources.yml"
)

func Build(profile *config.Profile) (*Application, error) {
	rc := pipeline.RunConfig{
		Threads:     profile.Threads,
		FullRefresh: profile.FullRefresh,
	}

	sources, err := config.LoadSourceCatalog(SourcesPath)
	if err != nil {
		return nil, err
	}

	proj, err := config.LoadProject(
		profile.ModelsFolder,
		sqlrender.ParseSQLFileModelConfig,
		sqlrender.ParseSQLFileRefs,
		sqlrender.ParseSQLFileSources,
	)
	if err != nil {
		return nil, err
	}

	if err := config.ValidateProjectModels(proj); err != nil {
		return nil, err
	}
	if err := config.ValidateProjectSources(proj, sources); err != nil {
		return nil, err
	}

	modelsToProcess, modelRegistry, err := dag.ExecutionPlan(proj)
	if err != nil {
		return nil, err
	}

	rc.Profile = profile
	rc.Sources = sources
	rc.ModelRegistry = modelRegistry
	rc.LogsDir = profile.LogsFolder
	if rc.LogsDir == "" {
		rc.LogsDir = filepath.Join(SetupDir, "logs")
	}

	return &Application{
		runConfig:       rc,
		modelsToProcess: modelsToProcess,
	}, nil
}

func (a *Application) Run(ctx context.Context, db *sql.DB) error {
	return pipeline.ExecuteQueries(ctx, db, a.modelsToProcess, a.runConfig)
}

func SetupDB(ctx context.Context, duckDBFile string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", duckDBFile)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(180 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}
