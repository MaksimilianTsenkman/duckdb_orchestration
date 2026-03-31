package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/maksimilian/duckdb-orchestrator/internal/config"
	"github.com/maksimilian/duckdb-orchestrator/internal/dag"
	"github.com/maksimilian/duckdb-orchestrator/internal/pipeline"
	"github.com/maksimilian/duckdb-orchestrator/internal/sqlrender"
	_ "github.com/marcboeker/go-duckdb"
)

func main() {
	var rc pipeline.RunConfig
	var profilePath string
	flag.IntVar(&rc.Threads, "threads", 1, "Threads amount")
	flag.BoolVar(&rc.FullRefresh, "full-refresh", false, "Full refresh")
	flag.StringVar(&rc.ResultsFile, "results-file", "results.txt", "Results output file")
	flag.StringVar(&profilePath, "profile", "profile.yml", "Path to profile YAML")

	fmt.Println("Program started")
	start := time.Now()

	flag.Parse()
	profile, err := config.LoadProfile(profilePath)
	if err != nil {
		log.Fatalf("load profile: %v", err)
	}
	rc.Profile = profile

	db, err := sql.Open("duckdb", profile.DuckDBFile)
	if err != nil {
		log.Fatalf("open DuckDB: %v", err)
	}
	defer db.Close()
	db.SetConnMaxLifetime(180 * time.Minute)

	rc.Sources, err = config.LoadSourceCatalog(profile.SourcesPath)
	if err != nil {
		log.Fatalf("load sources: %v", err)
	}

	project, err := dag.LoadProject(
		profile.ModelsFolder,
		func(path string) (config.ModelConfig, error) {
			raw, err := os.ReadFile(path)
			if err != nil {
				return config.ModelConfig{}, err
			}
			return sqlrender.ParseModelConfig(string(raw))
		},
		sqlrender.ParseSQLFileRefs,
		sqlrender.ParseSQLFileSources,
	)
	if err != nil {
		log.Fatalf("load project: %v", err)
	}

	if err := dag.ValidateModelConfigs(project); err != nil {
		log.Fatalf("config validation: %v", err)
	}

	if err := dag.ValidateSources(project, rc.Sources); err != nil {
		log.Fatalf("source validation: %v", err)
	}

	modelsToProcess, modelRegistry, err := dag.ExecutionPlan(project)
	if err != nil {
		log.Fatalf("dependency graph: %v", err)
	}
	rc.ModelRegistry = modelRegistry

	pipeline.ExecuteQueries(db, modelsToProcess, rc)

	fmt.Printf("All completed in %v seconds\n", time.Since(start))
}
