package dag

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/maksimilian/duckdb-orchestrator/internal/config"
)

func TestLoadProject_Recursive(t *testing.T) {
	dir := t.TempDir()
	stagingDir := filepath.Join(dir, "stg")
	if err := os.MkdirAll(stagingDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	upstreamPath := filepath.Join(stagingDir, "upstream.sql")
	downstreamPath := filepath.Join(stagingDir, "downstream.sql")
	os.WriteFile(upstreamPath, []byte("SELECT 1"), 0644)
	os.WriteFile(downstreamPath, []byte("SELECT * FROM {{ ref('upstream') }}"), 0644)

	project, err := LoadProject(
		dir,
		func(path string) (config.ModelConfig, error) { return config.ModelConfig{}, nil },
		func(path string) ([]string, error) {
			data, err := os.ReadFile(path)
			if err != nil {
				return nil, err
			}
			if strings.Contains(string(data), "{{ ref('upstream') }}") {
				return []string{"upstream"}, nil
			}
			return nil, nil
		},
		func(path string) ([]string, error) { return nil, nil },
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if project.Models["upstream"].Config.SQLFile != upstreamPath {
		t.Fatalf("expected upstream path %s, got %s", upstreamPath, project.Models["upstream"].Config.SQLFile)
	}
	if project.Models["downstream"].Config.SQLFile != downstreamPath {
		t.Fatalf("expected downstream path %s, got %s", downstreamPath, project.Models["downstream"].Config.SQLFile)
	}
}

func TestValidateSources(t *testing.T) {
	project := &Project{
		Models: map[string]Model{
			"fact_orders": {
				Config:  config.ModelConfig{ModelName: "fact_orders"},
				Sources: []string{"warehouse.orders"},
			},
		},
	}
	sources := &config.SourceCatalog{
		Sources: []config.SourceDefinition{
			{
				Name: "warehouse",
				Path: "/tmp",
				Tables: []config.SourceTableSpec{
					{Name: "orders"},
				},
			},
		},
	}

	if err := ValidateSources(project, sources); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateSources_MissingSource(t *testing.T) {
	project := &Project{
		Models: map[string]Model{
			"fact_orders": {
				Config:  config.ModelConfig{ModelName: "fact_orders"},
				Sources: []string{"warehouse.orders"},
			},
		},
	}

	err := ValidateSources(project, &config.SourceCatalog{})
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "fact_orders") {
		t.Fatalf("expected model name in error, got %v", err)
	}
}

func TestValidateModelConfigs(t *testing.T) {
	project := &Project{
		Models: map[string]Model{
			"fact_orders": {
				Config: config.ModelConfig{
					ModelName:           "fact_orders",
					StorageLocation:     "gcs",
					StorageOption:       "gs://bucket/dwh/fact_orders",
					Incremental:         true,
					IncrementalStrategy: "insert_overwrite",
					PartitionColumn:     "event_date",
				},
			},
		},
	}

	if err := ValidateModelConfigs(project); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateModelConfigs_InvalidConfig(t *testing.T) {
	project := &Project{
		Models: map[string]Model{
			"fact_orders": {
				Config: config.ModelConfig{
					ModelName:           "fact_orders",
					StorageLocation:     "weirdfs",
					Incremental:         true,
					IncrementalStrategy: "merge",
				},
			},
		},
	}

	err := ValidateModelConfigs(project)
	if err == nil {
		t.Fatal("expected config validation error")
	}
	if !strings.Contains(err.Error(), "fact_orders") {
		t.Fatalf("expected model name in error, got %v", err)
	}
}

func TestExecutionPlan_UsesResolvedSQLPaths(t *testing.T) {
	project := &Project{
		Models: map[string]Model{
			"upstream": {
				Config: config.ModelConfig{
					ModelName: "upstream",
					SQLFile:   "/tmp/upstream.sql",
				},
			},
			"downstream": {
				Config: config.ModelConfig{
					ModelName: "downstream",
					SQLFile:   "/tmp/downstream.sql",
				},
				Refs: []string{"upstream"},
			},
		},
	}

	plan, registry, err := ExecutionPlan(project)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(plan) != 2 {
		t.Fatalf("expected 2 layers, got %d", len(plan))
	}
	if plan[0][0].SQLFile != "/tmp/upstream.sql" {
		t.Fatalf("unexpected upstream sql path: %s", plan[0][0].SQLFile)
	}
	if plan[1][0].SQLFile != "/tmp/downstream.sql" {
		t.Fatalf("unexpected downstream sql path: %s", plan[1][0].SQLFile)
	}
	if registry["downstream"].ModelName != "downstream" {
		t.Fatalf("unexpected registry model name: %s", registry["downstream"].ModelName)
	}
}

func TestExecutionPlan_DetectsCycle(t *testing.T) {
	project := &Project{
		Models: map[string]Model{
			"a": {Config: config.ModelConfig{ModelName: "a"}, Refs: []string{"b"}},
			"b": {Config: config.ModelConfig{ModelName: "b"}, Refs: []string{"a"}},
		},
	}

	_, _, err := ExecutionPlan(project)
	if err == nil {
		t.Fatal("expected cycle error")
	}
	if !strings.Contains(err.Error(), "a -> b -> a") {
		t.Fatalf("expected cycle path in error, got %v", err)
	}
}

func TestNewGraph(t *testing.T) {
	project := &Project{
		Models: map[string]Model{
			"upstream": {
				Config: config.ModelConfig{ModelName: "upstream"},
			},
			"downstream": {
				Config: config.ModelConfig{ModelName: "downstream"},
				Refs:   []string{"upstream"},
			},
		},
	}

	graph, err := NewGraph(project)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if graph.inDegree["upstream"] != 0 {
		t.Fatalf("expected upstream indegree 0, got %d", graph.inDegree["upstream"])
	}
	if graph.inDegree["downstream"] != 1 {
		t.Fatalf("expected downstream indegree 1, got %d", graph.inDegree["downstream"])
	}
	if len(graph.dependents["upstream"]) != 1 || graph.dependents["upstream"][0] != "downstream" {
		t.Fatalf("unexpected dependents for upstream: %v", graph.dependents["upstream"])
	}
}
