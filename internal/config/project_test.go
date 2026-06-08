package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadProject_Recursive(t *testing.T) {
	dir := t.TempDir()
	stagingDir := filepath.Join(dir, "stg")
	if err := os.MkdirAll(stagingDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	upstreamPath := filepath.Join(stagingDir, "upstream.sql")
	downstreamPath := filepath.Join(stagingDir, "downstream.sql")
	_ = os.WriteFile(upstreamPath, []byte("SELECT 1"), 0644)
	_ = os.WriteFile(downstreamPath, []byte("SELECT * FROM {{ ref('upstream') }}"), 0644)

	project, err := LoadProject(
		dir,
		func(string) (ModelConfig, error) { return ModelConfig{}, nil },
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
		func(string) ([]string, error) { return nil, nil },
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

func TestValidateProjectSources(t *testing.T) {
	project := &Project{
		Models: map[string]ProjectModel{
			"fact_orders": {
				Config:  ModelConfig{ModelName: "fact_orders"},
				Sources: []string{"warehouse.orders"},
			},
		},
	}
	sources := &SourceCatalog{
		Sources: []SourceDefinition{
			{
				Name:   "warehouse",
				Path:   "/tmp",
				Tables: []SourceTableSpec{{Name: "orders"}},
			},
		},
	}

	if err := ValidateProjectSources(project, sources); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateProjectSources_MissingSource(t *testing.T) {
	project := &Project{
		Models: map[string]ProjectModel{
			"fact_orders": {
				Config:  ModelConfig{ModelName: "fact_orders"},
				Sources: []string{"warehouse.orders"},
			},
		},
	}

	err := ValidateProjectSources(project, &SourceCatalog{})
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "fact_orders") {
		t.Fatalf("expected model name in error, got %v", err)
	}
}

func TestValidateProjectModels(t *testing.T) {
	project := &Project{
		Models: map[string]ProjectModel{
			"fact_orders": {
				Config: ModelConfig{
					ModelName:           "fact_orders",
					StorageType:         "gcs",
					StoragePath:         "gs://bucket/dwh/fact_orders",
					Incremental:         true,
					IncrementalStrategy: "insert_overwrite",
					PartitionColumn:     "event_date",
				},
			},
		},
	}

	if err := ValidateProjectModels(project); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateProjectModels_InvalidConfig(t *testing.T) {
	project := &Project{
		Models: map[string]ProjectModel{
			"fact_orders": {
				Config: ModelConfig{
					ModelName:           "fact_orders",
					StorageType:         "weirdfs",
					Incremental:         true,
					IncrementalStrategy: "merge",
				},
			},
		},
	}

	err := ValidateProjectModels(project)
	if err == nil {
		t.Fatal("expected config validation error")
	}
	if !strings.Contains(err.Error(), "fact_orders") {
		t.Fatalf("expected model name in error, got %v", err)
	}
}
