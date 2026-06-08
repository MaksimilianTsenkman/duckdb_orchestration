package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadSourceCatalog(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sources.yml")
	os.WriteFile(path, []byte(`
sources:
  - name: ds_dbt
    type: gcs
    path: gs://bucket/raw
    tables:
      - name: kpi_forecast
        path: forecasts/*.parquet
  - name: local_source
    type: local
    path: /tmp/data
    tables:
      - name: dim_users
`), 0644)

	cfg, err := LoadSourceCatalog(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.Sources) != 2 {
		t.Fatalf("expected 2 sources, got %d", len(cfg.Sources))
	}
	got, err := cfg.Resolve("ds_dbt", "kpi_forecast")
	if err != nil {
		t.Fatalf("resolve gcs source: %v", err)
	}
	if got.Type != "gcs" {
		t.Fatalf("expected gcs type, got %s", got.Type)
	}
	if got.Location != "gs://bucket/raw/forecasts/*.parquet" {
		t.Fatalf("expected resolved GCS path, got %s", got.Location)
	}
	got, err = cfg.Resolve("local_source", "dim_users")
	if err != nil {
		t.Fatalf("resolve local source: %v", err)
	}
	if got.Type != "local" {
		t.Fatalf("expected local type, got %s", got.Type)
	}
	if got.Location != "/tmp/data/dim_users" {
		t.Fatalf("expected default joined local path, got %s", got.Location)
	}
}

func TestLoadProfile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "profile.yml")
	os.WriteFile(path, []byte(`
duckdb_file: /tmp/test.duckdb
models_folder: /tmp/models
output_folder: /tmp/output
logs_folder: /tmp/logs
threads: 4
full_refresh: true
`), 0644)

	cfg, err := LoadProfile(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.DuckDBFile != "/tmp/test.duckdb" {
		t.Fatalf("unexpected duckdb file: %s", cfg.DuckDBFile)
	}
	if cfg.ModelsFolder != "/tmp/models" {
		t.Fatalf("unexpected models folder: %s", cfg.ModelsFolder)
	}
	if cfg.OutputFolder != "/tmp/output" {
		t.Fatalf("unexpected output folder: %s", cfg.OutputFolder)
	}
	if cfg.LogsFolder != "/tmp/logs" {
		t.Fatalf("unexpected logs folder: %s", cfg.LogsFolder)
	}
	if cfg.Threads != 4 {
		t.Fatalf("unexpected threads: %d", cfg.Threads)
	}
	if !cfg.FullRefresh {
		t.Fatal("expected full refresh to be true")
	}
}

func TestLoadProfile_MissingRequiredField(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "profile.yml")
	os.WriteFile(path, []byte(`
duckdb_file: /tmp/test.duckdb
output_folder: /tmp/output
threads: 4
`), 0644)

	_, err := LoadProfile(path)
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestLoadSourceCatalog_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yml")
	os.WriteFile(path, []byte(`sources: [`), 0644)

	_, err := LoadSourceCatalog(path)
	if err == nil {
		t.Error("expected error for invalid YAML")
	}
}

func TestLoadSourceCatalog_FileNotFound(t *testing.T) {
	_, err := LoadSourceCatalog("/nonexistent/path.yml")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestResolve_MissingSourceOrTable(t *testing.T) {
	cfg := &SourceCatalog{
		Sources: []SourceDefinition{
			{
				Name: "warehouse",
				Path: "s3://bucket/raw",
				Tables: []SourceTableSpec{
					{Name: "events", Path: "events/*.parquet"},
				},
			},
		},
	}

	if _, err := cfg.Resolve("missing", "events"); err == nil {
		t.Fatal("expected error for missing source")
	}
	if _, err := cfg.Resolve("warehouse", "missing"); err == nil {
		t.Fatal("expected error for missing table")
	}
}
