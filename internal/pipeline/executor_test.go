package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/maksimilian/duckdb-orchestrator/internal/config"
)

// writeModel writes a temporary .sql file with the given body and returns its path.
func writeModel(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write model: %v", err)
	}
	return path
}

func TestMaterializeSourcesDedupesSharedSource(t *testing.T) {
	dir := t.TempDir()
	body := "SELECT * FROM {{ source('seed', 'users') }}"
	a := writeModel(t, dir, "a.sql", body)
	b := writeModel(t, dir, "b.sql", body)

	catalog := &config.SourceCatalog{
		Sources: []config.SourceDefinition{{
			Name: "seed",
			Type: "local",
			Path: dir,
			Tables: []config.SourceTableSpec{
				{Name: "users", Path: "users.parquet"},
			},
		}},
	}

	rc := RunConfig{
		Profile: &config.Profile{OutputFolder: dir},
		Sources: catalog,
	}
	layers := [][]config.ModelConfig{{
		{ModelName: "a", SQLFile: a},
		{ModelName: "b", SQLFile: b},
	}}

	locations, err := materializeSources(context.Background(), layers, rc)
	if err != nil {
		t.Fatalf("materializeSources: %v", err)
	}

	if len(locations) != 1 {
		t.Fatalf("expected one deduped source location, got %d: %v", len(locations), locations)
	}
	want := filepath.Join(dir, "users.parquet")
	if got := locations["seed.users"]; got != want {
		t.Fatalf("location = %q, want %q", got, want)
	}
}

func TestResolveModelSourcesFillsMappingFromShared(t *testing.T) {
	dir := t.TempDir()
	model := config.ModelConfig{
		ModelName: "a",
		SQLFile:   writeModel(t, dir, "a.sql", "SELECT * FROM {{ source('seed', 'users') }}"),
	}
	locations := map[string]string{"seed.users": "/data/users.parquet"}

	if err := resolveModelSources(&model, locations); err != nil {
		t.Fatalf("resolveModelSources: %v", err)
	}
	if got := model.SourceMapping["seed.users"]; got != "/data/users.parquet" {
		t.Fatalf("mapping = %q, want /data/users.parquet", got)
	}
}

func TestResolveModelSourcesErrorsOnMissingLocation(t *testing.T) {
	dir := t.TempDir()
	model := config.ModelConfig{
		ModelName: "a",
		SQLFile:   writeModel(t, dir, "a.sql", "SELECT * FROM {{ source('seed', 'users') }}"),
	}

	if err := resolveModelSources(&model, map[string]string{}); err == nil {
		t.Fatal("expected error for unmaterialized source, got nil")
	}
}

func TestDownloadIfStaleSkipsCachedFile(t *testing.T) {
	dir := t.TempDir()
	localPath := filepath.Join(dir, "obj.parquet")
	contents := []byte("cached-bytes")
	if err := os.WriteFile(localPath, contents, 0o644); err != nil {
		t.Fatalf("seed cache file: %v", err)
	}

	// A matching size is treated as a cache hit, so the (nil) client is never
	// dereferenced for a download. A panic here would mean the cache was missed.
	if err := downloadIfStale(context.Background(), nil, "obj.parquet", localPath, int64(len(contents))); err != nil {
		t.Fatalf("downloadIfStale: %v", err)
	}

	got, err := os.ReadFile(localPath)
	if err != nil {
		t.Fatalf("read cache file: %v", err)
	}
	if string(got) != string(contents) {
		t.Fatalf("cached file was modified: %q", got)
	}
}
