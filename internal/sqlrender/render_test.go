package sqlrender

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSanitizeSQL(t *testing.T) {
	input := "SELECT * FROM t; DROP TABLE t;"
	got := SanitizeSQL(input)
	if strings.Contains(got, ";") {
		t.Errorf("expected no semicolons, got %q", got)
	}
}

func TestParseSQLFileRefs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sql")
	os.WriteFile(path, []byte(`
		SELECT * FROM {{ ref('orders') }}
		JOIN {{ ref("customers") }} ON 1=1
	`), 0644)

	refs, err := ParseSQLFileRefs(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(refs) != 2 {
		t.Fatalf("expected 2 refs, got %d", len(refs))
	}
	if refs[0] != "orders" || refs[1] != "customers" {
		t.Errorf("expected [orders, customers], got %v", refs)
	}
}

func TestParseSQLFileRefs_NoRefs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sql")
	os.WriteFile(path, []byte("SELECT 1"), 0644)

	refs, err := ParseSQLFileRefs(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(refs) != 0 {
		t.Errorf("expected 0 refs, got %d", len(refs))
	}
}

func TestParseSQLFileSources(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sql")
	os.WriteFile(path, []byte(`
		SELECT *
		FROM {{ source('ds_dbt', 'forecast') }}
		JOIN {{ source("warehouse", "users") }} ON 1=1
	`), 0644)

	sources, err := ParseSQLFileSources(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sources) != 2 {
		t.Fatalf("expected 2 sources, got %d", len(sources))
	}
	if sources[0] != "ds_dbt.forecast" || sources[1] != "warehouse.users" {
		t.Fatalf("unexpected sources: %v", sources)
	}
}

func TestRenderTemplate_Incremental(t *testing.T) {
	raw := `SELECT * FROM t
{% if is_incremental() %}
WHERE date > '2024-01-01'
{% else %}
WHERE 1=1
{% endif %}`

	got := RenderTemplate(raw, true)
	if !strings.Contains(got, "WHERE date > '2024-01-01'") {
		t.Errorf("expected incremental branch, got %q", got)
	}
	if strings.Contains(got, "WHERE 1=1") {
		t.Errorf("should not contain else branch")
	}
}

func TestRenderTemplate_FullRefresh(t *testing.T) {
	raw := `SELECT * FROM t
{% if is_incremental() %}
WHERE date > '2024-01-01'
{% else %}
WHERE 1=1
{% endif %}`

	got := RenderTemplate(raw, false)
	if strings.Contains(got, "WHERE date > '2024-01-01'") {
		t.Errorf("should not contain incremental branch")
	}
	if !strings.Contains(got, "WHERE 1=1") {
		t.Errorf("expected else branch, got %q", got)
	}
}

func TestRenderTemplate_IsIncrementalBoolean(t *testing.T) {
	raw := "SELECT {{ is_incremental() }} AS flag"
	got := RenderTemplate(raw, true)
	if !strings.Contains(got, "TRUE") {
		t.Errorf("expected TRUE, got %q", got)
	}
	got = RenderTemplate(raw, false)
	if !strings.Contains(got, "FALSE") {
		t.Errorf("expected FALSE, got %q", got)
	}
}

func TestHasIncremental(t *testing.T) {
	if !HasIncremental("SELECT {{ is_incremental() }}") {
		t.Fatal("expected incremental detection")
	}
	if HasIncremental("SELECT 1") {
		t.Fatal("did not expect incremental detection")
	}
}

func TestParseModelConfig(t *testing.T) {
	raw := `
{{
    config(
        schema='dwh',
        materialized='incremental',
        storage_type='gcs',
        storage_path='gs://bucket/dwh/some_model',
        partition_column='event_date',
        incremental_strategy='insert_overwrite',
        cluster_by=['game_id']
    )
}}
SELECT 1
`

	cfg, err := ParseModelConfig(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.StorageType != "gcs" {
		t.Fatalf("expected gcs storage type, got %s", cfg.StorageType)
	}
	if cfg.StoragePath != "gs://bucket/dwh/some_model" {
		t.Fatalf("unexpected storage path: %s", cfg.StoragePath)
	}
	if cfg.PartitionColumn != "event_date" {
		t.Fatalf("unexpected partition column: %s", cfg.PartitionColumn)
	}
	if cfg.IncrementalStrategy != "insert_overwrite" {
		t.Fatalf("unexpected incremental strategy: %s", cfg.IncrementalStrategy)
	}
	if !cfg.Incremental {
		t.Fatal("expected incremental to be enabled")
	}
}

func TestStripConfigBlocks(t *testing.T) {
	raw := "{{ config(materialized='table') }}\nSELECT 1"
	got := StripConfigBlocks(raw)
	if strings.Contains(got, "config(") {
		t.Fatalf("expected config block to be removed, got %q", got)
	}
	if !strings.Contains(got, "SELECT 1") {
		t.Fatalf("expected SQL to remain, got %q", got)
	}
}

func TestRenderTemplate_IfWithoutElse(t *testing.T) {
	raw := `SELECT * FROM t
{% if is_incremental() %}
AND date > '2024-01-01'
{% endif %}`

	got := RenderTemplate(raw, false)
	if strings.Contains(got, "AND date") {
		t.Errorf("should not contain if block when not incremental, got %q", got)
	}

	got = RenderTemplate(raw, true)
	if !strings.Contains(got, "AND date > '2024-01-01'") {
		t.Errorf("expected if block when incremental, got %q", got)
	}
}

func TestSubstituteRefs(t *testing.T) {
	sql := "SELECT * FROM {{ ref('orders') }} JOIN {{ ref('customers') }}"
	refs := map[string]string{
		"orders":    "output/orders/*.parquet",
		"customers": "output/customers/*.parquet",
	}
	got := SubstituteRefs(sql, refs)
	if !strings.Contains(got, "read_parquet('output/orders/*.parquet')") {
		t.Errorf("expected orders ref substituted, got %q", got)
	}
	if !strings.Contains(got, "read_parquet('output/customers/*.parquet')") {
		t.Errorf("expected customers ref substituted, got %q", got)
	}
}

func TestSubstituteSources(t *testing.T) {
	sql := "SELECT * FROM {{ source('ds_dbt', 'forecast') }} JOIN {{ source('warehouse', 'users') }}"
	sources := map[string]string{
		"ds_dbt.forecast": "gs://bucket/forecast/*.parquet",
		"warehouse.users": "/tmp/users/*.parquet",
	}
	got := SubstituteSources(sql, sources)
	if !strings.Contains(got, "read_parquet('gs://bucket/forecast/*.parquet')") {
		t.Fatalf("expected forecast source substituted, got %q", got)
	}
	if !strings.Contains(got, "read_parquet('/tmp/users/*.parquet')") {
		t.Fatalf("expected users source substituted, got %q", got)
	}
}
