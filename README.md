# DuckDB SQL Pipeline Orchestrator

This project provides a lightweight SQL execution framework using DuckDB. 
It processes templated SQL models with dependency resolution, executes them in topological order and uploads the Parquet files to Google Cloud Storage (GCS).

## Features

- Dependency graph creation from `{{ ref("...") }}` statements
- External dataset resolution from dbt-style `{{ source("...", "...") }}` statements
- Source validation against `sources.yml` before execution planning
- Dependency cycle detection with cycle path reporting
- Layered execution using topological sorting
- Incremental model logic (`is_incremental()` templating) support
- Output Parquet files with compression (date partitions are supported for incremental loads)
- GCS integration for upload/download
- Concurrent processing with configurable threading
- Logs execution times and errors per model

## Tech Stack

- **Go**
- **DuckDB**
- **Google Cloud Storage**

## Requirements

- Go 1.20+
- DuckDB installed
- `setup/profile.yml` file with:
  - `duckdb_file`
  - `models_folder`
  - `output_folder` (`_build`, `_refs`, `_sources` working area)
  - `logs_folder` (`results.txt` style execution log output)
  - `threads`
  - `full_refresh`

Profile example:
```yaml
duckdb_file: /tmp/orchestrator.duckdb
models_folder: /path/to/models
output_folder: /path/to/output
logs_folder: /path/to/logs
threads: 4
full_refresh: false
```

`setup/sources.yml` example:
```
sources:
  - name: ds_dbt
    type: gcs
    path: gs://your-bucket-name/raw
    tables:
      - name: kpi_forecast_actives_daily_percentage_splits
        path: kpi_forecast_actives_daily_percentage_splits/*.parquet
  - name: local_seed
    type: local
    path: /tmp/input-data
    tables:
      - name: dim_users
        path: dim_users/*.parquet
```

Sql file example:
```
{{
    config(
        materialized='incremental',
        storage_type='gcs',
        storage_path='gs://your-bucket-name/dwh/kpi_forecast_actives_daily_percentage_splits',
        partition_column='FlightDate',
        incremental_strategy='insert_overwrite'
    )
}}

SELECT *
FROM {{ source('ds_dbt', 'kpi_forecast_actives_daily_percentage_splits') }}
{% if is_incremental() %}
WHERE FlightDate > DATE '2006-02-22'
{% else %}
WHERE 1=1
{% endif %}
```

Ref example:
```
SELECT *
FROM {{ ref('stg_orders') }}
```

Notes:
- Models are discovered by recursively scanning `MODELS_FOLDER` for `.sql` files.
- Model output config is parsed from the SQL `config(...)` block.
- All `source()` references are validated against `sources.yml` before the DAG is planned.
- Dependency cycles are rejected with the model path reported, for example `a -> b -> a`.
- `ref()` dependencies resolve to locally materialized parquet outputs from upstream models.
- `source()` dependencies resolve through `sources.yml`.
- `local` sources are read directly and `gcs` sources are downloaded locally before query execution.
- `local` and `gcs` model output destinations are implemented.
- `s3` and Azure Blob source/model storage types are not implemented yet.

## Usage
- `cd {project_name}`
- `go mod tidy`
- `go build .`
- `go run ./cmd/orchestrator`
