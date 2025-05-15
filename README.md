# DuckDB SQL Pipeline Orchestrator

This project provides a lightweight SQL execution framework using DuckDB. 
It processes templated SQL models with dependency resolution, executes them in topological order and uploads the Parquet files to Google Cloud Storage (GCS).

## Features

- Dependency graph creation from `{{ ref("...") }}` statements
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
- `.env` file with:
  - `DUCK_DB_FILE=path/to/db.duckdb`
  - `MAPPINGS_PATH=path/to/mappings.json`
  - `MODELS_FOLDER=path/to/models`
  - `OUTPUT_FOLDER=path/to/output`
  - `GCP_BUCKET=your-bucket-name`

Mappings file example:
```
[
  {
    "sql_file": {sql_file_name},
    "partition_column_date": {column}, # Optional
    "incremental": true/false # Optional
  }
]
```

Sql file example:
```
SELECT *
FROM {{ ref('testing_model_name') }}
{% if is_incremental() %}
WHERE FlightDate > DATE '2006-02-22'
{% else %}
WHERE 1=1
{% endif %}
```

GCS structure:
`gs://your-bucket-name/testing_model_name/*.parquet`

Note: The model name in `{{ ref('...') }}` should correspond to the folder name in GCS for consistency.

## Usage
- `cd {project_name}`
- `go mod tidy`
- `go build .`
- `go run . -threads=4 -full-refresh=true`
