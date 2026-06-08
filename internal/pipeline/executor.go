package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/maksimilian/duckdb-orchestrator/internal/config"
	"github.com/maksimilian/duckdb-orchestrator/internal/sqlrender"
	"github.com/maksimilian/duckdb-orchestrator/internal/storage"
	"golang.org/x/sync/errgroup"
)

type RunConfig struct {
	Threads       int
	FullRefresh   bool
	Profile       *config.Profile
	Sources       *config.SourceCatalog
	ModelRegistry map[string]config.ModelConfig
	LogsDir       string
}

func prepareMappingConfig(m *config.ModelConfig, rc RunConfig) error {
	if m.OutputDir == "" && rc.Profile != nil {
		m.OutputDir = rc.Profile.OutputFolder
	}
	if m.SplitRows <= 0 {
		m.SplitRows = 1_000_000
	}
	if m.StorageType == "" {
		m.StorageType = "local"
	}
	if m.StoragePath == "" {
		m.StoragePath = filepath.Join(m.OutputDir, m.ModelName)
	} else if m.StorageType == "local" && !filepath.IsAbs(m.StoragePath) {
		m.StoragePath = filepath.Join(m.OutputDir, m.StoragePath)
	}
	if m.IncrementalStrategy != "" && m.IncrementalStrategy != "insert_overwrite" {
		return fmt.Errorf("unsupported incremental strategy %q for model %s", m.IncrementalStrategy, m.ModelName)
	}
	sqlBytes, err := os.ReadFile(m.SQLFile)
	if err != nil {
		return fmt.Errorf("read %s: %w", m.SQLFile, err)
	}
	if !m.Incremental {
		m.Incremental = sqlrender.HasIncremental(string(sqlBytes))
	}
	if len(m.RefMapping) == 0 {
		refs, err := sqlrender.ParseSQLFileRefs(m.SQLFile)
		if err != nil {
			return fmt.Errorf("parse refs in %s: %w", m.SQLFile, err)
		}
		m.RefMapping = make(map[string]string, len(refs))
		for _, r := range refs {
			m.RefMapping[r] = fmt.Sprintf("%s/*.parquet", r)
		}
	}
	if len(m.SourceMapping) == 0 {
		sourceRefs, err := sqlrender.ParseSQLFileSources(m.SQLFile)
		if err != nil {
			return fmt.Errorf("parse sources in %s: %w", m.SQLFile, err)
		}
		m.SourceMapping = make(map[string]string, len(sourceRefs))
	}
	return nil
}

func materializeSources(ctx context.Context, m *config.ModelConfig, sources *config.SourceCatalog) error {
	sourceRefs, err := sqlrender.ParseSQLFileSources(m.SQLFile)
	if err != nil {
		return fmt.Errorf("parse sources in %s: %w", m.SQLFile, err)
	}
	if len(sourceRefs) == 0 {
		return nil
	}

	cacheDir := filepath.Join(m.OutputDir, "_sources")
	for _, src := range sourceRefs {
		parts := strings.SplitN(src, ".", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid source reference %q", src)
		}

		resolved, err := sources.Resolve(parts[0], parts[1])
		if err != nil {
			return err
		}

		location, err := materializeSourceLocation(ctx, resolved, cacheDir)
		if err != nil {
			return fmt.Errorf("materialize source %s: %w", src, err)
		}
		m.SourceMapping[src] = location
	}

	return nil
}

func materializeSourceLocation(ctx context.Context, source *config.ResolvedSource, cacheDir string) (string, error) {
	switch source.Type {
	case "", "local":
		return source.Location, nil
	case "gcs":
		return downloadGCSSource(ctx, source.Location, cacheDir)
	case "s3", "blob", "azure", "azblob":
		return "", fmt.Errorf("source type %q is not implemented yet", source.Type)
	default:
		return "", fmt.Errorf("unsupported source type %q", source.Type)
	}
}

func downloadGCSSource(ctx context.Context, location, cacheDir string) (string, error) {
	bucket, objectPath, err := parseGCSLocation(location)
	if err != nil {
		return "", err
	}

	client, err := storage.NewGCPStorage(ctx, bucket)
	if err != nil {
		return "", err
	}
	defer client.Close()

	baseDir := filepath.Join(cacheDir, bucket)
	if strings.Contains(objectPath, "*") {
		prefix := objectPath[:strings.Index(objectPath, "*")]
		objects, err := client.ListFiles(ctx, prefix)
		if err != nil {
			return "", err
		}
		eg, ctx := errgroup.WithContext(ctx)
		eg.SetLimit(20)
		for _, obj := range objects {
			obj := obj
			eg.Go(func() error {
				localPath := filepath.Join(baseDir, filepath.FromSlash(obj))
				if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
					return err
				}
				if strings.HasSuffix(obj, "/") {
					return nil
				}
				return client.DownloadFile(ctx, obj, localPath)
			})
		}
		if err := eg.Wait(); err != nil {
			return "", err
		}
		return filepath.Join(baseDir, filepath.FromSlash(objectPath)), nil
	}

	localPath := filepath.Join(baseDir, filepath.FromSlash(objectPath))
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return "", err
	}
	if err := client.DownloadFile(ctx, objectPath, localPath); err != nil {
		return "", err
	}
	return localPath, nil
}

func parseGCSLocation(location string) (string, string, error) {
	const prefix = "gs://"
	if !strings.HasPrefix(location, prefix) {
		return "", "", fmt.Errorf("invalid GCS location %q", location)
	}

	trimmed := strings.TrimPrefix(location, prefix)
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid GCS location %q", location)
	}

	return parts[0], parts[1], nil
}

func generateDuckDbSyntax(m *config.ModelConfig, outDir string, model string, fullRefresh bool) (string, error) {
	sqlBytes, err := os.ReadFile(m.SQLFile)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", m.SQLFile, err)
	}

	templated := sqlrender.RenderTemplate(string(sqlBytes), m.Incremental && !fullRefresh)
	baseQuery := sqlrender.SubstituteRefs(templated, m.RefMapping)
	baseQuery = sqlrender.SubstituteSources(baseQuery, m.SourceMapping)

	var copySQL string
	if m.PartitionColumn != "" {
		copySQL = fmt.Sprintf(`
		SET partitioned_write_max_open_files = 1000;
        COPY (SELECT *, CAST(%s AS DATE) AS duck_db_partition_date FROM (%s))
        TO '%s/'
        (FORMAT PARQUET,
         COMPRESSION GZIP,
         FILENAME_PATTERN '%s_{i}',
         PARTITION_BY duck_db_partition_date,
         OVERWRITE_OR_IGNORE)`,
			m.PartitionColumn, sqlrender.SanitizeSQL(baseQuery), outDir, model)
	} else {
		copySQL = fmt.Sprintf(`
        COPY (%s)
        TO '%s/'
        (FORMAT PARQUET,
         COMPRESSION GZIP,
         ROW_GROUP_SIZE %d,
         ROW_GROUPS_PER_FILE 1,
         PER_THREAD_OUTPUT TRUE,
         FILENAME_PATTERN '%s_{i}',
         OVERWRITE_OR_IGNORE)`,
			sqlrender.SanitizeSQL(baseQuery), outDir, m.SplitRows, model)
	}
	return copySQL, nil
}

func collectParquetFiles(outDir, partitionCol string) ([]string, error) {
	pattern := "*.parquet"
	if partitionCol != "" {
		pattern = "*/*.parquet"
	}
	files, err := filepath.Glob(filepath.Join(outDir, pattern))
	if err != nil {
		return nil, fmt.Errorf("glob parquet files: %w", err)
	}
	return files, nil
}

func processMapping(ctx context.Context, m config.ModelConfig, db *sql.DB, rc RunConfig) error {
	ctx, cancel := context.WithTimeout(ctx, 180*time.Minute)
	defer cancel()

	if err := prepareMappingConfig(&m, rc); err != nil {
		return err
	}
	if err := materializeSources(ctx, &m, rc.Sources); err != nil {
		return err
	}

	if err := PullRefs(ctx, &m, rc.ModelRegistry); err != nil {
		return err
	}

	model := m.ModelName
	outDir := filepath.Join(m.OutputDir, "_build", model)
	_ = os.RemoveAll(outDir)
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return err
	}

	copySQL, err := generateDuckDbSyntax(&m, outDir, model, rc.FullRefresh)
	if err != nil {
		return err
	}

	if _, err := db.ExecContext(ctx, copySQL); err != nil {
		return fmt.Errorf("duckdb export failed: %w", err)
	}

	if err := publishModelOutput(ctx, &m, outDir, rc.FullRefresh); err != nil {
		return err
	}

	return nil
}

func ExecuteQueries(ctx context.Context, db *sql.DB, modelsToProcess [][]config.ModelConfig, rc RunConfig) error {
	var logMu sync.Mutex
	for layerIdx, layer := range modelsToProcess {
		slog.Info("starting execution layer", "layer", layerIdx+1, "models", len(layer))
		eg, layerCtx := errgroup.WithContext(ctx)
		eg.SetLimit(rc.Threads)

		for _, m := range layer {
			m := m
			eg.Go(func() error {
				start := time.Now()
				modelErr := processMapping(layerCtx, m, db, rc)
				elapsed := time.Since(start).Seconds()
				logMu.Lock()
				appendModelLog(rc.LogsDir, m, elapsed, modelErr)
				logMu.Unlock()

				if modelErr != nil {
					slog.Error("model execution failed", "model", m.ModelName, "sql_file", m.SQLFile, "duration_seconds", elapsed, "error", modelErr)
					return modelErr
				}
				slog.Info("model execution finished", "model", m.ModelName, "sql_file", m.SQLFile, "duration_seconds", elapsed)
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			return err
		}
		slog.Info("execution layer finished", "layer", layerIdx+1, "models", len(layer))
	}
	return nil
}

func appendModelLog(logsDir string, m config.ModelConfig, duration float64, modelErr error) {
	if logsDir == "" {
		return
	}
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		slog.Error("create logs dir", "path", logsDir, "error", err)
		return
	}

	path := filepath.Join(logsDir, "results.txt")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		slog.Error("open model log", "path", path, "error", err)
		return
	}
	defer f.Close()

	if modelErr != nil {
		_, _ = fmt.Fprintf(f, "%s - error: %v\n", m.SQLFile, modelErr)
		return
	}
	_, _ = fmt.Fprintf(f, "%s - duration: %.2fs\n", m.SQLFile, duration)
}
