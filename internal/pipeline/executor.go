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

// maxStorageConcurrency bounds parallel object transfers to remote storage.
const maxStorageConcurrency = 20

type RunConfig struct {
	Threads       int
	FullRefresh   bool
	Profile       *config.Profile
	Sources       *config.SourceCatalog
	ModelRegistry map[string]config.ModelConfig
	LogsDir       string
	// SourceLocations maps "source.table" to its materialized read location.
	// It is populated once per run before execution so that a source shared by
	// multiple models is downloaded a single time. Read-only during execution.
	SourceLocations map[string]string
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
	return nil
}

// materializeSources resolves and downloads every distinct source referenced by
// any model exactly once, returning a map from "source.table" to its read
// location. A source shared by several models is therefore fetched a single
// time per run rather than once per consuming model.
func materializeSources(ctx context.Context, layers [][]config.ModelConfig, rc RunConfig) (map[string]string, error) {
	unique := make(map[string]struct{})
	for _, layer := range layers {
		for _, m := range layer {
			refs, err := sqlrender.ParseSQLFileSources(m.SQLFile)
			if err != nil {
				return nil, fmt.Errorf("parse sources in %s: %w", m.SQLFile, err)
			}
			for _, ref := range refs {
				unique[ref] = struct{}{}
			}
		}
	}
	if len(unique) == 0 {
		return map[string]string{}, nil
	}

	cacheDir := filepath.Join(rc.Profile.OutputFolder, "_sources")
	locations := make(map[string]string, len(unique))
	var mu sync.Mutex

	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(maxStorageConcurrency)
	for src := range unique {
		src := src
		eg.Go(func() error {
			parts := strings.SplitN(src, ".", 2)
			if len(parts) != 2 {
				return fmt.Errorf("invalid source reference %q", src)
			}
			resolved, err := rc.Sources.Resolve(parts[0], parts[1])
			if err != nil {
				return err
			}
			location, err := materializeSourceLocation(ctx, resolved, cacheDir)
			if err != nil {
				return fmt.Errorf("materialize source %s: %w", src, err)
			}
			mu.Lock()
			locations[src] = location
			mu.Unlock()
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return locations, nil
}

// resolveModelSources fills a model's SourceMapping from the run-level locations
// that materializeSources already produced.
func resolveModelSources(m *config.ModelConfig, locations map[string]string) error {
	refs, err := sqlrender.ParseSQLFileSources(m.SQLFile)
	if err != nil {
		return fmt.Errorf("parse sources in %s: %w", m.SQLFile, err)
	}
	if len(refs) == 0 {
		return nil
	}
	if m.SourceMapping == nil {
		m.SourceMapping = make(map[string]string, len(refs))
	}
	for _, ref := range refs {
		location, ok := locations[ref]
		if !ok {
			return fmt.Errorf("source %q was not materialized", ref)
		}
		m.SourceMapping[ref] = location
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
		objects, err := client.ListObjects(ctx, prefix)
		if err != nil {
			return "", err
		}
		eg, ctx := errgroup.WithContext(ctx)
		eg.SetLimit(maxStorageConcurrency)
		for _, obj := range objects {
			obj := obj
			eg.Go(func() error {
				if strings.HasSuffix(obj.Name, "/") {
					return nil
				}
				localPath := filepath.Join(baseDir, filepath.FromSlash(obj.Name))
				return downloadIfStale(ctx, client, obj.Name, localPath, obj.Size)
			})
		}
		if err := eg.Wait(); err != nil {
			return "", err
		}
		return filepath.Join(baseDir, filepath.FromSlash(objectPath)), nil
	}

	localPath := filepath.Join(baseDir, filepath.FromSlash(objectPath))
	size, err := client.AttrsSize(ctx, objectPath)
	if err != nil {
		return "", err
	}
	if err := downloadIfStale(ctx, client, objectPath, localPath, size); err != nil {
		return "", err
	}
	return localPath, nil
}

// downloadIfStale downloads obj to localPath unless a local file of the same
// size already exists, treating the byte size as a freshness proxy so repeated
// runs skip objects that are already cached.
func downloadIfStale(ctx context.Context, client *storage.GCPStorage, obj, localPath string, size int64) error {
	if fi, err := os.Stat(localPath); err == nil && !fi.IsDir() && fi.Size() == size {
		slog.Debug("source cache hit", "object", obj, "path", localPath)
		return nil
	}
	return client.DownloadFile(ctx, obj, localPath)
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
	if err := resolveModelSources(&m, rc.SourceLocations); err != nil {
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
	sourceLocations, err := materializeSources(ctx, modelsToProcess, rc)
	if err != nil {
		return fmt.Errorf("materialize sources: %w", err)
	}
	rc.SourceLocations = sourceLocations

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
