package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/maksimilian/duckdb-orchestrator/internal/config"
	"github.com/maksimilian/duckdb-orchestrator/internal/storage"
	"golang.org/x/sync/errgroup"
)

func PullRefs(ctx context.Context, model *config.ModelConfig, registry map[string]config.ModelConfig) error {
	for ref := range model.RefMapping {
		upstream, ok := registry[ref]
		if !ok {
			return fmt.Errorf("ref %q not found in model registry", ref)
		}
		if err := prepareMappingConfig(&upstream, RunConfig{Profile: &config.Profile{OutputFolder: model.OutputDir}}); err != nil {
			return err
		}

		localPattern, err := materializeRef(ctx, upstream, model.OutputDir)
		if err != nil {
			return fmt.Errorf("ref %s: %w", ref, err)
		}
		model.RefMapping[ref] = localPattern
	}
	return nil
}

func materializeRef(ctx context.Context, upstream config.ModelConfig, outputDir string) (string, error) {
	switch upstream.StorageType {
	case "", "local":
		return modelReadPattern(upstream), nil
	case "gcs":
		cacheDir := filepath.Join(outputDir, "_refs", upstream.ModelName)
		return downloadGCSLocation(ctx, upstream.StoragePath, upstream.PartitionColumn, cacheDir)
	case "s3", "blob", "azure", "azblob":
		return "", fmt.Errorf("ref storage type %q is not implemented yet", upstream.StorageType)
	default:
		return "", fmt.Errorf("unsupported ref storage type %q", upstream.StorageType)
	}
}

func publishModelOutput(ctx context.Context, model *config.ModelConfig, buildDir string, fullRefresh bool) error {
	switch model.StorageType {
	case "", "local":
		return publishLocalOutput(ctx, model, buildDir, fullRefresh)
	case "gcs":
		return publishGCSOutput(ctx, model, buildDir, fullRefresh)
	case "s3", "blob", "azure", "azblob":
		return fmt.Errorf("model storage type %q is not implemented yet", model.StorageType)
	default:
		return fmt.Errorf("unsupported model storage type %q", model.StorageType)
	}
}

func publishLocalOutput(ctx context.Context, model *config.ModelConfig, buildDir string, fullRefresh bool) error {
	targetDir := model.StoragePath
	if targetDir == "" {
		return fmt.Errorf("missing local storage target for model %s", model.ModelName)
	}
	store := storage.NewLocalStorage("/")

	if fullRefresh || !model.Incremental {
		if err := store.DeletePrefix(ctx, targetDir); err != nil {
			return err
		}
	}

	parts, err := collectParquetFiles(buildDir, model.PartitionColumn)
	if err != nil {
		return err
	}

	if model.PartitionColumn != "" && model.Incremental && !fullRefresh {
		if err := cleanupLocalPartitions(parts, buildDir, targetDir); err != nil {
			return err
		}
	}

	for _, src := range parts {
		rel, err := filepath.Rel(buildDir, src)
		if err != nil {
			return err
		}
		dst := filepath.Join(targetDir, rel)
		if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
			return err
		}
		if err := store.UploadFile(ctx, src, dst); err != nil {
			return err
		}
	}

	return nil
}

func publishGCSOutput(ctx context.Context, model *config.ModelConfig, buildDir string, fullRefresh bool) error {
	bucket, prefix, err := parseGCSLocation(model.StoragePath)
	if err != nil {
		return err
	}

	client, err := storage.NewGCPStorage(ctx, bucket)
	if err != nil {
		return err
	}
	defer client.Close()

	if fullRefresh || !model.Incremental {
		if err := client.DeletePrefix(ctx, ensureTrailingSlash(prefix)); err != nil {
			return err
		}
	}

	parts, err := collectParquetFiles(buildDir, model.PartitionColumn)
	if err != nil {
		return err
	}

	if model.PartitionColumn != "" && model.Incremental && !fullRefresh {
		if err := cleanupGCSPartitions(ctx, parts, buildDir, prefix, client); err != nil {
			return err
		}
	}

	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(10)
	for _, src := range parts {
		src := src
		eg.Go(func() error {
			rel, err := filepath.Rel(buildDir, src)
			if err != nil {
				return err
			}
			objectName := joinStorageLocation(prefix, filepath.ToSlash(rel))
			return client.UploadFile(ctx, src, objectName)
		})
	}
	return eg.Wait()
}

func cleanupLocalPartitions(parts []string, buildDir, targetDir string) error {
	partitions := partitionRelDirs(parts, buildDir)
	for _, partition := range partitions {
		if err := os.RemoveAll(filepath.Join(targetDir, partition)); err != nil {
			return err
		}
	}
	return nil
}

func cleanupGCSPartitions(ctx context.Context, parts []string, buildDir, prefix string, client *storage.GCPStorage) error {
	partitions := partitionRelDirs(parts, buildDir)
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(10)
	for _, partition := range partitions {
		partition := partition
		eg.Go(func() error {
			return client.DeletePrefix(ctx, ensureTrailingSlash(joinStorageLocation(prefix, filepath.ToSlash(partition))))
		})
	}
	return eg.Wait()
}

func partitionRelDirs(parts []string, root string) []string {
	partitions := make(map[string]struct{})
	for _, file := range parts {
		rel, err := filepath.Rel(root, file)
		if err != nil {
			continue
		}
		dir := filepath.Dir(rel)
		if dir != "." {
			partitions[dir] = struct{}{}
		}
	}

	out := make([]string, 0, len(partitions))
	for partition := range partitions {
		out = append(out, partition)
	}
	return out
}

func modelReadPattern(model config.ModelConfig) string {
	pattern := "*.parquet"
	if model.PartitionColumn != "" {
		pattern = "*/*.parquet"
	}
	return filepath.Join(model.StoragePath, filepath.FromSlash(pattern))
}

func downloadGCSLocation(ctx context.Context, location, partitionColumn, cacheDir string) (string, error) {
	bucket, prefix, err := parseGCSLocation(location)
	if err != nil {
		return "", err
	}

	client, err := storage.NewGCPStorage(ctx, bucket)
	if err != nil {
		return "", err
	}
	defer client.Close()

	objects, err := client.ListFiles(ctx, ensureTrailingSlash(prefix))
	if err != nil {
		return "", err
	}

	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(20)
	for _, obj := range objects {
		obj := obj
		eg.Go(func() error {
			if strings.HasSuffix(obj, "/") {
				return nil
			}
			rel := strings.TrimPrefix(obj, ensureTrailingSlash(prefix))
			localPath := filepath.Join(cacheDir, filepath.FromSlash(rel))
			if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
				return err
			}
			return client.DownloadFile(ctx, obj, localPath)
		})
	}
	if err := eg.Wait(); err != nil {
		return "", err
	}
	slog.Info("materialized gcs location", "source", location, "cache_dir", cacheDir)

	pattern := "*.parquet"
	if partitionColumn != "" {
		pattern = "*/*.parquet"
	}
	return filepath.Join(cacheDir, filepath.FromSlash(pattern)), nil
}

func ensureTrailingSlash(input string) string {
	if strings.HasSuffix(input, "/") {
		return input
	}
	return input + "/"
}

func joinStorageLocation(base, rel string) string {
	if rel == "" {
		return strings.TrimSpace(base)
	}
	if base == "" || strings.Contains(rel, "://") || strings.HasPrefix(rel, "/") {
		return strings.TrimSpace(rel)
	}
	return strings.TrimRight(strings.TrimSpace(base), "/") + "/" + strings.TrimLeft(strings.TrimSpace(rel), "/")
}
