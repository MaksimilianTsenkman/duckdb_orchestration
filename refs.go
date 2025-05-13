package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sync/errgroup"
)

func pullRefs(ctx context.Context, model string, refMap map[string]string, inputDir string, gcs *GCPStorage) error {
	if gcs == nil {
		return nil
	}
	fmt.Printf("Downloading: %s\n", model)
	for ref, pattern := range refMap {
		if err := processRef(ctx, refMap, ref, pattern, inputDir, gcs); err != nil {
			return fmt.Errorf("ref %s: %w", ref, err)
		}
	}
	return nil
}

func processRef(ctx context.Context, refMap map[string]string, ref, pattern, inputDir string, gcs *GCPStorage) error {
	localPattern := filepath.Join(inputDir, pattern)
	rootDir := filepath.Dir(localPattern)
	if err := os.RemoveAll(rootDir); err != nil {
		return fmt.Errorf("cleanup %s: %w", rootDir, err)
	}
	fmt.Printf("Downloading files for ref %s\n", ref)
	if strings.Contains(pattern, "*") {
		return downloadMultipleFiles(ctx, refMap, ref, pattern, inputDir, gcs)
	}
	return downloadSingleFile(ctx, refMap, ref, pattern, inputDir, gcs)
}

func localFiles(pattern string) ([]string, error) {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, m := range matches {
		info, err := os.Stat(m)
		if err == nil && !info.IsDir() {
			files = append(files, m)
		}
	}
	return files, nil
}

func downloadMultipleFiles(ctx context.Context, refMap map[string]string, ref, pattern, inputDir string, gcs *GCPStorage) error {
	prefix := pattern[:strings.Index(pattern, "*")]
	objects, err := gcs.ListFiles(ctx, prefix)
	if err != nil {
		return err
	}
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(20)
	for _, obj := range objects {
		obj := obj
		eg.Go(func() error {
			local := filepath.Join(inputDir, obj)
			dir := filepath.Dir(local)
			if err := os.MkdirAll(dir, 0755); err != nil {
				return fmt.Errorf("mkdir fail %s: %w", dir, err)
			}
			if strings.HasSuffix(obj, "/") {
				return nil
			}
			if err := gcs.DownloadFile(ctx, obj, local); err != nil {
				return fmt.Errorf("download fail %s: %w", obj, err)
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	refMap[ref] = filepath.Join(inputDir, pattern)
	return nil
}

func downloadSingleFile(ctx context.Context, refMap map[string]string, ref, pattern, inputDir string, gcs *GCPStorage) error {
	local := filepath.Join(inputDir, pattern)
	dir := filepath.Dir(local)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("mkdir fail %s: %w", dir, err)
	}
	if err := gcs.DownloadFile(ctx, pattern, local); err != nil {
		return fmt.Errorf("download fail %s: %w", pattern, err)
	}
	refMap[ref] = local
	return nil
}
