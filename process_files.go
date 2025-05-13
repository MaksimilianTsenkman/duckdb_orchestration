package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

type MappingConfig struct {
	SQLFile         string            `json:"sql_file"`
	RefMapping      map[string]string `json:"ref_mapping"`
	OutputDir       string            `json:"output_dir"`
	SplitRows       int               `json:"split_rows,omitempty"`
	Incremental     bool              `json:"incremental"`
	PartitionColumn string            `json:"partition_column_date"`
}

func loadMappingConfig(configPath string) ([]MappingConfig, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	var mappings []MappingConfig
	err = json.Unmarshal(data, &mappings)
	if err != nil {
		return nil, err
	}
	return mappings, nil
}

func modifyMappingConfig(m *MappingConfig) error {
	m.SQLFile = "models/" + m.SQLFile
	if m.OutputDir == "" {
		m.OutputDir = os.Getenv("OUTPUT_FOLDER")
	}
	if m.SplitRows <= 0 {
		m.SplitRows = 1_000_000
	}
	if len(m.RefMapping) == 0 {
		refs, err := parseSQLFileRefs(m.SQLFile)
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

func generateDuckDbSyntax(m *MappingConfig, outDir string, model string) (string, error) {
	sqlBytes, err := os.ReadFile(m.SQLFile)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", m.SQLFile, err)
	}

	templated := renderTemplate(string(sqlBytes), m.Incremental && !fullRefresh)
	baseQuery := substituteRefs(templated, m.RefMapping)

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
			m.PartitionColumn, sanitizeSQL(baseQuery), outDir, model)
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
			sanitizeSQL(baseQuery), outDir, m.SplitRows, model)
	}
	return copySQL, nil
}

func handleGCS(ctx context.Context, m *MappingConfig, model, outDir string, fullRefresh bool, gcs *GCPStorage) error {
	if fullRefresh || !m.Incremental {
		if err := gcs.DeletePrefix(ctx, model+"/"); err != nil {
			return fmt.Errorf("remote cleanup failed for %s: %v\n", model, err)
		}
		fmt.Println("remote cleanup done")
	}

	parts, err := collectParquetFiles(outDir, m.PartitionColumn)
	if err != nil {
		return err
	}

	if m.PartitionColumn != "" && m.Incremental && !fullRefresh {
		if err := cleanupIncrementalPartitions(ctx, parts, gcs); err != nil {
			return err
		}
	}

	return uploadFiles(ctx, parts, m.OutputDir, gcs)
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

func cleanupIncrementalPartitions(ctx context.Context, parts []string, gcs *GCPStorage) error {
	partSet := make(map[string]struct{})
	for _, f := range parts {
		seg := strings.Split(f, string(filepath.Separator))
		for i, s := range seg {
			if strings.HasPrefix(s, "duck_db_partition_date=") {
				prefix := strings.Join(seg[1:i+1], "/")
				partSet[prefix] = struct{}{}
				break
			}
		}
	}

	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(10)
	for p := range partSet {
		p := p
		fmt.Printf("deleting %s\n", p)
		eg.Go(func() error { return gcs.DeletePrefix(ctx, p) })
	}
	return eg.Wait()
}

func uploadFiles(ctx context.Context, parts []string, baseDir string, gcs *GCPStorage) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(10)
	for _, p := range parts {
		p := p
		eg.Go(func() error {
			rel, err := filepath.Rel(baseDir, p)
			if err != nil {
				return fmt.Errorf("rel %s: %w", p, err)
			}
			fmt.Printf("uploading %s\n", p)
			return gcs.UploadFile(ctx, p, rel)
		})
	}
	return eg.Wait()
}

func processMapping(m MappingConfig, db *sql.DB, gcs *GCPStorage, fullRefresh bool) error {

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Minute)
	defer cancel()

	if err := modifyMappingConfig(&m); err != nil {
		return err
	}

	model := strings.TrimSuffix(filepath.Base(m.SQLFile), filepath.Ext(m.SQLFile))
	if err := pullRefs(ctx, model, m.RefMapping, m.OutputDir, gcs); err != nil {
		return err
	}

	outDir := filepath.Join(m.OutputDir, model)
	_ = os.RemoveAll(outDir)
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return err
	}

	copySQL, err := generateDuckDbSyntax(&m, outDir, model)
	if err != nil {
		return err
	}

	if _, err := db.ExecContext(ctx, copySQL); err != nil {
		return fmt.Errorf("duckdb export failed: %w", err)
	}

	if gcs != nil {
		if err := handleGCS(ctx, &m, model, outDir, fullRefresh, gcs); err != nil {
			return err
		}
	}

	return nil
}

func executeQueries(db *sql.DB, gcs *GCPStorage, modelsToProcess [][]MappingConfig) {
	var fileMu sync.Mutex
	for _, layer := range modelsToProcess {
		var wg sync.WaitGroup
		sem := make(chan struct{}, threads)

		for _, m := range layer {
			m := m
			wg.Add(1)
			sem <- struct{}{}

			go func(m MappingConfig) {
				defer wg.Done()
				defer func() { <-sem }()
				start := time.Now()
				modelErr := processMapping(m, db, gcs, fullRefresh)
				elapsed := time.Since(start).Seconds()

				fileMu.Lock()
				logResult(m.SQLFile, elapsed, modelErr)
				fileMu.Unlock()
			}(m)
		}
		wg.Wait()
	}
}

func logResult(sqlFile string, duration float64, err error) {
	f, ferr := os.OpenFile("results.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if ferr != nil {
		log.Printf("open results.txt: %v", ferr)
		return
	}
	defer f.Close()
	if err != nil {
		fmt.Fprintf(f, "%s - error: %v\n", sqlFile, err)
	} else {
		fmt.Fprintf(f, "%s - duration: %.2fs\n", sqlFile, duration)
	}
}
