package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type ProjectModel struct {
	Config  ModelConfig
	Refs    []string
	Sources []string
}

type Project struct {
	Models map[string]ProjectModel
}

var supportedStorageTypes = map[string]struct{}{
	"":       {},
	"local":  {},
	"gcs":    {},
	"s3":     {},
	"blob":   {},
	"azure":  {},
	"azblob": {},
}

func LoadProject(
	dir string,
	modelParser func(string) (ModelConfig, error),
	refParser func(string) ([]string, error),
	sourceParser func(string) ([]string, error),
) (*Project, error) {
	project := &Project{Models: make(map[string]ProjectModel)}

	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || filepath.Ext(d.Name()) != ".sql" {
			return nil
		}

		modelName := strings.TrimSuffix(d.Name(), ".sql")
		if existing, ok := project.Models[modelName]; ok {
			return fmt.Errorf("duplicate model name %q found in %s and %s", modelName, existing.Config.SQLFile, path)
		}

		cfg, err := modelParser(path)
		if err != nil {
			return err
		}
		refs, err := refParser(path)
		if err != nil {
			return err
		}
		sources, err := sourceParser(path)
		if err != nil {
			return err
		}

		cfg.ModelName = modelName
		cfg.SQLFile = path
		project.Models[modelName] = ProjectModel{
			Config:  cfg,
			Refs:    refs,
			Sources: sources,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return project, nil
}

func ValidateProjectSources(project *Project, sources *SourceCatalog) error {
	for modelName, model := range project.Models {
		for _, sourceRef := range model.Sources {
			parts := strings.SplitN(sourceRef, ".", 2)
			if len(parts) != 2 {
				return fmt.Errorf("model %q has invalid source reference %q", modelName, sourceRef)
			}
			if _, err := sources.Resolve(parts[0], parts[1]); err != nil {
				return fmt.Errorf("model %q references invalid source %q: %w", modelName, sourceRef, err)
			}
		}
	}
	return nil
}

func ValidateProjectModels(project *Project) error {
	for modelName, model := range project.Models {
		cfg := model.Config

		if _, ok := supportedStorageTypes[cfg.StorageType]; !ok {
			return fmt.Errorf("model %q has unsupported storage_type %q", modelName, cfg.StorageType)
		}
		if cfg.StorageType != "" && strings.TrimSpace(cfg.StoragePath) == "" {
			return fmt.Errorf("model %q has storage_type %q but no storage_path", modelName, cfg.StorageType)
		}
		if cfg.IncrementalStrategy != "" && cfg.IncrementalStrategy != "insert_overwrite" {
			return fmt.Errorf("model %q has unsupported incremental_strategy %q", modelName, cfg.IncrementalStrategy)
		}
		if cfg.IncrementalStrategy != "" && !cfg.Incremental {
			return fmt.Errorf("model %q sets incremental_strategy but is not incremental", modelName)
		}
		if cfg.PartitionColumn == "" && cfg.IncrementalStrategy == "insert_overwrite" {
			return fmt.Errorf("model %q uses incremental_strategy %q without partition_column", modelName, cfg.IncrementalStrategy)
		}
	}
	return nil
}
