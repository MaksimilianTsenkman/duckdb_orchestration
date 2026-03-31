package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type ModelConfig struct {
	ModelName           string            `json:"-"`
	SQLFile             string            `json:"sql_file"`
	RefMapping          map[string]string `json:"ref_mapping"`
	SourceMapping       map[string]string `json:"-"`
	OutputDir           string            `json:"output_dir"`
	StorageLocation     string            `json:"-"`
	StorageOption       string            `json:"-"`
	SplitRows           int               `json:"split_rows,omitempty"`
	Incremental         bool              `json:"incremental"`
	IncrementalStrategy string            `json:"-"`
	PartitionColumn     string            `json:"partition_column_date"`
}

type Profile struct {
	DuckDBFile   string `yaml:"duckdb_file"`
	ModelsFolder string `yaml:"models_folder"`
	OutputFolder string `yaml:"output_folder"`
	SourcesPath  string `yaml:"sources_path"`
}

type SourceCatalog struct {
	Sources []SourceDefinition `yaml:"sources"`

	index map[string]ResolvedSource
}

type ResolvedSource struct {
	Type     string
	Location string
}

type SourceDefinition struct {
	Name   string            `yaml:"name"`
	Type   string            `yaml:"type"`
	Path   string            `yaml:"path"`
	Tables []SourceTableSpec `yaml:"tables"`
}

type SourceTableSpec struct {
	Name string `yaml:"name"`
	Path string `yaml:"path"`
}

func LoadProfile(configPath string) (*Profile, error) {
	var cfg Profile
	if err := loadYAML(configPath, &cfg); err != nil {
		return nil, err
	}
	return &cfg, cfg.Validate()
}

func (p *Profile) Validate() error {
	if p == nil {
		return fmt.Errorf("profile is not configured")
	}
	if strings.TrimSpace(p.DuckDBFile) == "" {
		return fmt.Errorf("profile.duckdb_file is required")
	}
	if strings.TrimSpace(p.ModelsFolder) == "" {
		return fmt.Errorf("profile.models_folder is required")
	}
	if strings.TrimSpace(p.OutputFolder) == "" {
		return fmt.Errorf("profile.output_folder is required")
	}
	if strings.TrimSpace(p.SourcesPath) == "" {
		return fmt.Errorf("profile.sources_path is required")
	}
	return nil
}

func LoadSourceCatalog(configPath string) (*SourceCatalog, error) {
	if configPath == "" {
		return &SourceCatalog{index: make(map[string]ResolvedSource)}, nil
	}

	var cfg SourceCatalog
	if err := loadYAML(configPath, &cfg); err != nil {
		return nil, err
	}
	cfg.buildIndex()

	return &cfg, nil
}

func (c *SourceCatalog) Resolve(sourceName, tableName string) (*ResolvedSource, error) {
	if c == nil {
		return nil, fmt.Errorf("source catalog is not configured")
	}
	if c.index == nil {
		c.buildIndex()
	}

	key := sourceKey(sourceName, tableName)
	resolved, ok := c.index[key]
	if !ok {
		sourceExists := false
		for _, source := range c.Sources {
			if source.Name == sourceName {
				sourceExists = true
				break
			}
		}
		if !sourceExists {
			return nil, fmt.Errorf("source %q not found", sourceName)
		}
		return nil, fmt.Errorf("table %q not found under source %q", tableName, sourceName)
	}
	return &resolved, nil
}

func (c *SourceCatalog) buildIndex() {
	c.index = make(map[string]ResolvedSource, len(c.Sources))
	for _, source := range c.Sources {
		sourceType := strings.ToLower(strings.TrimSpace(source.Type))
		for _, table := range source.Tables {
			location := table.Path
			if location == "" {
				location = joinLocation(source.Path, table.Name)
			} else {
				location = joinLocation(source.Path, location)
			}
			if location == "" {
				continue
			}
			c.index[sourceKey(source.Name, table.Name)] = ResolvedSource{
				Type:     sourceType,
				Location: location,
			}
		}
	}
}

func joinLocation(base, rel string) string {
	if rel == "" {
		return strings.TrimSpace(base)
	}
	if base == "" || strings.Contains(rel, "://") || strings.HasPrefix(rel, "/") {
		return strings.TrimSpace(rel)
	}
	return strings.TrimRight(strings.TrimSpace(base), "/") + "/" + strings.TrimLeft(strings.TrimSpace(rel), "/")
}

func sourceKey(sourceName, tableName string) string {
	return sourceName + "." + tableName
}

func loadYAML(path string, target any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, target)
}
