package dag

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/maksimilian/duckdb-orchestrator/internal/config"
)

type Graph struct {
	project    *Project
	dependents map[string][]string
	inDegree   map[string]int
	registry   map[string]config.ModelConfig
}

type Project struct {
	Models map[string]Model
}

type Model struct {
	Config  config.ModelConfig
	Refs    []string
	Sources []string
}

var supportedStorageLocations = map[string]struct{}{
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
	modelParser func(string) (config.ModelConfig, error),
	refParser func(string) ([]string, error),
	sourceParser func(string) ([]string, error),
) (*Project, error) {
	project := &Project{Models: make(map[string]Model)}

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
		project.Models[modelName] = Model{
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

func ValidateSources(project *Project, sources *config.SourceCatalog) error {
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

func ValidateModelConfigs(project *Project) error {
	for modelName, model := range project.Models {
		cfg := model.Config

		if _, ok := supportedStorageLocations[cfg.StorageLocation]; !ok {
			return fmt.Errorf("model %q has unsupported storage_location %q", modelName, cfg.StorageLocation)
		}
		if cfg.StorageLocation != "" && strings.TrimSpace(cfg.StorageOption) == "" {
			return fmt.Errorf("model %q has storage_location %q but no storage_option", modelName, cfg.StorageLocation)
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

func ExecutionPlan(project *Project) ([][]config.ModelConfig, map[string]config.ModelConfig, error) {
	graph, err := NewGraph(project)
	if err != nil {
		return nil, nil, err
	}

	plan, processed := graph.LayeredPlan()
	if processed != len(project.Models) {
		cycle := graph.DetectCycle()
		if len(cycle) > 0 {
			return nil, nil, fmt.Errorf("dependency cycle detected: %s", strings.Join(cycle, " -> "))
		}
		return nil, nil, fmt.Errorf("dependency cycle detected")
	}

	return plan, graph.Registry(), nil
}

func NewGraph(project *Project) (*Graph, error) {
	graph := &Graph{
		project:    project,
		dependents: make(map[string][]string, len(project.Models)),
		inDegree:   make(map[string]int, len(project.Models)),
		registry:   make(map[string]config.ModelConfig, len(project.Models)),
	}

	for name, model := range project.Models {
		graph.registry[name] = model.Config
		graph.inDegree[name] = 0
	}

	for name, model := range project.Models {
		seen := make(map[string]struct{})
		for _, ref := range model.Refs {
			if _, ok := project.Models[ref]; !ok {
				return nil, fmt.Errorf("model %q references unknown model %q", name, ref)
			}
			if _, ok := seen[ref]; ok {
				continue
			}
			seen[ref] = struct{}{}
			graph.dependents[ref] = append(graph.dependents[ref], name)
			graph.inDegree[name]++
		}
	}

	return graph, nil
}

func (g *Graph) Registry() map[string]config.ModelConfig {
	return g.registry
}

func (g *Graph) LayeredPlan() ([][]config.ModelConfig, int) {
	inDegree := make(map[string]int, len(g.inDegree))
	for name, deg := range g.inDegree {
		inDegree[name] = deg
	}

	var zeroInDegree []string
	for name, deg := range inDegree {
		if deg == 0 {
			zeroInDegree = append(zeroInDegree, name)
		}
	}

	processed := 0
	var plan [][]config.ModelConfig
	for len(zeroInDegree) > 0 {
		layerNames := zeroInDegree
		zeroInDegree = nil

		layer := make([]config.ModelConfig, 0, len(layerNames))
		for _, name := range layerNames {
			layer = append(layer, g.registry[name])
			processed++
			for _, dependent := range g.dependents[name] {
				inDegree[dependent]--
				if inDegree[dependent] == 0 {
					zeroInDegree = append(zeroInDegree, dependent)
				}
			}
		}
		plan = append(plan, layer)
	}

	return plan, processed
}

func (g *Graph) DetectCycle() []string {
	visited := make(map[string]bool, len(g.project.Models))
	onStack := make(map[string]bool, len(g.project.Models))
	stack := make([]string, 0, len(g.project.Models))

	for name := range g.project.Models {
		if !visited[name] {
			if cycle := visit(g.project, name, visited, onStack, &stack); len(cycle) > 0 {
				return cycle
			}
		}
	}
	return nil
}

func visit(project *Project, node string, visited, onStack map[string]bool, stack *[]string) []string {
	visited[node] = true
	onStack[node] = true
	*stack = append(*stack, node)

	for _, ref := range project.Models[node].Refs {
		if _, ok := project.Models[ref]; !ok {
			continue
		}
		if !visited[ref] {
			if cycle := visit(project, ref, visited, onStack, stack); len(cycle) > 0 {
				return cycle
			}
			continue
		}
		if onStack[ref] {
			start := 0
			for i, name := range *stack {
				if name == ref {
					start = i
					break
				}
			}
			cycle := append([]string{}, (*stack)[start:]...)
			cycle = append(cycle, ref)
			return cycle
		}
	}

	onStack[node] = false
	*stack = (*stack)[:len(*stack)-1]
	return nil
}
