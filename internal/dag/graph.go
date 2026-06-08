package dag

import (
	"fmt"
	"strings"

	"github.com/maksimilian/duckdb-orchestrator/internal/config"
)

type Graph struct {
	project    *config.Project
	dependents map[string][]string
	inDegree   map[string]int
	registry   map[string]config.ModelConfig
}

func ExecutionPlan(project *config.Project) ([][]config.ModelConfig, map[string]config.ModelConfig, error) {
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

	return plan, graph.registry, nil
}

func NewGraph(project *config.Project) (*Graph, error) {
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

func visit(project *config.Project, node string, visited, onStack map[string]bool, stack *[]string) []string {
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
