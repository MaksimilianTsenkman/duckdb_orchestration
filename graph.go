package main

import (
	"log"
	"os"
	"path/filepath"
	"strings"
)

type Graph map[string][]string

func buildGraphFromRefs(dir string) (Graph, error) {
	g := make(Graph)
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, f := range files {
		if filepath.Ext(f.Name()) != ".sql" {
			continue
		}
		model := strings.TrimSuffix(f.Name(), ".sql")

		refs, err := parseSQLFileRefs(filepath.Join(dir, f.Name()))
		if err != nil {
			return nil, err
		}

		for _, dep := range refs {
			g[dep] = append(g[dep], model)
		}

		if _, ok := g[model]; !ok {
			g[model] = []string{}
		}
	}

	return g, nil
}

func topologicalSortLayers(g Graph) [][]string {
	inDegrees := computeInDegrees(g)
	var zeroInDegree []string
	for node, deg := range inDegrees {
		if deg == 0 {
			zeroInDegree = append(zeroInDegree, node)
		}
	}

	var layers [][]string
	for len(zeroInDegree) > 0 {
		layer := zeroInDegree
		layers = append(layers, layer)
		var nextZero []string

		for _, node := range layer {
			for _, dep := range g[node] {
				inDegrees[dep]--
				if inDegrees[dep] == 0 {
					nextZero = append(nextZero, dep)
				}
			}
		}
		zeroInDegree = nextZero
	}

	return layers
}

func computeInDegrees(g Graph) map[string]int {
	inDegrees := make(map[string]int)
	for node := range g {
		inDegrees[node] = 0
	}

	for _, dependents := range g {
		for _, dep := range dependents {
			inDegrees[dep]++
		}
	}
	return inDegrees
}

func modelsExecutionOrder(modelsDir string, mappings map[string]MappingConfig) [][]MappingConfig {
	graph, err := buildGraphFromRefs(modelsDir)
	if err != nil {
		log.Fatalf("dependency graph build failed: %v", err)
	}
	layers := topologicalSortLayers(graph)
	plan := make([][]MappingConfig, 0, len(layers))
	for _, layer := range layers {
		var batch []MappingConfig
		for _, model := range layer {
			if cfg, ok := mappings[model]; ok {
				batch = append(batch, cfg)
			}
		}
		if len(batch) > 0 {
			plan = append(plan, batch)
		}
	}
	return plan
}
