package dag

import (
	"strings"
	"testing"

	"github.com/maksimilian/duckdb-orchestrator/internal/config"
)

func TestExecutionPlan_UsesResolvedSQLPaths(t *testing.T) {
	project := &config.Project{
		Models: map[string]config.ProjectModel{
			"upstream": {
				Config: config.ModelConfig{
					ModelName: "upstream",
					SQLFile:   "/tmp/upstream.sql",
				},
			},
			"downstream": {
				Config: config.ModelConfig{
					ModelName: "downstream",
					SQLFile:   "/tmp/downstream.sql",
				},
				Refs: []string{"upstream"},
			},
		},
	}

	plan, registry, err := ExecutionPlan(project)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(plan) != 2 {
		t.Fatalf("expected 2 layers, got %d", len(plan))
	}
	if plan[0][0].SQLFile != "/tmp/upstream.sql" {
		t.Fatalf("unexpected upstream sql path: %s", plan[0][0].SQLFile)
	}
	if plan[1][0].SQLFile != "/tmp/downstream.sql" {
		t.Fatalf("unexpected downstream sql path: %s", plan[1][0].SQLFile)
	}
	if registry["downstream"].ModelName != "downstream" {
		t.Fatalf("unexpected registry model name: %s", registry["downstream"].ModelName)
	}
}

func TestExecutionPlan_DetectsCycle(t *testing.T) {
	project := &config.Project{
		Models: map[string]config.ProjectModel{
			"a": {Config: config.ModelConfig{ModelName: "a"}, Refs: []string{"b"}},
			"b": {Config: config.ModelConfig{ModelName: "b"}, Refs: []string{"a"}},
		},
	}

	_, _, err := ExecutionPlan(project)
	if err == nil {
		t.Fatal("expected cycle error")
	}
	if !strings.Contains(err.Error(), "a -> b -> a") {
		t.Fatalf("expected cycle path in error, got %v", err)
	}
}

func TestNewGraph(t *testing.T) {
	project := &config.Project{
		Models: map[string]config.ProjectModel{
			"upstream": {
				Config: config.ModelConfig{ModelName: "upstream"},
			},
			"downstream": {
				Config: config.ModelConfig{ModelName: "downstream"},
				Refs:   []string{"upstream"},
			},
		},
	}

	graph, err := NewGraph(project)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if graph.inDegree["upstream"] != 0 {
		t.Fatalf("expected upstream indegree 0, got %d", graph.inDegree["upstream"])
	}
	if graph.inDegree["downstream"] != 1 {
		t.Fatalf("expected downstream indegree 1, got %d", graph.inDegree["downstream"])
	}
	if len(graph.dependents["upstream"]) != 1 || graph.dependents["upstream"][0] != "downstream" {
		t.Fatalf("unexpected dependents for upstream: %v", graph.dependents["upstream"])
	}
}
