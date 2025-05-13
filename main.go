package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/marcboeker/go-duckdb"
)

var (
	threads     int
	fullRefresh bool
)

func init() {
	flag.IntVar(&threads, "threads", 1, "Threads amount")
	flag.BoolVar(&fullRefresh, "full-refresh", false, "Full refresh")
}

func main() {
	fmt.Println("Program started")
	start := time.Now()

	flag.Parse()
	err := godotenv.Load()
	if err != nil {
		fmt.Println("Error loading .env file")
	}

	db, err := sql.Open("duckdb", os.Getenv("DUCK_DB_FILE"))
	if err != nil {
		log.Fatalf("open DuckDB: %v", err)
	}
	defer db.Close()
	db.SetConnMaxLifetime(180 * time.Minute)

	mappings, err := loadMappingConfig(os.Getenv("MAPPINGS_PATH"))
	if err != nil {
		log.Fatalf("load mappings: %v", err)
	}

	models := make(map[string]MappingConfig)
	for _, m := range mappings {
		models[strings.Split(string(m.SQLFile), ".sql")[0]] = m
	}

	modelsToProcess := modelsExecutionOrder(os.Getenv("MODELS_FOLDER"), models)
	fmt.Println(modelsToProcess)

	gcs := initGCS(os.Getenv("GCP_BUCKET"))

	executeQueries(db, gcs, modelsToProcess)

	fmt.Printf("All completed in %v seconds\n", time.Since(start))
}
