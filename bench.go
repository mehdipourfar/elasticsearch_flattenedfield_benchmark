package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
)

// Query represents a single ES query
type Query struct {
	Index string                 `json:"index"`
	Body  map[string]interface{} `json:"body"`
}

// Result holds benchmark results
type Result struct {
	AvgLatencyMs      float64 `json:"avg_latency_ms"`
	P95LatencyMs      float64 `json:"p95_latency_ms"`
	P99LatencyMs      float64 `json:"p99_latency_ms"`
	ThroughputReqSec  float64 `json:"throughput_req_sec"`
	SuccessCount      int64   `json:"success_count"`
	ErrorCount        int64   `json:"error_count"`
	ErrorRate         float64 `json:"error_rate"`
	ElapsedSeconds    float64 `json:"elapsed_seconds"`
	WarmupRequests    int     `json:"warmup_requests"`
	BenchmarkRequests int     `json:"benchmark_requests"`
}

func loadQueries(filename string) ([]Query, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var queries []Query
	if err := json.Unmarshal(data, &queries); err != nil {
		return nil, err
	}

	return queries, nil
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	idx := int(math.Ceil(float64(len(values)) * p / 100.0))
	if idx > len(values) {
		idx = len(values)
	}
	if idx == 0 {
		idx = 1
	}
	return values[idx-1]
}

func runBenchmark(
	client *elasticsearch.Client,
	queries []Query,
	concurrency int,
	warmupRequests int,
	totalRequests int,
	timeoutMs int,
	seed int64,
) (*Result, error) {
	rng := rand.New(rand.NewSource(seed))
	var latencies []float64
	var latenciesMutex sync.Mutex

	var successCount int64
	var errorCount int64

	// Warmup phase
	fmt.Printf("Warmup phase: %d requests...\n", warmupRequests)
	err := runPhase(client, queries, concurrency, warmupRequests, timeoutMs, rng, nil)
	if err != nil {
		return nil, err
	}

	// Benchmark phase
	fmt.Printf("Benchmark phase: %d requests...\n", totalRequests)
	startTime := time.Now()

	latencies = make([]float64, 0, totalRequests)
	err = runPhase(client, queries, concurrency, totalRequests, timeoutMs, rng, &PhaseResults{
		latencies:    &latencies,
		latenciesMu:  &latenciesMutex,
		successCount: &successCount,
		errorCount:   &errorCount,
	})
	if err != nil {
		return nil, err
	}

	elapsedSec := time.Since(startTime).Seconds()

	// Calculate metrics
	avgLat := 0.0
	for _, lat := range latencies {
		avgLat += lat
	}
	avgLat /= float64(len(latencies))

	p95 := percentile(latencies, 95)
	p99 := percentile(latencies, 99)
	throughput := float64(successCount) / elapsedSec

	totalReq := successCount + errorCount
	errorRate := 0.0
	if totalReq > 0 {
		errorRate = float64(errorCount) / float64(totalReq)
	}

	result := &Result{
		AvgLatencyMs:      avgLat,
		P95LatencyMs:      p95,
		P99LatencyMs:      p99,
		ThroughputReqSec:  throughput,
		SuccessCount:      successCount,
		ErrorCount:        errorCount,
		ErrorRate:         errorRate,
		ElapsedSeconds:    elapsedSec,
		WarmupRequests:    warmupRequests,
		BenchmarkRequests: totalRequests,
	}

	return result, nil
}

// PhaseResults holds mutable results during a phase
type PhaseResults struct {
	latencies    *[]float64
	latenciesMu  *sync.Mutex
	successCount *int64
	errorCount   *int64
}

func runPhase(
	client *elasticsearch.Client,
	queries []Query,
	concurrency int,
	numRequests int,
	timeoutMs int,
	rng *rand.Rand,
	results *PhaseResults,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	var wg sync.WaitGroup
	requestChan := make(chan int, concurrency)

	// Worker pool - each worker gets its own RNG
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		workerID := int64(i)
		workerRNG := rand.New(rand.NewSource(rng.Int63() + workerID))

		go func(workerRNG *rand.Rand) {
			defer wg.Done()
			for range requestChan {
				// Pick random query using worker's own RNG
				query := queries[workerRNG.Intn(len(queries))]

				// Send request and measure latency
				start := time.Now()
				statusCode, err := sendQuery(client, &query, time.Duration(timeoutMs)*time.Millisecond)
				elapsed := time.Since(start)

				if results != nil {
					if err != nil || statusCode < 200 || statusCode >= 300 {
						atomic.AddInt64(results.errorCount, 1)
					} else {
						atomic.AddInt64(results.successCount, 1)
						latMs := elapsed.Seconds() * 1000
						results.latenciesMu.Lock()
						*results.latencies = append(*results.latencies, latMs)
						results.latenciesMu.Unlock()
					}
				}
			}
		}(workerRNG)
	}

	// Feed requests
	for i := 0; i < numRequests; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case requestChan <- i:
		}
	}
	close(requestChan)

	wg.Wait()
	return nil
}

func extractFieldsFromQuery(query *Query) []string {
	// Extract field names from query filters
	fields := make(map[string]bool)
	
	if body, ok := query.Body["query"].(map[string]interface{}); ok {
		if boolQuery, ok := body["bool"].(map[string]interface{}); ok {
			if filterList, ok := boolQuery["filter"].([]interface{}); ok {
				for _, f := range filterList {
					if filterMap, ok := f.(map[string]interface{}); ok {
						if term, ok := filterMap["term"].(map[string]interface{}); ok {
							for field := range term {
								// Remove "data." prefix if present (flattened queries)
								cleanField := field
								if len(cleanField) > 5 && cleanField[:5] == "data." {
									cleanField = cleanField[5:]
								}
								fields[cleanField] = true
							}
						}
					}
				}
			}
		}
	}
	
	var result []string
	for field := range fields {
		result = append(result, field)
	}
	return result
}

func mutateQueryWithAgg(query *Query, rng *rand.Rand) *Query {
	// Create a copy of query with random terms aggregation
	mutated := *query
	mutated.Body = make(map[string]interface{})
	
	// Copy existing body fields
	for k, v := range query.Body {
		mutated.Body[k] = v
	}
	
	// Extract fields from query filters
	fields := extractFieldsFromQuery(query)
	if len(fields) == 0 {
		return &mutated
	}
	
	// Pick random field
	selectedField := fields[rng.Intn(len(fields))]
	
	// Determine actual field path (with "data." for flattened)
	fieldPath := selectedField
	if query.Index == "bench_flattened" {
		fieldPath = "data." + selectedField
	}
	
	// Add aggregation
	mutated.Body["aggs"] = map[string]interface{}{
		"field_values": map[string]interface{}{
			"terms": map[string]interface{}{
				"field": fieldPath,
				"size":  10,
			},
		},
	}
	
	return &mutated
}

func mutateQueries(queries []Query, rng *rand.Rand) []Query {
	// Mutate all queries by adding aggregations
	mutated := make([]Query, len(queries))
	for i, q := range queries {
		mutated[i] = *mutateQueryWithAgg(&q, rng)
	}
	return mutated
}

func sendQuery(client *elasticsearch.Client, query *Query, timeout time.Duration) (int, error) {
	bodyJSON, _ := json.Marshal(query.Body)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	resp, err := client.Search(
		client.Search.WithContext(ctx),
		client.Search.WithIndex(query.Index),
		client.Search.WithBody(bytes.NewReader(bodyJSON)),
	)

	if err != nil {
		return 0, err
	}

	statusCode := resp.StatusCode
	io.ReadAll(resp.Body)
	resp.Body.Close()

	return statusCode, nil
}

func main() {
	var (
		esURL          = flag.String("es-url", "http://localhost:9200", "Elasticsearch URL")
		queriesFile    = flag.String("queries-file", "", "Queries JSON file")
		concurrency    = flag.Int("concurrency", 32, "Number of concurrent workers")
		warmupRequests = flag.Int("warmup-requests", 5000, "Warmup requests")
		totalRequests  = flag.Int("total-requests", 100000, "Total benchmark requests")
		timeoutMs      = flag.Int("timeout-ms", 2000, "Request timeout in milliseconds")
		seed           = flag.Int64("seed", 42, "Random seed")
		outputFile     = flag.String("output", "results.json", "Output JSON file")
		benchmarkAggs  = flag.Bool("benchmark-aggs", false, "Also benchmark with aggregations")
	)
	flag.Parse()

	if *queriesFile == "" {
		fmt.Fprintf(os.Stderr, "ERROR: --queries-file is required\n")
		os.Exit(1)
	}

	// Load queries
	queries, err := loadQueries(*queriesFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR loading queries: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("✓ Loaded %d queries\n", len(queries))

	// Get auth from environment if provided
	user := os.Getenv("ES_USER")
	pass := os.Getenv("ES_PASS")
	if user != "" && pass != "" {
		fmt.Printf("✓ Using authentication (ES_USER=%s)\n", user)
	}

	// Connect to ES
	cfg := elasticsearch.Config{
		Addresses: []string{*esURL},
	}

	// Add authentication if provided
	if user != "" && pass != "" {
		cfg.Username = user
		cfg.Password = pass
	}

	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR connecting to ES: %v\n", err)
		os.Exit(1)
	}

	info, err := client.Info()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Could not connect to Elasticsearch\n")
		os.Exit(1)
	}
	info.Body.Close()
	fmt.Printf("✓ Connected to Elasticsearch\n")

	// Run benchmark
	result, err := runBenchmark(
		client,
		queries,
		*concurrency,
		*warmupRequests,
		*totalRequests,
		*timeoutMs,
		*seed,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR during benchmark: %v\n", err)
		os.Exit(1)
	}

	// Write output
	outputJSON, _ := json.MarshalIndent(result, "", "  ")
	if err := os.WriteFile(*outputFile, outputJSON, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR writing output: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("✓ Results written to %s\n\n", *outputFile)

	// Print summary
	fmt.Printf("═══════════════════════════════════════\n")
	fmt.Printf("BENCHMARK RESULTS (Search Only)\n")
	fmt.Printf("═══════════════════════════════════════\n")
	fmt.Printf("Requests (warmup):    %d\n", result.WarmupRequests)
	fmt.Printf("Requests (benchmark): %d\n", result.BenchmarkRequests)
	fmt.Printf("Successes:            %d\n", result.SuccessCount)
	fmt.Printf("Errors:               %d (%.2f%%)\n", result.ErrorCount, result.ErrorRate*100)
	fmt.Printf("───────────────────────────────────────\n")
	fmt.Printf("Elapsed:              %.2fs\n", result.ElapsedSeconds)
	fmt.Printf("Throughput:           %.2f req/sec\n", result.ThroughputReqSec)
	fmt.Printf("───────────────────────────────────────\n")
	fmt.Printf("Avg Latency:          %.2f ms\n", result.AvgLatencyMs)
	fmt.Printf("p95 Latency:          %.2f ms\n", result.P95LatencyMs)
	fmt.Printf("p99 Latency:          %.2f ms\n", result.P99LatencyMs)
	fmt.Printf("═══════════════════════════════════════\n")
	
	// Benchmark with aggregations if requested
	if *benchmarkAggs {
		fmt.Printf("\n")
		rng := rand.New(rand.NewSource(*seed))
		mutatedQueries := mutateQueries(queries, rng)
		
		fmt.Printf("Running aggregation benchmark...\n")
		resultAgg, err := runBenchmark(
			client,
			mutatedQueries,
			*concurrency,
			*warmupRequests,
			*totalRequests,
			*timeoutMs,
			*seed+1, // Different seed for agg phase
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR during agg benchmark: %v\n", err)
			os.Exit(1)
		}
		
		// Write agg output
		aggsOutputFile := strings.Replace(*outputFile, ".json", "_with_aggs.json", 1)
		outputJSON, _ := json.MarshalIndent(resultAgg, "", "  ")
		if err := os.WriteFile(aggsOutputFile, outputJSON, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR writing agg output: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("✓ Aggregation results written to %s\n\n", aggsOutputFile)
		
		// Print agg summary
		fmt.Printf("═══════════════════════════════════════\n")
		fmt.Printf("BENCHMARK RESULTS (Search + Aggs)\n")
		fmt.Printf("═══════════════════════════════════════\n")
		fmt.Printf("Requests (warmup):    %d\n", resultAgg.WarmupRequests)
		fmt.Printf("Requests (benchmark): %d\n", resultAgg.BenchmarkRequests)
		fmt.Printf("Successes:            %d\n", resultAgg.SuccessCount)
		fmt.Printf("Errors:               %d (%.2f%%)\n", resultAgg.ErrorCount, resultAgg.ErrorRate*100)
		fmt.Printf("───────────────────────────────────────\n")
		fmt.Printf("Elapsed:              %.2fs\n", resultAgg.ElapsedSeconds)
		fmt.Printf("Throughput:           %.2f req/sec\n", resultAgg.ThroughputReqSec)
		fmt.Printf("───────────────────────────────────────\n")
		fmt.Printf("Avg Latency:          %.2f ms\n", resultAgg.AvgLatencyMs)
		fmt.Printf("p95 Latency:          %.2f ms\n", resultAgg.P95LatencyMs)
		fmt.Printf("p99 Latency:          %.2f ms\n", resultAgg.P99LatencyMs)
		fmt.Printf("═══════════════════════════════════════\n")
	}
}
