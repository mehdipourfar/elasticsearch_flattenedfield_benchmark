.PHONY: help generate-bulk index generate-queries benchmark run clean

# Default target
help:
	@echo "Elasticsearch 8 Benchmark: flattened vs keyword fields"
	@echo ""
	@echo "Available targets:"
	@echo "  make generate-bulk      Generate 100k docs bulk JSONL files (both modes)"
	@echo "  make index              Index data into ES (both indices)"
	@echo "  make generate-queries   Generate 5k filter queries (both modes)"
	@echo "  make benchmark          Run benchmarks (both indices)"
	@echo "  make run                Full pipeline: generate + index + query + benchmark"
	@echo "  make clean              Clean up bulk/query/result files"
	@echo "  make help               Show this help"
	@echo ""
	@echo "Configuration:"
	@echo "  ES_URL          = http://localhost:9200"
	@echo "  ES_USER         = (optional, env var for auth)"
	@echo "  ES_PASS         = (optional, env var for auth)"
	@echo "  DOC_COUNT       = 100000"
	@echo "  QUERY_COUNT     = 5000"
	@echo "  SEED            = 42"
	@echo "  CONCURRENCY     = 32"
	@echo "  WARMUP          = 5000"
	@echo "  BENCH_REQS      = 100000"
	@echo "  BENCHMARK_AGGS  = false (set to 'true' to benchmark with aggregations)"
	@echo ""
	@echo "Examples:"
	@echo "  make run                                    # Basic benchmark"
	@echo "  make run ES_USER=elastic ES_PASS=password  # With authentication"
	@echo "  make run BENCHMARK_AGGS=true               # With aggregation benchmarks"
	@echo "  make benchmark BENCHMARK_AGGS=true         # Only aggregation benchmarks"

# Configuration variables
ES_URL ?= http://localhost:9200
ES_USER ?=
ES_PASS ?=
DOC_COUNT ?= 100000
QUERY_COUNT ?= 5000
SEED ?= 42
CONCURRENCY ?= 32
WARMUP_REQUESTS ?= 5000
BENCH_REQUESTS ?= 100000
CHUNK_DOCS ?= 2000
TIMEOUT_SEC ?= 120
BENCHMARK_AGGS ?= false

# Export environment variables
export ES_URL
export ES_USER
export ES_PASS

# Generated files
BULK_KW = bulk_bench_keywords.jsonl
BULK_FLAT = bulk_bench_flattened.jsonl
QUERIES_KW = queries_bench_keywords.json
QUERIES_FLAT = queries_bench_flattened.json
RESULTS_KW = results_bench_keywords.json
RESULTS_FLAT = results_bench_flattened.json

# Targets
generate-bulk: $(BULK_KW) $(BULK_FLAT)
	@echo "✓ Bulk files generated"

$(BULK_KW):
	@echo "Generating keyword bulk file..."
	python3 generate_bulk.py \
		--mode keyword \
		--index-name bench_keywords \
		--fields-file fields.json \
		--doc-count $(DOC_COUNT) \
		--seed $(SEED) \
		--output $(BULK_KW)

$(BULK_FLAT):
	@echo "Generating flattened bulk file..."
	python3 generate_bulk.py \
		--mode flattened \
		--index-name bench_flattened \
		--fields-file fields.json \
		--doc-count $(DOC_COUNT) \
		--seed $(SEED) \
		--output $(BULK_FLAT)

index: $(BULK_KW) $(BULK_FLAT)
	@echo "Indexing keyword documents..."
	python3 index_data.py \
		--es-url $(ES_URL) \
		--bulk-file $(BULK_KW) \
		--chunk-docs $(CHUNK_DOCS) \
		--timeout-seconds $(TIMEOUT_SEC) \
		--recreate true \
		--refresh true
	@echo ""
	@echo "Indexing flattened documents..."
	python3 index_data.py \
		--es-url $(ES_URL) \
		--bulk-file $(BULK_FLAT) \
		--chunk-docs $(CHUNK_DOCS) \
		--timeout-seconds $(TIMEOUT_SEC) \
		--recreate true \
		--refresh true

generate-queries: $(QUERIES_KW) $(QUERIES_FLAT)
	@echo "✓ Query files generated"

$(QUERIES_KW) $(QUERIES_FLAT):
	@echo "Generating query sets..."
	python3 generate_queries.py \
		--fields-file fields.json \
		--keyword-index bench_keywords \
		--flattened-index bench_flattened \
		--query-count $(QUERY_COUNT) \
		--min-filters 1 \
		--max-filters 5 \
		--seed $(SEED) \
		--output-keyword $(QUERIES_KW) \
		--output-flattened $(QUERIES_FLAT)

benchmark: $(QUERIES_KW) $(QUERIES_FLAT)
	@echo "Benchmarking keyword index..."
	go run bench.go \
		--es-url $(ES_URL) \
		--queries-file $(QUERIES_KW) \
		--concurrency $(CONCURRENCY) \
		--warmup-requests $(WARMUP_REQUESTS) \
		--total-requests $(BENCH_REQUESTS) \
		--timeout-ms 2000 \
		--seed $(SEED) \
		--output $(RESULTS_KW) \
		--benchmark-aggs $(BENCHMARK_AGGS)
	@echo ""
	@echo "Benchmarking flattened index..."
	go run bench.go \
		--es-url $(ES_URL) \
		--queries-file $(QUERIES_FLAT) \
		--concurrency $(CONCURRENCY) \
		--warmup-requests $(WARMUP_REQUESTS) \
		--total-requests $(BENCH_REQUESTS) \
		--timeout-ms 2000 \
		--seed $(SEED) \
		--output $(RESULTS_FLAT) \
		--benchmark-aggs $(BENCHMARK_AGGS)

run: generate-bulk index generate-queries benchmark
	@echo ""
	@echo "✓ Full benchmark pipeline complete!"
	@echo "Results: benchmark_results.md"

clean:
	@echo "Cleaning up generated files..."
	rm -f $(BULK_KW) $(BULK_FLAT)
	rm -f $(QUERIES_KW) $(QUERIES_FLAT)
	rm -f $(RESULTS_KW) $(RESULTS_FLAT)
	@echo "✓ Cleaned"

.PHONY: help generate-bulk index generate-queries benchmark run clean
