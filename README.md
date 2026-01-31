# Elasticsearch 8 Benchmark: `flattened` vs explicit `keyword` fields

This project benchmarks Elasticsearch 8 performance comparing:
- **Keyword Index**: all fields as explicit root-level `keyword` fields
- **Flattened Index**: single `flattened` field containing all data

## Quick Start

### Prerequisites
- Elasticsearch 8.x running on `http://localhost:9200`
- Python 3.7+
- Go 1.16+
- `make` installed

### Running the Full Benchmark

```bash
# Default benchmark (search queries only)
make run

# With aggregation benchmarks
make run BENCHMARK_AGGS=true

# With authentication
make run ES_USER=elastic ES_PASS=yourpassword

# Combined
make run ES_USER=elastic ES_PASS=yourpassword BENCHMARK_AGGS=true
```

### Individual Steps

```bash
# 1. Generate bulk data files (100k docs each)
make generate-bulk

# 2. Index data into Elasticsearch
make index

# 3. Generate query files (5k queries each)
make generate-queries

# 4. Run benchmarks
make benchmark

# Or with aggregations
make benchmark BENCHMARK_AGGS=true

# Clean up generated files
make clean
```

### Configuration

Customize via Makefile variables:

```bash
make run \
  ES_URL=http://localhost:9200 \
  DOC_COUNT=100000 \
  QUERY_COUNT=5000 \
  CONCURRENCY=32 \
  WARMUP_REQUESTS=5000 \
  BENCH_REQUESTS=100000 \
  BENCHMARK_AGGS=true \
  SEED=42
```

### Help

```bash
make help
```

## Test Results

### Indexing Performance

✓ INDEXING COMPLETE
  Index: bench_keywords
  Documents: 100000
  Time: 16.31s
  Throughput: 6131.43 docs/sec


✓ INDEXING COMPLETE
  Index: bench_flattened
  Documents: 100000
  Time: 21.24s
  Throughput: 4707.55 docs/sec

**Conclusion**: Keyword fields are **30% faster** for indexing.

### Search Benchmark Results

#### Keyword Index

```text
═══════════════════════════════════════
BENCHMARK RESULTS (Search Only)
═══════════════════════════════════════
Requests (warmup):    5000
Requests (benchmark): 100000
Successes:            99077
Errors:               923 (0.92%)
───────────────────────────────────────
Elapsed:              67.29s
Throughput:           1472.47 req/sec
───────────────────────────────────────
Avg Latency:          124.26 ms
p95 Latency:          700.85 ms
p99 Latency:          1388.83 ms
═══════════════════════════════════════


═══════════════════════════════════════
BENCHMARK RESULTS (Search + Aggs)
═══════════════════════════════════════
Requests (warmup):    5000
Requests (benchmark): 100000
Successes:            99888
Errors:               112 (0.11%)
───────────────────────────────────────
Elapsed:              68.88s
Throughput:           1450.24 req/sec
───────────────────────────────────────
Avg Latency:          135.72 ms
p95 Latency:          708.49 ms
p99 Latency:          1305.16 ms
═══════════════════════════════════════
```

#### Flattened Index

```text
═══════════════════════════════════════
BENCHMARK RESULTS (Search Only)
═══════════════════════════════════════
Requests (warmup):    5000
Requests (benchmark): 100000
Successes:            99909
Errors:               91 (0.09%)
───────────────────────────────────────
Elapsed:              70.39s
Throughput:           1419.36 req/sec
───────────────────────────────────────
Avg Latency:          139.20 ms
p95 Latency:          719.03 ms
p99 Latency:          1283.28 ms
═══════════════════════════════════════

Running aggregation benchmark...
Warmup phase: 5000 requests...
Benchmark phase: 100000 requests...
✓ Aggregation results written to results_bench_flattened_with_aggs.json

═══════════════════════════════════════
BENCHMARK RESULTS (Search + Aggs)
═══════════════════════════════════════
Requests (warmup):    5000
Requests (benchmark): 100000
Successes:            98060
Errors:               1940 (1.94%)
───────────────────────────────────────
Elapsed:              61.37s
Throughput:           1597.81 req/sec
───────────────────────────────────────
Avg Latency:          98.46 ms
p95 Latency:          172.35 ms
p99 Latency:          1097.28 ms
═══════════════════════════════════════
```
