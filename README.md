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



Benchmarking keyword index...
./bench \
	--es-url http://172.21.40.169:9200 \
	--queries-file queries_bench_keywords.json \
	--concurrency 32 \
	--warmup-requests 5000 \
	--total-requests 100000 \
	--timeout-ms 2000 \
	--seed 42 \
	--output results_bench_keywords.json
✓ Loaded 5000 queries
✓ Using authentication (ES_USER=elastic)
✓ Connected to Elasticsearch
Warmup phase: 5000 requests...
Benchmark phase: 100000 requests...
✓ Results written to results_bench_keywords.json

═══════════════════════════════════════
BENCHMARK RESULTS
═══════════════════════════════════════
Requests (warmup):    5000
Requests (benchmark): 100000
Successes:            100000
Errors:               0 (0.00%)
───────────────────────────────────────
Elapsed:              65.25s
Throughput:           1532.61 req/sec
───────────────────────────────────────
Avg Latency:          20.80 ms
p95 Latency:          91.37 ms
p99 Latency:          300.34 ms
═══════════════════════════════════════

Benchmarking flattened index...
./bench \
	--es-url http://172.21.40.169:9200 \
	--queries-file queries_bench_flattened.json \
	--concurrency 32 \
	--warmup-requests 5000 \
	--total-requests 100000 \
	--timeout-ms 2000 \
	--seed 42 \
	--output results_bench_flattened.json
✓ Loaded 5000 queries
✓ Using authentication (ES_USER=elastic)
✓ Connected to Elasticsearch
Warmup phase: 5000 requests...
Benchmark phase: 100000 requests...
✓ Results written to results_bench_flattened.json

═══════════════════════════════════════
BENCHMARK RESULTS
═══════════════════════════════════════
Requests (warmup):    5000
Requests (benchmark): 100000
Successes:            100000
Errors:               0 (0.00%)
───────────────────────────────────────
Elapsed:              66.16s
Throughput:           1511.60 req/sec
───────────────────────────────────────
Avg Latency:          21.12 ms
p95 Latency:          96.04 ms
p99 Latency:          301.76 ms
═══════════════════════════════════════
