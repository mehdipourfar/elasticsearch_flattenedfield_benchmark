# Elasticsearch 8 Benchmark Spec: `flattened` vs explicit `keyword` fields

## Objective

Benchmark Elasticsearch 8 `flattened` field versus explicit `keyword` fields using **identical logical data** and **identical logical queries**, only differing in field pathing. Measure:

* Indexing time (end-to-end bulk ingestion duration)
* Search latency: average, p95, p99
* Throughput (requests/sec)
* (CPU/RAM/index size observed externally; not required in scripts)

---

## High-Level Plan (Deliverables)

1. `fields.json` — e-commerce field catalog with allowed values
2. `generate_bulk.py` — generate dataset bulk JSONL (100,000 docs) + index creation payload
3. `index_data.py` — create index and bulk ingest, measure indexing time
4. `generate_queries.py` — generate 5,000 filter-only queries for both index types
5. `bench.go` — Go benchmarking tool using official ES client (configurable concurrency, warmup, total requests)

---

## Global Constraints (Must Follow Exactly)

### Elasticsearch Version

* Elasticsearch **8.x**

### Two Indices

* Keyword index: all fields as explicit root-level `keyword`
* Flattened index: only `id` + `data` (`flattened`)

### Common Index Settings (Both indices)

* `number_of_shards = 1`
* `number_of_replicas = 1`
* `dynamic = false`

### Data Types

* All values are **strings only** (no numbers, booleans, ranges)

---

## 1) `fields.json` — Field Catalog

### Output

A JSON file with exactly **100** fields. Each field has **5–50** unique string values.

### Format

```json
{
  "brand": ["nike", "adidas", "..."],
  "color": ["black", "white", "..."],
  "...": ["..."]
}
```

### Domain Rules (E-commerce realism)

* Field names must be meaningful for an online store (products/orders/shipping/seller/region).
* Values must be human-readable strings (no random tokens).

### Acceptance Criteria

* Valid JSON
* Exactly 100 keys
* Each value list length in [5..50]
* No duplicate values within a field

---

## 2) Dataset Generation — Skewed (Medium) Distribution

We generate 100,000 docs. For each field, values are chosen using a **fixed skewed distribution**.

### Skew Definition (Medium)

For any field with **K** possible values (K in 5..50):

* Sort values by rank (1..K) in the order they appear in `fields.json` array.
* Use the following probability model:

**Top-heavy rule:**

* Rank 1 gets **30%**
* Rank 2 gets **20%**
* Rank 3 gets **10%**
* Remaining **40%** is distributed among ranks 4..K using a geometric tail.

**Tail distribution (ranks 4..K):**
Let `T = K - 3` be tail size. For tail indices `i = 1..T` (where i=1 corresponds to rank 4):

* Weight `w_i = 0.85^(i-1)`
* Normalize tail weights: `p_i = w_i / sum(w)`
* Assign probability: `P(rank=3+i) = 0.40 * p_i`

This is deterministic and reproducible and yields “medium skew”.

### RNG & Reproducibility

All generators MUST accept `--seed`. With a fixed seed, outputs must be deterministic.

---

## 3) `generate_bulk.py` — Bulk JSONL Generator (Python)

### Purpose

Generate one bulk JSONL file for either index mode. File includes:

1. A single-line **index creation payload** (settings + mappings)
2. Bulk actions and documents

### CLI

```bash
python generate_bulk.py \
  --mode keyword|flattened \
  --index-name <index_name> \
  --fields-file fields.json \
  --doc-count 100000 \
  --seed 42 \
  --output bulk_<index_name>.jsonl
```

### Document Shapes

Each document MUST include `id` (string unique).

**Keyword mode doc:**

```json
{
  "id": "doc-000001",
  "brand": "nike",
  "color": "black",
  ...
}
```

**Flattened mode doc:**

```json
{
  "id": "doc-000001",
  "data": {
    "brand": "nike",
    "color": "black",
    ...
  }
}
```

### Mapping Rules

* `dynamic: false` in the mapping root
* Keyword mode: `id` + all 100 fields mapped as `keyword`
* Flattened mode: `id` as `keyword`, `data` as `flattened`

### Index Settings

Include in index creation payload:

* `number_of_shards: 1`
* `number_of_replicas: 1`

### Bulk JSONL Format (strict)

Line 1: **Create index request payload** for Elasticsearch Create Index API:

```json
{
  "index": "<index-name>",
  "settings": {...},
  "mappings": {...}
}
```

Then for each document:

* Action/meta line:

```json
{"index":{"_index":"<index-name>","_id":"doc-000001"}}
```

* Source line: the document JSON

### Acceptance Criteria

* File has 1 + (doc_count * 2) lines
* Deterministic with seed
* Valid JSON on every line

---

## 4) `index_data.py` — Index Creator + Bulk Loader (Python)

### Purpose

Create index based on line 1 payload in bulk file, then bulk ingest remaining lines. Measure total indexing time.

### CLI

```bash
python index_data.py \
  --es-url http://localhost:9200 \
  --bulk-file bulk_<index_name>.jsonl \
  --chunk-docs 2000 \
  --timeout-seconds 120 \
  --recreate true \
  --refresh true
```

### Behavior (must)

1. Read line 1 payload:

   * Extract index name from payload
2. If `--recreate true`:

   * Delete index if exists
3. Create index using the payload (settings + mappings)
4. Start timer immediately before first bulk request
5. Send bulk requests in chunks of `chunk-docs` documents
6. Stop timer after last bulk request completes successfully
7. If `--refresh true`, refresh index
8. Print a final summary line including:

   * index name
   * docs indexed
   * elapsed seconds
   * docs/sec

### Error Handling

* Bulk errors must be detected:

  * If any item failed, print sample failures and exit non-zero.

---

## 5) `generate_queries.py` — Query Generator (Python)

### Purpose

Generate **5,000** filter-only search queries derived from `fields.json`, using the same skewed value selection rules as data generation.

### CLI

```bash
python generate_queries.py \
  --fields-file fields.json \
  --keyword-index <bench_keywords> \
  --flattened-index <bench_flattened> \
  --query-count 5000 \
  --min-filters 1 \
  --max-filters 5 \
  --seed 42 \
  --output-keyword queries_<bench_keywords>.json \
  --output-flattened queries_<bench_flattened>.json
```

### Query Composition Rules

For each query:

* Randomly pick N fields where N ∈ [1..5]
* For each chosen field, pick one value using the **same Skewed (Medium)** distribution.
* Use **filter-only** queries:

**Keyword mode query:**

```json
{
  "index": "bench_keywords",
  "body": {
    "track_total_hits": false,
    "query": {
      "bool": {
        "filter": [
          {"term": {"brand": "nike"}},
          {"term": {"color": "black"}}
        ]
      }
    }
  }
}
```

**Flattened mode query (path differs only):**

```json
{
  "index": "bench_flattened",
  "body": {
    "track_total_hits": false,
    "query": {
      "bool": {
        "filter": [
          {"term": {"data.brand": "nike"}},
          {"term": {"data.color": "black"}}
        ]
      }
    }
  }
}
```

### Output

* Two JSON files, each a JSON array of length 5000:

  * `queries_<keyword_index>.json`
  * `queries_<flattened_index>.json`

### Acceptance Criteria

* Exactly 5000 queries in each file
* Deterministic with seed
* Each query has 1..5 filters
* Filter-only: bool/filter + term only

---

## 6) `bench.go` — Go Benchmark Tool

### Purpose

Load query file into memory and send a configurable number of requests to Elasticsearch using random sampling from the query set.

### Dependencies

* Official client: `github.com/elastic/go-elasticsearch/v8`

### CLI

```bash
go run bench.go \
  --es-url http://localhost:9200 \
  --queries-file queries_<index>.json \
  --concurrency 32 \
  --warmup-requests 5000 \
  --total-requests 100000 \
  --timeout-ms 2000 \
  --seed 42 \
  --output results_<index>.json
```

### Required Behavior

1. Load all queries (JSON array) into memory.
2. Warmup phase:

   * Send `warmup-requests` requests
   * Exclude warmup from metrics
3. Benchmark phase:

   * Send exactly `total-requests` requests
   * Each request picks a query uniformly at random from loaded queries
4. Concurrency:

   * Worker pool of size `concurrency`
5. Metrics (benchmark phase only):

   * average latency (ms)
   * p95 latency (ms)
   * p99 latency (ms)
   * throughput (successful requests per second)
   * error_count, error_rate
6. Output:

   * Write JSON results to `--output`
   * Print human-readable summary to stdout

### Latency Measurement Rules

* Measure wall time per request from just before calling ES to after reading response body.
* Count only HTTP 2xx as success.

### Determinism Notes

* Use seeded RNG for query selection.
* Concurrency can reorder completion times; that’s OK. Selection should still be driven by seeded RNG per worker (document the approach).

---

## Execution Checklist (How to Run End-to-End)

1. ✅ Create `fields.json` — DONE
2. ✅ Generate `generate_bulk.py` — DONE
3. ✅ Generate `index_data.py` — DONE
4. ✅ Generate `generate_queries.py` — DONE
5. ✅ Generate `bench.go` — DONE
6. Generate bulk files:

   ```bash
   python generate_bulk.py --mode keyword --index-name bench_keywords ...
   python generate_bulk.py --mode flattened --index-name bench_flattened ...
   ```
   7. Index both:

       ```bash
       python index_data.py --bulk-file bulk_bench_keywords.jsonl ...
       python index_data.py --bulk-file bulk_bench_flattened.jsonl ...
       ```
       8. Generate queries:

       ```bash
       python generate_queries.py --keyword-index bench_keywords --flattened-index bench_flattened ...
       ```
       9. Benchmark each index separately:

       ```bash
       go run bench.go --queries-file queries_bench_keywords.json --output results_bench_keywords.json ...
       go run bench.go --queries-file queries_bench_flattened.json --output results_bench_flattened.json ...
       ```

   ---
