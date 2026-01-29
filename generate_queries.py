#!/usr/bin/env python3
"""
generate_queries.py: Generate filter-only queries for benchmarking.
Creates separate query sets for keyword vs flattened indices.
"""

import json
import argparse
import sys
import random
from typing import Dict, List, Any


def load_fields(fields_file: str) -> Dict[str, List[str]]:
    """Load fields catalog from JSON file."""
    with open(fields_file) as f:
        return json.load(f)


def compute_skewed_probabilities(num_values: int) -> List[float]:
    """
    Compute skewed (medium) probabilities for K values.
    (Same as generate_bulk.py for consistency)
    """
    if num_values == 1:
        return [1.0]
    if num_values == 2:
        return [0.3, 0.7]
    if num_values == 3:
        return [0.3, 0.2, 0.5]
    
    # num_values >= 4
    probs = [0.3, 0.2, 0.1]
    tail_size = num_values - 3
    tail_weights = [0.85 ** (i - 1) for i in range(1, tail_size + 1)]
    tail_sum = sum(tail_weights)
    tail_probs = [0.40 * (w / tail_sum) for w in tail_weights]
    
    probs.extend(tail_probs)
    return probs


class SkewedSampler:
    """Deterministic, seeded sampler using skewed distribution."""
    
    def __init__(self, values: List[str], seed: int):
        self.values = values
        self.rng = random.Random(seed)
        self.probs = compute_skewed_probabilities(len(values))
    
    def sample(self) -> str:
        """Sample one value from values using skewed distribution."""
        return self.rng.choices(self.values, weights=self.probs, k=1)[0]


def generate_query(
    index_name: str,
    mode: str,
    field_names: List[str],
    field_values: Dict[str, str]
) -> Dict[str, Any]:
    """
    Generate a single filter-only query.
    
    mode: 'keyword' or 'flattened'
    field_values: {field_name: sampled_value}
    """
    filter_terms = []
    
    for field_name in field_names:
        value = field_values[field_name]
        
        if mode == "keyword":
            filter_terms.append({"term": {field_name: value}})
        elif mode == "flattened":
            filter_terms.append({"term": {f"data.{field_name}": value}})
        else:
            raise ValueError(f"Unknown mode: {mode}")
    
    query = {
        "index": index_name,
        "body": {
            "track_total_hits": False,
            "query": {
                "bool": {
                    "filter": filter_terms
                }
            }
        }
    }
    
    return query


def generate_queries(
    keyword_index: str,
    flattened_index: str,
    fields: Dict[str, List[str]],
    query_count: int,
    min_filters: int,
    max_filters: int,
    seed: int,
    output_keyword: str,
    output_flattened: str
) -> None:
    """
    Generate query sets for both indices.
    """
    # Create samplers (seeded for determinism)
    samplers = {}
    for field_name, values in fields.items():
        field_seed = seed + hash(field_name) % (2**31)
        samplers[field_name] = SkewedSampler(values, field_seed)
    
    # RNG for query generation
    rng = random.Random(seed)
    field_names_list = list(fields.keys())
    
    keyword_queries = []
    flattened_queries = []
    
    for _ in range(query_count):
        # Pick N fields (N in [min_filters..max_filters])
        num_filters = rng.randint(min_filters, max_filters)
        selected_fields = rng.sample(field_names_list, num_filters)
        
        # Sample values for each selected field
        field_values = {}
        for field_name in selected_fields:
            field_values[field_name] = samplers[field_name].sample()
        
        # Generate keyword mode query
        kw_query = generate_query(keyword_index, "keyword", selected_fields, field_values)
        keyword_queries.append(kw_query)
        
        # Generate flattened mode query
        flat_query = generate_query(flattened_index, "flattened", selected_fields, field_values)
        flattened_queries.append(flat_query)
    
    # Write output files
    with open(output_keyword, 'w') as f:
        json.dump(keyword_queries, f, indent=2)
    print(f"✓ Generated {output_keyword}: {len(keyword_queries)} queries")
    
    with open(output_flattened, 'w') as f:
        json.dump(flattened_queries, f, indent=2)
    print(f"✓ Generated {output_flattened}: {len(flattened_queries)} queries")
    
    # Verify
    assert len(keyword_queries) == query_count, f"Expected {query_count} queries"
    assert len(flattened_queries) == query_count, f"Expected {query_count} queries"
    assert all(min_filters <= len(q["body"]["query"]["bool"]["filter"]) <= max_filters for q in keyword_queries)
    print(f"✓ All queries have {min_filters}..{max_filters} filters")


def main():
    parser = argparse.ArgumentParser(description="Generate filter-only queries")
    parser.add_argument("--fields-file", required=True)
    parser.add_argument("--keyword-index", required=True)
    parser.add_argument("--flattened-index", required=True)
    parser.add_argument("--query-count", type=int, default=5000)
    parser.add_argument("--min-filters", type=int, default=1)
    parser.add_argument("--max-filters", type=int, default=5)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-keyword", required=True)
    parser.add_argument("--output-flattened", required=True)
    
    args = parser.parse_args()
    
    fields = load_fields(args.fields_file)
    generate_queries(
        args.keyword_index,
        args.flattened_index,
        fields,
        args.query_count,
        args.min_filters,
        args.max_filters,
        args.seed,
        args.output_keyword,
        args.output_flattened
    )


if __name__ == "__main__":
    main()
