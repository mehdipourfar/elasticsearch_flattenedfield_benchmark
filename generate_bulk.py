#!/usr/bin/env python3
"""
generate_bulk.py: Generate bulk JSONL for Elasticsearch indexing.
Supports both 'keyword' and 'flattened' modes with skewed (medium) distribution.
"""

import json
import argparse
import sys
from typing import Dict, List, Any
import random


def load_fields(fields_file: str) -> Dict[str, List[str]]:
    """Load fields catalog from JSON file."""
    with open(fields_file) as f:
        return json.load(f)


def compute_skewed_probabilities(num_values: int) -> List[float]:
    """
    Compute skewed (medium) probabilities for K values.
    
    Top-heavy rule:
    - Rank 1: 30%
    - Rank 2: 20%
    - Rank 3: 10%
    - Ranks 4..K: 40% distributed via geometric tail (0.85^(i-1))
    """
    if num_values == 1:
        return [1.0]
    if num_values == 2:
        return [0.3, 0.7]
    if num_values == 3:
        return [0.3, 0.2, 0.5]
    
    # num_values >= 4
    probs = [0.3, 0.2, 0.1]  # Ranks 1, 2, 3
    
    # Tail: ranks 4..K
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


def create_index_payload(index_name: str, mode: str, fields: Dict[str, List[str]]) -> Dict[str, Any]:
    """
    Create index creation payload (settings + mappings).
    
    mode: 'keyword' or 'flattened'
    """
    settings = {
        "number_of_shards": 1,
        "number_of_replicas": 1
    }
    
    if mode == "keyword":
        # All fields as explicit keyword
        properties = {
            "id": {"type": "keyword"}
        }
        for field_name in fields.keys():
            properties[field_name] = {"type": "keyword"}
        
        mappings = {
            "dynamic": False,
            "properties": properties
        }
    elif mode == "flattened":
        # Only id as keyword, data as flattened
        mappings = {
            "dynamic": False,
            "properties": {
                "id": {"type": "keyword"},
                "data": {"type": "flattened"}
            }
        }
    else:
        raise ValueError(f"Unknown mode: {mode}")
    
    return {
        "index": index_name,
        "settings": settings,
        "mappings": mappings
    }


def generate_document(doc_id: int, mode: str, fields: Dict[str, List[str]], samplers: Dict[str, SkewedSampler]) -> Dict[str, Any]:
    """
    Generate a single document.
    
    mode: 'keyword' or 'flattened'
    """
    doc_id_str = f"doc-{doc_id:06d}"
    
    if mode == "keyword":
        doc = {"id": doc_id_str}
        for field_name in fields.keys():
            doc[field_name] = samplers[field_name].sample()
        return doc
    elif mode == "flattened":
        doc = {
            "id": doc_id_str,
            "data": {}
        }
        for field_name in fields.keys():
            doc["data"][field_name] = samplers[field_name].sample()
        return doc
    else:
        raise ValueError(f"Unknown mode: {mode}")


def generate_bulk(
    mode: str,
    index_name: str,
    fields: Dict[str, List[str]],
    doc_count: int,
    seed: int,
    output_file: str
) -> None:
    """
    Generate bulk JSONL file.
    
    Format:
    - Line 1: Index creation payload
    - Lines 2+: Alternating action/meta lines and document lines
    """
    # Create payload
    payload = create_index_payload(index_name, mode, fields)
    
    # Create samplers for each field (seeded for determinism)
    samplers = {}
    for field_name, values in fields.items():
        field_seed = seed + hash(field_name) % (2**31)
        samplers[field_name] = SkewedSampler(values, field_seed)
    
    # Write bulk JSONL
    with open(output_file, 'w') as f:
        # Line 1: Index creation
        f.write(json.dumps(payload) + '\n')
        
        # Lines 2+: Bulk docs
        for doc_id in range(1, doc_count + 1):
            # Action/meta line
            action_line = {
                "index": {
                    "_index": index_name,
                    "_id": f"doc-{doc_id:06d}"
                }
            }
            f.write(json.dumps(action_line) + '\n')
            
            # Document line
            doc = generate_document(doc_id, mode, fields, samplers)
            f.write(json.dumps(doc) + '\n')
    
    # Verify line count
    with open(output_file) as f:
        line_count = sum(1 for _ in f)
    
    expected = 1 + (doc_count * 2)
    if line_count != expected:
        print(f"ERROR: Expected {expected} lines, got {line_count}", file=sys.stderr)
        sys.exit(1)
    
    print(f"✓ Generated {output_file}: {line_count} lines ({doc_count} docs)")


def main():
    parser = argparse.ArgumentParser(description="Generate bulk JSONL for Elasticsearch")
    parser.add_argument("--mode", required=True, choices=["keyword", "flattened"])
    parser.add_argument("--index-name", required=True)
    parser.add_argument("--fields-file", required=True)
    parser.add_argument("--doc-count", type=int, default=100000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", required=True)
    
    args = parser.parse_args()
    
    fields = load_fields(args.fields_file)
    generate_bulk(args.mode, args.index_name, fields, args.doc_count, args.seed, args.output)


if __name__ == "__main__":
    main()
