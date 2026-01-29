#!/usr/bin/env python3
"""
index_data.py: Create index and bulk ingest documents into Elasticsearch.
Measures total indexing time (create index + bulk load).
"""

import json
import argparse
import sys
import time
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


def read_bulk_file_header(bulk_file: str) -> dict:
    """Read first line of bulk file (index creation payload)."""
    with open(bulk_file) as f:
        line = f.readline()
    return json.loads(line)


def delete_index_if_exists(client: Elasticsearch, index_name: str) -> None:
    """Delete index if it exists."""
    try:
        if client.indices.exists(index=index_name):
            client.indices.delete(index=index_name)
            print(f"✓ Deleted existing index: {index_name}")
    except Exception as e:
        print(f"ERROR deleting index: {e}", file=sys.stderr)
        sys.exit(1)


def create_index(client: Elasticsearch, index_name: str, settings: dict, mappings: dict) -> None:
    """Create index with provided settings and mappings."""
    try:
        client.indices.create(
            index=index_name,
            settings=settings,
            mappings=mappings
        )
        print(f"✓ Created index: {index_name}")
    except Exception as e:
        print(f"ERROR creating index: {e}", file=sys.stderr)
        sys.exit(1)


def read_bulk_documents(bulk_file: str):
    """
    Generator: yields (action_metadata, doc_source) tuples from bulk file.
    Skips line 1 (index creation payload).
    """
    with open(bulk_file) as f:
        # Skip line 1
        f.readline()
        
        while True:
            action_line = f.readline()
            if not action_line:
                break
            
            doc_line = f.readline()
            if not doc_line:
                # Malformed: action without doc
                print(f"ERROR: Action line without document", file=sys.stderr)
                sys.exit(1)
            
            action_meta = json.loads(action_line)
            doc_source = json.loads(doc_line)
            
            yield action_meta, doc_source


def bulk_ingest(
    client: Elasticsearch,
    index_name: str,
    bulk_file: str,
    chunk_size: int,
    timeout_seconds: int
) -> tuple:
    """
    Bulk ingest documents. Returns (doc_count, error_count, errors_list).
    """
    client.transport.perform_request(
        "PUT",
        f"/{index_name}/_settings",
        body={"index.refresh_interval": "-1"}  # Disable refresh during bulk
    )
    print(f"✓ Disabled refresh_interval for bulk ingest")
    
    doc_count = 0
    error_count = 0
    errors = []
    
    def action_generator():
        """Yield actions for bulk API."""
        nonlocal doc_count, error_count, errors
        
        for action_meta, doc_source in read_bulk_documents(bulk_file):
            # Extract metadata
            index_op = action_meta.get("index", {})
            _id = index_op.get("_id")
            
            action = {
                "_op_type": "index",
                "_index": index_name,
                "_id": _id,
            }
            action.update(doc_source)
            
            yield action
    
    # Perform bulk ingest
    try:
        for ok, result in bulk(
            client,
            action_generator(),
            chunk_size=chunk_size,
            raise_on_error=False,
            timeout=f"{timeout_seconds}s"
        ):
            if not ok:
                error_count += 1
                errors.append(result)
            else:
                doc_count += 1
    except Exception as e:
        print(f"ERROR during bulk ingest: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Re-enable refresh
    client.transport.perform_request(
        "PUT",
        f"/{index_name}/_settings",
        body={"index.refresh_interval": "1s"}
    )
    
    return doc_count, error_count, errors


def main():
    parser = argparse.ArgumentParser(description="Create index and bulk ingest data")
    parser.add_argument("--es-url", default="http://localhost:9200")
    parser.add_argument("--bulk-file", required=True)
    parser.add_argument("--chunk-docs", type=int, default=2000)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--recreate", type=lambda x: x.lower() == "true", default=False)
    parser.add_argument("--refresh", type=lambda x: x.lower() == "true", default=False)
    
    args = parser.parse_args()
    
    # Read header
    header = read_bulk_file_header(args.bulk_file)
    index_name = header["index"]
    settings = header["settings"]
    mappings = header["mappings"]
    
    # Connect to ES
    try:
        client = Elasticsearch([args.es_url])
        info = client.info()
        print(f"✓ Connected to Elasticsearch {info['version']['number']}")
    except Exception as e:
        print(f"ERROR connecting to ES: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Delete if needed
    if args.recreate:
        delete_index_if_exists(client, index_name)
    
    # Create index
    create_index(client, index_name, settings, mappings)
    
    # Start timer (before bulk ingest)
    start_time = time.time()
    
    # Bulk ingest
    doc_count, error_count, errors = bulk_ingest(
        client,
        index_name,
        args.bulk_file,
        args.chunk_docs,
        args.timeout_seconds
    )
    
    # Stop timer
    elapsed = time.time() - start_time
    
    # Refresh if requested
    if args.refresh:
        try:
            client.indices.refresh(index=index_name)
            print(f"✓ Refreshed index")
        except Exception as e:
            print(f"WARNING: Failed to refresh: {e}")
    
    # Check for errors
    if error_count > 0:
        print(f"\n✗ ERRORS DETECTED: {error_count} failures", file=sys.stderr)
        # Print sample errors (up to 5)
        for i, err in enumerate(errors[:5]):
            print(f"  [{i+1}] {err}", file=sys.stderr)
        sys.exit(1)
    
    # Summary
    throughput = doc_count / elapsed if elapsed > 0 else 0
    summary = {
        "index_name": index_name,
        "docs_indexed": doc_count,
        "elapsed_seconds": round(elapsed, 2),
        "throughput_docs_per_sec": round(throughput, 2)
    }
    print(f"\n✓ INDEXING COMPLETE")
    print(f"  Index: {summary['index_name']}")
    print(f"  Documents: {summary['docs_indexed']}")
    print(f"  Time: {summary['elapsed_seconds']}s")
    print(f"  Throughput: {summary['throughput_docs_per_sec']} docs/sec")


if __name__ == "__main__":
    main()
