#!/usr/bin/env python3
"""
index_data.py: Create index and bulk ingest documents into Elasticsearch.
Measures total indexing time (create index + bulk load).
Supports authentication via ES_USER and ES_PASS environment variables.
"""

import json
import argparse
import sys
import time
import requests
import os
from requests.auth import HTTPBasicAuth


def read_bulk_file_header(bulk_file: str) -> dict:
    """Read first line of bulk file (index creation payload)."""
    with open(bulk_file) as f:
        line = f.readline()
    return json.loads(line)


def get_auth():
    """Get authentication tuple if credentials are provided."""
    user = os.getenv("ES_USER")
    passwd = os.getenv("ES_PASS")
    if user and passwd:
        return HTTPBasicAuth(user, passwd)
    return None


def delete_index_if_exists(es_url: str, index_name: str, auth) -> None:
    """Delete index if it exists."""
    try:
        resp = requests.head(f"{es_url}/{index_name}", auth=auth)
        if resp.status_code == 200:
            r = requests.delete(f"{es_url}/{index_name}", auth=auth)
            if r.status_code == 200:
                print(f"✓ Deleted existing index: {index_name}")
    except Exception as e:
        print(f"ERROR deleting index: {e}", file=sys.stderr)
        sys.exit(1)


def create_index(es_url: str, index_name: str, settings: dict, mappings: dict, auth) -> None:
    """Create index with provided settings and mappings."""
    try:
        payload = {
            "settings": settings,
            "mappings": mappings
        }
        r = requests.put(
            f"{es_url}/{index_name}",
            json=payload,
            headers={"Content-Type": "application/json"},
            auth=auth
        )
        if r.status_code not in (200, 201):
            print(f"ERROR creating index: {r.status_code} {r.text}", file=sys.stderr)
            sys.exit(1)
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
    es_url: str,
    index_name: str,
    bulk_file: str,
    chunk_size: int,
    timeout_seconds: int,
    auth
) -> tuple:
    """
    Bulk ingest documents. Returns (doc_count, error_count, errors_list).
    """
    # Disable refresh
    r = requests.put(
        f"{es_url}/{index_name}/_settings",
        json={"index.refresh_interval": "-1"},
        headers={"Content-Type": "application/json"},
        auth=auth
    )
    print(f"✓ Disabled refresh_interval for bulk ingest")

    doc_count = 0
    error_count = 0
    errors = []

    # Read all bulk lines (skip header)
    bulk_lines = []
    with open(bulk_file) as f:
        f.readline()  # Skip header
        bulk_lines = [line.rstrip('\n') for line in f]

    # Send in chunks (each doc = 2 lines: action + source)
    for chunk_start in range(0, len(bulk_lines), chunk_size * 2):
        chunk_end = min(chunk_start + chunk_size * 2, len(bulk_lines))
        chunk = bulk_lines[chunk_start:chunk_end]

        # Join lines and send
        body = '\n'.join(chunk) + '\n'
        try:
            r = requests.post(
                f"{es_url}/_bulk",
                data=body,
                headers={"Content-Type": "application/x-ndjson"},
                timeout=timeout_seconds,
                auth=auth
            )

            if r.status_code != 200:
                print(f"WARNING: Bulk response {r.status_code}")

            # Count successful docs from response
            try:
                resp_json = r.json()
                if "items" in resp_json:
                    for item in resp_json["items"]:
                        if "index" in item:
                            if item["index"].get("status") in (200, 201):
                                doc_count += 1
                            else:
                                error_count += 1
                                errors.append(item["index"])
            except Exception as e:
                print(f"WARNING: Could not parse bulk response: {e}")
        except Exception as e:
            print(f"ERROR during bulk chunk: {e}", file=sys.stderr)
            sys.exit(1)

    # Re-enable refresh
    requests.put(
        f"{es_url}/{index_name}/_settings",
        json={"index.refresh_interval": "1s"},
        headers={"Content-Type": "application/json"},
        auth=auth
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

    # Get auth if provided
    auth = get_auth()
    if auth:
        print(f"✓ Using authentication (ES_USER={os.getenv('ES_USER')})")

    # Connect to ES
    try:
        r = requests.get(f"{args.es_url}/", auth=auth)
        if r.status_code == 200:
            info = r.json()
            print(f"✓ Connected to Elasticsearch {info['version']['number']}")
        else:
            print(f"ERROR: Could not connect to Elasticsearch", file=sys.stderr)
            sys.exit(1)
    except Exception as e:
        print(f"ERROR connecting to ES: {e}", file=sys.stderr)
        sys.exit(1)

    # Delete if needed
    if args.recreate:
        delete_index_if_exists(args.es_url, index_name, auth)

    # Create index
    create_index(args.es_url, index_name, settings, mappings, auth)

    # Start timer (before bulk ingest)
    start_time = time.time()

    # Bulk ingest
    doc_count, error_count, errors = bulk_ingest(
        args.es_url,
        index_name,
        args.bulk_file,
        args.chunk_docs,
        args.timeout_seconds,
        auth
    )

    # Stop timer
    elapsed = time.time() - start_time

    # Refresh if requested
    if args.refresh:
        try:
            r = requests.post(f"{args.es_url}/{index_name}/_refresh", auth=auth)
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
