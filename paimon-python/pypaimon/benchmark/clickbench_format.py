################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""
Clickbench benchmark for Paimon file formats.

Downloads the Clickbench hits.parquet dataset and compares compression ratios
and read performance across Parquet, ORC, and Vortex formats in Paimon tables.

Usage:
    python benchmarks/clickbench_format.py [--rows N] [--data-path PATH]

    --rows N         Number of rows to use (default: 1_000_000, use 0 for full dataset)
    --data-path PATH Path to existing hits.parquet (skips download if provided)
"""

import argparse
import os
import random
import shutil
import sys
import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pypaimon import CatalogFactory, Schema
from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.utils.range import Range

CLICKBENCH_URL = "https://datasets.clickhouse.com/hits_compatible/hits.parquet"

# Formats to benchmark
FORMATS = ["parquet", "orc", "lance", "vortex"]


def download_clickbench(dest_path: str):
    """Download Clickbench hits.parquet if not already present."""
    if os.path.exists(dest_path):
        print(f"[INFO] Using cached dataset: {dest_path}")
        return
    print(f"[INFO] Downloading Clickbench dataset to {dest_path} ...")
    print(f"[INFO] URL: {CLICKBENCH_URL}")
    print("[INFO] This is ~14GB, may take a while.")

    import urllib.request
    urllib.request.urlretrieve(CLICKBENCH_URL, dest_path)
    print("[INFO] Download complete.")


def load_data(data_path: str, max_rows: int) -> pa.Table:
    """Load Clickbench parquet data, optionally limiting rows."""
    print(f"[INFO] Loading data from {data_path} ...")
    pf = pq.ParquetFile(data_path)
    if max_rows > 0:
        # Read enough row groups to satisfy max_rows
        batches = []
        total = 0
        for rg_idx in range(pf.metadata.num_row_groups):
            rg = pf.read_row_group(rg_idx)
            batches.append(rg)
            total += rg.num_rows
            if total >= max_rows:
                break
        table = pa.concat_tables(batches)
        if table.num_rows > max_rows:
            table = table.slice(0, max_rows)
    else:
        table = pf.read()

    # Cast unsigned integer types to signed (Paimon doesn't support unsigned)
    uint_to_int = {
        pa.uint8(): pa.int16(),
        pa.uint16(): pa.int32(),
        pa.uint32(): pa.int64(),
        pa.uint64(): pa.int64(),
    }
    new_fields = []
    for field in table.schema:
        if field.type in uint_to_int:
            new_fields.append((field.name, uint_to_int[field.type]))
    if new_fields:
        print(f"[INFO] Casting {len(new_fields)} unsigned int columns to signed")
        for name, target_type in new_fields:
            table = table.set_column(
                table.schema.get_field_index(name),
                pa.field(name, target_type, nullable=table.schema.field(name).nullable),
                table.column(name).cast(target_type),
            )

    print(f"[INFO] Loaded {table.num_rows:,} rows, {table.num_columns} columns")
    print(f"[INFO] In-memory size: {table.nbytes / 1024 / 1024:.1f} MB")
    return table


def get_dir_size(path: str) -> int:
    """Get total size of all files under a directory."""
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.isfile(fp):
                total += os.path.getsize(fp)
    return total


def write_paimon_table(catalog, table_name: str, pa_schema: pa.Schema,
                       data: pa.Table, file_format: str) -> dict:
    """Write data to a Paimon table and return metrics."""
    schema = Schema.from_pyarrow_schema(pa_schema, options={
        'file.format': file_format,
        'data-evolution.enabled': 'true',
        'row-tracking.enabled': 'true',
    })
    catalog.create_table(f'default.{table_name}', schema, False)
    table = catalog.get_table(f'default.{table_name}')

    write_builder = table.new_batch_write_builder()
    table_write = write_builder.new_write()
    table_commit = write_builder.new_commit()

    t0 = time.time()
    table_write.write_arrow(data)
    table_commit.commit(table_write.prepare_commit())
    write_time = time.time() - t0

    table_write.close()
    table_commit.close()

    return {
        'write_time': write_time,
    }


def read_paimon_table(catalog, table_name: str) -> dict:
    """Read all data from a Paimon table and return metrics."""
    table = catalog.get_table(f'default.{table_name}')
    read_builder = table.new_read_builder()
    table_scan = read_builder.new_scan()
    table_read = read_builder.new_read()

    t0 = time.time()
    result = table_read.to_arrow(table_scan.plan().splits())
    read_time = time.time() - t0

    return {
        'read_time': read_time,
        'num_rows': result.num_rows,
    }


def point_lookup_paimon_table(catalog, table_name: str, num_lookups: int, rows_per_lookup: int) -> dict:
    """Run row-ID-based point lookups on a data-evolution Paimon table."""
    table = catalog.get_table(f'default.{table_name}')
    read_builder = table.new_read_builder()
    scan = read_builder.new_scan()
    splits = scan.plan().splits()

    # Determine total row range from splits
    total_rows = sum(s.row_count for s in splits)

    # Generate random row ID ranges for lookups
    random.seed(42)
    lookup_ranges = []
    for _ in range(num_lookups):
        start = random.randint(0, total_rows - rows_per_lookup)
        lookup_ranges.append(Range(start, start + rows_per_lookup - 1))

    total_result_rows = 0
    t0 = time.time()
    for rng in lookup_ranges:
        indexed_splits = []
        for s in splits:
            # Intersect the lookup range with each split's file ranges
            file_ranges = []
            for f in s.files:
                if f.first_row_id is not None:
                    file_ranges.append(Range(f.first_row_id, f.first_row_id + f.row_count - 1))
            split_range = Range.and_([rng], file_ranges)
            if split_range:
                indexed_splits.append(IndexedSplit(s, [rng]))

        if indexed_splits:
            read = read_builder.new_read()
            result = read.to_arrow(indexed_splits)
            total_result_rows += result.num_rows

    lookup_time = time.time() - t0

    return {
        'lookup_time': lookup_time,
        'num_lookups': num_lookups,
        'total_rows': total_result_rows,
    }


def predicate_lookup_paimon_table(catalog, table_name: str, num_lookups: int, rows_per_lookup: int) -> dict:
    """Run predicate-based point lookups using _ROW_ID filter on a data-evolution Paimon table."""
    table = catalog.get_table(f'default.{table_name}')

    # Get total rows to generate random ranges
    rb = table.new_read_builder()
    splits = rb.new_scan().plan().splits()
    total_rows = sum(s.row_count for s in splits)

    # Build predicate builder with _ROW_ID in projection
    all_cols = [f.name for f in table.fields] + ['_ROW_ID']
    pb = table.new_read_builder().with_projection(all_cols).new_predicate_builder()

    random.seed(42)
    total_result_rows = 0
    t0 = time.time()
    for _ in range(num_lookups):
        start = random.randint(0, total_rows - rows_per_lookup)
        end = start + rows_per_lookup - 1
        pred = pb.between('_ROW_ID', start, end)
        filtered_rb = table.new_read_builder().with_filter(pred)
        scan = filtered_rb.new_scan()
        read = filtered_rb.new_read()
        result = read.to_arrow(scan.plan().splits())
        total_result_rows += result.num_rows

    lookup_time = time.time() - t0

    return {
        'lookup_time': lookup_time,
        'num_lookups': num_lookups,
        'total_rows': total_result_rows,
    }


def run_benchmark(data: pa.Table, warehouse_dir: str):
    """Run the compression benchmark across all formats."""
    catalog = CatalogFactory.create({'warehouse': warehouse_dir})
    catalog.create_database('default', True)

    pa_schema = data.schema
    in_memory_mb = data.nbytes / 1024 / 1024
    results = {}

    num_lookups = 20
    rows_per_lookup = 100

    for fmt in FORMATS:
        table_name = f"clickbench_{fmt}"
        print(f"\n{'='*60}")
        print(f"  Format: {fmt.upper()}")
        print(f"{'='*60}")

        # Write
        print(f"  Writing {data.num_rows:,} rows ...")
        write_metrics = write_paimon_table(catalog, table_name, pa_schema, data, fmt)
        print(f"  Write time: {write_metrics['write_time']:.2f}s")

        # Measure on-disk size
        table = catalog.get_table(f'default.{table_name}')
        table_path = table.table_path
        disk_size = get_dir_size(table_path)
        disk_mb = disk_size / 1024 / 1024
        ratio = in_memory_mb / disk_mb if disk_mb > 0 else 0
        print(f"  On-disk size: {disk_mb:.1f} MB  (ratio: {ratio:.2f}x)")

        # Full read
        print(f"  Reading back ...")
        read_metrics = read_paimon_table(catalog, table_name)
        print(f"  Read time: {read_metrics['read_time']:.2f}s")
        print(f"  Rows read: {read_metrics['num_rows']:,}")

        # Point lookups by row ID
        print(f"  Point lookups ({num_lookups} queries, {rows_per_lookup} rows each) ...")
        lookup_metrics = point_lookup_paimon_table(catalog, table_name, num_lookups, rows_per_lookup)
        print(f"  Lookup time: {lookup_metrics['lookup_time']:.2f}s")
        print(f"  Total rows matched: {lookup_metrics['total_rows']:,}")

        # Predicate-based point lookups by _ROW_ID
        print(f"  Predicate lookups ({num_lookups} queries, {rows_per_lookup} rows each) ...")
        pred_metrics = predicate_lookup_paimon_table(catalog, table_name, num_lookups, rows_per_lookup)
        print(f"  Predicate lookup time: {pred_metrics['lookup_time']:.2f}s")
        print(f"  Total rows matched: {pred_metrics['total_rows']:,}")

        results[fmt] = {
            'write_time': write_metrics['write_time'],
            'read_time': read_metrics['read_time'],
            'disk_mb': disk_mb,
            'ratio': ratio,
            'lookup_time': lookup_metrics['lookup_time'],
            'lookup_rows': lookup_metrics['total_rows'],
            'pred_lookup_time': pred_metrics['lookup_time'],
            'pred_lookup_rows': pred_metrics['total_rows'],
        }

    return results


def print_summary(results: dict, in_memory_mb: float, num_rows: int):
    """Print a summary comparison table."""
    print(f"\n{'='*80}")
    print(f"  CLICKBENCH COMPRESSION BENCHMARK SUMMARY")
    print(f"  Rows: {num_rows:,}  |  In-memory: {in_memory_mb:.1f} MB")
    print(f"{'='*80}")
    print(f"  {'Format':<10} {'Disk (MB)':>10} {'Ratio':>8} {'Write (s)':>10} {'Read (s)':>10} {'Lookup (s)':>11} {'Pred (s)':>10}")
    print(f"  {'-'*69}")

    for fmt in FORMATS:
        if fmt in results:
            r = results[fmt]
            print(f"  {fmt:<10} {r['disk_mb']:>10.1f} {r['ratio']:>7.2f}x {r['write_time']:>10.2f} {r['read_time']:>10.2f} {r['lookup_time']:>11.2f} {r['pred_lookup_time']:>10.2f}")

    # Vortex vs Parquet comparison
    if 'vortex' in results and 'parquet' in results:
        v = results['vortex']
        p = results['parquet']
        print(f"\n  Vortex vs Parquet:")
        print(f"    Size:   {v['disk_mb'] / p['disk_mb'] * 100:.1f}% of Parquet")
        print(f"    Write:  {v['write_time'] / p['write_time']:.2f}x")
        print(f"    Read:   {v['read_time'] / p['read_time']:.2f}x")
        print(f"    Lookup: {v['lookup_time'] / p['lookup_time']:.2f}x")
        print(f"    Pred:   {v['pred_lookup_time'] / p['pred_lookup_time']:.2f}x")


def main():
    parser = argparse.ArgumentParser(description="Clickbench compression benchmark for Paimon")
    parser.add_argument("--rows", type=int, default=3_000_000,
                        help="Number of rows to use (0 = full dataset, default: 3000000)")
    parser.add_argument("--data-path", type=str, default=None,
                        help="Path to existing hits.parquet (skips download)")
    args = parser.parse_args()

    # Resolve data path
    if args.data_path:
        data_path = args.data_path
    else:
        cache_dir = os.path.join(Path.home(), ".cache", "paimon-bench")
        os.makedirs(cache_dir, exist_ok=True)
        data_path = os.path.join(cache_dir, "hits.parquet")
        download_clickbench(data_path)

    data = load_data(data_path, args.rows)
    in_memory_mb = data.nbytes / 1024 / 1024

    warehouse_dir = tempfile.mkdtemp(prefix="paimon_bench_")
    print(f"[INFO] Warehouse: {warehouse_dir}")

    try:
        results = run_benchmark(data, warehouse_dir)
        print_summary(results, in_memory_mb, data.num_rows)
    finally:
        shutil.rmtree(warehouse_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
