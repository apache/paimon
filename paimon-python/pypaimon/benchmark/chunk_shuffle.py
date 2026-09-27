# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Benchmark chunk-shuffle planning with synthetic file metadata and no data I/O.

Run the same command on the base and candidate revisions, for example:
    python -m pypaimon.benchmark.chunk_shuffle --files 100 --rows-per-file 100000
    python -m pypaimon.benchmark.chunk_shuffle --mode data-evolution --shards 8

Timing runs do not enable tracemalloc. A separate run measures peak Python
allocations during create_splits, including returned splits but excluding the
input metadata and imports. This is not process RSS or data-read throughput.
"""

import argparse
import gc
import hashlib
import json
import platform
import statistics
import time
import tracemalloc
from types import SimpleNamespace

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.read.scanner.chunk_shuffle_split_generator import (
    AppendChunkShuffleSplitGenerator,
    DataEvolutionChunkShuffleSplitGenerator,
)
from pypaimon.table.row.generic_row import GenericRow


def _entries(file_count, rows_per_file):
    partition = GenericRow([], [])
    stats = SimpleStats.empty_stats()
    return [
        ManifestEntry(0, partition, 0, 1, DataFileMeta(
            file_name="data-{:08d}.parquet".format(i),
            file_size=rows_per_file * 8,
            row_count=rows_per_file,
            min_key=partition, max_key=partition,
            key_stats=stats, value_stats=stats,
            min_sequence_number=0, max_sequence_number=0,
            schema_id=0, level=0, extra_files=[],
            first_row_id=i * rows_per_file,
            file_path="/chunk-shuffle-benchmark/data-{:08d}.parquet".format(i),
        ))
        for i in range(file_count)
    ]


def _fingerprint(splits):
    digest = hashlib.sha256()
    for split in splits:
        signature = (
            [file.file_name for file in split.data_split().files],
            [(r.from_, r.to) for r in split.row_ranges()],
        )
        digest.update(json.dumps(signature).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("append", "data-evolution"), default="append")
    parser.add_argument("--files", type=_positive_int, default=100)
    parser.add_argument("--rows-per-file", type=_positive_int, default=100000)
    parser.add_argument("--chunk-size", type=_positive_int, default=100)
    parser.add_argument("--shards", type=_positive_int, default=1)
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--repeats", type=_positive_int, default=3)
    args = parser.parse_args()
    if not 0 <= args.shard_index < args.shards:
        parser.error("--shard-index must be in [0, --shards)")

    table = SimpleNamespace(table_path="/chunk-shuffle-benchmark", options=CoreOptions({}))
    entries = _entries(args.files, args.rows_per_file)
    generator_class = (AppendChunkShuffleSplitGenerator if args.mode == "append"
                       else DataEvolutionChunkShuffleSplitGenerator)
    generator = generator_class(table, 128 * 1024 * 1024, 4 * 1024 * 1024,
                                seed=42, chunk_size=args.chunk_size)
    if args.shards > 1:
        generator.with_shard(args.shard_index, args.shards)

    elapsed = []
    for _ in range(args.repeats):
        gc.collect()
        started = time.perf_counter()
        splits = generator.create_splits(entries)
        elapsed.append(time.perf_counter() - started)
        del splits

    gc.collect()
    tracemalloc.start()
    splits = generator.create_splits(entries)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(json.dumps(dict(
        vars(args), python=platform.python_version(),
        median_seconds=statistics.median(elapsed),
        peak_python_mib=peak / (1024 * 1024),
        split_count=len(splits), fingerprint=_fingerprint(splits),
    ), sort_keys=True))


if __name__ == "__main__":
    main()
