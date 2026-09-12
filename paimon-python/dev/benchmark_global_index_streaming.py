#!/usr/bin/env python
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

"""Measure global-index ingestion and native builds in fresh processes.

Run from paimon-python with the project dependencies installed::

    PYTHONPATH=. python dev/benchmark_global_index_streaming.py prepare \
        --warehouse /tmp/paimon-stream-bench --rows 65536 --dimension 256
    PYTHONPATH=. python dev/benchmark_global_index_streaming.py run \
        --warehouse /tmp/paimon-stream-bench --mode stream --batch-size 1024

Repeat run in separate processes for baseline, python-only, arrow-only, stream.
Use --native to include IVF-Flat training, serialization and query validation
(requires paimon-vindex==0.4.0). Runs build but do not commit index manifests.
Generated index files are removed after validation. Use a dedicated warehouse.
Do not compare dataset creation RSS to run RSS. Filesystem cache is uncontrolled.
"""

import argparse
from contextlib import ExitStack
import hashlib
import importlib
import json
import os
import platform
import resource
import sys
import time
from unittest.mock import patch

import numpy as np
import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.globalindex.create_global_index import GlobalIndexBuilder
from pypaimon.globalindex.vindex.vindex_vector_index_writer import VindexVectorIndexWriter
from pypaimon.read.table_read import _ClosableArrowBatchReader
from pypaimon.table.special_fields import SpecialFields
from pypaimon.write.commit_message import CommitMessage

build_module = importlib.import_module('pypaimon.globalindex.create_global_index')


def peak_rss_mib():
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss / (1024 * 1024 if sys.platform == 'darwin' else 1024)


def digest_file(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def prepare(args):
    catalog = CatalogFactory.create({'warehouse': args.warehouse})
    catalog.create_database('default', True)
    schema = pa.schema([('embedding', pa.list_(pa.float32()))])
    catalog.create_table('default.vectors', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'global-index.enabled': 'true', 'bucket': '-1', 'file.format': 'parquet',
    }), False)
    table = catalog.get_table('default.vectors')
    wb = table.new_batch_write_builder()
    writer, commit = wb.new_write(), wb.new_commit()
    rng = np.random.RandomState(20260912)
    try:
        for start in range(0, args.rows, 4096):
            count = min(4096, args.rows - start)
            data = rng.standard_normal((count, args.dimension)).astype(np.float32)
            array = pa.ListArray.from_arrays(
                np.arange(count + 1, dtype=np.int32) * args.dimension,
                pa.array(data.reshape(-1)))
            writer.write_arrow(pa.Table.from_arrays([array], schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    with open(os.path.join(args.warehouse, 'benchmark.json'), 'w') as stream:
        json.dump({'rows': args.rows, 'dimension': args.dimension}, stream)


def ablation_build(mode):
    # The baseline preserves the original builder's whole-table lifetime.
    # All variants use the same row extraction, writer, shard plan and finish.
    def build(self, splits, ranges, field, table_read, index_path):
        messages = []
        shard_size = self._core_options.global_index_row_count_per_shard()
        for split, row_range in build_module._split_by_global_index_shard(
                splits, shard_size, ranges):
            writer = None
            try:
                if mode == 'arrow-only':
                    reader, batches = table_read._new_arrow_batch_reader([split])
                    with _ClosableArrowBatchReader(reader, batches) as reader:
                        rows = []
                        for batch in reader:
                            rows.extend(build_module._extract_index_rows(
                                batch, self._index_columns[0],
                                SpecialFields.ROW_ID.name, row_range))
                        del batch
                    chunks = [rows]
                else:
                    table = table_read.to_arrow([split])
                    if table is None or table.num_rows == 0:
                        continue
                    batches = table.to_batches() if mode == 'python-only' else [table]
                    chunks = (build_module._extract_index_rows(
                        batch, self._index_columns[0], SpecialFields.ROW_ID.name,
                        row_range) for batch in batches)
                writer = self._create_generic_index_writer(index_path, field)
                for rows in chunks:
                    for value, row_id in rows:
                        writer.write(value, row_id - row_range.from_)
                del rows, chunks
                adds = build_module._to_index_manifest_entries(
                    self._table, split.partition, row_range, field.id,
                    self._index_type, writer.finish())
            finally:
                if writer is not None:
                    writer.close()
            if adds:
                messages.append(CommitMessage(
                    partition=tuple(split.partition.values), bucket=0,
                    new_files=[], index_adds=adds))
        return messages
    return build


def run(args):
    with open(os.path.join(args.warehouse, 'benchmark.json')) as stream:
        metadata = json.load(stream)
    table = CatalogFactory.create({'warehouse': args.warehouse}).get_table('default.vectors')
    table = table.copy({'read.batch-size': str(args.batch_size)})
    dimension = metadata['dimension']
    result = dict(metadata, mode=args.mode, batch_size=args.batch_size,
                  native=args.native, python=platform.python_version(),
                  pyarrow=pa.__version__, numpy=np.__version__, platform=platform.platform())
    if args.native:
        # Import before timing so native initialization does not skew modes.
        from paimon_vindex import VectorIndexReader, SearchParams
        from pypaimon.globalindex.vindex.vindex_vector_global_index_reader import PaimonVindexInput
    original_finish = VindexVectorIndexWriter.finish
    hash_seconds = [0.0]
    hashes = []

    def finish(writer):
        assert writer._train_sample_ratio == 0.25
        result['train_sample_ratio'] = writer._train_sample_ratio
        writer._close_temp_files()
        result['ingest_seconds'] = time.perf_counter() - started
        result['ingest_peak_rss_mib'] = peak_rss_mib()
        before_hash = time.perf_counter()
        hashes.append({
            'row_ids': digest_file(writer._row_id_temp_path),
            'vectors': digest_file(writer._vector_temp_path),
            'vector_count': writer._vector_count,
        })
        hash_seconds[0] += time.perf_counter() - before_hash
        return original_finish(writer) if args.native else []

    builder = GlobalIndexBuilder(table, 'embedding', 'ivf-flat', options={
        'global-index.row-count-per-shard': str(metadata['rows'] + 1),
        'ivf-flat.dimension': str(dimension), 'ivf-flat.nlist': '16',
        'ivf-flat.train.sample-ratio': '0.25', 'ivf-flat.distance.metric': 'l2',
    })
    result['before_build_peak_rss_mib'] = peak_rss_mib()
    started = time.perf_counter()
    with ExitStack() as stack:
        if args.mode != 'stream':
            stack.enter_context(patch.object(
                GlobalIndexBuilder, '_build_generic_index', ablation_build(args.mode)))
        stack.enter_context(patch.object(VindexVectorIndexWriter, 'finish', finish))
        messages = builder.build()
    result['build_seconds'] = time.perf_counter() - started - hash_seconds[0]
    result['build_peak_rss_mib'] = peak_rss_mib()
    result['input_hashes'] = hashes
    assert len(hashes) == 1, 'Benchmark requires one index shard'
    assert hashes[0]['vector_count'] == metadata['rows']
    entries = [entry for msg in messages for entry in msg.index_adds]
    paths = [table.path_factory().global_index_path_factory().to_path(
        entry.index_file.file_name) for entry in entries]
    try:
        if args.native:
            assert len(paths) == 1
            queries = np.random.RandomState(42).standard_normal((16, dimension)).astype(np.float32)
            scores = {}
            with table.file_io.new_input_stream(paths[0]) as stream:
                reader = VectorIndexReader(PaimonVindexInput(stream))
                try:
                    for nprobe in (4, 16):
                        scores[str(nprobe)] = []
                        for query in queries:
                            ids, distances = reader.search(
                                query, SearchParams.ivf(top_k=10, nprobe=nprobe))
                            scores[str(nprobe)].append([ids.tolist(), distances.tolist()])
                finally:
                    reader.close()
            result['queries'] = scores
            result['query_sha256'] = hashlib.sha256(json.dumps(scores).encode()).hexdigest()
    finally:
        for path in paths:
            table.file_io.delete(path)
    output = json.dumps(result, sort_keys=True)
    if args.output:
        with open(args.output, 'w') as stream:
            stream.write(output + '\n')
    print(output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['prepare', 'run'])
    parser.add_argument('--warehouse', required=True)
    parser.add_argument('--rows', type=int, default=65536)
    parser.add_argument('--dimension', type=int, default=256)
    parser.add_argument('--batch-size', type=int, default=1024)
    parser.add_argument('--mode', choices=['baseline', 'python-only', 'arrow-only', 'stream'],
                        default='stream')
    parser.add_argument('--native', action='store_true')
    parser.add_argument('--output')
    args = parser.parse_args()
    if min(args.rows, args.dimension, args.batch_size) <= 0:
        parser.error('rows, dimension and batch-size must be positive')
    (prepare if args.action == 'prepare' else run)(args)


if __name__ == '__main__':
    main()
