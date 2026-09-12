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

"""Compare repeated raw scans with eager and streaming shared scans.

Run from paimon-python with PYTHONPATH=. and project dependencies installed::

    python dev/benchmark_batch_vector_raw_scan.py prepare --warehouse /tmp/raw-scan-bench
    python dev/benchmark_batch_vector_raw_scan.py run --warehouse /tmp/raw-scan-bench \
        --mode shared-stream --queries 8 --output /tmp/shared.json

Run each mode in a fresh process. Preparation is excluded from measurements.
The table has no vector indexes and uses vector-index.search-mode=full. All
variants keep the existing scalar distance calculation and top-k rules.
Filesystem cache is not controlled. Reported row counts measure rows delivered
by Arrow readers, not physical disk reads. Read parallelism is fixed to one.
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
from pypaimon.read.table_read import TableRead
from pypaimon.table.source.vector_search_read import BatchVectorSearchReadImpl
from pypaimon.table.special_fields import SpecialFields

search_module = importlib.import_module('pypaimon.table.source.vector_search_read')


def peak_rss_mib():
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss / (1024 * 1024 if sys.platform == 'darwin' else 1024)


def prepare(args):
    catalog = CatalogFactory.create({'warehouse': args.warehouse})
    catalog.create_database('default', True)
    schema = pa.schema([('embedding', pa.list_(pa.float32()))])
    catalog.create_table('default.vectors', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'global-index.enabled': 'true', 'bucket': '-1', 'file.format': 'parquet',
        'vector-index.search-mode': 'full', 'read.parallelism': '1',
    }), False)
    table = catalog.get_table('default.vectors')
    wb = table.new_batch_write_builder()
    writer, commit = wb.new_write(), wb.new_commit()
    rng = np.random.RandomState(20260912)
    try:
        for start in range(0, args.rows, 4096):
            count = min(4096, args.rows - start)
            values = rng.standard_normal((count, args.dimension)).astype(np.float32)
            vectors = pa.ListArray.from_arrays(
                np.arange(count + 1, dtype=np.int32) * args.dimension,
                pa.array(values.reshape(-1)))
            writer.write_arrow(pa.Table.from_arrays([vectors], schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    with open(os.path.join(args.warehouse, 'benchmark.json'), 'w') as stream:
        json.dump({'rows': args.rows, 'dimension': args.dimension}, stream)


def repeated(self, ranges, pre_filter, index_type=None, snapshot=None):
    return [self._read_raw_search(ranges, pre_filter, query, index_type, snapshot=snapshot)
            for query in self._query_vectors]


def shared_table(self, ranges, pre_filter, index_type=None, snapshot=None):
    heaps = [[] for _ in self._query_vectors]
    ranges = search_module._filtered_raw_row_ranges(ranges, pre_filter)
    if not ranges or not heaps:
        return [search_module._scored_result(heap) for heap in heaps]
    table = self._read_raw_arrow(ranges, True, snapshot)
    if table is None or table.num_rows == 0:
        return [search_module._scored_result(heap) for heap in heaps]
    metric = search_module._raw_search_metric(
        self._table, self._vector_column, self._options, index_type)
    ids = table.column(SpecialFields.ROW_ID.name).to_pylist()
    vectors = table.column(self._vector_column.name).to_pylist()
    for row_id, stored in zip(ids, vectors):
        if stored is None:
            continue
        vector = search_module._to_vector_list(stored)
        for query, heap in zip(self._query_vectors, heaps):
            search_module._check_vector_dimension(query, vector)
            search_module._offer_score(heap, self._limit, row_id,
                                       search_module._compute_score(query, vector, metric))
    return [search_module._scored_result(heap) for heap in heaps]


def run(args):
    with open(os.path.join(args.warehouse, 'benchmark.json')) as stream:
        metadata = json.load(stream)
    table = CatalogFactory.create({'warehouse': args.warehouse}).get_table('default.vectors')
    table = table.copy({'read.batch-size': str(args.batch_size), 'read.parallelism': '1'})
    queries = np.random.RandomState(42).standard_normal(
        (args.queries, metadata['dimension'])).astype(np.float32).tolist()
    builder = table.new_batch_vector_search_builder().with_vector_column(
        'embedding').with_query_vectors(queries).with_limit(args.top_k).with_option('metric', 'l2')
    stats = dict(metadata, mode=args.mode, queries=args.queries, top_k=args.top_k,
                 batch_size=args.batch_size, raw_plans=0, delivered_rows=0,
                 delivered_batches=0, source_passes=0,
                 before_peak_rss_mib=peak_rss_mib(), python=platform.python_version(),
                 pyarrow=pa.__version__, numpy=np.__version__, platform=platform.platform())
    original_plan = BatchVectorSearchReadImpl._plan_raw_read
    original_generator = TableRead._arrow_batch_generator

    def plan(reader, *a, **kw):
        stats['raw_plans'] += 1
        return original_plan(reader, *a, **kw)

    def batches(reader, *a, **kw):
        stats['source_passes'] += 1
        source = original_generator(reader, *a, **kw)
        try:
            for batch in source:
                stats['delivered_rows'] += batch.num_rows
                stats['delivered_batches'] += 1
                yield batch
        finally:
            source.close()

    with ExitStack() as stack:
        stack.enter_context(patch.object(BatchVectorSearchReadImpl, '_plan_raw_read', plan))
        stack.enter_context(patch.object(TableRead, '_arrow_batch_generator', batches))
        if args.mode != 'shared-stream':
            stack.enter_context(patch.object(
                BatchVectorSearchReadImpl, '_read_raw_batch_search',
                repeated if args.mode == 'repeated' else shared_table))
        start = time.perf_counter()
        results = builder.execute_batch_local()
        stats['seconds'] = time.perf_counter() - start
        stats['peak_rss_mib'] = peak_rss_mib()
    expected_passes = args.queries if args.mode == 'repeated' else 1
    assert stats['raw_plans'] == expected_passes, stats
    assert stats['source_passes'] == expected_passes, stats
    assert stats['delivered_rows'] == metadata['rows'] * expected_passes, stats
    scores = []
    for result in results:
        getter = result.score_getter()
        scores.append([[int(row_id), float(getter(row_id))] for row_id in sorted(result.results())])
    assert len(scores) == args.queries
    assert all(len(score) == min(args.top_k, metadata['rows']) for score in scores)
    stats['results'] = scores
    stats['result_sha256'] = hashlib.sha256(json.dumps(scores).encode()).hexdigest()
    output = json.dumps(stats, sort_keys=True)
    if args.output:
        with open(args.output, 'w') as stream:
            stream.write(output + '\n')
    print(output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['prepare', 'run'])
    parser.add_argument('--warehouse', required=True)
    parser.add_argument('--mode', choices=['repeated', 'shared-table', 'shared-stream'],
                        default='shared-stream')
    parser.add_argument('--rows', type=int, default=16384)
    parser.add_argument('--dimension', type=int, default=128)
    parser.add_argument('--queries', type=int, default=8)
    parser.add_argument('--top-k', type=int, default=10)
    parser.add_argument('--batch-size', type=int, default=1024)
    parser.add_argument('--output')
    args = parser.parse_args()
    if min(args.rows, args.dimension, args.queries, args.top_k, args.batch_size) <= 0:
        parser.error('Numeric arguments must be positive')
    (prepare if args.action == 'prepare' else run)(args)


if __name__ == '__main__':
    main()
