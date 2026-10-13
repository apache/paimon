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

"""Observe BLOB index cold loads through public, disjoint shard reads.

Requires Python 3.7+ for thread CPU timing. Cold means an empty Catalog
index cache, not an empty OS page cache. No delay or barrier is injected
inside a reader. Timing-only runs do not patch any production method.
"""

import argparse
import json
import platform
import random
import sys
import tempfile
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.read.reader import format_blob_reader


class _IndexProbe:
    def __init__(self, file_io, cache):
        self.file_io = file_io
        self.cache = cache
        self.local = threading.local()
        self.lock = threading.Lock()
        self.files = {}

    def add(self, path, **values):
        with self.lock:
            counters = self.files.setdefault(path, Counter())
            counters.update(values)

    def install(self, stack):
        original_index = format_blob_reader.FormatBlobReader._read_index
        original_decode = format_blob_reader._decode_blob_index
        io_type = type(self.file_io)
        original_open = io_type.new_input_stream
        probe = self

        def read_index(reader):
            assert reader._index_cache is probe.cache
            probe.local.path = reader.file_path
            probe.add(reader.file_path, reader_entries=1)
            try:
                return original_index(reader)
            finally:
                probe.local.path = None

        def decode(data):
            path = probe.local.path
            started = time.thread_time()
            result = original_decode(data)
            probe.add(path, decodes=1, decoded_bytes=len(data),
                      decode_cpu_ms=(time.thread_time() - started) * 1000)
            return result

        class Stream:
            def __init__(self, wrapped, path):
                self.wrapped = wrapped
                self.path = path

            def __getattr__(self, name):
                return getattr(self.wrapped, name)

            def read(self, size=-1):
                data = self.wrapped.read(size)
                if getattr(probe.local, 'path', None) == self.path:
                    probe.add(self.path, index_read_calls=1,
                              index_read_bytes=len(data))
                return data

            def __enter__(self):
                self.wrapped.__enter__()
                return self

            def __exit__(self, *args):
                return self.wrapped.__exit__(*args)

        def open_stream(file_io, path):
            stream = original_open(file_io, path)
            if str(path).endswith('.blob'):
                return Stream(stream, str(path))
            return stream

        stack.enter_context(patch.object(
            format_blob_reader.FormatBlobReader, '_read_index', read_index))
        stack.enter_context(patch.object(
            format_blob_reader, '_decode_blob_index', decode))
        stack.enter_context(patch.object(io_type, 'new_input_stream', open_stream))


def _create_table(catalog, rows):
    schema = pa.schema([('id', pa.int64()), ('payload', pa.large_binary())])
    catalog.create_database('default', False)
    catalog.create_table('default.blobs', Schema.from_pyarrow_schema(
        schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
        }), False)
    table = catalog.get_table('default.blobs')
    expected = pa.table({
        'id': list(range(rows)),
        'payload': [('payload-%08d' % i).encode('ascii') for i in range(rows)],
    }, schema=schema)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(expected)
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    return table, expected


def _plans(catalog, workers, cache):
    jobs, details = [], []
    for shard in range(workers):
        table = catalog.get_table('default.blobs')
        assert table.catalog_environment.blob_index_cache() is cache
        assert not table.options.native_read_enabled()
        builder = table.new_read_builder()
        splits = builder.new_scan().with_shard(shard, workers).plan().splits()
        assert len(splits) == 1
        files = [f for f in splits[0].files if f.file_name.endswith('.blob')]
        assert len(files) == 1
        ranges = [(r.from_, r.to) for r in splits[0].row_ranges()]
        details.append({'shard': shard, 'blob_file': files[0].file_name,
                        'row_ranges': ranges})
        jobs.append((builder, splits))
    assert len({detail['blob_file'] for detail in details}) == 1
    return jobs, details


def _read_jobs(jobs, concurrent, parallelism):
    barrier = threading.Barrier(len(jobs), timeout=60) if concurrent else None

    def read(job):
        builder, splits = job
        reader = builder.new_read()
        if barrier is not None:
            barrier.wait()
        return reader.to_arrow(splits, parallelism=parallelism)

    if concurrent:
        with ThreadPoolExecutor(max_workers=len(jobs)) as executor:
            return list(executor.map(read, jobs))
    return [read(job) for job in jobs]


def _measure(name, jobs, concurrent, parallelism, cache, file_io,
             expected, warm, instrumented):
    cache.clear()
    if warm:
        _read_jobs(jobs, False, parallelism)
        assert len(cache) == 1
    probe = _IndexProbe(file_io, cache)
    with ExitStack() as stack:
        if instrumented:
            probe.install(stack)
        cpu_start, wall_start = time.process_time(), time.perf_counter()
        results = _read_jobs(jobs, concurrent, parallelism)
        wall_ms = (time.perf_counter() - wall_start) * 1000
        cpu_ms = (time.process_time() - cpu_start) * 1000
    # Validate every ID and payload, including shard order, outside the timer.
    assert pa.concat_tables(results).equals(expected)
    assert len(cache) == 1
    totals = Counter()
    for counters in probe.files.values():
        totals.update(counters)
    if instrumented:
        assert len(probe.files) == 1
        assert totals['reader_entries'] == len(jobs)
        assert totals['index_read_calls'] == 2 * totals['decodes']
        assert totals['index_read_bytes'] == (
            totals['decoded_bytes'] + 5 * totals['decodes'])
        if warm:
            assert totals['decodes'] == 0
        elif not concurrent:
            assert totals['decodes'] == 1
        else:
            assert 1 <= totals['decodes'] <= len(jobs)
    return {
        'scenario': name, 'warm': warm, 'instrumented': instrumented,
        'wall_ms': wall_ms, 'process_cpu_ms': cpu_ms,
        'cache_bytes_after': cache.size_bytes,
        'files': {Path(path).name: dict(counts)
                  for path, counts in probe.files.items()},
    }


def run(rows, workers, repeats):
    with tempfile.TemporaryDirectory(prefix='blob-cold-load-') as root:
        catalog = CatalogFactory.create({'warehouse': root})
        table, expected = _create_table(catalog, rows)
        cache = table.catalog_environment.blob_index_cache()
        shard_jobs, details = _plans(catalog, workers, cache)
        builder = table.new_read_builder()
        full_splits = builder.new_scan().plan().splits()
        assert len(full_splits) == 1
        full_jobs = [(builder, full_splits)]
        assert _read_jobs(full_jobs, False, workers)[0].equals(expected)
        scenarios = [
            ('single_query', full_jobs, False, workers),
            ('serial_shards', shard_jobs, False, 1),
            ('concurrent_shards', shard_jobs, True, 1),
        ]
        runs = []
        rng = random.Random(10151)
        for instrumented in (True, False):
            for repeat in range(repeats):
                cases = [(scenario, warm) for scenario in scenarios
                         for warm in (False, True)]
                rng.shuffle(cases)
                for (name, jobs, concurrent, parallelism), warm in cases:
                    result = _measure(
                        name, jobs, concurrent, parallelism, cache,
                        table.file_io, expected, warm, instrumented)
                    result['repeat'] = repeat
                    runs.append(result)
                print('instrumented=%s repeat=%d/%d passed' % (
                    instrumented, repeat + 1, repeats), file=sys.stderr)
        return {
            'python': sys.version, 'pyarrow': pa.__version__,
            'platform': platform.platform(), 'rows': rows,
            'workers': workers, 'repeats': repeats,
            'native_read_enabled': table.options.native_read_enabled(),
            'cache_budget_bytes': cache.max_size_bytes,
            'shards': details, 'runs': runs,
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, default=200000)
    parser.add_argument('--workers', type=int, default=8)
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.workers < 2 or args.rows < args.workers or args.repeats < 1:
        parser.error('Require workers >= 2, rows >= workers, repeats >= 1')
    result = run(args.rows, args.workers, args.repeats)
    args.output.write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
    print(str(args.output))


if __name__ == '__main__':
    main()
