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

"""Linux split-read matrix; remote mode simulates 5 ms open RTT.

Run from paimon-python: ``python dev/benchmark_split_read.py``.
Each measurement uses a fresh process so peak RSS is comparable.
``--with-rust`` compares the same table through the direct Rust reader.
"""

import json
import os
import statistics
import subprocess
import sys
import tempfile
import threading
import time

import pyarrow as pa

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from pypaimon import CatalogFactory, Schema


def prepare(warehouse):
    catalog = CatalogFactory.create({'warehouse': warehouse})
    catalog.create_database('default', False)
    for name, payload_size in [('small', 4096), ('large', 4 * 1024 * 1024)]:
        arrow_schema = pa.schema([
            ('id', pa.int64()), ('payload', pa.binary()), ('partition', pa.string())
        ])
        schema = Schema.from_pyarrow_schema(
            arrow_schema, partition_keys=['partition'], options={
                'source.split.target-size': '4mb',
                'read.batch-size': '1',
            })
        catalog.create_table('default.' + name, schema, False)
        table = catalog.get_table('default.' + name)
        payloads = [os.urandom(payload_size) for _ in range(8)]
        data = pa.Table.from_pydict({
            'id': list(range(8)),
            'payload': payloads,
            'partition': [str(index) for index in range(8)],
        }, schema=arrow_schema)
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(data)
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()


def measure(warehouse, name, backend, mode):
    if mode == 'rust':
        from pypaimon_rust.datafusion import PaimonCatalog
        table = PaimonCatalog({'warehouse': warehouse}).get_table(
            'default.' + name)
        read_builder = table.new_read_builder({
            'source.split.target-size': '4mb',
            'read.batch-size': '1',
        })
        splits = read_builder.new_scan().plan().splits()
        read = read_builder.new_read()
    else:
        catalog = CatalogFactory.create({'warehouse': warehouse})
        table = catalog.get_table('default.' + name)
        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        read = read_builder.new_read()

        if backend == 'remote-simulated':
            read._pipeline_reads_are_local = lambda unused: False
            original_create = read._TableRead__create_reader_for_split

            def delayed_create(*args, **kwargs):
                time.sleep(0.005)
                return original_create(*args, **kwargs)

            read._TableRead__create_reader_for_split = delayed_create

    parallelism = {'serial': 1, 'auto': None, 'four': 4, 'rust': None}[mode]

    def rss_mib():
        with open('/proc/self/statm') as statm:
            resident_pages = int(statm.read().split()[1])
        return resident_pages * os.sysconf('SC_PAGE_SIZE') / (1024 * 1024)

    initial_rss = rss_mib()
    peak_rss = [initial_rss]
    stop_sampling = threading.Event()

    def sample_rss():
        while not stop_sampling.wait(0.001):
            peak_rss[0] = max(peak_rss[0], rss_mib())

    sampler = threading.Thread(target=sample_rss, daemon=True)
    sampler.start()
    start = time.perf_counter()
    rows = 0
    first_ms = None
    try:
        reader = (read.read(splits) if mode == 'rust'
                  else read.to_arrow_batch_reader(
                      splits, parallelism=parallelism))
        try:
            for batch in reader:
                if first_ms is None:
                    first_ms = (time.perf_counter() - start) * 1000
                rows += batch.num_rows
                peak_rss[0] = max(peak_rss[0], rss_mib())
        finally:
            close = getattr(reader, 'close', None)
            if close is not None:
                close()
    finally:
        stop_sampling.set()
        sampler.join()
    elapsed = time.perf_counter() - start
    peak_rss[0] = max(peak_rss[0], rss_mib())
    if rows != 8 or len(splits) != 8:
        raise AssertionError((rows, len(splits)))
    return {
        'input': name,
        'backend': backend,
        'mode': mode,
        'workers': (None if mode == 'rust'
                    else read._read_workers(splits, parallelism)),
        'first_ms': round(first_ms, 3),
        'total_ms': round(elapsed * 1000, 3),
        'rows_per_second': round(rows / elapsed, 1),
        'baseline_rss_mib': round(initial_rss, 1),
        'sampled_peak_rss_mib': round(peak_rss[0], 1),
        'read_rss_delta_mib': round(peak_rss[0] - initial_rss, 1),
    }


def main():
    if len(sys.argv) == 6 and sys.argv[1] == '--measure':
        print(json.dumps(measure(*sys.argv[2:])))
        return
    with_rust = '--with-rust' in sys.argv[1:]
    with tempfile.TemporaryDirectory(prefix='paimon-split-bench-') as directory:
        prepare(directory)
        print(json.dumps({
            'python': sys.version.split()[0],
            'pyarrow': pa.__version__,
            'cpu_count': os.cpu_count(),
            'small_bytes_per_split': 4096,
            'large_bytes_per_split': 4 * 1024 * 1024,
            'splits': 8,
            'target_split_size': 4 * 1024 * 1024,
            'rss_sample_period_ms': 1,
        }), flush=True)
        results = {}
        for name in ('small', 'large'):
            for backend in ('local', 'remote-simulated'):
                modes = ('serial', 'auto', 'four')
                if backend == 'local' and with_rust:
                    modes += ('rust',)
                for repeat in range(3):
                    for mode in (modes if repeat % 2 == 0 else modes[::-1]):
                        command = [sys.executable, __file__, '--measure',
                                   directory, name, backend, mode]
                        completed = subprocess.run(
                            command, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            universal_newlines=True)
                        if completed.returncode:
                            raise RuntimeError(completed.stderr)
                        result = json.loads(completed.stdout)
                        results.setdefault((name, backend, mode), []).append(result)
        for (name, backend, mode), measurements in results.items():
            print(json.dumps({
                'input': name,
                'backend': backend,
                'mode': mode,
                'workers': measurements[0]['workers'],
                **{
                    key: round(statistics.median(row[key] for row in measurements), 3)
                    for key in ('first_ms', 'total_ms', 'rows_per_second',
                                'baseline_rss_mib', 'sampled_peak_rss_mib',
                                'read_rss_delta_mib')
                },
            }))


if __name__ == '__main__':
    main()
