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

"""Benchmark matching DOUBLE shredding and Parquet serialization in memory.

Example: python -m pypaimon.benchmark.variant_shredding --rows 20000 --fields 10
Use --reference label=/path/to/variant_shredding.py to compare revisions.
"""

import argparse
import hashlib
import importlib.util
import json
import platform
import statistics
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq

from pypaimon.data.generic_variant import GenericVariant
from pypaimon.data import variant_shredding


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, default=20000)
    parser.add_argument('--fields', type=int, default=10)
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--warmup', type=int, default=1)
    parser.add_argument('--reference', action='append', default=[], metavar='LABEL=FILE')
    args = parser.parse_args()
    if min(args.rows, args.fields, args.repeats) < 1 or args.warmup < 0:
        parser.error('rows, fields and repeats must be positive; warmup must be non-negative')

    modules = {'current': variant_shredding}
    sources = {}
    for reference in args.reference:
        label, path = reference.split('=', 1)
        if label in modules:
            parser.error('duplicate label: ' + label)
        spec = importlib.util.spec_from_file_location('_benchmark_' + label, path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        modules[label] = module
    for label, module in modules.items():
        with open(module.__file__, 'rb') as source:
            sources[label] = hashlib.sha256(source.read()).hexdigest()

    names = ['f{}'.format(i) for i in range(args.fields)]
    column = GenericVariant.to_arrow_array([
        GenericVariant.from_python({name: float(row + i) + 0.25
                                    for i, name in enumerate(names)})
        for row in range(args.rows)
    ])
    option = json.dumps({'type': 'ROW', 'fields': [{'name': 'payload', 'type': {
        'type': 'ROW', 'fields': [{'name': name, 'type': 'DOUBLE'} for name in names]
    }}]})
    schemas = {label: module.parse_shredding_schema_option(option)['payload']
               for label, module in modules.items()}
    types = {label: module.shredding_schema_to_arrow_type(schemas[label])
             for label, module in modules.items()}
    times = {label: [] for label in modules}
    expected = None
    labels = list(modules)
    for repeat in range(args.warmup + args.repeats):
        # Rotate execution order to reduce order bias.
        order = labels[repeat % len(labels):] + labels[:repeat % len(labels)]
        for label in order:
            start = time.perf_counter()
            shredded = modules[label].shred_variant_column(column, schemas[label], types[label])
            sink = pa.BufferOutputStream()
            pq.write_table(pa.table({'payload': shredded}), sink, compression='zstd')
            output = sink.getvalue()
            elapsed = time.perf_counter() - start
            fingerprint = (output.size, hashlib.sha256(output).hexdigest())
            if expected is None:
                expected = fingerprint
            if fingerprint != expected:
                raise AssertionError('Parquet output differs: ' + label)
            if repeat >= args.warmup:
                times[label].append(elapsed)
    print(json.dumps({
        'python': platform.python_version(), 'pyarrow': pa.__version__,
        'platform': platform.platform(), 'parameters': vars(args),
        'source_sha256': sources, 'parquet_bytes': expected[0],
        'parquet_sha256': expected[1], 'seconds': times,
        'median_seconds': {label: statistics.median(values) for label, values in times.items()},
    }, indent=2))


if __name__ == '__main__':
    main()
