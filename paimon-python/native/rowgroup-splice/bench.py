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

"""Isolated FileSystemCatalog benchmark; synthetic data and local writes only."""

import hashlib
from contextlib import nullcontext
import json
import os
import resource
import tempfile
import time

import pyarrow as pa
import pyarrow.parquet as pq

from pypaimon import CatalogFactory, Schema
from pypaimon.filesystem.local_file_io import LocalFileIO


ROWS = int(os.environ.get('ROWS', '30000'))
FIELDS = ['q{}'.format(i) for i in range(10)]


def update(table, arrow, fields):
    builder = table.new_batch_write_builder()
    start = time.perf_counter()
    messages = (builder.new_update().with_update_type(fields)
                .update_by_arrow_with_row_id(arrow))
    prepared = time.perf_counter()
    commit = builder.new_commit()
    commit.commit(messages)
    committed = time.perf_counter()
    commit.close()
    return messages, prepared - start, committed - prepared


def run(catalog, optimized, initial, field):
    options = {
        'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true',
        'file.block-size': '1mb',
        'parquet.write-page-index.enabled': 'true',
        'data-evolution.row-group-reuse.enabled': str(optimized).lower(),
    }
    name = 'default.{}'.format('optimized' if optimized else 'baseline')
    schema = Schema.from_pyarrow_schema(initial.schema, options=options)
    catalog.create_table(name, schema, False)
    table = catalog.get_table(name)
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    writer.write_arrow(initial)
    commit = builder.new_commit()
    commit.commit(writer.prepare_commit())
    writer.close()
    commit.close()
    first, _, _ = update(table, field, FIELDS)
    source = LocalFileIO()._to_file(first[0].new_files[0].file_path)
    metadata = pq.ParquetFile(source).metadata
    last_start = sum(metadata.row_group(i).num_rows
                     for i in range(metadata.num_row_groups - 1))
    ids = list(range(42)) + list(range(last_start, last_start + 42))
    sparse = pa.table({
        '_ROW_ID': pa.array(ids, pa.int64()),
        **{field: pa.array(['changed-{}-{}'.format(field, i) for i in ids])
           for field in FIELDS},
    })
    second, prepare_s, commit_s = update(table, sparse, FIELDS)
    read = table.new_read_builder()
    final = read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id')
    for i in ids:
        assert final['q0'][i].as_py() == 'changed-q0-{}'.format(i)
    assert final.num_rows == ROWS
    return {
        'prepare_s': round(prepare_s, 3), 'commit_s': round(commit_s, 3),
        'source_row_groups': metadata.num_row_groups,
        'source_bytes': os.path.getsize(source),
        'output_bytes': second[0].new_files[0].file_size,
        'rss_kib': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        'arrow_peak_bytes': pa.default_memory_pool().max_memory(),
    }


def main():
    schema = pa.schema([('id', pa.int32())] + [(f, pa.string()) for f in FIELDS])
    data = {'id': pa.array(range(ROWS), pa.int32())}
    for field in FIELDS:
        data[field] = pa.array([
            hashlib.sha256('{}:{}:{}'.format(field, i, 'base').encode()).hexdigest()
            for i in range(ROWS)
        ])
    initial = pa.table(data, schema=schema)
    field = pa.table({
        '_ROW_ID': pa.array(range(ROWS), pa.int64()),
        **{field: pa.array([
            hashlib.sha256('{}:{}:{}'.format(field, i, 'field').encode()).hexdigest()
            for i in range(ROWS)
        ]) for field in FIELDS},
    })
    warehouse = os.environ.get('WAREHOUSE')
    directory = nullcontext(warehouse) if warehouse else tempfile.TemporaryDirectory(prefix='paimon-rowgroup-bench-')
    with directory as tmp:
        catalog = CatalogFactory.create({'warehouse': tmp})
        catalog.create_database('default', True)
        mode = os.environ.get('MODE')
        if mode in ('baseline', 'optimized'):
            result = {mode: run(catalog, mode == 'optimized', initial, field)}
        else:
            result = {'baseline': run(catalog, False, initial, field),
                      'optimized': run(catalog, True, initial, field)}
        print(json.dumps({'rows': ROWS, 'updated': 84, **result}, indent=2))


if __name__ == '__main__':
    main()
