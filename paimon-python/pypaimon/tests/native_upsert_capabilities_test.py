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

"""Native upserts preserve temporal keys and read only input partitions."""

from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.table_upsert_by_key import TableUpsertByKey


pytestmark = pytest.mark.native_plan


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('streaming', [False, True])
@pytest.mark.parametrize('row_input', [False, True])
@pytest.mark.parametrize('key_type', [
    pa.time32('ms'), pa.timestamp('ms'), pa.timestamp('us'), pa.timestamp('ns'),
    pa.timestamp('us', 'UTC'), pa.timestamp('ns', 'UTC'),
])
def test_temporal_upsert_keys(tmp_path, native, streaming, row_input, key_type):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('key', key_type), ('tag', pa.int32()), ('value', pa.string())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'write.native.enabled': str(native).lower(),
    }), False)
    table = catalog.get_table('default.t')
    schema = PyarrowFieldParser.from_paimon_schema(table.fields)
    key_type = schema.field('key').type
    # Adjacent storage-unit values must stay distinct, including nanoseconds.
    keys = pa.array([10, 10, None, 11], type=key_type)
    seed = pa.table({'key': keys, 'tag': [0, 1, 2, 3], 'value': ['old'] * 4}, schema=schema)
    batch_builder = table.new_batch_write_builder()
    writer, commit = batch_builder.new_write(), batch_builder.new_commit()
    try:
        writer.write_arrow(seed)
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    source = pa.table({'key': pa.array([None, 10, 12, 10], type=key_type),
                       'tag': [99, 99, 4, 99],
                       'value': ['null update', 'discarded', 'new', 'final']}, schema=schema)
    builder = table.new_stream_write_builder() if streaming else table.new_batch_write_builder()
    update = builder.new_update().with_update_type(['value'])
    with ExitStack() as stack:
        if native:
            for method in ('_upsert_partition', '_upsert_row_partition'):
                stack.enter_context(patch.object(TableUpsertByKey, method,
                                                 side_effect=AssertionError('Python upsert fallback')))
        if row_input:
            rows = [GenericRow([row[field.name] for field in table.fields], table.fields)
                    for row in source.to_pylist()]
            messages = (update.upsert_by_key(rows, ['key'], 7) if streaming
                        else update.upsert_by_key(rows, ['key']))
        else:
            source = source.select(['value', 'key', 'tag'])
            messages = (update.upsert_by_arrow_with_key(source, ['key'], 7) if streaming
                        else update.upsert_by_arrow_with_key(source, ['key']))
    commit = builder.new_commit()
    try:
        commit.commit(messages, 7) if streaming else commit.commit(messages)
    finally:
        commit.close()
    for native_read in (False, True):
        copy = table.copy({'read.native.enabled': str(native_read).lower(),
                           'scan.native-plan.enabled': str(native_read).lower()})
        reader = copy.new_read_builder()
        result = reader.new_read().to_arrow(reader.new_scan().plan().splits()).sort_by('tag')
        assert result['value'].to_pylist() == ['final', 'final', 'null update', 'old', 'new']
        assert result['tag'].to_pylist() == [0, 1, 2, 3, 4]
        assert result['key'].cast(pa.int32() if pa.types.is_time32(key_type)
                                  else pa.int64()).to_pylist() == [10, 10, None, 11, 12]


@pytest.mark.parametrize('native', [False, True])
def test_upsert_reads_only_exact_input_partitions(tmp_path, native):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('pt', pa.string()), ('sub', pa.int32()),
                        ('value', pa.int32())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, partition_keys=['pt', 'sub'], options={
            'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
            'write.native.enabled': str(native).lower(),
        }), False)
    table = catalog.get_table('default.t')
    data = pa.table({'id': [1] * 5, 'pt': ['a', 'a', 'b', None, None],
                     'sub': [1, 2, 1, 1, 2], 'value': [10] * 5}, schema=schema)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(data)
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()

    def relevant(path):
        return any(part in str(path) for part in (
            '/pt=a/sub=1/', '/pt=b/sub=2/', '/pt=__DEFAULT_PARTITION__/sub=1/'))

    removed = 0
    for path in tmp_path.rglob('*.parquet'):
        if not relevant(path):
            path.unlink()
            removed += 1
    assert removed == 3
    source = pa.table({'id': [1] * 4, 'pt': ['a', 'b', None, 'a'],
                       'sub': [1, 2, 1, 1], 'value': [15, 30, 40, 20]}, schema=schema)
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch.object(TableUpsertByKey, '_upsert_partition',
                                             side_effect=AssertionError('Python upsert fallback')))
        messages = builder.new_update().with_update_type(['value']).upsert_by_arrow_with_key(
            source, ['id'])
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    # Read only the partitions intentionally left accessible. The missing
    # unrelated files prove that matching did not scan their data.
    read = table.new_read_builder()
    splits = [split for split in read.new_scan().plan().splits()
              if relevant(table.path_factory().bucket_path(tuple(split.partition.values), split.bucket))]
    assert sorted(read.new_read().to_arrow(splits)['value'].to_pylist()) == [20, 30, 40]
