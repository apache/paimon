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

"""DV table writes use the existing Java merge functions before file flush."""

from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon import Schema
from pypaimon.table.bucket_mode import BucketMode
from pypaimon.write.native_write import NativeTableWrite

pytestmark = pytest.mark.native_plan


def _table(catalog, engine, bucket, native, changelog=False, extra=None):
    schema = pa.schema([
        ('p', pa.string()), ('id', pa.int32()), ('value', pa.int32()), ('text', pa.string()),
    ])
    options = {
        'bucket': str(bucket), 'merge-engine': engine, 'deletion-vectors.enabled': 'true',
        'write.native.enabled': str(native).lower(),
        'source.split.target-size': '1b', 'source.split.open-file-cost': '1b',
    }
    if bucket == -2:
        options['postpone.default-bucket-num'] = '3'
    if engine == 'aggregation':
        options.update({'fields.value.aggregate-function': 'sum',
                        'fields.text.aggregate-function': 'last_non_null_value'})
    if changelog:
        options['changelog-producer'] = 'input'
    options.update(extra or {})
    catalog.create_table('default.dv_merge', Schema.from_pyarrow_schema(
        schema, primary_keys=['p', 'id'], partition_keys=['p'], options=options), False)
    return catalog.get_table('default.dv_merge'), schema


def _write(table, schema, rows, native, stream=False, identifier=1, overwrite=None, restore=None):
    if table.bucket_mode() == BucketMode.POSTPONE_MODE:
        builder = table.new_postpone_fixed_bucket_write_builder()
    else:
        builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    if overwrite is not None:
        builder.overwrite(overwrite)
    if restore is not None:
        builder.with_restore_snapshot(restore)
    writer, commit = builder.new_write(), builder.new_commit()
    assert isinstance(writer, NativeTableWrite) == native
    try:
        for group in rows:
            writer.write_arrow(pa.Table.from_pylist(group, schema=schema))
        messages = writer.prepare_commit(identifier) if stream else writer.prepare_commit()
        commit.commit(messages, identifier) if stream else commit.commit(messages)
        return messages
    finally:
        writer.close()
        commit.close()


def _rows(table, native=True, merge_on_read=True, predicate=None, projection=None):
    table = table.copy({
        'scan.native-plan.enabled': str(native).lower(), 'read.native.enabled': str(native).lower(),
        'deletion-vectors.merge-on-read': str(merge_on_read).lower(),
    })
    builder = table.new_read_builder()
    if predicate is not None:
        builder.with_filter(predicate(builder.new_predicate_builder()))
    if projection is not None:
        builder.with_projection(projection)
    read = builder.new_read()
    if native:
        with patch.object(read, '_create_split_read', side_effect=AssertionError('Python read fallback')):
            result = read.to_arrow(builder.new_scan().plan().splits())
    else:
        result = read.to_arrow(builder.new_scan().plan().splits())
    return sorted(result.to_pylist(), key=lambda row: (row.get('p', ''), row.get('id', 0)))


def _first_rows():
    return [
        [dict(p='a', id=1, value=10, text='first'), dict(p='b', id=2, value=20, text='second')],
        [dict(p='a', id=1, value=None, text='patched')],
    ]


@pytest.mark.parametrize('native', [False, True], ids=['python-write', 'native-write'])
@pytest.mark.parametrize('engine', ['partial-update', 'aggregation'])
@pytest.mark.parametrize('bucket,stream', [
    (1, False), (1, True), (-1, False), (-1, True), (-2, False),
], ids=['fixed-batch', 'fixed-stream', 'dynamic-batch', 'dynamic-stream', 'postpone-fixed-batch'])
@pytest.mark.parametrize('changelog', [False, True], ids=['none', 'input'])
def test_dv_merge_engines_fold_and_restore_across_commits(
        native_rest_catalog, native, engine, bucket, stream, changelog):
    table, schema = _table(native_rest_catalog, engine, bucket, native, changelog)
    first = _write(table, schema, _first_rows(), native, stream)
    assert sum(file.row_count for message in first for file in message.new_files) == 2
    assert all(file.level == 0 and file.delete_row_count == 0 for message in first for file in message.new_files)
    assert sum(file.row_count for message in first for file in message.changelog_files) == (3 if changelog else 0)
    expected = [dict(p='a', id=1, value=10, text='patched'), dict(p='b', id=2, value=20, text='second')]
    for native_read in (False, True):
        assert _rows(table, native_read) == expected
        # Java batchScanSkipLevel0 keeps the default materialized view empty.
        assert _rows(table, native_read, merge_on_read=False) == []
    if changelog:
        inputs = []
        for message in first:
            for file in message.changelog_files:
                for row in pq.ParquetFile(file.file_path).read().to_pylist():
                    inputs.append((row['p'], row['id'], row['value'], row['text'], row['_VALUE_KIND']))
        assert sorted(inputs, key=lambda row: (row[0], row[1], row[3])) == [
            ('a', 1, 10, 'first', 0), ('a', 1, None, 'patched', 0), ('b', 2, 20, 'second', 0),
        ]
    second = _write(table, schema, [[
        dict(p='a', id=1, value=3, text=None), dict(p='c', id=3, value=30, text='third'),
    ]], native, stream, 2)
    previous_max = max(file.max_sequence_number for message in first if message.partition == ('a',)
                       for file in message.new_files)
    assert all(file.min_sequence_number > previous_max for message in second if message.partition == ('a',)
               for file in message.new_files)
    expected[0]['value'] = 13 if engine == 'aggregation' else 3
    expected.append(dict(p='c', id=3, value=30, text='third'))
    for native_read in (False, True):
        assert _rows(table, native_read) == expected
        assert _rows(table, native_read, merge_on_read=False) == []
    assert table.snapshot_manager().get_latest_snapshot().id == 2


@pytest.mark.parametrize('engine', ['partial-update', 'aggregation'])
@pytest.mark.parametrize('bucket', [1, -1], ids=['fixed', 'dynamic'])
def test_native_dv_merge_engines_overwrite_and_dynamic_restore(native_rest_catalog, engine, bucket):
    table, schema = _table(native_rest_catalog, engine, bucket, True, changelog=True)
    _write(table, schema, _first_rows(), True)
    _write(table, schema, [[dict(p='a', id=1, value=3, text=None)]], True, restore=1 if bucket == -1 else None)
    assert _rows(table) == [
        dict(p='a', id=1, value=13 if engine == 'aggregation' else 3, text='patched'),
        dict(p='b', id=2, value=20, text='second'),
    ]
    overwritten = _write(table, schema, [[dict(p='a', id=1, value=100, text=None)]], True, overwrite={'p': 'a'})
    # Overwrite resets the partition's merge state and does not publish INPUT logs.
    assert not any(message.changelog_files for message in overwritten)
    assert _rows(table) == [
        dict(p='a', id=1, value=100, text=None), dict(p='b', id=2, value=20, text='second'),
    ]
    _write(table, schema, [[dict(p='a', id=1, value=7, text='after')]], True)
    expected = [
        dict(p='a', id=1, value=107 if engine == 'aggregation' else 7, text='after'),
        dict(p='b', id=2, value=20, text='second'),
    ]
    for native in (False, True):
        assert _rows(table, native) == expected
    assert table.snapshot_manager().get_latest_snapshot().id == 4


@pytest.mark.parametrize('engine', ['partial-update', 'aggregation'])
def test_native_dv_merge_on_read_filters_the_merged_value(native_rest_catalog, engine):
    table, schema = _table(native_rest_catalog, engine, 1, True)
    _write(table, schema, [[dict(p='a', id=1, value=5, text='old')]], True)
    _write(table, schema, [[dict(p='a', id=1, value=6, text=None)]], True)
    for native in (False, True):
        # Each file is below this threshold. Aggregation must evaluate the
        # merged 11 rather than discard both files using value statistics.
        result = _rows(table, native, predicate=lambda builder: builder.greater_than('value', 10),
                       projection=['p', 'id'])
        assert result == ([dict(p='a', id=1)] if engine == 'aggregation' else [])
        assert _rows(table, native, predicate=lambda builder: builder.equal('text', 'old'),
                     projection=['p', 'id']) == [dict(p='a', id=1)]


@pytest.mark.parametrize('engine', ['partial-update', 'aggregation'])
def test_native_dv_merge_engines_preserve_values_across_buffer_flushes(native_rest_catalog, engine):
    table, schema = _table(native_rest_catalog, engine, 1, True, changelog=True,
                           extra={'write.parquet-buffer-size': '1b'})
    messages = _write(table, schema, [
        [dict(p='a', id=1, value=1, text='first')],
        [dict(p='a', id=1, value=2, text=None)],
        [dict(p='a', id=1, value=3, text='last')],
    ], True)
    assert len([file for message in messages for file in message.new_files]) == 3
    assert sum(file.row_count for message in messages for file in message.changelog_files) == 3
    for native in (False, True):
        assert _rows(table, native) == [dict(p='a', id=1, value=6 if engine == 'aggregation' else 3, text='last')]


@pytest.mark.parametrize('engine', ['deduplicate', 'partial-update', 'aggregation'])
def test_native_dv_postpone_fixed_bucket_append_and_rescale(native_rest_catalog, engine):
    table, schema = _table(native_rest_catalog, engine, -2, True, changelog=True)
    first = _write(table, schema, [
        [dict(p='a', id=1, value=10, text='first')],
        [dict(p='a', id=1, value=20, text=None)],
    ], True)
    assert all(message.total_buckets == 3 and 0 <= message.bucket < 3 for message in first)
    assert sum(file.row_count for message in first for file in message.new_files) == 1
    expected_text = None if engine == 'deduplicate' else 'first'
    expected = [dict(p='a', id=1, value=30 if engine == 'aggregation' else 20, text=expected_text)]
    for native in (False, True):
        assert _rows(table, native) == expected
        assert _rows(table, native, merge_on_read=False) == []
    replacement = table.copy({'postpone.default-bucket-num': '5'})
    appended = _write(replacement, schema, [[
        dict(p='a', id=1, value=30, text='after'), dict(p='b', id=2, value=40, text='new'),
    ]], True)
    assert {message.partition: message.total_buckets for message in appended} == {('a',): 3, ('b',): 5}
    assert _rows(replacement) == [
        dict(p='a', id=1, value=60 if engine == 'aggregation' else 30, text='after'),
        dict(p='b', id=2, value=40, text='new'),
    ]
    messages = _write(replacement, schema, [[dict(p='a', id=1, value=100, text=None)]], True, overwrite={'p': 'a'})
    assert all(message.total_buckets == 5 and 0 <= message.bucket < 5 for message in messages)
    assert not any(message.changelog_files for message in messages)
    for native in (False, True):
        assert _rows(replacement, native) == [
            dict(p='a', id=1, value=100, text=None), dict(p='b', id=2, value=40, text='new'),
        ]
