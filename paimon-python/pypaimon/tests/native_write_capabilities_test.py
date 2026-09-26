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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Paimon writer capabilities shared by Rust core and the Python bridge."""

from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.table.row.generic_row import GenericRowDeserializer
from pypaimon.write.native_write import NativeTableWrite


pytestmark = pytest.mark.native_plan


def _table(tmp_path, schema, primary_key=False, options=None, partition_keys=None):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    opts = {'file.format': 'parquet', 'write.native.enabled': 'true',
            'bucket': '1' if primary_key else '-1'}
    opts.update(options or {})
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'] if primary_key else [],
        partition_keys=partition_keys or [], options=opts), False)
    return catalog.get_table('default.t')


def _rows(table, native_read, for_write=False, chunk_shuffle=False):
    table = table.copy({'read.native.enabled': str(native_read).lower(),
                        'scan.native-plan.enabled': str(native_read).lower()})
    builder = table.new_read_builder()
    scan = builder.new_scan()
    if chunk_shuffle:
        scan.with_chunk_shuffle(seed=7, chunk_size=1)
    splits = (scan.plan_for_write() if for_write else scan.plan()).splits()
    read = builder.new_read()
    if native_read:
        with patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
            rows = read.to_arrow(splits).to_pylist()
    else:
        rows = read.to_arrow(splits).to_pylist()
    return sorted(rows, key=lambda row: row['id'])


@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('engine,expected', [
    ('first-row', {'id': 1, 'a': 1, 'b': 'first'}),
    ('partial-update', {'id': 1, 'a': 3, 'b': 'second'}),
    ('aggregation', {'id': 1, 'a': 6, 'b': 'second'}),
])
def test_native_merge_engines_within_and_across_commits(tmp_path, stream, engine, expected):
    schema = pa.schema([('id', pa.int64()), ('a', pa.int64()), ('b', pa.string())])
    options = {'merge-engine': engine}
    if engine == 'aggregation':
        options['fields.a.aggregate-function'] = 'sum'
    table = _table(tmp_path, schema, True, options)
    builder = (table.new_stream_write_builder() if stream else table.new_batch_write_builder())
    writer, commit = builder.new_write(), builder.new_commit()
    assert isinstance(writer, NativeTableWrite)
    try:
        for row in [{'id': 1, 'a': 1, 'b': 'first'}, {'id': 1, 'a': 2, 'b': 'second'}]:
            writer.write_arrow(pa.Table.from_pylist([row], schema=schema))
        if stream:
            commit.commit(writer.prepare_commit(1), 1)
        else:
            commit.commit(writer.prepare_commit())
            writer.close()
            commit.close()
            builder = table.new_batch_write_builder()
            writer, commit = builder.new_write(), builder.new_commit()
            assert isinstance(writer, NativeTableWrite)
        writer.write_arrow(pa.Table.from_pylist([{'id': 1, 'a': 3, 'b': None}], schema=schema))
        if stream:
            commit.commit(writer.prepare_commit(2), 2)
        else:
            commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    # Java first-row batch scans hide un-compacted L0 files; write scans expose them.
    for native_read in (False, True):
        assert _rows(table, native_read, engine == 'first-row') == [expected]


@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('data_evolution', [False, True])
def test_native_append_row_rolling_and_checkpoints(tmp_path, stream, data_evolution):
    schema = pa.schema([('id', pa.int64()), ('name', pa.string())])
    table = _table(tmp_path, schema, options={
        'target-file-row-num': '3',
        'row-tracking.enabled': str(data_evolution).lower(),
        'data-evolution.enabled': str(data_evolution).lower(),
    })
    builder = (table.new_stream_write_builder() if stream else table.new_batch_write_builder())
    writer, commit = builder.new_write(), builder.new_commit()
    assert isinstance(writer, NativeTableWrite)
    expected = []
    try:
        for checkpoint in range(2 if stream else 1):
            for ids in ([0, 1], [2, 3], [4]):
                rows = [{'id': i + checkpoint * 5, 'name': 'value'} for i in ids]
                expected.extend(rows)
                writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
            messages = writer.prepare_commit(checkpoint) if stream else writer.prepare_commit()
            counts = sorted(file.row_count for message in messages for file in message.new_files)
            # Java bundled writes may cross the limit by one input batch.
            assert sum(counts) == 5
            assert len(counts) >= 2
            assert max(counts) <= 4
            if stream:
                commit.commit(messages, checkpoint)
            else:
                commit.commit(messages)
    finally:
        writer.close()
        commit.close()
    for native_read in (False, True):
        assert _rows(table, native_read) == expected
    if data_evolution:
        assert table.snapshot_manager().get_latest_snapshot().next_row_id == len(expected)


@pytest.mark.parametrize('primary_key', [False, True])
def test_native_nested_and_fixed_binary_input(tmp_path, primary_key):
    schema = pa.schema([
        ('id', pa.int64()), ('bytes', pa.binary()),
        ('items', pa.list_(pa.struct([('label', pa.string()), ('value', pa.int64())]))),
        ('mapping', pa.map_(pa.string(), pa.list_(pa.int64()))),
    ])
    table = _table(tmp_path, schema, primary_key)
    input_schema = schema.set(1, pa.field('bytes', pa.binary(3)))
    rows = [
        {'id': 0, 'bytes': b'xyz', 'items': [], 'mapping': []},
        {'id': 1, 'bytes': b'abc', 'items': [{'label': 'a', 'value': None}, None],
         'mapping': [('a', [1, None]), ('b', [])]},
        {'id': 2, 'bytes': None, 'items': None, 'mapping': None},
        {'id': 3, 'bytes': b'def', 'items': [], 'mapping': [('a', None)]},
    ]
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    assert isinstance(writer, NativeTableWrite)
    try:
        # A sliced batch checks offsets and null parents in nested Arrow input.
        writer.write_arrow(pa.Table.from_pylist(rows, schema=input_schema).slice(1))
        assert writer._python_writer is None
        # Fixed binary must also work after native data has already been written.
        writer.write_arrow(pa.Table.from_pylist(rows[:1], schema=input_schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    for native_read in (False, True):
        assert _rows(table, native_read) == rows


@pytest.mark.parametrize('mode', ['full', 'truncate(3)', 'counts'])
def test_native_primary_key_value_stats(tmp_path, mode):
    schema = pa.schema([('id', pa.int64()), ('value', pa.string())])
    table = _table(tmp_path, schema, True, {'metadata.stats-mode': mode})
    rows = [{'id': 1, 'value': 'abcdef'}, {'id': 2, 'value': None}]
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    assert isinstance(writer, NativeTableWrite)
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
        messages = writer.prepare_commit()
        files = [file for message in messages for file in message.new_files]
        assert len(files) == 1
        stats = files[0].value_stats
        assert stats.null_counts == [0, 1]
        expected_min, expected_max = {
            'full': ('abcdef', 'abcdef'), 'truncate(3)': ('abc', 'abd'), 'counts': (None, None),
        }[mode]
        assert GenericRowDeserializer.from_bytes(
            stats.min_values.data, table.fields).values[1] == expected_min
        assert GenericRowDeserializer.from_bytes(
            stats.max_values.data, table.fields).values[1] == expected_max
        commit.commit(messages)
    finally:
        writer.close()
        commit.close()
    for native_read in (False, True):
        assert _rows(table, native_read) == rows


@pytest.mark.parametrize('separate_commits', [False, True])
def test_native_partial_update_sequence_group_aggregation(tmp_path, separate_commits):
    schema = pa.schema([(name, pa.int64()) for name in ('id', 'seq', 'total', 'latest', 'free')])
    table = _table(tmp_path, schema, True, {
        'merge-engine': 'partial-update', 'fields.seq.sequence-group': 'total,latest',
        'fields.total.aggregate-function': 'sum',
    })
    rows = [
        {'id': 1, 'seq': 2, 'total': 10, 'latest': 20, 'free': 1},
        {'id': 1, 'seq': 1, 'total': 3, 'latest': 10, 'free': 2},
        {'id': 1, 'seq': 3, 'total': 7, 'latest': 30, 'free': None},
    ]
    groups = [[row] for row in rows] if separate_commits else [rows]
    for group in groups:
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        assert isinstance(writer, NativeTableWrite)
        try:
            for row in group:
                writer.write_arrow(pa.Table.from_pylist([row], schema=schema))
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()
    # Java aggregates every input, while non-aggregate fields follow sequence
    # groups and ungrouped fields keep the last non-null value in arrival order.
    assert _rows(table, True) == [{'id': 1, 'seq': 3, 'total': 20, 'latest': 30, 'free': 2}]


@pytest.mark.parametrize('engine', ['first-row', 'partial-update', 'aggregation'])
def test_native_ignore_delete_filters_retractions(tmp_path, engine):
    schema = pa.schema([('id', pa.int64()), ('value', pa.int64()), ('op', pa.string())])
    options = {'merge-engine': engine, 'ignore-delete': 'true', 'rowkind.field': 'op'}
    if engine == 'aggregation':
        options['fields.value.aggregate-function'] = 'sum'
    table = _table(tmp_path, schema, True, options)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    assert isinstance(writer, NativeTableWrite)
    try:
        writer.write_arrow(pa.Table.from_pylist([
            {'id': 1, 'value': 10, 'op': '+I'}, {'id': 1, 'value': 99, 'op': '-D'},
            {'id': 2, 'value': 20, 'op': '-U'},
        ], schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    assert _rows(table, True, engine == 'first-row') == [{'id': 1, 'value': 10, 'op': '+I'}]


@pytest.mark.parametrize('chunk_shuffle', [False, True])
def test_native_data_evolution_partitioned_append_delete_and_time_travel(tmp_path, chunk_shuffle):
    schema = pa.schema([('id', pa.int64()), ('p', pa.string())])
    table = _table(tmp_path, schema, partition_keys=['p'], options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'deletion-vectors.enabled': 'true', 'index-file-in-data-file-dir': 'true',
        'read.native.enabled': 'true',
    })
    rows = [{'id': 1, 'p': 'a/b'}, {'id': 2, 'p': 'b'}, {'id': 3, 'p': 'a/b'}]
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    assert isinstance(writer, NativeTableWrite)
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    read = table.new_read_builder().with_projection(['id', '_ROW_ID'])
    mapped = read.new_read().to_arrow(read.new_scan().plan().splits()).to_pylist()
    # Row IDs are allocated by file commit order, not logical ID/partition order.
    row_id = next(row['_ROW_ID'] for row in mapped if row['id'] == 1)
    builder = table.new_batch_write_builder()
    messages = builder.new_update().delete_by_row_id([row_id])
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    for native_read in (False, True):
        assert _rows(table, native_read, chunk_shuffle=chunk_shuffle) == rows[1:]
        assert _rows(table.copy({'scan.snapshot-id': '1'}), native_read,
                     chunk_shuffle=chunk_shuffle) == rows
