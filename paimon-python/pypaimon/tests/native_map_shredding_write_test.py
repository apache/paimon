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

"""Native MAP shared-shredding writes must preserve Java's physical contract."""

from contextlib import nullcontext
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.data.map_shared_shredding import is_shared_shredding, parse_shared_shredding_metadata
from pypaimon.write.native_write import NativeTableWrite
from pypaimon.table.row.generic_row import GenericRow

pytestmark = pytest.mark.native_plan


def _table(tmp_path, mode='append', policy='lru', extra=None, value_type=None):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([
        ('id', pa.int32()), ('pt', pa.string()),
        ('metrics', pa.map_(pa.string(), value_type or pa.int64())),
    ])
    options = {
        'file.format': 'parquet', 'file.compression': 'zstd',
        'bucket': '1' if mode == 'pk' else '-1', 'write.native.enabled': 'true',
        'fields.metrics.map.storage-layout': 'shared-shredding',
        'fields.metrics.map.shared-shredding.max-columns': '3',
        'fields.metrics.map.shared-shredding.column-placement-policy': policy,
    }
    if mode == 'evolution':
        options.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'})
    options.update(extra or {})
    catalog.create_table('default.maps', Schema.from_pyarrow_schema(
        schema, partition_keys=['pt'], primary_keys=['pt', 'id'] if mode == 'pk' else [],
        options=options), False)
    return catalog.get_table('default.maps'), schema


def _write(table, schema, rows, stream=False):
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    assert isinstance(writer, NativeTableWrite)
    try:
        with patch.object(writer, '_switch_to_python', side_effect=AssertionError('Python writer fallback')):
            for row in rows:
                writer.write_arrow(pa.Table.from_pylist([row], schema=schema))
            messages = writer.prepare_commit(1) if stream else writer.prepare_commit()
            if stream:
                commit.commit(messages, 1)
            else:
                commit.commit(messages)
        return messages
    finally:
        writer.close()
        commit.close()


def _read(table, native, projection=None):
    table = table.copy({'read.native.enabled': str(native).lower(),
                        'scan.native-plan.enabled': str(native).lower()})
    builder = table.new_read_builder()
    if projection:
        builder.with_projection(projection)
    reader = builder.new_read()
    splits = builder.new_scan().plan().splits()
    guard = patch.object(reader, '_create_split_read', side_effect=AssertionError('Python reader fallback'))
    with guard if native else nullcontext():
        return sorted(reader.to_arrow(splits).to_pylist(), key=lambda row: row['id'])


@pytest.mark.parametrize('mode', ['append', 'pk', 'evolution'])
@pytest.mark.parametrize('policy', ['plain', 'sequential', 'lru'])
@pytest.mark.parametrize('stream', [False, True])
def test_native_map_layout_and_cross_read(tmp_path, mode, policy, stream):
    table, schema = _table(tmp_path, mode, policy)
    rows = [
        {'id': 1, 'pt': 'p', 'metrics': [('a', 1), ('b', 2), ('c', 3)]},
        {'id': 2, 'pt': 'p', 'metrics': [('b', 20), ('a', None)]},
        {'id': 3, 'pt': 'p', 'metrics': [('d', 4), ('e', 5), ('f', 6)]},
        {'id': 4, 'pt': 'p', 'metrics': [('a', 7), ('d', 8), ('e', 9), ('f', 10)]},
        {'id': 5, 'pt': 'p', 'metrics': []},
        {'id': 6, 'pt': 'p', 'metrics': None},
    ]
    messages = _write(table, schema, rows, stream)
    files = [file for msg in messages for file in msg.new_files]
    assert len(files) == 1
    physical = pq.ParquetFile(files[0].file_path).read()
    field = physical.schema.field('metrics')
    assert is_shared_shredding(field)
    assert parse_shared_shredding_metadata(field)[1] == 3
    mappings = physical.column('metrics').combine_chunks().field('__field_mapping').to_pylist()
    assert mappings[1] == ([1, 0, -1] if policy == 'plain' else [0, 1, -1])
    assert mappings[2] == ([4, 5, 3] if policy == 'lru' else [3, 4, 5])
    for row in rows:
        if row['metrics'] is not None:
            row['metrics'] = dict(row['metrics'])
    for native in (False, True):
        actual = _read(table, native)
        for row in actual:
            if row['metrics'] is not None:
                row['metrics'] = dict(row['metrics'])
        assert actual == rows
        assert _read(table, native, ['id', "metrics['a']", "metrics['f']"]) == [
            {'id': row['id'], 'metrics_a': (row['metrics'] or {}).get('a'),
             'metrics_f': (row['metrics'] or {}).get('f')} for row in rows]


@pytest.mark.parametrize('mode', ['append', 'evolution'])
def test_native_map_adapts_width_after_completed_files(tmp_path, mode):
    table, schema = _table(tmp_path, mode, extra={'target-file-row-num': '1'})
    rows = [
        {'id': 1, 'pt': 'p', 'metrics': [('a', 1)]},
        {'id': 2, 'pt': 'p', 'metrics': [('b', 2), ('c', 3)]},
        {'id': 3, 'pt': 'p', 'metrics': [('a', 4), ('c', 5), ('d', 6)]},
        {'id': 4, 'pt': 'q', 'metrics': None},
        {'id': 5, 'pt': 'q', 'metrics': []},
    ]
    messages = _write(table, schema, rows)
    widths = {}
    for message in messages:
        for file in message.new_files:
            physical = pq.ParquetFile(file.file_path).read()
            widths[physical.column('id')[0].as_py()] = parse_shared_shredding_metadata(
                physical.schema.field('metrics'))[1]
    # The first file uses max-columns; later files use completed-file widths.
    # Each partition owns an independent context; zero widths clamp to one.
    assert widths == {1: 3, 2: 1, 3: 2, 4: 3, 5: 1}
    assert _read(table, True) == rows


@pytest.mark.parametrize('policy', ['plain', 'sequential', 'lru'])
@pytest.mark.parametrize('codec', ['none', 'lz4', 'zstd'])
def test_native_map_duplicate_keys_take_last_value(tmp_path, policy, codec):
    table, schema = _table(tmp_path, policy=policy, extra={
        'file.compression': codec, 'fields.metrics.map.shared-shredding.max-columns': '1',
    })
    rows = [
        {'id': 1, 'pt': 'p', 'metrics': [('a', 1), ('a', 2), ('b', 3), ('b', None)]},
        {'id': 2, 'pt': 'p', 'metrics': [('b', 4), ('a', 5)]},
    ]
    _write(table, schema, rows)
    for native in (False, True):
        actual = _read(table, native)
        assert [dict(row['metrics']) for row in actual] == [{'a': 2, 'b': None}, {'a': 5, 'b': 4}]
        # Java retains the shared/overflow duplicate, both using the last value.
        assert actual[0]['metrics'] == [('a', 2), ('a', 2), ('b', None)]
        assert _read(table, native, ['id', "metrics['a']", "metrics['b']"]) == [
            {'id': 1, 'metrics_a': 2, 'metrics_b': None},
            {'id': 2, 'metrics_a': 5, 'metrics_b': 4},
        ]


@pytest.mark.parametrize('mode', ['append', 'pk', 'evolution'])
@pytest.mark.parametrize('value_type,value', [
    (pa.list_(pa.int32()), [1, None, 3]),
    (pa.struct([('number', pa.int32()), ('label', pa.string())]), {'number': 4, 'label': '中文'}),
    (pa.map_(pa.string(), pa.int64()), [('nested', 7)]),
])
def test_native_map_nested_values_and_sliced_input(tmp_path, mode, value_type, value):
    table, schema = _table(tmp_path, mode, value_type=value_type)
    rows = [
        {'id': 0, 'pt': 'p', 'metrics': [('discard', value)]},
        {'id': 1, 'pt': 'p', 'metrics': [('a', value)]},
        {'id': 2, 'pt': 'p', 'metrics': None},
        {'id': 3, 'pt': 'p', 'metrics': []},
        {'id': 4, 'pt': 'p', 'metrics': [('b', value), ('a', None)]},
    ]
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    assert isinstance(writer, NativeTableWrite)
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=schema).slice(1, 3))
        writer.write_row(GenericRow([rows[-1][name] for name in schema.names], table.fields))
        assert writer._python_writer is None
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    for native in (False, True):
        actual = _read(table, native)
        for row in actual + rows:
            if row['metrics'] is not None:
                row['metrics'] = dict(row['metrics'])
        assert actual == rows[1:]


@pytest.mark.parametrize('engine', ['deduplicate', 'partial-update', 'aggregation'])
def test_native_pk_map_merge_and_input_changelog(tmp_path, engine):
    extra = {'merge-engine': engine, 'changelog-producer': 'input'}
    if engine == 'aggregation':
        extra['fields.metrics.aggregate-function'] = 'last_non_null_value'
    table, schema = _table(tmp_path, 'pk', extra=extra)
    _write(table, schema, [{'id': 1, 'pt': 'p', 'metrics': [('a', 1)]}])
    messages = _write(table, schema, [
        {'id': 1, 'pt': 'p', 'metrics': [('b', 2)]},
        {'id': 1, 'pt': 'p', 'metrics': [('a', 3), ('c', None)]},
    ])
    for message in messages:
        assert message.changelog_files
        for file in message.new_files + message.changelog_files:
            assert is_shared_shredding(pq.read_schema(file.file_path).field('metrics'))
            # The two physical KV system columns must not shift value statistics.
            assert len(file.value_stats.null_counts) == len(file.value_stats_cols or schema.names)
            assert not any(name.startswith('_') for name in (file.value_stats_cols or []))
    for native in (False, True):
        assert _read(table, native) == [{'id': 1, 'pt': 'p', 'metrics': [('a', 3), ('c', None)]}]


def test_native_map_abort_removes_rolled_files(tmp_path):
    table, schema = _table(tmp_path, extra={'target-file-row-num': '1'})
    writer = table.new_batch_write_builder().new_write()
    assert isinstance(writer, NativeTableWrite)
    writer.write_arrow(pa.Table.from_pylist([{'id': 1, 'pt': 'p', 'metrics': [('a', 1)]}], schema=schema))
    writer.abort()
    assert not list(tmp_path.rglob('*.parquet'))


@pytest.mark.parametrize('native_update', [False, True])
def test_map_row_id_update_keeps_physical_layout(tmp_path, native_update):
    from pypaimon.write.table_update_by_row_id import TableUpdateByRowId

    table, schema = _table(tmp_path, 'evolution')
    _write(table, schema, [
        {'id': 1, 'pt': 'p', 'metrics': [('a', 1)]},
        {'id': 2, 'pt': 'p', 'metrics': [('b', 2)]},
    ])
    row_ids = {row['id']: row['_ROW_ID'] for row in _read(table, True, ['id', '_ROW_ID'])}
    update_schema = pa.schema([('_ROW_ID', pa.int64()), schema.field('metrics')])
    update = pa.Table.from_pylist([
        {'_ROW_ID': row_ids[2], 'metrics': [('new', 7), ('a', None)]},
    ], schema=update_schema)
    table = table.copy({'write.native.enabled': str(native_update).lower()})
    builder = table.new_batch_write_builder()
    guard = patch.object(TableUpdateByRowId, '_load_existing_files_info',
                         side_effect=AssertionError('Python update fallback'))
    with guard if native_update else nullcontext():
        messages = builder.new_update().with_update_type(['metrics']).update_by_arrow_with_row_id(update)
    for message in messages:
        for file in message.new_files:
            assert is_shared_shredding(pq.read_schema(file.file_path).field('metrics'))
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    expected = [
        {'id': 1, 'pt': 'p', 'metrics': [('a', 1)]},
        {'id': 2, 'pt': 'p', 'metrics': [('a', None), ('new', 7)]},
    ]
    for native_read in (False, True):
        assert _read(table, native_read) == expected


@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('operation', ['predicate_scalar', 'predicate_callable', 'upsert'])
def test_native_map_predicate_update_and_upsert(tmp_path, stream, operation):
    from contextlib import ExitStack
    from pypaimon.common.predicate_builder import PredicateBuilder
    from pypaimon.write.table_update import TableUpdate
    from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
    from pypaimon.write.table_upsert_by_key import TableUpsertByKey

    table, schema = _table(tmp_path, 'evolution')
    _write(table, schema, [
        {'id': 1, 'pt': 'p', 'metrics': [('a', 1)]},
        {'id': 2, 'pt': 'p', 'metrics': [('b', 2)]},
    ])
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    update = builder.new_update().with_update_type(['metrics'])
    replacement = [('new', 7), ('a', None)]
    with ExitStack() as stack:
        for cls, method in [(TableUpdateByRowId, '_load_existing_files_info'),
                            (TableUpdate, '_build_predicate_update_table'),
                            (TableUpsertByKey, '_upsert_partition')]:
            stack.enter_context(patch.object(cls, method, side_effect=AssertionError('Python update fallback')))
        if operation == 'upsert':
            data = pa.Table.from_pylist([
                {'id': 2, 'pt': 'p', 'metrics': replacement},
                {'id': 3, 'pt': 'p', 'metrics': []},
            ], schema=schema)
            messages = (update.upsert_by_arrow_with_key(data, ['id'], 2) if stream
                        else update.upsert_by_arrow_with_key(data, ['id']))
        else:
            predicate = PredicateBuilder(table.fields).equal('id', 2)
            assignment = replacement
            kwargs = {}
            if operation == 'predicate_callable':
                def assign_map(matched):
                    assert matched.column_names == ['id', '_ROW_ID']
                    assert matched['id'].to_pylist() == [2]
                    return pa.array([replacement], type=schema.field('metrics').type)
                assignment = assign_map
                kwargs['read_columns'] = ['id']
            messages = (update.update_by_predicate(predicate, {'metrics': assignment}, 2, **kwargs) if stream
                        else update.update_by_predicate(predicate, {'metrics': assignment}, **kwargs))
    for message in messages:
        for file in message.new_files:
            assert is_shared_shredding(pq.read_schema(file.file_path).field('metrics'))
    commit = builder.new_commit()
    try:
        commit.commit(messages, 2) if stream else commit.commit(messages)
    finally:
        commit.close()
    for native in (False, True):
        actual = _read(table, native)
        assert [(row['id'], dict(row['metrics'])) for row in actual] == (
            [(1, {'a': 1}), (2, {'new': 7, 'a': None})]
            + ([(3, {})] if operation == 'upsert' else []))
