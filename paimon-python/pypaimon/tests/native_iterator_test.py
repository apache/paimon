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

"""The existing InternalRow iterator can consume native table reads."""

import json
from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.table.row.row_kind import RowKind
from pypaimon.tests import native_plan_rest_test

rest_catalog = native_plan_rest_test.rest_catalog

pytestmark = pytest.mark.native_plan


def _write(table, rows, schema):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _table(tmp_path, mode='append'):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('db', True)
    schema = pa.schema([
        ('id', pa.int32()), ('p', pa.string()), ('v', pa.int64()),
        ('profile', pa.struct([('score', pa.int32()), ('label', pa.string())])),
        ('items', pa.list_(pa.int32())), ('attrs', pa.map_(pa.string(), pa.int32())),
    ])
    options = {'bucket': '1' if mode == 'pk' else '-1', 'write.native.enabled': 'false',
               'read.batch-size': '2', 'source.split.target-size': '1b'}
    if mode == 'de':
        options.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
                        'deletion-vectors.enabled': 'true'})
    catalog.create_table('db.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['p', 'id'] if mode == 'pk' else None,
        partition_keys=['p'], options=options), False)
    table = catalog.get_table('db.t')
    rows = [
        {'id': 1, 'p': 'a', 'v': 10, 'profile': {'score': 7, 'label': 'one'},
         'items': [1, None], 'attrs': [('a', 1), ('b', None)]},
        {'id': 2, 'p': 'a', 'v': 20, 'profile': None, 'items': None, 'attrs': None},
        {'id': 3, 'p': 'b', 'v': 30, 'profile': {'score': None, 'label': 'three'},
         'items': [], 'attrs': []},
    ]
    _write(table, rows[:2], schema)
    _write(table, rows[2:], schema)
    if mode == 'pk':
        _write(table, [dict(rows[0], v=11)], schema)
    return table.copy({'read.native.enabled': 'true', 'scan.native-plan.enabled': 'true'}), schema


def _rows(read, splits):
    return [(tuple(row.get_field(i) for i in range(len(row))), row.get_row_kind())
            for row in read.to_iterator(splits)]


@pytest.mark.parametrize('mode', ['append', 'de', 'pk'])
@pytest.mark.parametrize('projection', [None, ['v', 'id'], ['id', 'id'], []])
@pytest.mark.parametrize('limit', [None, 0, 1, 5])
def test_iterator_uses_native_read(tmp_path, mode, projection, limit):
    table, _ = _table(tmp_path, mode)
    builder = table.new_read_builder()
    if projection is not None:
        builder.with_projection(projection)
    if limit is not None:
        builder.with_limit(limit)
    predicate = table.new_read_builder().new_predicate_builder().greater_than('v', 10)
    builder.with_filter(predicate)
    splits = builder.new_scan().plan().splits()
    python = table.copy({'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'}).new_read_builder()
    if projection is not None:
        python.with_projection(projection)
    if limit is not None:
        python.with_limit(limit)
    python.with_filter(predicate)
    expected = _rows(python.new_read(), splits)
    read = builder.new_read()
    with patch.object(read, '_create_split_read', side_effect=AssertionError('Python read fallback')):
        actual = _rows(read, splits)
    assert [row for row, _ in actual] == [row for row, _ in expected]
    assert all(kind == RowKind.INSERT for _, kind in actual)


def test_append_iterator_row_kind_is_insert(tmp_path):
    table, _ = _table(tmp_path)
    table = table.copy({'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    builder = table.new_read_builder().with_projection(['id'])
    rows = _rows(builder.new_read(), builder.new_scan().plan().splits())
    assert rows and all(kind == RowKind.INSERT for _, kind in rows)


class _Batches:
    def __init__(self, values):
        self.values = iter(values)
        self.closed = False

    def __iter__(self):
        return self

    def __next__(self):
        value = next(self.values)
        if isinstance(value, Exception):
            raise value
        return value

    def close(self):
        self.closed = True


def _batch(ids, kinds=None):
    return pa.record_batch([
        pa.array(kinds or ['+I'] * len(ids)), pa.array(ids, type=pa.int32())], names=['rowkind', 'id'])


def _planned_read(tmp_path, limit=None):
    table, _ = _table(tmp_path)
    builder = table.new_read_builder().with_projection(['id'])
    if limit is not None:
        builder.with_limit(limit)
    return builder.new_read(), builder.new_scan().plan().splits()


@pytest.mark.parametrize('consume', [False, True])
def test_iterator_is_lazy_and_close_stops_native_input(tmp_path, consume):
    read, splits = _planned_read(tmp_path)
    batches = _Batches([_batch([1, 2]), RuntimeError('must not fetch next batch')])
    with patch('pypaimon.read.native_plan._prepare_native_read', return_value=lambda _: batches) as prepare, \
            patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
        iterator = read.to_iterator(splits)
        prepare.assert_not_called()
        if consume:
            row = next(iterator)
            assert row.get_field(0) == 1
            assert row.get_row_kind() == RowKind.INSERT
        iterator.close()
        assert batches.closed == consume
        assert prepare.call_count == int(consume)
    assert read.include_row_kind is False


@pytest.mark.parametrize('failure', ['read', 'schema', 'kind'])
def test_iterator_closes_native_input_on_failure_without_replaying_python(tmp_path, failure):
    read, splits = _planned_read(tmp_path)
    bad = (RuntimeError('native read failed') if failure == 'read' else
           pa.record_batch([pa.array(['+I']), pa.array(['wrong type'])], names=['rowkind', 'id'])
           if failure == 'schema' else _batch([2], ['invalid']))
    batches = _Batches([_batch([1]), bad])
    with patch('pypaimon.read.native_plan._prepare_native_read', return_value=lambda _: batches), \
            patch.object(read, '_create_split_read', side_effect=AssertionError('Python replay')):
        iterator = read.to_iterator(splits)
        assert next(iterator).get_field(0) == 1
        with pytest.raises((RuntimeError, TypeError, ValueError)):
            next(iterator)
    assert batches.closed


def test_iterator_spends_limit_before_opening_next_split(tmp_path):
    read, splits = _planned_read(tmp_path, limit=3)
    assert len(splits) >= 2
    prepared = []
    opened = []
    readers = []

    def prepare(table, **kwargs):
        prepared.append(kwargs['limit'])

        def open_split(group):
            assert len(group) == 1
            assert not readers or readers[-1].closed
            opened.append(group[0])
            reader = _Batches([_batch(list(range(min(2, kwargs['limit']))))])
            readers.append(reader)
            return reader
        return open_split

    with patch('pypaimon.read.native_plan._prepare_native_read', side_effect=prepare), \
            patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
        result = _rows(read, splits + splits)
    assert len(result) == 3
    assert prepared == [3, 1]
    assert opened == [splits[0]._native_split, splits[1]._native_split]
    assert all(reader.closed for reader in readers)
    assert read.limit == 3


def test_iterator_setup_failure_falls_back_before_native_rows(tmp_path):
    read, splits = _planned_read(tmp_path)
    expected = _rows(read.table.copy({'read.native.enabled': 'false'}).new_read_builder()
                     .with_projection(['id']).new_read(), splits)
    with patch('pypaimon.read.native_plan._prepare_native_read', side_effect=RuntimeError('setup failed')):
        assert _rows(read, splits) == expected


def test_iterator_qualifies_every_split_before_starting_native(tmp_path):
    read, splits = _planned_read(tmp_path)
    with patch.object(read, '_native_split_files_supported', side_effect=[True, False]), \
            patch('pypaimon.read.native_plan._prepare_native_read') as prepare:
        assert len(_rows(read, splits)) == 3
    prepare.assert_not_called()


@pytest.mark.parametrize('limit', [0, 1, None])
def test_iterator_preserves_all_row_kinds_and_hides_transport_column(tmp_path, limit):
    read, splits = _planned_read(tmp_path, limit)
    kinds = ['+I', '-U', '+U', '-D']
    batches = _Batches([_batch([1, 2, 3, 4], kinds)])
    with patch('pypaimon.read.native_plan._prepare_native_read', return_value=lambda _: batches) as prepare:
        rows = _rows(read, splits[:1])
    count = 4 if limit is None else limit
    assert rows == [((i + 1,), RowKind.from_string(kind)) for i, kind in enumerate(kinds[:count])]
    assert prepare.call_count == int(limit != 0)
    assert batches.closed == (limit != 0)


@pytest.mark.parametrize('mode', ['append', 'pk'])
@pytest.mark.parametrize('projection', [
    ['profile.score', 'id'], ["attrs['a']", 'id'], ['profile', 'profile.score', 'id']])
def test_iterator_nested_projection_and_filter_only_inputs(tmp_path, mode, projection):
    table, _ = _table(tmp_path, mode)
    predicate = table.new_read_builder().new_predicate_builder().greater_than('v', 10)
    builder = table.new_read_builder().with_projection(projection).with_filter(predicate)
    splits = builder.new_scan().plan().splits()
    expected = _rows(table.copy({'read.native.enabled': 'false'}).new_read_builder()
                     .with_projection(projection).with_filter(predicate).new_read(), splits)
    read = builder.new_read()
    with patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
        actual = _rows(read, splits)
    assert actual == expected
    assert actual and all(len(values) == len(projection) for values, _ in actual)


@pytest.mark.parametrize('mode', ['append', 'de', 'pk'])
@pytest.mark.parametrize('projection', [['id', 'id'], ['v', 'id', 'v']])
def test_duplicate_arrow_outputs_align_metadata_by_position(tmp_path, mode, projection):
    table, _ = _table(tmp_path, mode)
    builder = table.new_read_builder().with_projection(projection)
    splits = builder.new_scan().plan().splits()
    read = builder.new_read()
    with patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
        for result in (read.to_arrow(splits), read.to_arrow_batch_reader(splits).read_all()):
            assert result.schema.names == projection
            assert result.column(0).to_pylist() == result.column(1 if len(projection) == 2 else 2).to_pylist()


@pytest.mark.parametrize('mode', ['append', 'de', 'pk'])
def test_iterator_schema_evolution_keeps_current_nested_types(tmp_path, mode):
    from pypaimon.schema.schema_change import SchemaChange
    from pypaimon.schema.data_types import AtomicType

    table, _ = _table(tmp_path, mode)
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.alter_table('db.t', [
        SchemaChange.rename_column(['profile', 'score'], 'points'),
        SchemaChange.update_column_type(['profile', 'points'], AtomicType('BIGINT')),
        SchemaChange.update_column_type(['v'], AtomicType('STRING')),
    ], False)
    table = catalog.get_table('db.t').copy({'read.native.enabled': 'true', 'scan.native-plan.enabled': 'true'})
    builder = table.new_read_builder().with_projection(['profile', 'v', 'id'])
    splits = builder.new_scan().plan().splits()
    read = builder.new_read()
    expected = _rows(table.copy({'read.native.enabled': 'false'}).new_read_builder()
                     .with_projection(['profile', 'v', 'id']).new_read(), splits)
    with patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
        assert _rows(read, splits) == expected


@pytest.mark.parametrize('descriptors', [False, True])
@pytest.mark.parametrize('inline', [False, True])
@pytest.mark.parametrize('duplicate', [False, True])
def test_iterator_blob_accessors_survive_projection_and_close(tmp_path, descriptors, inline, duplicate):
    from pypaimon.tests.native_blob_write_test import _table as blob_table, _data

    options = {'read.native.enabled': 'true', 'scan.native-plan.enabled': 'true',
               'blob-as-descriptor': str(descriptors).lower()}
    if inline:
        options['blob-inline-field'] = 'large'
    table = blob_table(tmp_path, options)
    data = _data()
    _write(table, data.to_pylist(), data.schema)
    projection = ['small', 'large', 'id'] + (['large'] if duplicate else [])
    builder = table.new_read_builder().with_projection(projection)
    read, splits = builder.new_read(), builder.new_scan().plan().splits()
    result = []
    with patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
        iterator = read.to_iterator(splits)
        for row in iterator:
            result.append((row.get_field(2), [row.get_blob(index) for index in [0, 1] + ([3] if duplicate else [])]))
        iterator.close()
    result.sort(key=lambda pair: pair[0])
    payloads = [[None if blob is None else blob.to_data() for blob in blobs] for _, blobs in result]
    assert payloads == [[b'', b'\x01' * 40] + ([b'\x01' * 40] if duplicate else []),
                        [b'abc', None] + ([None] if duplicate else [])]


@pytest.mark.parametrize('descriptors', [False, True])
def test_iterator_pk_blob_respects_payload_mode_and_limit(tmp_path, descriptors):
    from pypaimon.tests.native_pk_blob_limit_test import _table as blob_table, _descriptor, _write as write

    table = blob_table(tmp_path).copy({'blob-as-descriptor': str(descriptors).lower()})
    write(table, [
        {'id': 1, 'tag': 1, 'payload': _descriptor(tmp_path, 'valid', b'selected'),
         'output': _descriptor(tmp_path, 'output', b'returned')},
        {'id': 2, 'tag': 2, 'payload': _descriptor(tmp_path, 'missing'),
         'output': _descriptor(tmp_path, 'also-missing')},
    ])
    builder = table.new_read_builder().with_projection(['id', 'output']).with_limit(1)
    read, splits = builder.new_read(), builder.new_scan().plan().splits()
    with patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
        iterator = read.to_iterator(splits)
        row = next(iterator)
        assert row.get_field(0) == 1
        assert row.get_blob(1).to_data() == b'returned'
        with pytest.raises(StopIteration):
            next(iterator)


@pytest.mark.parametrize('native_write', [False, True])
@pytest.mark.parametrize('projection', [['value', 'key'], []])
def test_iterator_reads_actual_incremental_changelog_kinds(tmp_path, native_write, projection):
    from pypaimon.tests.native_changelog_batch_test import _table as changelog_table, _write as write

    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    table = changelog_table(catalog, {'bucket': '1', 'write.native.enabled': str(native_write).lower()})
    for index, kind in enumerate([RowKind.INSERT, RowKind.UPDATE_AFTER, RowKind.UPDATE_BEFORE, RowKind.DELETE]):
        write(table, (index + 1) * 100, [{'key': 1, 'value': index}], kind=kind)
    table = table.copy({'scan.mode': 'incremental', 'incremental-between-timestamp': '0,400',
                        'read.native.enabled': 'true', 'scan.native-plan.enabled': 'true'})
    builder = table.new_read_builder().with_projection(projection)
    read, splits = builder.new_read(), builder.new_scan().plan().splits()
    with patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
        rows = _rows(read, splits)
    assert sorted((values, kind.value) for values, kind in rows) == sorted([
        ((index, 1) if projection else (), kind.value)
        for index, kind in enumerate([RowKind.INSERT, RowKind.UPDATE_AFTER, RowKind.UPDATE_BEFORE, RowKind.DELETE])])


@pytest.mark.parametrize('mode', ['append', 'de', 'pk'])
@pytest.mark.parametrize('projection', [None, ['_row_kind'], ['_row_kind', 'id', '_row_kind']])
def test_iterator_user_row_kind_column_does_not_collide_with_transport(tmp_path, mode, projection):
    table, _ = _table(tmp_path, mode)
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    from pypaimon.schema.schema_change import SchemaChange
    from pypaimon.schema.data_types import AtomicType, PyarrowFieldParser

    catalog.alter_table('db.t', [SchemaChange.add_column('_row_kind', AtomicType('STRING'))], False)
    table = catalog.get_table('db.t').copy({'read.native.enabled': 'true', 'scan.native-plan.enabled': 'true'})
    schema = PyarrowFieldParser.from_paimon_schema(table.fields)
    _write(table, [{'id': 4, 'p': 'c', 'v': 40, '_row_kind': 'user value'}], schema)
    builder = table.new_read_builder()
    python = table.copy({'read.native.enabled': 'false'}).new_read_builder()
    if projection is not None:
        builder.with_projection(projection)
        python.with_projection(projection)
    read, splits = builder.new_read(), builder.new_scan().plan().splits()
    expected = _rows(python.new_read(), splits)
    with patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
        assert _rows(read, splits) == expected


@pytest.mark.parametrize('duplicate', [False, True])
def test_iterator_reads_parquet_vector_sidecars_and_keeps_typed_accessors(tmp_path, duplicate):
    from pypaimon.tests.native_parquet_vector_update_test import _table as vector_table

    table, _ = vector_table(tmp_path, native=True)
    table = table.copy({'read.native.enabled': 'true', 'scan.native-plan.enabled': 'true'})
    projection = ['embedding', 'id', 'payload'] + (['embedding'] if duplicate else [])
    builder = table.new_read_builder().with_projection(projection)
    read, splits = builder.new_read(), builder.new_scan().plan().splits()
    result = []
    with patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
        for row in read.to_iterator(splits):
            id_ = row.get_field(1)
            result.append(id_)
            assert row.get_vector(0).to_list() == [float(id_), float(id_ + 1)]
            assert row.get_blob(2).to_data() == ('blob-%d' % id_).encode()
            if duplicate:
                assert row.get_vector(3) == row.get_vector(0)
    assert sorted(result) == [1, 2, 3, 4]


@pytest.mark.parametrize('mode', ['native', 'setup-fallback', 'wire-fallback'])
def test_iterator_keeps_plan_authorization_across_splits_and_fallback(rest_catalog, mode):
    from pypaimon.api.api_response import AuthTableQueryResponse
    from pypaimon.common.predicate_builder import PredicateBuilder
    from pypaimon.read.split_serializer import deserialize_split_v1, serialize_split_v1
    from pypaimon.schema.schema_change import SchemaChange

    catalog, server = rest_catalog
    schema = pa.schema([('id', pa.int32()), ('p', pa.string()), ('secret', pa.string())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, partition_keys=['p'], options={
        'write.native.enabled': 'false', 'source.split.target-size': '1b'}), False)
    table = catalog.get_table('default.t')
    _write(table, [{'id': i, 'p': str(i), 'secret': 'hidden'} for i in [1, 2, 3]], schema)
    catalog.alter_table(table.identifier, [SchemaChange.set_option('query-auth.enabled', 'true')])
    table = catalog.get_table('default.t').copy({'read.native.enabled': 'true', 'scan.native-plan.enabled': 'true'})
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        filter=[json.dumps({'kind': 'LEAF', 'transform': {'name': 'FIELD_REF', 'fieldRef': {
            'name': 'id', 'type': 'INT', 'index': 0}}, 'function': 'GREATER_THAN', 'literals': [1]})],
        column_masking={'secret': json.dumps({'name': 'CONCAT', 'inputs': ['visible']})})
    builder = (table.new_read_builder().with_projection(['id']).with_limit(2)
               .with_filter(PredicateBuilder(table.fields).equal('secret', 'visible')))
    splits = builder.new_scan().plan().splits()
    assert len(splits) == 3
    # A later grant must not replace the policy captured by the plan, including
    # its hidden masking dependency and the quota after authorization filtering.
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse()
    read = builder.new_read()
    if mode == 'native':
        context = patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback'))
    else:
        failure = RuntimeError('setup failed')
        if mode == 'wire-fallback':
            splits = [deserialize_split_v1(serialize_split_v1(split), []) for split in splits]
            failure = AssertionError('Serialized rules must not mint a native grant')
        context = patch('pypaimon.read.native_plan._prepare_native_read', side_effect=failure)
    with context:
        assert sorted(_rows(read, splits), key=lambda row: row[0]) == [
            ((2,), RowKind.INSERT), ((3,), RowKind.INSERT)]


@pytest.mark.parametrize('operation', ['update', 'delete'])
def test_iterator_data_evolution_applies_partial_updates_and_deletion_vectors(tmp_path, operation):
    table, _ = _table(tmp_path, 'de')
    builder = table.new_batch_write_builder()
    update = builder.new_update()
    predicate = table.new_read_builder().new_predicate_builder().equal('id', 2)
    messages = (update.update_by_predicate(predicate, {'v': 99}) if operation == 'update'
                else update.delete_by_predicate(predicate))
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    projection = ['id', 'v', '_ROW_ID']
    builder = table.new_read_builder().with_projection(projection)
    read, splits = builder.new_read(), builder.new_scan().plan().splits()
    expected = _rows(table.copy({'read.native.enabled': 'false'}).new_read_builder()
                     .with_projection(projection).new_read(), splits)
    with patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
        actual = _rows(read, splits)
    assert actual == expected
    assert {row[0]: row[1] for row, _ in actual} == (
        {1: 10, 2: 99, 3: 30} if operation == 'update' else {1: 10, 3: 30})


@pytest.mark.parametrize('descriptors', [False, True])
@pytest.mark.parametrize('duplicate', [False, True])
def test_iterator_map_blob_leaf_keeps_blob_accessor(tmp_path, descriptors, duplicate):
    from pypaimon.tests.native_blob_collection_write_test import _table as collection_table, _data

    table = collection_table(tmp_path, native=False)
    data, _ = _data(table, tmp_path, False, 1)
    _write(table, data.to_pylist(), data.schema)
    projection = ["attrs['first']", 'id'] + (["attrs['first']"] if duplicate else [])
    results = []
    for native in [False, True]:
        view = table.copy({'read.native.enabled': str(native).lower(), 'scan.native-plan.enabled': 'true',
                           'blob-as-descriptor': str(descriptors).lower()})
        builder = view.new_read_builder().with_projection(projection)
        read, splits = builder.new_read(), builder.new_scan().plan().splits()
        values = []
        with ExitStack() as stack:
            if native:
                stack.enter_context(patch.object(
                    read, '_create_split_read', side_effect=AssertionError('Python fallback')))
            for row in read.to_iterator(splits):
                assert len(row) == len(projection)
                blob = row.get_blob(0)
                payload = None if blob is None else blob.to_data()
                values.append((row.get_field(1), payload))
                if duplicate:
                    other = row.get_blob(2)
                    assert (None if other is None else other.to_data()) == payload
        results.append(sorted(values))
    assert results == [[(1, b'payload' * 10), (2, None), (3, None)]] * 2
