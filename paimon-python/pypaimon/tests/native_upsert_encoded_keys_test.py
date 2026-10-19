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

"""Existing upsert keys compare logical values, independently of encoding."""

from contextlib import ExitStack
from pathlib import Path
from urllib.parse import unquote, urlparse
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.write.native_update import (
    _native_upsert_key_compatible, _native_upsert_key_type_compatible, create_native_upsert,
)
from pypaimon.write.table_upsert_by_key import TableUpsertByKey


pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
_ENGINES = [False, pytest.param(True, marks=pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust is not installed'))]
_KINDS = [
    'dense_union', 'sparse_union', 'dictionary_dense_union', 'dictionary_sparse_union',
    pytest.param('dictionary_string_view', marks=pytest.mark.skipif(
        not hasattr(pa, 'string_view'), reason='PyArrow view arrays are unavailable')),
    pytest.param('dictionary_binary_view', marks=pytest.mark.skipif(
        not hasattr(pa, 'binary_view'), reason='PyArrow view arrays are unavailable')),
    pytest.param('dictionary_run_end', marks=pytest.mark.skipif(
        not hasattr(pa, 'RunEndEncodedArray'), reason='PyArrow run-end arrays are unavailable')),
]


def _files(table, pattern='*'):
    root = Path(unquote(urlparse(table.table_path).path))
    assert root.is_dir(), table.table_path
    return set(root.rglob(pattern))


def _union(dense):
    # The first logical value overflows INT and the final child never occurs.
    # Slicing excludes the first value; inactive sparse children still contain
    # overflows and text which must never participate in key conversion.
    tags = pa.array([42, 7, 42, 7, 42], type=pa.int8())
    fields = ['small', 'large', 'unused']
    if dense:
        children = [pa.array([0, 1, 2], type=pa.int32()),
                    pa.array([2 ** 63 - 1, 1, None], type=pa.int64()), pa.array(['unused'])]
        return pa.UnionArray.from_dense(tags, pa.array([0, 1, 1, 2, 2], type=pa.int32()),
                                        children, field_names=fields, type_codes=[7, 42, 99])
    children = [pa.array([0, 1, 0, 2, 0], type=pa.int32()),
                pa.array([2 ** 63 - 1, 2 ** 63 - 1, 1, 2 ** 63 - 1, None], type=pa.int64()),
                pa.array(['unused'] * 5)]
    return pa.UnionArray.from_sparse(tags, children, field_names=fields, type_codes=[7, 42, 99])


def _keys(kind):
    if 'union' in kind:
        values = _union('sparse' not in kind).slice(1, 4)
        if kind.startswith('dictionary'):
            values = pa.DictionaryArray.from_arrays(pa.array([0, None, 1, 2], type=pa.int8()), values)
        else:
            values = values.take(pa.array([0, 3, 1, 2], type=pa.int32()))
        return pa.int32(), values, [1, 2]
    if kind == 'dictionary_run_end':
        values = pa.RunEndEncodedArray.from_arrays(
            pa.array([2, 4, 5, 6], type=pa.int16()), pa.array([999, 1, 2, None], type=pa.int32()))
        values = values.slice(2, 4)
        values = pa.DictionaryArray.from_arrays(pa.array([0, None, 1, 2], type=pa.int8()), values)
        return pa.int32(), values, [1, 2]
    binary = kind == 'dictionary_binary_view'
    first, second = (b'\xff\0', b'longer-than-inline-view') if binary else ('a', 'longer-than-inline-view')
    view = pa.binary_view() if binary else pa.string_view()
    values = pa.array([first, second, None, first], type=view)
    values = pa.DictionaryArray.from_arrays(pa.array([0, None, 3, 1], type=pa.int8()), values)
    return pa.binary() if binary else pa.string(), values, [first, second]


def _table(catalog, key_type, keys, native, guarded):
    schema = pa.schema([('key', key_type), ('p', pa.string()), ('value', pa.int32()), ('keep', pa.string())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, partition_keys=['p'], options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'query-auth.enabled': str(guarded).lower(), 'write.native.enabled': 'false',
        'commit.native.enabled': 'false', 'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
    }), False)
    table = catalog.get_table('default.t')
    rows = [dict(key=keys[0], p='a', value=10, keep='first'),
            dict(key=keys[0], p='a', value=11, keep='duplicate'),
            dict(key=keys[1], p='a', value=20, keep='second'),
            dict(key=None, p='a', value=30, keep='null'),
            dict(key=keys[0], p='b', value=40, keep='other-partition')]
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    return table.copy({'write.native.enabled': str(native).lower()})


def _native_only(stack, native):
    if native:
        stack.enter_context(patch.object(TableUpsertByKey, '_upsert_partition',
                                         side_effect=AssertionError('Python Arrow upsert fallback')))
        stack.enter_context(patch.object(TableUpsertByKey, '_upsert_row_partition',
                                         side_effect=AssertionError('Python Row upsert fallback')))


def _read(table):
    read = table.new_read_builder()
    return read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('keep').to_pylist()


@pytest.mark.parametrize('native', _ENGINES)
@pytest.mark.parametrize('guarded', [False, True])
@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('kind', _KINDS)
def test_encoded_upsert_keys_preserve_last_wins_nulls_and_partitions(
        native_rest_catalog, native, guarded, stream, kind):
    key_type, keys, seed = _keys(kind)
    table = _table(native_rest_catalog, key_type, seed, native, guarded)
    values = pa.array([90, 91, 92, 93], type=pa.int32())
    # Duplicate source keys cross Arrow batch boundaries, while target keys
    # fan out to duplicate existing rows within one partition only.
    source = pa.table({'value': values, 'p': ['a'] * 4, 'key': keys})
    source = pa.concat_tables([source.slice(0, 2), source.slice(2)])
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    with ExitStack() as stack:
        _native_only(stack, native)
        update = builder.new_update().with_update_type(['value'])
        messages = (update.upsert_by_arrow_with_key(source, ['key'], 17) if stream
                    else update.upsert_by_arrow_with_key(source, ['key']))
    commit = builder.new_commit()
    try:
        commit.commit(messages, 17) if stream else commit.commit(messages)
    finally:
        commit.close()
    assert _read(table) == [
        dict(key=seed[0], p='a', value=92, keep='duplicate'),
        dict(key=seed[0], p='a', value=92, keep='first'),
        dict(key=None, p='a', value=91, keep='null'),
        dict(key=seed[0], p='b', value=40, keep='other-partition'),
        dict(key=seed[1], p='a', value=93, keep='second'),
    ]


@pytest.mark.parametrize('kind', _KINDS)
def test_native_key_gate_qualifies_existing_encoded_upsert_layouts(kind):
    key_type, keys, _ = _keys(kind)
    assert _native_upsert_key_type_compatible(keys.type, key_type)
    if kind in ('dictionary_string_view', 'dictionary_binary_view', 'dictionary_run_end'):
        assert not _native_upsert_key_type_compatible(keys.dictionary.type, key_type), (
            'Bare views/run-end arrays have no PyArrow take kernel and remain outside this bridge')


@pytest.mark.parametrize('native', _ENGINES)
@pytest.mark.parametrize('invalid', [2 ** 63 - 1, 1.5, '1'])
def test_active_union_key_cast_failure_never_matches_or_stages(
        native_rest_catalog, native, invalid):
    table = _table(native_rest_catalog, pa.int32(), [1, 2], native, False)
    child = pa.array([invalid])
    keys = pa.UnionArray.from_dense(pa.array([7], type=pa.int8()), pa.array([0], type=pa.int32()),
                                    [child, pa.array([1], type=pa.int32())], type_codes=[7, 42])
    source = pa.table({'key': keys, 'p': ['a'], 'value': pa.array([99], type=pa.int32())})
    before = _files(table, '*.parquet')
    snapshot = table.snapshot_manager().get_latest_snapshot().id
    expected = _read(table)
    with ExitStack() as stack:
        _native_only(stack, native)
        with pytest.raises((ValueError, RuntimeError, pa.ArrowInvalid)):
            table.new_batch_write_builder().new_update().with_update_type(['value']).upsert_by_arrow_with_key(
                source, ['key'])
    assert table.snapshot_manager().get_latest_snapshot().id == snapshot
    assert _files(table, '*.parquet') == before
    assert _read(table) == expected


@pytest.mark.parametrize('native', _ENGINES)
@pytest.mark.parametrize('dense', [False, True])
@pytest.mark.parametrize('dictionary', [False, True])
@pytest.mark.parametrize('child_type', [pa.string(), pa.struct([('unused', pa.string())])])
def test_typed_null_union_children_match_existing_null_keys(
        native_rest_catalog, native, dense, dictionary, child_type):
    table = _table(native_rest_catalog, pa.int32(), [1, 2], native, True)
    tags = pa.array([7, 42], type=pa.int8())
    if dense:
        keys = pa.UnionArray.from_dense(
            tags, pa.array([0, 0], type=pa.int32()),
            [pa.array([1], type=pa.int32()), pa.nulls(1, type=child_type)], type_codes=[7, 42])
    else:
        inactive = 'not an integer' if pa.types.is_string(child_type) else {'unused': 'not an integer'}
        keys = pa.UnionArray.from_sparse(
            tags, [pa.array([1, 0], type=pa.int32()), pa.array([inactive, None], type=child_type)],
            type_codes=[7, 42])
    if dictionary:
        keys = pa.DictionaryArray.from_arrays(pa.array([0, 1], type=pa.int8()), keys)
    source = pa.table({'key': keys, 'p': ['a', 'a'], 'value': pa.array([90, 91], type=pa.int32())})
    source = pa.concat_tables([source.slice(0, 1), source.slice(1)])
    builder = table.new_batch_write_builder()
    with ExitStack() as stack:
        _native_only(stack, native)
        messages = builder.new_update().with_update_type(['value']).upsert_by_arrow_with_key(source, ['key'])
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    assert _read(table) == [
        dict(key=1, p='a', value=90, keep='duplicate'), dict(key=1, p='a', value=90, keep='first'),
        dict(key=None, p='a', value=91, keep='null'), dict(key=1, p='b', value=40, keep='other-partition'),
        dict(key=2, p='a', value=20, keep='second'),
    ]


def _typed_null_keys(kind, value_type):
    if kind == 'plain':
        return pa.nulls(3, type=value_type)
    unused = 'not an integer' if pa.types.is_string(value_type) else {'unused': 'not an integer'}
    values = pa.array([unused, None, unused], type=value_type)
    if kind == 'nested_dictionary':
        values = pa.DictionaryArray.from_arrays(pa.array([0, 1, 2], type=pa.int8()), values)
    return pa.DictionaryArray.from_arrays(pa.array([0, 1, None, 1, 2], type=pa.int8()), values).slice(1, 3)


def test_unrelated_non_null_key_types_still_use_the_existing_python_path():
    for source in [pa.array([None, '1']), pa.array([True]), pa.array([{'unused': 'value'}])]:
        assert not _native_upsert_key_compatible(source, pa.int32())
    source = pa.DictionaryArray.from_arrays(
        pa.array([0, 1], type=pa.int8()), pa.array([None, '1']))
    assert not _native_upsert_key_compatible(source, pa.int32())
    assert not _native_upsert_key_compatible(pa.chunked_array([source.slice(0, 1), source]), pa.int32())
    # Already compatible types do not inspect value or kernel availability.
    with patch('pypaimon.write.native_update.pc.take', side_effect=AssertionError('unnecessary take')):
        assert _native_upsert_key_compatible(pa.array([1], type=pa.int32()), pa.int32())


@pytest.mark.parametrize('kind', ['plain', 'dictionary', 'nested_dictionary'])
@pytest.mark.parametrize('value_type', [pa.string(), pa.struct([('unused', pa.string())])])
def test_all_null_key_columns_select_native_even_with_an_unrelated_physical_type(
        native_rest_catalog, kind, value_type):
    table = _table(native_rest_catalog, pa.int32(), [1, 2], False, True)
    keys = _typed_null_keys(kind, value_type)
    source = pa.table({'key': keys, 'p': ['a'] * 3, 'value': pa.array([90, 91, 92], type=pa.int32())})
    source = pa.concat_tables([source.slice(0, 1), source.slice(1)])
    native_table = MagicMock()
    with patch('pypaimon.write.native_update._native_row_id_table', return_value=native_table):
        selected = create_native_upsert(table, 'null-keys', source, ['key'], ['value'])
    assert selected is not None
    native_table.new_batch_write_builder.assert_called_once()


@pytest.mark.parametrize('native', _ENGINES)
@pytest.mark.parametrize('kind', ['plain', 'dictionary', 'nested_dictionary'])
@pytest.mark.parametrize('value_type', [pa.string(), pa.struct([('unused', pa.string())])])
def test_all_null_key_columns_update_existing_null_rows(
        native_rest_catalog, native, kind, value_type):
    table = _table(native_rest_catalog, pa.int32(), [1, 2], native, True)
    keys = _typed_null_keys(kind, value_type)
    source = pa.table({'key': keys, 'p': ['a'] * 3, 'value': pa.array([90, 91, 92], type=pa.int32())})
    source = pa.concat_tables([source.slice(0, 1), source.slice(1)])
    builder = table.new_batch_write_builder()
    with ExitStack() as stack:
        _native_only(stack, native)
        messages = builder.new_update().with_update_type(['value']).upsert_by_arrow_with_key(source, ['key'])
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    assert _read(table) == [
        dict(key=1, p='a', value=11, keep='duplicate'), dict(key=1, p='a', value=10, keep='first'),
        dict(key=None, p='a', value=92, keep='null'), dict(key=1, p='b', value=40, keep='other-partition'),
        dict(key=2, p='a', value=20, keep='second'),
    ]
