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

"""Native MERGE must join dictionary values, never dictionary indices."""

from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.table.data_evolution_merge_into import WhenMatched, WhenNotMatched, lit, source_col


pytestmark = pytest.mark.native_plan
_KEYS = [(pa.int32(), 1, 2, 3), (pa.string(), '中文', 'keep', 'new'),
         (pa.binary(), b'key', b'keep', b'new')]


def _dictionary(values, data_type):
    return pa.array(values, type=data_type).dictionary_encode()


def _table(tmp_path, native, key_type, key, other):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('db', True)
    schema = pa.schema([('key', key_type), ('pt', pa.string()), ('value', pa.int32()), ('keep', pa.int32())])
    catalog.create_table('db.t', Schema.from_pyarrow_schema(schema, partition_keys=['pt'], options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'deletion-vectors.enabled': 'true', 'write.native.enabled': str(native).lower(),
    }), False)
    table = catalog.get_table('db.t')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table({'key': [key, key, other, None], 'pt': ['a'] * 4,
                                    'value': [0] * 4, 'keep': [0, 1, 2, 3]}, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    return table


def _source(key_type, values):
    source = pa.table({'match_key': _dictionary([key for key, _ in values], key_type),
                       'match_pt': _dictionary(['a'] * len(values), pa.string()),
                       'value': pa.array([value for _, value in values], type=pa.int32())})
    # Retain source chunks while using the shared dictionaries required by
    # PyArrow's reference join. Rust's independent dictionaries have core tests.
    return pa.concat_tables([source.slice(index, 1) for index in range(len(values))])


def _merge(table, source, native, stream, matched, inserted=()):
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch('pypaimon.table.data_evolution_merge_into._build_tables',
                                      side_effect=AssertionError('Python MERGE join')))
        update = builder.new_update()
        kwargs = dict(on={'key': 'match_key', 'pt': 'match_pt'}, when_matched=matched, when_not_matched=inserted)
        messages = update.merge_into(source, commit_identifier=53, **kwargs) if stream else update.merge_into(
            source, **kwargs)
    commit = builder.new_commit()
    try:
        commit.commit(messages, 53) if stream else commit.commit(messages)
    finally:
        commit.close()


def _rows(table):
    for native in (False, True):
        view = table.copy({'read.native.enabled': str(native).lower(),
                           'scan.native-plan.enabled': str(native).lower()})
        builder = view.new_read_builder().with_projection(['key', 'pt', 'value', 'keep', '_ROW_ID'])
        yield builder.new_read().to_arrow(builder.new_scan().plan().splits()).to_pylist()


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('key_type,key,other,new', _KEYS)
def test_dictionary_merge_update_insert_and_null_key_semantics(tmp_path, native, stream, key_type, key, other, new):
    table = _table(tmp_path, native, key_type, key, other)
    before = {row['keep']: row['_ROW_ID'] for row in next(_rows(table))}
    source = _source(key_type, [(key, 42), (new, 7), (None, 9)])
    _merge(table, source, native, stream,
           [WhenMatched.update({'value': source_col('value')}, condition='s.value > t.value')],
           [WhenNotMatched({'key': source_col('match_key'), 'pt': source_col('match_pt'),
                            'value': source_col('value'), 'keep': lit(None)})])
    for rows in _rows(table):
        existing = sorted([row for row in rows if row['keep'] is not None], key=lambda row: row['keep'])
        assert [row['value'] for row in existing] == [42, 42, 0, 0]
        assert {row['keep']: row['_ROW_ID'] for row in existing} == before
        inserted = [row for row in rows if row['keep'] is None]
        assert len(inserted) == 2
        assert {(row['key'], row['value'], row['pt']) for row in inserted} == {(new, 7, 'a'), (None, 9, 'a')}


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('key_type,key,other,new', _KEYS)
def test_dictionary_cardinality_uses_values_across_source_chunks(
        tmp_path, native, key_type, key, other, new):
    table = _table(tmp_path, native, key_type, key, other)
    source = _source(key_type, [(key, 1), (new, 2), (key, 3)])
    before = next(_rows(table))
    files = set(tmp_path.rglob('*.parquet'))
    with pytest.raises(ValueError, match='multiple source rows'):
        _merge(table, source, native, False,
               [WhenMatched.update({'value': source_col('value')}, condition='s.value = 99')])
    assert set(tmp_path.rglob('*.parquet')) == files
    for rows in _rows(table):
        assert sorted(rows, key=lambda row: row['keep']) == sorted(before, key=lambda row: row['keep'])


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('key_type,key,other,new', _KEYS)
def test_dictionary_unconditional_delete_accepts_duplicate_source_matches(
        tmp_path, native, key_type, key, other, new):
    table = _table(tmp_path, native, key_type, key, other)
    source = _source(key_type, [(key, 1), (new, 2), (key, 3)])
    _merge(table, source, native, False, [WhenMatched.delete()])
    for rows in _rows(table):
        assert sorted(row['keep'] for row in rows) == [2, 3]


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
def test_pandas_categorical_merge_does_not_require_python_join(tmp_path, native):
    pd = pytest.importorskip('pandas')
    table = _table(tmp_path, native, pa.string(), '中文', 'keep')
    source = pd.DataFrame({'match_key': pd.Categorical(['中文', 'new']),
                           'match_pt': pd.Categorical(['a', 'a']), 'value': [42, 7]})
    _merge(table, source, native, False, [WhenMatched.update({'value': source_col('value')})])
    for rows in _rows(table):
        assert [row['value'] for row in sorted(rows, key=lambda row: row['keep'])] == [42, 42, 0, 0]


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
def test_dictionary_null_values_and_null_indices_do_not_match_in_merge(tmp_path, native, stream):
    table = _table(tmp_path, native, pa.string(), 'key', 'other')
    keys = pa.DictionaryArray.from_arrays(pa.array([0, 1, None], type=pa.int8()), pa.array([None, 'key']))
    source = pa.table({'match_key': keys, 'match_pt': ['a'] * 3,
                       'value': pa.array([9, 42, 11], type=pa.int32())})
    _merge(table, source, native, stream, [WhenMatched.update({'value': source_col('value')})])
    for rows in _rows(table):
        assert [row['value'] for row in sorted(rows, key=lambda row: row['keep'])] == [42, 42, 0, 0]
