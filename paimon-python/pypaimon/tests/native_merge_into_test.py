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

"""MERGE must match, select actions and stage files in native core."""

from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.table.data_evolution_merge_into import WhenMatched, WhenNotMatched, source_col, lit

pytestmark = pytest.mark.native_plan
_SCHEMA = pa.schema([('id', pa.int32()), ('value', pa.int32()), ('name', pa.string())])


def _table(tmp_path, rows=None):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('db', True)
    catalog.create_table('db.t', Schema.from_pyarrow_schema(_SCHEMA, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'write.native.enabled': 'true', 'deletion-vectors.enabled': 'true',
    }), False)
    table = catalog.get_table('db.t')
    if rows:
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.Table.from_pylist(rows, schema=_SCHEMA))
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()
    return table


def _rows(table):
    reader = table.new_read_builder()
    return sorted(reader.new_read().to_arrow(reader.new_scan().plan().splits()).to_pylist(),
                  key=lambda row: row['id'] if row['id'] is not None else -1)


def test_native_merge_updates_and_inserts_without_python_join(tmp_path):
    table = _table(tmp_path, [dict(id=1, value=10, name='old'), dict(id=2, value=20, name='keep')])
    source = pa.Table.from_pylist([dict(id=1, value=11, name='new'), dict(id=3, value=30, name='insert')],
                                  schema=_SCHEMA)
    builder = table.new_batch_write_builder()
    with patch('pypaimon.table.data_evolution_merge_into._build_tables',
               side_effect=AssertionError('Python merge orchestration')):
        messages = builder.new_update().merge_into(source, on=['id'],
                                                   when_matched=[WhenMatched.update({'value': source_col('value')})],
                                                   when_not_matched=[WhenNotMatched('*')])
    builder.new_commit().commit(messages)
    assert _rows(table) == [dict(id=1, value=11, name='old'), dict(id=2, value=20, name='keep'),
                            dict(id=3, value=30, name='insert')]


@pytest.mark.parametrize('native', [False, True])
def test_duplicate_source_is_rejected_before_action_conditions(tmp_path, native):
    table = _table(tmp_path, [dict(id=1, value=10, name='old')]).copy({'write.native.enabled': str(native).lower()})
    source = pa.Table.from_pylist([dict(id=1, value=11, name='a'), dict(id=1, value=12, name='b')], schema=_SCHEMA)
    files = set(tmp_path.rglob('*.parquet'))
    with pytest.raises(ValueError, match='multiple source rows'):
        table.new_batch_write_builder().new_update().merge_into(
            source, on=['id'], when_matched=[
                WhenMatched.update({'value': source_col('value')}, condition='s.value = 11')])
    assert set(tmp_path.rglob('*.parquet')) == files
    assert _rows(table) == [dict(id=1, value=10, name='old')]


@pytest.mark.parametrize('native', [False, True])
def test_unconditional_delete_allows_multiple_source_matches(tmp_path, native):
    table = _table(tmp_path, [dict(id=1, value=10, name='remove'), dict(id=2, value=20, name='keep')])
    table = table.copy({'write.native.enabled': str(native).lower()})
    source = pa.Table.from_pylist([dict(id=1, value=11, name='a'), dict(id=1, value=12, name='b')], schema=_SCHEMA)
    builder = table.new_batch_write_builder()
    messages = builder.new_update().merge_into(source, on=['id'], when_matched=[WhenMatched.delete()])
    builder.new_commit().commit(messages)
    assert _rows(table) == [dict(id=2, value=20, name='keep')]


@pytest.mark.parametrize('stream', [False, True])
def test_ordered_conditions_null_keys_and_three_action_types(tmp_path, stream):
    table = _table(tmp_path, [
        dict(id=1, value=10, name='remove'), dict(id=2, value=20, name='keep'),
        dict(id=None, value=90, name='null-old')])
    source = pa.Table.from_pylist([dict(id=1, value=-1, name='remove'), dict(id=2, value=None, name='new'),
                                  dict(id=3, value=30, name='insert'), dict(id=None, value=91, name='null-new')],
                                  schema=_SCHEMA)
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    with patch('pypaimon.table.data_evolution_merge_into._build_tables',
               side_effect=AssertionError('Python merge orchestration')):
        update = builder.new_update()
        kwargs = {'commit_identifier': 27} if stream else {}
        messages = update.merge_into(source, on=['id'], when_matched=[
            WhenMatched.delete('s.value < 0'),
            WhenMatched.update({'value': source_col('value')}, condition='s.value > t.value'),
            WhenMatched.update({'value': lit(73)}),
        ], when_not_matched=[WhenNotMatched('*')], **kwargs)
    commit = builder.new_commit()
    commit.commit(messages, 27) if stream else commit.commit(messages)
    assert sorted(_rows(table), key=lambda row: row['name']) == [
        dict(id=3, value=30, name='insert'), dict(id=2, value=73, name='keep'),
        dict(id=None, value=91, name='null-new'), dict(id=None, value=90, name='null-old')]


def test_native_self_merge_and_empty_target_insert(tmp_path):
    table = _table(tmp_path, [dict(id=1, value=10, name='old'), dict(id=2, value=20, name='keep')])
    builder = table.new_batch_write_builder()
    with patch('pypaimon.table.data_evolution_merge_into._build_tables',
               side_effect=AssertionError('Python self merge orchestration')):
        messages = builder.new_update().merge_into(table, on=['_ROW_ID'], when_matched=[
            WhenMatched.update({'value': lit(99)}, condition="s.id = 1 AND t.name = 'old'")])
    builder.new_commit().commit(messages)
    assert _rows(table) == [dict(id=1, value=99, name='old'), dict(id=2, value=20, name='keep')]
    empty = _table(tmp_path / 'empty')
    source = pa.Table.from_pylist([dict(id=3, value=30, name='new')], schema=_SCHEMA)
    builder = empty.new_batch_write_builder()
    with patch('pypaimon.table.data_evolution_merge_into._build_tables',
               side_effect=AssertionError('Python merge orchestration')):
        messages = builder.new_update().merge_into(source, on=['id'], when_not_matched=[
            WhenNotMatched({'value': source_col('value'), 'name': lit('literal s.id')}, condition='s.value > 10')])
    builder.new_commit().commit(messages)
    assert _rows(empty) == [dict(id=3, value=30, name='literal s.id')]


@pytest.mark.parametrize('condition', [
    's.value > (SELECT 10)',
    's.value IN (SELECT v FROM (VALUES (11), (30)) AS m(v))',
    's.value > (SELECT MAX(v) FROM (VALUES (0), (10)) AS m(v))',
])
def test_native_merge_preserves_sql_subquery_conditions(tmp_path, condition):
    table = _table(tmp_path, [dict(id=1, value=10, name='a'), dict(id=2, value=20, name='b')])
    source = pa.Table.from_pylist([dict(id=2, value=None, name='b'), dict(id=1, value=11, name='a'),
                                  dict(id=3, value=30, name='c')], schema=_SCHEMA)
    builder = table.new_batch_write_builder()
    with patch('pypaimon.table.data_evolution_merge_into._build_tables',
               side_effect=AssertionError('Python merge orchestration')):
        messages = builder.new_update().merge_into(source, on=['id'], when_matched=[
            WhenMatched.update({'value': source_col('value')}, condition=condition)],
            when_not_matched=[WhenNotMatched('*', condition=condition)])
    builder.new_commit().commit(messages)
    assert _rows(table) == [dict(id=1, value=11, name='a'), dict(id=2, value=20, name='b'),
                            dict(id=3, value=30, name='c')]


def test_native_merge_renamed_keys_pandas_and_duplicate_targets(tmp_path):
    import pandas as pd

    table = _table(tmp_path, [dict(id=1, value=10, name='a'), dict(id=1, value=20, name='b')])
    source = pd.DataFrame({'key': [1, 3], 'value': [99, 30], 'name': ['updated', 'new']})
    # Preserve the declared ON type when converting a pandas frame to Arrow.
    source['key'] = source['key'].astype('int32')
    source['value'] = source['value'].astype('int32')
    builder = table.new_batch_write_builder()
    with patch('pypaimon.table.data_evolution_merge_into._build_tables',
               side_effect=AssertionError('Python merge orchestration')):
        messages = builder.new_update().merge_into(source, on={'id': 'key'}, when_matched=[
            WhenMatched.update({'value': source_col('value')})], when_not_matched=[WhenNotMatched('*')])
    builder.new_commit().commit(messages)
    assert _rows(table) == [dict(id=1, value=99, name='a'), dict(id=1, value=99, name='b'),
                            dict(id=3, value=30, name='new')]


def test_native_merge_defers_literals_until_a_clause_is_selected(tmp_path):
    table = _table(tmp_path, [dict(id=1, value=10, name='a')])
    source = pa.Table.from_pylist([dict(id=1, value=11, name='a')], schema=_SCHEMA)
    update = table.new_batch_write_builder().new_update()
    files = set(tmp_path.rglob('*.parquet'))
    messages = update.merge_into(source, on=['id'], when_matched=[
        WhenMatched.update({'value': lit('invalid int')}, condition='s.value < 0')])
    assert messages == []
    with pytest.raises((ValueError, pa.ArrowInvalid)):
        update.merge_into(source, on=['id'], when_matched=[WhenMatched.update({'value': lit('invalid int')})])
    assert set(tmp_path.rglob('*.parquet')) == files
    assert _rows(table) == [dict(id=1, value=10, name='a')]


def test_native_merge_keeps_video_inserts_on_python_path(tmp_path):
    from pypaimon.write.native_merge_into import create_native_merge_into
    from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER

    table = _table(tmp_path).copy({'video-frame-field': 'name'})
    source = pa.Table.from_pylist([dict(id=1, value=10, name='frame')], schema=_SCHEMA)
    assert create_native_merge_into(table, source, ['id'], [], [WhenNotMatched('*')],
                                    'merge', BATCH_COMMIT_IDENTIFIER) is None


@pytest.mark.parametrize('condition', [None, 's.id = 1', 'EXISTS (SELECT 1 WHERE s.id = 1)'])
def test_native_self_merge_does_not_read_unreferenced_blob(tmp_path, condition):
    from pypaimon.table.row.blob import BlobDescriptor

    schema = pa.schema([('id', pa.int32()), ('name', pa.string()), ('payload', pa.large_binary())])
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('db', True)
    catalog.create_table('db.t', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'blob-descriptor-field': 'payload', 'blob-as-descriptor': 'false', 'write.native.enabled': 'true',
    }), False)
    table = catalog.get_table('db.t')
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    descriptor = BlobDescriptor(str(tmp_path / 'unreadable.bin'), 0, 1).serialize()
    try:
        writer.write_arrow(pa.Table.from_pylist([dict(id=1, name='before', payload=descriptor)], schema=schema))
        builder.new_commit().commit(writer.prepare_commit())
    finally:
        writer.close()
    with patch('pypaimon.table.data_evolution_merge_into._build_tables',
               side_effect=AssertionError('Python self merge orchestration')):
        messages = builder.new_update().merge_into(table, on=['_ROW_ID'], when_matched=[
            WhenMatched.update({'name': lit('after')}, condition=condition)])
    builder.new_commit().commit(messages)
    reader = table.new_read_builder().with_projection(['id', 'name'])
    assert reader.new_read().to_arrow(reader.new_scan().plan().splits()).to_pylist() == [dict(id=1, name='after')]
