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
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
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


@pytest.mark.parametrize('stream', [False, True])
def test_native_merge_reads_paimon_source_in_core(tmp_path, stream):
    target = _table(tmp_path / 'target', [dict(id=1, value=10, name='old')])
    source = _table(tmp_path / 'source', [dict(id=1, value=11, name='new'), dict(id=2, value=22, name='insert')])
    builder = target.new_stream_write_builder() if stream else target.new_batch_write_builder()
    kwargs = dict(commit_identifier=57) if stream else {}
    with patch('pypaimon.table.data_evolution_merge_into._normalize_source',
               side_effect=AssertionError('Python source materialization')), \
            patch('pypaimon.table.data_evolution_merge_into._build_tables',
                  side_effect=AssertionError('Python merge orchestration')):
        messages = builder.new_update().merge_into(source, on=['id'], when_matched=[
            WhenMatched.update({'value': source_col('value')}, condition='s.value > t.value')],
            when_not_matched=[WhenNotMatched('*')], **kwargs)
    commit = builder.new_commit()
    commit.commit(messages, 57) if stream else commit.commit(messages)
    assert _rows(target) == [dict(id=1, value=11, name='old'), dict(id=2, value=22, name='insert')]
    assert _rows(source) == [dict(id=1, value=11, name='new'), dict(id=2, value=22, name='insert')]


@pytest.mark.parametrize('native', [False, True])
def test_table_source_respects_point_in_time_read_options(tmp_path, native):
    target = _table(tmp_path / 'target', [dict(id=1, value=10, name='old')])
    source = _table(tmp_path / 'source', [dict(id=1, value=11, name='new')])
    snapshot = source.snapshot_manager().get_latest_snapshot().id
    builder = source.new_batch_write_builder()
    writer = builder.new_write()
    try:
        writer.write_arrow(pa.Table.from_pylist([dict(id=2, value=22, name='later')], schema=_SCHEMA))
        builder.new_commit().commit(writer.prepare_commit())
    finally:
        writer.close()
    target = target.copy({'write.native.enabled': str(native).lower()})
    historical = source.copy({'scan.snapshot-id': str(snapshot)})
    builder = target.new_batch_write_builder()
    messages = builder.new_update().merge_into(historical, on=['id'], when_matched=[
        WhenMatched.update({'value': source_col('value')})], when_not_matched=[WhenNotMatched('*')])
    builder.new_commit().commit(messages)
    assert _rows(target) == [dict(id=1, value=11, name='old')]
    assert _rows(source) == [dict(id=1, value=11, name='new'), dict(id=2, value=22, name='later')]


def test_native_table_source_projects_away_unreadable_blob(tmp_path):
    from pypaimon.table.row.blob import BlobDescriptor

    target = _table(tmp_path / 'target', [dict(id=1, value=10, name='old')])
    schema = pa.schema(list(_SCHEMA) + [pa.field('payload', pa.large_binary())])
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'source')})
    catalog.create_database('db', True)
    catalog.create_table('db.source', Schema.from_pyarrow_schema(schema, options={
        'blob-descriptor-field': 'payload', 'blob-as-descriptor': 'false', 'write.native.enabled': 'true',
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
    }), False)
    source = catalog.get_table('db.source')
    builder = source.new_batch_write_builder()
    writer = builder.new_write()
    try:
        values = dict(id=1, value=11, name='new',
                      payload=BlobDescriptor(str(tmp_path / 'unreadable.bin'), 0, 1).serialize())
        writer.write_arrow(pa.Table.from_pylist([values], schema=schema))
        builder.new_commit().commit(writer.prepare_commit())
    finally:
        writer.close()
    builder = target.new_batch_write_builder()
    with patch('pypaimon.table.data_evolution_merge_into._normalize_source',
               side_effect=AssertionError('Python source materialization')):
        messages = builder.new_update().merge_into(source, on=['id'], when_matched=[
            WhenMatched.update({'value': source_col('value')}, condition='s.value > t.value')])
    builder.new_commit().commit(messages)
    assert _rows(target) == [dict(id=1, value=11, name='old')]


def test_native_table_source_pk_reads_logical_rows_and_needs_no_de(tmp_path):
    target = _table(tmp_path / 'target', [dict(id=1, value=10, name='old')])
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'source')})
    catalog.create_database('db', True)
    source_schema = Schema.from_pyarrow_schema(
        _SCHEMA, primary_keys=['id'], options={'bucket': '1', 'write.native.enabled': 'true'})
    catalog.create_table('db.source', source_schema, False)
    source = catalog.get_table('db.source')
    for values in [dict(id=1, value=11, name='first'), dict(id=1, value=12, name='latest'),
                   dict(id=2, value=22, name='new')]:
        builder = source.new_batch_write_builder()
        writer = builder.new_write()
        try:
            writer.write_arrow(pa.Table.from_pylist([values], schema=_SCHEMA))
            builder.new_commit().commit(writer.prepare_commit())
        finally:
            writer.close()
    builder = target.new_batch_write_builder()
    with patch('pypaimon.table.data_evolution_merge_into._normalize_source',
               side_effect=AssertionError('Python source materialization')):
        messages = builder.new_update().merge_into(source, on=['id'], when_matched=[
            WhenMatched.update({'value': source_col('value')})], when_not_matched=[WhenNotMatched('*')])
    builder.new_commit().commit(messages)
    assert _rows(target) == [dict(id=1, value=12, name='old'), dict(id=2, value=22, name='new')]


def _write_rows(table, schema, rows):
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
        builder.new_commit().commit(writer.prepare_commit())
    finally:
        writer.close()


def test_native_table_source_preserves_nested_values_and_null_keys_across_files(tmp_path):
    schema = pa.schema([
        ('id', pa.int32()), ('items', pa.list_(pa.int32())), ('lookup', pa.map_(pa.string(), pa.int32())),
    ])
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('db', True)
    common = {'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
              'write.native.enabled': 'true'}
    for name in ['source', 'target']:
        catalog.create_table('db.' + name, Schema.from_pyarrow_schema(schema, options=common), False)
    source, target = catalog.get_table('db.source'), catalog.get_table('db.target')
    _write_rows(target, schema, [dict(id=1, items=[0], lookup=[('old', 0)]),
                                 dict(id=None, items=[9], lookup=[])])
    values = [dict(id=2, items=[], lookup=[('two', None)]),
              dict(id=1, items=[1, None], lookup=[('one', 1)]),
              dict(id=None, items=None, lookup=None)]
    for value in values:
        _write_rows(source, schema, [value])
    builder = target.new_batch_write_builder()
    with patch('pypaimon.table.data_evolution_merge_into._normalize_source',
               side_effect=AssertionError('Python source materialization')):
        messages = builder.new_update().merge_into(
            source, on=['id'], when_matched=[WhenMatched.update('*')],
            when_not_matched=[WhenNotMatched('*')])
    builder.new_commit().commit(messages)
    reader = target.new_read_builder()
    actual = reader.new_read().to_arrow(reader.new_scan().plan().splits()).to_pylist()
    assert sorted([row for row in actual if row['id'] is not None], key=lambda row: row['id']) == sorted(
        [row for row in values if row['id'] is not None], key=lambda row: row['id'])
    assert sorted([row for row in actual if row['id'] is None], key=lambda row: row['items'] is None) == [
        dict(id=None, items=[9], lookup=[]), dict(id=None, items=None, lookup=None)]


@pytest.mark.parametrize('native', [False, True])
def test_table_source_resolves_filesystem_blob_views_before_merge(tmp_path, native):
    from pypaimon.table.row.blob import BlobViewStruct

    schema = pa.schema([('id', pa.int32()), ('payload', pa.large_binary())])
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('db', True)
    common = {'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
    for name, extra in [('upstream', {}), ('source', {'blob-view-field': 'payload'}),
                        ('target', {'write.native.enabled': str(native).lower()})]:
        catalog.create_table('db.' + name, Schema.from_pyarrow_schema(schema, options=dict(common, **extra)), False)
    upstream, source = catalog.get_table('db.upstream'), catalog.get_table('db.source')
    _write_rows(upstream, schema, [dict(id=1, payload=b'actual-payload')])
    reference = BlobViewStruct('db.upstream', upstream.field_dict['payload'].id, 0).serialize()
    _write_rows(source, schema, [dict(id=1, payload=reference)])
    target = catalog.get_table('db.target')
    builder = target.new_batch_write_builder()
    messages = builder.new_update().merge_into(source, on=['id'], when_not_matched=[WhenNotMatched('*')])
    builder.new_commit().commit(messages)
    reader = target.new_read_builder()
    assert reader.new_read().to_arrow(reader.new_scan().plan().splits()).to_pylist() == [
        dict(id=1, payload=b'actual-payload')]


def test_native_table_source_rejects_invalid_scan_options(tmp_path):
    target = _table(tmp_path / 'target', [dict(id=1, value=10, name='old')])
    source = _table(tmp_path / 'source', [dict(id=1, value=11, name='new')])
    source = source.copy({'scan.mode': 'default', 'scan.file-creation-time-millis': '1'})
    before = set(tmp_path.rglob('*.parquet'))
    with pytest.raises(ValueError, match='not yet supported|conflicts'):
        target.new_batch_write_builder().new_update().merge_into(
            source, on=['id'], when_matched=[WhenMatched.update({'value': source_col('value')})])
    assert set(tmp_path.rglob('*.parquet')) == before
    assert _rows(target) == [dict(id=1, value=10, name='old')]


def test_native_table_source_keeps_its_selected_branch(tmp_path):
    from pypaimon.common.identifier import Identifier

    target = _table(tmp_path / 'target', [dict(id=1, value=10, name='old')])
    source = _table(tmp_path / 'source', [dict(id=1, value=11, name='base')])
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'source')})
    catalog.create_tag(source.identifier, 'base')
    catalog.create_branch(source.identifier, 'dev', tag_name='base')
    branch = catalog.get_table(Identifier('db', 't', branch='dev'))
    _write_rows(branch, _SCHEMA, [dict(id=2, value=22, name='branch')])
    _write_rows(source, _SCHEMA, [dict(id=3, value=33, name='main')])
    builder = target.new_batch_write_builder()
    with patch('pypaimon.table.data_evolution_merge_into._normalize_source',
               side_effect=AssertionError('Python source materialization')):
        messages = builder.new_update().merge_into(
            branch, on=['id'], when_matched=[WhenMatched.update({'value': source_col('value')})],
            when_not_matched=[WhenNotMatched('*')])
    builder.new_commit().commit(messages)
    assert _rows(target) == [dict(id=1, value=11, name='old'), dict(id=2, value=22, name='branch')]
    assert _rows(source) == [dict(id=1, value=11, name='base'), dict(id=3, value=33, name='main')]


@pytest.mark.python_write
@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('source_mode', ['append', 'pk', 'evolution'])
@pytest.mark.parametrize('location', ['local', 'file-uri', 'directory', 'external'])
def test_python_written_partition_source_uses_java_paths_for_native_merge(tmp_path, stream, source_mode, location):
    from pathlib import Path
    from pypaimon.write.native_write import NativeTableWrite

    target = _table(tmp_path / 'target', [dict(id=1, value=10, name='old')])
    warehouse = tmp_path / 'source'
    catalog = CatalogFactory.create({'warehouse': warehouse.as_uri() if location == 'file-uri' else str(warehouse)})
    catalog.create_database('db', True)
    schema = pa.schema(list(_SCHEMA) + [pa.field('pa#rt', pa.string())])
    options = {'write.native.enabled': 'false'}
    if source_mode == 'pk':
        options.update({'bucket': '1', 'changelog-producer': 'input'})
    if source_mode == 'evolution':
        schema = pa.schema(list(schema) + [pa.field('payload', pa.large_binary())])
        options.update({'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
                        'data-evolution.row-sidecar.enabled': 'true'})
    if location == 'directory':
        options['data-file.path-directory'] = 'data/nested'
    if location == 'external':
        options.update({'data-file.external-paths': (tmp_path / 'external').as_uri(),
                        'data-file.external-paths.strategy': 'round-robin'})
    catalog.create_table('db.source', Schema.from_pyarrow_schema(
        schema, primary_keys=['id', 'pa#rt'] if source_mode == 'pk' else [],
        partition_keys=['pa#rt'], options=options), False)
    source = catalog.get_table('db.source')
    builder = source.new_batch_write_builder()
    writer = builder.new_write()
    assert not isinstance(writer, NativeTableWrite)
    values = [dict(id=1, value=11, name='new', **{'pa#rt': 'a/b'}),
              dict(id=2, value=22, name='insert', **{'pa#rt': 'a%b'})]
    if source_mode == 'evolution':
        for value in values:
            value['payload'] = b'blob-payload'
    try:
        writer.write_arrow(pa.Table.from_pylist(values, schema=schema))
        messages = writer.prepare_commit()
        for message in messages:
            component = 'pa%23rt=' + ('a%2Fb' if message.partition == ('a/b',) else 'a%25b')
            for file in message.new_files + message.changelog_files:
                assert component in file.file_path.split('/'), file.file_path
                assert Path(file.file_path).is_file(), file.file_path
                for extra_file in file.extra_files:
                    assert Path(file.file_path).with_name(extra_file).is_file()
        builder.new_commit().commit(messages)
    finally:
        writer.close()
    # Java's path wins even if an incorrectly laid-out file has the same name.
    for message in messages:
        wrong_bucket = source.path_factory().bucket_path(tuple(message.partition), message.bucket)
        for file in message.new_files:
            if not file.external_path:
                with source.file_io.new_output_stream(wrong_bucket + '/' + file.file_name) as output:
                    output.write(b'wrong partition directory')
    assert _rows(source) == values
    ranges = catalog.get_table('db.source$file_key_ranges').new_read_builder()
    reported = ranges.new_read().to_arrow(ranges.new_scan().plan().splits())['file_path'].to_pylist()
    assert reported and all(source.file_io.exists(path) for path in reported)
    assert all('pa%23rt=' in path for path in reported)
    builder = target.new_stream_write_builder() if stream else target.new_batch_write_builder()
    kwargs = dict(commit_identifier=57) if stream else {}
    with patch('pypaimon.table.data_evolution_merge_into._normalize_source',
               side_effect=AssertionError('Python source materialization')), \
            patch('pypaimon.table.data_evolution_merge_into._build_tables',
                  side_effect=AssertionError('Python merge orchestration')):
        messages = builder.new_update().merge_into(
            source, on=['id'], when_matched=[WhenMatched.update({'value': source_col('value')})],
            when_not_matched=[WhenNotMatched('*')], **kwargs)
    commit = builder.new_commit()
    commit.commit(messages, 57) if stream else commit.commit(messages)
    assert _rows(target) == [dict(id=1, value=11, name='old'), dict(id=2, value=22, name='insert')]


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('partition_type', [pa.float32(), pa.float64()])
def test_float_partition_source_uses_native_merge(tmp_path, native, stream, partition_type):
    target = _table(tmp_path / 'target', [dict(id=1, value=10, name='old')]).copy({
        'write.native.enabled': str(native).lower()})
    schema = pa.schema(list(_SCHEMA) + [pa.field('part', partition_type)])
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'source')})
    catalog.create_database('db', True)
    catalog.create_table('db.source', Schema.from_pyarrow_schema(schema, partition_keys=['part'], options={
        'write.native.enabled': 'false', 'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false',
    }), False)
    source = catalog.get_table('db.source')
    _write_rows(source, schema, [dict(id=1, value=11, name='new', part=1.5),
                                 dict(id=2, value=22, name='insert', part=2.25)])
    builder = target.new_stream_write_builder() if stream else target.new_batch_write_builder()
    kwargs = dict(commit_identifier=57) if stream else {}
    from contextlib import ExitStack
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch('pypaimon.table.data_evolution_merge_into._build_tables',
                                      side_effect=AssertionError('Python source materialization')))
        messages = builder.new_update().merge_into(
            source, on=['id'], when_matched=[WhenMatched.update({'value': source_col('value')})],
            when_not_matched=[WhenNotMatched('*')], **kwargs)
    commit = builder.new_commit()
    commit.commit(messages, 57) if stream else commit.commit(messages)
    assert _rows(target) == [dict(id=1, value=11, name='old'), dict(id=2, value=22, name='insert')]
