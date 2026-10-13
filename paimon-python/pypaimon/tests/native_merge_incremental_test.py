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

"""Timestamp-window table sources must be planned and read by Rust core."""

import json
from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.table.data_evolution_merge_into import WhenMatched, WhenNotMatched, source_col
from pypaimon.tests.native_merge_into_test import _SCHEMA, _table, _rows, _write_rows

pytestmark = pytest.mark.native_plan


def _set_time(table, timestamp):
    snapshot = table.snapshot_manager().get_latest_snapshot()
    path = table.snapshot_manager().get_snapshot_path(snapshot.id)
    with open(path) as reader:
        data = json.load(reader)
    data['timeMillis'] = timestamp
    with open(path, 'w') as writer:
        json.dump(data, writer)


def _source(tmp_path, mode='append', options=None, schema=None):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('db', True)
    config = {'write.native.enabled': 'false', 'read.native.enabled': 'false',
              'scan.native-plan.enabled': 'false'}
    if mode == 'pk':
        config['bucket'] = '1'
    if mode == 'evolution':
        config.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'})
    config.update(options or {})
    catalog.create_table('db.source', Schema.from_pyarrow_schema(
        schema or _SCHEMA, primary_keys=['id'] if mode == 'pk' else [], options=config), False)
    return catalog.get_table('db.source')


def _merge(target, source, stream, native, value_column='value'):
    target = target.copy({'write.native.enabled': str(native).lower()})
    builder = target.new_stream_write_builder() if stream else target.new_batch_write_builder()
    kwargs = {'commit_identifier': 57} if stream else {}
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch.object(source, 'snapshot_manager',
                                             side_effect=AssertionError('Python source snapshot resolution')))
            stack.enter_context(patch('pypaimon.table.data_evolution_merge_into._normalize_source',
                                      side_effect=AssertionError('Python source materialization')))
            stack.enter_context(patch('pypaimon.table.data_evolution_merge_into._build_tables',
                                      side_effect=AssertionError('Python MERGE matching')))
        messages = builder.new_update().merge_into(
            source, on=['id'], when_matched=[WhenMatched.update({'value': source_col(value_column)})],
            when_not_matched=[WhenNotMatched({
                'id': source_col('id'), 'value': source_col(value_column), 'name': source_col('name')})], **kwargs)
    commit = builder.new_commit()
    commit.commit(messages, 57) if stream else commit.commit(messages)


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('mode', ['append', 'pk', 'evolution'])
@pytest.mark.parametrize('window,expected', [
    ('100,200', [dict(id=1, value=11, name='old'), dict(id=2, value=22, name='insert')]),
    ('0,100', [dict(id=1, value=-1, name='old')]),
    ('200,300', [dict(id=1, value=10, name='old'), dict(id=3, value=33, name='later')]),
    ('100,150', [dict(id=1, value=10, name='old')]),
    ('100,100', [dict(id=1, value=10, name='old')]),
    ('350,500', [dict(id=1, value=10, name='old')]),
    ('0,50', [dict(id=1, value=10, name='old')]),
])
def test_timestamp_window_source_merge(tmp_path, native, stream, mode, window, expected):
    target = _table(tmp_path / 'target', [dict(id=1, value=10, name='old')])
    source = _source(tmp_path / 'source', mode)
    for timestamp, values in [
            (100, [dict(id=1, value=-1, name='base')]),
            (200, [dict(id=1, value=11, name='new'), dict(id=2, value=22, name='insert')]),
            (300, [dict(id=3, value=33, name='later')])]:
        _write_rows(source, _SCHEMA, values)
        _set_time(source, timestamp)
    source = source.copy({'scan.mode': 'incremental', 'incremental-between-timestamp': window})
    _merge(target, source, stream, native)
    assert _rows(target) == expected


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('stream', [False, True])
def test_incremental_source_uses_selected_branch(tmp_path, native, stream):
    target = _table(tmp_path / 'target', [dict(id=1, value=10, name='old')])
    source = _source(tmp_path / 'source')
    _write_rows(source, _SCHEMA, [dict(id=1, value=-1, name='base')])
    _set_time(source, 100)
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'source')})
    catalog.create_tag(source.identifier, 'base')
    catalog.create_branch(source.identifier, 'dev', tag_name='base')
    branch = catalog.get_table('db.source$branch_dev')
    _write_rows(branch, _SCHEMA, [dict(id=1, value=11, name='branch'), dict(id=2, value=22, name='branch-insert')])
    _set_time(branch, 200)
    _write_rows(source, _SCHEMA, [dict(id=1, value=99, name='main'), dict(id=3, value=33, name='main-insert')])
    _set_time(source, 200)
    _merge(target, branch.copy({'incremental-between-timestamp': '100,200'}), stream, native)
    assert _rows(target) == [dict(id=1, value=11, name='old'), dict(id=2, value=22, name='branch-insert')]


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('mode', ['append', 'pk'])
def test_incremental_source_projects_evolved_field_ids(tmp_path, native, stream, mode):
    from pypaimon.schema.schema_change import SchemaChange
    from pypaimon.schema.data_types import AtomicType

    target = _table(tmp_path / 'target', [dict(id=1, value=10, name='old')])
    old_schema = pa.schema([_SCHEMA.field('id'), _SCHEMA.field('value')])
    source = _source(tmp_path / 'source', mode, schema=old_schema)
    _write_rows(source, old_schema, [dict(id=1, value=11)])
    _set_time(source, 100)
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'source')})
    catalog.alter_table('db.source', [
        SchemaChange.rename_column('value', 'incoming'), SchemaChange.add_column('name', AtomicType('STRING'))], False)
    source = catalog.get_table('db.source')
    evolved_schema = pa.schema([_SCHEMA.field('id'), pa.field('incoming', pa.int32()), _SCHEMA.field('name')])
    _write_rows(source, evolved_schema, [dict(id=2, incoming=22, name='insert')])
    _set_time(source, 200)
    _merge(target, source.copy({'incremental-between-timestamp': '0,200'}), stream, native, 'incoming')
    assert _rows(target) == [dict(id=1, value=11, name='old'), dict(id=2, value=22, name='insert')]


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('window', ['200,100', '100', 'one,200', '100, 200', '1_00,200',
                                    '0,9223372036854775808'])
def test_invalid_incremental_source_never_stages_target(tmp_path, native, stream, window):
    target = _table(tmp_path / 'target', [dict(id=1, value=10, name='old')])
    source = _source(tmp_path / 'source')
    _write_rows(source, _SCHEMA, [dict(id=1, value=11, name='new')])
    _set_time(source, 100)
    before_snapshot = target.snapshot_manager().get_latest_snapshot().id
    before_files = {path for path in (tmp_path / 'target').rglob('*') if path.is_file()}
    with pytest.raises(ValueError):
        _merge(target, source.copy({'incremental-between-timestamp': window}), stream, native)
    assert target.snapshot_manager().get_latest_snapshot().id == before_snapshot
    assert {path for path in (tmp_path / 'target').rglob('*') if path.is_file()} == before_files
    assert _rows(target) == [dict(id=1, value=10, name='old')]


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('stream', [False, True])
def test_incremental_source_auto_uses_changelog(tmp_path, native, stream):
    target = _table(tmp_path / 'target', [dict(id=1, value=10, name='old')])
    source = _source(tmp_path / 'source', 'pk', options={'changelog-producer': 'input'})
    for time, values in [(100, [dict(id=1, value=-1, name='base')]),
                         (200, [dict(id=1, value=11, name='new')]),
                         (300, [dict(id=2, value=22, name='insert')])]:
        _write_rows(source, _SCHEMA, values)
        _set_time(source, time)
    _merge(target, source.copy({'incremental-between-timestamp': '100,300'}), stream, native)
    assert _rows(target) == [dict(id=1, value=11, name='old'), dict(id=2, value=22, name='insert')]


@pytest.mark.parametrize('stream', [False, True])
def test_missing_source_snapshot_does_not_stage_target(tmp_path, stream):
    target = _table(tmp_path / 'target', [dict(id=1, value=10, name='old')])
    source = _source(tmp_path / 'source')
    for time in (100, 200, 300):
        _write_rows(source, _SCHEMA, [dict(id=time, value=time, name='source')])
        _set_time(source, time)
    source.file_io.delete(source.snapshot_manager().get_snapshot_path(2), False)
    before_snapshot = target.snapshot_manager().get_latest_snapshot().id
    before_files = {path for path in (tmp_path / 'target').rglob('*') if path.is_file()}
    with pytest.raises(ValueError, match='Snapshot 2 does not exist'):
        _merge(target, source.copy({'incremental-between-timestamp': '0,300'}), stream, True)
    assert target.snapshot_manager().get_latest_snapshot().id == before_snapshot
    assert {path for path in (tmp_path / 'target').rglob('*') if path.is_file()} == before_files
