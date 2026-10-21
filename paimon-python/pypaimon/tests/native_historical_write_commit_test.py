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

"""Historical read views retain their schema while writes publish at latest."""

from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.execution.native.runtime import (native_runtime_available)
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.tests import native_plan_rest_test
from pypaimon.execution.write_dispatch import NativeTableWrite


pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
rest_catalog = native_plan_rest_test.rest_catalog
_ENGINES = [False, pytest.param(True, marks=[pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed')])]
_SCHEMA = pa.schema([('id', pa.int32()), ('pt', pa.int32()), ('value', pa.int32())])
_LATEST_SCHEMA = _SCHEMA.append(pa.field('extra', pa.int32()))


def _commit_guard(commit, native):
    stack = ExitStack()
    if native:
        for method in ('commit', 'overwrite', 'truncate_table', 'drop_partitions'):
            stack.enter_context(patch.object(commit.file_store_commit, method,
                                             side_effect=AssertionError('Python commit fallback')))
    return stack


def _write(table, rows, native=False, overwrite=False):
    builder = table.new_batch_write_builder()
    if overwrite:
        builder = builder.overwrite({'pt': '1'})
    writer = builder.new_write()
    try:
        assert isinstance(writer, NativeTableWrite) == native
        schema = _LATEST_SCHEMA if 'extra' in table.field_names else _SCHEMA
        writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
        return writer.prepare_commit()
    finally:
        writer.close()


def _commit(table, messages, native=False, overwrite=False):
    builder = table.new_batch_write_builder()
    if overwrite:
        builder = builder.overwrite({'pt': '1'})
    commit = builder.new_commit()
    try:
        with _commit_guard(commit, native):
            commit.commit(messages)
    finally:
        commit.close()


def _rows(table):
    builder = table.new_read_builder()
    return builder.new_read().to_arrow(builder.new_scan().plan().splits()).sort_by('id').to_pylist()


def _evolved(catalog, mode, native, view='snapshot'):
    options = {key: 'false' for key in (
        'write.native.enabled', 'commit.native.enabled', 'read.native.enabled', 'scan.native-plan.enabled')}
    if mode == 'de':
        options.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'})
    if mode == 'pk':
        options['bucket'] = '1'
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        _SCHEMA, partition_keys=['pt'], primary_keys=['id', 'pt'] if mode == 'pk' else [],
        options=options), False)
    original = catalog.get_table('default.t')
    _commit(original, _write(original, [dict(id=1, pt=1, value=10)]))
    catalog.alter_table('default.t', [SchemaChange.add_column('extra', AtomicType('INT'))])
    latest = catalog.get_table('default.t')
    data = [dict(id=2, pt=1, value=20, extra=200), dict(id=4, pt=2, value=40, extra=400)]
    if mode == 'pk':
        data.append(dict(id=1, pt=1, value=25, extra=250))
    _commit(latest, _write(latest, data))
    historical = latest.copy({'scan.snapshot-id': '1'})
    if view == 'empty':
        historical = historical._copy_with_snapshot(None)
    options = {'write.native.enabled': str(native).lower(), 'commit.native.enabled': str(native).lower()}
    return latest.copy(options), historical.copy(options)


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('mode', ['append', 'de', 'pk'])
@pytest.mark.parametrize('operation', ['append', 'commit', 'overwrite', 'truncate', 'partitions'])
def test_historical_write_and_commit_use_latest_state(rest_catalog, native, mode, operation):
    catalog, _ = rest_catalog
    latest, historical = _evolved(catalog, mode, native)
    expected = _rows(latest)
    assert _rows(historical) == [dict(id=1, pt=1, value=10)]
    if operation in ('truncate', 'partitions'):
        commit = historical.new_batch_write_builder().new_commit()
        try:
            with _commit_guard(commit, native):
                if operation == 'truncate':
                    commit.truncate_table()
                    expected = []
                else:
                    commit.truncate_partitions([{'pt': '1'}])
                    expected = [row for row in expected if row['pt'] != 1]
        finally:
            commit.close()
    else:
        current_input = operation == 'commit'
        row = dict(id=3, pt=1, value=30)
        if current_input:
            row['extra'] = 300
        messages = _write(latest if current_input else historical, [row], native, operation == 'overwrite')
        assert all(file.schema_id == int(current_input) for message in messages for file in message.new_files)
        _commit(historical, messages, native, operation == 'overwrite')
        if operation == 'overwrite':
            expected = [row for row in expected if row['pt'] != 1]
        expected.append(dict(row, extra=300 if current_input else None))
        expected.sort(key=lambda row: row['id'])
    assert _rows(latest) == expected
    snapshot = latest.snapshot_manager().get_latest_snapshot()
    assert snapshot.id == 3
    assert snapshot.schema_id == 1
    assert _rows(historical) == [dict(id=1, pt=1, value=10)]


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('mode', ['append', 'de', 'pk'])
def test_captured_empty_write_does_not_change_its_read_view(rest_catalog, native, mode):
    catalog, _ = rest_catalog
    latest, empty = _evolved(catalog, mode, native, 'empty')
    assert _rows(empty) == []
    before = _rows(latest)
    messages = _write(empty, [dict(id=3, pt=1, value=30)], native)
    _commit(empty, messages, native)
    assert _rows(empty) == []
    assert _rows(latest) == sorted(before + [dict(id=3, pt=1, value=30, extra=None)], key=lambda row: row['id'])


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
def test_historical_primary_key_write_restores_current_sequences(rest_catalog, native):
    catalog, _ = rest_catalog
    latest, historical = _evolved(catalog, 'pk', native)
    messages = _write(historical, [dict(id=1, pt=1, value=99)], native)
    _commit(historical, messages, native)
    assert _rows(latest)[0] == dict(id=1, pt=1, value=99, extra=None)
    assert _rows(historical) == [dict(id=1, pt=1, value=10)]


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
def test_historical_branch_publication_does_not_change_main(rest_catalog, native):
    catalog, _ = rest_catalog
    main, _ = _evolved(catalog, 'de', False)
    catalog.create_tag(main.identifier, 'base', 2)
    catalog.create_branch(main.identifier, 'dev', tag_name='base')
    branch = catalog.get_table('default.t$branch_dev').copy({
        'write.native.enabled': str(native).lower(), 'commit.native.enabled': str(native).lower()})
    historical = branch.copy({'scan.snapshot-id': '2'})
    before = _rows(main)
    _commit(branch, _write(branch, [dict(id=5, pt=1, value=50, extra=500)], native), native)
    _commit(historical, _write(historical, [dict(id=6, pt=1, value=60, extra=600)], native), native)
    assert _rows(main) == before
    assert len(_rows(branch)) == 5
    commit = historical.new_batch_write_builder().new_commit()
    try:
        with _commit_guard(commit, native):
            commit.truncate_table()
    finally:
        commit.close()
    assert _rows(branch) == []
    assert _rows(main) == before
