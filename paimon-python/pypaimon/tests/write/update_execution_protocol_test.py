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

"""Bound update execution retains public methods and preparation boundaries."""

import subprocess
import sys

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.table_delete import TableDeleteByRowId
from pypaimon.write.table_update import BatchTableUpdate
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
from pypaimon.write.table_upsert_by_key import TableUpsertByKey

pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]


@pytest.fixture
def table(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('value', pa.int32())])
    catalog.create_table('default.data', Schema.from_pyarrow_schema(schema, options={
        'bucket': '-1', 'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'deletion-vectors.enabled': 'true', 'write.native.enabled': 'false',
        'commit.native.enabled': 'false', 'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
    }), False)
    table = catalog.get_table('default.data')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table({'id': [1, 2], 'value': [10, 20]}, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    return table


def _commit(table, messages):
    commit = table.new_batch_write_builder().new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()


def _values(table):
    read = table.new_read_builder()
    return read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id').to_pydict()


def test_bound_update_uses_live_configuration_and_callable_assignments(table):
    built = []

    def assignment(matched):
        built.append(matched.to_pydict())
        return pa.compute.add(matched['value'], 1)

    update = BatchTableUpdate(table, 'public-update')
    update.with_update_type(['value'])
    _commit(table, update.update_by_arrow_with_row_id(pa.table({'_ROW_ID': [0], 'value': [11]})))
    update.with_update_type(['id'])
    _commit(table, update.update_by_arrow_with_row_id(pa.table({'_ROW_ID': [1], 'id': [3]})))
    predicate = update.new_predicate_builder().equal('id', 3)
    _commit(table, update.update_by_predicate(
        predicate, {'value': assignment}, read_columns=['value']))
    assert built == [{'value': [20], '_ROW_ID': [1]}]
    assert _values(table) == {'id': [1, 3], 'value': [11, 21]}


def test_bound_row_id_execution_owns_file_routing_and_committable_state(table):
    update = TableUpdateByRowId(table, 'row-id-update', BATCH_COMMIT_IDENTIFIER)
    messages = update.update_columns(pa.table({'_ROW_ID': [0], 'value': [33]}), ['value'])
    assert messages is update.commit_messages
    assert not hasattr(update, 'first_row_ids')
    assert not hasattr(update, '_write_by_first_row_id')
    _commit(table, messages)
    assert _values(table) == {'id': [1, 2], 'value': [33, 20]}


def test_bound_delete_owns_anchor_routing_and_committable_result(table):
    delete = TableDeleteByRowId(table)
    messages = delete.delete([0, 0])
    assert not hasattr(delete, '_build_commit_message')
    assert not hasattr(delete, '_write_deletion_vector_index')
    _commit(table, messages)
    assert _values(table) == {'id': [2], 'value': [20]}


@pytest.mark.parametrize('rows', [False, True])
def test_prepared_python_upsert_keeps_public_input_methods(table, rows):
    visited = []

    class PublicUpsert(TableUpsertByKey):
        def upsert(self, data, keys, update_cols=None):
            visited.append(('arrow', data.num_rows))
            return super().upsert(data, keys, update_cols)

        def upsert_rows(self, row_items, keys, update_cols=None):
            visited.append(('rows', len(row_items)))
            return super().upsert_rows(row_items, keys, update_cols)

    update = PublicUpsert(table, 'upsert', BATCH_COMMIT_IDENTIFIER)
    data = [{'id': 1, 'value': 12}, {'id': 3, 'value': 30}]
    if rows:
        messages = update.upsert_rows(
            [GenericRow([row['id'], row['value']], table.fields) for row in data], ['id'], ['value'])
    else:
        arrow_data = pa.Table.from_pylist(data, schema=pa.schema([('id', pa.int32()), ('value', pa.int32())]))
        messages = update.upsert(arrow_data, ['id'], ['value'])
    assert visited == [('rows' if rows else 'arrow', 2)]
    _commit(table, messages)
    assert _values(table) == {'id': [1, 2, 3], 'value': [12, 20, 30]}


@pytest.mark.parametrize('operation', ['direct', 'grouped', 'predicate', 'delete-predicate', 'delete-id',
                                       'upsert-arrow', 'upsert-rows'])
@pytest.mark.parametrize('fails', [False, True])
def test_prepared_native_updates_do_not_initialize_or_replay_python(tmp_path, operation, fails):
    source = r'''
import importlib.abc
import sys
from types import SimpleNamespace
from unittest.mock import Mock, patch
import pyarrow as pa

class BlockPythonExecution(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'pypaimon.execution.python' or fullname.startswith('pypaimon.execution.python.'):
            raise AssertionError('Native update loaded Python execution: ' + fullname)

sys.meta_path.insert(0, BlockPythonExecution())
from pypaimon import CatalogFactory, Schema
from pypaimon.table.row.generic_row import GenericRow

operation, fails, warehouse = sys.argv[1:]
catalog = CatalogFactory.create({'warehouse': warehouse})
catalog.create_database('default', True)
catalog.create_table('default.data', Schema.from_pyarrow_schema(
    pa.schema([('id', pa.int32()), ('value', pa.int32())]), options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true', 'deletion-vectors.enabled': 'true',
        'write.native.enabled': 'true', 'commit.native.enabled': 'false',
        'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false'}), False)
table = catalog.get_table('default.data')
update = table.new_batch_write_builder().new_update().with_update_type(['value'])
data = pa.table({'id': [1], 'value': [10]})
row_ids = pa.table({'_ROW_ID': [0], 'value': [11]})
expected = [object()]
failure = OSError('A staged mutation may already have succeeded')
execute = Mock(return_value=expected, side_effect=failure if fails == 'True' else None)
method, args = {
    'direct': ('update_by_arrow_with_row_id', (row_ids,)),
    'grouped': ('update_by_arrow_batches_with_row_id', (iter([row_ids]),)),
    'predicate': ('update_by_predicate', (None, {'value': 12})),
    'delete-predicate': ('delete_by_predicate', (None,)),
    'delete-id': ('delete_by_row_id', ([0],)),
    'upsert-arrow': ('upsert_by_arrow_with_key', (data, ['id'])),
    'upsert-rows': ('upsert_by_key', ([GenericRow([1, 10], table.fields)], ['id'])),
}[operation]
backend_method = {'predicate': 'update', 'delete-predicate': 'delete',
                  'upsert-arrow': 'upsert', 'upsert-rows': 'upsert'}.get(operation, method)
backend = SimpleNamespace(**{backend_method: execute})
with patch('pypaimon.write.update_dispatch.prepare_native_write_operation', return_value=backend):
    try:
        result = getattr(update, method)(*args)
    except OSError as caught:
        assert fails == 'True' and caught is failure
    else:
        assert fails == 'False' and result is expected
execute.assert_called_once()
'''
    result = subprocess.run(
        [sys.executable, '-c', source, operation, str(fails), str(tmp_path / 'warehouse')],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=30)
    assert result.returncode == 0, result.stderr
