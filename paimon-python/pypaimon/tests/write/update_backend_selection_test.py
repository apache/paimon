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

"""Public update constructors select one backend before acquiring resources."""

import subprocess
import sys
from unittest.mock import Mock, patch

import pytest

from pypaimon.write.table_update import ShardTableUpdator


@pytest.mark.parametrize('fails', [False, True])
def test_direct_shard_constructor_never_loads_or_replays_python(tmp_path, fails):
    source = r'''
import importlib.abc
import sys
from types import SimpleNamespace
from unittest.mock import Mock, patch

class BlockPythonExecution(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'pypaimon.execution.python' or fullname.startswith('pypaimon.execution.python.'):
            raise AssertionError('Native shard loaded Python execution: ' + fullname)

sys.meta_path.insert(0, BlockPythonExecution())
from pypaimon.write.table_update import ShardTableUpdator

failure = OSError('A staged mutation may already have succeeded')
core = Mock()
core.prepare_commit.return_value = ['prepared']
core.update_by_arrow_batch.side_effect = failure if sys.argv[1] == 'True' else None
with patch('pypaimon.write.backend_dispatch.prepare_native_write_operation', return_value=core) as prepare:
    table = SimpleNamespace(options=object(), identifier=object(),
                            catalog_environment=SimpleNamespace(table_query_auth=lambda *args: None))
    shard = ShardTableUpdator(table, ['value'], ['value'], 'writer', 0, 2)
    prepare.assert_called_once_with('shard_update', table, 'writer', ['value'], ['value'], 0, 2)
    assert shard.arrow_reader() is core.arrow_reader.return_value
    data = object()
    try:
        shard.update_by_arrow_batch(data)
    except OSError as caught:
        assert sys.argv[1] == 'True' and caught is failure
    else:
        assert sys.argv[1] == 'False'
        assert shard.prepare_commit() == ['prepared']
    shard.close()
core.update_by_arrow_batch.assert_called_once_with(data)
core.close.assert_called_once_with()
'''
    result = subprocess.run(
        [sys.executable, '-c', source, str(fails)], stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, universal_newlines=True, timeout=30)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize('shard,count,columns', [
    (-1, 1, ['value']), (1, 1, ['value']), (0, 0, ['value']), (0, 1, [])])
def test_direct_shard_constructor_validates_before_backend_preparation(shard, count, columns):
    with patch('pypaimon.write.backend_dispatch.prepare_native_write_operation') as prepare:
        with pytest.raises(ValueError):
            ShardTableUpdator(Mock(), None, columns, 'writer', shard, count)
    prepare.assert_not_called()


@pytest.mark.parametrize('operation', ['direct', 'grouped', 'predicate', 'upsert-arrow', 'upsert-rows',
                                       'delete-predicate', 'delete-id', 'shard'])
def test_python_update_keeps_selected_backend_for_nested_work(tmp_path, operation):
    import pyarrow as pa

    from pypaimon import CatalogFactory, Schema
    from pypaimon.table.row.generic_row import GenericRow

    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('value', pa.int32())])
    catalog.create_table('default.data', Schema.from_pyarrow_schema(schema, options={
        'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
        'deletion-vectors.enabled': 'true', 'write.native.enabled': 'false',
        'commit.native.enabled': 'false', 'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false',
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

    table = table.copy({'write.native.enabled': 'true'})
    update = table.new_batch_write_builder().new_update().with_update_type(['value'])
    data = pa.table({'id': [1], 'value': [11]}, schema=schema)
    row_ids = pa.table({'_ROW_ID': [0], 'value': pa.array([11], pa.int32())})
    # The top-level preparation is unsupported. Nested target-file writing
    # must use the selected Python backend without reopening Native resources.
    prepare = Mock(return_value=None)
    with patch('pypaimon.write.backend_dispatch.prepare_native_write_operation', prepare), \
            patch('pypaimon.write.update_dispatch.prepare_native_write_operation', prepare):
        if operation == 'direct':
            messages = update.update_by_arrow_with_row_id(row_ids)
        elif operation == 'grouped':
            messages = update.update_by_arrow_batches_with_row_id(iter([row_ids]))
        elif operation == 'predicate':
            messages = update.update_by_predicate(None, {'value': 11})
        elif operation == 'upsert-arrow':
            messages = update.upsert_by_arrow_with_key(data, ['id'])
        elif operation == 'upsert-rows':
            messages = update.upsert_by_key([GenericRow([1, 11], table.fields)], ['id'])
        elif operation == 'delete-predicate':
            messages = update.delete_by_predicate(update.new_predicate_builder().equal('id', 1))
        elif operation == 'delete-id':
            messages = update.delete_by_row_id([0])
        else:
            shard = update.new_shard_updator(0, 1)
            try:
                reader = shard.arrow_reader()
                for batch in reader:
                    shard.update_by_arrow_batch(pa.record_batch(
                        [pa.array([11] * batch.num_rows, pa.int32())], names=['value']))
                messages = shard.prepare_commit()
            finally:
                shard.close()
    assert messages
    prepare.assert_called_once()
    commit = table.new_batch_write_builder().new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    read = table.new_read_builder()
    rows = read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id').to_pylist()
    if operation.startswith('delete'):
        assert rows == [{'id': 2, 'value': 20}]
    else:
        assert rows == [{'id': 1, 'value': 11},
                        {'id': 2, 'value': 11 if operation in ('predicate', 'shard') else 20}]
