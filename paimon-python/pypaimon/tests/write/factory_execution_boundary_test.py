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

"""Selected Python factories retain state without reopening Native execution."""

from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.write.table_delete import TableDeleteByRowId
from pypaimon.write.table_update import ShardTableUpdator, SingleWriter
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId

pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]


@pytest.fixture
def table(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('value', pa.int32())])
    catalog.create_table('default.data', Schema.from_pyarrow_schema(schema, options={
        'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
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
    return table.copy({'write.native.enabled': 'true'})


@pytest.mark.parametrize('operation', ['row-id', 'delete', 'shard'])
def test_explicit_python_factory_keeps_captured_state_and_committable_results(table, operation):
    with patch('pypaimon.write.backend_dispatch.prepare_native_write_operation',
               side_effect=AssertionError('Selected Python operation reopened Native')):
        if operation == 'row-id':
            updater = TableUpdateByRowId._from_python(table, 'writer', BATCH_COMMIT_IDENTIFIER)
            captured = updater._snapshot_files_info()
            messages = updater.update_columns(pa.table({'_ROW_ID': [0], 'value': [11]}), ['value'])
            # Different columns of the same file group share the captured
            # snapshot and append to the same executor's accumulated messages.
            assert updater.update_columns(pa.table({'_ROW_ID': [1], 'id': [3]}), ['id']) is messages
            assert updater._snapshot_files_info().snapshot_id == captured.snapshot_id
            assert all(message.check_from_snapshot == captured.snapshot_id for message in messages)
            expected = [{'id': 1, 'value': 11}, {'id': 3, 'value': 20}]
        elif operation == 'delete':
            coordinator = TableDeleteByRowId._from_python(table)
            captured = coordinator._snapshot_anchor_ranges()
            worker = TableDeleteByRowId._from_python(table, captured)
            messages = worker.delete([0, 0])
            assert all(message.check_from_snapshot == captured.snapshot_id for message in messages)
            expected = [{'id': 2, 'value': 20}]
        else:
            shard = ShardTableUpdator._from_python(table, ['value'], ['value'], 'writer', 0, 1)
            try:
                reader = shard.arrow_reader()
                for batch in reader:
                    shard.update_by_arrow_batch(pa.record_batch([
                        pa.array([11] * batch.num_rows, type=pa.int32())], names=['value']))
                messages = shard.prepare_commit()
                assert shard.prepare_commit() == messages
            finally:
                shard.close()
            expected = [{'id': 1, 'value': 11}, {'id': 2, 'value': 11}]
    assert messages
    commit = table.new_batch_write_builder().new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    read = table.new_read_builder()
    assert read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id').to_pylist() == expected


def test_single_writer_factory_preserves_capacity_and_metadata_across_calls(table):
    data_writer = Mock()
    read = table.new_read_builder()
    metadata = read.new_scan().plan().splits()[0].files[0]
    data_writer.prepare_commit.return_value = [metadata]
    writer = SingleWriter(data_writer, (), 100, 2)
    assert writer.capacity() == 2
    with pytest.raises(Exception, match='exceeds capacity'):
        writer.write(pa.record_batch([pa.array([1, 2, 3])], names=['value']))
    assert writer.capacity() == 2
    data_writer.write.assert_not_called()
    for value in [10, 20]:
        writer.write(pa.record_batch([pa.array([value])], names=['value']))
        if writer.capacity():
            with pytest.raises(Exception, match='capacity left'):
                writer.end()
            data_writer.prepare_commit.assert_not_called()
    assert writer.capacity() == 0
    result = writer.end()
    assert result.first_row_id == 100
    assert result.row_count == 2
    assert data_writer.write.call_count == 2
    data_writer.prepare_commit.assert_called_once_with()
