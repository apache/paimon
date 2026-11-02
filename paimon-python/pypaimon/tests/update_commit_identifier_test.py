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

"""Update commit identifiers use Java long and Rust i64 before execution."""

from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.table.data_evolution_merge_into import merge_into
from pypaimon.write.table_update import StreamTableUpdate
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
from pypaimon.write.table_upsert_by_key import TableUpsertByKey

pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]

_OPERATIONS = [
    ('new_update_by_row_id', '_new_row_id_updater', 0),
    ('update_by_arrow_with_row_id', '_update_by_arrow_with_row_id', 1),
    ('upsert_by_arrow_with_key', '_upsert_by_arrow_with_key', 2),
    ('upsert_by_key', '_upsert_by_key', 2),
    ('update_by_predicate', '_update_by_predicate', 2),
    ('delete_by_predicate', '_delete_by_predicate', 1),
    ('delete_by_row_id', '_delete_by_row_id', 1),
    ('merge_into', '_merge_into', 4),
]
_INVALID_IDENTIFIERS = [(1 << 63, OverflowError), (-(1 << 63) - 1, OverflowError),
                        (1.5, TypeError), ('7', TypeError)]


class _IndexValue:
    def __init__(self, value):
        self.value = value

    def __index__(self):
        return self.value


def _invoke(update, method, identifier):
    if method == 'new_update_by_row_id':
        return update.new_update_by_row_id(identifier)
    if method == 'update_by_arrow_with_row_id':
        return update.update_by_arrow_with_row_id(object(), identifier)
    if method == 'upsert_by_arrow_with_key':
        return update.upsert_by_arrow_with_key(object(), ['id'], identifier)
    if method == 'upsert_by_key':
        return update.upsert_by_key(object(), ['id'], identifier)
    if method == 'update_by_predicate':
        return update.update_by_predicate(None, {'value': 42}, identifier)
    if method == 'delete_by_predicate':
        return update.delete_by_predicate(None, identifier)
    if method == 'delete_by_row_id':
        return update.delete_by_row_id([0], identifier)
    return update.merge_into(object(), on=['id'], commit_identifier=identifier)


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('identifier, error', _INVALID_IDENTIFIERS)
@pytest.mark.parametrize('method, hook, position', _OPERATIONS)
def test_stream_update_rejects_invalid_identifiers_before_delegation(native, identifier, error, method, hook, position):
    table = SimpleNamespace(options=SimpleNamespace(native_write_enabled=lambda: native))
    update = StreamTableUpdate(table, 'writer')
    with patch.object(update, hook, side_effect=AssertionError('An invalid update reached execution')) as execute:
        with pytest.raises(error):
            _invoke(update, method, identifier)
        execute.assert_not_called()


@pytest.mark.parametrize('identifier', [-(1 << 63), (1 << 63) - 1, _IndexValue(37)])
@pytest.mark.parametrize('method, hook, position', _OPERATIONS)
def test_stream_update_preserves_long_bounds_and_normalizes_index_values(identifier, method, hook, position):
    update = StreamTableUpdate(object(), 'writer')
    expected = identifier.value if isinstance(identifier, _IndexValue) else identifier
    with patch.object(update, hook, return_value=['prepared']) as execute:
        assert _invoke(update, method, identifier) == ['prepared']
        forwarded = execute.call_args[0][position]
        assert type(forwarded) is int
        assert forwarded == expected


@pytest.mark.parametrize('identifier, error', _INVALID_IDENTIFIERS)
@pytest.mark.parametrize('operation', ['row-id', 'upsert', 'merge'])
def test_standalone_update_entries_reject_invalid_identifiers_before_loading_execution(identifier, error, operation):
    with pytest.raises(error):
        if operation == 'row-id':
            TableUpdateByRowId(object(), 'writer', identifier)
        elif operation == 'upsert':
            TableUpsertByKey(object(), 'writer', identifier)
        else:
            merge_into(object(), object(), on=['id'], commit_user='writer', commit_identifier=identifier)


def test_invalid_identifier_leaves_input_callbacks_files_and_snapshot_untouched(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('value', pa.int32())])
    catalog.create_table('default.records', Schema.from_pyarrow_schema(schema, options={
        'bucket': '-1', 'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
        'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false',
    }), False)
    table = catalog.get_table('default.records')
    batch = table.new_batch_write_builder()
    writer, commit = batch.new_write(), batch.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pydict({'id': [1, 2], 'value': [10, 20]}, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    builder = table.new_stream_write_builder()
    update = builder.new_update()
    before = set(tmp_path.rglob('*.parquet'))

    class UnconsumedRows:
        def __iter__(self):
            raise AssertionError('Invalid identifier consumed source rows')

    def assignment(_):
        raise AssertionError('Invalid identifier ran an assignment')

    with pytest.raises(OverflowError):
        update.upsert_by_key(UnconsumedRows(), ['id'], 1 << 63)
    with pytest.raises(TypeError):
        update.update_by_predicate(None, {'value': assignment}, 1.5, read_columns=['value'])
    assert set(tmp_path.rglob('*.parquet')) == before
    assert table.snapshot_manager().get_latest_snapshot().id == 1

    # A rejected call must leave the configured update usable for a valid round.
    messages = update.update_by_arrow_with_row_id(pa.table({'_ROW_ID': [0], 'value': [99]}), _IndexValue(37))
    commit = builder.new_commit()
    try:
        commit.commit(messages, _IndexValue(37))
    finally:
        commit.close()
    assert table.snapshot_manager().get_latest_snapshot().commit_identifier == 37
    read = table.new_read_builder()
    assert read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id').to_pylist() == [
        {'id': 1, 'value': 99}, {'id': 2, 'value': 20}]
