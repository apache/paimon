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

"""Historical read snapshots remain the conflict baseline of partial updates."""

from contextlib import ExitStack
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.execution.native.runtime import (native_runtime_available)
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.table.data_evolution_merge_into import WhenMatched, WhenNotMatched, source_col
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.execution.native.write.native_update import NativeShardTableUpdator
from pypaimon.execution.python.write.table_update_by_row_id import PythonTableUpdateByRowId
from pypaimon.write.backend_dispatch import native_row_id_update_selected


pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
_NATIVE = pytest.mark.skipif(not native_runtime_available(), reason='pypaimon-rust not installed')
_ENGINES = [False, pytest.param(True, marks=[pytest.mark.native_plan, _NATIVE])]
_OPERATIONS = ['row_id', 'incremental', 'groups', 'predicate', 'shard', 'merge']


def _append(table, data):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(data)
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _tables(catalog, native, selector):
    schema = pa.schema([('id', pa.int32()), ('value', pa.int32()), ('keep', pa.int32())])
    name = 'default.historical_updates'
    catalog.create_table(name, Schema.from_pyarrow_schema(schema, options={
        'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
        'deletion-vectors.enabled': 'true',
        'write.native.enabled': str(native).lower(), 'commit.native.enabled': 'false',
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
    }), False)
    table = catalog.get_table(name)
    _append(table, pa.table({'id': [1, 2], 'value': [10, 20], 'keep': [100, 200]}, schema=schema))
    snapshot = table.snapshot_manager().get_latest_snapshot()
    table.create_tag('before_new')
    catalog.alter_table(name, [SchemaChange.add_column('extra', AtomicType('INT'))])
    latest = catalog.get_table(name)
    _append(latest, pa.table({'id': [3], 'value': [30], 'keep': [300], 'extra': [999]},
                             schema=schema.append(pa.field('extra', pa.int32()))))
    historical = latest.copy({selector: 'before_new' if selector == 'scan.tag-name' else str(snapshot.id)})
    assert historical.field_names == ['id', 'value', 'keep']
    assert latest.field_names == ['id', 'value', 'keep', 'extra']
    return latest, historical, snapshot.id


def _rows(table):
    builder = table.new_read_builder()
    return builder.new_read().to_arrow(builder.new_scan().plan().splits()).sort_by('id').to_pydict()


def _commit(latest, messages):
    commit = latest.new_batch_write_builder().new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()


def _update(table, operation, stream, native):
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    update = builder.new_update().with_update_type(['value'])
    kwargs = dict(commit_identifier=37) if stream else {}
    changed = pa.table({'_ROW_ID': [0], 'value': [11]})
    guard = patch.object(PythonTableUpdateByRowId, 'update_columns',
                         side_effect=AssertionError('Python historical update fallback')) if native else ExitStack()
    with guard:
        if operation == 'row_id':
            return update.update_by_arrow_with_row_id(changed, **kwargs)
        if operation == 'groups':
            return update.update_by_arrow_batches_with_row_id(iter([changed]), **kwargs)
        if operation == 'incremental':
            updater = update.new_update_by_row_id(**kwargs)
            assert native_row_id_update_selected(updater) == native
            return updater.update_columns(changed, ['value'])
        if operation == 'predicate':
            predicate = table.new_read_builder().new_predicate_builder().equal('id', 1)
            return update.update_by_predicate(predicate, {'value': 11}, **kwargs)
        if operation == 'merge':
            source = pa.table({'id': pa.array([1], type=pa.int32()), 'value': [11]})
            return update.merge_into(source, on=['id'], when_matched=[
                WhenMatched.update({'value': source_col('value')})], **kwargs)
        shard = update.new_shard_updator(0, 1)
        if native:
            assert isinstance(shard, NativeShardTableUpdator)
        try:
            seen = []
            for batch in shard.arrow_reader():
                ids = batch.column('id').to_pylist()
                seen.extend(ids)
                shard.update_by_arrow_batch(pa.record_batch([
                    pa.array([11 if value == 1 else 20 for value in ids], type=pa.int32()),
                ], names=['value']))
            assert sorted(seen) == [1, 2]
            return shard.prepare_commit()
        finally:
            if native:
                shard.close()


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('selector', ['scan.snapshot-id', 'scan.tag-name'])
@pytest.mark.parametrize('operation,stream', [
    (operation, stream) for operation in _OPERATIONS for stream in (False, True)
    if operation != 'groups' or not stream])
def test_historical_update_commits_old_schema_with_snapshot_conflict_baseline(
        native_rest_catalog, native, stream, selector, operation):
    latest, historical, snapshot_id = _tables(native_rest_catalog, native, selector)
    messages = _update(historical, operation, stream, native)
    assert messages
    assert all(message.check_from_snapshot == snapshot_id for message in messages)
    assert all(file.schema_id == historical.table_schema.id for message in messages for file in message.new_files)
    _commit(latest, messages)
    expected = {'id': [1, 2, 3], 'value': [11, 20, 30], 'keep': [100, 200, 300], 'extra': [None, None, 999]}
    assert _rows(latest) == expected
    # A second operation still observes the historical group. Its conflict
    # baseline must not silently move to latest just before publication.
    stale = _update(historical, 'row_id', stream, native)
    assert all(message.check_from_snapshot == snapshot_id for message in stale)
    with pytest.raises(RuntimeError, match='(?i)conflict'):
        _commit(latest, stale)
    assert _rows(latest) == expected


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('operation', ['row_id', 'predicate'])
def test_historical_delete_does_not_delete_newer_rows(native_rest_catalog, native, stream, operation):
    latest, historical, snapshot_id = _tables(native_rest_catalog, native, 'scan.snapshot-id')
    builder = historical.new_stream_write_builder() if stream else historical.new_batch_write_builder()
    update = builder.new_update()
    kwargs = dict(commit_identifier=37) if stream else {}
    guard = patch('pypaimon.write.table_delete.TableDeleteByRowId.delete',
                  side_effect=AssertionError('Python historical delete fallback')) if native else ExitStack()
    with guard:
        if operation == 'row_id':
            messages = update.delete_by_row_id([0, 0], **kwargs)
        else:
            predicate = historical.new_read_builder().new_predicate_builder().equal('id', 1)
            messages = update.delete_by_predicate(predicate, **kwargs)
    assert messages
    assert all(message.check_from_snapshot == snapshot_id for message in messages)
    _commit(latest, messages)
    assert _rows(latest) == {'id': [2, 3], 'value': [20, 30], 'keep': [200, 300], 'extra': [None, 999]}


def _append_mutation(table, native, operation, only_matched=False, stream=False):
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    update = builder.new_update().with_update_type(['value'])
    kwargs = dict(commit_identifier=37) if stream else {}
    data = pa.table({'id': [1] if only_matched else [1, 4],
                     'value': [11] if only_matched else [11, 40],
                     'keep': [101] if only_matched else [101, 400]},
                    schema=pa.schema([('id', pa.int32()), ('value', pa.int32()), ('keep', pa.int32())]))
    with ExitStack() as stack:
        if native:
            for method in (
                    'pypaimon.execution.python.write.table_upsert_by_key.PythonTableUpsert._upsert_partition',
                    'pypaimon.execution.python.write.table_upsert_by_key.PythonTableUpsert._upsert_row_partition',
                    'pypaimon.execution.python.write.merge_into._build_tables'):
                stack.enter_context(patch(method, side_effect=AssertionError('Python historical mutation fallback')))
        if operation == 'upsert':
            return update.upsert_by_arrow_with_key(data, ['id'], **kwargs)
        if operation == 'row-upsert':
            return update.upsert_by_key([
                GenericRow([row[name] for name in table.field_names], table.fields) for row in data.to_pylist()
            ], ['id'], **kwargs)
        return update.merge_into(data, on=['id'], when_matched=[
            WhenMatched.update({'value': source_col('value')})],
            when_not_matched=[WhenNotMatched('*')], **kwargs)


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('operation', ['upsert', 'row-upsert', 'merge'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('only_matched', [False, True], ids=['mixed', 'matched'])
def test_historical_append_mutations_preserve_schema_and_conflict_baseline(
        native_rest_catalog, native, operation, stream, only_matched):
    latest, historical, snapshot_id = _tables(native_rest_catalog, native, 'scan.snapshot-id')
    messages = _append_mutation(historical, native, operation, only_matched, stream)
    assert any(message.check_from_snapshot == snapshot_id for message in messages)
    assert all(message.check_from_snapshot in (None, snapshot_id) for message in messages)
    assert all(file.schema_id == historical.table_schema.id for message in messages for file in message.new_files)
    if not only_matched:
        assert any(message.check_from_snapshot is None for message in messages)
        assert all(file.first_row_id is None for message in messages if message.check_from_snapshot is None
                   for file in message.new_files)
    _commit(latest, messages)
    expected = {'id': [1, 2, 3], 'value': [11, 20, 30], 'keep': [100, 200, 300], 'extra': [None, None, 999]}
    if not only_matched:
        for name, value in zip(('id', 'value', 'keep', 'extra'), (4, 40, 400, None)):
            expected[name].append(value)
    assert _rows(latest) == expected
    stale = _append_mutation(historical, native, operation, only_matched, stream)
    with pytest.raises(RuntimeError, match='(?i)conflict'):
        _commit(latest, stale)
    assert _rows(latest) == expected


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('operation', ['upsert', 'merge'])
def test_captured_empty_append_mutations_use_new_row_ids(native_rest_catalog, native, operation):
    latest, historical, _ = _tables(native_rest_catalog, native, 'scan.snapshot-id')
    empty = historical._copy_with_snapshot(None)
    messages = _append_mutation(empty, native, operation)
    assert messages
    assert all(message.check_from_snapshot is None for message in messages)
    assert all(file.first_row_id is None and file.schema_id == historical.table_schema.id
               for message in messages for file in message.new_files)
    _commit(latest, messages)
    # The existing key 1 was not visible in the captured empty target. It must
    # append a fresh row, rather than update the concurrently visible row 1.
    assert _rows(latest) == {
        'id': [1, 1, 2, 3, 4], 'value': [10, 11, 20, 30, 40],
        'keep': [100, 101, 200, 300, 400], 'extra': [None, None, None, 999, None]}


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('change', ['rename', 'drop_readd', 'widen'])
@pytest.mark.parametrize('operation', ['row-id', 'upsert', 'merge'])
def test_historical_partial_files_keep_original_field_ids(native_rest_catalog, native, change, operation):
    latest, historical, snapshot_id = _tables(native_rest_catalog, native, 'scan.snapshot-id')
    catalog, name = native_rest_catalog, 'default.historical_updates'
    if change == 'rename':
        catalog.alter_table(name, [SchemaChange.rename_column('value', 'renamed')])
    elif change == 'drop_readd':
        catalog.alter_table(name, [SchemaChange.drop_column('value')])
        catalog.alter_table(name, [SchemaChange.add_column('value', AtomicType('INT'))])
    else:
        catalog.alter_table(name, [SchemaChange.update_column_type('value', AtomicType('BIGINT'))])
    latest = catalog.get_table(name)
    value_name = 'renamed' if change == 'rename' else 'value'
    values = {'id': [4], value_name: [2 ** 40 if change == 'widen' else 40], 'keep': [400], 'extra': [1000]}
    from pypaimon.schema.data_types import PyarrowFieldParser
    _append(latest, pa.table(values, schema=PyarrowFieldParser.from_paimon_schema(latest.fields)))
    messages = (_update(historical, 'row_id', False, native) if operation == 'row-id' else
                _append_mutation(historical, native, operation))
    assert all(message.check_from_snapshot in (None, snapshot_id) for message in messages)
    for message in messages:
        for file in message.new_files:
            assert file.schema_id == historical.table_schema.id
            if message.check_from_snapshot is not None:
                assert file.write_cols == ['value']
    _commit(historical, messages)
    values = [None, None, None, 40] if change == 'drop_readd' else [11, 20, 30, 2 ** 40 if change == 'widen' else 40]
    expected = {
        'id': [1, 2, 3, 4], value_name: values, 'keep': [100, 200, 300, 400],
        'extra': [None, None, 999, 1000]}
    if operation != 'row-id':
        # A second row with logical key 4 is new to snapshot 1 and receives a
        # fresh physical row ID, even though latest already contains key 4.
        expected['id'].append(4)
        expected[value_name].append(None if change == 'drop_readd' else 40)
        expected['keep'].append(400)
        expected['extra'].append(None)
    assert _rows(latest) == expected
    stale = (_update(historical, 'row_id', False, native) if operation == 'row-id' else
             _append_mutation(historical, native, operation))
    with pytest.raises(RuntimeError, match='(?i)conflict'):
        _commit(historical, stale)
    assert _rows(latest) == expected


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
def test_historical_deletion_vector_does_not_restore_concurrent_deletion(native_rest_catalog, native):
    latest, historical, snapshot_id = _tables(native_rest_catalog, native, 'scan.snapshot-id')
    newer = latest.new_batch_write_builder().new_update().delete_by_row_id([1])
    _commit(latest, newer)
    stale = historical.new_batch_write_builder().new_update().delete_by_row_id([0])
    assert all(message.check_from_snapshot == snapshot_id for message in stale)
    with pytest.raises(RuntimeError, match='(?i)conflict'):
        _commit(latest, stale)
    assert _rows(latest) == {
        'id': [1, 3], 'value': [10, 30], 'keep': [100, 300], 'extra': [None, 999]}


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('operation', ['predicate', 'delete', 'shard', 'merge'])
def test_captured_empty_update_view_stays_empty(native_rest_catalog, native, operation):
    latest, _, _ = _tables(native_rest_catalog, native, 'scan.snapshot-id')
    captured = latest._copy_with_snapshot(None)
    update = captured.new_batch_write_builder().new_update().with_update_type(['value'])
    assignment = Mock(side_effect=AssertionError('An empty view must not evaluate assignments'))
    if operation == 'predicate':
        messages = update.update_by_predicate(None, {'value': assignment}, read_columns=['value'])
    elif operation == 'delete':
        messages = update.delete_by_predicate(None)
    elif operation == 'merge':
        messages = update.merge_into(
            pa.table({'id': pa.array([1], type=pa.int32())}), on=['id'], when_matched=[
                WhenMatched.update({'value': assignment})])
    else:
        shard = update.new_shard_updator(0, 1)
        if native:
            assert isinstance(shard, NativeShardTableUpdator)
        try:
            assert list(shard.arrow_reader()) == []
            messages = shard.prepare_commit()
        finally:
            if native:
                shard.close()
    assert messages == []
    assignment.assert_not_called()
    assert _rows(latest) == {
        'id': [1, 2, 3], 'value': [10, 20, 30], 'keep': [100, 200, 300], 'extra': [None, None, 999]}


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('source_view', [
    'latest', 'snapshot', 'captured', 'empty', 'same-current', 'same-captured'])
def test_row_id_merge_respects_independent_source_read_view(native_rest_catalog, native, source_view):
    latest, historical, snapshot_id = _tables(native_rest_catalog, native, 'scan.snapshot-id')
    builder = latest.new_batch_write_builder()
    _commit(latest, builder.new_update().update_by_arrow_with_row_id(
        pa.table({'_ROW_ID': [0], 'value': [99]})))
    current_snapshot = latest.snapshot_manager().get_latest_snapshot()
    target = historical
    if source_view == 'snapshot':
        source = latest.copy({'scan.snapshot-id': str(current_snapshot.id)})
    elif source_view == 'captured':
        source = latest._copy_with_snapshot(current_snapshot)
    elif source_view == 'empty':
        source = latest._copy_with_snapshot(None)
    elif source_view == 'same-current':
        target = source = latest
    elif source_view == 'same-captured':
        target = source = historical._copy_with_snapshot(
            historical.snapshot_manager().get_snapshot_by_id(snapshot_id))
    else:
        source = latest
    assignment = Mock(side_effect=lambda batch: batch['s.value'])
    guard = patch('pypaimon.execution.python.write.merge_into._build_tables',
                  side_effect=AssertionError('Python MERGE fallback')) if native else ExitStack()
    with guard:
        messages = target.new_batch_write_builder().new_update().merge_into(
            source, on=['_ROW_ID'], when_matched=[WhenMatched.update({'keep': assignment})])
    expected = {'id': [1, 2, 3], 'value': [99, 20, 30], 'keep': [100, 200, 300],
                'extra': [None, None, 999]}
    if source_view == 'empty':
        assert messages == []
        assignment.assert_not_called()
    else:
        expected_values = ([99, 20, 30] if source_view == 'same-current' else
                           [10, 20] if source_view == 'same-captured' else [99, 20])
        assignment.assert_called_once()
        assert assignment.call_args[0][0]['s.value'].to_pylist() == expected_values
        assert messages
        baseline = current_snapshot.id if source_view == 'same-current' else snapshot_id
        assert all(message.check_from_snapshot == baseline for message in messages)
        _commit(latest, messages)
        expected['keep'] = expected_values + ([300] if len(expected_values) == 2 else [])
    assert _rows(latest) == expected
