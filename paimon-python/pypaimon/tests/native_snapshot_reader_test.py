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

"""Native streaming frames follow Java's per-snapshot reader modes."""

import asyncio
from contextlib import ExitStack
import json
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema


pytestmark = pytest.mark.native_plan
SCHEMA = pa.schema([('id', pa.int32()), ('value', pa.int32())])


def _table(tmp_path, engine='deduplicate', producer='none', buckets=1, extra=None, schema=SCHEMA):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('db', True)
    options = {
        'bucket': str(buckets), 'merge-engine': engine,
        'changelog-producer': producer, 'write.native.enabled': 'false',
        'source.split.target-size': '1 b', 'source.split.open-file-cost': '1 b',
    }
    options.update(extra or {})
    catalog.create_table('db.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'], options=options), False)
    return catalog.get_table('db.t')


def _write(table, rows, overwrite=False, schema=SCHEMA):
    builder = table.new_batch_write_builder()
    if overwrite:
        builder.overwrite()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    return table.snapshot_manager().get_latest_snapshot()


def _frame(table, snapshot, mode, native, bucket_filter=None, projection=None, predicate=None):
    builder = table.copy({
        'scan.native-plan.enabled': str(native).lower(),
        'read.native.enabled': str(native).lower(),
    }).new_stream_read_builder().with_include_row_kind()
    if bucket_filter is not None:
        builder.with_bucket_filter(bucket_filter)
    if projection is not None:
        builder.with_projection(projection)
    if predicate is not None:
        builder.with_filter(predicate)
    scan = builder.new_streaming_scan()
    try:
        with ExitStack() as stack:
            if native:
                stack.enter_context(patch.object(
                    scan, '_AsyncStreamingTableScan__create_initial_plan_raw',
                    side_effect=AssertionError('Python initial planning')))
                stack.enter_context(patch.object(
                    scan, '_create_plan_from_manifests',
                    side_effect=AssertionError('Python incremental planning')))
            plan = getattr(scan, '_create_{}_plan'.format(mode))(snapshot)
        reader = builder.new_read()
        with ExitStack() as stack:
            if native:
                stack.enter_context(patch.object(
                    reader, '_create_split_read', side_effect=AssertionError('Python read')))
            rows = reader.to_arrow(plan.splits()).to_pylist()
        assert plan.snapshot_id == snapshot.id
        if native:
            assert all(getattr(split, '_native_split', None) is not None for split in plan.splits())
        return plan, sorted(rows, key=lambda row: (row.get('id', 0), repr(row)))
    finally:
        if scan._prefetch_executor is not None:
            scan._prefetch_executor.shutdown(wait=True)


@pytest.mark.parametrize('engine', ['deduplicate', 'first-row'])
def test_initial_frame_includes_level_zero_and_pins_selected_snapshot(tmp_path, engine):
    table = _table(tmp_path, engine)
    _write(table, [{'id': 1, 'value': 10}, {'id': 2, 'value': 20}])
    selected = _write(table, [{'id': 1, 'value': 11}, {'id': 3, 'value': 30}])
    _write(table, [{'id': 4, 'value': 40}])
    expected = [
        {'_row_kind': '+I', 'id': 1, 'value': 10 if engine == 'first-row' else 11},
        {'_row_kind': '+I', 'id': 2, 'value': 20},
        {'_row_kind': '+I', 'id': 3, 'value': 30},
    ]
    for native in (False, True):
        plan, rows = _frame(table, selected, 'initial', native)
        assert rows == expected
        assert all(not split.is_streaming for split in plan.splits())


def test_overwrite_changelog_is_a_stream_frame_not_an_incremental_range(tmp_path):
    table = _table(tmp_path, producer='input')
    first = _write(table, [{'id': 1, 'value': 10}])
    selected = _write(table, [{'id': 2, 'value': 20}], overwrite=True)
    # The local writer does not publish an overwrite changelog. A fixture with
    # a real physical changelog list exercises producers which do; its rows
    # differ from the overwrite's delta data to catch reading the wrong list.
    path = table.snapshot_manager().get_snapshot_path(selected.id)
    with open(path) as reader:
        metadata = json.load(reader)
    metadata['changelogManifestList'] = first.changelog_manifest_list
    with open(path, 'w') as writer:
        json.dump(metadata, writer)
    selected = table.snapshot_manager().get_snapshot_by_id(selected.id)
    _write(table, [{'id': 3, 'value': 30}])
    assert selected.commit_kind == 'OVERWRITE'
    for native in (False, True):
        plan, rows = _frame(table, selected, 'changelog', native)
        assert rows == [{'_row_kind': '+I', 'id': 1, 'value': 10}]
        assert all(split.is_streaming for split in plan.splits())
        assert all(file.file_name.startswith('changelog-')
                   for split in plan.splits() for file in split.files)


@pytest.mark.parametrize('mode', ['initial', 'delta', 'changelog'])
@pytest.mark.parametrize('selected', [[], [0], [0, 2], [0, 1, 2, 3]])
def test_bucket_filter_is_applied_to_every_streaming_frame(tmp_path, mode, selected):
    table = _table(tmp_path, producer='input', buckets=4)
    _write(table, [{'id': i, 'value': i + 100} for i in range(32)])
    snapshot = _write(table, [{'id': i, 'value': i + 200} for i in range(32)])
    predicate = table.new_read_builder().new_predicate_builder().greater_than('id', 7)
    callback = lambda bucket: bucket in selected
    python_plan, expected = _frame(table, snapshot, mode, False, callback, predicate=predicate)
    native_plan, actual = _frame(table, snapshot, mode, True, callback, predicate=predicate)
    assert actual == expected
    assert all(split.bucket in selected for split in python_plan.splits() + native_plan.splits())
    if not selected:
        assert not actual and not native_plan.splits()
    elif len(selected) == 4:
        assert len(actual) == 24


@pytest.mark.parametrize('mode', ['initial', 'delta', 'changelog'])
def test_empty_selection_retains_snapshot_and_reader_schema(tmp_path, mode):
    table = _table(tmp_path, producer='input', buckets=4)
    snapshot = _write(table, [{'id': 1, 'value': 10}])
    for native in (False, True):
        plan, rows = _frame(table, snapshot, mode, native,
                            bucket_filter=lambda bucket: False, projection=['value'])
        assert plan.snapshot_id == snapshot.id
        assert rows == []


@pytest.mark.parametrize('mode', ['initial', 'delta', 'changelog'])
def test_bucket_callback_failure_is_not_retried_or_hidden(tmp_path, mode):
    table = _table(tmp_path, producer='input', buckets=4)
    snapshot = _write(table, [{'id': i, 'value': i + 100} for i in range(32)])
    error = LookupError('bucket callback failure')
    calls = []

    def callback(bucket):
        calls.append(bucket)
        if len(calls) == 2:
            raise error
        return True

    with pytest.raises(LookupError) as raised:
        _frame(table, snapshot, mode, True, bucket_filter=callback)
    assert raised.value is error
    assert len(calls) == 2


@pytest.mark.parametrize('mode', ['initial', 'delta', 'changelog'])
def test_projected_predicate_operand_stays_internal(tmp_path, mode):
    table = _table(tmp_path, producer='input')
    snapshot = _write(table, [{'id': 1, 'value': 10}, {'id': 2, 'value': 20}])
    predicate = table.new_read_builder().new_predicate_builder().greater_than('id', 1)
    for native in (False, True):
        _, rows = _frame(table, snapshot, mode, native, projection=['value'], predicate=predicate)
        assert rows == [{'_row_kind': '+I', 'value': 20}]


@pytest.mark.parametrize('merge_on_read', [False, True])
def test_initial_deletion_vector_table_keeps_uncompacted_level_zero(tmp_path, merge_on_read):
    table = _table(tmp_path, extra={
        'deletion-vectors.enabled': 'true',
        'deletion-vectors.merge-on-read': str(merge_on_read).lower(),
    })
    snapshot = _write(table, [{'id': 1, 'value': 10}, {'id': 2, 'value': 20}])
    snapshot = _write(table, [{'id': 1, 'value': 11}, {'id': 3, 'value': 30}])
    for native in (False, True):
        plan, rows = _frame(table, snapshot, 'initial', native)
        assert rows == [{'_row_kind': '+I', 'id': 1, 'value': 11},
                        {'_row_kind': '+I', 'id': 2, 'value': 20},
                        {'_row_kind': '+I', 'id': 3, 'value': 30}]
        assert all(file.level == 0 for split in plan.splits() for file in split.files)
        predicate = table.new_read_builder().new_predicate_builder().equal('value', 10)
        _, rows = _frame(table, snapshot, 'initial', native, predicate=predicate)
        assert rows == []


@pytest.mark.parametrize('producer', ['none', 'input'])
@pytest.mark.parametrize('mode', ['initial', 'delta', 'changelog'])
def test_postpone_pending_visibility_matches_java_producer_rule(tmp_path, producer, mode):
    table = _table(tmp_path, producer=producer, buckets=-2)
    snapshot = _write(table, [{'id': 1, 'value': 10}])
    expected = ([] if producer != 'none' or mode == 'changelog' else
                [{'_row_kind': '+I', 'id': 1, 'value': 10}])
    for native in (False, True):
        plan, rows = _frame(table, snapshot, mode, native)
        assert rows == expected
        if rows:
            assert all(split.bucket == -2 for split in plan.splits())


@pytest.mark.parametrize('mode', ['initial', 'delta', 'changelog'])
def test_snapshot_plan_retains_structured_read_type_for_nested_and_map_projection(tmp_path, mode):
    schema = pa.schema([
        ('id', pa.int32()),
        ('payload', pa.struct([('x', pa.int32()), ('unused', pa.string())])),
        ('attrs', pa.map_(pa.string(), pa.int32())),
    ])
    table = _table(tmp_path, producer='input', schema=schema)
    snapshot = _write(table, [
        {'id': 1, 'payload': {'x': 10, 'unused': 'drop'}, 'attrs': [('selected', 100), ('unused', 200)]},
        {'id': 2, 'payload': None, 'attrs': []},
    ], schema=schema)
    projection = {'leaf': 'payload.x', 'lookup': 'attrs["selected"]'}
    for native in (False, True):
        _, rows = _frame(table, snapshot, mode, native, projection=projection)
        assert rows == [{'_row_kind': '+I', 'leaf': 10, 'lookup': 100},
                        {'_row_kind': '+I', 'leaf': None, 'lookup': None}]


@pytest.mark.parametrize('mode', ['initial', 'delta', 'changelog'])
def test_zero_column_projection_keeps_frame_row_count(tmp_path, mode):
    table = _table(tmp_path, producer='input')
    snapshot = _write(table, [{'id': 1, 'value': 10}, {'id': 2, 'value': 20}])
    for native in (False, True):
        _, rows = _frame(table, snapshot, mode, native, projection=[])
        assert rows == [{'_row_kind': '+I'}, {'_row_kind': '+I'}]


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('initial', [False, True])
def test_stream_callback_failure_does_not_advance_or_stage_consumer(tmp_path, native, initial):
    from pypaimon.read.streaming_table_scan import AsyncStreamingTableScan

    table = _table(tmp_path)
    _write(table, [{'id': 1, 'value': 10}])
    table = table.copy({'scan.native-plan.enabled': str(native).lower(),
                        'read.native.enabled': str(native).lower()})
    failure = LookupError('retry selected snapshot')
    calls = []

    def callback(bucket):
        calls.append(bucket)
        if len(calls) == 1:
            raise failure
        return True

    scan = AsyncStreamingTableScan(
        table, bucket_filter=callback, prefetch_enabled=False, consumer_id='failed-frame')
    if not initial:
        scan.next_snapshot_id = 1

    async def scenario():
        failed_stream = scan.stream()
        with pytest.raises(LookupError) as caught:
            await failed_stream.__anext__()
        assert caught.value is failure
        assert scan.next_snapshot_id == (None if initial else 1)
        assert scan._pending_consumer_snapshot is None
        assert scan._consumer_manager.consumer('failed-frame') is None
        _write(table, [{'id': 2, 'value': 20}])
        retry = scan.stream()
        try:
            plan = await retry.__anext__()
            expected_id = 2 if initial else 1
            assert plan.snapshot_id == expected_id
            builder = table.new_stream_read_builder().with_include_row_kind()
            rows = builder.new_read().to_arrow(plan.splits()).to_pylist()
            expected = [{'_row_kind': '+I', 'id': 1, 'value': 10}]
            if initial:
                expected.append({'_row_kind': '+I', 'id': 2, 'value': 20})
            assert sorted(rows, key=lambda row: row['id']) == expected
            assert scan.next_snapshot_id == expected_id + 1
            assert scan._pending_consumer_snapshot == expected_id + 1
            # Consumer progress waits for the caller to process the yielded plan.
            assert scan._consumer_manager.consumer('failed-frame') is None
        finally:
            await retry.aclose()
            await failed_stream.aclose()

    asyncio.run(scenario())
