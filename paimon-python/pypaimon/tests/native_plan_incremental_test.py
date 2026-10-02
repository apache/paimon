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

"""Compare committed timestamp windows, including cross-snapshot change events."""

import json
import os
from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.common.identifier import Identifier
from pypaimon.read.native_plan import native_method_available
from pypaimon.read.split import DataSplit
from pypaimon.utils.range import Range


@pytest.fixture(params=[False, pytest.param(
    True, marks=[pytest.mark.native_plan, pytest.mark.skipif(
        not native_method_available('Split', 'is_streaming'),
        reason='pypaimon_rust combined incremental planning API required')])],
    ids=['python', 'native'])
def native(request):
    return request.param


@pytest.fixture
def catalog(tmp_path):
    result = CatalogFactory.create({'warehouse': str(tmp_path)})
    result.create_database('default', True)
    return result


SCHEMA = pa.schema([('k', pa.int64()), ('v', pa.string())])


def _table(catalog, name, primary_key=False, options=None):
    catalog.create_table('default.' + name, Schema.from_pyarrow_schema(
        SCHEMA, primary_keys=['k'] if primary_key else None,
        options=options), False)
    return catalog.get_table('default.' + name)


def _set_time(table, timestamp):
    manager = table.snapshot_manager()
    snapshot = manager.get_latest_snapshot()
    path = manager.get_snapshot_path(snapshot.id)
    with open(path) as reader:
        data = json.load(reader)
    data['timeMillis'] = timestamp
    with open(path, 'w') as writer:
        json.dump(data, writer)
    return snapshot.id


def _write(table, timestamp, rows, overwrite=False):
    builder = table.new_batch_write_builder()
    if overwrite:
        builder.overwrite()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pydict({
            key: [row[key] for row in rows] for key in SCHEMA.names}, schema=SCHEMA))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    return _set_time(table, timestamp)


def _read(table, native, window, predicate=None, limit=None, shard=None,
          slice_=None, row_ranges=None, with_stats=False):
    builder = table.copy({
        'scan.native-plan.enabled': str(native).lower(),
        'read.native.enabled': str(native).lower(),
        'scan.mode': 'incremental',
        'incremental-between-timestamp': '%s,%s' % window,
    }).new_read_builder()
    if predicate is not None:
        builder.with_filter(predicate)
    if limit is not None:
        builder.with_limit(limit)
    scan = builder.new_scan()
    if shard is not None:
        scan.with_shard(*shard)
    if slice_ is not None:
        scan.with_slice(*slice_)
    if row_ranges is not None:
        scan.with_row_ranges(row_ranges)
    with ExitStack() as stack:
        if native:
            for method in ('scan', 'scan_with_stats'):
                stack.enter_context(patch.object(
                    scan.file_scanner, method, side_effect=AssertionError(
                        'incremental native plan fell back to Python')))
        plan = scan.scan_with_stats()[0] if with_stats else scan.plan()
    with ExitStack() as stack:
        if native:
            assert all(
                getattr(split, '_native_split', None) is not None
                for split in plan.splits())
            stack.enter_context(patch(
                'pypaimon.read.table_read.TableRead._create_split_read',
                side_effect=AssertionError(
                    'incremental native read fell back to Python')))
        result = builder.new_read().to_arrow(
            plan.splits(), parallelism=1).to_pydict()
    return plan, [dict(zip(result, row)) for row in zip(*result.values())]


@pytest.fixture
def history(catalog):
    table = _table(catalog, 'history', True, {'bucket': '1'})
    _write(table, 100, [{'k': 1, 'v': 'base'}, {'k': 2, 'v': 'base'}])
    _write(table, 200, [{'k': 1, 'v': 'intermediate'}, {'k': 3, 'v': 'third'}])
    _write(table, 200, [{'k': 1, 'v': 'latest'}, {'k': 4, 'v': 'fourth'}])
    _write(table, 300, [{'k': 9, 'v': 'overwrite'}], overwrite=True)
    assert table.snapshot_manager().get_latest_snapshot().commit_kind == 'OVERWRITE'
    _write(table, 400, [{'k': 5, 'v': 'fifth'}])
    return table


@pytest.mark.parametrize('window,snapshot_id,expected', [
    ((100, 200), 3, [(1, 'intermediate'), (1, 'latest'), (3, 'third'), (4, 'fourth')]),
    ((200, 300), 4, []),
    ((100, 300), 4, [(1, 'intermediate'), (1, 'latest'), (3, 'third'), (4, 'fourth')]),
    ((100, 150), 1, []),
    ((500, 600), None, []),
    ((0, 50), None, []),
    ((0, 100), 1, [(1, 'base'), (2, 'base')]),
    ((200, 400), 5, [(5, 'fifth')]),
])
def test_timestamp_windows_preserve_append_events_and_end_snapshot(
        native, history, window, snapshot_id, expected):
    plan, rows = _read(history, native, window)
    assert plan.snapshot_id == snapshot_id
    assert sorted((row['k'], row['v']) for row in rows) == expected
    assert all(split.snapshot_id == snapshot_id for split in plan.splits())


def test_predicate_can_select_an_earlier_event(native, history):
    predicate = history.new_read_builder().new_predicate_builder().equal('v', 'intermediate')
    plan, rows = _read(history, native, (100, 200), predicate=predicate, with_stats=True)
    assert plan.snapshot_id == 3
    assert all(split.snapshot_id == plan.snapshot_id for split in plan.splits())
    assert rows == [{'k': 1, 'v': 'intermediate'}]


def test_append_distribution_precedes_limit_in_incremental_window(native, catalog):
    table = _table(catalog, 'append', options={'source.split.target-size': '1b'})
    rows = [{'k': key, 'v': str(key)} for key in range(12)]
    for offset in range(0, 12, 3):
        _write(table, (offset // 3 + 1) * 100, rows[offset:offset + 3])
    for options in ({'shard': (1, 3)}, {'slice_': (3, 8)}):
        plan, actual = _read(table, native, (100, 400), limit=2, **options)
        assert plan.snapshot_id == 4
        assert actual == rows[6:8]


def test_primary_key_shards_preserve_all_selected_commits(native, catalog):
    table = _table(catalog, 'pk_shards', True, {'bucket': '4'})
    rows = [{'k': key, 'v': 'base'} for key in range(20)]
    _write(table, 100, rows)
    _write(table, 200, [dict(row, v='old') for row in rows])
    _write(table, 300, [dict(row, v='new') for row in rows])
    result = []
    for shard in range(4):
        plan, actual = _read(table, native, (100, 300), shard=(shard, 4))
        assert plan.snapshot_id == 3
        assert all(split.bucket % 4 == shard for split in plan.splits())
        result.extend(actual)
        if actual:
            _, limited = _read(table, native, (100, 300), shard=(shard, 4), limit=1)
            assert len(limited) == 1
            assert limited[0] in actual
    assert sorted((row['k'], row['v']) for row in result) == [
        (key, version) for key in range(20) for version in ('new', 'old')]


def test_data_evolution_positions_intersect_incremental_row_ranges(native, catalog):
    table = _table(catalog, 'de', options={
        'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
        'source.split.target-size': '1b',
    })
    rows = [{'k': key, 'v': str(key)} for key in range(9)]
    for offset in range(0, 9, 3):
        _write(table, (offset // 3 + 1) * 100, rows[offset:offset + 3])
    plan, actual = _read(table, native, (100, 300), slice_=(1, 5),
                         row_ranges=[Range(5, 7)], limit=2)
    assert plan.snapshot_id == 3
    assert actual == rows[5:7]
    for selection in ({'slice_': (3, 6)}, {'shard': (1, 2)}):
        plan, actual = _read(table, native, (100, 300),
                             row_ranges=[Range(7, 7)], **selection)
        assert plan.snapshot_id == 3
        assert actual == rows[7:8]
    _, actual = _read(table, native, (100, 300), slice_=(0, 1),
                      row_ranges=[Range(5, 7)])
    assert actual == []


def test_incremental_branch_uses_its_own_snapshot_history(native, catalog):
    table = _table(catalog, 'branch')
    _write(table, 100, [{'k': 1, 'v': 'base'}])
    table.create_tag('base')
    catalog.create_branch(table.identifier, 'test', tag_name='base')
    branch = catalog.get_table(Identifier('default', 'branch', branch='test'))
    _write(branch, 200, [{'k': 2, 'v': 'branch'}])
    _write(table, 300, [{'k': 3, 'v': 'main'}])
    plan, actual = _read(branch, native, (100, 300))
    assert plan.snapshot_id == 2
    assert actual == [{'k': 2, 'v': 'branch'}]


def test_incremental_ignores_deletion_vectors_from_window_end(native, catalog):
    table = _table(catalog, 'deletions', options={
        'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
        'deletion-vectors.enabled': 'true', 'index-file-in-data-file-dir': 'true',
    })
    rows = [{'k': key, 'v': str(key)} for key in range(6)]
    _write(table, 100, rows[:3])
    _write(table, 200, rows[3:])
    builder = table.new_batch_write_builder()
    messages = builder.new_update().delete_by_row_id([1, 4])
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    assert _set_time(table, 300) == 3

    historical, actual = _read(table, native, (100, 200))
    assert historical.snapshot_id == 2
    assert actual == rows[3:]
    current, actual = _read(table, native, (100, 300))
    assert current.snapshot_id == 3
    assert actual == rows[3:]
    _, actual = _read(table, native, (100, 300), slice_=(1, 3), limit=1)
    assert actual == [rows[4]]


@pytest.mark.parametrize('window', ['100,100', '200,100', '100', 'one,200'])
def test_invalid_timestamp_window_is_rejected_even_for_empty_tables(catalog, window):
    table = _table(catalog, 'invalid')
    with pytest.raises(ValueError):
        table.copy({'incremental-between-timestamp': window}).new_read_builder().new_scan()


@pytest.mark.parametrize('engine,dv', [
    ('deduplicate', False), ('deduplicate', True),
    ('partial-update', False), ('first-row', False)])
def test_incremental_includes_l0_across_merge_engines(catalog, native, engine, dv):
    table = _table(catalog, 'engines', True, {
        'bucket': '1', 'merge-engine': engine,
        'deletion-vectors.enabled': str(dv).lower(), 'source.split.target-size': '1b'})
    _write(table, 100, [{'k': 1, 'v': 'old'}])
    _write(table, 200, [{'k': 1, 'v': 'new'}])
    for value in (None, 'old', 'new'):
        predicate = table.new_read_builder().new_predicate_builder().equal('v', value) \
            if value is not None else None
        plan, rows = _read(table, native, (0, 200), predicate=predicate,
                           limit=1 if value else None)
        assert all(s.is_streaming and not s.data_deletion_files for s in plan.splits())
        assert sorted(row['v'] for row in rows) == ([value] if value else ['new', 'old'])


def test_incremental_does_not_evaluate_endpoint_global_indexes(catalog, native):
    table = _table(catalog, 'endpoint_index', options={
        'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
        'deletion-vectors.enabled': 'true'})
    _write(table, 100, [{'k': 1, 'v': 'old'}])
    _write(table, 200, [{'k': 2, 'v': 'new'}])
    path = table.snapshot_manager().get_snapshot_path(2)
    data = json.loads(table.file_io.read_file_utf8(path))
    data['indexManifest'] = 'missing-endpoint-index'
    table.file_io.write_file(path, json.dumps(data), overwrite=True)
    predicate = table.new_read_builder().new_predicate_builder().equal('v', 'old')
    _, rows = _read(table, native, (0, 200), predicate=predicate)
    assert rows == [{'k': 1, 'v': 'old'}]


def test_incremental_rejects_manifest_delete_before_reconciliation(catalog, native):
    from pypaimon.read.streaming_table_scan import AsyncStreamingTableScan
    table = _table(catalog, 'invalid_delta', True)
    _write(table, 100, [{'k': 1, 'v': 'old'}])
    _write(table, 200, [{'k': 1, 'v': 'new'}], overwrite=True)
    path = table.snapshot_manager().get_snapshot_path(2)
    data = json.loads(table.file_io.read_file_utf8(path))
    data['commitKind'] = 'APPEND'
    table.file_io.write_file(path, json.dumps(data), overwrite=True)
    # Allow fallback here: both implementations must reject the malformed input.
    table = table.copy({'scan.native-plan.enabled': str(native).lower(),
                        'incremental-between-timestamp': '0,200'})
    with pytest.raises(ValueError, match='only ADD'):
        table.new_read_builder().new_scan().plan()
    scan = AsyncStreamingTableScan(table, prefetch_enabled=False)
    with pytest.raises(ValueError, match='only ADD'):
        scan._create_delta_plan(table.snapshot_manager().get_snapshot_by_id(2))


def test_incremental_reader_preserves_all_physical_row_kinds(catalog, native):
    import pyarrow.parquet as pq
    from pypaimon.read.streaming_table_scan import AsyncStreamingTableScan
    table = _table(catalog, 'row_kinds', True, {'bucket': '1'})
    scan = AsyncStreamingTableScan(table, prefetch_enabled=False)
    expected = []
    for kind in range(4):
        snapshot_id = _write(table, (kind + 1) * 100, [{'k': 1, 'v': str(kind)}])
        plan = scan._create_delta_plan(table.snapshot_manager().get_snapshot_by_id(snapshot_id))
        file = plan.splits()[0].files[0]
        physical = pq.read_table(file.file_path)
        index = physical.schema.get_field_index('_VALUE_KIND')
        physical = physical.set_column(index, physical.schema.field(index), pa.array([kind], pa.int8()))
        pq.write_table(physical, file.file_path)
        rows = list(table.new_read_builder().new_read().to_iterator(plan.splits()))
        assert [(row.get_field(1), row.get_row_kind().value) for row in rows] == [(str(kind), kind)]
        assert plan.snapshot_id == snapshot_id
        expected.append((str(kind), kind))
    builder = table.copy({'scan.native-plan.enabled': str(native).lower(),
                          'read.native.enabled': str(native).lower(),
                          'incremental-between-timestamp': '0,400'}).new_read_builder()
    batch_scan = builder.new_scan()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch.object(batch_scan.file_scanner, 'scan',
                                             side_effect=AssertionError('native fallback')))
    plan = batch_scan.plan()
    if native:
        # The fixture rewrites Parquet bytes after commit to manufacture all
        # four physical kinds. Refresh the opaque split's file-size metadata;
        # real committed files already have matching manifest sizes.
        from pypaimon_rust.datafusion import Split as NativeSplit
        for split in plan.splits():
            _, (state,) = split._native_split.__reduce__()
            native_state = json.loads(bytes(state))
            for native_file, data_file in zip(
                    native_state['data_files'], split.files):
                native_file['_FILE_SIZE'] = os.path.getsize(
                    data_file.file_path)
            split._native_split = NativeSplit(
                json.dumps(native_state).encode())
    rows = list(builder.new_read().to_iterator(plan.splits()))
    assert sorted((row.get_field(1), row.get_row_kind().value) for row in rows) == expected
    if native:
        read = builder.new_read()
        read.include_row_kind = True
        with patch(
                'pypaimon.read.table_read.TableRead._create_split_read',
                side_effect=AssertionError('native row-kind read fell back')):
            arrow = read.to_arrow(plan.splits(), parallelism=2)
        assert sorted(zip(
            arrow.column('v').to_pylist(),
            arrow.column('_row_kind').to_pylist(),
        )) == [('0', '+I'), ('1', '-U'), ('2', '+U'), ('3', '-D')]
    # Initial streaming bootstrap remains a merged snapshot, where the last -D removes the key.
    initial = scan._create_initial_plan(table.snapshot_manager().get_snapshot_by_id(4))
    assert all(not split.is_streaming for split in initial.splits())
    assert list(table.new_read_builder().new_read().to_iterator(initial.splits())) == []


@pytest.mark.native_plan
@pytest.mark.skipif(
    not native_method_available('ReadBuilder', 'with_nested_projection'),
    reason='pypaimon_rust nested native reader API required')
def test_stream_read_builder_combines_native_nested_projection_and_row_kind(
        catalog):
    from pypaimon.read.streaming_table_scan import AsyncStreamingTableScan

    schema = pa.schema([
        ('k', pa.int64()),
        ('payload', pa.struct([
            ('score', pa.int32()),
            ('ignored', pa.string()),
        ])),
    ])
    catalog.create_table(
        'default.stream_nested',
        Schema.from_pyarrow_schema(schema, options={'bucket': '-1'}),
        False,
    )
    table = catalog.get_table('default.stream_nested')
    write_builder = table.new_batch_write_builder()
    writer, commit = write_builder.new_write(), write_builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist([
            {'k': 1, 'payload': {'score': 10, 'ignored': 'a'}},
            {'k': 2, 'payload': None},
        ], schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()

    native_table = table.copy({
        'scan.native-plan.enabled': 'true',
        'read.native.enabled': 'true',
    })
    builder = (native_table.new_stream_read_builder()
               .with_projection(['payload.score', 'k'])
               .with_include_row_kind())
    # Use the same delta-plan primitive as the streaming loop without polling.
    scan = builder.new_streaming_scan()
    assert isinstance(scan, AsyncStreamingTableScan)
    snapshot = native_table.snapshot_manager().get_latest_snapshot()
    plan = scan._create_delta_plan(snapshot)
    with patch(
            'pypaimon.read.table_read.TableRead._create_split_read',
            side_effect=AssertionError('streaming nested native read fell back')):
        rows = builder.new_read().to_arrow(plan.splits()).to_pylist()

    assert rows == [
        {'_row_kind': '+I', 'payload_score': 10, 'k': 1},
        {'_row_kind': '+I', 'payload_score': None, 'k': 2},
    ]


@pytest.mark.native_plan
def test_streaming_changelog_frames_use_native_plan_and_read(catalog):
    import asyncio

    table = _table(catalog, 'native_changelog', True, {
        'bucket': '1',
        'changelog-producer': 'input',
        'source.split.target-size': '1 b',
        'source.split.open-file-cost': '1 b',
    })
    _write(table, 100, [{'k': 1, 'v': 'a'}, {'k': 2, 'v': 'b'}])
    _write(table, 200, [{'k': 3, 'v': 'c'}])

    native_table = table.copy({
        'scan.native-plan.enabled': 'true',
        'read.native.enabled': 'true',
    })
    predicate = (native_table.new_read_builder().new_predicate_builder()
                 .greater_than('k', 1))
    builder = (native_table.new_stream_read_builder()
               .with_filter(predicate)
               .with_projection(['v'])
               .with_include_row_kind())
    scan = builder.new_streaming_scan()
    scan.next_snapshot_id = 1

    async def first_two_frames():
        plans = []
        async for plan in scan.stream():
            plans.append(plan)
            if len(plans) == 2:
                return plans

    with patch.object(
            scan, '_create_plan_from_manifests',
            side_effect=AssertionError(
                'streaming changelog native plan fell back to Python')):
        plans = asyncio.run(first_two_frames())

    assert [plan.snapshot_id for plan in plans] == [1, 2]
    assert all(
        getattr(split, '_native_split', None) is not None
        for plan in plans for split in plan.splits())
    assert all(
        file.file_name.startswith('changelog-')
        for plan in plans for split in plan.splits() for file in split.files)

    with patch(
            'pypaimon.read.table_read.TableRead._create_split_read',
            side_effect=AssertionError(
                'streaming changelog native read fell back to Python')):
        rows = [
            row
            for plan in plans
            for row in builder.new_read().to_arrow(
                plan.splits(), parallelism=2).to_pylist()
        ]
    assert rows == [
        {'_row_kind': '+I', 'v': 'b'},
        {'_row_kind': '+I', 'v': 'c'},
    ]


def test_streaming_overwrite_is_skipped_by_default_like_java(catalog):
    import asyncio

    table = _table(catalog, 'native_overwrite_changelog', True, {
        'bucket': '1',
        'changelog-producer': 'input',
    })
    _write(table, 100, [{'k': 1, 'v': 'before'}])
    _write(table, 200, [{'k': 2, 'v': 'after'}], overwrite=True)
    overwrite = table.snapshot_manager().get_latest_snapshot()
    assert overwrite.commit_kind == 'OVERWRITE'
    assert overwrite.changelog_manifest_list is None
    _write(table, 300, [{'k': 3, 'v': 'next'}])

    builder = (table.new_stream_read_builder()
               .with_projection(['v'])
               .with_include_row_kind())
    scan = builder.new_streaming_scan()
    scan.next_snapshot_id = overwrite.id

    async def next_plan():
        async for plan in scan.stream():
            return plan

    plan = asyncio.run(next_plan())
    assert plan.snapshot_id == 3
    assert builder.new_read().to_arrow(plan.splits()).to_pylist() == [
        {'_row_kind': '+I', 'v': 'next'}]


def test_streaming_reader_honors_explicit_split_deletion_vector(catalog, native, tmp_path):
    from pypaimon.deletionvectors.bitmap_deletion_vector import BitmapDeletionVector
    from pypaimon.table.source.deletion_file import DeletionFile
    table = _table(catalog, 'explicit_dv', True, {'bucket': '1'})
    _write(table, 100, [{'k': key, 'v': str(key)} for key in (1, 2, 3)])
    plan, rows = _read(table, native, (0, 100))
    assert len(rows) == 3
    vector = BitmapDeletionVector()
    vector.delete(1)
    encoded = vector.serialize()
    path = tmp_path / 'explicit-dv'
    path.write_bytes(encoded)
    assert len(plan.splits()) == 1
    split = plan.splits()[0]
    assert split.is_streaming
    # The planner cannot attach an endpoint DV. Build the reader input split
    # with that DV instead of mutating a previously planned (and cached) split.
    dv_split = DataSplit(
        files=split.files,
        partition=split.partition,
        bucket=split.bucket,
        raw_convertible=split.raw_convertible,
        data_deletion_files=[DeletionFile(str(path), 0, len(encoded) - 8, 1)],
        snapshot_id=split.snapshot_id,
        is_streaming=split.is_streaming,
        bucket_path=split.bucket_path,
        total_buckets=split.total_buckets,
    )
    read_table = table.copy({'read.native.enabled': str(native).lower()})
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch(
                'pypaimon.read.table_read.TableRead._create_split_read',
                side_effect=AssertionError('explicit DV native read fell back')))
        result = read_table.new_read_builder().new_read().to_arrow([dv_split]).to_pylist()
    assert result == [{'k': 1, 'v': '1'}, {'k': 3, 'v': '3'}]
