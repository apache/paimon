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

"""Compare committed timestamp windows, including cross-snapshot PK merges."""

import json
from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.common.identifier import Identifier
from pypaimon.read.native_plan import native_method_available
from pypaimon.utils.range import Range


@pytest.fixture(params=[False, pytest.param(
    True, marks=[pytest.mark.native_plan, pytest.mark.skipif(
        not native_method_available('ReadBuilder', 'new_incremental_scan'),
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
    result = builder.new_read().to_arrow(plan.splits(), parallelism=1).to_pydict()
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
    ((100, 200), 3, [(1, 'latest'), (3, 'third'), (4, 'fourth')]),
    ((200, 300), 4, []),
    ((100, 300), 4, [(1, 'latest'), (3, 'third'), (4, 'fourth')]),
    ((100, 150), 1, []),
    ((500, 600), None, []),
    ((0, 50), None, []),
    ((0, 100), 1, [(1, 'base'), (2, 'base')]),
    ((200, 400), 5, [(5, 'fifth')]),
])
def test_timestamp_windows_merge_appends_and_preserve_end_snapshot(
        native, history, window, snapshot_id, expected):
    plan, rows = _read(history, native, window)
    assert plan.snapshot_id == snapshot_id
    assert sorted((row['k'], row['v']) for row in rows) == expected
    if native:
        assert all(split.snapshot_id == snapshot_id for split in plan.splits())


def test_predicate_does_not_resurrect_an_earlier_version(native, history):
    predicate = history.new_read_builder().new_predicate_builder().equal('v', 'intermediate')
    plan, rows = _read(history, native, (100, 200), predicate=predicate, with_stats=True)
    assert plan.snapshot_id == 3
    assert rows == []


def test_append_distribution_precedes_limit_in_incremental_window(native, catalog):
    table = _table(catalog, 'append', options={'source.split.target-size': '1b'})
    rows = [{'k': key, 'v': str(key)} for key in range(12)]
    for offset in range(0, 12, 3):
        _write(table, (offset // 3 + 1) * 100, rows[offset:offset + 3])
    for options in ({'shard': (1, 3)}, {'slice_': (3, 8)}):
        plan, actual = _read(table, native, (100, 400), limit=2, **options)
        assert plan.snapshot_id == 4
        assert actual == rows[6:8]


def test_primary_key_shards_merge_all_selected_commits(native, catalog):
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
        (key, 'new') for key in range(20)]


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


def test_incremental_uses_deletion_vectors_from_window_end(native, catalog):
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
    assert actual == [rows[3], rows[5]]
    _, actual = _read(table, native, (100, 300), slice_=(1, 3), limit=1)
    assert actual == [rows[5]]


@pytest.mark.parametrize('window', ['100,100', '200,100', '100', 'one,200'])
def test_invalid_timestamp_window_is_rejected_even_for_empty_tables(catalog, window):
    table = _table(catalog, 'invalid')
    with pytest.raises(ValueError):
        table.copy({'incremental-between-timestamp': window}).new_read_builder().new_scan()
