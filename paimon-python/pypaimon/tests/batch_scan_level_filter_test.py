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

"""Java AbstractBatchTableScan L0 filtering, including native planning."""

import json
from dataclasses import replace
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.read.native_plan import native_runtime_available


SCHEMA = pa.schema([('id', pa.int64()), ('v', pa.string())])


@pytest.fixture(params=[False, pytest.param(
    True, marks=[pytest.mark.native_plan, pytest.mark.skipif(
        not native_runtime_available(), reason='native planning required')])],
    ids=['python', 'native'])
def native(request):
    return request.param


def _table(tmp_path, engine, dv, mor):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        SCHEMA, primary_keys=['id'], options={
            'bucket': '1', 'merge-engine': engine,
            'deletion-vectors.enabled': str(dv).lower(),
            'deletion-vectors.merge-on-read': str(mor).lower(),
            'source.split.target-size': '1 b',
            'source.split.open-file-cost': '1 b',
        }), False)
    return catalog.get_table('default.t')


def _write(table, rows, timestamp, level=0, overwrite=False):
    builder = table.new_batch_write_builder()
    if overwrite:
        builder.overwrite()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=SCHEMA))
        messages = writer.prepare_commit()
        # A unique-key file marked L1 models the output of Java compaction.
        # Subsequent unmerged updates stay at L0.
        for message in messages:
            message.new_files = [replace(file, level=level) for file in message.new_files]
        commit.commit(messages)
    finally:
        writer.close()
        commit.close()
    manager = table.snapshot_manager()
    path = manager.get_snapshot_path(manager.get_latest_snapshot().id)
    snapshot = json.loads(table.file_io.read_file_utf8(path))
    snapshot['timeMillis'] = timestamp
    table.file_io.write_file(path, json.dumps(snapshot), overwrite=True)


def _read(table, native, predicate=None):
    builder = table.copy({'scan.native-plan.enabled': str(native).lower()}).new_read_builder()
    if predicate is not None:
        builder.with_filter(predicate)
    scan = builder.new_scan()
    if native:
        with patch.object(scan.file_scanner, 'scan', side_effect=AssertionError('native fallback')):
            plan = scan.plan()
    else:
        plan = scan.plan()
    return plan, sorted(builder.new_read().to_arrow(plan.splits()).to_pylist(), key=lambda row: row['id'])


@pytest.mark.parametrize('engine,dv,mor,skip_l0', [
    ('first-row', False, False, True),
    ('deduplicate', True, False, True),
    ('deduplicate', True, True, False),
    ('deduplicate', False, False, False),
])
def test_batch_scan_level_filter_and_value_predicate(tmp_path, native, engine, dv, mor, skip_l0):
    table = _table(tmp_path, engine, dv, mor)
    _write(table, [{'id': 1, 'v': 'base'}], 100, level=1)
    _write(table, [{'id': 1, 'v': 'updated'}, {'id': 2, 'v': 'new'}], 200)
    expected = ([{'id': 1, 'v': 'base'}] if skip_l0 else
                [{'id': 1, 'v': 'updated'}, {'id': 2, 'v': 'new'}])
    plan, rows = _read(table, native)
    assert rows == expected
    assert plan.snapshot_id == 2
    assert sum(len(split.files) for split in plan.splits()) == (1 if skip_l0 else 2)
    pb = table.new_read_builder().new_predicate_builder()
    for value in ('base', 'updated', 'absent'):
        _, rows = _read(table, native, pb.equal('v', value))
        assert rows == [row for row in expected if row['v'] == value]
    assert _read(table.copy({'scan.snapshot-id': '1'}), native)[1] == [{'id': 1, 'v': 'base'}]


@pytest.mark.parametrize('engine,dv', [('first-row', False), ('deduplicate', True)])
def test_write_and_incremental_scans_keep_l0(tmp_path, engine, dv):
    table = _table(tmp_path, engine, dv, False).copy({'scan.native-plan.enabled': 'false'})
    _write(table, [{'id': 1, 'v': 'base'}], 100, level=1)
    _write(table, [{'id': 1, 'v': 'updated'}, {'id': 2, 'v': 'new'}], 200)
    scan = table.new_read_builder().new_scan()
    assert sum(len(split.files) for split in scan.plan().splits()) == 1
    assert sum(len(split.files) for split in scan.plan_for_write().splits()) == 2
    assert sum(len(split.files) for split in scan.plan().splits()) == 1
    incremental = table.copy({'incremental-between-timestamp': '100,200'})
    assert _read(incremental, False)[1] == [{'id': 1, 'v': 'updated'}, {'id': 2, 'v': 'new'}]


@pytest.mark.parametrize('override', [None, 'false'])
def test_copy_can_disable_persisted_merge_on_read(tmp_path, native, override):
    table = _table(tmp_path, 'deduplicate', True, True)
    _write(table, [{'id': 1, 'v': 'base'}], 100, level=1)
    _write(table, [{'id': 1, 'v': 'updated'}], 200)
    assert _read(table, native)[1] == [{'id': 1, 'v': 'updated'}]
    table = table.copy({'deletion-vectors.merge-on-read': override})
    assert _read(table, native)[1] == [{'id': 1, 'v': 'base'}]


def test_first_row_streaming_and_overwrite_see_uncompacted_files(tmp_path):
    from pypaimon.read.streaming_table_scan import AsyncStreamingTableScan

    table = _table(tmp_path, 'first-row', False, False)
    _write(table, [{'id': 1, 'v': 'before'}], 100)
    assert _read(table, False)[1] == []
    stream = AsyncStreamingTableScan(table, prefetch_enabled=False)
    plan = stream._create_initial_plan(table.snapshot_manager().get_latest_snapshot())
    assert sum(len(split.files) for split in plan.splits()) == 1
    assert table.new_read_builder().new_read().to_arrow(plan.splits()).to_pylist() == [
        {'id': 1, 'v': 'before'}]

    _write(table, [{'id': 2, 'v': 'after'}], 200, overwrite=True)
    builder = table.new_read_builder()
    plan = builder.new_scan().plan_for_write()
    assert sum(len(split.files) for split in plan.splits()) == 1
    assert builder.new_read().to_arrow(plan.splits()).to_pylist() == [{'id': 2, 'v': 'after'}]
