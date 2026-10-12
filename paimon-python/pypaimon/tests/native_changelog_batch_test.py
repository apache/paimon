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

"""Java batch changelog packing and sharding through the Native planner."""

import json
from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.read.scan_distribution import java_file_name_shard
from pypaimon.schema.data_types import AtomicType, PyarrowFieldParser
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.internal_row import RowKind


pytestmark = pytest.mark.native_plan
SCHEMA = pa.schema([('key', pa.int64()), ('value', pa.int64())])


@pytest.fixture(params=[False, True], ids=['python', 'native'])
def native(request):
    return request.param


@pytest.fixture
def catalog(tmp_path):
    result = CatalogFactory.create({'warehouse': str(tmp_path)})
    result.create_database('default', True)
    return result


def _table(catalog, options=None, schema=SCHEMA, partitions=None):
    config = {
        'bucket': '4', 'changelog-producer': 'input',
        'write.native.enabled': 'false',
        'source.split.target-size': '1mb', 'source.split.open-file-cost': '1b',
    }
    config.update(options or {})
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, primary_keys=list(partitions or []) + ['key'],
        partition_keys=partitions, options=config), False)
    return catalog.get_table('default.t')


def _patch_snapshot(table, snapshot_id, **changes):
    path = table.snapshot_manager().get_snapshot_path(snapshot_id)
    data = json.loads(table.file_io.read_file_utf8(path))
    data.update(changes)
    table.file_io.write_file(path, json.dumps(data), overwrite=True)


def _write(table, timestamp, rows, kind=None, overwrite=False):
    builder = table.new_batch_write_builder()
    if overwrite:
        builder.overwrite()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        if kind is None:
            # Use the declared Arrow types even for NULL nested values.
            schema = PyarrowFieldParser.from_paimon_schema(table.fields)
            writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
        else:
            for row in rows:
                writer.write_row(GenericRow(
                    [row[field.name] for field in table.fields], table.fields, kind))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    snapshot_id = table.snapshot_manager().get_latest_snapshot().id
    _patch_snapshot(table, snapshot_id, timeMillis=timestamp)
    return snapshot_id


def _read(table, native, window=(0, 300), shard=None, predicate=None,
          projection=None, limit=None, with_stats=False):
    builder = table.copy({
        'scan.native-plan.enabled': str(native).lower(),
        'read.native.enabled': str(native).lower(),
        'scan.mode': 'incremental',
        'incremental-between-timestamp': '%s,%s' % window,
    }).new_read_builder()
    if predicate is not None:
        builder.with_filter(predicate)
    if projection is not None:
        builder.with_projection(projection)
    if limit is not None:
        builder.with_limit(limit)
    scan = builder.new_scan()
    if shard is not None:
        scan.with_shard(*shard)
    with ExitStack() as stack:
        if native:
            for method in ('scan', 'scan_with_stats'):
                stack.enter_context(patch.object(
                    scan.file_scanner, method,
                    side_effect=AssertionError('batch changelog planning used Python')))
        plan = scan.scan_with_stats()[0] if with_stats else scan.plan()
    read = builder.new_read()
    read.include_row_kind = True
    with ExitStack() as stack:
        if native:
            assert all(getattr(split, '_native_split', None) is not None for split in plan.splits())
            stack.enter_context(patch.object(
                read, '_create_split_read',
                side_effect=AssertionError('batch changelog reading used Python')))
        rows = read.to_arrow(plan.splits(), parallelism=1).to_pylist()
    return plan, rows


@pytest.mark.parametrize('bucket', ['1', '4', '-1'])
@pytest.mark.parametrize('dv', [False, True])
def test_changelog_shards_cover_all_events_and_pack_the_whole_window(catalog, native, bucket, dv):
    table = _table(catalog, {'bucket': bucket, 'deletion-vectors.enabled': str(dv).lower()})
    expected = []
    for version in range(3):
        rows = [dict(key=key, value=version) for key in range(32)]
        _write(table, (version + 1) * 100, rows)
        expected.extend(('+I', row['key'], row['value']) for row in rows)
    full, rows = _read(table, native)
    assert sorted((row['_row_kind'], row['key'], row['value']) for row in rows) == sorted(expected)
    # Files from multiple commits in the same bucket must share batch splits.
    assert len(full.splits()) == len({split.bucket for split in full.splits()})
    full_files = {file.file_name for split in full.splits() for file in split.files}
    for count in (2, 5):
        seen_files, union = set(), []
        for index in range(count):
            plan, rows = _read(table, native, shard=(index, count))
            assert plan.snapshot_id == 3
            assert all(split.is_streaming and split.snapshot_id == 3 for split in plan.splits())
            names = {file.file_name for split in plan.splits() for file in split.files}
            assert seen_files.isdisjoint(names)
            seen_files.update(names)
            union.extend((row['_row_kind'], row['key'], row['value']) for row in rows)
            for split in plan.splits():
                if dv:
                    assert all(java_file_name_shard(file.file_name, count) == index for file in split.files)
                else:
                    assert split.bucket % count == index
            _, limited = _read(table, native, shard=(index, count), limit=1)
            assert len(limited) == min(1, len(rows))
            assert all(row in rows for row in limited)
        assert seen_files == full_files
        assert sorted(union) == sorted(expected)


@pytest.mark.parametrize('native_write', [False, True])
def test_changelog_keeps_all_row_kinds_and_snapshot_boundaries(catalog, native, native_write):
    table = _table(catalog, {'bucket': '1', 'write.native.enabled': str(native_write).lower()})
    kinds = [RowKind.INSERT, RowKind.UPDATE_AFTER, RowKind.UPDATE_BEFORE, RowKind.DELETE]
    for index, kind in enumerate(kinds):
        _write(table, (index + 1) * 100, [dict(key=1, value=index)], kind=kind)
    plan, rows = _read(table, native, window=(100, 400))
    assert plan.snapshot_id == 4
    assert len(plan.splits()) == 1
    assert sorted((row['_row_kind'], row['key'], row['value']) for row in rows) == [
        ('+U', 1, 1), ('-D', 1, 3), ('-U', 1, 2)]
    for window in [(100, 100), (400, 400), (500, 600)]:
        plan, rows = _read(table, native, window=window, shard=(1, 3))
        assert rows == []
        assert plan.splits() == []


@pytest.mark.parametrize('limit', [0, 1, 2, 5])
def test_changelog_limit_is_applied_to_the_batch_once(catalog, native, limit):
    table = _table(catalog, {'bucket': '1', 'source.split.target-size': '1b'})
    for key in range(4):
        _write(table, (key + 1) * 100, [dict(key=key, value=key * 10)])
    plan, rows = _read(table, native, window=(0, 400), limit=limit)
    assert plan.snapshot_id == 4
    assert len(rows) == min(limit, 4)
    if limit:
        assert len(plan.splits()) == min(limit, 4)
    assert all(row['_row_kind'] == '+I' for row in rows)


@pytest.mark.parametrize('projection', [['value', 'key'], ['value'], []])
def test_sharded_changelog_filters_before_limit_and_preserves_projection(catalog, native, projection):
    table = _table(catalog, {'source.split.target-size': '1b'})
    for version in range(3):
        _write(table, (version + 1) * 100,
               [dict(key=key, value=version) for key in range(32)])
    predicate = table.new_read_builder().new_predicate_builder().greater_than('value', 0)
    total = 0
    for index in range(5):
        kwargs = dict(shard=(index, 5), predicate=predicate, projection=projection)
        _, rows = _read(table, native, **kwargs)
        total += len(rows)
        _, limited = _read(table, native, limit=1, **kwargs)
        assert len(limited) == min(1, len(rows))
        assert all(row in rows for row in limited)
        assert all(list(row) == ['_row_kind'] + projection for row in limited)
        if 'value' in projection:
            assert all(row['value'] > 0 for row in limited)
    assert total == 64


def test_changelog_skips_overwrite_but_includes_compact_and_keeps_empty_end_metadata(catalog, native):
    table = _table(catalog, {'bucket': '1'})
    for key in range(4):
        _write(table, (key + 1) * 100, [dict(key=key, value=key)])
    _patch_snapshot(table, 2, commitKind='COMPACT')
    _patch_snapshot(table, 3, commitKind='OVERWRITE')
    _patch_snapshot(table, 4, changelogManifestList=None)
    plan, rows = _read(table, native, window=(0, 400), with_stats=True)
    assert plan.snapshot_id == 4
    assert sorted((row['key'], row['value']) for row in rows) == [(0, 0), (1, 1)]
    assert all(split.snapshot_id == 4 for split in plan.splits())
    plan, rows = _read(table, native, window=(200, 400), shard=(0, 2))
    assert plan.snapshot_id == 4
    assert plan.splits() == []
    assert rows == []


def test_changelog_partition_filter_and_nested_projection_remain_native(catalog, native):
    schema = SCHEMA.append(pa.field('part', pa.string())).append(pa.field(
        'payload', pa.struct([('score', pa.int64()), ('unused', pa.string())])))
    table = _table(catalog, schema=schema, partitions=['part'])
    for version in range(3):
        _write(table, (version + 1) * 100, [
            dict(key=key, value=version, part=part,
                 payload=dict(score=version * 10 + key, unused='large'))
            for part in ['keep', 'drop'] for key in range(8)])
    predicate = table.new_read_builder().new_predicate_builder().equal('part', 'keep')
    union = []
    for index in range(3):
        plan, rows = _read(table, native, shard=(index, 3), predicate=predicate,
                           projection=['payload.score'])
        assert plan.snapshot_id == 3
        assert all(split.partition.get_field(0) == 'keep' for split in plan.splits())
        union.extend(row['payload_score'] for row in rows)
        assert all(row['_row_kind'] == '+I' for row in rows)
    assert sorted(union) == [version * 10 + key for version in range(3) for key in range(8)]


@pytest.mark.parametrize('engine', ['deduplicate', 'partial-update', 'aggregation'])
def test_batch_changelog_preserves_inputs_without_running_merge_engine(catalog, native, engine):
    options = {'bucket': '1', 'merge-engine': engine}
    if engine == 'aggregation':
        options['fields.value.aggregate-function'] = 'sum'
    table = _table(catalog, options)
    for version in range(3):
        _write(table, (version + 1) * 100, [dict(key=1, value=version + 1)])
    plan, rows = _read(table, native, shard=(0, 2))
    assert len(plan.splits()) == 1
    assert sorted((row['_row_kind'], row['key'], row['value']) for row in rows) == [
        ('+I', 1, 1), ('+I', 1, 2), ('+I', 1, 3)]


def test_batch_changelog_packs_evolved_schemas_and_filters_missing_fields(catalog, native):
    table = _table(catalog, {'bucket': '1'})
    _write(table, 100, [dict(key=1, value=10)])
    catalog.alter_table('default.t', [
        SchemaChange.rename_column('value', 'renamed'),
        SchemaChange.add_column('added', AtomicType('BIGINT'))], False)
    table = catalog.get_table('default.t')
    _write(table, 200, [dict(key=1, renamed=20, added=2)])
    _write(table, 300, [dict(key=2, renamed=30, added=3)])
    plan, rows = _read(table, native, projection=['key', 'renamed', 'added'])
    assert len(plan.splits()) == 1
    assert sorted((row['key'], row['renamed'], row['added']) for row in rows) == [
        (1, 10, None), (1, 20, 2), (2, 30, 3)]
    predicate = table.new_read_builder().new_predicate_builder().greater_than('added', 2)
    _, rows = _read(table, native, shard=(0, 2), predicate=predicate,
                    projection=['renamed', 'added'], limit=1)
    assert rows == [dict(_row_kind='+I', renamed=30, added=3)]
