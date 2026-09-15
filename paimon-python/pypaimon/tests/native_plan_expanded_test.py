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

"""Native planning coverage for Java time travel, postpone and scored DE reads."""

import json
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.utils.range import Range
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.file_store_write import FileStoreWrite


@pytest.fixture(params=[False, pytest.param(True, marks=[
    pytest.mark.native_plan, pytest.mark.skipif(
        not native_runtime_available(), reason='Rust main required')])], ids=['python', 'native'])
def native(request):
    return request.param


@pytest.fixture
def catalog(tmp_path):
    result = CatalogFactory.create({'warehouse': str(tmp_path)})
    result.create_database('default', True)
    return result


def create(catalog, schema, **kwargs):
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, **kwargs), False)
    return catalog.get_table('default.t')


def write(table, rows, schema):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pydict(
            {name: [row[name] for row in rows] for name in schema.names}, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def read(table, native, predicate=None, shard=None, limit=None, result=None, ranges=None):
    builder = table.copy({'scan.native-plan.enabled': str(native).lower()}).new_read_builder()
    if predicate is not None:
        builder.with_filter(predicate)
    if limit is not None:
        builder.with_limit(limit)
    scan = builder.new_scan()
    if shard is not None:
        scan.with_shard(*shard)
    if result is not None:
        scan.with_global_index_result(result)
    if ranges is not None:
        scan.with_row_ranges(ranges)
    if native:
        with patch.object(scan.file_scanner, 'scan', side_effect=AssertionError('native fallback')):
            plan = scan.plan()
    else:
        plan = scan.plan()
    return plan, builder.new_read().to_arrow(plan.splits()).to_pylist()


@pytest.mark.parametrize('version,expected', [('1', 1), ('base', 1), ('watermark-150', 2)])
def test_scan_version_resolves_java_selectors(catalog, native, version, expected):
    schema = pa.schema([('id', pa.int64())])
    table = create(catalog, schema)
    for snapshot, value in enumerate((1, 2), 1):
        write(table, [{'id': value}], schema)
        path = table.snapshot_manager().get_snapshot_path(snapshot)
        metadata = json.loads(table.file_io.read_file_utf8(path))
        metadata['watermark'] = snapshot * 100
        table.file_io.write_file(path, json.dumps(metadata), overwrite=True)
    table.create_tag('base', 1)
    plan, rows = read(table.copy({'scan.version': version}), native)
    assert plan.snapshot_id == expected
    assert sorted(row['id'] for row in rows) == list(range(1, expected + 1))
    # A numeric tag wins over the snapshot with the same name, as in Java.
    table.create_tag('2', 1)
    assert read(table.copy({'scan.version': '2'}), native)[0].snapshot_id == 1


@pytest.mark.parametrize('options', [
    {'scan.version': 'missing'}, {'scan.version': 'watermark-invalid'},
    {'scan.version': '1', 'scan.timestamp-millis': '1'},
    {'scan.version': '1', 'incremental-between-timestamp': '1,2'},
])
def test_scan_version_rejects_invalid_or_conflicting_selectors(catalog, options):
    table = create(catalog, pa.schema([('id', pa.int64())]))
    with pytest.raises((ValueError, RuntimeError)):
        read(table.copy(options), False)


def test_postpone_reads_only_real_buckets(catalog, native):
    schema = pa.schema([('id', pa.int64()), ('p', pa.string()), ('v', pa.string())])
    table = create(catalog, schema, primary_keys=['id', 'p'], partition_keys=['p'], options={
        'bucket': '-2', 'source.split.target-size': '1 b', 'source.split.open-file-cost': '1 b'})
    writer = FileStoreWrite(table, 'postpone')
    rows = [{'id': 1, 'p': 'a', 'v': 'real'}, {'id': 2, 'p': 'a', 'v': 'pending'},
            {'id': 3, 'p': 'b', 'v': 'real'}, {'id': 4, 'p': 'b', 'v': 'pending'}]
    try:
        for row, bucket in zip(rows, (0, -1, 1, -1)):
            batch = pa.RecordBatch.from_pydict(
                {name: [row[name]] for name in schema.names}, schema=schema)
            writer.write((row['p'],), bucket, batch)
        commit = table.new_batch_write_builder().new_commit()
        try:
            commit.commit(writer.prepare_commit(1))
        finally:
            commit.close()
    finally:
        writer.close()
    pb = table.new_read_builder().new_predicate_builder()
    for predicate in (None, pb.equal('p', 'a'), pb.equal('id', 2), pb.equal('v', 'real')):
        for shard in (None, (0, 2), (1, 2)):
            for limit in (None, 1):
                plan, actual = read(table, native, predicate, shard, limit)
                expected = [row for row, bucket in zip(rows, (0, -1, 1, -1))
                            if bucket >= 0 and (shard is None or bucket % shard[1] == shard[0])
                            and (predicate is None or predicate.test(GenericRow(list(row.values()), table.fields)))]
                assert all(split.bucket >= 0 for split in plan.splits())
                assert plan.snapshot_id == 1
                expected_count = min(len(expected), limit) if limit is not None else len(expected)
                assert len(actual) == expected_count
                assert all(row in expected for row in actual)
    assert read(table.copy({'scan.snapshot-id': '1'}), native)[1] == [rows[0], rows[2]]


@pytest.mark.parametrize('dv', [False, True])
def test_scored_de_preserves_score_mapping_and_selection(catalog, native, dv):
    schema = pa.schema([('id', pa.int64()), ('v', pa.string())])
    table = create(catalog, schema, options={
        'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
        'deletion-vectors.enabled': str(dv).lower(),
        'source.split.target-size': '1 b', 'source.split.open-file-cost': '1 b'})
    for start in (0, 3):
        write(table, [{'id': i, 'v': str(i)} for i in range(start, start + 3)], schema)
    if dv:
        builder = table.new_batch_write_builder()
        commit = builder.new_commit()
        try:
            commit.commit(builder.new_update().delete_by_row_id([1]))
        finally:
            commit.close()
    scores = {5: 0.9, 1: 0.2, 3: 0.5}
    result = DictBasedScoredIndexResult(scores)
    pb = table.new_read_builder().new_predicate_builder()
    for shard in (None, (0, 2), (1, 2)):
        for predicate in (None, pb.greater_than('id', 1)):
            plan, actual = read(table, native, predicate, shard, result=result)
            for split in plan.splits():
                ids = [i for r in split.row_ranges() for i in range(r.from_, r.to + 1)]
                assert split.scores() == [scores[i] for i in ids]
            python_rows = read(table, False, predicate, shard, result=result)[1]
            assert actual == python_rows
            assert all(row['id'] in scores and (not dv or row['id'] != 1) for row in actual)
            limited = read(table, native, predicate, shard, limit=1, result=result)[1]
            assert len(limited) == min(len(actual), 1)
            assert all(row in actual for row in limited)
    assert {row['id'] for row in read(table, native, result=result)[1]} == ({3, 5} if dv else {1, 3, 5})
    empty, rows = read(table, native, result=DictBasedScoredIndexResult({}))
    assert rows == [] and empty.snapshot_id == (3 if dv else 2)
    with pytest.raises(ValueError, match='mutually exclusive'):
        read(table, native, result=result, ranges=[Range(0, 0)])


@pytest.mark.parametrize('covered', [True, False])
def test_pk_sorted_index_refines_native_splits_at_selected_snapshot(catalog, native, covered):
    from pypaimon.globalindex.global_index_meta import GlobalIndexMeta
    from pypaimon.globalindex.indexed_split import IndexedSplit
    from pypaimon.index.index_file_meta import IndexFileMeta
    from pypaimon.index.pk.primary_key_index_source_file import PrimaryKeyIndexSourceFile
    from pypaimon.index.pk.primary_key_index_source_meta import PrimaryKeyIndexSourceMeta
    from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
    from pypaimon.tests.primary_key_sorted_index_scan_test import _Reader

    schema = pa.schema([('id', pa.int64()), ('v', pa.int64())])
    table = create(catalog, schema, primary_keys=['id'], options={
        'bucket': '1', 'pk-btree.index.columns': 'v'})
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pydict(
            {'id': [1, 2, 3], 'v': [10, 20, 10]}, schema=schema))
        messages = writer.prepare_commit()
        file = messages[0].new_files[0]
        # A compaction-produced L1 file is eligible for the sorted index.
        file.level, file.file_source = 1, 1
        commit.commit(messages)
    finally:
        writer.close()
        commit.close()
    source_meta = PrimaryKeyIndexSourceMeta(1, [
        PrimaryKeyIndexSourceFile(file.file_name if covered else 'retired-file', 3)]).serialize()
    payload = IndexFileMeta('btree', 'index', 1, 3,
                            global_index_meta=GlobalIndexMeta(0, 2, 1, source_meta=source_meta))
    entry = IndexManifestEntry(0, GenericRow([], []), 0, payload)
    write(table, [{'id': 2, 'v': 99}], schema)
    predicate = table.new_read_builder().new_predicate_builder().equal('v', 20)
    with patch('pypaimon.index.index_file_handler.IndexFileHandler.scan', return_value=[entry]) as scan_index, \
            patch('pypaimon.table.source.primary_key_sorted_index_scan.reader_factory',
                  return_value=lambda *args: _Reader([Range(1, 1)])):
        plan, rows = read(table.copy({'scan.version': '1'}), native, predicate)
    assert rows == [{'id': 2, 'v': 20}]
    assert plan.snapshot_id == 1
    assert scan_index.call_args[0][0].id == 1
    assert any(isinstance(split, IndexedSplit) for split in plan.splits()) == covered
    if native:
        assert all(split.snapshot_id == 1 for split in plan.splits())
    # The latest snapshot includes a newer version. Index pruning must not
    # resurrect the old matching version from a merge-required split.
    with patch('pypaimon.index.index_file_handler.IndexFileHandler.scan', return_value=[entry]), \
            patch('pypaimon.table.source.primary_key_sorted_index_scan.reader_factory',
                  return_value=lambda *args: _Reader([Range(1, 1)])):
        assert read(table, native, predicate)[1] == []


def test_scored_result_requires_a_score_for_each_selected_row(catalog, native):
    from pypaimon.globalindex.global_index_result import GlobalIndexResult
    from pypaimon.globalindex.vector_search_result import ScoredGlobalIndexResult
    schema = pa.schema([('id', pa.int64())])
    table = create(catalog, schema, options={
        'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'})
    write(table, [{'id': 1}], schema)
    result = ScoredGlobalIndexResult.create(
        GlobalIndexResult.from_range(Range(0, 0)).results(), lambda _: None)
    builder = table.copy({'scan.native-plan.enabled': str(native).lower()}).new_read_builder()
    with pytest.raises(ValueError, match='score'):
        builder.new_scan().with_global_index_result(result).plan()


@pytest.mark.parametrize('version', ['1', 'before-drop'])
def test_scan_version_restores_historical_schema(catalog, native, version):
    from pypaimon.schema.schema_change import SchemaChange
    schema = pa.schema([('id', pa.int64()), ('old_value', pa.string())])
    table = create(catalog, schema)
    write(table, [{'id': 1, 'old_value': 'historic'}], schema)
    table.create_tag('before-drop', 1)
    catalog.alter_table('default.t', [SchemaChange.drop_column('old_value')], False)
    table = catalog.get_table('default.t')
    historical = table.copy({'scan.version': version})
    assert historical.field_names == ['id', 'old_value']
    plan, rows = read(historical, native)
    assert plan.snapshot_id == 1
    assert rows == [{'id': 1, 'old_value': 'historic'}]
    assert table.field_names == ['id']


@pytest.mark.parametrize('version,key', [('1', 'scan.snapshot-id'), ('before', 'scan.tag-name')])
def test_scan_version_overwrites_same_selector(catalog, native, version, key):
    schema = pa.schema([('id', pa.int64())])
    table = create(catalog, schema)
    write(table, [{'id': 1}], schema)
    table.create_tag('before', 1)
    selected = table.copy({'scan.version': version, key: 'invalid-overridden-value'})
    assert read(selected, native)[1] == [{'id': 1}]
    assert selected.options.options.to_map()[key] == 'invalid-overridden-value'
