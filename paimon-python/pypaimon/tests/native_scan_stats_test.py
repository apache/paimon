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

"""Native explain keeps the complete metadata funnel in one planning pass."""

from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.read.native_plan import _native_table, native_runtime_available
from pypaimon.tests import native_plan_rest_test, native_query_auth_test
from pypaimon.utils.file_store_path_factory import canonical_data_file_path

rest_catalog = native_plan_rest_test.rest_catalog
guarded_table = native_query_auth_test.guarded_table
pytestmark = [pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='Rust planner required')]


def _create(catalog, schema=None, partition_keys=None, primary_keys=None, options=None):
    schema = schema or pa.schema([('id', pa.int32()), ('value', pa.int32())])
    settings = {'bucket': '-1', 'source.split.target-size': '1b',
                'source.split.open-file-cost': '1b', 'scan.native-plan.enabled': 'true',
                'read.native.enabled': 'false', 'write.native.enabled': 'false',
                'commit.native.enabled': 'false', 'metadata.stats-mode': 'full'}
    settings.update(options or {})
    catalog.create_table('default.scan_stats', Schema.from_pyarrow_schema(
        schema, partition_keys=partition_keys or [], primary_keys=primary_keys or [], options=settings), False)
    return catalog.get_table('default.scan_stats'), schema


def _append(table, rows, schema, overwrite=False):
    builder = table.new_batch_write_builder()
    if overwrite:
        builder.overwrite()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
        messages = writer.prepare_commit()
        commit.commit(messages)
        return messages
    finally:
        writer.close()
        commit.close()


def _native_explain(builder, verbose=False):
    with patch('pypaimon.read.scanner.file_scanner.FileScanner.scan_with_stats',
               side_effect=AssertionError('explain fell back to Python')):
        result = builder.explain(verbose)
    assert result.native_planned
    assert 'pruning not tracked' not in str(result)
    return result


@pytest.mark.parametrize('limit,expected_files', [(None, 2), (0, 0), (1, 2)])
def test_native_explain_counts_all_stats_candidates_before_limit(rest_catalog, limit, expected_files):
    catalog, _ = rest_catalog
    table, schema = _create(catalog)
    for start in [0, 10, 100]:
        _append(table, [{'id': start + i, 'value': start + i} for i in range(3)], schema)
    builder = table.new_read_builder()
    builder.with_filter(builder.new_predicate_builder().less_or_equal('value', 20))
    if limit is not None:
        builder.with_limit(limit)
    result = _native_explain(builder, True)
    assert result.snapshot_id == 3
    assert result.file_count == expected_files
    assert (result.file_skipping.before, result.file_skipping.after, result.file_skipping.pruned) == (3, 2, 1)
    assert result.split_count == len(builder.new_scan().plan().splits())
    assert result.splits is not None


def test_native_explain_partition_and_bucket_funnel_matches_python(rest_catalog):
    catalog, _ = rest_catalog
    schema = pa.schema([('pt', pa.int32()), ('id', pa.int32()), ('value', pa.int32())])
    table, _ = _create(catalog, schema, ['pt'], ['pt', 'id'], {'bucket': '4', 'bucket-key': 'id'})
    _append(table, [{'pt': pt, 'id': i, 'value': i} for pt in [1, 2] for i in range(64)], schema)

    def explain(native):
        selected = table.copy({'scan.native-plan.enabled': str(native).lower()}).new_read_builder()
        pb = selected.new_predicate_builder()
        selected.with_filter(pb.and_predicates([pb.equal('pt', 1), pb.equal('id', 7)]))
        return _native_explain(selected) if native else selected.explain()

    expected, actual = explain(False), explain(True)
    for name in ['partition_pruning', 'bucket_pruning', 'file_skipping']:
        assert getattr(actual, name) == getattr(expected, name)
    assert (actual.partition_pruning.before, actual.partition_pruning.after) == (8, 4)
    assert (actual.bucket_pruning.before, actual.bucket_pruning.after) == (4, 1)
    assert actual.file_count == expected.file_count == 1


@pytest.mark.parametrize('limit,expected_files', [(None, 3), (0, 0), (1, 1)])
def test_native_explain_counts_candidates_before_limit_without_row_filter(rest_catalog, limit, expected_files):
    catalog, _ = rest_catalog
    table, schema = _create(catalog)
    for start in range(3):
        _append(table, [{'id': start, 'value': start}], schema)
    reader = table.new_read_builder()
    if limit is not None:
        reader.with_limit(limit)
    scan = reader.new_scan()
    with patch.object(scan.file_scanner, 'scan_with_stats', side_effect=AssertionError('Python stats ran')):
        plan, stats = scan.scan_with_stats()
    assert stats.entries_after_stats == 3
    assert len(plan.splits()) == expected_files
    assert [split.file_paths for split in plan.splits()] == [
        split.file_paths for split in reader.new_scan().plan().splits()]


def test_native_explain_counts_cross_schema_overwrite_entries_before_netting(rest_catalog):
    from pypaimon.schema.data_types import AtomicType
    from pypaimon.schema.schema_change import SchemaChange
    catalog, _ = rest_catalog
    table, schema = _create(catalog)
    _append(table, [{'id': 1, 'value': 1}], schema)
    catalog.alter_table('default.scan_stats', [SchemaChange.add_column('extra', AtomicType('INT'))], False)
    table = catalog.get_table('default.scan_stats')
    schema = schema.append(pa.field('extra', pa.int32()))
    _append(table, [{'id': 2, 'value': 100, 'extra': 3}], schema, overwrite=True)
    outcomes = []
    for native in [False, True]:
        reader = table.copy({'scan.native-plan.enabled': str(native).lower()}).new_read_builder()
        reader.with_filter(reader.new_predicate_builder().greater_than('value', 50))
        outcomes.append(_native_explain(reader) if native else reader.explain())
    expected, actual = outcomes
    assert actual.file_count == expected.file_count == 1
    assert actual.file_skipping == expected.file_skipping
    assert actual.file_skipping.before == 3
    assert actual.file_skipping.after == 2


def test_native_explain_keeps_empty_and_historical_snapshot_metadata(rest_catalog):
    catalog, _ = rest_catalog
    table, schema = _create(catalog)
    empty = table._copy_with_snapshot(None)
    result = _native_explain(empty.new_read_builder())
    assert result.snapshot_id is None
    assert result.file_count == 0
    _append(table, [{'id': 1, 'value': 10}], schema)
    first = table.snapshot_manager().get_latest_snapshot()
    historical = table._copy_with_snapshot(first)
    _append(table, [{'id': 2, 'value': 20}], schema)
    result = _native_explain(historical.new_read_builder())
    assert result.snapshot_id == 1
    assert result.file_count == 1
    assert _native_explain(empty.new_read_builder()).file_count == 0
    builder = table.new_read_builder()
    builder.with_filter(builder.new_predicate_builder().greater_than('value', 1000))
    result = _native_explain(builder)
    assert result.snapshot_id == 2
    assert result.file_count == 0
    assert (result.file_skipping.before, result.file_skipping.after) == (2, 0)


def test_native_explain_does_not_open_data_files(rest_catalog):
    catalog, _ = rest_catalog
    table, schema = _create(catalog)
    messages = _append(table, [{'id': 1, 'value': 10}], schema)
    with ExitStack() as cleanup:
        for message in messages:
            for file in message.new_files:
                path = LocalFileIO()._to_file(canonical_data_file_path(
                    table, message.partition, message.bucket, file.file_name))
                hidden = path.with_suffix('.unavailable')
                path.rename(hidden)
                cleanup.callback(hidden.rename, path)
        result = _native_explain(table.new_read_builder())
    assert result.file_count == 1
    assert result.estimated_row_count == 1


def test_native_stats_are_readonly_and_normal_planning_stays_lightweight(rest_catalog):
    catalog, _ = rest_catalog
    table, schema = _create(catalog)
    _append(table, [{'id': i, 'value': i} for i in range(3)], schema)
    scan = _native_table(table).new_read_builder().new_scan()
    assert scan.plan().scan_stats() is None
    plan = scan.with_stats().plan()
    stats = plan.scan_stats()
    assert stats.complete
    assert stats.entries_potential_total == stats.manifest_entries_read == 1
    assert stats.entries_after_partition == stats.entries_after_bucket == stats.entries_after_stats == 1
    assert plan.snapshot_id() == 1
    with pytest.raises(AttributeError):
        stats.manifest_entries_read = 100
    with patch('pypaimon.read.scan_stats.ScanStats.from_native',
               side_effect=AssertionError('ordinary plan requested statistics')):
        assert table.new_read_builder().new_scan().plan().snapshot_id == 1


@pytest.mark.parametrize('changelog', [False, True])
def test_native_incremental_explain_reports_only_the_selected_window(rest_catalog, changelog):
    import json
    catalog, _ = rest_catalog
    table, schema = _create(catalog, primary_keys=['id'] if changelog else None,
                            options={'bucket': '1', 'changelog-producer': 'input'} if changelog else {})
    for index, timestamp in enumerate([1000, 2000, 3000]):
        _append(table, [{'id': index, 'value': timestamp}], schema)
        snapshot = table.snapshot_manager().get_latest_snapshot()
        path = table.snapshot_manager().snapshot_dir + '/snapshot-' + str(snapshot.id)
        with table.file_io.new_input_stream(path) as stream:
            data = json.loads(stream.read())
        data['timeMillis'] = timestamp
        table.file_io.overwrite_file_utf8(path, json.dumps(data))
    selected = table.copy({'incremental-between-timestamp': '1000,3000'})
    result = _native_explain(selected.new_read_builder(), True)
    assert result.snapshot_id == 3
    assert result.file_count == 2
    scan = selected.new_read_builder().new_scan()
    with patch.object(scan.file_scanner, 'scan_with_stats', side_effect=AssertionError('Python stats ran')):
        plan, stats = scan.scan_with_stats()
    assert stats.native_planned
    assert stats.entries_potential_total == stats.entries_after_stats == 2
    assert plan.snapshot_id == 3
    assert all(split.is_streaming for split in plan.splits())
    assert all(file.file_name.startswith('changelog-') == changelog
               for split in plan.splits() for file in split.files)


@pytest.mark.parametrize('limit', [None, 0, 1])
def test_native_data_evolution_explain_tracks_projected_files_before_limit(rest_catalog, limit):
    catalog, _ = rest_catalog
    table, schema = _create(catalog, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'})
    _append(table, [{'id': i, 'value': i} for i in range(3)], schema)
    builder = table.new_batch_write_builder()
    updater = builder.new_update().new_update_by_row_id()
    messages = updater.update_columns(pa.table({'_ROW_ID': [0, 1, 2], 'value': pa.array([10, 11, 12], pa.int32())}),
                                      ['value'])
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    reader = table.new_read_builder().with_projection(['id'])
    if limit is not None:
        reader.with_limit(limit)
    scan = reader.new_scan()
    with patch.object(scan.file_scanner, 'scan_with_stats', side_effect=AssertionError('Python stats ran')):
        plan, stats = scan.scan_with_stats()
    assert stats.native_planned
    assert stats.entries_potential_total == stats.entries_after_bucket == 2
    assert stats.entries_after_stats == 1
    assert len(plan.splits()) == (0 if limit == 0 else 1)


def test_native_explain_preserves_the_authorized_plan_and_captured_policy(guarded_table):
    import json
    from pypaimon.api.api_response import AuthTableQueryResponse
    from pypaimon.common.predicate_builder import PredicateBuilder
    table, server = guarded_table
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        filter=[native_query_auth_test._filter('id', 'GREATER_THAN', [1])],
        column_masking={'value': json.dumps({
            'name': 'SUBSTRING', 'inputs': [native_query_auth_test._field('source'), 1, 2]})})
    table = table.copy({'scan.native-plan.enabled': 'true', 'read.native.enabled': 'true'})
    reader = table.new_read_builder().with_projection(['id', 'value'])
    reader.with_filter(PredicateBuilder(table.fields).equal('value', 'ab')).with_limit(1)
    scan = reader.new_scan()
    with patch.object(scan.file_scanner, 'scan_with_stats', side_effect=AssertionError('Python stats ran')):
        plan, stats = scan.scan_with_stats()
    assert stats.native_planned
    assert plan.splits()
    assert all(getattr(split, '_native_split', None) is not None for split in plan.splits())
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse()
    with patch('pypaimon.read.table_read.TableRead._create_split_read',
               side_effect=AssertionError('Native authorized read fell back')):
        assert reader.new_read().to_arrow(plan.splits()).to_pylist() == [{'id': 2, 'value': 'ab'}]
