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

"""Captured row-ID execution preserves row order, ownership and read grants."""

from dataclasses import replace
import pickle
import sys
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon.catalog.table_query_auth import TableQueryAuthResult
from pypaimon.execution.native.write.row_id_plan import NativeRowIdPlan
from pypaimon.execution.python.write.row_id_plan import PythonRowIdPlan
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.read.plan import Plan
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.read.scan_stats import ScanStats, _NATIVE_COUNTER_FIELDS
from pypaimon.read.split import DataSplit
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.source.deletion_file import DeletionFile
from pypaimon.utils.range import Range
from pypaimon.write.row_id_plan import CapturedRowIdPlan

pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]


def _file(name, first, count):
    empty = GenericRow([], [])
    return DataFileMeta.create(
        name, 100, count, empty, empty, SimpleStats.empty_stats(), SimpleStats.empty_stats(),
        0, 0, 0, 0, [], first_row_id=first, external_path='memory:/external/' + name,
        file_path='memory:/external/' + name)


def _split(first, count, partition, bucket=0, dedicated=False):
    files = [_file('data-{}.parquet'.format(first), first, count)]
    if dedicated:
        files += [replace(files[0], file_name='delta.parquet', max_sequence_number=1,
                          write_cols=['value']), _file('payload.blob', first, count)]
    field = DataField(0, 'p', AtomicType('STRING'))
    return DataSplit(files, GenericRow([partition], [field]), bucket,
                     data_deletion_files=[DeletionFile('memory:/dv', 0, 8, 1)] * len(files),
                     snapshot_id=7, bucket_path='memory:/captured/bucket-{}'.format(bucket),
                     total_buckets=4)


@pytest.fixture
def captured():
    grant = TableQueryAuthResult(['value > 0'], {'value': 'value + 1'})
    splits = [_split(50, 2, 'west', 1), _split(20, 3, 'west'),
              QueryAuthSplit(_split(10, 3, 'west', dedicated=True), grant),
              _split(30, 2, 'east'), _split((1 << 63) - 1, 1, 'west')]
    plan = Plan(splits, 7, _scan_stats=ScanStats(entries_total=17), _stats_limit=3)
    execution = PythonRowIdPlan(plan)
    return CapturedRowIdPlan(None, plan, execution.groups, execution)


def test_python_routing_keeps_unsorted_duplicates_and_inclusive_ends(captured):
    row_ids = pa.array([22, 10, 31, 11, 20, 51, 22, (1 << 63) - 1], type=pa.int64())
    assert captured.route(row_ids).to_pylist() == [20, 10, 30, 10, 20, 50, 20, (1 << 63) - 1]
    # Two independent anchors in one partition/bucket share the oldest owner.
    # The same bucket in a different partition and a different bucket do not.
    assert captured.route_by_bucket(row_ids).to_pylist() == [10, 10, 30, 10, 10, 50, 10, 10]


@pytest.mark.parametrize('operation', ['route', 'route_by_bucket', 'select'])
@pytest.mark.parametrize('row_id', [-1, 9, 13, 19, 23, 32, 49, 52])
def test_python_routing_rejects_foreign_ids_and_gaps(captured, operation, row_id):
    with pytest.raises(ValueError, match='valid range'):
        getattr(captured, operation)(pa.array([10, row_id], type=pa.int64()))


@pytest.mark.parametrize('operation', ['route', 'route_by_bucket', 'select'])
@pytest.mark.parametrize('row_ids,error', [
    (pa.array([10.5]), TypeError), (pa.array([True]), TypeError), (pa.array(['10']), TypeError),
    (pa.array([10, None], type=pa.int64()), ValueError),
    (pa.array([1 << 63], type=pa.uint64()), ValueError), ([10], TypeError),
])
def test_public_routing_validates_ids_before_backend_execution(captured, operation, row_ids, error):
    execution = Mock()
    captured = replace(captured, _execution=execution)
    with pytest.raises(error):
        getattr(captured, operation)(row_ids)
    getattr(execution, operation).assert_not_called()


def test_chunked_integer_input_is_normalized_without_float_coercion(captured):
    row_ids = pa.chunked_array([pa.array([10], type=pa.int32()), pa.array([22, 31], type=pa.int32())])
    assert captured.route(row_ids).to_pylist() == [10, 20, 30]


def test_selection_deduplicates_absolute_ids_and_preserves_complete_groups(captured):
    original_splits = captured.plan.splits()[:]
    selected = captured.select(pa.array([21, 12, 10, 11, 21, (1 << 63) - 1], type=pa.int64()))
    assert selected.snapshot_id == 7
    assert selected._scan_stats is not captured.plan._scan_stats
    assert selected._scan_stats == captured.plan._scan_stats
    assert selected._stats_limit == 3
    assert [split.row_ranges() for split in selected.splits()] == [
        [Range(10, 12)], [Range(21, 21)], [Range((1 << 63) - 1, (1 << 63) - 1)]]
    authorized = selected.splits()[0]
    original = next(split for split in original_splits if isinstance(split, QueryAuthSplit))
    assert isinstance(authorized, QueryAuthSplit)
    assert authorized.auth_result is original.auth_result
    assert authorized.files == original.files
    assert len(authorized.files) == 3
    assert authorized.data_deletion_files == original.data_deletion_files
    assert authorized.split.data_split().bucket_path == original.bucket_path
    assert authorized.split.data_split().total_buckets == original.total_buckets
    assert authorized.snapshot_id == original.snapshot_id
    assert captured.plan.splits() == original_splits
    assert captured.groups[0][2].bucket_path == original.bucket_path


def test_empty_routing_and_selection_keep_snapshot(captured):
    ids = pa.array([], type=pa.int64())
    for view in (captured, CapturedRowIdPlan(None, Plan([], None), [], PythonRowIdPlan(Plan([], None)))):
        assert view.route(ids).to_pylist() == []
        assert view.route_by_bucket(ids).to_pylist() == []
        assert view.select(ids).splits() == []
        assert view.select(ids).snapshot_id == view.plan.snapshot_id


def test_python_captured_routing_pickle_and_complete_group_scope(captured):
    restored = pickle.loads(pickle.dumps(captured))
    assert restored.snapshot_id == 7
    assert restored.route(pa.array([22, 10])).to_pylist() == [20, 10]
    single = restored.for_split(restored.groups[1][2])
    assert single.route_by_bucket(pa.array([20, 22])).to_pylist() == [20, 20]
    with pytest.raises(ValueError, match='valid range'):
        single.route(pa.array([10]))


@pytest.mark.parametrize('operation', ['route', 'route_by_bucket', 'select'])
def test_native_routing_is_arrow_transport_and_never_replays_python(captured, operation):
    core = Mock()
    expected = (Plan([], 7) if operation == 'select' else pa.array([10, 20], type=pa.int64()))
    getattr(core, operation).return_value = expected
    execution = NativeRowIdPlan(None, captured.plan, native=core, groups=captured.groups)
    captured = replace(captured, _execution=execution)
    source = pa.chunked_array([pa.array([12], type=pa.int32()), pa.array([20], type=pa.int32())])
    with patch.object(PythonRowIdPlan, '__init__', side_effect=AssertionError('Python replay')), \
            patch('pypaimon.execution.native.read.plan._from_native_plan', return_value=expected) as decode:
        actual = getattr(captured, operation)(source)
        if operation == 'select':
            assert actual == expected
            assert actual._scan_stats is not captured.plan._scan_stats
            assert actual._scan_stats == captured.plan._scan_stats
            assert actual._stats_limit == 3
        else:
            assert actual is expected
    transported = getattr(core, operation).call_args.args[0]
    assert transported.type == pa.int64()
    assert transported.to_pylist() == [12, 20]
    assert decode.call_count == int(operation == 'select')
    failure = PermissionError('injected Core routing error')
    getattr(core, operation).side_effect = failure
    with patch.object(PythonRowIdPlan, '__init__', side_effect=AssertionError('Python replay')):
        with pytest.raises(PermissionError) as raised:
            getattr(captured, operation)(source)
    assert raised.value is failure


def test_native_routing_pickle_rebuilds_only_the_captured_plan(captured, monkeypatch):
    sdk = SimpleNamespace(RowIdPlan=Mock())
    monkeypatch.setitem(sys.modules, 'pypaimon_rust.datafusion', sdk)
    sdk.RowIdPlan.return_value.route.return_value = pa.array([20, 10], type=pa.int64())
    execution = NativeRowIdPlan(None, captured.plan, native=lambda: None, groups=captured.groups)
    restored = pickle.loads(pickle.dumps(replace(captured, _execution=execution)))
    assert restored._execution._native is None
    assert restored.snapshot_id == 7
    wire = object()
    with patch('pypaimon.execution.native.write.row_id_plan.native_plan', return_value=wire) as transport, \
            patch('pypaimon.execution.native.write.row_id_plan.create_native_row_id_plan',
                  side_effect=AssertionError('backend reselected')), \
            patch('pypaimon.read.table_scan.TableScan.plan_all', side_effect=AssertionError('worker replanned')), \
            patch.object(PythonRowIdPlan, '__init__', side_effect=AssertionError('Python replay')):
        assert restored.route(pa.array([22, 10])).to_pylist() == [20, 10]
        assert restored.route(pa.array([22, 10])).to_pylist() == [20, 10]
    sdk.RowIdPlan.assert_called_once_with(wire)
    assert transport.call_args.args[1].snapshot_id == 7
    assert transport.call_args.args[1].splits() == restored.plan.splits()
    scoped = restored.for_split(restored.groups[0][2])
    assert isinstance(scoped._execution, NativeRowIdPlan)
    assert len(scoped.plan.splits()) == 1


@pytest.mark.parametrize('dedicated_name', ['payload.blob', 'embedding.vector.parquet'])
def test_dedicated_files_do_not_merge_normal_owners_or_fill_row_id_gaps(dedicated_name):
    original = _split(10, 3, 'west')
    other = _file('data-20.parquet', 20, 3)
    dedicated = _file(dedicated_name, 10, 13)
    files = original.files + [other, dedicated]
    deletion_files = [DeletionFile('memory:/dv-' + str(index), 0, 8, 1) for index in range(3)]
    split = DataSplit(files, original.partition, 0, data_deletion_files=deletion_files,
                      snapshot_id=7, bucket_path=original.bucket_path, total_buckets=4)
    grant = TableQueryAuthResult(['value > 0'], {})
    plan = Plan([QueryAuthSplit(split, grant)], 7)
    execution = PythonRowIdPlan(plan)
    captured = CapturedRowIdPlan(None, plan, execution.groups, execution)
    assert [(first, last) for first, last, _ in captured.groups] == [(10, 12), (20, 22)]
    assert captured.route(pa.array([10, 21])).to_pylist() == [10, 20]
    assert captured.route_by_bucket(pa.array([10, 21])).to_pylist() == [10, 10]
    for first, _, group in captured.groups:
        assert isinstance(group, QueryAuthSplit)
        assert group.auth_result is grant
        assert group.bucket_path == split.bucket_path
        assert group.total_buckets == 4
        assert {file.file_name for file in group.files} == {'data-{}.parquet'.format(first), dedicated_name}
        assert group.data_deletion_files == [deletion_files[0 if first == 10 else 1], deletion_files[2]]
    with pytest.raises(ValueError, match='valid range'):
        captured.route(pa.array([15]))
    selected = captured.select(pa.array([21, 11]))
    assert [group.row_ranges() for group in selected.splits()] == [[Range(11, 11)], [Range(21, 21)]]
    assert all(dedicated in group.files for group in selected.splits())


def test_overlapping_normal_anchors_with_different_ranges_are_rejected():
    original = _split(10, 3, 'west')
    overlap = _file('overlap.parquet', 11, 3)
    split = DataSplit(original.files + [overlap], original.partition, 0, snapshot_id=7)
    with pytest.raises(ValueError, match='identical ranges'):
        PythonRowIdPlan(Plan([split], 7))


@pytest.mark.parametrize('state', ['lazy', 'materialized', 'pickled'])
@pytest.mark.parametrize('selected_ids', [[], [10, 21]])
def test_selected_plan_stats_keep_work_and_update_output_without_mutating_original(captured, state, selected_ids):
    counters = {field: 0 for field in _NATIVE_COUNTER_FIELDS}
    counters.update(manifest_entries_read=99, manifest_entries_after_merge=27)
    captured.plan._scan_stats = None
    captured.plan._native_stats = SimpleNamespace(**counters)
    if state == 'materialized':
        captured.plan.stats()
    elif state == 'pickled':
        captured = pickle.loads(pickle.dumps(captured))
    selected = captured.select(pa.array(selected_ids, type=pa.int64()))
    source = dict(captured.plan.stats().planning_counters)
    selected_stats = selected.stats().planning_counters
    assert selected_stats['manifest_entries_read'] == 99
    assert selected_stats['manifest_entries_after_merge'] == 27
    assert selected_stats['limit'] == 3
    assert selected_stats['final_splits'] == selected_stats['splits_after_limit'] == len(selected_ids)
    assert selected_stats['final_files'] == (4 if selected_ids else 0)
    assert selected_stats['planned_data_file_bytes'] == (400 if selected_ids else 0)
    assert captured.plan.stats().planning_counters == source
    assert selected.stats() is not captured.plan.stats()


def test_selecting_python_stats_does_not_invent_native_counters(captured):
    assert captured.plan.stats().planning_counters is None
    selected = captured.select(pa.array([10]))
    assert selected.stats().planning_counters is None
    assert selected.stats() is not captured.plan.stats()
