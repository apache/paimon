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

"""Shared requests keep Native planning independent of Python execution."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.common.predicate import Predicate
from pypaimon.execution.native.read.plan import NativeTableScanMixin
from pypaimon.read.plan import Plan
from pypaimon.read.scan_request import ScanRequest
from pypaimon.read.table_scan import TableScan
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.utils.range import Range


class _Split:
    partition = None

    def __init__(self, merged):
        self._merged = merged

    def merged_row_count(self):
        return self._merged


def _scan():
    predicate = Predicate('equal', 0, 'pt', [1])
    scan = TableScan.__new__(TableScan)
    scan.table = SimpleNamespace(
        is_primary_key_table=False, partition_keys=['pt'], options=CoreOptions(Options({})))
    scan.file_scanner = SimpleNamespace(
        predicate=predicate, _user_predicate=predicate,
        partition_key_predicate=predicate, _explicit_partition_filter=False,
        auth_has_non_partition_filter=False, limit=10,
        data_evolution=True, deletion_vectors_enabled=False,
        idx_of_this_subtask=None, number_of_para_subtasks=None,
        start_pos_of_this_subtask=None, end_pos_of_this_subtask=None,
        chunk_shuffle=None, _global_index_result=None, _row_ranges=[Range(1, 3)])
    scan.predicate = predicate
    scan.limit = 10
    scan.partition_predicate = None
    scan._read_type = [DataField(0, 'pt', AtomicType('INT'))]
    scan._incremental_snapshot_range = (2, 4)
    return scan


def test_request_recaptures_external_configuration_and_authorization_changes():
    scan = _scan()
    before = scan._scan_request()
    scanner = scan.file_scanner
    scanner._row_ranges[0].to = 8
    scanner.limit = 2
    scanner.predicate = None
    scanner.auth_has_non_partition_filter = True
    scanner.idx_of_this_subtask, scanner.number_of_para_subtasks = 1, 3
    scan._read_type.append(DataField(1, 'extra', AtomicType('STRING')))
    after = scan._scan_request()

    assert before.row_ranges == ((1, 3),)
    assert after.row_ranges == ((1, 8),)
    assert before.limit == 10 and after.limit == 2
    assert after.read_limit == 10
    assert after.predicate is None and after.read_predicate is scan.predicate
    assert after.user_predicate is scan.predicate
    assert not before.auth_has_non_partition_filter and after.auth_has_non_partition_filter
    assert (after.idx_of_this_subtask, after.number_of_para_subtasks) == (1, 3)
    assert len(before.read_type) == 1 and len(after.read_type) == 2
    assert after.incremental_snapshot_range == (2, 4)


def test_native_prepare_observes_the_latest_public_scanner_configuration():
    scan = _scan()
    with patch('pypaimon.execution.native.read.plan.native_plan', return_value=Plan([], 7)) as native:
        assert scan._try_native_plan().snapshot_id == 7
        assert native.call_args[1]['row_ranges'] == [(1, 3)]
        scan.file_scanner._row_ranges = []
        scan.file_scanner.idx_of_this_subtask, scan.file_scanner.number_of_para_subtasks = 1, 3
        assert scan._try_native_plan().snapshot_id == 7
        assert native.call_args[1]['row_ranges'] == []
        assert native.call_args[1]['row_position_shard'] == (1, 3)


def test_native_planner_and_metadata_limit_only_consume_a_shared_request():
    class NativeScan(NativeTableScanMixin):
        table = SimpleNamespace(options=CoreOptions(Options({})))

        @property
        def file_scanner(self):
            raise AssertionError('Native planning touched a Python scanner')

        def _scan_request(self):
            return ScanRequest(limit=2, read_limit=2, idx_of_this_subtask=0, number_of_para_subtasks=2)

    splits = [_Split(2), _Split(8)]
    with patch('pypaimon.execution.native.read.plan.native_plan', return_value=Plan(splits, 7)) as native:
        plan = NativeScan()._try_native_plan()
    assert plan.splits() == splits[:1]
    assert plan.snapshot_id == 7 and plan._stats_limit == 2
    assert native.call_args[1]['limit'] is None
    assert native.call_args[1]['row_position_shard'] == (0, 2)


def test_metadata_limit_uses_merged_counts_and_keeps_unknown_counts():
    request = ScanRequest(limit=3, deletion_vectors_enabled=True)
    splits = [_Split(2), _Split(2), _Split(8)]
    assert request.apply_push_down_limit(splits) == splits[:2]
    unknown = [splits[0], _Split(None), splits[2]]
    assert request.apply_push_down_limit(unknown) is unknown


@pytest.mark.parametrize('changes', [
    {'data_evolution': True, 'deletion_vectors_enabled': True},
    {'auth_has_non_partition_filter': True},
    {'predicate': Predicate('equal', 0, 'payload', [1])},
    {'explicit_partition_filter': True, 'predicate': Predicate('equal', 0, 'pt', [1])},
])
def test_metadata_limit_waits_for_row_filters_and_deletion_vectors(changes):
    class UnreadSplit:
        def merged_row_count(self):
            raise AssertionError('Unsafe metadata LIMIT inspected row counts')

    request = replace(ScanRequest(limit=1, partition_keys=('pt',)), **changes)
    splits = [UnreadSplit()]
    assert request.apply_push_down_limit(splits) is splits


def test_chunk_validation_keeps_the_original_filter_and_empty_range_selection():
    request = ScanRequest(
        partition_keys=('pt',), user_predicate=Predicate('equal', 0, 'payload', [1]))
    with pytest.raises(ValueError, match='only partition keys'):
        request.validate_chunk_shuffle_compat()
    with pytest.raises(ValueError, match='row ranges'):
        ScanRequest(row_ranges=()).validate_chunk_shuffle_compat()
