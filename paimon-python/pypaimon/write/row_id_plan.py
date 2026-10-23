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


"""Serializable routing metadata for a captured row-ID mutation plan.

Ray transports Python metadata; file grouping belongs to the selected backend.
Workers reuse complete logical groups and the original snapshot.
"""

from dataclasses import dataclass, field, replace

import pyarrow as pa

from pypaimon.read.plan import Plan
from pypaimon.utils.data_evolution_utils import retrieve_anchor_file
from pypaimon.utils.range import Range


@dataclass(frozen=True)
class _RowIdAnchor:
    row_range: Range
    partition: object
    bucket: int
    anchor_file: object


@dataclass(frozen=True)
class CapturedRowIdPlan:
    table: object
    plan: Plan
    groups: list
    _execution: object = field(default=None, repr=False, compare=False)

    @property
    def snapshot_id(self):
        return self.plan.snapshot_id if self.plan.snapshot_id is not None else -1

    @property
    def first_row_ids(self):
        return [start for start, _, _ in self.groups]

    @property
    def first_row_id_index(self):
        return {start: (split, split.files) for start, _, split in self.groups}

    @property
    def valid_row_id_ranges(self):
        return Range.sort_and_merge_overlap(
            [Range(start, end) for start, end, _ in self.groups], True, True)

    @property
    def anchors(self):
        return [_RowIdAnchor(anchor.row_id_range(), split.partition, split.bucket, anchor)
                for _, _, split in self.groups
                for anchor in [retrieve_anchor_file(split.files)]]

    def route(self, row_ids):
        """Return each input row's captured logical file-group owner."""
        return self._executor().route(_row_id_array(row_ids))

    def route_by_bucket(self, row_ids):
        """Keep each captured partition/bucket under one deletion writer."""
        return self._executor().route_by_bucket(_row_id_array(row_ids))

    def select(self, row_ids):
        """Select absolute row IDs while retaining complete captured groups."""
        return self._executor().select(_row_id_array(row_ids))

    def _executor(self):
        if self._execution is None:
            from pypaimon.write.backend_dispatch import create_row_id_plan_execution
            object.__setattr__(self, '_execution', create_row_id_plan_execution(self.table, self.plan))
        return self._execution

    def for_split(self, split):
        """Keep one complete logical group and its captured snapshot metadata."""
        names = {file.file_name for file in split.files}
        groups = [group for group in self.groups
                  if group[2].bucket == split.bucket and group[2].partition == split.partition
                  and {file.file_name for file in group[2].files} == names]
        if len(groups) != 1:
            raise ValueError('Split is not a complete captured row-ID group')
        plan = self.plan._with_splits([groups[0][2]])
        return CapturedRowIdPlan(self.table, plan, groups, self._executor().for_plan(plan, groups))


def _row_id_array(row_ids):
    """Accept integer Arrow inputs without silently truncating floating IDs."""
    if not isinstance(row_ids, (pa.Array, pa.ChunkedArray)) or not pa.types.is_integer(row_ids.type):
        raise TypeError('_ROW_ID input must be an integer Arrow array')
    if row_ids.null_count:
        raise ValueError('_ROW_ID is null; planner snapshot is stale or row IDs come from a different table.')
    row_ids = row_ids.cast(pa.int64(), safe=True)
    return row_ids.combine_chunks() if isinstance(row_ids, pa.ChunkedArray) else row_ids


def _mutation_routing_split(split):
    from pypaimon.globalindex.indexed_split import IndexedSplit
    from pypaimon.read.query_auth_split import QueryAuthSplit

    if isinstance(split, QueryAuthSplit):
        complete = _mutation_routing_split(split.split)
        return split if complete is split.split else QueryAuthSplit(complete, split.auth_result)
    if isinstance(split, IndexedSplit):
        # A trusted scan's index selection limits reads, not the physical file
        # group rewritten by an update. Keep every file and deletion vector;
        # the caller's read predicate still selects the rows to mutate.
        return _mutation_routing_split(split.data_split())
    return split


def capture_row_id_plan(table, plan=None):
    """Resolve one read view before planning, then retain it across Ray tasks."""
    from pypaimon.snapshot.time_travel_util import TimeTravelUtil
    snapshot = TimeTravelUtil.resolve_snapshot(table)
    if plan is not None and (snapshot.id if snapshot is not None else None) != plan.snapshot_id:
        raise ValueError('Row-ID plan does not belong to the captured snapshot')
    table = table._copy_with_snapshot(snapshot)
    if plan is None:
        plan = table.new_read_builder().new_scan().plan_all()
    plan = replace(plan, _splits=[_mutation_routing_split(split) for split in plan.splits()])
    from pypaimon.write.backend_dispatch import create_row_id_plan_execution
    execution = create_row_id_plan_execution(table, plan)
    return CapturedRowIdPlan(table, plan, execution.groups, execution)
