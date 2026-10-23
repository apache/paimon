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


"""Python grouping for distributed row-ID plans, selected before execution."""

import pyarrow as pa

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.utils.data_evolution_utils import retrieve_anchor_file
from pypaimon.utils.range import Range
from pypaimon.utils.range_helper import RangeHelper


def _file_groups(split):
    if isinstance(split, QueryAuthSplit):
        for group in _file_groups(split.split):
            yield QueryAuthSplit(group, split.auth_result)
    else:
        normal, dedicated = [], []
        names = set()
        for file in split.files:
            file.non_null_row_id_range()
            if file.file_name in names:
                raise ValueError('A row-ID split contains duplicate file metadata')
            names.add(file.file_name)
            if DataFileMeta.is_blob_file(file.file_name) or DataFileMeta.is_vector_file(file.file_name):
                dedicated.append(file)
            else:
                normal.append(file)
        if not normal:
            raise ValueError('A row-ID split requires a normal anchor file')
        if split.data_deletion_files is not None and len(split.data_deletion_files) != len(split.files):
            raise ValueError('Row-ID deletion metadata must match the captured data files')
        matched = set()
        for files in RangeHelper(lambda file: file.non_null_row_id_range()).merge_overlapping_ranges(normal):
            anchor_range = retrieve_anchor_file(files).non_null_row_id_range()
            if any(file.non_null_row_id_range() != anchor_range for file in files):
                raise ValueError('Overlapping row-ID anchors must have identical ranges')
            files += [file for file in dedicated if anchor_range.overlaps(file.non_null_row_id_range())]
            selected_names = {file.file_name for file in files}
            matched.update(selected_names)
            # Keep the captured physical paths and aligned deletion metadata.
            # Dedicated files may serve more than one normal anchor, but never
            # merge those owners or make the gaps between them addressable.
            group = split.filter_file(lambda file: file.file_name in selected_names)
            group.raw_convertible = len(group.files) == 1
            yield group
        if matched != names:
            raise ValueError('A row-ID split contains a dedicated file without its normal anchor')


def row_id_groups(plan):
    groups = []
    for planned in plan.splits():
        for split in _file_groups(planned):
            anchor_range = retrieve_anchor_file(split.files).non_null_row_id_range()
            groups.append((anchor_range.from_, anchor_range.to, split))
    return sorted(groups, key=lambda group: group[0])


class PythonRowIdPlan:
    """Route captured metadata with no file access or mutation state."""

    def __init__(self, plan, groups=None):
        import numpy as np
        from pypaimon.table.row.generic_row import GenericRowSerializer

        self.plan = plan
        self.groups = row_id_groups(plan) if groups is None else groups
        self._starts = np.asarray([first for first, _, _ in self.groups], dtype=np.int64)
        self._ends = np.asarray([last for _, last, _ in self.groups], dtype=np.int64)
        if any(left[1] >= right[0] for left, right in zip(self.groups, self.groups[1:])):
            raise ValueError('A row-ID plan contains overlapping file groups')
        owners = {}
        bucket_owners = []
        for first, _, split in self.groups:
            identity = (GenericRowSerializer.to_bytes(split.partition), split.bucket)
            bucket_owners.append(owners.setdefault(identity, first))
        self._bucket_owners = np.asarray(bucket_owners, dtype=np.int64)

    def _positions(self, row_ids):
        import numpy as np

        values = row_ids.to_numpy(zero_copy_only=False)
        positions = np.searchsorted(self._starts, values, side='right') - 1
        if len(values) == 0:
            return positions
        if not self.groups:
            valid = np.zeros(len(values), dtype=bool)
        else:
            valid = (values >= 0) & (positions >= 0) & (values <= self._ends[np.maximum(positions, 0)])
        if not valid.all():
            bad = values[~valid][0]
            ranges = ['[{}, {}]'.format(first, last) for first, last, _ in self.groups]
            raise ValueError('_ROW_ID {} does not belong to any valid range {}; planner snapshot is stale '
                             'or matched rows come from a different table.'.format(bad, ranges))
        return positions

    def route(self, row_ids):
        return pa.array(self._starts[self._positions(row_ids)], type=pa.int64())

    def route_by_bucket(self, row_ids):
        return pa.array(self._bucket_owners[self._positions(row_ids)], type=pa.int64())

    def select(self, row_ids):
        from pypaimon.globalindex.indexed_split import IndexedSplit

        positions = self._positions(row_ids)
        requested = {}
        for position, row_id in zip(positions, row_ids.to_pylist()):
            requested.setdefault(int(position), set()).add(row_id)
        splits = []
        for position in sorted(requested):
            split = self.groups[position][2]
            ranges = Range.to_ranges(list(requested[position]))
            if isinstance(split, QueryAuthSplit):
                splits.append(QueryAuthSplit(IndexedSplit(split.split, ranges), split.auth_result))
            else:
                splits.append(IndexedSplit(split, ranges))
        return self.plan._with_splits(splits)

    def for_plan(self, plan, groups):
        return PythonRowIdPlan(plan, groups)
