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

"""Convert public Python split values for Native execution."""

from pypaimon.read import split_serializer


def _native_read_split(split, ranges_use_row_ids):
    """Express Python's file slices with the existing indexed-split selector."""
    from pypaimon.globalindex.indexed_split import IndexedSplit
    from pypaimon.read.query_auth_split import QueryAuthSplit
    from pypaimon.read.sliced_split import SlicedSplit
    from pypaimon.utils.range import Range

    def select_rows(base, ranges):
        if isinstance(base, QueryAuthSplit):
            return QueryAuthSplit(select_rows(base.split, ranges), base.auth_result)
        if isinstance(base, IndexedSplit):
            return select_rows(base.data_split(), Range.and_(base.row_ranges(), ranges))
        return IndexedSplit(base, ranges)

    if isinstance(split, QueryAuthSplit):
        base = _native_read_split(split.split, ranges_use_row_ids)
        return split if base is split.split else QueryAuthSplit(base, split.auth_result)
    if isinstance(split, IndexedSplit):
        base = _native_read_split(split.data_split(), ranges_use_row_ids)
        return split if base is split.data_split() else select_rows(base, split.row_ranges())
    if not isinstance(split, SlicedSplit):
        return split
    if ranges_use_row_ids is None:
        raise ValueError('Native file-slice conversion requires the reader row-tracking mode')
    ranges = []
    offset = 0
    slices = split.shard_file_idx_map()
    for file in split.files:
        start, end = slices.get(file.file_name, (0, file.row_count))
        start, end = max(0, start), min(file.row_count, end)
        if start < end:
            base = file.first_row_id if ranges_use_row_ids else offset
            if base is None:
                raise ValueError("Row-tracked file '%s' is missing first_row_id" % file.file_name)
            ranges.append(Range(base + start, base + end - 1))
        offset += file.row_count
    return select_rows(_native_read_split(split.data_split(), ranges_use_row_ids), ranges)


def native_split_from_python(split, *, ranges_use_row_ids=None):
    """Convert a Python-planned split for the Rust reader.

    Vector scores intentionally stay on the Python IndexedSplit. Rust needs
    only its row ranges to perform the physical read. File slices need the
    reader's tracking mode to map their local offsets to those ranges.
    """
    from pypaimon_rust.datafusion import Split as NativeSplit
    return NativeSplit.deserialize(split_serializer.serialize_split_v1(
        _native_read_split(split, ranges_use_row_ids), include_scores=False))
