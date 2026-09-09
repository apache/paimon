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

"""Utilities for data-evolution tables."""

from bisect import bisect_left
from typing import Callable, Iterable, List, TypeVar

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.utils.range_helper import RangeHelper

T = TypeVar("T")


def split_normal_file_groups(files: List[DataFileMeta]) -> List[List[DataFileMeta]]:
    """Keep normal ranges separate and attach every intersecting dedicated file."""
    normal_files = []
    dedicated_files = []
    for file in files:
        if DataFileMeta.is_blob_file(file.file_name) or DataFileMeta.is_vector_file(file.file_name):
            dedicated_files.append(file)
        else:
            normal_files.append(file)

    helper = RangeHelper(lambda file: file.row_id_range())
    groups = helper.merge_overlapping_ranges(normal_files)
    starts = [min(file.row_id_range().from_ for file in group) for group in groups]
    ends = [max(file.row_id_range().to for file in group) for group in groups]
    unassociated = []
    for file in dedicated_files:
        file_range = file.row_id_range()
        index = bisect_left(ends, file_range.from_)
        associated = False
        while index < len(groups) and starts[index] <= file_range.to:
            groups[index].append(file)
            associated = True
            index += 1
        if not associated:
            unassociated.append(file)

    groups.extend(helper.merge_overlapping_ranges(unassociated))

    def group_start(group):
        normal_starts = [file.row_id_range().from_ for file in group
                         if not DataFileMeta.is_blob_file(file.file_name)
                         and not DataFileMeta.is_vector_file(file.file_name)]
        return min(normal_starts or [file.row_id_range().from_ for file in group])

    groups.sort(key=group_start)
    return groups


def retrieve_anchor_file(
    entries: Iterable[T],
    file_meta_func: Callable[[T], DataFileMeta] = lambda entry: entry,
) -> T:
    """Return the oldest normal file in a data-evolution row-range group."""
    anchor = None
    anchor_key = None

    for entry in entries:
        meta = file_meta_func(entry)
        if DataFileMeta.is_blob_file(meta.file_name) or DataFileMeta.is_vector_file(meta.file_name):
            continue

        key = (meta.max_sequence_number, meta.file_name)
        if anchor_key is None or key < anchor_key:
            anchor = entry
            anchor_key = key

    if anchor is None:
        raise ValueError(
            "Data-evolution deletion vectors should have a normal anchor file "
            "in each row range group."
        )

    return anchor
