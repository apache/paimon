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

"""Row-position selection shared by the Python and native planning adapters."""

from typing import List, Tuple

from pypaimon.read.sliced_split import SlicedSplit
from pypaimon.read.split import Split


def validate_shard(index: int, count: int) -> None:
    if not isinstance(count, int) or count <= 0:
        raise ValueError("number_of_para_subtasks must be a positive integer")
    if not isinstance(index, int) or index < 0:
        raise ValueError("idx_of_this_subtask must be a non-negative integer")
    if index >= count:
        raise ValueError("idx_of_this_subtask must be less than number_of_para_subtasks")


def validate_slice(start: int, end: int) -> None:
    if not isinstance(start, int) or not isinstance(end, int) or start < 0 or start >= end:
        raise ValueError("start_pos must be non-negative and less than end_pos; both must be integers")


def shard_range(total: int, index: int, count: int) -> Tuple[int, int]:
    base, remainder = divmod(total, count)
    start = index * base + min(index, remainder)
    return start, start + base + int(index < remainder)


def slice_append_splits(splits: List[Split], start: int, end: int) -> List[Split]:
    """Select physical rows in [start, end), preserving file order and DVs.

    File statistics have already pruned the input; residual row predicates and
    deletion vectors are applied by the reader after selecting these positions.
    An end beyond the table is naturally clamped by each file's row count.
    """
    if start >= end:
        return []
    selected = []
    offset = 0
    for split in splits:
        keep_ids, ranges = set(), {}
        for file in split.files:
            begin, stop = max(0, start - offset), min(file.row_count, end - offset)
            offset += file.row_count
            if begin < stop:
                keep_ids.add(id(file))
                if begin != 0 or stop != file.row_count:
                    ranges[file.file_name] = (begin, stop)
        if not keep_ids:
            continue
        kept = split.filter_file(lambda file: id(file) in keep_ids)
        selected.append(SlicedSplit(kept, ranges) if ranges else kept)
        if offset >= end:
            break
    return selected
