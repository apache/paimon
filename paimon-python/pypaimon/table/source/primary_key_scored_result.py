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

from dataclasses import dataclass, replace
import math

from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.read.split import DataSplit
from pypaimon.table.source.global_index_split_result import GlobalIndexSplitResult
from pypaimon.utils.range import Range


@dataclass(frozen=True, order=True)
class PrimaryKeySearchPosition:
    """A scored row addressed by its primary-key data-file position."""

    partition_bytes: bytes
    bucket: int
    data_file_name: str
    row_position: int
    score: float

    def __post_init__(self):
        if self.row_position < 0:
            raise ValueError("Row position must not be negative: %s." % self.row_position)
        if not self.data_file_name:
            raise ValueError("Data file name must not be empty.")
        if not math.isfinite(self.score):
            raise ValueError("Search score must be finite: %s." % self.score)

    def with_score(self, score):
        return replace(self, score=score)


class PrimaryKeyScoredResult(GlobalIndexSplitResult):
    """Scored result materialized as single-file physical-position splits."""

    def __init__(self, snapshot_id, source_splits, positions):
        self._snapshot_id = snapshot_id
        self._positions = tuple(positions)
        if len(set(_position_key(position) for position in positions)) != len(positions):
            raise ValueError("Primary-key search result contains duplicate physical positions.")
        self._splits = tuple(_materialize(source_splits, positions))

    @property
    def snapshot_id(self):
        return self._snapshot_id

    @property
    def positions(self):
        return self._positions

    @property
    def splits(self):
        return self._splits


def _materialize(source_splits, positions):
    sources = {}
    for split in source_splits:
        partition_bytes = _partition_bytes(split.partition)
        for index, data_file in enumerate(split.files):
            key = (partition_bytes, split.bucket, data_file.file_name)
            if key in sources:
                raise ValueError("Data file %s appears more than once." % data_file.file_name)
            sources[key] = (split, index, data_file)

    selected = {}
    for position in sorted(positions, key=_position_key):
        key = (position.partition_bytes, position.bucket, position.data_file_name)
        source = sources.get(key)
        if source is None:
            raise ValueError("Primary-key search position references unknown data file %s."
                             % position.data_file_name)
        if position.row_position >= source[2].row_count or position.row_position > 2147483647:
            raise ValueError("Primary-key search row position is outside data file %s."
                             % position.data_file_name)
        selected.setdefault(key, {})[position.row_position] = position.score

    for key, scores_by_position in selected.items():
        split, file_index, data_file = sources[key]
        deletion_files = None
        if split.data_deletion_files is not None:
            deletion_files = [split.data_deletion_files[file_index]]
        single = DataSplit([data_file], split.partition, split.bucket, False, deletion_files)
        ordered = sorted(scores_by_position)
        yield IndexedSplit(single, _ranges(ordered),
                           [scores_by_position[position] for position in ordered])


def _ranges(positions):
    result = []
    start = end = None
    for position in positions:
        if start is None:
            start = position
        elif position != end + 1:
            result.append(Range(start, end))
            start = position
        end = position
    if start is not None:
        result.append(Range(start, end))
    return result


def _partition_bytes(partition):
    to_bytes = getattr(partition, "to_bytes", None)
    if callable(to_bytes):
        return bytes(to_bytes())
    values = getattr(partition, "values", ())
    return repr(tuple(values)).encode("utf-8")


def _position_key(position):
    return (position.partition_bytes, position.bucket,
            position.data_file_name, position.row_position)
