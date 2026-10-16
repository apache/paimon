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
        self._splits = tuple(_materialize(
            source_splits, positions, snapshot_id))
        self._native_result = None

    @property
    def snapshot_id(self):
        return self._snapshot_id

    @property
    def positions(self):
        return self._positions

    @property
    def splits(self):
        # Exposing mutable Python splits transfers metadata ownership to the
        # caller. A later native scan must observe any edits to that view.
        self._native_result = None
        return self._splits

    def __getstate__(self):
        # GlobalIndexResult owns a live core table. Transport the materialized
        # Python metadata. copy.copy also uses this protocol and shares the
        # mutable splits, so the original must stop using its native cache too.
        self._native_result = None
        state = self.__dict__.copy()
        state.pop('_native_result', None)
        return state

    @classmethod
    def _from_native(cls, table, result):
        """Wrap core GlobalIndexResult; retain it for direct native scan handoff."""
        from pypaimon.read.native_plan import _partition_fields
        from pypaimon.read.split_serializer import deserialize_split_v1

        value = cls.__new__(cls)
        value._snapshot_id = result.snapshot_id() or 0
        value._positions = tuple(PrimaryKeySearchPosition(*position) for position in result.positions())
        value._splits = tuple(deserialize_split_v1(
            split, _partition_fields(table), table.trimmed_primary_keys_fields)
            for split in result.splits())
        value._native_result = result
        return value


def _materialize(source_splits, positions, snapshot_id):
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
        single = DataSplit(
            [data_file], split.partition, split.bucket, False, deletion_files,
            snapshot_id=snapshot_id)
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
    from pypaimon.table.row.generic_row import GenericRowSerializer

    # Java PrimaryKeySearchPosition compares serialized BinaryRow partitions.
    # Reuse the wire encoder for both GenericRow and BinaryRow, including the
    # empty partition's arity prefix and eight-byte body.
    return bytes(GenericRowSerializer.to_bytes(partition))


def _position_key(position):
    return (position.partition_bytes, position.bucket,
            position.data_file_name, position.row_position)
