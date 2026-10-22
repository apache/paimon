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

"""Captured scan configuration and metadata-only rules shared by backends."""

from dataclasses import dataclass, replace
from typing import Any, List, Optional, Sequence, Tuple

from pypaimon.common.predicate import Predicate
from pypaimon.read.push_down_utils import (
    _get_all_fields, predicate_for_physical_fields, predicate_matches_physical_names,
    rewrite_predicate_indices, trim_and_transform_predicate, trim_predicate_by_fields)
from pypaimon.read.scan_distribution import validate_shard, validate_slice
from pypaimon.read.split import DataSplit
from pypaimon.schema.data_types import DataField
from pypaimon.utils.range import Range


@dataclass(frozen=True)
class ScanRequest:
    """One scan preparation's configuration, without an execution backend.

    Scanner filters may have been changed by authorization or callers using
    the public ``file_scanner``. Keep the reader's original predicate and LIMIT
    separate from those effective scanner settings. Row ranges and projection
    lists are copied into tuples; shared predicate and index-result values
    retain their public identities.
    """

    is_primary_key_table: bool = False
    partition_keys: Tuple[str, ...] = ()
    predicate: Optional[Predicate] = None
    user_predicate: Optional[Predicate] = None
    partition_key_predicate: Optional[Predicate] = None
    explicit_partition_filter: bool = False
    auth_has_non_partition_filter: bool = False
    limit: Optional[int] = None
    data_evolution: bool = False
    deletion_vectors_enabled: bool = False
    idx_of_this_subtask: Optional[int] = None
    number_of_para_subtasks: Optional[int] = None
    start_pos_of_this_subtask: Optional[int] = None
    end_pos_of_this_subtask: Optional[int] = None
    chunk_shuffle: Optional[Tuple[int, int]] = None
    global_index_result: Any = None
    row_ranges: Optional[Tuple[Tuple[int, int], ...]] = None
    read_predicate: Optional[Predicate] = None
    read_limit: Optional[int] = None
    partition_filter: Optional[Predicate] = None
    read_type: Optional[Tuple[DataField, ...]] = None

    @classmethod
    def for_table(cls, table, predicate=None, limit=None, partition_predicate=None):
        return cls(
            is_primary_key_table=table.is_primary_key_table,
            partition_keys=tuple(table.partition_keys or ()),
            predicate=predicate,
            user_predicate=predicate,
            partition_key_predicate=scan_partition_predicate(table, predicate, partition_predicate),
            explicit_partition_filter=partition_predicate is not None,
            limit=limit,
            data_evolution=table.options.data_evolution_enabled(),
            deletion_vectors_enabled=table.options.deletion_vectors_enabled(),
        )

    def with_shard(self, index, count):
        validate_shard_selection(index, count, self.start_pos_of_this_subtask)
        return replace(self, idx_of_this_subtask=index, number_of_para_subtasks=count)

    def with_slice(self, start, end):
        validate_slice_selection(start, end, self.is_primary_key_table, self.idx_of_this_subtask)
        return replace(self, start_pos_of_this_subtask=start, end_pos_of_this_subtask=end)

    def with_global_index_result(self, result):
        validate_global_index_selection(self.row_ranges)
        return replace(self, global_index_result=result)

    def with_row_ranges(self, row_ranges):
        ranges = normalize_row_ranges(row_ranges, self.data_evolution, self.global_index_result)
        return replace(self, row_ranges=tuple((range_.from_, range_.to) for range_ in ranges))

    def with_chunk_shuffle(self, seed, chunk_size):
        validate_chunk_shuffle(seed, chunk_size)
        return replace(self, chunk_shuffle=(seed, chunk_size))

    def validate_chunk_shuffle_compat(self) -> None:
        validate_chunk_shuffle_compat(
            is_primary_key_table=self.is_primary_key_table,
            start_pos_of_this_subtask=self.start_pos_of_this_subtask,
            limit=self.limit,
            global_index_result=self.global_index_result,
            row_ranges=self.row_ranges,
            auth_has_non_partition_filter=self.auth_has_non_partition_filter,
            user_predicate=self.user_predicate,
            partition_keys=self.partition_keys,
        )

    def apply_push_down_limit(self, splits: List[DataSplit]) -> List[DataSplit]:
        non_partition_filter = (
            self.limit is not None
            and not (self.data_evolution and self.deletion_vectors_enabled)
            and has_non_partition_filter(self.predicate, self.partition_keys))
        return apply_push_down_limit(
            splits,
            limit=self.limit,
            data_evolution=self.data_evolution,
            deletion_vectors_enabled=self.deletion_vectors_enabled,
            non_partition_filter=non_partition_filter,
            auth_has_non_partition_filter=self.auth_has_non_partition_filter,
            explicit_partition_filter=self.explicit_partition_filter,
            predicate=self.predicate,
        )


def validate_chunk_shuffle_compat(
        *, is_primary_key_table, start_pos_of_this_subtask, limit,
        global_index_result, row_ranges, auth_has_non_partition_filter,
        user_predicate, partition_keys) -> None:
    if is_primary_key_table:
        raise ValueError("chunk_shuffle only supports append tables")
    if start_pos_of_this_subtask is not None:
        raise ValueError("chunk_shuffle cannot combine with with_slice")
    if limit is not None:
        raise ValueError("chunk_shuffle cannot combine with limit")
    if global_index_result is not None:
        raise ValueError("chunk_shuffle cannot combine with global index")
    if row_ranges is not None:
        raise ValueError("chunk_shuffle cannot combine with row ranges")
    if auth_has_non_partition_filter:
        raise ValueError("chunk_shuffle cannot combine with authorization filters evaluated during reading")
    # Row predicates would shrink each chunk's effective row count, breaking
    # the chunk_size contract expected by DataLoader callers.
    if user_predicate is not None:
        partition_keys = set(partition_keys or [])
        non_partition_fields = _get_all_fields(user_predicate) - partition_keys
        if non_partition_fields or not predicate_matches_physical_names(user_predicate):
            raise ValueError(
                "chunk_shuffle predicate must reference only partition keys; "
                "got non-partition fields: "
                f"{sorted(non_partition_fields)}"
            )


def apply_push_down_limit(
        splits: List[DataSplit], *, limit, data_evolution,
        deletion_vectors_enabled, non_partition_filter,
        auth_has_non_partition_filter, explicit_partition_filter,
        predicate) -> List[DataSplit]:
    """Apply Java's DV-aware metadata LIMIT only when no row filter remains."""
    if limit is None:
        return splits
    if data_evolution and deletion_vectors_enabled:
        return splits
    if (non_partition_filter or auth_has_non_partition_filter
            # An explicit selector replaces ordinary partition pruning.
            # Count rows only after the reader has checked the full filter.
            or (explicit_partition_filter and predicate is not None)):
        return splits

    scanned_row_count = 0
    limited_splits: List[DataSplit] = []
    for split in splits:
        merged = split.merged_row_count()
        if merged is None:
            return splits
        limited_splits.append(split)
        scanned_row_count += merged
        if scanned_row_count >= limit:
            return limited_splits
    return splits


def has_non_partition_filter(predicate: Optional[Predicate], partition_keys: Sequence[str]) -> bool:
    """Mirror Java ``SnapshotReaderImpl.hasNonPartitionFilter``."""
    if predicate is None:
        return False
    if not predicate_matches_physical_names(predicate):
        return True
    partition_keys = set(partition_keys or [])
    return not _get_all_fields(predicate).issubset(partition_keys)


def scan_partition_predicate(table, predicate, partition_predicate):
    """Bind the public partition selection without constructing a scanner."""
    if partition_predicate is None:
        physical = predicate_for_physical_fields(predicate, table.fields)
        return trim_and_transform_predicate(physical, table.field_names, table.partition_keys)
    return rewrite_predicate_indices(
        trim_predicate_by_fields(partition_predicate, table.partition_keys),
        table.partition_keys_fields)


def validate_shard_selection(index, count, slice_start):
    validate_shard(index, count)
    if slice_start is not None:
        raise ValueError("with_shard and with_slice cannot be used simultaneously")


def validate_slice_selection(start, end, is_primary_key_table, shard_index):
    validate_slice(start, end)
    if is_primary_key_table:
        raise NotImplementedError("Primary key tables do not support with_slice(); use with_shard instead")
    if shard_index is not None:
        raise ValueError("with_slice and with_shard cannot be used simultaneously")


def validate_global_index_selection(row_ranges):
    if row_ranges is not None:
        raise ValueError("with_global_index_result and with_row_ranges are mutually exclusive")


def normalize_row_ranges(row_ranges, data_evolution, global_index_result):
    if not data_evolution:
        raise ValueError("Row ranges are only supported for data evolution tables")
    if row_ranges is None:
        raise ValueError("row_ranges cannot be None")
    if global_index_result is not None:
        raise ValueError("with_row_ranges and with_global_index_result are mutually exclusive")
    return Range.sort_and_merge_overlap(list(row_ranges), True)


def validate_chunk_shuffle(seed, chunk_size):
    if not isinstance(seed, int):
        raise ValueError("chunk_shuffle seed must be an int")
    if not isinstance(chunk_size, int) or chunk_size <= 0:
        raise ValueError("chunk_shuffle chunk_size must be a positive int")
