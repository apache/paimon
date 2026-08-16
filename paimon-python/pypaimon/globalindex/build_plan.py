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

"""Reusable global index build planning helpers."""

from typing import List, Optional, Sequence

from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.read.split import DataSplit
from pypaimon.utils.range import Range
from pypaimon.utils.range_helper import RangeHelper


def calc_row_range(split) -> Range:
    ranges = []
    for file in split.files:
        row_range = file.row_id_range()
        if row_range is None:
            raise ValueError(
                "Cannot build global index because file '%s' has no row id range."
                % file.file_name
            )
        ranges.append(row_range)
    if not ranges:
        raise ValueError("Cannot build global index for an empty split.")
    merged = Range.sort_and_merge_overlap(ranges, True, True)
    return Range(merged[0].from_, merged[-1].to)


def unindexed_row_ranges(
    table,
    snapshot,
    partition_filter,
    index_field_id: int,
    index_type: str,
) -> List[Range]:
    next_row_id = getattr(snapshot, "next_row_id", None)
    if snapshot is None or next_row_id is None or next_row_id <= 0:
        return []

    indexed_ranges = indexed_row_ranges(
        table, snapshot, partition_filter, index_field_id, index_type)
    return Range.sort_and_merge_overlap(
        Range(0, next_row_id - 1).exclude(indexed_ranges), True)


def indexed_row_ranges(
    table,
    snapshot,
    partition_filter,
    index_field_id: int,
    index_type: str,
) -> List[Range]:
    from pypaimon.index.index_file_handler import IndexFileHandler

    ranges = []
    for entry in IndexFileHandler(table).scan(snapshot):
        if getattr(entry, "kind", 0) != 0:
            continue
        if (partition_filter is not None
                and not partition_filter.test(entry.partition)):
            continue

        index_file = entry.index_file
        meta = index_file.global_index_meta
        if (
            meta is None
            or index_file.index_type != index_type
            or meta.index_field_id != index_field_id
            or meta.extra_field_ids
        ):
            continue
        ranges.append(Range(meta.row_range_start, meta.row_range_end))
    return Range.sort_and_merge_overlap(ranges, True)


def split_by_contiguous_row_range(splits):
    result = []
    for split in splits:
        result.extend(split_one_by_contiguous_row_range(split))
    return result


def split_by_contiguous_unindexed_row_range(splits, unindexed_ranges):
    result = []
    for split in split_by_contiguous_row_range(splits):
        row_range = calc_row_range(split)
        for index_range in Range.and_([row_range], unindexed_ranges):
            index_split = indexed_split_for_row_range(split, index_range)
            if index_split is not None:
                result.append((index_split, index_range))
    return result


def indexed_split_for_row_range(split, row_range):
    files = [
        file for file in split.files
        if file.row_id_range() is not None
        and file.row_id_range().overlaps(row_range)
    ]
    if not files:
        return None
    return IndexedSplit(copy_split_with_files(split, files), [row_range])


def filter_non_indexable_splits(table, splits, index_columns: Sequence[str]):
    boundary = find_min_non_indexable_row_id(
        table.schema_manager,
        [file for split in splits for file in split.files],
        index_columns,
    )
    if boundary is None:
        return splits

    result = []
    for split in splits:
        files = [
            file for file in split.files
            if file.row_id_range() is not None
            and file.row_id_range().from_ < boundary
        ]
        if files:
            result.append(copy_split_with_files(split, files))
    return result


def find_min_non_indexable_row_id(schema_manager, files, index_columns):
    schema_contains_columns = {}
    index_column_set = set(index_columns)
    boundary = None
    for file in files:
        row_range = file.row_id_range()
        if row_range is None:
            continue

        schema_id = file.schema_id
        if schema_id not in schema_contains_columns:
            schema = schema_manager.get_schema(schema_id)
            schema_field_names = {field.name for field in schema.fields}
            schema_contains_columns[schema_id] = (
                index_column_set.issubset(schema_field_names)
            )
        if not schema_contains_columns[schema_id]:
            if boundary is None or row_range.from_ < boundary:
                boundary = row_range.from_
    return boundary


def split_by_global_index_shard(
    splits,
    rows_per_shard: int,
    row_ranges_to_build: Optional[List[Range]] = None,
):
    if rows_per_shard <= 0:
        raise ValueError(
            "Option 'global-index.row-count-per-shard' must be greater than 0."
        )
    if row_ranges_to_build is not None:
        row_ranges_to_build = Range.sort_and_merge_overlap(
            row_ranges_to_build, True)
        if not row_ranges_to_build:
            return []

    groups = {}
    for split in splits:
        key = (partition_key(split.partition), split.bucket)
        if key not in groups:
            groups[key] = {
                "partition": split.partition,
                "bucket": split.bucket,
                "files": [],
            }
        for file in split.files:
            if file.row_id_range() is None:
                continue
            groups[key]["files"].append(file)

    result = []
    for group in groups.values():
        files_by_shard = {}
        for file in group["files"]:
            file_range = file.row_id_range()
            start_shard = file_range.from_ // rows_per_shard
            end_shard = file_range.to // rows_per_shard
            for shard_id in range(start_shard, end_shard + 1):
                shard_start = shard_id * rows_per_shard
                files_by_shard.setdefault(shard_start, []).append(file)

        for shard_start in sorted(files_by_shard):
            shard_end = shard_start + rows_per_shard - 1
            shard_files = sorted(
                files_by_shard[shard_start],
                key=lambda file: file.row_id_range().from_,
            )
            current_group = []
            current_group_end = None
            for file in shard_files:
                file_range = file.row_id_range()
                if not current_group:
                    current_group.append(file)
                    current_group_end = file_range.to
                elif file_range.from_ <= current_group_end + 1:
                    current_group.append(file)
                    current_group_end = max(current_group_end, file_range.to)
                else:
                    append_shard_split(
                        result,
                        current_group,
                        shard_start,
                        shard_end,
                        group["partition"],
                        group["bucket"],
                        row_ranges_to_build,
                    )
                    current_group = [file]
                    current_group_end = file_range.to

            if current_group:
                append_shard_split(
                    result,
                    current_group,
                    shard_start,
                    shard_end,
                    group["partition"],
                    group["bucket"],
                    row_ranges_to_build,
                )

    return result


def partition_key(partition):
    values = getattr(partition, "values", None)
    return tuple(values) if values is not None else partition


def append_shard_split(
    result,
    files,
    shard_start,
    shard_end,
    partition,
    bucket,
    row_ranges_to_build,
):
    group_start = min(file.row_id_range().from_ for file in files)
    group_end = max(file.row_id_range().to for file in files)
    row_range = Range(max(group_start, shard_start), min(group_end, shard_end))
    task_ranges = (
        [row_range]
        if row_ranges_to_build is None
        else Range.and_([row_range], row_ranges_to_build)
    )
    if not task_ranges:
        return

    data_split = DataSplit(
        files=list(files),
        partition=partition,
        bucket=bucket,
        raw_convertible=False,
    )
    for task_range in task_ranges:
        result.append((IndexedSplit(data_split, [task_range]), task_range))


def split_one_by_contiguous_row_range(split):
    for file in split.files:
        if file.row_id_range() is None:
            raise ValueError(
                "Cannot build global index because file '%s' has no row id range."
                % file.file_name
            )

    range_helper = RangeHelper(lambda file: file.row_id_range())
    ranges = range_helper.merge_overlapping_ranges(split.files)
    if not ranges:
        return []

    result = []
    current_segment = []
    current_max_row_id = None
    for range_files in ranges:
        min_row_id = min(file.row_id_range().from_ for file in range_files)
        max_row_id = max(file.row_id_range().to for file in range_files)
        if (
            not current_segment
            or current_max_row_id is None
            or current_max_row_id >= min_row_id - 1
        ):
            current_segment.extend(range_files)
            current_max_row_id = max_row_id
        else:
            result.append(copy_split_with_files(split, current_segment))
            current_segment = list(range_files)
            current_max_row_id = max_row_id

    if current_segment:
        result.append(copy_split_with_files(split, current_segment))
    return result


def copy_split_with_files(split, files):
    data_deletion_files = None
    if getattr(split, "data_deletion_files", None) is not None:
        index_by_file = {id(file): i for i, file in enumerate(split.files)}
        data_deletion_files = [
            split.data_deletion_files[index_by_file[id(file)]]
            for file in files
        ]
    return DataSplit(
        files=list(files),
        partition=split.partition,
        bucket=split.bucket,
        raw_convertible=getattr(split, "raw_convertible", False),
        data_deletion_files=data_deletion_files,
    )
