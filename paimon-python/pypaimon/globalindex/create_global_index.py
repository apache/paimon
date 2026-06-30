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

"""Build global index files from Python."""

from functools import cmp_to_key
from typing import Dict, List, Optional, Sequence, Union

import pyarrow as pa

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.common.predicate import Predicate
from pypaimon.globalindex.btree.btree_index_writer import (
    BTREE_IDENTIFIER,
    BTreeIndexWriter,
)
from pypaimon.globalindex.global_index_meta import GlobalIndexMeta
from pypaimon.globalindex.key_serializer import create_serializer
from pypaimon.globalindex.vindex.vindex_vector_global_index_reader import (
    VINDEX_IDENTIFIERS,
)
from pypaimon.globalindex.vindex.vindex_vector_index_writer import (
    VindexVectorIndexWriter,
)
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.read.split import DataSplit
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.special_fields import SpecialFields
from pypaimon.utils.range import Range
from pypaimon.utils.range_helper import RangeHelper
from pypaimon.write.commit_message import CommitMessage


def create_global_index(
    table,
    index_column: Union[str, Sequence[str]],
    index_type: str = BTREE_IDENTIFIER,
    partition_filter: Optional[Predicate] = None,
    partitions: Optional[Union[Dict[str, object], Sequence[Dict[str, object]]]] = None,
    options: Optional[Dict[str, object]] = None,
) -> int:
    """Build and commit global index files for a table.

    Returns the number of index files added to the table snapshot.
    """

    builder = GlobalIndexBuilder(
        table,
        index_column,
        index_type=index_type,
        partition_filter=partition_filter,
        partitions=partitions,
        options=options,
    )
    messages = builder.build()
    if not messages:
        return 0

    write_builder = table.new_batch_write_builder()
    commit = write_builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    return sum(len(message.index_adds) for message in messages)


class GlobalIndexBuilder:
    """Small Python builder for global indexes."""

    def __init__(
        self,
        table,
        index_column: Union[str, Sequence[str]],
        index_type: str = BTREE_IDENTIFIER,
        partition_filter: Optional[Predicate] = None,
        partitions: Optional[Union[Dict[str, object], Sequence[Dict[str, object]]]] = None,
        options: Optional[Dict[str, object]] = None,
    ):
        self._table = table
        self._index_columns = _normalize_index_columns(index_column)
        self._index_type = index_type.lower().strip()
        self._partition_filter = partition_filter
        self._partitions = partitions
        self._options = _merged_options(table, options)
        self._core_options = CoreOptions(self._options)

        if self._index_type != BTREE_IDENTIFIER and self._index_type not in VINDEX_IDENTIFIERS:
            raise ValueError(
                "Python global index build currently supports '%s' and %s, got '%s'."
                % (BTREE_IDENTIFIER, VINDEX_IDENTIFIERS, index_type)
            )
        if len(self._index_columns) != 1:
            raise ValueError(
                "Python global index build currently supports one column, got %s."
                % self._index_columns
            )
        if not self._table.options.row_tracking_enabled():
            raise ValueError(
                "Table '%s' must enable 'row-tracking.enabled=true' before "
                "creating global index." % self._table.identifier
            )
        for column in self._index_columns:
            if column not in self._table.field_dict:
                raise ValueError(
                    "Column '%s' does not exist in table '%s'."
                    % (column, self._table.identifier)
                )
        if self._index_type in VINDEX_IDENTIFIERS:
            self._validate_vindex_table()

    def build(self) -> List[CommitMessage]:
        read_builder = self._table.new_read_builder()
        partition_filter = self._resolve_partition_filter(read_builder)
        if partition_filter is not None:
            read_builder = read_builder.with_partition_filter(partition_filter)

        scan = read_builder.new_scan()
        splits = scan.plan().splits()
        if not splits:
            return []

        index_field = self._table.field_dict[self._index_columns[0]]
        if self._index_type in VINDEX_IDENTIFIERS:
            splits = _filter_non_indexable_splits(
                self._table, splits, self._index_columns)
            if not splits:
                return []

        read_type = [index_field, SpecialFields.ROW_ID]
        from pypaimon.read.table_read import TableRead

        table_read = TableRead(
            table=self._table,
            predicate=None,
            read_type=read_type,
        )
        index_path_factory = self._table.path_factory().global_index_path_factory()
        index_path = index_path_factory.global_index_root_path()

        if self._index_type == BTREE_IDENTIFIER:
            return self._build_btree(splits, index_field, table_read, index_path)
        return self._build_vindex(splits, index_field, table_read, index_path)

    def _build_btree(
        self, splits, index_field, table_read, index_path: str
    ) -> List[CommitMessage]:
        key_serializer = create_serializer(index_field.type)
        block_size = self._core_options.btree_index_block_size()
        records_per_range = self._core_options.sorted_index_records_per_range()
        if records_per_range <= 0:
            raise ValueError("sorted-index.records-per-range must be positive.")

        messages = []
        for split in _split_by_contiguous_row_range(splits):
            row_range = _calc_row_range(split)
            table = table_read.to_arrow([split])
            if table is None or table.num_rows == 0:
                continue
            rows = _extract_sorted_rows(
                table,
                self._index_columns[0],
                SpecialFields.ROW_ID.name,
                key_serializer,
            )
            if not rows:
                continue
            index_adds = []
            for chunk in _chunks(rows, records_per_range):
                writer = BTreeIndexWriter(
                    self._table.file_io,
                    index_path,
                    key_serializer,
                    block_size=block_size,
                )
                for key, row_id in chunk:
                    writer.write(key, row_id - row_range.from_)
                result_entries = writer.finish()
                index_adds.extend(
                    _to_index_manifest_entries(
                        self._table,
                        split.partition,
                        row_range,
                        index_field.id,
                        self._index_type,
                        result_entries,
                    )
                )
            if index_adds:
                messages.append(
                    CommitMessage(
                        partition=tuple(split.partition.values),
                        bucket=0,
                        new_files=[],
                        index_adds=index_adds,
                    )
                )
        return messages

    def _build_vindex(
        self, splits, index_field, table_read, index_path: str
    ) -> List[CommitMessage]:
        rows_per_shard = self._core_options.global_index_row_count_per_shard()
        if rows_per_shard <= 0:
            raise ValueError(
                "Option 'global-index.row-count-per-shard' must be greater than 0."
            )

        messages = []
        for split, row_range in _split_by_global_index_shard(
            splits, rows_per_shard
        ):
            table = table_read.to_arrow([split])
            if table is None or table.num_rows == 0:
                continue

            writer = VindexVectorIndexWriter(
                self._table.file_io,
                index_path,
                index_field.type,
                self._index_type,
                self._options.to_map(),
                index_field.name,
            )
            try:
                for vector, row_id in _extract_vector_rows(
                    table,
                    self._index_columns[0],
                    SpecialFields.ROW_ID.name,
                    row_range,
                ):
                    writer.write(vector, row_id - row_range.from_)

                index_adds = _to_index_manifest_entries(
                    self._table,
                    split.partition,
                    row_range,
                    index_field.id,
                    self._index_type,
                    writer.finish(),
                )
            finally:
                writer.close()
            if index_adds:
                messages.append(
                    CommitMessage(
                        partition=tuple(split.partition.values),
                        bucket=0,
                        new_files=[],
                        index_adds=index_adds,
                    )
                )
        return messages

    def _validate_vindex_table(self) -> None:
        bucket = self._core_options.bucket()
        if bucket != -1:
            raise ValueError(
                "Generic global index only supports unaware-bucket tables "
                "(bucket = -1), but table '%s' has bucket = %s."
                % (self._table.identifier, bucket)
            )
        if self._core_options.deletion_vectors_enabled():
            raise ValueError(
                "Generic global index does not support tables with deletion "
                "vectors enabled. Table '%s' has "
                "'deletion-vectors.enabled' = true."
                % self._table.identifier
            )

    def _resolve_partition_filter(self, read_builder) -> Optional[Predicate]:
        if self._partition_filter is not None:
            return self._partition_filter
        if self._partitions is None:
            return None

        partitions = self._partitions
        if isinstance(partitions, dict):
            partitions = [partitions]

        predicate_builder = read_builder.new_predicate_builder()
        partition_predicates = []
        for partition in partitions:
            sub_predicates = []
            for key, value in partition.items():
                if key not in self._table.partition_keys:
                    raise ValueError(
                        "Partition spec key '%s' is not a partition column. "
                        "Partition keys are: %s"
                        % (key, list(self._table.partition_keys))
                    )
                if value is None:
                    sub_predicates.append(predicate_builder.is_null(key))
                else:
                    sub_predicates.append(predicate_builder.equal(key, value))
            if sub_predicates:
                partition_predicates.append(
                    predicate_builder.and_predicates(sub_predicates))
        return predicate_builder.or_predicates(partition_predicates)


def _normalize_index_columns(index_column: Union[str, Sequence[str]]) -> List[str]:
    if isinstance(index_column, str):
        return [c.strip() for c in index_column.split(",") if c.strip()]
    return [str(c).strip() for c in index_column if str(c).strip()]


def _merged_options(table, options: Optional[Dict[str, object]]) -> Options:
    merged = dict(table.options.options.to_map())
    if options:
        merged.update(options)
    return Options(merged)


def _calc_row_range(split) -> Range:
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


def _split_by_contiguous_row_range(splits):
    result = []
    for split in splits:
        result.extend(_split_one_by_contiguous_row_range(split))
    return result


def _filter_non_indexable_splits(table, splits, index_columns):
    boundary = _find_min_non_indexable_row_id(
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
            result.append(_copy_split_with_files(split, files))
    return result


def _find_min_non_indexable_row_id(schema_manager, files, index_columns):
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


def _split_by_global_index_shard(splits, rows_per_shard):
    if rows_per_shard <= 0:
        raise ValueError(
            "Option 'global-index.row-count-per-shard' must be greater than 0."
        )

    groups = {}
    for split in splits:
        key = (_partition_key(split.partition), split.bucket)
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
                    _append_shard_split(
                        result,
                        current_group,
                        shard_start,
                        shard_end,
                        group["partition"],
                        group["bucket"],
                    )
                    current_group = [file]
                    current_group_end = file_range.to

            if current_group:
                _append_shard_split(
                    result,
                    current_group,
                    shard_start,
                    shard_end,
                    group["partition"],
                    group["bucket"],
                )

    return result


def _partition_key(partition):
    values = getattr(partition, "values", None)
    return tuple(values) if values is not None else partition


def _append_shard_split(
    result,
    files,
    shard_start,
    shard_end,
    partition,
    bucket,
):
    group_start = min(file.row_id_range().from_ for file in files)
    group_end = max(file.row_id_range().to for file in files)
    row_range = Range(max(group_start, shard_start), min(group_end, shard_end))
    result.append((
        DataSplit(
            files=list(files),
            partition=partition,
            bucket=bucket,
            raw_convertible=False,
        ),
        row_range,
    ))


def _split_one_by_contiguous_row_range(split):
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
            result.append(_copy_split_with_files(split, current_segment))
            current_segment = list(range_files)
            current_max_row_id = max_row_id

    if current_segment:
        result.append(_copy_split_with_files(split, current_segment))
    return result


def _copy_split_with_files(split, files):
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


def _extract_sorted_rows(
    table: pa.Table,
    index_column: str,
    row_id_column: str,
    key_serializer,
):
    keys = table.column(index_column).to_pylist()
    row_ids = table.column(row_id_column).to_pylist()
    rows = []
    for key, row_id in zip(keys, row_ids):
        if row_id is None:
            raise ValueError("Cannot build global index because _ROW_ID is null.")
        rows.append((key, int(row_id)))

    comparator = key_serializer.create_comparator()

    def compare(left, right):
        left_key = left[0]
        right_key = right[0]
        if left_key is None and right_key is None:
            return 0
        if left_key is None:
            return -1
        if right_key is None:
            return 1
        return comparator(left_key, right_key)

    return sorted(rows, key=cmp_to_key(compare))


def _extract_vector_rows(
    table: pa.Table,
    index_column: str,
    row_id_column: str,
    row_range: Optional[Range] = None,
):
    vectors = table.column(index_column).to_pylist()
    row_ids = table.column(row_id_column).to_pylist()
    rows = []
    for vector, row_id in zip(vectors, row_ids):
        if row_id is None:
            raise ValueError("Cannot build global index because _ROW_ID is null.")
        row_id = int(row_id)
        if row_range is not None and not row_range.contains(row_id):
            continue
        rows.append((vector, row_id))
    return rows


def _chunks(rows, size):
    for start in range(0, len(rows), size):
        yield rows[start:start + size]


def _to_index_manifest_entries(
    table,
    partition: GenericRow,
    row_range: Range,
    index_field_id: int,
    index_type: str,
    result_entries,
) -> List[IndexManifestEntry]:
    path_factory = table.path_factory().global_index_path_factory()
    entries = []
    for result in result_entries:
        file_path = path_factory.to_path(result.file_name)
        file_size = table.file_io.get_file_size(file_path)
        external_path = file_path if path_factory.is_external_path() else None
        index_file = IndexFileMeta(
            index_type=index_type,
            file_name=result.file_name,
            file_size=file_size,
            row_count=result.row_count,
            global_index_meta=GlobalIndexMeta(
                row_range_start=row_range.from_,
                row_range_end=row_range.to,
                index_field_id=index_field_id,
                extra_field_ids=None,
                index_meta=result.meta,
            ),
            external_path=external_path,
        )
        entries.append(
            IndexManifestEntry(
                kind=0,
                partition=partition,
                bucket=0,
                index_file=index_file,
            )
        )
    return entries
