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

from collections import defaultdict
import logging
from typing import Any, Iterable, List, Mapping, Optional, Sequence, Tuple

import pyarrow
import pyarrow as pa

from pypaimon.common.memory_size import MemorySize
from pypaimon.common.options.core_options import (
    CoreOptions,
    GlobalIndexSearchMode,
)
from pypaimon.common.predicate import Predicate
from pypaimon.globalindex import Range
from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.manifest.index_manifest_file import IndexManifestFile
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.execution.python.read.scanner.file_scanner import (FileScanner)
from pypaimon.execution.python.read.scanner.data_evolution_split_generator import (DataEvolutionSplitGenerator)
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.read.split import DataSplit
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.snapshot.time_travel_util import TimeTravelUtil
from pypaimon.table.special_fields import SpecialFields
from pypaimon.write.commit_message import CommitMessage
from pypaimon.execution.python.write.row_id_file_index import RowIdFileIndex
from pypaimon.write.table_delete import TableDeleteByRowId
from pypaimon.execution.python.write.writer.data_writer import DataWriter
from pypaimon.execution.python.write.writer.append_only_data_writer import AppendOnlyDataWriter

from pypaimon.write.update_validation import _authorize_update

logger = logging.getLogger(__name__)


def _filter_by_whole_file_shard(splits: List[DataSplit], sub_task_id: int, total_tasks: int) -> List[DataSplit]:
    list_ranges = []
    for split in splits:
        for file in split.files:
            list_ranges.append(file.row_id_range())

    sorted_ranges = Range.sort_and_merge_overlap(list_ranges, True, False)

    start_range, end_range = _divide_ranges(sorted_ranges, sub_task_id, total_tasks)
    if start_range is None or end_range is None:
        return []
    start_first_row_id = start_range.from_
    end_first_row_id = end_range.to

    def filter_data_file(f: DataFileMeta) -> bool:
        return start_first_row_id <= f.first_row_id <= end_first_row_id

    filtered_splits = []

    for split in splits:
        split = split.filter_file(filter_data_file)
        if split is not None:
            filtered_splits.append(split)

    return filtered_splits


def _divide_ranges(
        sorted_ranges: List[Range], sub_task_id: int, total_tasks: int
) -> Tuple[Optional[Range], Optional[Range]]:
    if not sorted_ranges:
        return None, None

    num_ranges = len(sorted_ranges)

    # If more tasks than ranges, some tasks get nothing
    if sub_task_id >= num_ranges:
        return None, None

    # Calculate balanced distribution of ranges across tasks
    base_ranges_per_task = num_ranges // total_tasks
    remainder = num_ranges % total_tasks

    # Each of the first 'remainder' tasks gets one extra range
    if sub_task_id < remainder:
        num_ranges_for_task = base_ranges_per_task + 1
        start_idx = sub_task_id * (base_ranges_per_task + 1)
    else:
        num_ranges_for_task = base_ranges_per_task
        start_idx = (remainder * (base_ranges_per_task + 1) + (sub_task_id - remainder) * base_ranges_per_task)
    end_idx = start_idx + num_ranges_for_task - 1
    return sorted_ranges[start_idx], sorted_ranges[end_idx]


class PythonTableUpdateMixin:
    """Python scan and file rewrite algorithms for the public update API."""

    def _matched_delete_row_ids(
            self, predicate: Optional[Predicate]) -> List[int]:
        scan_table = self._matched_update_scan_table()
        read_builder = scan_table.new_read_builder()
        if predicate is not None:
            read_builder.with_filter(predicate)
            read_builder.with_projection(
                list(scan_table.field_names) + [SpecialFields.ROW_ID.name]
            )
        else:
            read_builder.with_projection([SpecialFields.ROW_ID.name])

        scan = read_builder.new_scan()
        splits = scan.plan_all_files().splits()
        if (splits
                and self.table.options.native_write_enabled()
                and not any(isinstance(split, QueryAuthSplit)
                            for split in splits)):
            from pypaimon.execution.dispatch import try_native_predicate_row_ids
            row_ids = try_native_predicate_row_ids(scan_table, predicate, splits)
            if row_ids is not None:
                return row_ids
        matched = read_builder.new_read().to_arrow(splits)
        if matched.num_rows == 0:
            return []
        return matched[SpecialFields.ROW_ID.name].to_pylist()

    def _update_by_arrow_batches_with_row_id(
            self, tables: Iterable[pa.Table], commit_identifier: int
    ) -> List[CommitMessage]:
        updater = None
        try:
            for table in tables:
                cols = self.update_cols if self.update_cols is not None else [
                    c for c in table.column_names
                    if c != SpecialFields.ROW_ID.name
                ]
                if updater is None:
                    updater = self._new_row_id_updater(commit_identifier)
                updater.update_columns(table, cols)
            return [] if updater is None else updater.commit_messages
        except Exception:
            # Never delete files from CommitMessage on failure.
            # A commit can succeed even when its response raises an exception.
            raise

    @staticmethod
    def _predicate_update_file_groups(splits):
        for split in splits:
            data_split = (
                split.data_split()
                if isinstance(split, IndexedSplit) else split
            )
            deletion_files = data_split.data_deletion_files
            deletion_by_name = (
                {
                    file.file_name: deletion
                    for file, deletion in zip(
                        data_split.files, deletion_files
                    )
                }
                if deletion_files is not None else None
            )
            groups = DataEvolutionSplitGenerator._split_by_row_id(
                data_split.files
            )
            for files in groups:
                group = DataSplit(
                    files=files,
                    partition=data_split.partition,
                    bucket=data_split.bucket,
                    raw_convertible=len(files) == 1,
                    data_deletion_files=(
                        [deletion_by_name[file.file_name] for file in files]
                        if deletion_by_name is not None else None
                    ),
                    snapshot_id=data_split.snapshot_id,
                )
                if isinstance(split, IndexedSplit):
                    group_ranges = Range.sort_and_merge_overlap(
                        [file.row_id_range() for file in files], True, True
                    )
                    row_ranges = Range.and_(
                        split.row_ranges(), group_ranges
                    )
                    if not row_ranges:
                        continue
                    group = IndexedSplit(group, row_ranges)
                yield group

    def _matched_update_scan_table(self):
        snapshot = TimeTravelUtil.resolve_snapshot(self.table)
        return self.table.copy_without_time_travel({
            CoreOptions.SCALAR_INDEX_SEARCH_MODE.key():
                GlobalIndexSearchMode.FULL.value,
        })._copy_with_snapshot(snapshot)

    def _validate_predicate_update(
            self,
            assignments: Mapping[str, Any],
            read_columns: Optional[Sequence[str]],
            has_callable: bool,
            has_array: bool,
    ):
        if not self.table.options.data_evolution_enabled():
            raise ValueError(
                "update_by_predicate requires "
                "'data-evolution.enabled' = 'true'."
            )
        if not self.table.options.row_tracking_enabled():
            raise ValueError(
                "update_by_predicate requires "
                "'row-tracking.enabled' = 'true'."
            )
        if not assignments:
            raise ValueError("assignments must not be empty.")
        if read_columns and not has_callable:
            raise ValueError(
                "read_columns requires a callable assignment."
            )
        if has_callable:
            if has_array:
                raise ValueError(
                    "Callable assignments cannot be combined with Arrow "
                    "array assignments."
                )
            if not read_columns:
                raise ValueError(
                    "Callable assignments require read_columns."
                )
            for col in read_columns:
                if col not in self.table.field_names:
                    raise ValueError(
                        f"Read column {col} is not in table schema."
                    )

        partition_keys = set(self.table.partition_keys)
        for col in assignments:
            if col not in self.table.field_names:
                raise ValueError(f"Column {col} is not in table schema.")
            if col in partition_keys:
                raise ValueError(
                    "update_by_predicate does not support updating "
                    f"partition column '{col}'."
                )

    def _build_predicate_update_table(
            self,
            assignments: Mapping[str, Any],
            matched: pa.Table,
    ) -> pa.Table:
        table_schema = PyarrowFieldParser.from_paimon_schema(
            self.table.table_schema.fields
        )
        arrays = [matched[SpecialFields.ROW_ID.name]]
        fields = [pa.field(SpecialFields.ROW_ID.name, pa.int64())]
        for col, value in assignments.items():
            if callable(value):
                value = value(matched)
                if not isinstance(value, (pa.Array, pa.ChunkedArray)):
                    raise ValueError(
                        f"Callable assignment for {col} must return a "
                        "pyarrow.Array or pyarrow.ChunkedArray."
                    )
            target_field = table_schema.field(col)
            arrays.append(
                self._assignment_to_array(
                    value, target_field.type, matched.num_rows
                )
            )
            fields.append(target_field)
        return pa.Table.from_arrays(arrays, schema=pa.schema(fields))

    @staticmethod
    def _assignment_to_array(
            value: Any, data_type: pa.DataType, row_count: int):
        if isinstance(value, pa.ChunkedArray):
            array = value
        elif isinstance(value, pa.Array):
            array = value
        else:
            if isinstance(value, pa.Scalar):
                value = value.as_py()
            return pa.array([value] * row_count, type=data_type)

        if len(array) != row_count:
            raise ValueError(
                "Assignment array length must match matched row count: "
                f"{len(array)} != {row_count}."
            )
        if array.type != data_type:
            if isinstance(array, pa.ChunkedArray):
                array = pa.chunked_array(
                    [chunk.cast(data_type) for chunk in array.chunks],
                    type=data_type,
                )
            else:
                array = array.cast(data_type)
        return array

    def _delete_by_partition_filter(
            self, partition_filter: Predicate) -> List[CommitMessage]:
        _authorize_update(self.table)
        snapshot = TimeTravelUtil.resolve_snapshot(self.table)
        if snapshot is None:
            return []

        messages = {}
        manifest_list_manager = ManifestListManager(self.table)
        data_entries = FileScanner(
            self.table,
            lambda: ([], None),
            partition_predicate=partition_filter,
        ).with_all_buckets().read_manifest_entries(manifest_list_manager.read_all(snapshot))

        for entry in data_entries:
            message = self._partition_delete_message(
                messages,
                tuple(entry.partition.values),
                entry.bucket,
                snapshot.id,
            )
            message.deleted_files.append(entry.file)

        for entry in self._partition_index_entries(snapshot, partition_filter):
            message = self._partition_delete_message(
                messages,
                tuple(entry.partition.values),
                entry.bucket,
                snapshot.id,
            )
            message.index_deletes.append(IndexManifestEntry(
                kind=1,
                partition=entry.partition,
                bucket=entry.bucket,
                index_file=entry.index_file,
            ))

        return [message for message in messages.values() if not message.is_empty()]

    @staticmethod
    def _partition_delete_message(messages, partition, bucket, snapshot_id):
        key = (partition, bucket)
        if key not in messages:
            messages[key] = CommitMessage(
                partition=partition,
                bucket=bucket,
                new_files=[],
                check_from_snapshot=snapshot_id,
            )
        return messages[key]

    def _partition_index_entries(self, snapshot, partition_filter: Predicate):
        if snapshot.index_manifest is None:
            return []
        return [
            entry for entry in IndexManifestFile(self.table).read(
                snapshot.index_manifest)
            if partition_filter.test(entry.partition)
        ]

    def _partition_only_delete_filter(
            self, predicate: Optional[Predicate]) -> Optional[Predicate]:
        if predicate is None or not self.table.partition_keys:
            return None
        predicate_fields = self._predicate_fields(predicate)
        if not predicate_fields:
            return None
        partition_keys = set(self.table.partition_keys)
        if not predicate_fields.issubset(partition_keys):
            return None
        partition_index = {
            name: index for index, name in enumerate(self.table.partition_keys)
        }
        return self._rewrite_predicate_to_partition_indices(
            predicate, partition_index
        )

    def _rewrite_predicate_to_partition_indices(
            self,
            predicate: Predicate,
            partition_index: Mapping[str, int],
    ) -> Predicate:
        if predicate.method in ('and', 'or'):
            return predicate.new_literals([
                self._rewrite_predicate_to_partition_indices(
                    child, partition_index,
                )
                for child in (predicate.literals or [])
            ])
        if predicate.field not in partition_index:
            raise ValueError(
                "Partition delete predicate references non-partition "
                f"field '{predicate.field}'."
            )
        return predicate.new_index(partition_index[predicate.field])

    def _execute_predicate_update(
            self, predicate, assignments, commit_identifier, read_columns, has_callable, has_array):
        scan_table = self._matched_update_scan_table()
        read_builder = scan_table.new_read_builder()
        if predicate is not None:
            read_builder.with_filter(predicate)
        if has_callable:
            projection = list(dict.fromkeys(read_columns))
            projection.append(SpecialFields.ROW_ID.name)
            read_builder.with_projection(projection)
        else:
            read_builder.with_projection([SpecialFields.ROW_ID.name])

        plan = read_builder.new_scan().plan_all_files()
        splits = plan.splits()
        snapshot_id = plan.snapshot_id if plan.snapshot_id is not None else -1
        files_info = RowIdFileIndex.from_splits(
            snapshot_id, splits
        )
        table_read = read_builder.new_read()
        updater = self._new_row_id_updater(commit_identifier, files_info)
        try:
            if has_array:
                matched = table_read.to_arrow(splits)
                if matched.num_rows > 0:
                    update_table = self._build_predicate_update_table(
                        assignments,
                        matched,
                    )
                    updater.update_columns(
                        update_table, list(assignments.keys())
                    )
            else:
                for split in self._predicate_update_file_groups(splits):
                    matched = table_read.to_arrow([split], parallelism=1)
                    if matched.num_rows == 0:
                        continue
                    update_table = self._build_predicate_update_table(
                        assignments,
                        matched,
                    )
                    updater.update_columns(
                        update_table, list(assignments.keys())
                    )
        except Exception:
            # Never delete files from CommitMessage on failure.
            # A commit can succeed even when its response raises an exception.
            raise
        return updater.commit_messages

    def _execute_predicate_delete(self, predicate, commit_identifier):
        partition_filter = self._partition_only_delete_filter(predicate)
        if partition_filter is not None:
            return self._delete_by_partition_filter(partition_filter)

        row_ids = self._matched_delete_row_ids(predicate)
        return self._delete_by_row_id(row_ids, commit_identifier)

    def _new_python_shard_updator(self, shard_num, total_shard_count):
        return ShardTableUpdator(self.table, self.projection, self.update_cols,
                                 self.commit_user, shard_num, total_shard_count)

    def _delete_rows_with_python(self, row_ids):
        return TableDeleteByRowId(self.table).delete(list(row_ids))


class ShardTableUpdator:

    def __init__(
            self,
            table,
            projection: Optional[List[str]],
            write_cols: List[str],
            commit_user,
            shard_num: int,
            total_shard_count: int,
    ):
        from pypaimon.table.file_store_table import FileStoreTable
        self.table: FileStoreTable = table
        self.projection = projection
        self.write_cols = write_cols
        self.commit_user = commit_user
        self.total_shard_count = total_shard_count
        self.shard_num = shard_num

        self.write_pos = 0
        self.writer: Optional[SingleWriter] = None
        self.dict = defaultdict(list)

        scanner = self.table.new_read_builder().new_scan()
        plan = scanner.plan_all_files()
        self.snapshot_id = plan.snapshot_id if plan.snapshot_id is not None else -1
        splits = plan.splits()
        splits = _filter_by_whole_file_shard(splits, shard_num, total_shard_count)
        self.splits = splits

        self.row_ranges: List[(Tuple, Range)] = []
        for split in self.splits:
            if not isinstance(split, DataSplit):
                raise ValueError(f"Split {split} is not DataSplit.")
            files = split.files
            ranges = self.compute_from_files(files)
            for row_range in ranges:
                self.row_ranges.append((tuple(split.partition.values), row_range))

    @staticmethod
    def compute_from_files(files: List[DataFileMeta]) -> List[Range]:
        ranges = [file.row_id_range() for file in files]
        return Range.sort_and_merge_overlap(ranges, True, False)

    def arrow_reader(self) -> pyarrow.ipc.RecordBatchReader:
        read_builder = self.table.new_read_builder()
        read_builder.with_projection(self.projection)
        # Updates consume row ranges in split order. Parallel native readers
        # return batches in completion order, which would assign values to other rows.
        return read_builder.new_read().to_arrow_batch_reader(self.splits, parallelism=1)

    def prepare_commit(self) -> List[CommitMessage]:
        commit_messages = []
        for (partition, files) in self.dict.items():
            commit_messages.append(CommitMessage(partition, 0, files, self.snapshot_id))
        return commit_messages

    def update_by_arrow_batch(self, data: pa.RecordBatch):
        self._init_writer()

        capacity = self.writer.capacity()
        if capacity <= 0:
            raise RuntimeError("Writer has no remaining capacity.")

        # Split the batch across writers.
        first, rest = (data, None) if capacity >= data.num_rows else (data.slice(0, capacity), data.slice(capacity))

        self.writer.write(first)
        if self.writer.capacity() == 0:
            self.dict[self.writer.partition()].append(self.writer.end())
            self.writer = None

        if rest is not None:
            if self.writer is not None:
                raise RuntimeError("Should not get here, rest and current writer exist in the same time.")
            self.update_by_arrow_batch(rest)

    def _init_writer(self):
        if self.writer is None:
            if self.write_pos >= len(self.row_ranges):
                raise RuntimeError(
                    "No more row ranges to write. "
                    "Ensure you write exactly the same number of rows as read from this shard."
                )
            item = self.row_ranges[self.write_pos]
            self.write_pos += 1
            partition = item[0]
            row_range = item[1]
            writer = AppendOnlyDataWriter(self.table, partition, 0, 0, self.table.options, self.write_cols)
            # A shard maps to exactly one output file, so disable both size- and
            # row-count based rolling; otherwise target-file-row-num would split
            # the shard and SingleWriter.end() fails with "Should have one file."
            writer.target_file_size = MemorySize.of_mebi_bytes(999999999).get_bytes()
            writer.target_file_row_num = CoreOptions.TARGET_FILE_ROW_NUM.default_value()
            self.writer = SingleWriter(writer, partition, row_range.from_, row_range.to - row_range.from_ + 1)


class SingleWriter:

    def __init__(self, writer: DataWriter, partition, first_row_id: int, row_count: int):
        self.writer: DataWriter = writer
        self._partition = partition
        self.first_row_id = first_row_id
        self.row_count = row_count
        self.written_records_count = 0

    def capacity(self) -> int:
        return self.row_count - self.written_records_count

    def write(self, data: pa.RecordBatch):
        if data.num_rows > self.capacity():
            raise Exception("Data num size exceeds capacity.")
        self.written_records_count += data.num_rows
        self.writer.write(data)
        return

    def partition(self) -> Tuple:
        return self._partition

    def end(self) -> DataFileMeta:
        if self.capacity() != 0:
            raise Exception("There still capacity left in the writer.")
        files = self.writer.prepare_commit()
        if len(files) != 1:
            raise Exception("Should have one file.")
        file = files[0]
        if file.row_count != self.row_count:
            raise Exception("File row count mismatch.")
        file = file.assign_first_row_id(self.first_row_id)
        return file
