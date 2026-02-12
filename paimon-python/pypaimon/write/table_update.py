################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from collections import defaultdict
from typing import List, Optional, Tuple

import pyarrow
import pyarrow as pa

from pypaimon.common.memory_size import MemorySize
from pypaimon.globalindex import Range
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.split import DataSplit
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
from pypaimon.write.writer.data_writer import DataWriter
from pypaimon.write.writer.append_only_data_writer import AppendOnlyDataWriter


def _filter_by_whole_file_shard(splits: List[DataSplit], sub_task_id: int, total_tasks: int) -> List[DataSplit]:
    list_ranges = []
    for split in splits:
        for file in split.files:
            first_row_id = file.first_row_id
            list_ranges.append(Range(first_row_id, first_row_id + file.row_count - 1))

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


class TableUpdate:
    def __init__(self, table, commit_user):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.commit_user = commit_user
        self.update_cols = None
        self.projection = None

    def with_update_type(self, update_cols: List[str]):
        for col in update_cols:
            if col not in self.table.field_names:
                raise ValueError(f"Column {col} is not in table schema.")
        if len(update_cols) == len(self.table.field_names):
            update_cols = None
        self.update_cols = update_cols
        return self

    def with_read_projection(self, projection: List[str]):
        self.projection = projection

    def new_shard_updator(self, shard_num: int, total_shard_count: int):
        """Create a shard updater for scan+rewrite style updates.

        Args:
            shard_num: Index of this shard/subtask.
            total_shard_count: Total number of shards/subtasks.
        """
        return ShardTableUpdator(
            self.table,
            self.projection,
            self.update_cols,
            self.commit_user,
            shard_num,
            total_shard_count,
        )

    def update_by_arrow_with_row_id(self, table: pa.Table) -> List[CommitMessage]:
        update_by_row_id = TableUpdateByRowId(self.table, self.commit_user)
        update_by_row_id.update_columns(table, self.update_cols)
        return update_by_row_id.commit_messages


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
        splits = scanner.plan().splits()
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
        ranges = []
        for file in files:
            ranges.append(Range(file.first_row_id, file.first_row_id + file.row_count - 1))

        return Range.sort_and_merge_overlap(ranges, True, False)

    def arrow_reader(self) -> pyarrow.ipc.RecordBatchReader:
        read_builder = self.table.new_read_builder()
        read_builder.with_projection(self.projection)
        return read_builder.new_read().to_arrow_batch_reader(self.splits)

    def prepare_commit(self) -> List[CommitMessage]:
        commit_messages = []
        for (partition, files) in self.dict.items():
            commit_messages.append(CommitMessage(partition, 0, files))
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
            writer.target_file_size = MemorySize.of_mebi_bytes(999999999).get_bytes()
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
