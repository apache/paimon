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
from typing import Any, List, Mapping, Optional, Sequence, Tuple

import pyarrow
import pyarrow as pa

from pypaimon.common.memory_size import MemorySize
from pypaimon.common.options.core_options import (
    CoreOptions,
    GlobalIndexSearchMode,
    StartupMode,
)
from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.globalindex import Range
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.manifest.index_manifest_file import IndexManifestFile
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.scanner.file_scanner import FileScanner
from pypaimon.read.split import DataSplit
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.snapshot.time_travel_util import SCAN_KEYS, TimeTravelUtil
from pypaimon.table.special_fields import SpecialFields
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.table_delete import TableDeleteByRowId
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
from pypaimon.write.table_upsert_by_key import TableUpsertByKey
from pypaimon.write.writer.data_writer import DataWriter
from pypaimon.write.writer.append_only_data_writer import AppendOnlyDataWriter


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


class TableUpdate:
    """Common base for batch and stream table-update builders.

    Holds the shared configuration (``update_cols``, ``projection``) and the
    canonical ``commit_identifier``-aware implementations of the update /
    upsert operations. The concrete subclasses
    :class:`BatchTableUpdate` and :class:`StreamTableUpdate` expose
    mode-specific public method signatures.
    """

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

    def new_predicate_builder(self) -> PredicateBuilder:
        return self.table.new_read_builder().new_predicate_builder()

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

    def _update_by_arrow_with_row_id(
            self, table: pa.Table, commit_identifier: int
    ) -> List[CommitMessage]:
        cols = self.update_cols if self.update_cols is not None else [
            c for c in table.column_names if c != SpecialFields.ROW_ID.name
        ]
        return TableUpdateByRowId(
            self.table, self.commit_user, commit_identifier,
        ).update_columns(table, cols)

    def _upsert_by_arrow_with_key(
            self,
            table: pa.Table,
            upsert_keys: List[str],
            commit_identifier: int,
    ) -> List[CommitMessage]:
        """Shared implementation for ``upsert_by_arrow_with_key``.

        For each row in the input Arrow table:

        * If one or more rows with the same composite ``upsert_keys`` value
          already exist → update all of them in-place.
        * Otherwise → append as a new row.

        The public method lives on the concrete subclasses so each can
        expose the signature appropriate to its mode (batch vs stream).

        Args:
            table: Input Arrow table containing rows to upsert.
            upsert_keys: One or more column names forming the composite match key.
            commit_identifier: Identifier to tag the produced commit messages with.

        Returns:
            List of :class:`CommitMessage` objects to be committed.
        """
        return TableUpsertByKey(
            self.table, self.commit_user, commit_identifier
        ).upsert(table, upsert_keys, self.update_cols)

    def _upsert_by_key(
            self,
            rows,
            upsert_keys: List[str],
            commit_identifier: int,
    ) -> List[CommitMessage]:
        return TableUpsertByKey(
            self.table, self.commit_user, commit_identifier
        ).upsert_rows(rows, upsert_keys, self.update_cols)

    def _merge_into(
            self,
            source: Any,
            on,
            when_matched: Sequence,
            when_not_matched: Sequence,
            commit_identifier: int,
    ) -> List[CommitMessage]:
        from pypaimon.table.data_evolution_merge_into import merge_into

        return merge_into(
            self.table,
            source,
            on=on,
            when_matched=when_matched,
            when_not_matched=when_not_matched,
            commit_user=self.commit_user,
            commit_identifier=commit_identifier,
        )

    def _update_by_predicate(
            self,
            predicate: Optional[Predicate],
            assignments: Mapping[str, Any],
            commit_identifier: int,
    ) -> List[CommitMessage]:
        """Shared implementation for SQL-like ``UPDATE ... WHERE ...``.

        ``predicate`` identifies the target rows. ``assignments`` maps target
        column names to literal values. The method reads matching ``_ROW_ID``
        values, builds an Arrow update table, then delegates to the existing
        row-id update path.
        """
        self._validate_predicate_update(assignments)

        scan_table = self._matched_update_scan_table()
        read_builder = scan_table.new_read_builder()
        if predicate is not None:
            read_builder.with_filter(predicate)
            read_builder.with_projection(
                list(scan_table.field_names) + [SpecialFields.ROW_ID.name]
            )
        else:
            read_builder.with_projection([SpecialFields.ROW_ID.name])

        splits = read_builder.new_scan().plan().splits()
        matched = read_builder.new_read().to_arrow(splits)
        if matched.num_rows == 0:
            return []

        update_table = self._build_predicate_update_table(
            matched[SpecialFields.ROW_ID.name],
            assignments,
            matched.num_rows,
        )
        return TableUpdateByRowId(
            self.table, self.commit_user, commit_identifier,
        ).update_columns(update_table, list(assignments.keys()))

    def _matched_update_scan_table(self):
        snapshot_manager = self.table.snapshot_manager()
        snapshot = TimeTravelUtil.try_travel_to_snapshot(
            self.table.options.options,
            self.table.tag_manager(),
            snapshot_manager,
        )
        if snapshot is None:
            snapshot = snapshot_manager.get_latest_snapshot()
        if snapshot is None:
            return self.table

        dynamic_options = {
            CoreOptions.GLOBAL_INDEX_SEARCH_MODE.key():
                GlobalIndexSearchMode.FULL.value,
            CoreOptions.SCAN_MODE.key(): StartupMode.DEFAULT.value,
            CoreOptions.SCAN_SNAPSHOT_ID.key(): str(snapshot.id),
        }
        for scan_key in SCAN_KEYS:
            if (
                    scan_key != CoreOptions.SCAN_SNAPSHOT_ID.key()
                    and self.table.options.options.contains_key(scan_key)
            ):
                dynamic_options[scan_key] = None

        return self.table.copy(dynamic_options)

    def _validate_predicate_update(self, assignments: Mapping[str, Any]):
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
            row_ids,
            assignments: Mapping[str, Any],
            row_count: int,
    ) -> pa.Table:
        table_schema = PyarrowFieldParser.from_paimon_schema(
            self.table.table_schema.fields
        )
        arrays = [row_ids]
        fields = [pa.field(SpecialFields.ROW_ID.name, pa.int64())]
        for col, value in assignments.items():
            target_field = table_schema.field(col)
            arrays.append(
                self._assignment_to_array(value, target_field.type, row_count)
            )
            fields.append(target_field)
        return pa.Table.from_arrays(arrays, schema=pa.schema(fields))

    @staticmethod
    def _assignment_to_array(
            value: Any, data_type: pa.DataType, row_count: int):
        if isinstance(value, pa.ChunkedArray):
            array = value.combine_chunks()
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
            array = array.cast(data_type)
        return array

    def _delete_by_predicate(
            self,
            predicate: Optional[Predicate],
            commit_identifier: int,
    ) -> List[CommitMessage]:
        partition_filter = self._partition_only_delete_filter(predicate)
        if partition_filter is not None:
            return self._delete_by_partition_filter(partition_filter)

        row_ids = self._matched_delete_row_ids(predicate)
        return TableDeleteByRowId(self.table).delete(row_ids)

    def _delete_by_partition_filter(
            self, partition_filter: Predicate) -> List[CommitMessage]:
        snapshot = self.table.snapshot_manager().get_latest_snapshot()
        if snapshot is None:
            return []

        messages = {}
        manifest_list_manager = ManifestListManager(self.table)
        data_entries = FileScanner(
            self.table,
            lambda: ([], None),
            partition_predicate=partition_filter,
        ).read_manifest_entries(manifest_list_manager.read_all(snapshot))

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

    def _delete_by_row_id(
            self,
            row_ids: Sequence[int],
            commit_identifier: int,
    ) -> List[CommitMessage]:
        return TableDeleteByRowId(self.table).delete(list(row_ids))

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

    @staticmethod
    def _predicate_fields(predicate: Predicate) -> set:
        if predicate.field is not None:
            return {predicate.field}
        fields = set()
        for child in predicate.literals or []:
            fields.update(TableUpdate._predicate_fields(child))
        return fields

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

        splits = read_builder.new_scan().plan().splits()
        matched = read_builder.new_read().to_arrow(splits)
        if matched.num_rows == 0:
            return []
        return matched[SpecialFields.ROW_ID.name].to_pylist()


class BatchTableUpdate(TableUpdate):
    """Batch-mode table update; commit messages always use
    :data:`BATCH_COMMIT_IDENTIFIER`."""

    def update_by_arrow_with_row_id(self, table: pa.Table) -> List[CommitMessage]:
        """Apply column updates keyed by ``_ROW_ID`` to existing rows."""
        return self._update_by_arrow_with_row_id(table, BATCH_COMMIT_IDENTIFIER)

    def upsert_by_arrow_with_key(
            self, table: pa.Table, upsert_keys: List[str]
    ) -> List[CommitMessage]:
        """Upsert rows into an append-only table by one or more key columns."""
        return self._upsert_by_arrow_with_key(
            table, upsert_keys, BATCH_COMMIT_IDENTIFIER
        )

    def upsert_by_key(
            self, rows, upsert_keys: List[str]
    ) -> List[CommitMessage]:
        """Upsert rows into an append-only table by key columns."""
        return self._upsert_by_key(
            rows, upsert_keys, BATCH_COMMIT_IDENTIFIER
        )

    def update_by_predicate(
            self,
            predicate: Optional[Predicate],
            assignments: Mapping[str, Any],
    ) -> List[CommitMessage]:
        """Update rows matching ``predicate`` with literal assignments."""
        return self._update_by_predicate(
            predicate, assignments, BATCH_COMMIT_IDENTIFIER
        )

    def delete_by_predicate(
            self,
            predicate: Optional[Predicate],
    ) -> List[CommitMessage]:
        """Delete rows matching ``predicate`` using deletion vectors."""
        return self._delete_by_predicate(predicate, BATCH_COMMIT_IDENTIFIER)

    def delete_by_row_id(self, row_ids: Sequence[int]) -> List[CommitMessage]:
        """Delete rows by ``_ROW_ID`` using deletion vectors."""
        return self._delete_by_row_id(row_ids, BATCH_COMMIT_IDENTIFIER)

    def merge_into(
            self,
            source: Any,
            *,
            on,
            when_matched: Sequence = (),
            when_not_matched: Sequence = (),
    ) -> List[CommitMessage]:
        """Prepare batch MERGE INTO commit messages."""
        return self._merge_into(
            source,
            on,
            when_matched,
            when_not_matched,
            BATCH_COMMIT_IDENTIFIER,
        )


class StreamTableUpdate(TableUpdate):
    """Stream-mode table update; the same instance may drive many rounds,
    each tagged with its own ``commit_identifier``."""

    def update_by_arrow_with_row_id(
            self, table: pa.Table, commit_identifier: int
    ) -> List[CommitMessage]:
        """Apply column updates keyed by ``_ROW_ID`` to existing rows,
        tagging the produced commit messages with ``commit_identifier``."""
        return self._update_by_arrow_with_row_id(table, commit_identifier)

    def upsert_by_arrow_with_key(
            self,
            table: pa.Table,
            upsert_keys: List[str],
            commit_identifier: int,
    ) -> List[CommitMessage]:
        """Upsert rows into an append-only table by one or more key columns,
        tagging the produced commit messages with ``commit_identifier``."""
        return self._upsert_by_arrow_with_key(
            table, upsert_keys, commit_identifier
        )

    def upsert_by_key(
            self,
            rows,
            upsert_keys: List[str],
            commit_identifier: int,
    ) -> List[CommitMessage]:
        """Upsert rows into an append-only table by key columns,
        tagging the produced commit messages with ``commit_identifier``."""
        return self._upsert_by_key(
            rows, upsert_keys, commit_identifier
        )

    def update_by_predicate(
            self,
            predicate: Optional[Predicate],
            assignments: Mapping[str, Any],
            commit_identifier: int,
    ) -> List[CommitMessage]:
        """Update rows matching ``predicate`` with literal assignments,
        tagging the produced commit messages with ``commit_identifier``."""
        return self._update_by_predicate(
            predicate, assignments, commit_identifier
        )

    def delete_by_predicate(
            self,
            predicate: Optional[Predicate],
            commit_identifier: int,
    ) -> List[CommitMessage]:
        """Delete rows matching ``predicate`` using deletion vectors,
        tagging the produced commit messages with ``commit_identifier``."""
        return self._delete_by_predicate(predicate, commit_identifier)

    def delete_by_row_id(
            self,
            row_ids: Sequence[int],
            commit_identifier: int,
    ) -> List[CommitMessage]:
        """Delete rows by ``_ROW_ID`` using deletion vectors,
        tagging the produced commit messages with ``commit_identifier``."""
        return self._delete_by_row_id(row_ids, commit_identifier)

    def merge_into(
            self,
            source: Any,
            *,
            on,
            when_matched: Sequence = (),
            when_not_matched: Sequence = (),
            commit_identifier: int,
    ) -> List[CommitMessage]:
        """Prepare stream MERGE INTO commit messages."""
        return self._merge_into(
            source,
            on,
            when_matched,
            when_not_matched,
            commit_identifier,
        )


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
        plan = scanner.plan()
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
        return read_builder.new_read().to_arrow_batch_reader(self.splits)

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
