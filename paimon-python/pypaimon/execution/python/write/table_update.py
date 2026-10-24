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

from bisect import bisect_right
from collections import deque
from itertools import islice
import logging
from typing import Any, Iterable, List, Mapping, Optional, Tuple

import pyarrow
import pyarrow as pa

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
from pypaimon.read.split import DataSplit
from pypaimon.read.read_type import OutputProjection
from pypaimon.read.read_support import _ClosableArrowBatchReader, close_read_source
from pypaimon.read.table_read import TableRead
from pypaimon.schema.arrow_schema import normalize_arrow_write_nullability
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.snapshot.time_travel_util import TimeTravelUtil
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.table.special_fields import SpecialFields
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.update_validation import predicate_fields
from pypaimon.execution.python.write.row_id_file_index import RowIdFileIndex
from pypaimon.write.table_delete import TableDeleteByRowId
from pypaimon.utils.arrow_utils import zero_column_batch

from pypaimon.write.update_validation import _authorize_update

logger = logging.getLogger(__name__)


def _filter_by_whole_file_shard(splits: List[DataSplit], sub_task_id: int, total_tasks: int) -> List[DataSplit]:
    list_ranges = []
    for split in splits:
        for file in split.files:
            if not (DataFileMeta.is_blob_file(file.file_name) or DataFileMeta.is_vector_file(file.file_name)):
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


class PythonTableUpdate:
    """Execute an update using the public configuration."""

    def __init__(self, context, commit_identifier=BATCH_COMMIT_IDENTIFIER, predicate=None,
                 *, has_callable=False, has_array=False):
        self._context = context
        self._commit_identifier = commit_identifier
        self._predicate = predicate
        self._has_callable = has_callable
        self._has_array = has_array

    def update_by_arrow_with_row_id(self, data):
        context = self._context
        columns = context.update_cols if context.update_cols is not None else [
            name for name in data.column_names if name != SpecialFields.ROW_ID.name]
        return self._new_row_id_updater(self._commit_identifier).update_columns(data, columns)

    def update_by_arrow_batches_with_row_id(self, tables):
        return self._update_by_arrow_batches_with_row_id(tables, self._commit_identifier)

    def delete_by_row_id(self, row_ids):
        return self._delete_rows_with_python(row_ids)

    def update(self, assignments, read_columns):
        return self._execute_predicate_update(
            self._predicate, assignments, self._commit_identifier, read_columns,
            self._has_callable, self._has_array)

    def delete(self):
        return self._execute_predicate_delete(self._predicate, self._commit_identifier)

    def _new_row_id_updater(self, commit_identifier, _precomputed_files_info=None):
        from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
        context = self._context
        _authorize_update(context.table)
        return TableUpdateByRowId._from_python(
            context.table, context.commit_user, commit_identifier,
            _precomputed_files_info=_precomputed_files_info)

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
        splits = scan.plan_all().splits()
        matched = read_builder.new_read().to_arrow(splits)
        if matched.num_rows == 0:
            return []
        return matched[SpecialFields.ROW_ID.name].to_pylist()

    def _update_by_arrow_batches_with_row_id(
            self, tables: Iterable[pa.Table], commit_identifier: int
    ) -> List[CommitMessage]:
        context = self._context
        updater = None
        try:
            for table in tables:
                cols = context.update_cols if context.update_cols is not None else [
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
        context = self._context
        snapshot = TimeTravelUtil.resolve_snapshot(context.table)
        return context.table.copy_without_time_travel({
            CoreOptions.SCALAR_INDEX_SEARCH_MODE.key():
                GlobalIndexSearchMode.FULL.value,
        })._copy_with_snapshot(snapshot)

    def _build_predicate_update_table(
            self,
            assignments: Mapping[str, Any],
            matched: pa.Table,
    ) -> pa.Table:
        context = self._context
        table_schema = PyarrowFieldParser.from_paimon_schema(
            context.table.table_schema.fields
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
            PythonTableUpdate._validate_assignment_struct_fields(
                array.type, data_type)
            if isinstance(array, pa.ChunkedArray):
                array = pa.chunked_array(
                    [chunk.cast(data_type) for chunk in array.chunks],
                    type=data_type,
                )
            else:
                array = array.cast(data_type)
        return array

    @staticmethod
    def _validate_assignment_struct_fields(source, target):
        if pa.types.is_struct(source) and pa.types.is_struct(target):
            source_fields = {field.name: field for field in source}
            target_names = {field.name for field in target}
            shared_names = source_fields.keys() & target_names
            if ([field.name for field in source if field.name in shared_names]
                    != [field.name for field in target if field.name in shared_names]):
                raise ValueError("Assignment struct fields must match in order.")
            for target_field in target:
                source_field = source_fields.get(target_field.name)
                if source_field is not None:
                    PythonTableUpdate._validate_assignment_struct_fields(
                        source_field.type, target_field.type)
        elif pa.types.is_map(source) and pa.types.is_map(target):
            PythonTableUpdate._validate_assignment_struct_fields(
                source.key_type, target.key_type)
            PythonTableUpdate._validate_assignment_struct_fields(
                source.item_type, target.item_type)
        else:
            list_types = (pa.types.is_list, pa.types.is_large_list,
                          pa.types.is_fixed_size_list)
            if (any(check(source) for check in list_types)
                    and any(check(target) for check in list_types)):
                PythonTableUpdate._validate_assignment_struct_fields(
                    source.value_type, target.value_type)

    def _delete_by_partition_filter(
            self, partition_filter: Predicate) -> List[CommitMessage]:
        context = self._context
        _authorize_update(context.table)
        snapshot = TimeTravelUtil.resolve_snapshot(context.table)
        if snapshot is None:
            return []

        messages = {}
        manifest_list_manager = ManifestListManager(context.table)
        data_entries = FileScanner(
            context.table,
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
        context = self._context
        if snapshot.index_manifest is None:
            return []
        return [
            entry for entry in IndexManifestFile(context.table).read(
                snapshot.index_manifest)
            if partition_filter.test(entry.partition)
        ]

    def _partition_only_delete_filter(
            self, predicate: Optional[Predicate]) -> Optional[Predicate]:
        context = self._context
        if predicate is None or not context.table.partition_keys:
            return None
        fields = predicate_fields(predicate)
        if not fields:
            return None
        partition_keys = set(context.table.partition_keys)
        if not fields.issubset(partition_keys):
            return None
        partition_index = {
            name: index for index, name in enumerate(context.table.partition_keys)
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

        plan = read_builder.new_scan().plan_all()
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
        return self._delete_rows_with_python(row_ids)

    def _delete_rows_with_python(self, row_ids):
        context = self._context
        return TableDeleteByRowId._from_python(context.table).delete(row_ids)


class _ShardBatchIterator:
    """Own a shard source even before its lazy batch generator starts."""

    def __init__(self, batches, source):
        self._batches = batches
        self._source = source

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._batches)

    def close_source(self, primary_error=None):
        source, self._source = self._source, None
        if source is not None:
            close_read_source(source, primary_error)

    def close(self):
        error = None
        try:
            self._batches.close()
        except BaseException as failure:
            error = failure
            raise
        finally:
            self.close_source(error)


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
        snapshot = TimeTravelUtil.resolve_snapshot(table)
        self.table: FileStoreTable = table.copy_without_time_travel({
            CoreOptions.SCALAR_INDEX_SEARCH_MODE.key(): GlobalIndexSearchMode.FULL.value,
        })._copy_with_snapshot(snapshot)
        self.projection = projection
        self.write_cols = list(write_cols) if write_cols is not None else list(table.field_names)
        self.commit_user = commit_user
        self.total_shard_count = total_shard_count
        self.shard_num = shard_num
        scanner = self.table.new_read_builder().new_scan()
        plan = scanner.plan_all()
        self.snapshot_id = plan.snapshot_id if plan.snapshot_id is not None else -1
        splits = plan.splits()
        splits = _filter_by_whole_file_shard(splits, shard_num, total_shard_count)
        self.splits = splits
        self._files_info = RowIdFileIndex.from_splits(self.snapshot_id, splits)
        self._write_schema = PyarrowFieldParser.from_paimon_schema(
            [table.field_dict[name] for name in self.write_cols])
        self.writer = None
        self._pending_row_ids = deque()
        self._group_batches = []
        self._current_group = None
        self._reader = None
        self._reader_started = False
        self._read_complete = not splits
        self._read_failed = False
        self._failed = False
        self._closed = False
        self._messages = None

    @staticmethod
    def compute_from_files(files: List[DataFileMeta]) -> List[Range]:
        ranges = [file.row_id_range() for file in files]
        return Range.sort_and_merge_overlap(ranges, True, False)

    def arrow_reader(self) -> pyarrow.ipc.RecordBatchReader:
        self._ensure_active()
        if self._reader_started:
            raise ValueError('Shard reader can only be opened once')
        read_builder = self.table.new_read_builder()
        read_builder.with_projection(self.projection)
        requested_read = read_builder.new_read()
        output_schema = requested_read._output_arrow_schema()
        read_type = list(read_builder.read_type())
        if not any(field.name == SpecialFields.ROW_ID.name for field in read_type):
            read_type.append(SpecialFields.ROW_ID)
        projection = requested_read.output_projection
        if projection is None:
            row_id_column = next(i for i, field in enumerate(read_type) if field.name == SpecialFields.ROW_ID.name)
        else:
            name = '__paimon_shard_row_id'
            while name in output_schema.names:
                name += '_'
            row_id_column = len(projection.columns)
            projection = OutputProjection(
                list(projection.columns) + [(name, [SpecialFields.ROW_ID.name])], projection.named)
        read = TableRead(self.table, predicate=None, read_type=read_type, output_projection=projection)
        source = read._to_managed_arrow_batch_reader(self.splits, parallelism=1)
        self._reader_started = True

        def batches():
            error = None
            try:
                for batch in source:
                    self._ensure_active()
                    row_ids = batch.column(row_id_column)
                    if row_ids.null_count:
                        raise ValueError('Shard reader returned null _ROW_ID values')
                    self._pending_row_ids.extend(row_ids.to_pylist())
                    arrays = [batch.column(i) for i in range(len(output_schema))]
                    yield (pa.RecordBatch.from_arrays(arrays, schema=output_schema) if arrays else
                           zero_column_batch(batch.num_rows, output_schema.metadata))
                self._read_complete = True
            except BaseException as failure:
                error = failure
                if not isinstance(failure, GeneratorExit):
                    self._read_failed = True
                raise
            finally:
                try:
                    iterator.close_source(error)
                except BaseException:
                    self._read_failed = True
                    raise

        iterator = _ShardBatchIterator(batches(), source)
        reader = pa.ipc.RecordBatchReader.from_batches(output_schema, iterator)
        self._reader = _ClosableArrowBatchReader(reader, iterator)
        return self._reader

    def prepare_commit(self) -> List[CommitMessage]:
        if self._messages is not None:
            return list(self._messages)
        self._ensure_active()
        if not self._read_complete or self._pending_row_ids:
            raise ValueError('Read and update exactly the same number of rows from this shard before preparing')
        self._flush_group()
        self._messages = [] if self.writer is None else list(self.writer.commit_messages)
        return list(self._messages)

    def update_by_arrow_batch(self, data: pa.RecordBatch):
        from pypaimon.execution.python.write.table_update_by_row_id import PythonTableUpdateByRowId

        self._ensure_active()
        if len(data.schema.names) != len(self.write_cols) or set(data.schema.names) != set(self.write_cols):
            raise ValueError('Shard input must contain exactly the configured update columns')
        arrays = [PythonTableUpdateByRowId._coerce_column(data.column(name), field.type)
                  for name, field in zip(self.write_cols, self._write_schema)]
        normalized = normalize_arrow_write_nullability(pa.RecordBatch.from_arrays(arrays, schema=self._write_schema),
                                                       self._write_schema)
        for field, column in zip(self._write_schema, normalized.columns):
            if not field.nullable and column.null_count:
                raise ValueError('Cannot write null to non-null column(%s)' % field.name)
        if data.num_rows > len(self._pending_row_ids):
            raise ValueError('Shard update contains more rows than have been read')
        row_ids = list(islice(self._pending_row_ids, data.num_rows))
        groups = [self._files_info.first_row_ids[bisect_right(self._files_info.first_row_ids, row_id) - 1]
                  for row_id in row_ids]
        batch = pa.RecordBatch.from_arrays(
            [pa.array(row_ids, type=pa.int64())] + list(normalized.columns),
            names=[SpecialFields.ROW_ID.name] + self.write_cols)
        try:
            start = 0
            while start < len(groups):
                group = groups[start]
                if self._current_group is not None and self._current_group != group:
                    self._flush_group()
                self._current_group = group
                end = start + 1
                while end < len(groups) and groups[end] == group:
                    end += 1
                self._group_batches.append(batch.slice(start, end - start))
                start = end
        except BaseException:
            self._failed = True
            raise
        for _ in row_ids:
            self._pending_row_ids.popleft()

    def _init_writer(self):
        if self.writer is None:
            from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
            self.writer = TableUpdateByRowId._from_python(
                self.table, self.commit_user, BATCH_COMMIT_IDENTIFIER,
                _precomputed_files_info=self._files_info)

    def _flush_group(self):
        if self._group_batches:
            self._init_writer()
            # Preserve staged messages if writing fails; they may already have
            # been published. A failed operation cannot later prepare success.
            self._failed = True
            self.writer.update_columns(pa.Table.from_batches(self._group_batches), self.write_cols)
            self._failed = False
            self._group_batches = []
        self._current_group = None

    def _ensure_active(self):
        if self._closed or self._failed or self._read_failed or self._messages is not None:
            raise ValueError('Shard updater is closed, failed or already prepared')

    def close(self):
        self._closed = True
        self._pending_row_ids.clear()
        self._group_batches = []
        if self._reader is not None:
            self._reader.close()
