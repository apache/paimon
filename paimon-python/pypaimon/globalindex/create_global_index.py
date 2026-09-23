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

import threading
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait
from functools import cmp_to_key
from typing import Dict, List, Optional, Sequence, Union

import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.common.predicate import Predicate
from pypaimon.globalindex.btree.btree_index_writer import (
    BTREE_IDENTIFIER,
    BTreeIndexWriter,
)
from pypaimon.globalindex.bitmap.bitmap_index_writer import (
    BITMAP_IDENTIFIER,
    BitmapIndexWriter,
)
from pypaimon.globalindex.build_plan import (
    filter_non_indexable_splits as _filter_non_indexable_splits,
    split_by_contiguous_unindexed_row_range as _split_by_contiguous_unindexed_row_range,
    split_by_global_index_shard as _split_by_global_index_shard,
    unindexed_row_ranges as _unindexed_row_ranges,
)
from pypaimon.globalindex.global_index_meta import GlobalIndexMeta
from pypaimon.globalindex.key_serializer import create_serializer
from pypaimon.globalindex.full_text.native_full_text_global_index_reader import (
    FULL_TEXT_IDENTIFIER,
)
from pypaimon.globalindex.full_text.native_full_text_index_writer import (
    NativeFullTextIndexWriter,
)
from pypaimon.globalindex.vindex.vindex_vector_global_index_reader import (
    VINDEX_IDENTIFIERS,
)
from pypaimon.globalindex.vindex.vindex_vector_index_writer import (
    ADD_BATCH_SIZE,
    VindexVectorIndexWriter,
)
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.special_fields import SpecialFields
from pypaimon.utils.range import Range
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


_SORTED_INDEX_IDENTIFIERS = (BTREE_IDENTIFIER, BITMAP_IDENTIFIER)
_GENERIC_INDEX_IDENTIFIERS = tuple(VINDEX_IDENTIFIERS) + (
    FULL_TEXT_IDENTIFIER,
)
_SORTED_INDEX_RECORDS_PER_FILE_FLOATING = 1.2


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
        self._user_options = dict(options or {})
        self._options = _merged_options(table, options)
        self._core_options = CoreOptions(self._options)

        if (
            self._index_type not in _SORTED_INDEX_IDENTIFIERS
            and self._index_type not in _GENERIC_INDEX_IDENTIFIERS
        ):
            raise ValueError(
                "Python global index build currently supports %s and %s, got '%s'."
                % (
                    _SORTED_INDEX_IDENTIFIERS,
                    _GENERIC_INDEX_IDENTIFIERS,
                    index_type,
                )
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
        if self._index_type in _GENERIC_INDEX_IDENTIFIERS:
            self._validate_generic_index_table()

    def build(self) -> List[CommitMessage]:
        read_builder = self._table.new_read_builder()
        partition_filter = self._resolve_partition_filter(read_builder)
        if partition_filter is not None:
            read_builder = read_builder.with_partition_filter(partition_filter)

        scan = read_builder.new_scan()
        plan = scan.plan_for_write()
        splits = plan.splits()
        if not splits:
            return []

        index_field = self._table.field_dict[self._index_columns[0]]
        snapshot = self._snapshot_for_plan(plan)
        unindexed_ranges = _unindexed_row_ranges(
            self._table,
            snapshot,
            partition_filter,
            index_field.id,
            self._index_type,
        )
        if not unindexed_ranges:
            return []

        if self._index_type in _GENERIC_INDEX_IDENTIFIERS:
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

        if self._index_type in _SORTED_INDEX_IDENTIFIERS:
            return self._build_sorted_index(
                splits, unindexed_ranges, index_field, table_read, index_path)
        return self._build_generic_index(
            splits, unindexed_ranges, index_field, table_read, index_path)

    def _snapshot_for_plan(self, plan):
        snapshot_id = getattr(plan, "snapshot_id", None)
        snapshot_manager = self._table.snapshot_manager()
        if snapshot_id is not None:
            return snapshot_manager.get_snapshot_by_id(snapshot_id)
        return snapshot_manager.get_latest_snapshot()

    def _build_sorted_index(
        self, splits, unindexed_ranges, index_field, table_read, index_path: str
    ) -> List[CommitMessage]:
        key_serializer = create_serializer(index_field.type)
        configured_records_per_range = (
            self._core_options.sorted_index_records_per_range())
        if configured_records_per_range <= 0:
            raise ValueError("sorted-index.records-per-file must be positive.")
        records_per_range = int(
            configured_records_per_range
            * _SORTED_INDEX_RECORDS_PER_FILE_FLOATING
        )

        messages = []
        for split, row_range in _split_by_contiguous_unindexed_row_range(
            splits, unindexed_ranges
        ):
            table = table_read.to_arrow([split])
            if table is None or table.num_rows == 0:
                continue
            rows = _extract_sorted_rows(
                table,
                self._index_columns[0],
                SpecialFields.ROW_ID.name,
                key_serializer,
                row_range,
            )
            if not rows:
                continue
            index_adds = []
            for chunk in _chunks(rows, records_per_range):
                writer = self._create_sorted_index_writer(
                    index_path, key_serializer)
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

    def _create_sorted_index_writer(self, index_path: str, key_serializer):
        if self._index_type == BTREE_IDENTIFIER:
            return BTreeIndexWriter(
                self._table.file_io,
                index_path,
                key_serializer,
                block_size=self._core_options.btree_index_block_size(),
                bloom_filter_enabled=(
                    self._core_options.btree_index_bloom_filter_enabled()),
            )
        if self._index_type == BITMAP_IDENTIFIER:
            return BitmapIndexWriter(
                self._table.file_io,
                index_path,
                key_serializer,
                dictionary_block_size=(
                    self._core_options.bitmap_index_dictionary_block_size()),
                compression=self._core_options.bitmap_index_compression(),
            )
        raise ValueError("Unsupported sorted global index type: %s" % self._index_type)

    def _build_generic_index(
        self, splits, unindexed_ranges, index_field, table_read, index_path: str
    ) -> List[CommitMessage]:
        rows_per_shard = self._core_options.global_index_row_count_per_shard()
        if rows_per_shard <= 0:
            raise ValueError(
                "Option 'global-index.row-count-per-shard' must be greater than 0."
            )

        parallelism = self._core_options.global_index_build_parallelism()
        if parallelism <= 0:
            raise ValueError(
                "Option 'global-index.build.parallelism' must be greater than 0."
            )

        shards = _split_by_global_index_shard(
            splits, rows_per_shard, unindexed_ranges)
        if not shards:
            return []

        if parallelism == 1 or len(shards) == 1:
            messages = []
            try:
                for index_split, index_range in shards:
                    message = self._build_generic_shard(
                        index_split, index_range, index_field, table_read, index_path)
                    if message is not None:
                        messages.append(message)
                return messages
            except BaseException:
                self._delete_uncommitted_indexes(messages)
                raise

        # Workers record their own output so that rollback never depends on the
        # future list being fully built. ThreadPoolExecutor.submit() enqueues the
        # work item before it starts an extra worker, so a submission that raises
        # (RuntimeError: can't start new thread) may still run its shard on an
        # already running worker.
        completed = []
        completed_lock = threading.Lock()

        def build_shard(index_split, index_range):
            message = self._build_generic_shard(
                index_split, index_range, index_field, table_read, index_path)
            if message is not None:
                with completed_lock:
                    completed.append(message)
            return message

        futures = []
        try:
            with ThreadPoolExecutor(
                max_workers=min(parallelism, len(shards)),
                thread_name_prefix="paimon-global-index-build",
            ) as executor:
                try:
                    for index_split, index_range in shards:
                        futures.append(
                            executor.submit(build_shard, index_split, index_range))
                except BaseException:
                    # Keep queued shards that have not started from building an
                    # index file this build is about to delete.
                    for future in futures:
                        future.cancel()
                    raise
                done, _ = wait(futures, return_when=FIRST_EXCEPTION)
                failed = next(
                    (future for future in futures
                     if future in done and future.exception() is not None),
                    None,
                )
                if failed is not None:
                    for future in futures:
                        future.cancel()
                    failed.result()

                # Futures are consumed in shard-plan order so index manifest
                # messages are deterministic even when shards finish out of order.
                results = [future.result() for future in futures]
            return [message for message in results if message is not None]
        except BaseException:
            # Exiting the executor waits for in-flight shards to close their
            # readers and writers, so ``completed`` is stable and fully visible
            # here. Delete every index that was built because build() will not
            # return commit messages after a failure.
            self._delete_uncommitted_indexes(completed)
            raise

    def _build_generic_shard(
        self, index_split, index_range, index_field, table_read, index_path: str
    ) -> Optional[CommitMessage]:
        from pypaimon.read.table_read import _ClosableArrowBatchReader

        writer = None
        try:
            reader, batches = table_read._new_arrow_batch_reader([index_split])
            # Close the Python iterator explicitly on failure as well as
            # the Arrow reader, which may retain a suspended generator.
            with _ClosableArrowBatchReader(reader, batches) as batch_reader:
                for batch in batch_reader:
                    if batch.num_rows == 0:
                        continue
                    if writer is None:
                        writer = self._create_generic_index_writer(
                            index_path, index_field)
                    if self._index_type in VINDEX_IDENTIFIERS:
                        if batch.column(SpecialFields.ROW_ID.name).null_count:
                            raise ValueError(
                                "Cannot build global index because _ROW_ID is null.")
                        for offset in range(0, batch.num_rows, ADD_BATCH_SIZE):
                            _write_vector_batch(
                                writer, batch.slice(offset, ADD_BATCH_SIZE),
                                self._index_columns[0], index_range)
                    else:
                        for value, row_id in _extract_index_rows(
                            batch,
                            self._index_columns[0],
                            SpecialFields.ROW_ID.name,
                            index_range,
                        ):
                            writer.write(value, row_id - index_range.from_)
                    del batch

            if writer is None:
                return None

            index_adds = _to_index_manifest_entries(
                self._table,
                index_split.partition,
                index_range,
                index_field.id,
                self._index_type,
                writer.finish(),
            )
            if not index_adds:
                return None
            return CommitMessage(
                partition=tuple(index_split.partition.values),
                bucket=0,
                new_files=[],
                index_adds=index_adds,
            )
        except BaseException:
            if writer is not None:
                self._delete_writer_output(writer, index_path)
            raise
        finally:
            if writer is not None:
                writer.close()

    def _delete_writer_output(self, writer, index_path: str) -> None:
        file_name = getattr(writer, "file_name", None)
        if file_name:
            self._table.file_io.delete_quietly(
                "%s/%s" % (index_path.rstrip("/"), file_name))

    def _delete_uncommitted_indexes(self, messages) -> None:
        path_factory = self._table.path_factory().global_index_path_factory()
        for message in messages:
            for index_add in message.index_adds:
                index_file = index_add.index_file
                file_path = (
                    index_file.external_path
                    if index_file.external_path is not None
                    else path_factory.to_path(index_file.file_name)
                )
                self._table.file_io.delete_quietly(file_path)

    def _create_generic_index_writer(self, index_path: str, index_field):
        if self._index_type in VINDEX_IDENTIFIERS:
            return VindexVectorIndexWriter(
                self._table.file_io,
                index_path,
                index_field.type,
                self._index_type,
                self._table.options.options.to_map(),
                index_field.name,
                self._user_options,
            )
        if self._index_type == FULL_TEXT_IDENTIFIER:
            return NativeFullTextIndexWriter(
                self._table.file_io,
                index_path,
                index_field.type,
                self._options.to_map(),
            )
        raise ValueError("Unsupported generic global index type: %s" % self._index_type)

    def _validate_generic_index_table(self) -> None:
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


def _extract_sorted_rows(
    table: pa.Table,
    index_column: str,
    row_id_column: str,
    key_serializer,
    row_range: Optional[Range] = None,
):
    keys = table.column(index_column).to_pylist()
    row_ids = table.column(row_id_column).to_pylist()
    rows = []
    for key, row_id in zip(keys, row_ids):
        if row_id is None:
            raise ValueError("Cannot build global index because _ROW_ID is null.")
        row_id = int(row_id)
        if row_range is not None and not row_range.contains(row_id):
            continue
        rows.append((key, row_id))

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


def _write_vector_batch(writer, batch, index_column, row_range):
    row_ids = batch.column(SpecialFields.ROW_ID.name)
    if row_ids.null_count:
        raise ValueError("Cannot build global index because _ROW_ID is null.")
    vectors = batch.column(index_column)
    selected = pc.and_(
        pc.greater_equal(row_ids, row_range.from_),
        pc.less_equal(row_ids, row_range.to),
    )
    if not pc.all(selected).as_py():
        row_ids = pc.filter(row_ids, selected)
        vectors = pc.filter(vectors, selected)
    writer.write_batch(vectors, pc.subtract(row_ids, row_range.from_))


def _extract_index_rows(
    table: Union[pa.Table, pa.RecordBatch],
    index_column: str,
    row_id_column: str,
    row_range: Optional[Range] = None,
):
    values = table.column(index_column).to_pylist()
    row_ids = table.column(row_id_column).to_pylist()
    rows = []
    for value, row_id in zip(values, row_ids):
        if row_id is None:
            raise ValueError("Cannot build global index because _ROW_ID is null.")
        row_id = int(row_id)
        if row_range is not None and not row_range.contains(row_id):
            continue
        rows.append((value, row_id))
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
