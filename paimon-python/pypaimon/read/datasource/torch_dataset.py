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

"""
Module to read a Paimon table into PyTorch Dataset.
"""
import bisect
import operator
import os
import queue
import random
import threading
import warnings
from typing import Any, Callable, Iterator, List, Optional

import pyarrow as pa
import pyarrow.compute as pc
import torch
from torch.utils.data import Dataset, IterableDataset

from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.read.reader.concat_batch_reader import (
    _MAX_ARROW_OFFSET, _batch_offset_usage)
from pypaimon.read.split import DataSplit, Split
from pypaimon.read.table_read import TableRead
from pypaimon.table.special_fields import SpecialFields
from pypaimon.utils.range import Range


def _share_epoch_with_torch_workers(value):
    if isinstance(value, torch.Tensor):
        return value.share_memory_()
    return torch.tensor(value, dtype=torch.long).share_memory_()


def _validate_distributed_context(rank: int, world_size: int):
    if isinstance(rank, bool) or not isinstance(rank, int):
        raise ValueError("rank must be an int")
    if isinstance(world_size, bool) or not isinstance(world_size, int):
        raise ValueError("world_size must be an int")
    if world_size <= 0:
        raise ValueError("world_size must be greater than 0")
    if rank < 0 or rank >= world_size:
        raise ValueError("rank must satisfy 0 <= rank < world_size")
    return rank, world_size


def _resolve_distributed_context(
    auto_detect_rank: bool,
    sharding_rank: Optional[int] = None,
    sharding_world_size: Optional[int] = None,
):
    if not isinstance(auto_detect_rank, bool):
        raise ValueError("auto_detect_rank must be a bool")
    if sharding_rank is not None or sharding_world_size is not None:
        if auto_detect_rank:
            raise ValueError(
                "explicit sharding context cannot be combined with "
                "auto_detect_rank=True"
            )
        if sharding_rank is None or sharding_world_size is None:
            raise ValueError(
                "sharding_rank and sharding_world_size must be set together"
            )
        return _validate_distributed_context(
            sharding_rank, sharding_world_size
        )
    if not auto_detect_rank:
        return 0, 1

    distributed = getattr(torch, "distributed", None)
    if (
        distributed is not None
        and distributed.is_available()
        and distributed.is_initialized()
    ):
        rank = distributed.get_rank()
        world_size = distributed.get_world_size()
        return _validate_distributed_context(rank, world_size)

    env_rank = os.environ.get("RANK")
    env_world_size = os.environ.get("WORLD_SIZE")
    if env_rank is not None or env_world_size is not None:
        if env_rank is None or env_world_size is None:
            raise ValueError(
                "RANK and WORLD_SIZE environment variables must be set together"
            )
        try:
            rank, world_size = int(env_rank), int(env_world_size)
        except ValueError:
            raise ValueError(
                "RANK and WORLD_SIZE environment variables must be integers"
            )
        return _validate_distributed_context(rank, world_size)

    return 0, 1


def _balanced_slice(values: List[Any], shard_id: int, shard_count: int):
    base_size, remainder = divmod(len(values), shard_count)
    start = shard_id * base_size + min(shard_id, remainder)
    size = base_size + (1 if shard_id < remainder else 0)
    return values[start:start + size]


class _RowIdRangeIndex:

    def __init__(self, ranges, limit=None):
        self.ranges = []
        self.ends = []
        remaining = limit
        size = 0
        for row_range in ranges:
            count = row_range.count()
            if remaining is not None:
                if remaining <= 0:
                    break
                count = min(count, remaining)
                remaining -= count
            if count == 0:
                continue
            self.ranges.append(Range(
                row_range.from_, row_range.from_ + count - 1))
            size += count
            self.ends.append(size)

    def __len__(self):
        return self.ends[-1] if self.ends else 0

    def take(self, indices):
        row_ids = []
        for index in indices:
            range_index = bisect.bisect_right(self.ends, index)
            previous_end = self.ends[range_index - 1] if range_index else 0
            row_ids.append(
                self.ranges[range_index].from_ + index - previous_end)
        return row_ids


class _SplitRangeIndex:

    def __init__(self, ranges_by_split):
        self.intervals = sorted(
            (row_range.from_, row_range.to, split_index)
            for split_index, ranges in enumerate(ranges_by_split)
            for row_range in ranges
        )
        self.starts = [interval[0] for interval in self.intervals]
        self.max_ends = []
        max_end = -1
        for _, end, _ in self.intervals:
            max_end = max(max_end, end)
            self.max_ends.append(max_end)

    def find(self, ranges):
        split_indices = set()
        for row_range in ranges:
            right = bisect.bisect_right(self.starts, row_range.to)
            left = bisect.bisect_left(
                self.max_ends, row_range.from_, 0, right)
            for position in range(left, right):
                _, end, split_index = self.intervals[position]
                if end >= row_range.from_:
                    split_indices.add(split_index)
        return sorted(split_indices)


class TorchDataset(Dataset):
    """
    Map-style PyTorch Dataset for Paimon table data.

    Eligible data-evolution reads are fetched lazily by DataLoader batch.
    Other reads retain their Arrow representation instead of expanding all
    rows into Python objects.
    """

    def __init__(self, table_read: TableRead, splits: List[Split]):
        """
        Initialize TorchDataset.

        Args:
            table_read: TableRead instance for reading data
            splits: List of splits to read
        """
        self.table_read = table_read
        self.splits = splits
        self._data = None
        self._row_ids = None
        self._split_ranges = None
        self._split_range_index = None
        if self._supports_lazy_row_id_read():
            self._split_ranges = [
                self._row_ranges_for_split(split) for split in splits
            ]
            self._split_range_index = _SplitRangeIndex(self._split_ranges)
            self._row_ids = self._compact_row_id_index()
            if self._row_ids is None:
                row_id_read = TableRead(
                    table_read.table,
                    table_read.predicate,
                    [SpecialFields.ROW_ID],
                    limit=table_read.limit,
                )
                row_id_table = row_id_read.to_arrow(splits)
                self._row_ids = row_id_table.column(
                    SpecialFields.ROW_ID.name).combine_chunks()
                if pc.count_distinct(self._row_ids).as_py() != len(
                        self._row_ids):
                    self._materialize()
        else:
            self._materialize()

    def _supports_lazy_row_id_read(self) -> bool:
        if not self.table_read.table.options.row_tracking_enabled():
            return False
        if not self.table_read.table.options.data_evolution_enabled():
            return False
        if self.table_read.include_row_kind:
            return False
        if self.table_read.nested_name_paths:
            return False
        if any(self._row_id_is_masked(split) for split in self.splits):
            return False
        return all(self._supports_indexed_split(split) for split in self.splits)

    @staticmethod
    def _row_id_is_masked(split) -> bool:
        if not isinstance(split, QueryAuthSplit):
            return False
        masking = getattr(split.auth_result, "column_masking", None)
        return bool(masking and SpecialFields.ROW_ID.name in masking)

    @staticmethod
    def _supports_indexed_split(split) -> bool:
        if isinstance(split, QueryAuthSplit):
            split = split.split
        if isinstance(split, IndexedSplit):
            return split.scores() is None
        return isinstance(split, DataSplit)

    def _compact_row_id_index(self):
        if self.table_read.predicate is not None:
            return None
        ranges = []
        for original, split_ranges in zip(
                self.splits, self._split_ranges):
            split = original
            if isinstance(split, QueryAuthSplit):
                if getattr(split.auth_result, "filter", None):
                    return None
                split = split.split
            deletion_files = split.data_deletion_files or []
            if any(deletion is not None for deletion in deletion_files):
                return None
            ranges.extend(split_ranges)
        merged = Range.sort_and_merge_overlap(ranges, True)
        if sum(r.count() for r in ranges) != sum(
                r.count() for r in merged):
            return None
        return _RowIdRangeIndex(ranges, self.table_read.limit)

    @staticmethod
    def _row_ranges_for_split(split):
        if isinstance(split, QueryAuthSplit):
            split = split.split
        if isinstance(split, IndexedSplit):
            ranges = split.row_ranges()
        else:
            ranges = [
                data_file.row_id_range()
                for data_file in split.files
                if data_file.first_row_id is not None
            ]
        return Range.sort_and_merge_overlap(ranges, True)

    def _materialize(self):
        self._row_ids = None
        self._data = self.table_read.to_arrow(self.splits)
        self.table_read = None
        self.splits = None
        self._split_ranges = None
        self._split_range_index = None

    def __len__(self) -> int:
        """
        Return the total number of rows in the dataset.

        Returns:
            Total number of rows across all splits
        """
        if self._row_ids is not None:
            return len(self._row_ids)
        return 0 if self._data is None else self._data.num_rows

    def __getitem__(self, index: int):
        """
        Get a single item from the dataset.

        Args:
            index: Index of the item to retrieve

        Returns:
            Dictionary containing the row data
        """
        if len(self) == 0:
            return None
        if isinstance(index, slice):
            return self.__getitems__(range(*index.indices(len(self))))
        return self.__getitems__([index])[0]

    def __getitems__(self, indices) -> List[dict]:
        normalized = [self._normalize_index(index) for index in indices]
        if not normalized:
            return []
        if self._row_ids is None:
            return self._data.take(pa.array(
                normalized, type=pa.int64())).to_pylist()

        if isinstance(self._row_ids, _RowIdRangeIndex):
            row_ids = self._row_ids.take(normalized)
        else:
            row_ids = self._row_ids.take(pa.array(
                normalized, type=pa.int64())).to_pylist()
        ranges = Range.sort_and_merge_overlap(
            [Range(row_id, row_id) for row_id in set(row_ids)], True)
        splits = self._select_splits(ranges)

        output_has_row_id = any(
            field.name == SpecialFields.ROW_ID.name
            for field in self.table_read.read_type
        )
        read_type = list(self.table_read.read_type)
        if not output_has_row_id:
            read_type.append(SpecialFields.ROW_ID)
        batch_read = TableRead(
            self.table_read.table,
            self.table_read.predicate,
            read_type,
            include_row_kind=self.table_read.include_row_kind,
        )
        rows = batch_read.to_arrow(splits).to_pylist()
        by_row_id = {}
        for row in rows:
            row_id = row[SpecialFields.ROW_ID.name]
            if not output_has_row_id:
                del row[SpecialFields.ROW_ID.name]
            by_row_id[row_id] = row
        missing = set(row_ids) - set(by_row_id)
        if missing:
            raise RuntimeError(
                "Paimon rows disappeared while reading TorchDataset: %s"
                % sorted(missing)
            )
        return [by_row_id[row_id] for row_id in row_ids]

    def _normalize_index(self, index) -> int:
        index = operator.index(index)
        if index < 0:
            index += len(self)
        if index < 0 or index >= len(self):
            raise IndexError("TorchDataset index out of range")
        return index

    def _select_splits(self, ranges) -> List[Split]:
        selected = []
        split_indices = self._split_range_index.find(ranges)
        for split_index in split_indices:
            original = self.splits[split_index]
            auth_result = None
            split = original
            if isinstance(split, QueryAuthSplit):
                auth_result = split.auth_result
                split = split.split

            if isinstance(split, IndexedSplit):
                split = split.data_split()
            allowed = Range.and_(
                ranges, self._split_ranges[split_index])
            if not allowed:
                continue

            indexed = IndexedSplit(
                split,
                allowed,
                exact_merged_row_count=sum(r.count() for r in allowed),
            )
            selected.append(
                QueryAuthSplit(indexed, auth_result)
                if auth_result is not None else indexed
            )
        return selected


class _BaseTorchIterDataset(IterableDataset):
    """
    Shared helpers for streaming PyTorch datasets backed by Paimon splits.
    """

    def __init__(
        self,
        table_read: TableRead,
        splits: List[Split],
        auto_detect_rank: bool = False,
        sharding_rank: Optional[int] = None,
        sharding_world_size: Optional[int] = None,
    ):
        self.table_read = table_read
        self.splits = splits
        self.field_names = [field.name for field in table_read.read_type]
        self.auto_detect_rank = auto_detect_rank
        self.sharding_rank = sharding_rank
        self.sharding_world_size = sharding_world_size
        self.rank, self.world_size = _resolve_distributed_context(
            auto_detect_rank,
            sharding_rank,
            sharding_world_size,
        )
        self._context_pid = os.getpid()

    def __getstate__(self):
        state = self.__dict__.copy()
        rank, world_size = _resolve_distributed_context(
            self.auto_detect_rank,
            self.sharding_rank,
            self.sharding_world_size,
        )
        state["rank"] = rank
        state["world_size"] = world_size
        state["_context_pid"] = os.getpid()
        return state

    def _distributed_context(self):
        rank, world_size = _resolve_distributed_context(
            self.auto_detect_rank,
            self.sharding_rank,
            self.sharding_world_size,
        )
        current_pid = os.getpid()
        if (
            current_pid != self._context_pid
            and world_size == 1
            and self.world_size > 1
        ):
            return self.rank, self.world_size
        self.rank, self.world_size = rank, world_size
        self._context_pid = current_pid
        return rank, world_size

    def _row_to_dict(self, offset_row) -> dict:
        row_dict = {}
        for i, field_name in enumerate(self.field_names):
            value = offset_row.get_field(i)
            row_dict[field_name] = value
        return row_dict

    def _limit_covers_all_splits(self) -> bool:
        limit = self.table_read.limit
        if limit is None:
            return True
        total_rows = 0
        for split in self.splits:
            physical_row_count = getattr(split, "row_count", None)
            if (
                isinstance(physical_row_count, bool)
                or not isinstance(physical_row_count, int)
                or physical_row_count < 0
            ):
                return False
            row_count = physical_row_count
            merged_row_count = getattr(split, "merged_row_count", None)
            if callable(merged_row_count):
                try:
                    merged_row_count = merged_row_count()
                except Exception:
                    merged_row_count = None
                if (
                    not isinstance(merged_row_count, bool)
                    and isinstance(merged_row_count, int)
                    and 0 <= merged_row_count <= physical_row_count
                ):
                    row_count = merged_row_count
            total_rows += row_count
            if total_rows > limit:
                return False
        return True

    def _worker_splits(self, worker_info) -> List[Split]:
        rank, world_size = self._distributed_context()
        worker_id = worker_info.id if worker_info is not None else 0
        num_workers = worker_info.num_workers if worker_info is not None else 1

        if self.table_read.limit == 0:
            return []
        if (
            self.table_read.limit is not None
            and not self._limit_covers_all_splits()
        ):
            if world_size > 1:
                raise ValueError(
                    "limit is not supported with distributed Torch sharding"
                )
            # A binding limit cannot be shared safely.
            return self.splits if worker_id == 0 else []

        rank_splits = _balanced_slice(self.splits, rank, world_size)
        return _balanced_slice(rank_splits, worker_id, num_workers)


class TorchIterDataset(_BaseTorchIterDataset):
    """
    PyTorch IterableDataset implementation for reading Paimon table data.

    This class enables streaming data loading from Paimon tables, which is more
    memory-efficient for large datasets. Data is read on-the-fly as needed,
    rather than loading everything into memory upfront.
    """

    _SENTINEL = 0
    _ROW = 1
    _ERR = 2
    _PREFETCH_QUEUE_MAXSIZE = 512
    _PREFETCH_PUT_TIMEOUT_SEC = 30.0
    _PREFETCH_GET_TIMEOUT_SEC = 300.0
    _PREFETCH_JOIN_TIMEOUT_SEC = 5.0

    def __init__(
        self,
        table_read: TableRead,
        splits: List[Split],
        prefetch_concurrency: int = 1,
        auto_detect_rank: bool = False,
        sharding_rank: Optional[int] = None,
        sharding_world_size: Optional[int] = None,
    ):
        """
        Initialize TorchIterDataset.

        Args:
            table_read: TableRead instance for reading data
            splits: List of splits to read
            prefetch_concurrency: Number of threads to use for parallel OSS reads within
                this worker (default 1). When > 1, splits are partitioned across
                threads to increase read throughput.
        """
        super().__init__(
            table_read,
            splits,
            auto_detect_rank,
            sharding_rank,
            sharding_world_size,
        )
        self.prefetch_concurrency = max(1, int(prefetch_concurrency))

    def __iter__(self):
        """
        Iterate over the dataset, converting each OffsetRow to a dictionary.

        Supports multi-worker data loading by partitioning splits across workers.
        When num_workers > 0 in DataLoader, each worker will process a subset of splits.

        Yields:
            row data of dict type, where keys are column names
        """
        worker_info = torch.utils.data.get_worker_info()
        splits_to_process = self._worker_splits(worker_info)

        if self.prefetch_concurrency > 1:
            for row in self._iter_rows(splits_to_process):
                yield row
            return

        worker_iterator = self.table_read.to_iterator(splits_to_process)

        for offset_row in worker_iterator:
            yield self._row_to_dict(offset_row)

    def _iter_rows(self, splits: List[Split]):
        n = min(self.prefetch_concurrency, len(splits))
        if n == 0:
            return
        split_groups = [splits[i::n] for i in range(n)]

        q = queue.Queue(maxsize=self._PREFETCH_QUEUE_MAXSIZE)
        stop = threading.Event()

        def put_item(tag: int, payload):
            while not stop.is_set():
                try:
                    q.put((tag, payload), timeout=self._PREFETCH_PUT_TIMEOUT_SEC)
                    return True
                except queue.Full:
                    continue
            return False

        def producer(split_group: List):
            try:
                for offset_row in self.table_read.to_iterator(split_group):
                    if stop.is_set():
                        break
                    row_dict = self._row_to_dict(offset_row)
                    if not put_item(self._ROW, row_dict):
                        break
                put_item(self._SENTINEL, None)
            except Exception as e:
                put_item(self._ERR, e)

        threads = [threading.Thread(target=producer, args=(split_groups[i],), daemon=True)
                   for i in range(n)]
        for t in threads:
            t.start()

        try:
            done = 0
            while done < n:
                try:
                    tag, payload = q.get(timeout=self._PREFETCH_GET_TIMEOUT_SEC)
                except queue.Empty:
                    if stop.is_set():
                        break
                    continue
                if tag == self._SENTINEL:
                    done += 1
                elif tag == self._ERR:
                    raise payload
                else:
                    yield payload
        finally:
            stop.set()
            for t in threads:
                t.join(timeout=self._PREFETCH_JOIN_TIMEOUT_SEC)


def _concat_record_batches(batches: List[pa.RecordBatch]) -> pa.RecordBatch:
    if len(batches) == 1:
        return batches[0]
    return pa.RecordBatch.from_arrays(
        [
            pa.concat_arrays([batch.column(i) for batch in batches])
            for i in range(batches[0].num_columns)
        ],
        schema=batches[0].schema,
    )


def _sized_record_batches(
    batches: Iterator[pa.RecordBatch],
    batch_size: Optional[int],
) -> Iterator[pa.RecordBatch]:
    if batch_size is None:
        yield from batches
        return

    pending: List[pa.RecordBatch] = []
    pending_rows = 0
    offset_usage = {}
    for batch in batches:
        offset = 0
        while offset < batch.num_rows:
            take = min(batch_size - pending_rows, batch.num_rows - offset)
            piece = batch.slice(offset, take)
            piece_usage = _batch_offset_usage(piece)
            if pending and any(
                offset_usage.get(path, 0) + value > _MAX_ARROW_OFFSET
                for path, value in piece_usage.items()
            ):
                yield _concat_record_batches(pending)
                pending = []
                pending_rows = 0
                offset_usage = {}
                continue

            pending.append(piece)
            pending_rows += take
            offset += take
            for path, value in piece_usage.items():
                offset_usage[path] = offset_usage.get(path, 0) + value
            if pending_rows == batch_size or any(
                value >= _MAX_ARROW_OFFSET for value in offset_usage.values()
            ):
                yield _concat_record_batches(pending)
                pending = []
                pending_rows = 0
                offset_usage = {}

    if pending:
        yield _concat_record_batches(pending)


def _default_to_tensor(batch: pa.RecordBatch) -> dict:
    tensors = {}
    for name, array in zip(batch.schema.names, batch.columns):
        if array.null_count:
            raise ValueError(
                "Torch tensor conversion does not support null values in "
                "column %r; provide to_tensor_fn to handle them." % name
            )

        if pa.types.is_fixed_size_list(array.type):
            value_type = array.type.value_type
            if not (
                pa.types.is_integer(value_type)
                or pa.types.is_floating(value_type)
                or pa.types.is_boolean(value_type)
            ):
                raise ValueError(
                    "Torch tensor conversion does not support column %r with "
                    "type %s; provide to_tensor_fn." % (name, array.type)
                )
            values = array.values.slice(
                array.offset * array.type.list_size,
                len(array) * array.type.list_size,
            )
            if values.null_count:
                raise ValueError(
                    "Torch tensor conversion does not support null list values "
                    "in column %r; provide to_tensor_fn to handle them." % name
                )
            numpy_array = values.to_numpy(zero_copy_only=False).reshape(
                len(array), array.type.list_size
            )
        elif (
            pa.types.is_integer(array.type)
            or pa.types.is_floating(array.type)
            or pa.types.is_boolean(array.type)
        ):
            numpy_array = array.to_numpy(zero_copy_only=False)
        else:
            raise ValueError(
                "Torch tensor conversion only supports numeric, boolean, and "
                "fixed-size-list columns; column %r has type %s. Select "
                "batch_format='pyarrow' or provide to_tensor_fn."
                % (name, array.type)
            )
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="The given NumPy array is not writable",
                category=UserWarning,
            )
            tensors[name] = torch.from_numpy(numpy_array)
    return tensors


class TorchBatchIterDataset(_BaseTorchIterDataset):
    """Streaming IterableDataset which yields Arrow or Tensor batches."""

    def __init__(
        self,
        table_read: TableRead,
        splits: List[Split],
        batch_format: str,
        batch_size: Optional[int],
        to_tensor_fn: Optional[Callable[[pa.RecordBatch], Any]] = None,
        auto_detect_rank: bool = False,
        sharding_rank: Optional[int] = None,
        sharding_world_size: Optional[int] = None,
    ):
        super().__init__(
            table_read,
            splits,
            auto_detect_rank,
            sharding_rank,
            sharding_world_size,
        )
        self.batch_format = batch_format
        self.batch_size = batch_size
        self.to_tensor_fn = to_tensor_fn

    def __iter__(self):
        worker_info = torch.utils.data.get_worker_info()
        splits_to_process = self._worker_splits(worker_info)
        raw_batches = self._arrow_batches_for_splits(splits_to_process)

        batches = _sized_record_batches(
            self._limit_batches(raw_batches), self.batch_size
        )
        for batch in batches:
            if self.batch_format == "torch":
                converter = self.to_tensor_fn or _default_to_tensor
                yield converter(batch)
            else:
                yield batch

    def _arrow_batches_for_splits(
        self, splits: List[Split]
    ) -> Iterator[pa.RecordBatch]:
        reader = self.table_read.to_arrow_batch_reader(splits)
        try:
            for batch in iter(reader.read_next_batch, None):
                if batch.num_rows:
                    yield batch
        finally:
            close = getattr(reader, "close", None)
            if close is not None:
                close()

    def _limit_batches(
        self, batches: Iterator[pa.RecordBatch]
    ) -> Iterator[pa.RecordBatch]:
        remaining = self.table_read.limit
        for batch in batches:
            if remaining is not None:
                if remaining <= 0:
                    return
                if batch.num_rows > remaining:
                    batch = batch.slice(0, remaining)
                remaining -= batch.num_rows
            yield batch


class TorchShuffledIterDataset(_BaseTorchIterDataset):
    """
    PyTorch IterableDataset with Paimon-controlled streaming shuffle.

    This dataset consumes pre-planned splits, then mixes rows with split
    interleaving and a shuffle buffer. Chunk-level shuffle, when needed,
    stays in TableScan.with_chunk_shuffle().
    """

    def __init__(
        self,
        table_read: TableRead,
        splits: List[Split],
        seed: int = 0,
        buffer_size: int = 1000,
        max_buffer_input_splits: int = 10,
        auto_detect_rank: bool = False,
        sharding_rank: Optional[int] = None,
        sharding_world_size: Optional[int] = None,
    ):
        super().__init__(
            table_read,
            splits,
            auto_detect_rank,
            sharding_rank,
            sharding_world_size,
        )
        self.seed = self._require_int(seed, "seed")
        self.buffer_size = self._require_positive_int(buffer_size, "buffer_size")
        self.max_buffer_input_splits = self._require_positive_int(
            max_buffer_input_splits, "max_buffer_input_splits")
        self._epoch = _share_epoch_with_torch_workers(0)

    def __setstate__(self, state):
        self.__dict__ = state
        self._epoch = _share_epoch_with_torch_workers(self._epoch)

    @property
    def epoch(self) -> int:
        return int(self._epoch)

    @epoch.setter
    def epoch(self, epoch: int) -> None:
        epoch = self._require_int(epoch, "epoch")
        self._epoch += epoch - self._epoch

    @staticmethod
    def _require_int(value: int, name: str) -> int:
        if not isinstance(value, int):
            raise ValueError("%s must be an int" % name)
        return value

    @staticmethod
    def _require_positive_int(value: int, name: str) -> int:
        if not isinstance(value, int) or value <= 0:
            raise ValueError("%s must be a positive int" % name)
        return value

    def set_epoch(self, epoch: int) -> "TorchShuffledIterDataset":
        self.epoch = epoch
        return self

    def __iter__(self):
        worker_info = torch.utils.data.get_worker_info()
        worker_id = worker_info.id if worker_info is not None else 0
        splits_to_process = self._worker_splits(worker_info)

        if self.max_buffer_input_splits == 1:
            rows = self._iter_ordered_rows(splits_to_process)
        else:
            rows = self._iter_interleaved_rows(splits_to_process)
        for row in self._iter_buffer_shuffled_rows(rows, worker_id):
            yield row

    def _iter_ordered_rows(self, splits: List[Split]) -> Iterator[dict]:
        for offset_row in self.table_read.to_iterator(splits):
            yield self._row_to_dict(offset_row)

    def _iter_interleaved_rows(self, splits: List[Split]) -> Iterator[dict]:
        if not splits:
            return

        split_iter = iter(splits)
        active: List[Iterator] = []

        def add_next_split() -> bool:
            try:
                split = next(split_iter)
            except StopIteration:
                return False
            active.append(self.table_read.to_iterator([split]))
            return True

        for _ in range(min(self.max_buffer_input_splits, len(splits))):
            add_next_split()

        idx = 0
        try:
            while active:
                if idx >= len(active):
                    idx = 0
                row_iter = active[idx]
                try:
                    offset_row = next(row_iter)
                except StopIteration:
                    self._close_iterator(row_iter)
                    del active[idx]
                    add_next_split()
                    continue

                yield self._row_to_dict(offset_row)
                idx += 1
        finally:
            for row_iter in active:
                self._close_iterator(row_iter)

    @staticmethod
    def _close_iterator(row_iter) -> None:
        close = getattr(row_iter, "close", None)
        if close is not None:
            close()

    def _iter_buffer_shuffled_rows(
        self,
        rows: Iterator[dict],
        worker_id: int,
    ) -> Iterator[dict]:
        rank, world_size = self._distributed_context()
        rng_seed = self.seed + self.epoch * 1000003 + worker_id
        if world_size > 1:
            rng_seed = "%d:%d" % (rng_seed, rank)
        rng = random.Random(rng_seed)
        buffer = []
        for row in rows:
            if len(buffer) < self.buffer_size:
                buffer.append(row)
                continue
            idx = rng.randint(0, self.buffer_size - 1)
            yield buffer[idx]
            buffer[idx] = row

        rng.shuffle(buffer)
        for row in buffer:
            yield row
