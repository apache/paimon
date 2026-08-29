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
import os
import queue
import random
import threading
import warnings
from typing import Any, Callable, Iterator, List, Optional

import pyarrow as pa
import torch
from torch.utils.data import Dataset, IterableDataset

from pypaimon.read.reader.concat_batch_reader import (
    _MAX_ARROW_OFFSET, _batch_offset_usage)
from pypaimon.read.split import Split
from pypaimon.read.table_read import TableRead


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


def _resolve_distributed_context(auto_detect_rank: bool):
    if not isinstance(auto_detect_rank, bool):
        raise ValueError("auto_detect_rank must be a bool")
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


class TorchDataset(Dataset):
    """
    PyTorch Dataset implementation for reading Paimon table data.

    This class enables Paimon table data to be used directly with PyTorch's
    training pipeline, allowing for efficient data loading and batching.
    """

    def __init__(self, table_read: TableRead, splits: List[Split]):
        """
        Initialize TorchDataset.

        Args:
            table_read: TableRead instance for reading data
            splits: List of splits to read
        """
        arrow_table = table_read.to_arrow(splits)
        if arrow_table is None or arrow_table.num_rows == 0:
            self._data = []
        else:
            self._data = arrow_table.to_pylist()

    def __len__(self) -> int:
        """
        Return the total number of rows in the dataset.

        Returns:
            Total number of rows across all splits
        """
        return len(self._data)

    def __getitem__(self, index: int):
        """
        Get a single item from the dataset.

        Args:
            index: Index of the item to retrieve

        Returns:
            Dictionary containing the row data
        """
        if not self._data:
            return None

        return self._data[index]


class _BaseTorchIterDataset(IterableDataset):
    """
    Shared helpers for streaming PyTorch datasets backed by Paimon splits.
    """

    def __init__(
        self,
        table_read: TableRead,
        splits: List[Split],
        auto_detect_rank: bool = False,
    ):
        self.table_read = table_read
        self.splits = splits
        self.field_names = [field.name for field in table_read.read_type]
        self.auto_detect_rank = auto_detect_rank
        self.rank, self.world_size = _resolve_distributed_context(auto_detect_rank)
        self._context_pid = os.getpid()

    def __getstate__(self):
        state = self.__dict__.copy()
        rank, world_size = _resolve_distributed_context(self.auto_detect_rank)
        state["rank"] = rank
        state["world_size"] = world_size
        state["_context_pid"] = os.getpid()
        return state

    def _distributed_context(self):
        rank, world_size = _resolve_distributed_context(
            self.auto_detect_rank
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
        super().__init__(table_read, splits, auto_detect_rank)
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
    ):
        super().__init__(table_read, splits, auto_detect_rank)
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
    ):
        super().__init__(table_read, splits, auto_detect_rank)
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
