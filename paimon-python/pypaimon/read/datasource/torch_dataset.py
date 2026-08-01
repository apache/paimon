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
import queue
import random
import threading
from typing import Iterator, List

import torch
from torch.utils.data import Dataset, IterableDataset

from pypaimon.read.split import Split
from pypaimon.read.table_read import TableRead


def _share_epoch_with_torch_workers(value):
    if isinstance(value, torch.Tensor):
        return value.share_memory_()
    return torch.tensor(value, dtype=torch.long).share_memory_()


_ITERATOR_EXHAUSTED = object()
_LIMIT_EXHAUSTED = object()


class _SharedLimit:
    """A row limit shared by DataLoader workers and prefetch threads."""

    def __init__(self, limit: int):
        context = torch.multiprocessing.get_context("spawn")
        self._limit = limit
        self._condition = context.Condition()
        self._remaining = context.RawValue("q", 0)
        self._reserved = context.RawValue("q", 0)
        self._arrived = context.RawValue("i", 0)
        self._active = context.RawValue("i", 0)
        self._participants = context.RawValue("i", 0)
        self._generation = context.RawValue("q", 0)

    def begin_iteration(self, participants: int) -> None:
        """Reset the limit after every worker has finished the previous iteration."""
        with self._condition:
            while self._active.value > 0:
                self._condition.wait()

            generation = self._generation.value
            if self._arrived.value == 0:
                self._participants.value = participants
                self._remaining.value = self._limit
                self._reserved.value = 0
            elif self._participants.value != participants:
                raise RuntimeError("DataLoader worker count changed during iteration startup")

            self._arrived.value += 1
            if self._arrived.value == self._participants.value:
                self._arrived.value = 0
                self._active.value = self._participants.value
                self._generation.value += 1
                self._condition.notify_all()
                return

            while self._generation.value == generation:
                self._condition.wait()

    def end_iteration(self) -> None:
        with self._condition:
            self._active.value -= 1
            if self._active.value == 0:
                self._condition.notify_all()

    def reserve(self) -> bool:
        """Reserve one row before reading it, waiting for unfinished reads if needed."""
        with self._condition:
            while self._remaining.value <= 0 and self._reserved.value > 0:
                self._condition.wait()
            if self._remaining.value <= 0:
                return False
            self._remaining.value -= 1
            self._reserved.value += 1
            return True

    def commit(self) -> None:
        with self._condition:
            self._reserved.value -= 1
            self._condition.notify_all()

    def release(self) -> None:
        with self._condition:
            self._reserved.value -= 1
            self._remaining.value += 1
            self._condition.notify_all()


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

    def __init__(self, table_read: TableRead, splits: List[Split]):
        self.table_read = table_read
        self.splits = splits
        self.field_names = [field.name for field in table_read.read_type]
        self._shared_limit = (
            _SharedLimit(table_read.limit) if table_read.limit is not None else None
        )

    def _row_to_dict(self, offset_row) -> dict:
        row_dict = {}
        for i, field_name in enumerate(self.field_names):
            value = offset_row.get_field(i)
            row_dict[field_name] = value
        return row_dict

    def _worker_splits(self, worker_info) -> List[Split]:
        if worker_info is None:
            return self.splits

        worker_id = worker_info.id
        num_workers = worker_info.num_workers
        total_splits = len(self.splits)
        splits_per_worker = total_splits // num_workers
        remainder = total_splits % num_workers

        if worker_id < remainder:
            start_idx = worker_id * (splits_per_worker + 1)
            end_idx = start_idx + splits_per_worker + 1
        else:
            start_idx = worker_id * splits_per_worker + remainder
            end_idx = start_idx + splits_per_worker

        return self.splits[start_idx:end_idx]

    def _begin_iteration(self, worker_info) -> None:
        if self._shared_limit is not None:
            participants = worker_info.num_workers if worker_info is not None else 1
            self._shared_limit.begin_iteration(participants)

    def _end_iteration(self) -> None:
        if self._shared_limit is not None:
            self._shared_limit.end_iteration()

    def _next_row(self, row_iter):
        if self._shared_limit is None:
            try:
                return next(row_iter)
            except StopIteration:
                return _ITERATOR_EXHAUSTED

        if not self._shared_limit.reserve():
            return _LIMIT_EXHAUSTED
        try:
            row = next(row_iter)
        except StopIteration:
            self._shared_limit.release()
            return _ITERATOR_EXHAUSTED
        except BaseException:
            self._shared_limit.release()
            raise
        self._shared_limit.commit()
        return row

    @staticmethod
    def _close_iterator(row_iter) -> None:
        close = getattr(row_iter, "close", None)
        if close is not None:
            close()


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

    def __init__(self, table_read: TableRead, splits: List[Split], prefetch_concurrency: int = 1):
        """
        Initialize TorchIterDataset.

        Args:
            table_read: TableRead instance for reading data
            splits: List of splits to read
            prefetch_concurrency: Number of threads to use for parallel OSS reads within
                this worker (default 1). When > 1, splits are partitioned across
                threads to increase read throughput.
        """
        super().__init__(table_read, splits)
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
        self._begin_iteration(worker_info)
        try:
            if self.prefetch_concurrency > 1:
                for row in self._iter_rows(splits_to_process):
                    yield row
                return

            worker_iterator = self.table_read.to_iterator(splits_to_process)
            try:
                while True:
                    offset_row = self._next_row(worker_iterator)
                    if offset_row is _ITERATOR_EXHAUSTED or offset_row is _LIMIT_EXHAUSTED:
                        return
                    yield self._row_to_dict(offset_row)
            finally:
                self._close_iterator(worker_iterator)
        finally:
            self._end_iteration()

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
            row_iter = self.table_read.to_iterator(split_group)
            try:
                while True:
                    if stop.is_set():
                        break
                    offset_row = self._next_row(row_iter)
                    if offset_row is _ITERATOR_EXHAUSTED or offset_row is _LIMIT_EXHAUSTED:
                        break
                    row_dict = self._row_to_dict(offset_row)
                    if not put_item(self._ROW, row_dict):
                        break
                put_item(self._SENTINEL, None)
            except Exception as e:
                put_item(self._ERR, e)
            finally:
                self._close_iterator(row_iter)

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
    ):
        super().__init__(table_read, splits)
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
        self._begin_iteration(worker_info)
        try:
            if self.max_buffer_input_splits == 1:
                rows = self._iter_ordered_rows(splits_to_process)
            else:
                rows = self._iter_interleaved_rows(splits_to_process)
            for row in self._iter_buffer_shuffled_rows(rows, worker_id):
                yield row
        finally:
            self._end_iteration()

    def _iter_ordered_rows(self, splits: List[Split]) -> Iterator[dict]:
        row_iter = self.table_read.to_iterator(splits)
        try:
            while True:
                offset_row = self._next_row(row_iter)
                if offset_row is _ITERATOR_EXHAUSTED or offset_row is _LIMIT_EXHAUSTED:
                    return
                yield self._row_to_dict(offset_row)
        finally:
            self._close_iterator(row_iter)

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
                offset_row = self._next_row(row_iter)
                if offset_row is _LIMIT_EXHAUSTED:
                    return
                if offset_row is _ITERATOR_EXHAUSTED:
                    self._close_iterator(row_iter)
                    del active[idx]
                    add_next_split()
                    continue

                yield self._row_to_dict(offset_row)
                idx += 1
        finally:
            for row_iter in active:
                self._close_iterator(row_iter)

    def _iter_buffer_shuffled_rows(
        self,
        rows: Iterator[dict],
        worker_id: int,
    ) -> Iterator[dict]:
        rng = random.Random(self.seed + self.epoch * 1000003 + worker_id)
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
