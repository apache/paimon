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
import operator
import os
from typing import Any, Callable, Iterator, List, Optional

import pyarrow as pa
import torch
from torch.utils.data import Dataset, IterableDataset

from pypaimon.read.split import Split
from pypaimon.read.table_read import TableRead
from pypaimon.utils.range import Range


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


class SplitRangeIndex:

    def __init__(self, ranges_by_split):
        from pypaimon.execution.datasource.torch_dataset import initialize_split_range_index
        initialize_split_range_index(self, ranges_by_split)

    def find(self, ranges):
        from pypaimon.execution.datasource.torch_dataset import find_split_ranges
        return find_split_ranges(self, ranges)


def row_ranges_for_split(split) -> List[Range]:
    """Return the merged global row-ID ranges covered by ``split``."""
    from pypaimon.execution.datasource.torch_dataset import row_ranges_for_split
    return row_ranges_for_split(split)


def select_indexed_splits(
        splits, split_ranges, split_range_index, ranges) -> List[Split]:
    """Return ``IndexedSplit``s restricting ``splits`` to ``ranges``.

    Row ranges reach the format readers through ``IndexedSplit``, which lets a
    data-evolution read skip files and row blocks that no requested row ID
    touches. Authorization wrappers are preserved.
    """
    from pypaimon.execution.datasource.torch_dataset import select_indexed_splits
    return select_indexed_splits(splits, split_ranges, split_range_index, ranges)


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
        from pypaimon.execution.datasource.torch_dataset import initialize_map_dataset
        initialize_map_dataset(self, SplitRangeIndex)

    def _supports_lazy_row_id_read(self) -> bool:
        from pypaimon.execution.datasource.torch_dataset import supports_lazy_row_id_read
        return supports_lazy_row_id_read(self)

    @staticmethod
    def _row_id_is_masked(split) -> bool:
        from pypaimon.execution.datasource.torch_dataset import row_id_is_masked
        return row_id_is_masked(split)

    @staticmethod
    def _supports_indexed_split(split) -> bool:
        from pypaimon.execution.datasource.torch_dataset import supports_indexed_split
        return supports_indexed_split(split)

    def _compact_row_id_index(self):
        from pypaimon.execution.datasource.torch_dataset import compact_row_id_index
        return compact_row_id_index(self)

    def _materialize(self):
        from pypaimon.execution.datasource.torch_dataset import materialize_map_dataset
        return materialize_map_dataset(self)

    def __len__(self) -> int:
        """
        Return the total number of rows in the dataset.

        Returns:
            Total number of rows across all splits
        """
        from pypaimon.execution.datasource.torch_dataset import map_dataset_length
        return map_dataset_length(self)

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
        from pypaimon.execution.datasource.torch_dataset import map_dataset_getitems
        return map_dataset_getitems(self, indices)

    def _normalize_index(self, index) -> int:
        index = operator.index(index)
        if index < 0:
            index += len(self)
        if index < 0 or index >= len(self):
            raise IndexError("TorchDataset index out of range")
        return index

    def _select_splits(self, ranges) -> List[Split]:
        from pypaimon.execution.datasource.torch_dataset import select_map_dataset_splits
        return select_map_dataset_splits(self, ranges)


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
        self.field_names = table_read._output_arrow_schema().names
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
        from pypaimon.execution.datasource.torch_dataset import row_to_dict
        return row_to_dict(self, offset_row)

    def _limit_covers_all_splits(self) -> bool:
        from pypaimon.execution.datasource.torch_dataset import limit_covers_all_splits
        return limit_covers_all_splits(self)

    def _worker_splits(self, worker_info) -> List[Split]:
        from pypaimon.execution.datasource.torch_dataset import worker_splits
        return worker_splits(self, worker_info)


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
        from pypaimon.execution.datasource.torch_dataset import iterate_rows
        return iterate_rows(self)

    def _iter_rows(self, splits: List[Split]):
        from pypaimon.execution.datasource.torch_dataset import iterate_prefetched_rows
        return iterate_prefetched_rows(self, splits)


class _TorchShuffleMixin:

    def _init_shuffle(self, seed, buffer_size, max_buffer_input_splits):
        self.seed = self._require_int(seed, "seed")
        self.buffer_size = self._require_positive_int(buffer_size, "buffer_size")
        self.max_buffer_input_splits = self._require_positive_int(
            max_buffer_input_splits, "max_buffer_input_splits")
        from pypaimon.execution.datasource.torch_dataset import _share_epoch_with_torch_workers
        self._epoch = _share_epoch_with_torch_workers(0)

    def __setstate__(self, state):
        self.__dict__ = state
        from pypaimon.execution.datasource.torch_dataset import _share_epoch_with_torch_workers
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

    def set_epoch(self, epoch: int):
        self.epoch = epoch
        return self

    def _shuffle_rng(self, worker_id):
        from pypaimon.execution.datasource.torch_dataset import shuffle_rng
        return shuffle_rng(self, worker_id)


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
        from pypaimon.execution.datasource.torch_dataset import iterate_batches
        return iterate_batches(self)

    def _prepare_batches(self, raw_batches, worker_id):
        from pypaimon.execution.datasource.torch_dataset import prepare_batches
        return prepare_batches(self, raw_batches, worker_id)

    def _arrow_batches_for_splits(
        self, splits: List[Split]
    ) -> Iterator[pa.RecordBatch]:
        from pypaimon.execution.datasource.torch_dataset import arrow_batches_for_splits
        return arrow_batches_for_splits(self, splits)

    def _limit_batches(
        self, batches: Iterator[pa.RecordBatch]
    ) -> Iterator[pa.RecordBatch]:
        from pypaimon.execution.datasource.torch_dataset import limit_batches
        return limit_batches(self, batches)


class TorchShuffledBatchIterDataset(_TorchShuffleMixin, TorchBatchIterDataset):
    """Shuffle Arrow rows before sizing batches and converting to tensors."""

    def __init__(self, table_read, splits, seed=0, buffer_size=1000,
                 max_buffer_input_splits=10, **kwargs):
        super().__init__(table_read, splits, **kwargs)
        self._init_shuffle(seed, buffer_size, max_buffer_input_splits)

    def _prepare_batches(self, raw_batches, worker_id):
        from pypaimon.execution.datasource.torch_dataset import prepare_shuffled_batches
        return prepare_shuffled_batches(self, raw_batches, worker_id)

    def _arrow_batches_for_splits(self, splits):
        # Keep the selected prefix of a binding limit unchanged before shuffle.
        from pypaimon.execution.datasource.torch_dataset import interleaved_arrow_batches
        return interleaved_arrow_batches(
            self, splits, super()._arrow_batches_for_splits)


class TorchShuffledIterDataset(_TorchShuffleMixin, _BaseTorchIterDataset):
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
        self._init_shuffle(seed, buffer_size, max_buffer_input_splits)

    def __iter__(self):
        from pypaimon.execution.datasource.torch_dataset import iterate_shuffled_rows
        return iterate_shuffled_rows(self)

    def _iter_ordered_rows(self, splits: List[Split]) -> Iterator[dict]:
        from pypaimon.execution.datasource.torch_dataset import ordered_rows
        return ordered_rows(self, splits)

    def _iter_interleaved_rows(self, splits: List[Split]) -> Iterator[dict]:
        from pypaimon.execution.datasource.torch_dataset import interleaved_rows
        return interleaved_rows(self, splits)

    @staticmethod
    def _close_iterator(row_iter) -> None:
        from pypaimon.execution.datasource.torch_dataset import close_iterator
        return close_iterator(row_iter)

    def _iter_buffer_shuffled_rows(
        self,
        rows: Iterator[dict],
        worker_id: int,
    ) -> Iterator[dict]:
        from pypaimon.execution.datasource.torch_dataset import buffer_shuffled_rows
        return buffer_shuffled_rows(self, rows, worker_id)
