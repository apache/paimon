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

"""Backend-independent execution for public PyTorch dataset adapters.

Datasets read through their configured public TableRead. These helpers own
row selection, worker scheduling and Arrow/Torch conversion without selecting
or importing either Paimon execution backend.
"""

import bisect
import queue
import random
import sys
import threading
import warnings
from collections import deque
from typing import Any, Iterator, List, Optional

import pyarrow as pa
import pyarrow.compute as pc
import torch

from pypaimon.execution.read_support import (
    _MAX_ARROW_OFFSET, _batch_offset_usage, close_read_source)
from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.read.split import DataSplit, Split
from pypaimon.read.table_read import TableRead
from pypaimon.table.special_fields import SpecialFields
from pypaimon.utils.range import Range


def _share_epoch_with_torch_workers(value):
    if isinstance(value, torch.Tensor):
        return value.share_memory_()
    return torch.tensor(value, dtype=torch.long).share_memory_()


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


def initialize_split_range_index(self, ranges_by_split):
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


def find_split_ranges(self, ranges):
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


def row_ranges_for_split(split) -> List[Range]:
    """Return the merged global row-ID ranges covered by ``split``."""
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


def select_indexed_splits(
        splits, split_ranges, split_range_index, ranges) -> List[Split]:
    """Return ``IndexedSplit``s restricting ``splits`` to ``ranges``.

    Row ranges reach the format readers through ``IndexedSplit``, which lets a
    data-evolution read skip files and row blocks that no requested row ID
    touches. Authorization wrappers are preserved.
    """
    selected = []
    for split_index in split_range_index.find(ranges):
        original = splits[split_index]
        auth_result = None
        split = original
        if isinstance(split, QueryAuthSplit):
            auth_result = split.auth_result
            split = split.split

        if isinstance(split, IndexedSplit):
            split = split.data_split()
        allowed = Range.and_(ranges, split_ranges[split_index])
        if not allowed:
            continue

        indexed = IndexedSplit(split, allowed)
        selected.append(
            QueryAuthSplit(indexed, auth_result)
            if auth_result is not None else indexed
        )
    return selected


def initialize_map_dataset(self, split_range_index):
    table_read = self.table_read
    splits = self.splits
    if self._supports_lazy_row_id_read():
        self._split_ranges = [
            row_ranges_for_split(split) for split in splits
        ]
        self._split_range_index = split_range_index(self._split_ranges)
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


def supports_lazy_row_id_read(self) -> bool:
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


def row_id_is_masked(split) -> bool:
    if not isinstance(split, QueryAuthSplit):
        return False
    masking = getattr(split.auth_result, "column_masking", None)
    return bool(masking and SpecialFields.ROW_ID.name in masking)


def supports_indexed_split(split) -> bool:
    if isinstance(split, QueryAuthSplit):
        split = split.split
    if isinstance(split, IndexedSplit):
        return split.scores() is None
    return isinstance(split, DataSplit)


def compact_row_id_index(self):
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


def materialize_map_dataset(self):
    self._row_ids = None
    self._data = self.table_read.to_arrow(self.splits)
    self.table_read = None
    self.splits = None
    self._split_ranges = None
    self._split_range_index = None


def map_dataset_length(self) -> int:
    """
    Return the total number of rows in the dataset.

    Returns:
        Total number of rows across all splits
    """
    if self._row_ids is not None:
        return len(self._row_ids)
    return 0 if self._data is None else self._data.num_rows


def map_dataset_getitems(self, indices) -> List[dict]:
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


def select_map_dataset_splits(self, ranges) -> List[Split]:
    return select_indexed_splits(
        self.splits, self._split_ranges, self._split_range_index, ranges)


def row_to_dict(self, offset_row) -> dict:
    row_dict = {}
    for i, field_name in enumerate(self.field_names):
        value = offset_row.get_field(i)
        row_dict[field_name] = value
    return row_dict


def limit_covers_all_splits(self) -> bool:
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


def _balanced_slice(values: List[Any], shard_id: int, shard_count: int):
    base_size, remainder = divmod(len(values), shard_count)
    start = shard_id * base_size + min(shard_id, remainder)
    size = base_size + (1 if shard_id < remainder else 0)
    return values[start:start + size]


def worker_splits(self, worker_info) -> List[Split]:
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


def iterate_rows(self):
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
        yield from self._iter_rows(splits_to_process)
        return

    worker_iterator = self.table_read.to_iterator(splits_to_process)
    try:
        for offset_row in worker_iterator:
            yield self._row_to_dict(offset_row)
    finally:
        close_read_source(worker_iterator, sys.exc_info()[1])


def iterate_prefetched_rows(self, splits: List[Split]):
    n = min(self.prefetch_concurrency, len(splits))
    if n == 0:
        return
    split_groups = [splits[i::n] for i in range(n)]

    q = queue.Queue(maxsize=self._PREFETCH_QUEUE_MAXSIZE)
    stop = threading.Event()

    def put_item(tag: int, payload):
        while not stop.is_set():
            try:
                # A full queue must not hold a cancelled producer longer than
                # the join budget. Poll cancellation even when the configured
                # queue timeout is much longer than that budget.
                q.put((tag, payload), timeout=min(self._PREFETCH_PUT_TIMEOUT_SEC, 0.1))
                return True
            except queue.Full:
                continue
        return False

    def producer(split_group: List):
        try:
            source = self.table_read.to_iterator(split_group)
            try:
                for offset_row in source:
                    if stop.is_set():
                        break
                    row_dict = self._row_to_dict(offset_row)
                    if not put_item(self._ROW, row_dict):
                        break
            finally:
                close_read_source(source, sys.exc_info()[1])
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


def shuffle_rng(self, worker_id):
    rank, world_size = self._distributed_context()
    rng_seed = self.seed + self.epoch * 1000003 + worker_id
    if world_size > 1:
        rng_seed = "%d:%d" % (rng_seed, rank)
    return random.Random(rng_seed)


def iterate_batches(self):
    worker_info = torch.utils.data.get_worker_info()
    splits_to_process = self._worker_splits(worker_info)
    raw_batches = self._arrow_batches_for_splits(splits_to_process)
    worker_id = worker_info.id if worker_info is not None else 0
    try:
        for batch in self._prepare_batches(raw_batches, worker_id):
            if self.batch_format == "torch":
                converter = self.to_tensor_fn or _default_to_tensor
                yield converter(batch)
            else:
                yield batch
    finally:
        raw_batches.close()


def prepare_batches(self, raw_batches, worker_id):
    return _sized_record_batches(
        self._limit_batches(raw_batches), self.batch_size)


def arrow_batches_for_splits(
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


def limit_batches(
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


def prepare_shuffled_batches(self, raw_batches, worker_id):
    shuffled = _shuffle_record_batches(
        self._limit_batches(raw_batches), self.buffer_size,
        self._shuffle_rng(worker_id))
    return _sized_record_batches(shuffled, self.batch_size)


def interleaved_arrow_batches(self, splits, read_batches):
    # Keep the selected prefix of a binding limit unchanged before shuffle.
    if self.max_buffer_input_splits == 1 or not self._limit_covers_all_splits():
        yield from read_batches(splits)
        return

    split_iter = iter(splits)
    active = deque()

    def add_next():
        split = next(split_iter, None)
        if split is not None:
            active.append(read_batches([split]))

    try:
        for _ in range(min(self.max_buffer_input_splits, len(splits))):
            add_next()
        while active:
            try:
                yield next(active[0])
            except StopIteration:
                active.popleft().close()
                add_next()
            else:
                active.rotate(-1)
    finally:
        for batches in active:
            batches.close()


def _shuffle_record_batches(batches, buffer_size, rng):
    # Only row positions enter Python. Payloads, including nested values and
    # nulls, stay in Arrow. Each input block replaces random reservoir slots.
    buffer = None
    for batch in _sized_record_batches(batches, buffer_size):
        if buffer is None:
            buffer = batch
            continue
        usage = _batch_offset_usage(buffer)
        if any(usage.get(path, 0) + value > _MAX_ARROW_OFFSET
               for path, value in _batch_offset_usage(batch).items()):
            # Drain before concatenation would overflow a 32-bit Arrow offset.
            yield _shuffled_record_batch(buffer, rng)
            buffer = batch
            continue
        combined = _concat_record_batches([buffer, batch])
        retained = list(range(buffer.num_rows))
        selected = []
        for row in range(batch.num_rows):
            slot = rng.randrange(buffer.num_rows)
            selected.append(retained[slot])
            retained[slot] = buffer.num_rows + row
        output = combined.take(pa.array(selected, type=pa.int64()))
        buffer = combined.take(pa.array(retained, type=pa.int64()))
        yield output
    if buffer is not None:
        yield _shuffled_record_batch(buffer, rng)


def _shuffled_record_batch(batch, rng):
    order = list(range(batch.num_rows))
    rng.shuffle(order)
    return batch.take(pa.array(order, type=pa.int64()))


def iterate_shuffled_rows(self):
    worker_info = torch.utils.data.get_worker_info()
    worker_id = worker_info.id if worker_info is not None else 0
    splits_to_process = self._worker_splits(worker_info)

    if self.max_buffer_input_splits == 1:
        rows = self._iter_ordered_rows(splits_to_process)
    else:
        rows = self._iter_interleaved_rows(splits_to_process)
    shuffled = self._iter_buffer_shuffled_rows(rows, worker_id)
    try:
        for row in shuffled:
            yield row
    finally:
        close_read_source(shuffled, sys.exc_info()[1])


def ordered_rows(self, splits: List[Split]) -> Iterator[dict]:
    source = self.table_read.to_iterator(splits)
    try:
        for offset_row in source:
            yield self._row_to_dict(offset_row)
    finally:
        close_read_source(source, sys.exc_info()[1])


def interleaved_rows(self, splits: List[Split]) -> Iterator[dict]:
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


def close_iterator(row_iter) -> None:
    close = getattr(row_iter, "close", None)
    if close is not None:
        close()


def buffer_shuffled_rows(
    self,
    rows: Iterator[dict],
    worker_id: int,
) -> Iterator[dict]:
    rng = self._shuffle_rng(worker_id)
    buffer = []
    try:
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
    finally:
        close_read_source(rows, sys.exc_info()[1])
