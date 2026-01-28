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
"""
Module to read a Paimon table into PyTorch Dataset.
"""
import math
import queue
import threading
from typing import List

import torch
from torch.utils.data import Dataset, IterableDataset

from pypaimon.read.split import Split
from pypaimon.read.table_read import TableRead


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


class TorchIterDataset(IterableDataset):
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
        self.table_read = table_read
        self.splits = splits
        self.prefetch_concurrency = max(1, int(prefetch_concurrency))
        # Get field names from read_type
        self.field_names = [field.name for field in table_read.read_type]

    def _row_to_dict(self, offset_row) -> dict:
        row_dict = {}
        for i, field_name in enumerate(self.field_names):
            value = offset_row.get_field(i)
            row_dict[field_name] = value
        return row_dict

    def __iter__(self):
        """
        Iterate over the dataset, converting each OffsetRow to a dictionary.

        Supports multi-worker data loading by partitioning splits across workers.
        When num_workers > 0 in DataLoader, each worker will process a subset of splits.

        Yields:
            row data of dict type, where keys are column names
        """
        worker_info = torch.utils.data.get_worker_info()
        if worker_info is None:
            # Single-process data loading, iterate over all splits
            splits_for_worker = self.splits
        else:
            # Multi-worker: partition splits across workers
            per_worker = int(math.ceil(len(self.splits) / float(worker_info.num_workers)))
            start = worker_info.id * per_worker
            end = min(start + per_worker, len(self.splits))
            splits_for_worker = self.splits[start:end]

        if self.prefetch_concurrency <= 1:
            iterator = self.table_read.to_iterator(splits_for_worker)
            for offset_row in iterator:
                yield self._row_to_dict(offset_row)
            return

        n = min(self.prefetch_concurrency, len(splits_for_worker))
        split_groups = [splits_for_worker[i::n] for i in range(n)]
        if n == 0:
            return

        q = queue.Queue(maxsize=self._PREFETCH_QUEUE_MAXSIZE)
        stop_event = threading.Event()

        def producer(thread_id: int, split_group: List):
            try:
                for offset_row in self.table_read.to_iterator(split_group):
                    if stop_event.is_set():
                        break
                    try:
                        q.put((self._ROW, self._row_to_dict(offset_row)), timeout=30.0)
                    except queue.Full:
                        if stop_event.is_set():
                            break
                        q.put((self._ROW, self._row_to_dict(offset_row)))
                q.put((self._SENTINEL, thread_id))
            except Exception as e:
                q.put((self._ERR, e))

        threads = [
            threading.Thread(target=producer, args=(i, split_groups[i]), daemon=True)
            for i in range(n)
        ]
        for t in threads:
            t.start()

        try:
            sentinel_count = 0
            while sentinel_count < n:
                try:
                    tag, payload = q.get(timeout=300.0)
                except queue.Empty:
                    if stop_event.is_set():
                        break
                    continue
                if tag == self._SENTINEL:
                    sentinel_count += 1
                elif tag == self._ERR:
                    raise payload
                elif tag == self._ROW:
                    yield payload
        finally:
            stop_event.set()
            for t in threads:
                t.join(timeout=5.0)

