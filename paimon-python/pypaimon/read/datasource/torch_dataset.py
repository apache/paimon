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
            splits_to_process = self.splits
        else:
            per_worker = int(math.ceil(len(self.splits) / float(worker_info.num_workers)))
            start = worker_info.id * per_worker
            end = min(start + per_worker, len(self.splits))
            splits_to_process = self.splits[start:end]

        for row in self._iter_rows(splits_to_process):
            yield row

    def _iter_rows(self, splits: List[Split]):
        if self.prefetch_concurrency <= 1:
            for offset_row in self.table_read.to_iterator(splits):
                yield self._row_to_dict(offset_row)
            return

        n = min(self.prefetch_concurrency, len(splits))
        if n == 0:
            return
        split_groups = [splits[i::n] for i in range(n)]

        q = queue.Queue(maxsize=self._PREFETCH_QUEUE_MAXSIZE)
        stop = threading.Event()

        def producer(split_group: List):
            try:
                for offset_row in self.table_read.to_iterator(split_group):
                    if stop.is_set():
                        break
                    try:
                        q.put((self._ROW, self._row_to_dict(offset_row)), timeout=30.0)
                    except queue.Full:
                        if not stop.is_set():
                            q.put((self._ROW, self._row_to_dict(offset_row)))
                q.put((self._SENTINEL, None))
            except Exception as e:
                q.put((self._ERR, e))

        threads = [threading.Thread(target=producer, args=(split_groups[i],), daemon=True)
                   for i in range(n)]
        for t in threads:
            t.start()

        try:
            done = 0
            while done < n:
                try:
                    tag, payload = q.get(timeout=300.0)
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
                t.join(timeout=5.0)

