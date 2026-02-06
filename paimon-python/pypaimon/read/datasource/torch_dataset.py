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

    def __init__(self, table_read: TableRead, splits: List[Split]):
        """
        Initialize TorchIterDataset.

        Args:
            table_read: TableRead instance for reading data
            splits: List of splits to read
        """
        self.table_read = table_read
        self.splits = splits
        # Get field names from read_type
        self.field_names = [field.name for field in table_read.read_type]

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
            splits_to_process = self.splits
        else:
            # Multi-process data loading, partition splits across workers
            worker_id = worker_info.id
            num_workers = worker_info.num_workers

            # Calculate start and end indices for this worker
            # Distribute splits evenly by slicing
            total_splits = len(self.splits)
            splits_per_worker = total_splits // num_workers
            remainder = total_splits % num_workers

            # Workers with id < remainder get one extra split
            if worker_id < remainder:
                start_idx = worker_id * (splits_per_worker + 1)
                end_idx = start_idx + splits_per_worker + 1
            else:
                start_idx = worker_id * splits_per_worker + remainder
                end_idx = start_idx + splits_per_worker

            splits_to_process = self.splits[start_idx:end_idx]

        worker_iterator = self.table_read.to_iterator(splits_to_process)

        for offset_row in worker_iterator:
            row_dict = {}
            for i, field_name in enumerate(self.field_names):
                value = offset_row.get_field(i)
                row_dict[field_name] = value
            yield row_dict
