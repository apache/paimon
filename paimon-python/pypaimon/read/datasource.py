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
Module to read a Paimon table into a Ray Dataset, by using the Ray Datasource API.
"""
import heapq
import itertools
import logging
from functools import partial
from typing import List, Optional, Iterable

import pyarrow
from packaging.version import parse
import ray
import torch

from pypaimon.read.split import Split
from pypaimon.read.table_read import TableRead
from pypaimon.schema.data_types import PyarrowFieldParser

logger = logging.getLogger(__name__)

# Ray version constants for compatibility
RAY_VERSION_SCHEMA_IN_READ_TASK = "2.48.0"  # Schema moved from BlockMetadata to ReadTask
RAY_VERSION_PER_TASK_ROW_LIMIT = "2.52.0"  # per_task_row_limit parameter introduced

from ray.data.datasource import Datasource

from torch.utils.data import Dataset, IterableDataset


class RayDatasource(Datasource):
    """
    Ray Data Datasource implementation for reading Paimon tables.

    This datasource enables distributed parallel reading of Paimon table splits,
    allowing Ray to read multiple splits concurrently across the cluster.
    """

    def __init__(self, table_read: TableRead, splits: List[Split]):
        """
        Initialize PaimonDatasource.

        Args:
            table_read: TableRead instance for reading data
            splits: List of splits to read
        """
        self.table_read = table_read
        self.splits = splits
        self._schema = None

    def get_name(self) -> str:
        identifier = self.table_read.table.identifier
        table_name = identifier.get_full_name() if hasattr(identifier, 'get_full_name') else str(identifier)
        return f"PaimonTable({table_name})"

    def estimate_inmemory_data_size(self) -> Optional[int]:
        if not self.splits:
            return 0

        # Sum up file sizes from all splits
        total_size = sum(split.file_size for split in self.splits)
        return total_size if total_size > 0 else None

    @staticmethod
    def _distribute_splits_into_equal_chunks(
            splits: Iterable[Split], n_chunks: int
    ) -> List[List[Split]]:
        """
        Implement a greedy knapsack algorithm to distribute the splits across tasks,
        based on their file size, as evenly as possible.
        """
        chunks = [list() for _ in range(n_chunks)]
        chunk_sizes = [(0, chunk_id) for chunk_id in range(n_chunks)]
        heapq.heapify(chunk_sizes)

        # From largest to smallest, add the splits to the smallest chunk one at a time
        for split in sorted(
                splits, key=lambda s: s.file_size if hasattr(s, 'file_size') and s.file_size > 0 else 0, reverse=True
        ):
            smallest_chunk = heapq.heappop(chunk_sizes)
            chunks[smallest_chunk[1]].append(split)
            split_size = split.file_size if hasattr(split, 'file_size') and split.file_size > 0 else 0
            heapq.heappush(
                chunk_sizes,
                (smallest_chunk[0] + split_size, smallest_chunk[1]),
            )

        return chunks

    def get_read_tasks(self, parallelism: int, **kwargs) -> List:
        """Return a list of read tasks that can be executed in parallel."""
        from ray.data.datasource import ReadTask
        from ray.data.block import BlockMetadata

        per_task_row_limit = kwargs.get('per_task_row_limit', None)

        # Validate parallelism parameter
        if parallelism < 1:
            raise ValueError(f"parallelism must be at least 1, got {parallelism}")

        # Get schema for metadata
        if self._schema is None:
            self._schema = PyarrowFieldParser.from_paimon_schema(self.table_read.read_type)

        # Adjust parallelism if it exceeds the number of splits
        if parallelism > len(self.splits):
            parallelism = len(self.splits)
            logger.warning(
                f"Reducing the parallelism to {parallelism}, as that is the number of splits"
            )

        # Store necessary information for creating readers in Ray workers
        # Extract these to avoid serializing the entire self object in closures
        table = self.table_read.table
        predicate = self.table_read.predicate
        read_type = self.table_read.read_type
        schema = self._schema

        # Create a partial function to avoid capturing self in closure
        # This reduces serialization overhead (see https://github.com/ray-project/ray/issues/49107)
        def _get_read_task(
                splits: List[Split],
                table=table,
                predicate=predicate,
                read_type=read_type,
                schema=schema,
        ) -> Iterable[pyarrow.Table]:
            """Read function that will be executed by Ray workers."""
            from pypaimon.read.table_read import TableRead
            worker_table_read = TableRead(table, predicate, read_type)

            # Read all splits in this chunk
            arrow_table = worker_table_read.to_arrow(splits)

            # Return as a list to allow Ray to split into multiple blocks if needed
            if arrow_table is not None and arrow_table.num_rows > 0:
                return [arrow_table]
            else:
                # Return empty table with correct schema
                empty_table = pyarrow.Table.from_arrays(
                    [pyarrow.array([], type=field.type) for field in schema],
                    schema=schema
                )
                return [empty_table]

        # Use partial to create read function without capturing self
        get_read_task = partial(
            _get_read_task,
            table=table,
            predicate=predicate,
            read_type=read_type,
            schema=schema,
        )

        read_tasks = []

        # Distribute splits across tasks using load balancing algorithm
        for chunk_splits in self._distribute_splits_into_equal_chunks(self.splits, parallelism):
            if not chunk_splits:
                continue

            # Calculate metadata for this chunk
            total_rows = 0
            total_size = 0

            for split in chunk_splits:
                if predicate is None:
                    # Only estimate rows if no predicate (predicate filtering changes row count)
                    if hasattr(split, 'row_count') and split.row_count > 0:
                        total_rows += split.row_count
                if hasattr(split, 'file_size') and split.file_size > 0:
                    total_size += split.file_size

            input_files = list(itertools.chain.from_iterable(
                split.file_paths
                for split in chunk_splits
                if hasattr(split, 'file_paths') and split.file_paths
            ))

            # For PrimaryKey tables, we can't accurately estimate num_rows before merge
            if table and table.is_primary_key_table:
                num_rows = None  # Let Ray calculate actual row count after merge
            elif predicate is not None:
                num_rows = None  # Can't estimate with predicate filtering
            else:
                num_rows = total_rows if total_rows > 0 else None
            size_bytes = total_size if total_size > 0 else None

            metadata_kwargs = {
                'num_rows': num_rows,
                'size_bytes': size_bytes,
                'input_files': input_files if input_files else None,
                'exec_stats': None,  # Will be populated by Ray during execution
            }

            if parse(ray.__version__) < parse(RAY_VERSION_SCHEMA_IN_READ_TASK):
                metadata_kwargs['schema'] = schema

            metadata = BlockMetadata(**metadata_kwargs)

            read_fn = partial(get_read_task, chunk_splits)
            read_task_kwargs = {
                'read_fn': read_fn,
                'metadata': metadata,
            }

            if parse(ray.__version__) >= parse(RAY_VERSION_SCHEMA_IN_READ_TASK):
                read_task_kwargs['schema'] = schema

            if parse(ray.__version__) >= parse(RAY_VERSION_PER_TASK_ROW_LIMIT) and per_task_row_limit is not None:
                read_task_kwargs['per_task_row_limit'] = per_task_row_limit

            read_tasks.append(ReadTask(**read_task_kwargs))

        return read_tasks


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
        if self._data is not None:
            return len(self._data)
        else:
            return 0

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
