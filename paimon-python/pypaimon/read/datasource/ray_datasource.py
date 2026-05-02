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
from typing import Iterable, List, Optional

import pyarrow
from packaging.version import parse
import ray
from ray.data.datasource import Datasource

from pypaimon.read.datasource.split_provider import SplitProvider
from pypaimon.read.split import Split
from pypaimon.schema.data_types import PyarrowFieldParser

logger = logging.getLogger(__name__)

# Ray version constants for compatibility
RAY_VERSION_SCHEMA_IN_READ_TASK = "2.48.0"  # Schema moved from BlockMetadata to ReadTask
RAY_VERSION_PER_TASK_ROW_LIMIT = "2.52.0"  # per_task_row_limit parameter introduced


def _paimon_read_task(splits, table, predicate, read_type, schema):
    """Module-level read function that yields Arrow tables per batch.

    Using a generator avoids loading every split's rows into memory at once —
    memory usage stays proportional to one batch rather than the whole chunk.
    """
    from pypaimon.read.table_read import TableRead
    worker_table_read = TableRead(table, predicate, read_type)
    batch_reader = worker_table_read.to_arrow_batch_reader(splits)

    has_data = False
    for batch in iter(batch_reader.read_next_batch, None):
        if batch.num_rows == 0:
            continue
        has_data = True
        yield pyarrow.Table.from_batches([batch], schema=schema)

    if not has_data:
        yield pyarrow.Table.from_arrays(
            [pyarrow.array([], type=field.type) for field in schema],
            schema=schema,
        )


class RayDatasource(Datasource):
    """Ray Data ``Datasource`` implementation for reading Paimon tables.

    Holds a :class:`SplitProvider` that supplies the four planning artefacts
    needed to build read tasks (table, splits, read_type, predicate). Two
    provider implementations exist today:

    * :class:`CatalogSplitProvider` — resolves a fully-qualified table
      identifier through the catalog and runs the ``ReadBuilder`` plan.
      Used by the public :func:`pypaimon.ray.read_paimon` facade.
    * :class:`PreResolvedSplitProvider` — wraps an already-resolved
      ``(table, splits, read_type, predicate)`` tuple. Used by the legacy
      ``TableRead.to_ray()`` bridge to skip a second catalog round-trip.

    Both providers are cheap to instantiate; they defer the catalog
    round-trip and split planning until the first read.
    """

    def __init__(self, split_provider: SplitProvider):
        """Initialize a RayDatasource.

        Args:
            split_provider: The :class:`SplitProvider` that supplies the
                table, splits, read_type, and predicate. Construct one with
                :class:`CatalogSplitProvider` (from a table identifier +
                catalog options) or :class:`PreResolvedSplitProvider` (from
                an already-resolved ``TableRead``).
        """
        self._split_provider = split_provider
        self._schema = None

    @property
    def split_provider(self) -> SplitProvider:
        return self._split_provider

    @property
    def table(self):
        return self._split_provider.table()

    @property
    def splits(self) -> List[Split]:
        return self._split_provider.splits()

    @property
    def read_type(self):
        return self._split_provider.read_type()

    @property
    def predicate(self):
        return self._split_provider.predicate()

    def get_name(self) -> str:
        return f"PaimonTable({self._split_provider.display_name()})"

    def estimate_inmemory_data_size(self) -> Optional[int]:
        splits = self.splits
        if not splits:
            return 0

        total_size = sum(split.file_size for split in splits)
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

        if parallelism < 1:
            raise ValueError(f"parallelism must be at least 1, got {parallelism}")

        splits = self.splits
        if not splits:
            return []

        if self._schema is None:
            self._schema = PyarrowFieldParser.from_paimon_schema(self.read_type)

        if parallelism > len(splits):
            parallelism = len(splits)
            logger.warning(
                f"Reducing the parallelism to {parallelism}, as that is the number of splits"
            )

        # Store necessary information for creating readers in Ray workers.
        # Pulling them into locals avoids closure-over-self and reduces
        # serialization overhead (see ray-project/ray#49107).
        table = self.table
        predicate = self.predicate
        read_type = self.read_type
        schema = self._schema

        read_tasks = []

        # Distribute splits across tasks using load balancing algorithm
        for chunk_splits in self._distribute_splits_into_equal_chunks(splits, parallelism):
            if not chunk_splits:
                continue

            # Calculate metadata for this chunk
            total_rows = 0
            total_size = 0

            for split in chunk_splits:
                if predicate is None:
                    # Only estimate rows if no predicate (predicate filtering changes row count)
                    row_count = None
                    if hasattr(split, 'merged_row_count'):
                        merged_count = split.merged_row_count()
                        if merged_count is not None:
                            row_count = merged_count
                    if row_count is None and hasattr(split, 'row_count') and split.row_count > 0:
                        row_count = split.row_count
                    if row_count is not None and row_count > 0:
                        total_rows += row_count
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

            read_fn = partial(
                _paimon_read_task,
                chunk_splits,
                table=table,
                predicate=predicate,
                read_type=read_type,
                schema=schema,
            )
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
