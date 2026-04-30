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
from typing import Dict, Iterable, List, Optional

import pyarrow
from packaging.version import parse
import ray
from ray.data.datasource import Datasource

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
    """
    Ray Data Datasource implementation for reading Paimon tables.

    Self-contained: only requires a fully-qualified table identifier and the
    catalog options. The catalog, table, and splits are loaded lazily so the
    datasource is cheap to instantiate and easy to serialize across Ray workers
    (mirrors Iceberg's ``IcebergDatasource``).
    """

    def __init__(
        self,
        table_identifier: Optional[str] = None,
        catalog_options: Optional[Dict[str, str]] = None,
        predicate=None,
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
        _resolved=None,
    ):
        """
        Initialize RayDatasource.

        Args:
            table_identifier: Fully qualified table name, e.g. ``"db.table"``.
                Required unless ``_resolved`` is provided.
            catalog_options: Options passed to ``CatalogFactory.create()``.
                Required unless ``_resolved`` is provided.
            predicate: Optional ``Predicate`` for scan-time filtering.
            projection: Optional list of column names to read.
            limit: Optional row limit for the scan.
            _resolved: Internal sentinel used by ``TableRead.to_ray()``. When
                supplied, must be a ``(table, splits, read_type)`` tuple of
                already-resolved values; the catalog/identifier path is then
                skipped. Not part of the public API.
        """
        if _resolved is None:
            if not table_identifier:
                raise ValueError(
                    "table_identifier is required (or pass _resolved=...).")
            if catalog_options is None:
                raise ValueError(
                    "catalog_options is required (or pass _resolved=...).")
            self.table_identifier = table_identifier
            self.catalog_options = catalog_options
            self.predicate = predicate
            self.projection = projection
            self.limit = limit
            self._table = None
            self._splits = None
            self._read_type = None
        else:
            # Pre-resolved bridge path used by ``TableRead.to_ray()`` — the
            # caller has already loaded the table and planned splits, so we
            # short-circuit the catalog lookup and ``ReadBuilder`` planning.
            table, splits, read_type = _resolved
            self.table_identifier = None
            self.catalog_options = None
            self.predicate = predicate
            self.projection = projection
            self.limit = limit
            self._table = table
            self._splits = splits
            self._read_type = read_type
        self._schema = None

    @property
    def table(self):
        """Lazily load the table from the catalog."""
        if self._table is None:
            from pypaimon.catalog.catalog_factory import CatalogFactory
            catalog = CatalogFactory.create(self.catalog_options)
            self._table = catalog.get_table(self.table_identifier)
        return self._table

    @property
    def splits(self):
        """Lazily plan splits from the table."""
        self._ensure_planned()
        return self._splits

    @property
    def read_type(self):
        """Lazily resolve the scan read type from filter / projection / limit."""
        self._ensure_planned()
        return self._read_type

    def _ensure_planned(self):
        """Run the scan plan if either ``_splits`` or ``_read_type`` is missing.

        Both are populated together by a single ``ReadBuilder`` plan, so the
        ``splits`` / ``read_type`` properties share this entry point instead
        of duplicating the ``if x is None`` check.
        """
        if self._splits is not None and self._read_type is not None:
            return
        from pypaimon.read.read_builder import ReadBuilder
        rb = ReadBuilder(self.table)
        if self.predicate is not None:
            rb = rb.with_filter(self.predicate)
        if self.projection is not None:
            rb = rb.with_projection(self.projection)
        if self.limit is not None:
            rb = rb.with_limit(self.limit)
        self._read_type = rb.read_type()
        self._splits = rb.new_scan().plan().splits()

    @classmethod
    def _from_table_read(cls, table_read, splits):
        """Bridge for ``TableRead.to_ray()`` — wraps an already-resolved
        ``(table_read, splits)`` pair without going back through the catalog.

        Forwards through ``__init__`` via the ``_resolved`` sentinel so any
        future field added to ``__init__`` is automatically initialized for
        this path too.
        """
        return cls(
            predicate=table_read.predicate,
            _resolved=(table_read.table, splits, table_read.read_type),
        )

    def get_name(self) -> str:
        if self.table_identifier:
            return f"PaimonTable({self.table_identifier})"
        identifier = self.table.identifier
        table_name = identifier.get_full_name() if hasattr(identifier, 'get_full_name') else str(identifier)
        return f"PaimonTable({table_name})"

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
