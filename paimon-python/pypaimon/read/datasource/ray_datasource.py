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
Module to read a Paimon table into a Ray Dataset, by using the Ray Datasource API.
"""
from typing import Iterable, List, Optional

from ray.data.datasource import Datasource

from pypaimon.read.datasource.split_provider import SplitProvider
from pypaimon.read.split import Split


# Ray version constants for compatibility
RAY_VERSION_SCHEMA_IN_READ_TASK = "2.48.0"  # Schema moved from BlockMetadata to ReadTask
RAY_VERSION_PER_TASK_ROW_LIMIT = "2.52.0"  # per_task_row_limit parameter introduced


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

    def get_name(self) -> str:
        return f"PaimonTable({self._split_provider.display_name()})"

    def estimate_inmemory_data_size(self) -> Optional[int]:
        from pypaimon.execution.datasource.ray_datasource import estimate_inmemory_data_size
        return estimate_inmemory_data_size(self)

    @staticmethod
    def _distribute_splits_into_equal_chunks(
            splits: Iterable[Split], n_chunks: int
    ) -> List[List[Split]]:
        from pypaimon.execution.datasource.ray_datasource import _distribute_splits_into_equal_chunks
        return _distribute_splits_into_equal_chunks(splits, n_chunks)

    def get_read_tasks(self, parallelism: int, **kwargs) -> List:
        """Return a list of read tasks that can be executed in parallel."""
        if parallelism < 1:
            raise ValueError(f"parallelism must be at least 1, got {parallelism}")
        from pypaimon.execution.datasource.ray_datasource import get_read_tasks
        return get_read_tasks(self, parallelism, **kwargs)
