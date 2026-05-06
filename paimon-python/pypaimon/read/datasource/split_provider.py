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
"""SplitProvider abstraction used by ``RayDatasource``.

The datasource only needs four things to build read tasks: the underlying
table, the planned splits, the scan read type, and the optional predicate.
``SplitProvider`` decouples how those four items are obtained so the same
datasource can serve both the public ``read_paimon`` facade (which only has
a table identifier + catalog options) and the legacy ``TableRead.to_ray()``
bridge (which already has a fully resolved ``TableRead``).
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional

from pypaimon.read.split import Split


class SplitProvider(ABC):
    """Source of the planning artefacts required by ``RayDatasource``."""

    @abstractmethod
    def table(self):
        """Return the ``FileStoreTable`` to read."""

    @abstractmethod
    def splits(self) -> List[Split]:
        """Return the planned splits."""

    @abstractmethod
    def read_type(self):
        """Return the scan read type (row / record type)."""

    @abstractmethod
    def predicate(self):
        """Return the scan-time predicate, or ``None``."""

    @abstractmethod
    def display_name(self) -> str:
        """Return a short, human-readable name for the source.

        Used by ``RayDatasource.get_name()`` so the datasource doesn't have
        to peek at concrete provider types to format its name.
        """


class CatalogSplitProvider(SplitProvider):
    """Plan splits from a fully-qualified table identifier and catalog options.

    Resolves the catalog and the table lazily on first access, then runs a
    single ``ReadBuilder`` plan to populate splits + read type together. The
    same provider should be reused across calls — the planning is cached.
    """

    def __init__(
        self,
        table_identifier: str,
        catalog_options: Dict[str, str],
        predicate=None,
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ):
        if not table_identifier:
            raise ValueError("table_identifier is required")
        if catalog_options is None:
            raise ValueError("catalog_options is required")
        self._table_identifier = table_identifier
        self._catalog_options = catalog_options
        self._predicate = predicate
        self._projection = projection
        self._limit = limit
        self._table_cached = None
        self._splits_cached = None
        self._read_type_cached = None

    def _ensure_table(self):
        if self._table_cached is None:
            from pypaimon.catalog.catalog_factory import CatalogFactory
            catalog = CatalogFactory.create(self._catalog_options)
            self._table_cached = catalog.get_table(self._table_identifier)
        return self._table_cached

    def _ensure_planned(self):
        if self._splits_cached is not None and self._read_type_cached is not None:
            return
        from pypaimon.read.read_builder import ReadBuilder
        rb = ReadBuilder(self._ensure_table())
        if self._predicate is not None:
            rb = rb.with_filter(self._predicate)
        if self._projection is not None:
            rb = rb.with_projection(self._projection)
        if self._limit is not None:
            rb = rb.with_limit(self._limit)
        self._read_type_cached = rb.read_type()
        self._splits_cached = rb.new_scan().plan().splits()

    @property
    def table_identifier(self) -> str:
        return self._table_identifier

    def table(self):
        return self._ensure_table()

    def splits(self) -> List[Split]:
        self._ensure_planned()
        return self._splits_cached

    def read_type(self):
        self._ensure_planned()
        return self._read_type_cached

    def predicate(self):
        return self._predicate

    def display_name(self) -> str:
        return self._table_identifier


class PreResolvedSplitProvider(SplitProvider):
    """Wrap an already-planned ``(table, splits, read_type, predicate)`` tuple.

    Used by ``TableRead.to_ray()`` where the caller has already built a
    ``TableRead`` and planned splits, so the catalog round-trip should be
    skipped.
    """

    def __init__(self, table, splits: List[Split], read_type, predicate=None):
        self._table = table
        self._splits = splits
        self._read_type = read_type
        self._predicate = predicate

    def table(self):
        return self._table

    def splits(self) -> List[Split]:
        return self._splits

    def read_type(self):
        return self._read_type

    def predicate(self):
        return self._predicate

    def display_name(self) -> str:
        identifier = self._table.identifier
        if hasattr(identifier, 'get_full_name'):
            return identifier.get_full_name()
        return str(identifier)
