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

"""Abstract base class for read-only system tables.

System tables expose snapshot, schema, manifest and other catalog
metadata as queryable tables. They share the :class:`Table` contract
so callers use the same ``new_read_builder().new_read().to_arrow()``
flow as a regular data table, but every write or search builder
raises :class:`NotImplementedError` to signal the read-only contract.
"""

from abc import abstractmethod
from typing import TYPE_CHECKING, List, Optional

from pypaimon.common.identifier import Identifier
from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.schema.data_types import DataField, RowType
from pypaimon.table.table import Table

if TYPE_CHECKING:  # pragma: no cover - import for type hints only
    import pyarrow

    from pypaimon.table.file_store_table import FileStoreTable
    from pypaimon.table.system.system_table_read import SystemTableRead
    from pypaimon.table.system.system_table_scan import SystemTableScan


_READ_ONLY_MESSAGE = "System table is read-only"


class SystemTable(Table):
    """Read-only view backed by metadata of a base data table.

    Subclasses implement three hooks:

    * :meth:`system_table_name` returns the short identifier used after
      the ``$`` separator (e.g. ``"snapshots"``).
    * :meth:`row_type` declares the table schema as a :class:`RowType`.
    * :meth:`_build_arrow_table` materialises the entire table as a
      single PyArrow ``Table`` — system tables are typically small
      enough that one in-memory pass is the simplest correct shape.
    """

    def __init__(self, base_table: "FileStoreTable"):
        self.base_table = base_table
        self.file_io = base_table.file_io
        self.table_path = base_table.table_path
        self.identifier = Identifier.create(
            base_table.identifier.get_database_name(),
            base_table.identifier.get_table_name(),
            branch=base_table.identifier.get_branch_name(),
            system_table=self.system_table_name(),
        )

    @abstractmethod
    def system_table_name(self) -> str:
        """Return the short name appearing after the ``$`` separator."""

    @abstractmethod
    def row_type(self) -> RowType:
        """Return the schema of this system table."""

    def primary_keys(self) -> List[str]:
        """Return primary key column names; default is no primary key."""
        return []

    @abstractmethod
    def _build_arrow_table(self) -> "pyarrow.Table":
        """Return the entire table contents as a PyArrow Table."""

    def new_read_builder(self) -> "SystemReadBuilder":
        return SystemReadBuilder(self)

    def new_stream_read_builder(self):
        raise NotImplementedError(_READ_ONLY_MESSAGE)

    def new_batch_write_builder(self):
        raise NotImplementedError(_READ_ONLY_MESSAGE)

    def new_stream_write_builder(self):
        raise NotImplementedError(_READ_ONLY_MESSAGE)

    def new_full_text_search_builder(self):
        raise NotImplementedError(_READ_ONLY_MESSAGE)

    def new_vector_search_builder(self):
        raise NotImplementedError(_READ_ONLY_MESSAGE)


class SystemReadBuilder:
    """ReadBuilder-shaped facade exposing the system table's data.

    Mirrors :class:`pypaimon.read.read_builder.ReadBuilder` enough that
    callers can reach for ``with_filter``, ``with_projection``,
    ``with_limit``, ``new_predicate_builder``, ``read_type``,
    ``new_scan`` and ``new_read`` without knowing they are talking to a
    system table.
    """

    def __init__(self, system_table: SystemTable):
        self.system_table = system_table
        self._predicate: Optional[Predicate] = None
        self._projection: Optional[List[str]] = None
        self._limit: Optional[int] = None

    def with_filter(self, predicate: Predicate) -> "SystemReadBuilder":
        self._predicate = predicate
        return self

    def with_projection(self, projection: List[str]) -> "SystemReadBuilder":
        self._projection = list(projection)
        return self

    def with_limit(self, limit: int) -> "SystemReadBuilder":
        self._limit = limit
        return self

    def new_predicate_builder(self) -> PredicateBuilder:
        return PredicateBuilder(self.read_type())

    def read_type(self) -> List[DataField]:
        all_fields = self.system_table.row_type().fields
        if not self._projection:
            return list(all_fields)
        field_map = {f.name: f for f in all_fields}
        # Silently skip unknown names to match ReadBuilder.with_projection.
        return [field_map[name] for name in self._projection
                if name in field_map]

    def new_scan(self) -> "SystemTableScan":
        from pypaimon.table.system.system_table_scan import SystemTableScan
        return SystemTableScan(self.system_table)

    def new_read(self) -> "SystemTableRead":
        from pypaimon.table.system.system_table_read import SystemTableRead
        return SystemTableRead(
            system_table=self.system_table,
            read_type=self.read_type(),
            predicate=self._predicate,
            limit=self._limit,
        )
