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
from typing import TYPE_CHECKING, List

from pypaimon.common.identifier import Identifier
from pypaimon.schema.data_types import RowType
from pypaimon.table.table import Table

if TYPE_CHECKING:  # pragma: no cover - import for type hints only
    import pyarrow

    from pypaimon.table.file_store_table import FileStoreTable


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

    def new_read_builder(self):
        # Read pipeline lives in system_table_scan.py and is wired in a
        # later change; until then keep the contract uniform so callers
        # see a single "read-only" message regardless of which builder
        # they reach for.
        raise NotImplementedError(_READ_ONLY_MESSAGE)

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
