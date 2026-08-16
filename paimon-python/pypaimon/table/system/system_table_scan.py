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

"""Scan / split implementation for system tables.

System tables materialise their entire contents into a single PyArrow
table that is shipped through a single :class:`SystemSplit`. This keeps
the read pipeline trivially correct: there's no manifest pruning, no
predicate pushdown and no parallelism to coordinate, only metadata
that fits comfortably in memory.
"""

from typing import List, Optional, TYPE_CHECKING

import pyarrow

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.plan import Plan
from pypaimon.read.split import Split
from pypaimon.table.row.generic_row import GenericRow

if TYPE_CHECKING:  # pragma: no cover - type-only import
    from pypaimon.table.system.system_table import SystemTable


class SystemSplit(Split):
    """A single in-memory split carrying the whole system table."""

    def __init__(self, arrow_table: pyarrow.Table):
        self._arrow_table = arrow_table

    @property
    def row_count(self) -> int:
        return self._arrow_table.num_rows

    @property
    def files(self) -> List[DataFileMeta]:
        return []

    @property
    def partition(self) -> Optional[GenericRow]:
        return None

    @property
    def bucket(self) -> int:
        return -1

    def arrow_table(self) -> pyarrow.Table:
        return self._arrow_table


class SystemTableScan:
    """Returns a one-element plan containing the entire system table."""

    def __init__(self, system_table: "SystemTable"):
        self.system_table = system_table

    def plan(self) -> Plan:
        arrow_table = self.system_table._build_arrow_table()
        return Plan(_splits=[SystemSplit(arrow_table)])
