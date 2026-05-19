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

"""The ``$branches`` system table — every named branch and its mtime."""

from typing import List

import pyarrow

from pypaimon.schema.data_types import AtomicType, DataField, RowType
from pypaimon.table.system.system_table import SystemTable


TABLE_TYPE = RowType(False, [
    DataField(0, "branch_name", AtomicType("STRING", nullable=False)),
    DataField(1, "create_time", AtomicType("TIMESTAMP(3)", nullable=False)),
])


_TIMESTAMP_TYPE = pyarrow.timestamp("ms")


class BranchesTable(SystemTable):
    """Mirrors Java ``BranchesTable``."""

    def system_table_name(self) -> str:
        return "branches"

    def row_type(self) -> RowType:
        return TABLE_TYPE

    def primary_keys(self) -> List[str]:
        return ["branch_name"]

    def _build_arrow_table(self) -> pyarrow.Table:
        branch_manager = self.base_table.branch_manager()
        names = list(branch_manager.branches())
        create_times: List[int] = []
        for name in names:
            ms = branch_manager.branch_create_time(name)
            # Java declares create_time NOT NULL. When the backing store
            # cannot provide an mtime (some remote object stores via
            # PyArrowFileIO, REST-managed branches until the server side
            # surfaces it) fall back to epoch 0 so the schema contract
            # holds. TODO: revisit once REST exposes branch creation
            # timestamps.
            create_times.append(0 if ms is None else int(ms))
        return pyarrow.table({
            "branch_name": pyarrow.array(names, type=pyarrow.string()),
            "create_time": pyarrow.array(create_times, type=_TIMESTAMP_TYPE),
        })
