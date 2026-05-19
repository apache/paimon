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

"""The ``$options`` system table — every option from the latest schema."""

from typing import List

import pyarrow

from pypaimon.schema.data_types import AtomicType, DataField, RowType
from pypaimon.table.system.system_table import SystemTable


TABLE_TYPE = RowType(False, [
    DataField(0, "key", AtomicType("STRING", nullable=False)),
    DataField(1, "value", AtomicType("STRING", nullable=False)),
])


class OptionsTable(SystemTable):
    """The ``$options`` system table: one (key, value) row per option."""

    def system_table_name(self) -> str:
        return "options"

    def row_type(self) -> RowType:
        return TABLE_TYPE

    def primary_keys(self) -> List[str]:
        return ["key"]

    def _build_arrow_table(self) -> pyarrow.Table:
        schema = self.base_table.schema_manager.latest()
        options = schema.options if schema is not None else {}
        keys: List[str] = []
        values: List[str] = []
        for key, value in options.items():
            keys.append(str(key))
            values.append("" if value is None else str(value))
        return pyarrow.table({"key": keys, "value": values})
