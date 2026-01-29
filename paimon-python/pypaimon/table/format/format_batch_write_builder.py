# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Optional

from pypaimon.table.format.format_table import FormatTable
from pypaimon.table.format.format_table_commit import FormatTableCommit
from pypaimon.table.format.format_table_write import FormatTableWrite


class FormatBatchWriteBuilder:
    def __init__(self, table: FormatTable):
        self.table = table
        self._overwrite = False
        self._static_partition: Optional[dict] = None

    def overwrite(self, static_partition: Optional[dict] = None) -> "FormatBatchWriteBuilder":
        self._overwrite = True
        self._validate_static_partition(static_partition)
        self._static_partition = static_partition if static_partition is not None else {}
        return self

    def _validate_static_partition(self, static_partition: Optional[dict]) -> None:
        if not static_partition:
            return
        if not self.table.partition_keys:
            raise ValueError(
                "Format table is not partitioned, static partition values are not allowed."
            )
        for key in static_partition:
            if key not in self.table.partition_keys:
                raise ValueError(f"Unknown static partition column: {key}")

    def new_write(self) -> FormatTableWrite:
        return FormatTableWrite(self.table, overwrite=self._overwrite)

    def new_commit(self) -> FormatTableCommit:
        return FormatTableCommit(table=self.table)
