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

from typing import List, Optional


class ObjectReadBuilder:
    """Read builder for ObjectTable.

    Supports projection (column selection) and limit (row count).
    """

    def __init__(self, table):
        from pypaimon.table.object.object_table import ObjectTable
        self.table: ObjectTable = table
        self._projection: Optional[List[str]] = None
        self._limit: Optional[int] = None

    def with_projection(self, projection: List[str]) -> "ObjectReadBuilder":
        self._projection = projection
        return self

    def with_limit(self, limit: int) -> "ObjectReadBuilder":
        self._limit = limit
        return self

    def new_scan(self):
        from pypaimon.table.object.object_table_scan import ObjectTableScan
        return ObjectTableScan(self.table)

    def new_read(self):
        from pypaimon.table.object.object_table_read import ObjectTableRead
        return ObjectTableRead(
            table=self.table,
            projection=self._projection,
            limit=self._limit,
        )
