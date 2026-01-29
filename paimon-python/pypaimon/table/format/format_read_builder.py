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

from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read.push_down_utils import extract_partition_spec_from_predicate
from pypaimon.schema.data_types import DataField
from pypaimon.table.format.format_table import FormatTable
from pypaimon.table.format.format_table_scan import FormatTableScan
from pypaimon.table.format.format_table_read import FormatTableRead


class FormatReadBuilder:
    def __init__(self, table: FormatTable):
        self.table = table
        self._projection: Optional[List[str]] = None
        self._limit: Optional[int] = None
        self._partition_filter: Optional[dict] = None

    def with_filter(self, predicate: Predicate) -> "FormatReadBuilder":
        """
        Store predicate and, when table has partition keys and no partition filter is set,
        try to extract partition spec from predicate (AND of equality on partition columns)
        and set partition filter for scan, aligned with Java FormatReadBuilder.withFilter.
        Data predicate is not yet applied in read (FormatTableRead does not support filter).
        """
        if self._partition_filter is None and self.table.partition_keys and predicate:
            spec = extract_partition_spec_from_predicate(predicate, self.table.partition_keys)
            if spec is not None:
                self._partition_filter = spec
        return self

    def with_projection(self, projection: List[str]) -> "FormatReadBuilder":
        self._projection = projection
        return self

    def with_limit(self, limit: int) -> "FormatReadBuilder":
        self._limit = limit
        return self

    def with_partition_filter(self, partition_spec: Optional[dict]) -> "FormatReadBuilder":
        self._partition_filter = partition_spec
        return self

    def new_scan(self) -> FormatTableScan:
        return FormatTableScan(
            self.table,
            partition_filter=self._partition_filter,
            limit=self._limit,
        )

    def new_read(self) -> FormatTableRead:
        return FormatTableRead(
            table=self.table,
            projection=self._projection,
            limit=self._limit,
        )

    def new_predicate_builder(self) -> PredicateBuilder:
        return PredicateBuilder(self.read_type())

    def read_type(self) -> List[DataField]:
        if self._projection:
            return [f for f in self.table.fields if f.name in self._projection]
        return list(self.table.fields)
