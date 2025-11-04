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

from typing import List, Optional

from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read.table_read import TableRead
from pypaimon.read.table_scan import TableScan
from pypaimon.schema.data_types import DataField


class ReadBuilder:
    """Implementation of ReadBuilder for native Python reading."""

    def __init__(self, table):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self._predicate: Optional[Predicate] = None
        self._projection: Optional[List[str]] = None
        self._limit: Optional[int] = None
        self._target_split_size: Optional[int] = None
        self._open_file_cost: Optional[int] = None

    def with_filter(self, predicate: Predicate) -> 'ReadBuilder':
        self._predicate = predicate
        return self

    def with_projection(self, projection: List[str]) -> 'ReadBuilder':
        self._projection = projection
        return self

    def with_limit(self, limit: int) -> 'ReadBuilder':
        self._limit = limit
        return self

    def with_target_split_size(self, size: int) -> 'ReadBuilder':
        """
        Set target size of a source split when scanning a bucket.
        This overrides the table option 'source.split.target-size'.
        
        Args:
            size: Target split size in bytes
        
        Returns:
            ReadBuilder instance for method chaining
        
        Example:
            builder.with_target_split_size(256 * 1024 * 1024)  # 256MB
        """
        self._target_split_size = size
        return self

    def with_open_file_cost(self, cost: int) -> 'ReadBuilder':
        """
        Set open file cost of a source file.
        It is used to avoid reading too many files with a source split.
        This overrides the table option 'source.split.open-file-cost'.
        
        Args:
            cost: Open file cost in bytes
        
        Returns:
            ReadBuilder instance for method chaining
        
        Example:
            builder.with_open_file_cost(8 * 1024 * 1024)  # 8MB
        """
        self._open_file_cost = cost
        return self

    def new_scan(self) -> TableScan:
        return TableScan(
            table=self.table,
            predicate=self._predicate,
            limit=self._limit,
            target_split_size=self._target_split_size,
            open_file_cost=self._open_file_cost
        )

    def new_read(self) -> TableRead:
        return TableRead(
            table=self.table,
            predicate=self._predicate,
            read_type=self.read_type()
        )

    def new_predicate_builder(self) -> PredicateBuilder:
        return PredicateBuilder(self.read_type())

    def read_type(self) -> List[DataField]:
        table_fields = self.table.fields

        if not self._projection:
            return table_fields
        else:
            field_map = {field.name: field for field in self.table.fields}
            return [field_map[name] for name in self._projection if name in field_map]
