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
#################################################################################

from abc import ABC, abstractmethod
from pypaimon.api import TableRead, TableScan, Predicate, PredicateBuilder
from typing import List

from pypaimon.api.row_type import RowType


class ReadBuilder(ABC):
    """An interface for building the TableScan and TableRead."""

    @abstractmethod
    def with_filter(self, predicate: Predicate):
        """
        Push filters, will filter the data as much as possible,
        but it is not guaranteed that it is a complete filter.
        """

    @abstractmethod
    def with_projection(self, projection: List[str]) -> 'ReadBuilder':
        """Push nested projection."""

    @abstractmethod
    def with_limit(self, limit: int) -> 'ReadBuilder':
        """Push row number."""

    @abstractmethod
    def new_scan(self) -> TableScan:
        """Create a TableScan to perform batch planning."""

    @abstractmethod
    def new_read(self) -> TableRead:
        """Create a TableRead to read splits."""

    @abstractmethod
    def new_predicate_builder(self) -> PredicateBuilder:
        """Create a builder for Predicate."""

    @abstractmethod
    def read_type(self) -> RowType:
        """
        Return the row type of the builder. If there is a projection inside
        the builder, the row type will only contain the selected fields.
        """
