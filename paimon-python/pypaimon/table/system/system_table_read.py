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

"""Public system-table read facade; filtering executes lazily in Python."""

from typing import Any, Iterator, List, Optional, TYPE_CHECKING

import pandas
import pyarrow

from pypaimon.common.predicate import Predicate
from pypaimon.read.split import Split
from pypaimon.schema.data_types import DataField

if TYPE_CHECKING:
    from pypaimon.table.system.system_table import SystemTable


class SystemTableRead:
    """In-memory read pipeline backed by a single SystemSplit."""

    def __init__(
        self,
        system_table: "SystemTable",
        read_type: List[DataField],
        predicate: Optional[Predicate],
        limit: Optional[int],
    ):
        self.system_table = system_table
        self.read_type = read_type
        self.predicate = predicate
        self.limit = limit

    def to_arrow(self, splits: List[Split]) -> pyarrow.Table:
        from pypaimon.execution.python.read.system_table_read import to_arrow
        return to_arrow(self, splits)

    def to_pandas(self, splits: List[Split]) -> pandas.DataFrame:
        return self.to_arrow(splits).to_pandas()

    def to_iterator(self, splits: List[Split]) -> Iterator[dict]:
        from pypaimon.execution.python.read.system_table_read import to_iterator
        yield from to_iterator(self, splits)

    def to_record_batch_iterator(
        self, splits: List[Split]
    ) -> Iterator[pyarrow.RecordBatch]:
        from pypaimon.execution.python.read.system_table_read import to_record_batch_iterator
        yield from to_record_batch_iterator(self, splits)

    def to_duckdb(
        self,
        splits: List[Split],
        table_name: str,
        connection: Optional[Any] = None,
    ):
        from pypaimon.execution.python.read.system_table_read import to_duckdb
        return to_duckdb(self, splits, table_name, connection)
