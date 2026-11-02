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

from typing import Any, Iterator, List, Optional

import pandas
import pyarrow

from pypaimon.table.format.format_data_split import FormatDataSplit
from pypaimon.table.format.format_table import FormatTable


class FormatTableRead:
    """Public format-table reader; file decoding executes lazily in Python."""

    def __init__(
        self,
        table: FormatTable,
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ):
        self.table = table
        self.projection = projection
        self.limit = limit

    def to_arrow(self, splits: List[FormatDataSplit]) -> pyarrow.Table:
        from pypaimon.execution.python.read.format_table_read import to_arrow
        return to_arrow(self, splits)

    def to_pandas(self, splits: List[FormatDataSplit]) -> pandas.DataFrame:
        return self.to_arrow(splits).to_pandas()

    def to_iterator(self, splits: List[FormatDataSplit]) -> Iterator[Any]:
        from pypaimon.execution.python.read.format_table_read import to_iterator
        yield from to_iterator(self, splits)
