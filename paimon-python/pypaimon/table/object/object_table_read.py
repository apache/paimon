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

from typing import List, Optional

import pandas
import pyarrow

from pypaimon.table.object.object_split import ObjectSplit

# Shared schema remains available at the public module path.
OBJECT_TABLE_ARROW_SCHEMA = pyarrow.schema([
    pyarrow.field("path", pyarrow.string(), nullable=False),
    pyarrow.field("name", pyarrow.string(), nullable=False),
    pyarrow.field("length", pyarrow.int64(), nullable=False),
    pyarrow.field("mtime", pyarrow.int64(), nullable=False),
    pyarrow.field("atime", pyarrow.int64(), nullable=False),
    pyarrow.field("owner", pyarrow.string(), nullable=True),
])


class ObjectTableRead:
    """Public object-table reader; enumeration executes lazily in Python."""

    def __init__(
        self,
        table,
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ):
        self.table = table
        self.projection = projection
        self.limit = limit

    def to_arrow(self, splits: List[ObjectSplit]) -> pyarrow.Table:
        from pypaimon.execution.python.read.object_table_read import to_arrow
        return to_arrow(self, splits)

    def to_pandas(self, splits: List[ObjectSplit]) -> pandas.DataFrame:
        return self.to_arrow(splits).to_pandas()
