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

import pandas as pd
import pyarrow as pa

from abc import ABC, abstractmethod
from pypaimon.api import Split
from typing import List, Optional, TYPE_CHECKING, Iterator

if TYPE_CHECKING:
    import ray
    from duckdb.duckdb import DuckDBPyConnection


class TableRead(ABC):
    """To read data from data splits."""

    @abstractmethod
    def to_iterator(self, splits: List[Split]) -> Iterator[tuple]:
        """Read data from splits and converted to pyarrow.Table format."""

    @abstractmethod
    def to_arrow(self, splits: List[Split]) -> pa.Table:
        """Read data from splits and converted to pyarrow.Table format."""

    @abstractmethod
    def to_arrow_batch_reader(self, splits: List[Split]) -> pa.RecordBatchReader:
        """Read data from splits and converted to pyarrow.RecordBatchReader format."""

    @abstractmethod
    def to_pandas(self, splits: List[Split]) -> pd.DataFrame:
        """Read data from splits and converted to pandas.DataFrame format."""

    @abstractmethod
    def to_duckdb(
            self,
            splits: List[Split],
            table_name: str,
            connection: Optional["DuckDBPyConnection"] = None) -> "DuckDBPyConnection":
        """Convert splits into an in-memory DuckDB table which can be queried."""

    @abstractmethod
    def to_ray(self, splits: List[Split]) -> "ray.data.dataset.Dataset":
        """Convert splits into a Ray dataset format."""
