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
from pypaimon.api import CommitMessage
from typing import List


class BatchTableWrite(ABC):
    """A table write for batch processing. Recommended for one-time committing."""

    @abstractmethod
    def write_arrow(self, table: pa.Table, row_kind: List[int] = None):
        """ Write an arrow table to the writer."""

    @abstractmethod
    def write_arrow_batch(self, record_batch: pa.RecordBatch, row_kind: List[int] = None):
        """ Write an arrow record batch to the writer."""

    @abstractmethod
    def write_pandas(self, dataframe: pd.DataFrame):
        """ Write a pandas dataframe to the writer."""

    @abstractmethod
    def prepare_commit(self) -> List[CommitMessage]:
        """Prepare commit message for TableCommit. Collect incremental files for this writer."""

    @abstractmethod
    def close(self):
        """Close this resource."""
