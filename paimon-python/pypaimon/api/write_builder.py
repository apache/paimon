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
from pypaimon.api import BatchTableCommit, BatchTableWrite
from typing import Optional


class BatchWriteBuilder(ABC):
    """An interface for building the TableScan and TableRead."""

    @abstractmethod
    def overwrite(self, static_partition: Optional[dict] = None) -> 'BatchWriteBuilder':
        """
        Overwrite writing, same as the 'INSERT OVERWRITE T PARTITION (...)' semantics of SQL.
        If you pass None, it means OVERWRITE whole table.
        """

    @abstractmethod
    def new_write(self) -> BatchTableWrite:
        """Create a BatchTableWrite to perform batch writing."""

    @abstractmethod
    def new_commit(self) -> BatchTableCommit:
        """Create a BatchTableCommit to perform batch commiting."""
