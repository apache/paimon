"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from abc import ABC, abstractmethod

from pypaimon.read.plan import Plan


class StartingScanner(ABC):
    """Helper class for the first planning of TableScan."""

    @abstractmethod
    def scan(self) -> Plan:
        """Plan the files to read."""

    def with_shard(self, idx_of_this_subtask, number_of_para_subtasks) -> 'TableScan':
        """
        Filter file entries according to the id of the task
        """

    def with_row_shard(self, start_row, end_row) -> 'TableScan':
        """
        Filter file entries by row idx range. The row idx corresponds to the row position of the
        file in all file entries in table scan's partitioned_files.
        """

    def with_row_ranges(self, row_ranges) -> 'TableScan':
        """
        Filter manifest files by row id ranges.
        """
