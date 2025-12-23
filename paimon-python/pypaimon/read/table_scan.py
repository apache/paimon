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

from typing import Optional

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.predicate import Predicate

from pypaimon.read.plan import Plan
from pypaimon.read.scanner.empty_starting_scanner import EmptyStartingScanner
from pypaimon.read.scanner.full_starting_scanner import FullStartingScanner
from pypaimon.read.scanner.incremental_starting_scanner import \
    IncrementalStartingScanner
from pypaimon.read.scanner.starting_scanner import StartingScanner
from pypaimon.snapshot.snapshot_manager import SnapshotManager


class TableScan:
    """Implementation of TableScan for native Python reading."""

    def __init__(self, table, predicate: Optional[Predicate], limit: Optional[int]):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.predicate = predicate
        self.limit = limit
        self.starting_scanner = self._create_starting_scanner()

    def plan(self) -> Plan:
        return self.starting_scanner.scan()

    def _create_starting_scanner(self) -> Optional[StartingScanner]:
        options = self.table.options.options
        if options.contains(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP):
            ts = options.get(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP).split(",")
            if len(ts) != 2:
                raise ValueError(
                    "The incremental-between-timestamp must specific start(exclusive) and end timestamp. But is: " +
                    options.get(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP))
            earliest_snapshot = SnapshotManager(self.table).try_get_earliest_snapshot()
            latest_snapshot = SnapshotManager(self.table).get_latest_snapshot()
            if earliest_snapshot is None or latest_snapshot is None:
                return EmptyStartingScanner()
            start_timestamp = int(ts[0])
            end_timestamp = int(ts[1])
            if start_timestamp >= end_timestamp:
                raise ValueError(
                    "Ending timestamp %s should be >= starting timestamp %s." % (end_timestamp, start_timestamp))
            if (start_timestamp == end_timestamp or start_timestamp > latest_snapshot.time_millis
                    or end_timestamp < earliest_snapshot.time_millis):
                return EmptyStartingScanner()
            return IncrementalStartingScanner.between_timestamps(self.table, self.predicate, self.limit,
                                                                 start_timestamp, end_timestamp)
        return FullStartingScanner(self.table, self.predicate, self.limit)

    def with_shard(self, idx_of_this_subtask, number_of_para_subtasks) -> 'TableScan':
        self.starting_scanner.with_shard(idx_of_this_subtask, number_of_para_subtasks)
        return self
