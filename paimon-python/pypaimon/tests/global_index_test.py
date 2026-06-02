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

import unittest
from unittest.mock import patch

import pyarrow as pa

from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.index.index_file_handler import IndexFileHandler
from pypaimon.snapshot.snapshot_manager import SnapshotManager
from pypaimon.tests.data_evolution_test_helpers import (
    BatchModeMixin,
    DataEvolutionTestBase,
)
from pypaimon.utils.range import Range


class GlobalIndexTest(unittest.TestCase):

    def test_chained_or(self):
        result = GlobalIndexResult.create_empty()
        for i in range(600):
            other = GlobalIndexResult.from_range(Range(i * 10, i * 10 + 5))
            result = result.or_(other)

        self.assertEqual(result.results().cardinality(), 3600)

    def test_chained_and(self):
        result = GlobalIndexResult.from_range(Range(0, 10000))
        for i in range(600):
            other = GlobalIndexResult.from_range(Range(0, 10000))
            result = result.and_(other)

        self.assertEqual(result.results().cardinality(), 10001)


class PlanSnapshotFetchRegressionTest(
        BatchModeMixin, DataEvolutionTestBase, unittest.TestCase):

    table_options = {
        'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true',
        'global-index.enabled': 'true',
        'bucket': '-1',
    }

    def test_plan_fetches_latest_snapshot_only_once(self):
        table = self._create_table()
        self._write_arrow(table, pa.table(
            {'id': [1, 2, 3], 'name': ['a', 'b', 'c'],
             'age': [10, 20, 30], 'city': ['x', 'y', 'z']},
            schema=self.pa_schema))

        fresh_table = self.catalog.get_table(table.identifier.get_full_name())
        rb = fresh_table.new_read_builder()
        rb = rb.with_filter(rb.new_predicate_builder().equal('id', 1))

        orig_get_latest = SnapshotManager.get_latest_snapshot
        call_count = [0]

        def counting(self_sm, *args, **kwargs):
            call_count[0] += 1
            return orig_get_latest(self_sm, *args, **kwargs)

        with patch.object(SnapshotManager, 'get_latest_snapshot', counting):
            rb.new_scan().plan().splits()

        self.assertEqual(
            1, call_count[0],
            msg=f"Plan fetched latest snapshot {call_count[0]} times — "
                "duplicate from #7513: manifest_scanner + "
                "GlobalIndexScanner.create both fetch independently.")

    def test_time_travel_plan(self):
        table = self._create_table()
        self._write_arrow(table, pa.table(
            {'id': [1], 'name': ['a'], 'age': [10], 'city': ['x']},
            schema=self.pa_schema))
        snapshot_1_id = table.snapshot_manager().get_latest_snapshot().id
        self._write_arrow(table, pa.table(
            {'id': [2], 'name': ['b'], 'age': [20], 'city': ['y']},
            schema=self.pa_schema))

        travel_table = self.catalog.get_table(
            table.identifier.get_full_name()
        ).copy({'scan.snapshot-id': str(snapshot_1_id)})
        rb = travel_table.new_read_builder()
        rb = rb.with_filter(rb.new_predicate_builder().equal('id', 1))

        orig_scan = IndexFileHandler.scan
        seen_snapshot_ids = []

        def spy_scan(self_h, snapshot, entry_filter=None):
            seen_snapshot_ids.append(snapshot.id if snapshot else None)
            return orig_scan(self_h, snapshot, entry_filter)

        with patch.object(IndexFileHandler, 'scan', spy_scan):
            rb.new_scan().plan().splits()

        self.assertTrue(seen_snapshot_ids,
                        "IndexFileHandler.scan was never called")
        self.assertEqual(
            snapshot_1_id, seen_snapshot_ids[0],
            msg=f"Global index evaluated against snapshot "
                f"{seen_snapshot_ids[0]}, expected time-travel snapshot "
                f"{snapshot_1_id}. Before #7513 was fixed, "
                "GlobalIndexScanner.create self-fetched latest snapshot, "
                "so global index used latest while manifest used the "
                "time-travel snapshot — silent correctness bug.")
