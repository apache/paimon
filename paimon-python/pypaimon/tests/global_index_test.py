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
