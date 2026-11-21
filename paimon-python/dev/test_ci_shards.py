# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from contextlib import redirect_stdout
import io
from types import SimpleNamespace
import unittest

from ci_shards import COVERAGE_KEYS, matrix_entries, select_shard, verify_reports


class CiShardsTest(unittest.TestCase):
    def test_file_stays_together_and_every_test_is_selected_once(self):
        items = [SimpleNamespace(nodeid='pypaimon/tests/test_{}.py::test[{}]'.format(file, case))
                 for file in range(13) for case in range(file + 1)]
        selected_by_shard = []
        fingerprints = set()
        for index in range(1, 5):
            ordered = items if index % 2 else list(reversed(items))
            selected, deselected, report = select_shard(ordered, 'native', '3.13', index)
            self.assertEqual(len(selected) + len(deselected), len(items))
            self.assertEqual(report['selected'], len(selected))
            self.assertTrue(selected)
            selected_by_shard.append(selected)
            fingerprints.add(report['collection'])

        self.assertEqual(len(fingerprints), 1)
        self.assertCountEqual([item.nodeid for shard in selected_by_shard for item in shard],
                              [item.nodeid for item in items])
        assigned_paths = {item.nodeid.split('::')[0] for item in items}
        self.assertEqual(sum(len({item.nodeid.split('::')[0] for item in shard})
                             for shard in selected_by_shard), len(assigned_paths))

    def reports(self):
        reports = []
        entries = matrix_entries()
        for entry in entries:
            execution, version, shard = entry['execution'], entry['python-version'], entry['shard']
            lane_count = sum(other['execution'] == execution and other['python-version'] == version
                             for other in entries)
            reports.append({
                'execution': execution, 'python': version, 'shard': shard,
                'collection': 'same-collection', 'collected': lane_count, 'selected': 1,
                'native': dict.fromkeys(COVERAGE_KEYS, int(shard == lane_count)),
            })
        return reports

    def test_per_version_coverage_can_span_multiple_shards(self):
        reports = self.reports()
        reports[0]['native']['update.merge'] = 1
        reports[3]['native']['update.merge'] = 0
        with redirect_stdout(io.StringIO()):
            verify_reports(reports)

    def test_missing_shard_different_collection_and_missing_backend_fail(self):
        with self.assertRaisesRegex(ValueError, 'one report'):
            verify_reports(self.reports()[1:])
        reports = self.reports()
        reports[0]['collection'] = 'different-collection'
        with self.assertRaisesRegex(ValueError, 'collected different tests'):
            verify_reports(reports)
        reports = self.reports()
        reports[3]['native']['update.merge'] = 0
        with redirect_stdout(io.StringIO()), self.assertRaisesRegex(ValueError, '3.10 did not exercise update.merge'):
            verify_reports(reports)
