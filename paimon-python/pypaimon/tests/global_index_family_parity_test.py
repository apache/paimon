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

"""Java parity when one scalar column has several index families."""

import unittest

import pyarrow as pa

from pypaimon.globalindex.data_evolution_global_index_scanner import DataEvolutionGlobalIndexScanner
from pypaimon.index.index_file_handler import IndexFileHandler
from pypaimon.tests.data_evolution_test_helpers import BatchModeMixin, DataEvolutionTestBase
from pypaimon.utils.range import Range


class GlobalIndexFamilyParityTest(BatchModeMixin, DataEvolutionTestBase, unittest.TestCase):
    table_options = {
        'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true',
        'global-index.enabled': 'true',
        'bucket': '-1',
        'file.format': 'parquet',
        'scan.native-plan.enabled': 'false',
        'read.native.enabled': 'false',
        'write.native.enabled': 'false',
        'commit.native.enabled': 'false',
    }

    def _append(self, table, ids, names):
        self._write_arrow(table, pa.table({
            'id': ids,
            'name': names,
            'age': [10] * len(ids),
            'city': ['x'] * len(ids),
        }, schema=self.pa_schema))

    def _assert_scan(self, table, indexed_ranges, expected_ids):
        read = table.new_read_builder()
        predicate = read.new_predicate_builder().equal('name', 'match')
        with DataEvolutionGlobalIndexScanner.create(table, predicate=predicate) as scanner:
            result = scanner.scan(predicate)
        self.assertEqual(indexed_ranges, result.results().to_range_list())
        read.with_filter(predicate)
        rows = read.new_read().to_arrow(read.new_scan().plan().splits())
        actual = [] if rows is None else sorted(rows.column('id').to_pylist())
        self.assertEqual(expected_ids, actual)

    def test_families_union_matches_across_unequal_coverage(self):
        for first, second in [('btree', 'bitmap'), ('bitmap', 'btree')]:
            for mode in ['fast', 'full', 'detail']:
                with self.subTest(first=first, second=second, mode=mode):
                    options = dict(self.table_options, **{'global-index.search-mode': mode})
                    table = self._create_table(options=options)
                    self._append(table, [1, 2], ['old', 'other'])
                    self.assertGreater(table.create_global_index('name', index_type=first), 0)
                    self._append(table, [3, 4], ['match', 'other'])
                    self.assertGreater(table.create_global_index('name', index_type=second), 0)
                    # A third append is outside both families. Only FULL/DETAIL
                    # extend the indexed selection with these unindexed rows.
                    self._append(table, [5], ['match'])
                    coverage = {}
                    for entry in IndexFileHandler(table).scan(None):
                        meta = entry.index_file.global_index_meta
                        if meta is not None:
                            coverage.setdefault(entry.index_file.index_type, []).append(
                                (meta.row_range_start, meta.row_range_end))
                    self.assertEqual([(0, 1)], coverage[first])
                    self.assertEqual([(0, 3)], coverage[second])
                    # The later family owns the only indexed match. Intersecting
                    # it with the earlier family's empty result loses that row.
                    self._assert_scan(table, [Range(2, 2)], [3] if mode == 'fast' else [3, 5])

    def test_one_family_retains_matches_and_unindexed_fallback(self):
        for family in ['btree', 'bitmap']:
            for mode in ['fast', 'full', 'detail']:
                with self.subTest(family=family, mode=mode):
                    options = dict(self.table_options, **{'global-index.search-mode': mode})
                    table = self._create_table(options=options)
                    self._append(table, [1, 2], ['match', 'other'])
                    self.assertGreater(table.create_global_index('name', index_type=family), 0)
                    self._append(table, [3], ['match'])
                    self._assert_scan(table, [Range(0, 0)], [1] if mode == 'fast' else [1, 3])
