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

import unittest

from pypaimon.manifest.simple_stats_evolutions import SimpleStatsEvolutions
from pypaimon.schema.data_types import DataField, AtomicType
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.table.row.generic_row import GenericRow


class SimpleStatsEvolutionsTest(unittest.TestCase):

    def _make_fields(self, field_defs):
        """Helper: field_defs is list of (id, name, type_str)."""
        return [DataField(fid, fname, AtomicType(ftype)) for fid, fname, ftype in field_defs]

    def _make_stats(self, fields, min_vals, max_vals, null_counts):
        """Helper: build SimpleStats from parallel lists."""
        df = [DataField(fid, fname, AtomicType(ftype)) for fid, fname, ftype in fields]
        return SimpleStats(GenericRow(min_vals, df), GenericRow(max_vals, df), null_counts)

    def test_same_schema_no_mapping(self):
        """When data schema == table schema, index_mapping should be None."""
        fields = self._make_fields([(0, 'a', 'INT'), (1, 'b', 'STRING')])
        schemas = {0: fields}
        evolutions = SimpleStatsEvolutions(lambda sid: schemas[sid], 0)
        evolution = evolutions.get_or_create(0)
        self.assertIsNone(evolution.index_mapping)

    def test_added_column(self):
        """New column: mapping is -1, null_count = row_count."""
        data_fields = self._make_fields([(0, 'a', 'INT'), (1, 'b', 'INT')])
        table_fields = self._make_fields([(0, 'a', 'INT'), (1, 'b', 'INT'), (2, 'c', 'INT')])
        schemas = {0: data_fields, 1: table_fields}
        evolutions = SimpleStatsEvolutions(lambda sid: schemas[sid], 1)
        evolution = evolutions.get_or_create(0)

        self.assertEqual(evolution.index_mapping, [0, 1, -1])

        stats = self._make_stats([(0, 'a', 'INT'), (1, 'b', 'INT')],
                                 [1, 2], [10, 20], [0, 5])
        evolved = evolution.evolution(stats, row_count=500, stats_fields=None)
        self.assertIsNone(evolved.min_values.get_field(2))
        self.assertIsNone(evolved.max_values.get_field(2))
        self.assertEqual(evolved.null_counts, [0, 5, 500])

    def test_dropped_column(self):
        """Dropped column is excluded from mapping."""
        data_fields = self._make_fields([(0, 'a', 'INT'), (1, 'b', 'STRING'), (2, 'c', 'BIGINT')])
        table_fields = self._make_fields([(0, 'a', 'INT'), (2, 'c', 'BIGINT')])
        schemas = {0: data_fields, 1: table_fields}
        evolutions = SimpleStatsEvolutions(lambda sid: schemas[sid], 1)
        evolution = evolutions.get_or_create(0)
        # a->0, c->2 (b dropped)
        self.assertEqual(evolution.index_mapping, [0, 2])

    def test_renamed_column(self):
        """Column rename: matched by field ID, not name."""
        data_fields = self._make_fields([(0, 'old_name', 'INT'), (1, 'b', 'INT')])
        table_fields = self._make_fields([(0, 'new_name', 'INT'), (1, 'b', 'INT')])
        schemas = {0: data_fields, 1: table_fields}
        evolutions = SimpleStatsEvolutions(lambda sid: schemas[sid], 1)
        evolution = evolutions.get_or_create(0)

        self.assertEqual(evolution.index_mapping, [0, 1])
        self.assertNotEqual(evolution.index_mapping[0], -1)  # not missing

        stats = self._make_stats([(0, 'old_name', 'INT'), (1, 'b', 'INT')],
                                 [10, 20], [100, 200], [0, 5])
        evolved = evolution.evolution(stats, row_count=1000, stats_fields=None)
        self.assertEqual(evolved.min_values.get_field(0), 10)
        self.assertEqual(evolved.min_values.get_field(1), 20)
        self.assertEqual(evolved.max_values.get_field(0), 100)
        self.assertEqual(evolved.max_values.get_field(1), 200)
        self.assertEqual(evolved.null_counts, [0, 5])

    def test_reordered_columns(self):
        """Column order differs between schemas, matched by field ID."""
        data_fields = self._make_fields([(0, 'a', 'INT'), (1, 'b', 'STRING'), (2, 'c', 'BIGINT')])
        table_fields = self._make_fields([(2, 'c', 'BIGINT'), (0, 'a', 'INT'), (1, 'b', 'STRING')])
        schemas = {0: data_fields, 1: table_fields}
        evolutions = SimpleStatsEvolutions(lambda sid: schemas[sid], 1)
        evolution = evolutions.get_or_create(0)
        self.assertEqual(evolution.index_mapping, [2, 0, 1])

    def test_caching(self):
        """Evolution objects are cached by schema ID."""
        fields = self._make_fields([(0, 'a', 'INT')])
        schemas = {0: fields, 1: fields}
        evolutions = SimpleStatsEvolutions(lambda sid: schemas[sid], 1)
        e1 = evolutions.get_or_create(0)
        e2 = evolutions.get_or_create(0)
        self.assertIs(e1, e2)

    def test_complex_evolution(self):
        """Reorder + add + drop + rename combined.

        Data schema (id=0): a(0), b(1), c(2), d(3)
        Table schema (id=1): cc(2), a(0), d(3), e(4, new), bb(1)

        Verifies index_mapping, min/max projection, and null count evolution.
        """
        data_fields = self._make_fields([
            (0, 'a', 'INT'), (1, 'b', 'INT'), (2, 'c', 'INT'), (3, 'd', 'INT')
        ])
        table_fields = self._make_fields([
            (2, 'cc', 'INT'), (0, 'a', 'INT'), (3, 'd', 'INT'), (4, 'e', 'INT'), (1, 'bb', 'INT')
        ])
        schemas = {0: data_fields, 1: table_fields}
        evolutions = SimpleStatsEvolutions(lambda sid: schemas[sid], 1)
        evolution = evolutions.get_or_create(0)

        # Mapping: cc(id=2)->2, a(id=0)->0, d(id=3)->3, e(id=4)->-1, bb(id=1)->1
        self.assertEqual(evolution.index_mapping, [2, 0, 3, -1, 1])

        stats = self._make_stats(
            [(0, 'a', 'INT'), (1, 'b', 'INT'), (2, 'c', 'INT'), (3, 'd', 'INT')],
            [10, 20, 30, 40], [100, 200, 300, 400], [1, 2, 3, 4])
        evolved = evolution.evolution(stats, row_count=1000, stats_fields=None)

        # min: cc->30, a->10, d->40, e->None, bb->20
        self.assertEqual(evolved.min_values.get_field(0), 30)
        self.assertEqual(evolved.min_values.get_field(1), 10)
        self.assertEqual(evolved.min_values.get_field(2), 40)
        self.assertIsNone(evolved.min_values.get_field(3))
        self.assertEqual(evolved.min_values.get_field(4), 20)

        # max: cc->300, a->100, d->400, e->None, bb->200
        self.assertEqual(evolved.max_values.get_field(0), 300)
        self.assertEqual(evolved.max_values.get_field(1), 100)
        self.assertEqual(evolved.max_values.get_field(2), 400)
        self.assertIsNone(evolved.max_values.get_field(3))
        self.assertEqual(evolved.max_values.get_field(4), 200)

        # null counts: cc->3, a->1, d->4, e->1000(row_count), bb->2
        self.assertEqual(evolved.null_counts, [3, 1, 4, 1000, 2])


if __name__ == '__main__':
    unittest.main()
