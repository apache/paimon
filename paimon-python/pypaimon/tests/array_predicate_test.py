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

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.common.predicate import Predicate
from pypaimon.read.push_down_utils import predicate_supports_arrow_filter


class ArrayPredicateValueTest(unittest.TestCase):
    """Row-level semantics of the array testers, cross-checked against Java
    ArrayContains / ArraysOverlap / ArrayContainsAll."""

    def _tester(self, name):
        return Predicate.testers[name]

    def test_array_contains(self):
        t = self._tester('arrayContains')
        self.assertTrue(t.test_by_value(['cat', 'dog'], ['cat']))
        self.assertFalse(t.test_by_value(['cat', 'dog'], ['fox']))
        self.assertFalse(t.test_by_value(None, ['cat']))       # null array
        self.assertFalse(t.test_by_value(['cat'], [None]))     # null element
        self.assertTrue(t.test_by_value(['a', None, 'b'], ['b']))

    def test_arrays_overlap(self):
        t = self._tester('arraysOverlap')
        self.assertTrue(t.test_by_value(['cat', 'dog'], ['fox', 'dog']))
        self.assertFalse(t.test_by_value(['cat', 'dog'], ['fox', 'cow']))
        self.assertFalse(t.test_by_value(None, ['cat']))
        self.assertFalse(t.test_by_value(['cat'], []))         # empty -> no overlap
        self.assertFalse(t.test_by_value(['cat'], [None]))     # only null literal

    def test_array_contains_all(self):
        t = self._tester('arrayContainsAll')
        self.assertTrue(t.test_by_value(['cat', 'dog', 'fox'], ['cat', 'fox']))
        self.assertFalse(t.test_by_value(['cat', 'dog'], ['cat', 'fox']))
        self.assertFalse(t.test_by_value(None, ['cat']))
        self.assertFalse(t.test_by_value(['cat'], ['cat', None]))   # null literal
        self.assertTrue(t.test_by_value(['cat'], []))          # vacuously true

    def test_array_predicates_are_not_arrow_pushable(self):
        # Arrays have no dataset-expression form; they must run row-level so
        # the final filter is exact (not a no-op truthy expression).
        builder_methods = ('arrayContains', 'arraysOverlap', 'arrayContainsAll')
        for method in builder_methods:
            p = Predicate(method=method, index=0, field='labels', literals=['x'])
            self.assertFalse(predicate_supports_arrow_filter(p), method)


class ArrayPredicateE2ETest(unittest.TestCase):
    """End to end: a filter on an ARRAY column is applied (row-level) so
    pypaimon no longer has to read the whole table and filter client-side."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)
        cls.pa_schema = pa.schema([
            ('id', pa.int32(), False),
            ('labels', pa.list_(pa.string())),
        ])
        cls.rows = [
            {'id': 1, 'labels': ['cat', 'dog']},
            {'id': 2, 'labels': ['fox']},
            {'id': 3, 'labels': ['cat', 'fox', 'owl']},
            {'id': 4, 'labels': None},
        ]
        schema = Schema.from_pyarrow_schema(cls.pa_schema)
        cls.catalog.create_table('default.arr', schema, False)
        table = cls.catalog.get_table('default.arr')
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        c = wb.new_commit()
        try:
            w.write_arrow(pa.Table.from_pylist(cls.rows, schema=cls.pa_schema))
            c.commit(w.prepare_commit())
        finally:
            w.close()
            c.close()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _ids(self, predicate):
        table = self.catalog.get_table('default.arr')
        rb = table.new_read_builder().with_filter(predicate)
        result = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        return sorted(result.column('id').to_pylist())

    def _pb(self):
        return self.catalog.get_table('default.arr').new_read_builder() \
            .new_predicate_builder()

    def test_array_contains_filters_rows(self):
        self.assertEqual([1, 3], self._ids(self._pb().array_contains('labels', 'cat')))

    def test_arrays_overlap_filters_rows(self):
        self.assertEqual(
            [2, 3], self._ids(self._pb().arrays_overlap('labels', ['fox', 'zzz'])))

    def test_array_contains_all_filters_rows(self):
        self.assertEqual(
            [3], self._ids(self._pb().array_contains_all('labels', ['cat', 'fox'])))

    def test_non_array_field_is_rejected(self):
        # Mirror Java ArrayContains.elementType, which requires an ARRAY
        # column: an array predicate on the INT 'id' field must raise rather
        # than silently run `literal in value` and select wrong rows.
        pb = self._pb()
        with self.assertRaises(ValueError):
            pb.array_contains('id', 1)
        with self.assertRaises(ValueError):
            pb.arrays_overlap('id', [1])
        with self.assertRaises(ValueError):
            pb.array_contains_all('id', [1])


if __name__ == '__main__':
    unittest.main()
