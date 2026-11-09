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

import datetime
import os
import shutil
import sys
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read.push_down_utils import vortex_pushdown_loses_rows
from pypaimon.schema.data_types import AtomicType, DataField

UTC = datetime.timezone.utc


class VortexPushdownLosesRowsTest(unittest.TestCase):
    """The Vortex pushdown guard for floating point literals."""

    def setUp(self):
        self.fields = [
            DataField(0, 'id', AtomicType('INT')),
            DataField(1, 'd', AtomicType('DOUBLE')),
            DataField(2, 'f', AtomicType('FLOAT')),
        ]
        self.builder = PredicateBuilder(self.fields)

    def test_zero_and_nan_literals_block_pushdown(self):
        for predicate in [
            self.builder.equal('d', 0.0),
            self.builder.greater_or_equal('d', -0.0),
            self.builder.not_equal('f', float('nan')),
            self.builder.is_in('d', [1.0, 0.0]),
            self.builder.between('f', -1.0, 0.0),
            self.builder.and_predicates([
                self.builder.equal('id', 1), self.builder.less_than('d', float('nan'))]),
        ]:
            self.assertTrue(vortex_pushdown_loses_rows(predicate, self.fields), predicate)

    def test_other_literals_allow_pushdown(self):
        for predicate in [
            None,
            self.builder.equal('d', 1.5),
            self.builder.greater_than('f', -2.0),
            self.builder.equal('id', 0),
            self.builder.is_null('d'),
            self.builder.or_predicates([
                self.builder.equal('d', 2.0), self.builder.equal('id', 0)]),
        ]:
            self.assertFalse(vortex_pushdown_loses_rows(predicate, self.fields), predicate)


@unittest.skipIf(sys.version_info < (3, 11), "vortex-data requires Python >= 3.11")
class VortexPredicateSemanticsTest(unittest.TestCase):
    """Filtered reads of Vortex tables give the same rows as Parquet tables."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.catalog = CatalogFactory.create({'warehouse': os.path.join(cls.tempdir, 'warehouse')})
        cls.catalog.create_database('default', True)

        cls.pa_schema = pa.schema([
            ('id', pa.int32()),
            ('ltz', pa.timestamp('us', tz='UTC')),
            ('ts', pa.timestamp('us')),
            ('d', pa.float64()),
        ])
        cls.data = pa.table({
            'id': pa.array([1, 2, 3, 4], pa.int32()),
            'ltz': pa.array(
                [datetime.datetime(2024, 1, day, tzinfo=UTC) for day in (1, 2, 3, 4)],
                pa.timestamp('us', tz='UTC')),
            'ts': pa.array(
                [datetime.datetime(2024, 1, day) for day in (1, 2, 3, 4)], pa.timestamp('us')),
            'd': pa.array([0.0, -0.0, float('nan'), 1.5], pa.float64()),
        })
        for file_format in ('parquet', 'vortex'):
            schema = Schema.from_pyarrow_schema(cls.pa_schema, options={'file.format': file_format})
            cls.catalog.create_table(f'default.semantics_{file_format}', schema, False)
            table = cls.catalog.get_table(f'default.semantics_{file_format}')
            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_write.write_arrow(cls.data)
            table_commit = write_builder.new_commit()
            table_commit.commit(table_write.prepare_commit())
            table_write.close()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _read_ids(self, file_format, make_predicate):
        table = self.catalog.get_table(f'default.semantics_{file_format}')
        predicate = make_predicate(table.new_read_builder().new_predicate_builder())
        read_builder = table.new_read_builder().with_filter(predicate)
        result = read_builder.new_read().to_arrow(read_builder.new_scan().plan().splits())
        return sorted(result.column('id').to_pylist())

    def _assert_same_rows(self, make_predicate):
        self.assertEqual(
            self._read_ids('parquet', make_predicate), self._read_ids('vortex', make_predicate))

    def test_timestamp_ltz_filter_is_not_lost(self):
        # Vortex cannot convert a filter on a timestamp with a time zone. The scan then
        # reads every row, and the row filter after it must still apply the predicate.
        self._assert_same_rows(
            lambda pb: pb.equal('ltz', datetime.datetime(2024, 1, 2, tzinfo=UTC)))
        self.assertEqual(
            [2], self._read_ids(
                'vortex', lambda pb: pb.equal('ltz', datetime.datetime(2024, 1, 2, tzinfo=UTC))))

    def test_timestamp_filter(self):
        self._assert_same_rows(lambda pb: pb.equal('ts', datetime.datetime(2024, 1, 2)))

    def test_signed_zero_keeps_paimon_equality(self):
        # Vortex tells -0.0 and 0.0 apart, Paimon does not.
        self._assert_same_rows(lambda pb: pb.equal('d', 0.0))
        self._assert_same_rows(lambda pb: pb.greater_or_equal('d', 0.0))
        self._assert_same_rows(lambda pb: pb.less_or_equal('d', -0.0))
        self._assert_same_rows(lambda pb: pb.is_in('d', [0.0, 1.5]))

    def test_nan_rows_are_not_returned_by_comparisons(self):
        # Vortex orders NaN above every number, Paimon never matches it.
        self._assert_same_rows(lambda pb: pb.greater_than('d', 1.0))
        self._assert_same_rows(lambda pb: pb.greater_or_equal('d', 1.5))
        self._assert_same_rows(lambda pb: pb.less_than('d', 1.0))
        self._assert_same_rows(lambda pb: pb.not_equal('d', 1.5))
        self._assert_same_rows(lambda pb: pb.not_equal('d', float('nan')))
        self._assert_same_rows(lambda pb: pb.between('d', -1.0, 2.0))


if __name__ == '__main__':
    unittest.main()
