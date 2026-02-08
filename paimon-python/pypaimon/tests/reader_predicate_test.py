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

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory
from pypaimon import Schema
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read import push_down_utils
from pypaimon.read.split import Split
from pypaimon.schema.data_types import AtomicType, DataField


class ReaderPredicateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', False)

        cls.pa_schema = pa.schema([
            ('a', pa.int64()),
            ('pt', pa.int64())
        ])
        schema = Schema.from_pyarrow_schema(cls.pa_schema, partition_keys=['pt'])
        cls.catalog.create_table('default.test_reader_predicate', schema, False)
        cls.table = cls.catalog.get_table('default.test_reader_predicate')

        data1 = pa.Table.from_pydict({
            'a': [1, 2],
            'pt': [1001, 1002]}, schema=cls.pa_schema)
        write_builder = cls.table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(data1)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        data2 = pa.Table.from_pydict({
            'a': [3, 4],
            'pt': [1003, 1004]}, schema=cls.pa_schema)
        write_builder = cls.table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(data2)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_partition_predicate(self):
        predicate_builder = self.table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.equal('pt', 1003)
        read_builder = self.table.new_read_builder()
        read_builder.with_filter(predicate)
        splits: list[Split] = read_builder.new_scan().plan().splits()
        self.assertEqual(len(splits), 1)
        self.assertEqual(splits[0].partition.to_dict().get("pt"), 1003)

    def test_trim_predicate(self):
        predicate_builder = self.table.new_read_builder().new_predicate_builder()
        p1 = predicate_builder.between('pt', 1002, 1003)
        p2 = predicate_builder.and_predicates([predicate_builder.equal('pt', 1003), predicate_builder.equal('a', 3)])
        predicate = predicate_builder.and_predicates([p1, p2])
        pred = push_down_utils.trim_predicate_by_fields(predicate, self.table.partition_keys)
        self.assertEqual(len(pred.literals), 2)
        self.assertEqual(pred.literals[0].field, 'pt')
        self.assertEqual(pred.literals[1].field, 'pt')

    def test_remove_row_id_filter(self):
        fields = [
            DataField(0, '_ROW_ID', AtomicType('BIGINT')),
            DataField(1, 'f0', AtomicType('INT')),
        ]
        pb = PredicateBuilder(fields)
        and_pred = pb.and_predicates([pb.equal('_ROW_ID', 1), pb.greater_than('f0', 5)])
        result = push_down_utils.remove_row_id_filter(and_pred)
        self.assertIsNotNone(result)
        self.assertEqual(result.field, 'f0')
        self.assertEqual(result.method, 'greaterThan')
        or_mixed = pb.or_predicates([pb.equal('_ROW_ID', 1), pb.greater_than('f0', 5)])
        result = push_down_utils.remove_row_id_filter(or_mixed)
        self.assertIsNone(result)
        or_no_row_id = pb.or_predicates([pb.greater_than('f0', 5), pb.less_than('f0', 10)])
        result = push_down_utils.remove_row_id_filter(or_no_row_id)
        self.assertIsNotNone(result)
        self.assertEqual(result.method, 'or')
        self.assertEqual(len(result.literals), 2)
