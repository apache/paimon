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

from pypaimon.catalog.catalog_factory import CatalogFactory
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.schema.schema import Schema


class PredicatePushDownTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', False)

        cls.pa_schema = pa.schema([
            pa.field('key1', pa.int32(), nullable=False),
            pa.field('key2', pa.string(), nullable=False),
            ('behavior', pa.string()),
            pa.field('dt1', pa.string(), nullable=False),
            pa.field('dt2', pa.int32(), nullable=False)
        ])
        cls.expected = pa.Table.from_pydict({
            'key1': [1, 2, 3, 4, 5, 7, 8],
            'key2': ['h', 'g', 'f', 'e', 'd', 'b', 'a'],
            'behavior': ['a', 'b-new', 'c', None, 'e', 'g', 'h'],
            'dt1': ['p1', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2'],
            'dt2': [2, 2, 1, 2, 2, 1, 2],
        }, schema=cls.pa_schema)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def testPkReaderWithFilter(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema,
                                            partition_keys=['dt1', 'dt2'],
                                            primary_keys=['key1', 'key2'],
                                            options={'bucket': '1'})
        self.catalog.create_table('default.test_pk_filter', schema, False)
        table = self.catalog.get_table('default.test_pk_filter')

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data1 = {
            'key1': [1, 2, 3, 4],
            'key2': ['h', 'g', 'f', 'e'],
            'behavior': ['a', 'b', 'c', None],
            'dt1': ['p1', 'p1', 'p2', 'p1'],
            'dt2': [2, 2, 1, 2],
        }
        pa_table = pa.Table.from_pydict(data1, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data1 = {
            'key1': [5, 2, 7, 8],
            'key2': ['d', 'g', 'b', 'a'],
            'behavior': ['e', 'b-new', 'g', 'h'],
            'dt1': ['p2', 'p1', 'p1', 'p2'],
            'dt2': [2, 2, 1, 2]
        }
        pa_table = pa.Table.from_pydict(data1, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # test filter by partition
        predicate_builder: PredicateBuilder = table.new_read_builder().new_predicate_builder()
        p1 = predicate_builder.startswith('dt1', "p1")
        p2 = predicate_builder.is_in('dt1', ["p2"])
        p3 = predicate_builder.or_predicates([p1, p2])
        p4 = predicate_builder.equal('dt2', 2)
        g1 = predicate_builder.and_predicates([p3, p4])
        # (dt1 startswith 'p1' or dt1 is_in ["p2"]) and dt2 == 2
        read_builder = table.new_read_builder().with_filter(g1)
        splits = read_builder.new_scan().plan().splits()
        self.assertEqual(len(splits), 2)
        self.assertEqual(splits[0].partition.to_dict()["dt2"], 2)
        self.assertEqual(splits[1].partition.to_dict()["dt2"], 2)

        # test filter by stats
        predicate_builder: PredicateBuilder = table.new_read_builder().new_predicate_builder()
        p1 = predicate_builder.equal('key1', 7)
        p2 = predicate_builder.is_in('key2', ["e", "f"])
        p3 = predicate_builder.or_predicates([p1, p2])
        p4 = predicate_builder.greater_than('key1', 3)
        g1 = predicate_builder.and_predicates([p3, p4])
        # (key1 == 7 or key2 is_in ["e", "f"]) and key1 > 3
        read_builder = table.new_read_builder().with_filter(g1)
        splits = read_builder.new_scan().plan().splits()
        # initial splits meta:
        # p1, 2 -> 2g, 2g; 1e, 4h
        # p2, 1 -> 3f, 3f
        # p2, 2 -> 5a, 8d
        # p1, 1 -> 7b, 7b
        self.assertEqual(len(splits), 3)
        # expect to filter out `p1, 2 -> 2g, 2g` and `p2, 1 -> 3f, 3f`
        count = 0
        for split in splits:
            if split.partition.values == ["p1", 2]:
                count += 1
                self.assertEqual(len(split.files), 1)
                min_values = split.files[0].value_stats.min_values.to_dict()
                max_values = split.files[0].value_stats.max_values.to_dict()
                self.assertTrue(min_values["key1"] == 1 and min_values["key2"] == "e"
                                and max_values["key1"] == 4 and max_values["key2"] == "h")
            elif split.partition.values == ["p2", 2]:
                count += 1
                min_values = split.files[0].value_stats.min_values.to_dict()
                max_values = split.files[0].value_stats.max_values.to_dict()
                self.assertTrue(min_values["key1"] == 5 and min_values["key2"] == "a"
                                and max_values["key1"] == 8 and max_values["key2"] == "d")
            elif split.partition.values == ["p1", 1]:
                count += 1
                min_values = split.files[0].value_stats.min_values.to_dict()
                max_values = split.files[0].value_stats.max_values.to_dict()
                self.assertTrue(min_values["key1"] == max_values["key1"] == 7
                                and max_values["key2"] == max_values["key2"] == "b")
        self.assertEqual(count, 3)
