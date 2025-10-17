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
from pypaimon.read.split import Split


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
