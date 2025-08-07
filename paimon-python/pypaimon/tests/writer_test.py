# Licensed to the Apache Software Foundation (ASF) under one
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

import glob
import os
import shutil
import tempfile
import unittest

import pyarrow

from pypaimon.catalog.catalog_factory import CatalogFactory
from pypaimon.schema.schema import Schema


class WriterTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp(prefix="unittest_")
        cls.warehouse = os.path.join(cls.temp_dir, 'test_dir')

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.temp_dir, ignore_errors=True)

    def test_writer(self):
        pa_schema = pyarrow.schema([
            ('f0', pyarrow.int32()),
            ('f1', pyarrow.string()),
            ('f2', pyarrow.string())
        ])
        catalog = CatalogFactory.create({
            "warehouse": self.warehouse
        })
        catalog.create_database("test_db", False)
        catalog.create_table("test_db.test_table", Schema.from_pyarrow_schema(pa_schema), False)
        table = catalog.get_table("test_db.test_table")

        data = {
            'f0': [1, 2, 3],
            'f1': ['a', 'b', 'c'],
            'f2': ['X', 'Y', 'Z']
        }
        expect = pyarrow.Table.from_pydict(data, schema=pa_schema)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(expect)
        commit_messages = table_write.prepare_commit()
        table_commit.commit(commit_messages)
        table_write.close()
        table_commit.close()

        self.assertTrue(os.path.exists(self.warehouse + "/test_db.db/test_table/snapshot/LATEST"))
        self.assertTrue(os.path.exists(self.warehouse + "/test_db.db/test_table/snapshot/snapshot-1"))
        self.assertTrue(os.path.exists(self.warehouse + "/test_db.db/test_table/manifest"))
        self.assertTrue(os.path.exists(self.warehouse + "/test_db.db/test_table/bucket-0"))
        self.assertEqual(len(glob.glob(self.warehouse + "/test_db.db/test_table/manifest/*.avro")), 2)
        self.assertEqual(len(glob.glob(self.warehouse + "/test_db.db/test_table/bucket-0/*.parquet")), 1)
