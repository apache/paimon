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
import unittest

import pandas as pd
import pyarrow as pa
from pypaimon.catalog.catalog_factory import CatalogFactory
from pypaimon.schema.schema import Schema


class JavaPyReadWriteTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = os.path.abspath(".")
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        print("test", cls.warehouse)
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', True)

    def test_py_write_read(self):
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('category', pa.string()),
            ('value', pa.float64())
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            partition_keys=['category'],
            options={'dynamic-partition-overwrite': 'false'}
        )

        self.catalog.create_table('default.mixed_test_tablep', schema, False)
        table = self.catalog.get_table('default.mixed_test_tablep')

        initial_data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5, 6],
            'name': ['Apple', 'Banana', 'Carrot', 'Broccoli', 'Chicken', 'Beef'],
            'category': ['Fruit', 'Fruit', 'Vegetable', 'Vegetable', 'Meat', 'Meat'],
            'value': [1.5, 0.8, 0.6, 1.2, 5.0, 8.0]
        })
        # Write initial data
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        table_write.write_pandas(initial_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Verify initial data
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        initial_result = table_read.to_pandas(table_scan.plan().splits())
        print(initial_result)
        self.assertEqual(len(initial_result), 6)
        self.assertListEqual(
            initial_result['name'].tolist(),
            ['Apple', 'Banana', 'Carrot', 'Broccoli', 'Chicken', 'Beef']
        )

    def test_read(self):
        table = self.catalog.get_table('default.mixed_test_tablej')
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        res = table_read.to_pandas(table_scan.plan().splits())
        print(res)
