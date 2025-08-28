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
import glob
import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon.catalog.catalog_factory import CatalogFactory
from pypaimon.schema.schema import Schema
from pypaimon.tests.py4j_impl.java_implementation import CatalogPy4j


@unittest.skip("disable whole class")
class AlternativeWriteTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # for py4j env
        this_dir = os.path.abspath(os.path.dirname(__file__))
        project_dir = os.path.dirname(this_dir)
        deps_dir = os.path.join(project_dir, "tests/py4j_impl/test_deps/*")
        print(f"py4j deps_dir: {deps_dir}")
        for file in glob.glob(deps_dir):
            print(f"py4j deps_dir file: {file}")
        # os.environ[constants.PYPAIMON_HADOOP_CLASSPATH] = deps_dir
        # os.environ[constants.PYPAIMON4J_TEST_MODE] = 'true'

        # for default catalog
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.option = {
            'warehouse': cls.warehouse
        }
        cls.py_catalog = CatalogFactory.create(cls.option)
        cls.j_catalog = CatalogPy4j.create(cls.option)

        cls.pa_schema = pa.schema([
            pa.field('user_id', pa.int32(), nullable=False),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            pa.field('dt', pa.string(), nullable=False)
        ])
        cls.expected_data = {
            'user_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            'item_id': [1001, 2001, 3001, 4007, 5007, 6007, 7007, 8007, 9007, 1006, 1106, 1206],
            'behavior': ['l', 'k', 'j', 'i-new', None, 'g-new', 'f-new', 'e-new', 'd-new', 'c-new', 'b-new', 'a-new'],
            'dt': ['p1', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p1'],
        }
        cls.expected_result = pa.Table.from_pydict(cls.expected_data, schema=cls.pa_schema)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def testAlternativeWrite(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema,
                                            partition_keys=['dt'],
                                            primary_keys=['user_id', 'dt'],
                                            options={'bucket': '1'}
                                            )
        self.py_catalog.create_database('default', False)
        self.py_catalog.create_table('default.test_alternative_write', schema, False)

        self.py_table = self.py_catalog.get_table('default.test_alternative_write')
        self.j_table = self.j_catalog.get_table('default.test_alternative_write')

        self._write_data({
            'user_id': self.expected_data.get('user_id')[0:6],
            'item_id': [1001, 2001, 3001, 4001, 5001, 6001],
            'behavior': ['l', 'k', 'j', 'i', None, 'g'],
            'dt': self.expected_data.get('dt')[0:6]
        }, 1)
        data1 = {
            'user_id': self.expected_data.get('user_id')[6:12],
            'item_id': [7001, 8001, 9001, 1001, 1101, 1201],
            'behavior': ['f-new', 'e-new', 'd-new', 'c-new', 'b-new', 'a-new'],
            'dt': self.expected_data.get('dt')[6:12]
        }
        data2 = {
            'user_id': self.expected_data.get('user_id')[3:9],
            'item_id': [4001, 5001, 6001, 7001, 8001, 9001],
            'behavior': ['i-new', None, 'g-new', 'f-new', 'e-new', 'd-new'],
            'dt': self.expected_data.get('dt')[3:9]
        }
        datas = [data1, data2]
        for idx, using_py in enumerate([0, 0, 1, 1, 0, 0]):  # TODO: this can be a random list
            data1['item_id'] = [item + 1 for item in data1['item_id']]
            data2['item_id'] = [item + 1 for item in data2['item_id']]
            data = datas[idx % 2]
            self._write_data(data, using_py)

        j_read_builder = self.j_table.new_read_builder()
        j_table_read = j_read_builder.new_read()
        j_splits = j_read_builder.new_scan().plan().splits()
        j_actual = j_table_read.to_arrow(j_splits).sort_by('user_id')
        self.assertEqual(j_actual, self.expected_result)

        py_read_builder = self.py_table.new_read_builder()
        py_table_read = py_read_builder.new_read()
        py_splits = py_read_builder.new_scan().plan().splits()
        py_actual = py_table_read.to_arrow(py_splits).sort_by('user_id')
        self.assertEqual(py_actual, self.expected_result)

    def _write_data(self, data, using_py_table: int):
        if using_py_table == 1:
            write_builder = self.py_table.new_batch_write_builder()
        else:
            write_builder = self.j_table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        pa_table = pa.Table.from_pydict(data, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()
