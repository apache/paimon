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

import shutil
import tempfile
import unittest
import uuid
import pandas
from pathlib import Path

from pypaimon.api import ConfigResponse
from pypaimon.api.api_response import TableSchema, TableMetadata
from pypaimon.api.auth import BearTokenAuthProvider
from pypaimon.api.data_types import DataField, AtomicType
from pypaimon.filesystem.pvfs import PaimonVirtualFileSystem
from pypaimon.tests.api_test import RESTCatalogServer


class PVFSTestCase(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="unittest_")
        self.temp_path = Path(self.temp_dir)
        # Create config
        config = ConfigResponse(defaults={"prefix": "mock-test"})

        # Create server
        self.catalog = 'test_catalog'
        self.data_path = self.temp_dir
        self.catalog = 'test_warehouse'
        self.token = str(uuid.uuid4())
        # Create server
        self.server = RESTCatalogServer(
            data_path=self.data_path,
            auth_provider=BearTokenAuthProvider(self.token),
            config=config,
            warehouse=self.catalog)
        self.server.start()
        print(f"Server started at: {self.server.get_url()}")
        print(f"create: {self.temp_path}")
        options = {
            'uri': f"http://localhost:{self.server.port}",
            'warehouse': 'test_warehouse',
            'dlf.region': 'cn-hangzhou',
            "token.provider": "bear",
            'token': self.token
        }
        self.pvfs = PaimonVirtualFileSystem(options)
        self.database = 'test_database'
        self.table = 'test_table'
        self.test_databases = {
            self.database: self.server.mock_database(self.database, {"k1": "v1", "k2": "v2"}),
        }
        data_fields = [
            DataField(0, "id", AtomicType('INT'), 'id'),
            DataField(1, "name", AtomicType('STRING'), 'name')
        ]
        schema = TableSchema(len(data_fields), data_fields, len(data_fields), [], [], {}, "")
        self.server.database_store.update(self.test_databases)
        self.test_tables = {
            f"{self.database}.{self.table}": TableMetadata(uuid=str(uuid.uuid4()), is_external=True, schema=schema),
        }
        self.server.table_metadata_store.update(self.test_tables)

    def tearDown(self):
        if self.temp_path.exists():
            shutil.rmtree(self.temp_path)
            print(f"clean: {self.temp_path}")
        if self.server is not None:
            self.server.shutdown()
            print("Server stopped")

    @staticmethod
    def _create_parquet_file(path: str):
        data = {
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        }

        df = pandas.DataFrame(data)

        df.to_parquet(path, engine='pyarrow', index=False)

    def test_arrow(self):
        import pyarrow.parquet as pq
        fs = self.pvfs
        database = 'arrow_db'
        table = 'test_table'
        nested_dir = self.temp_path / database / table
        nested_dir.mkdir(parents=True)
        data_file_name = 'a.parquet'
        self._create_parquet_file(f"{nested_dir}/{data_file_name}")
        path = f'pvfs://{self.catalog}/{database}/{table}/{data_file_name}'
        fs.mkdir(f'pvfs://{self.catalog}/{database}/{table}')
        self.assertEqual(fs.exists(path), True)
        dataset = pq.ParquetDataset(path, filesystem=fs)
        table = dataset.read()
        first_row = table.slice(0, 1).to_pydict()
        print(f"first_row: {first_row}")
        df = table.to_pandas()
        self.assertEqual(len(df), 5)

    def test_ray(self):
        import ray
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)
        fs = self.pvfs
        database = 'ray_db'
        table = 'test_table'
        nested_dir = self.temp_path / database / table
        nested_dir.mkdir(parents=True)
        data_file_name = 'a.parquet'
        self._create_parquet_file(f"{nested_dir}/{data_file_name}")
        path = f'pvfs://{self.catalog}/{database}/{table}/{data_file_name}'
        fs.mkdir(f'pvfs://{self.catalog}/{database}/{table}')
        self.assertEqual(fs.exists(path), True)
        ds = ray.data.read_parquet(filesystem=fs, paths=path)
        print(ds.count())
        self.assertEqual(ds.count(), 5)

    def test_api(self):
        nested_dir = self.temp_path / self.database / self.table
        nested_dir.mkdir(parents=True)
        data_file_name = 'a.parquet'
        self._create_parquet_file(f"{nested_dir}/{data_file_name}")
        database_dirs = self.pvfs.ls(f"pvfs://{self.catalog}", detail=False)
        expect_database_dirs = set(map(
            lambda x: self.pvfs._convert_database_virtual_path(self.catalog, x),
            list(self.test_databases.keys())
        ))
        self.assertSetEqual(set(database_dirs), expect_database_dirs)
        table_dirs = self.pvfs.ls(f"pvfs://{self.catalog}/{self.database}", detail=False)
        expect_table_dirs = set(map(
            lambda x: self.pvfs._convert_table_virtual_path(self.catalog, self.database, x),
            [self.table]
        ))
        self.assertSetEqual(set(table_dirs), expect_table_dirs)
        database_virtual_path = f"pvfs://{self.catalog}/{self.database}"
        self.assertEqual(database_virtual_path, self.pvfs.info(database_virtual_path).get('name'))
        self.assertEqual(True, self.pvfs.exists(database_virtual_path))
        table_virtual_path = f"pvfs://{self.catalog}/{self.database}/{self.table}"
        self.assertEqual(table_virtual_path, self.pvfs.info(table_virtual_path).get('name'))
        self.assertEqual(True, self.pvfs.exists(database_virtual_path))
        user_dirs = self.pvfs.ls(f"pvfs://{self.catalog}/{self.database}/{self.table}", detail=False)
        self.assertSetEqual(set(user_dirs), {f'pvfs://{self.catalog}/{self.database}/{self.table}/{data_file_name}'})

        data_file_name = 'data.txt'
        data_file_path = self.temp_path / self.database / self.table / 'data.txt'
        data_file_path.touch()
        content = 'Hello World'
        date_file_virtual_path = f'pvfs://{self.catalog}/{self.database}/{self.table}/{data_file_name}'
        data_file_name = 'data_2.txt'
        date_file_new_virtual_path = f'pvfs://{self.catalog}/{self.database}/{self.table}/{data_file_name}'
        self.pvfs.cp(date_file_virtual_path, date_file_new_virtual_path)
        self.assertEqual(True, self.pvfs.exists(date_file_new_virtual_path))
        with self.pvfs.open(date_file_new_virtual_path, 'w') as w:
            w.write(content)

        with self.pvfs.open(date_file_new_virtual_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
            self.assertListEqual([content], lines)

        database_new_virtual_path = f"pvfs://{self.catalog}/new_db"
        self.assertEqual(False, self.pvfs.exists(database_new_virtual_path))
        self.pvfs.mkdir(database_new_virtual_path)
        self.assertEqual(True, self.pvfs.exists(database_new_virtual_path))

        table_data_new_virtual_path = f"pvfs://{self.catalog}/{self.database}/new_table/data.txt"
        self.assertEqual(False, self.pvfs.exists(table_data_new_virtual_path))
        self.pvfs.mkdir(table_data_new_virtual_path)
        self.assertEqual(True, self.pvfs.exists(table_data_new_virtual_path))
        self.pvfs.makedirs(table_data_new_virtual_path)
        self.assertEqual(True, self.pvfs.exists(table_data_new_virtual_path))

        print(self.pvfs.created(table_data_new_virtual_path))
        print(self.pvfs.modified(table_data_new_virtual_path))
        print(self.pvfs.cat_file(date_file_new_virtual_path))
