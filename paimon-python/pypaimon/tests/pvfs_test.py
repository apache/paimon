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
from pypaimon.api.data_types import DataField, AtomicType, ArrayType, MapType
from pypaimon.filesystem.pvfs import PaimonVirtualFileSystem
from pypaimon.tests.api_test import AUTHORIZATION_HEADER_KEY, RESTCatalogServer


class PVFSTestCase(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="unittest_")
        self.temp_path = Path(self.temp_dir)
        print(f"create: {self.temp_path}")

    def tearDown(self):
        if self.temp_path.exists():
            shutil.rmtree(self.temp_path)
            print(f"clean: {self.temp_path}")

    @staticmethod
    def _create_parquet_file(path: str):
        data = {
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        }

        df = pandas.DataFrame(data)

        df.to_parquet(path, engine='pyarrow', index=False)
        #
        # df_read = pandas.read_parquet(path)
        # print(f"row: {len(df_read)}")
        # print(f"column: {len(df_read.columns)}")
        # print(df_read.head())

    def test(self):
        # Create config
        config = ConfigResponse(defaults={"prefix": "mock-test"})

        # Create mock auth provider
        class MockAuthProvider:
            def merge_auth_header(self, headers, auth_param):
                return {AUTHORIZATION_HEADER_KEY: "Bearer test-token"}

        # Create server
        data_path = self.temp_dir
        catalog = 'test_warehouse'
        server = RESTCatalogServer(
            data_path=data_path,
            auth_provider=MockAuthProvider(),
            config=config,
            warehouse=catalog)
        try:
            # Start server
            server.start()
            print(f"Server started at: {server.get_url()}")
            test_databases = {
                "default": server.mock_database("default", {"env": "test"}),
                "test_db1": server.mock_database("test_db1", {"env": "test"}),
                "test_db2": server.mock_database("test_db2", {"env": "test"}),
                "prod_db": server.mock_database("prod_db", {"env": "prod"})
            }
            data_fields = [
                DataField(0, "id", AtomicType('INT'), 'id'),
                DataField(1, "name", AtomicType('STRING'), 'name')
            ]
            schema = TableSchema(len(data_fields), data_fields, len(data_fields), [], [], {}, "")
            test_tables = {
                "default.user": TableMetadata(uuid=str(uuid.uuid4()), is_external=True, schema=schema),
            }
            nested_dir = self.temp_path / "default" / "user"
            nested_dir.mkdir(parents=True)
            data_file_name = 'a.parquet'
            self._create_parquet_file(f"{nested_dir}/{data_file_name}")
            server.table_metadata_store.update(test_tables)
            server.database_store.update(test_databases)
            options = {
                'uri': f"http://localhost:{server.port}",
                'warehouse': 'test_warehouse',
                'dlf.region': 'cn-hangzhou',
                "token.provider": "xxxx",
                'dlf.access-key-id': 'xxxx',
                'dlf.access-key-secret': 'xxxx'
            }
            pvfs = PaimonVirtualFileSystem(options)
            database_dirs = pvfs.ls("pvfs://test_warehouse", detail=False)
            expect_database_dirs = set(map(
                lambda x: pvfs._convert_database_virtual_path(catalog, x),
                list(test_databases.keys())
            ))
            self.assertSetEqual(set(database_dirs), expect_database_dirs)
            table_dirs = pvfs.ls("pvfs://test_warehouse/default", detail=False)
            expect_table_dirs = set(map(
                lambda x: pvfs._convert_table_virtual_path(catalog, 'default', x),
                ['user']
            ))
            self.assertSetEqual(set(table_dirs), expect_table_dirs)
            user_dirs = pvfs.ls("pvfs://test_warehouse/default/user", detail=False)
            self.assertSetEqual(set(user_dirs), {f'pvfs://test_warehouse/default/user/{data_file_name}'})
        finally:
            # Shutdown server
            server.shutdown()
            print("Server stopped")
