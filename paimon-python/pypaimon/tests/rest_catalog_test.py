"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import glob
import logging
import os
import shutil
import tempfile
import unittest
import uuid

import pyarrow as pa


from pypaimon.api import ConfigResponse, Identifier
from pypaimon.api.auth import BearTokenAuthProvider
from pypaimon.api.options import Options
from pypaimon.catalog.catalog_context import CatalogContext
from pypaimon.catalog.catalog_factory import CatalogFactory
from pypaimon.catalog.table_metadata import TableMetadata
from pypaimon.catalog.rest.rest_catalog import RESTCatalog
from pypaimon.catalog.table_metadata import TableMetadata
from pypaimon.schema.data_types import (ArrayType, AtomicType, DataField,
                                        MapType)
from pypaimon.schema.table_schema import TableSchema
from pypaimon.tests.rest_server import RESTCatalogServer


class RESTCatalogTestCase(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="unittest_")
        self.tmp_path = os.path.join(self.temp_dir, 'test_dir')

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_rest_catalog(self):
        """Example usage of RESTCatalogServer"""
        # Setup logging
        logging.basicConfig(level=logging.INFO)

        # Create config
        config = ConfigResponse(defaults={"prefix": "mock-test"})
        token = str(uuid.uuid4())
        # Create server
        server = RESTCatalogServer(
            data_path="/tmp/test_warehouse",
            auth_provider=BearTokenAuthProvider(token),
            config=config,
            warehouse="test_warehouse"
        )
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
                DataField(0, "name", AtomicType('INT'), 'desc  name'),
                DataField(1, "arr11", ArrayType(True, AtomicType('INT')), 'desc  arr11'),
                DataField(2, "map11", MapType(False, AtomicType('INT'),
                                              MapType(False, AtomicType('INT'), AtomicType('INT'))),
                          'desc  arr11'),
            ]
            schema = TableSchema(TableSchema.CURRENT_VERSION, len(data_fields), data_fields, len(data_fields),
                                 [], [], {}, "")
            test_tables = {
                "default.user": TableMetadata(uuid=str(uuid.uuid4()), is_external=True, schema=schema),
            }
            server.table_metadata_store.update(test_tables)
            server.database_store.update(test_databases)
            options = {
                'uri': f"http://localhost:{server.port}",
                'warehouse': 'test_warehouse',
                'dlf.region': 'cn-hangzhou',
                "token.provider": "bear",
                'token': token
            }
            rest_catalog = RESTCatalog(CatalogContext.create_from_options(Options(options)))
            self.assertSetEqual(set(rest_catalog.list_databases()), {*test_databases})
            self.assertEqual(rest_catalog.get_database('default').name, test_databases.get('default').name)
            table = rest_catalog.get_table(Identifier.from_string('default.user'))
            self.assertEqual(table.identifier.get_full_name(), 'default.user')
        finally:
            # Shutdown server
            server.shutdown()
            print("Server stopped")

    def test_write(self):
        """Example usage of RESTCatalogServer"""
        # Setup logging
        logging.basicConfig(level=logging.INFO)

        # Create config
        config = ConfigResponse(defaults={"prefix": "mock-test"})
        token = str(uuid.uuid4())
        warehouse = "test_warehouse"
        # Create server
        server = RESTCatalogServer(
            data_path=self.tmp_path,
            auth_provider=BearTokenAuthProvider(token),
            config=config,
            warehouse=warehouse
        )
        try:
            # Start server
            server.start()
            print(f"Server started at: {server.get_url()}")

            pa_schema = pa.schema([
                ('f0', pa.int32()),
                ('f1', pa.string()),
                ('f2', pa.string())
            ])
            fields = [
                DataField.from_dict({"id": 1, "name": "f0", "type": "INT"}),
                DataField.from_dict({"id": 2, "name": "f1", "type": "STRING"}),
                DataField.from_dict({"id": 3, "name": "f2", "type": "STRING"}),
            ]
            options = {
                'metastore': 'rest',
                'uri': f"http://localhost:{server.port}",
                'warehouse': 'test_warehouse',
                'dlf.region': 'cn-hangzhou',
                "token.provider": "bear",
                'token': token,

            }
            catalog = CatalogFactory.create(options)
            catalog.create_database("test_db", False)
            catalog.create_table("test_db.test_table", Schema(fields=fields), False)
            table = catalog.get_table("test_db.test_table")

            data = {
                'f0': [1, 2, 3],
                'f1': ['a', 'b', 'c'],
                'f2': ['X', 'Y', 'Z']
            }
            expect = pa.Table.from_pydict(data, schema=pa_schema)

            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            table_write.write_arrow(expect)
            commit_messages = table_write.prepare_commit()
            table_commit.commit(commit_messages)
            table_write.close()
            table_commit.close()

            self.assertTrue(os.path.exists(self.tmp_path + "/test_warehouse/test_db/test_table/snapshot/LATEST"))
            self.assertTrue(os.path.exists(self.tmp_path + "/test_warehouse/test_db/test_table/snapshot/snapshot-1"))
            self.assertTrue(os.path.exists(self.tmp_path + "/test_warehouse/test_db/test_table/manifest"))
            self.assertTrue(os.path.exists(self.tmp_path + "/test_warehouse/test_db/test_table/bucket-0"))
            self.assertEqual(len(glob.glob(self.tmp_path + "/test_warehouse/test_db/test_table/manifest/*.avro")), 2)
            self.assertEqual(len(glob.glob(self.tmp_path + "/test_warehouse/test_db/test_table/bucket-0/*.parquet")), 1)
        finally:
            # Shutdown server
            server.shutdown()
            print("Server stopped")
