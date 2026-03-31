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

from pypaimon import CatalogFactory, Schema
from pypaimon.api.api_response import ConfigResponse, Partition
from pypaimon.api.auth import BearTokenAuthProvider
from pypaimon.common.options import Options
from pypaimon.catalog.catalog_context import CatalogContext
from pypaimon.catalog.rest.rest_catalog import RESTCatalog
from pypaimon.catalog.rest.table_metadata import TableMetadata
from pypaimon.common.identifier import Identifier
from pypaimon.schema.data_types import (ArrayType, AtomicType, DataField,
                                        MapType)
from pypaimon.schema.table_schema import TableSchema
from pypaimon.tests.rest.rest_server import RESTCatalogServer


class RESTBaseTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="unittest_")
        self.warehouse = os.path.join(self.temp_dir, 'warehouse')

        self.config = ConfigResponse(defaults={"prefix": "mock-test"})
        self.token = str(uuid.uuid4())
        self.server = RESTCatalogServer(
            data_path=self.temp_dir,
            auth_provider=BearTokenAuthProvider(self.token),
            config=self.config,
            warehouse="warehouse"
        )
        self.server.start()
        print(f"\nServer started at: {self.server.get_url()}")

        self.options = {
            'metastore': 'rest',
            'uri': f"http://localhost:{self.server.port}",
            'warehouse': "warehouse",
            'dlf.region': 'cn-hangzhou',
            "token.provider": "bear",
            'token': self.token,
        }
        self.rest_catalog = CatalogFactory.create(self.options)
        self.rest_catalog.create_database("default", False)

        self.pa_schema = pa.schema([
            ('user_id', pa.int64()),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            ('dt', pa.string()),
            ('long-dt', pa.string())
        ])
        self.pk_pa_schema = pa.schema([
            pa.field('user_id', pa.int64(), nullable=False),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            pa.field('dt', pa.string(), nullable=False),
            ('long-dt', pa.string())
        ])
        self.raw_data = {
            'user_id': [1, 2, 3, 4, 5, 6, 7, 8],
            'item_id': [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008],
            'behavior': ['a', 'b', 'c', None, 'e', 'f', 'g', 'h'],
            'dt': ['p1', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p2'],
            'long-dt': ['2024-10-10', '2024-10-10', '2024-10-10', '2024-01-01', '2024-10-10', '2025-01-23',
                        'abcdefghijklmnopk', '2025-08-08']
        }
        self.expected = pa.Table.from_pydict(self.raw_data, schema=self.pa_schema)
        self.pk_expected = pa.Table.from_pydict(self.raw_data, schema=self.pk_pa_schema)

        schema = Schema.from_pyarrow_schema(self.pa_schema)
        self.rest_catalog.create_table('default.test_reader_iterator', schema, False)
        self.table = self.rest_catalog.get_table('default.test_reader_iterator')
        write_builder = self.table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(self.expected)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

    def tearDown(self):
        # Shutdown server
        self.server.shutdown()
        print("Server stopped")
        import gc
        gc.collect()
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
        # Setup logging
        logging.basicConfig(level=logging.INFO)

        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.drop_table("default.test_table", True)
        self.rest_catalog.create_table("default.test_table", schema, False)
        table = self.rest_catalog.get_table("default.test_table")

        data = {
            'user_id': [1, 2, 3, 4],
            'item_id': [1001, 1002, 1003, 1004],
            'behavior': ['a', 'b', 'c', None],
            'dt': ['p1', 'p1', 'p2', 'p1'],
            'long-dt': ['2024-10-10', '2024-10-10', '2024-10-10', '2024-01-01']
        }
        expect = pa.Table.from_pydict(data, schema=self.pa_schema)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(expect)
        commit_messages = table_write.prepare_commit()
        table_commit.commit(commit_messages)
        table_write.close()
        table_commit.close()

        self.assertTrue(os.path.exists(self.warehouse + "/default/test_table/snapshot/LATEST"))
        self.assertTrue(os.path.exists(self.warehouse + "/default/test_table/snapshot/snapshot-1"))
        self.assertTrue(os.path.exists(self.warehouse + "/default/test_table/manifest"))
        self.assertTrue(os.path.exists(self.warehouse + "/default/test_table/dt=p1"))
        self.assertEqual(len(glob.glob(self.warehouse + "/default/test_table/manifest/*")), 3)

    def _write_test_table(self, table):
        write_builder = table.new_batch_write_builder()
        table_pa_schema = self.pk_pa_schema if table.primary_keys else self.pa_schema

        # first write
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data1 = {
            'user_id': [1, 2, 3, 4],
            'item_id': [1001, 1002, 1003, 1004],
            'behavior': ['a', 'b', 'c', None],
            'dt': ['p1', 'p1', 'p2', 'p1'],
            'long-dt': ['2024-10-10', '2024-10-10', '2024-10-10', '2024-01-01'],
        }
        pa_table = pa.Table.from_pydict(data1, schema=table_pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # second write
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data2 = {
            'user_id': [5, 6, 7, 8],
            'item_id': [1005, 1006, 1007, 1008],
            'behavior': ['e', 'f', 'g', 'h'],
            'dt': ['p2', 'p1', 'p2', 'p2'],
            'long-dt': ['2024-10-10', '2025-01-23', 'abcdefghijklmnopk', '2025-08-08'],
        }
        pa_table = pa.Table.from_pydict(data2, schema=table_pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

    def _read_test_table(self, read_builder):
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        return table_read.to_arrow(splits)

    def _write_test_table_with_schema(self, table, pa_schema):
        """Write test data using the specified PyArrow schema."""
        write_builder = table.new_batch_write_builder()

        # first write
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data1 = {
            'user_id': [1, 2, 3, 4],
            'item_id': [1001, 1002, 1003, 1004],
            'behavior': ['a', 'b', 'c', None],
            'dt': ['p1', 'p1', 'p2', 'p1'],
            'long-dt': ['2024-10-10', '2024-10-10', '2024-10-10', '2024-01-01'],
        }
        pa_table = pa.Table.from_pydict(data1, schema=pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # second write
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data2 = {
            'user_id': [5, 6, 7, 8],
            'item_id': [1005, 1006, 1007, 1008],
            'behavior': ['e', 'f', 'g', 'h'],
            'dt': ['p2', 'p1', 'p2', 'p2'],
            'long-dt': ['2024-10-10', '2025-01-23', 'abcdefghijklmnopk', '2025-08-08'],
        }
        pa_table = pa.Table.from_pydict(data2, schema=pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

    def test_list_partitions_paged(self):
        """Test list_partitions_paged returns partitions from server store."""
        identifier = Identifier.from_string('default.test_reader_iterator')

        # Add test partitions to server store
        p1 = Partition(
            spec={"dt": "p1"},
            record_count=4,
            file_size_in_bytes=1024,
            file_count=1,
            last_file_creation_time=1000,
            total_buckets=1,
            done=False,
        )
        p2 = Partition(
            spec={"dt": "p2"},
            record_count=4,
            file_size_in_bytes=2048,
            file_count=1,
            last_file_creation_time=2000,
            total_buckets=1,
            done=True,
        )
        p3 = Partition(
            spec={"dt": "p3"},
            record_count=2,
            file_size_in_bytes=512,
            file_count=1,
            last_file_creation_time=3000,
            total_buckets=1,
            done=False,
        )
        self.server.table_partitions_store[identifier.get_full_name()] = [p1, p2, p3]

        # Test: list all partitions
        result = self.rest_catalog.list_partitions_paged(identifier)
        self.assertEqual(len(result.elements), 3)

        # Test: list with max_results
        result = self.rest_catalog.list_partitions_paged(identifier, max_results=2)
        self.assertEqual(len(result.elements), 2)
        self.assertIsNotNone(result.next_page_token)

        # Test: list next page using page_token
        result2 = self.rest_catalog.list_partitions_paged(
            identifier, max_results=2, page_token=result.next_page_token
        )
        self.assertEqual(len(result2.elements), 1)
        self.assertIsNone(result2.next_page_token)

        # Test: list with pattern filter
        result = self.rest_catalog.list_partitions_paged(
            identifier, partition_name_pattern="dt=p1"
        )
        self.assertEqual(len(result.elements), 1)
        self.assertEqual(result.elements[0].spec, {"dt": "p1"})

        # Test: list with pattern using wildcard
        result = self.rest_catalog.list_partitions_paged(
            identifier, partition_name_pattern="dt=p%"
        )
        self.assertEqual(len(result.elements), 3)

    def test_alter_database(self):
        """Test alter_database sets and removes properties."""
        from pypaimon.catalog.rest.property_change import PropertyChange
        db_name = "alter_db_test"
        self.rest_catalog.create_database(db_name, True)

        # set property
        self.rest_catalog.alter_database(
            db_name,
            [PropertyChange.set_property("key1", "value1"),
             PropertyChange.set_property("key2", "value2")])
        db = self.rest_catalog.get_database(db_name)
        self.assertEqual(db.options.get("key1"), "value1")
        self.assertEqual(db.options.get("key2"), "value2")

        # remove property
        self.rest_catalog.alter_database(
            db_name,
            [PropertyChange.remove_property("key1")])
        db = self.rest_catalog.get_database(db_name)
        self.assertNotIn("key1", db.options)
        self.assertEqual(db.options.get("key2"), "value2")

    def test_list_partitions_paged_empty(self):
        """Test list_partitions_paged returns empty when no partitions."""
        identifier = Identifier.from_string('default.test_reader_iterator')
        result = self.rest_catalog.list_partitions_paged(identifier)
        self.assertEqual(len(result.elements), 0)
        self.assertIsNone(result.next_page_token)
