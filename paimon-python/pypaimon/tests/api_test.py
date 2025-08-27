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

import logging
import unittest
import uuid

from pypaimon.api.api_response import ConfigResponse
from pypaimon.api.auth import BearTokenAuthProvider
from pypaimon.api.rest_api import RESTApi
from pypaimon.api.token_loader import DLFToken, DLFTokenLoaderFactory
from pypaimon.catalog.rest.table_metadata import TableMetadata
from pypaimon.common.config import CatalogOptions
from pypaimon.common.identifier import Identifier
from pypaimon.common.json_util import JSON
from pypaimon.schema.data_types import (ArrayType, AtomicInteger, AtomicType,
                                        DataField, DataTypeParser, MapType,
                                        RowType)
from pypaimon.schema.table_schema import TableSchema
from pypaimon.tests.rest_server import RESTCatalogServer


class ApiTestCase(unittest.TestCase):

    def test_parse_data(self):
        simple_type_test_cases = [
            "DECIMAL",
            "DECIMAL(5)",
            "DECIMAL(10, 2)",
            "DECIMAL(38, 18)",
            "VARBINARY",
            "VARBINARY(100)",
            "VARBINARY(1024)",
            "BYTES",
            "VARCHAR(255)",
            "CHAR(10)",
            "INT",
            "BOOLEAN"
        ]
        for type_str in simple_type_test_cases:
            data_type = DataTypeParser.parse_data_type(type_str)
            self.assertEqual(data_type.nullable, True)
            self.assertEqual(data_type.type, type_str)
        field_id = AtomicInteger(0)
        simple_type = DataTypeParser.parse_data_type("VARCHAR(32)")
        self.assertEqual(simple_type.nullable, True)
        self.assertEqual(simple_type.type, 'VARCHAR(32)')

        array_json = {
            "type": "ARRAY",
            "element": "INT"
        }
        array_type = DataTypeParser.parse_data_type(array_json, field_id)
        self.assertEqual(array_type.element.type, 'INT')

        map_json = {
            "type": "MAP",
            "key": "STRING",
            "value": "INT"
        }
        map_type = DataTypeParser.parse_data_type(map_json, field_id)
        self.assertEqual(map_type.key.type, 'STRING')
        self.assertEqual(map_type.value.type, 'INT')
        row_json = {
            "type": "ROW",
            "fields": [
                {
                    "name": "id",
                    "type": "BIGINT",
                    "description": "Primary key"
                },
                {
                    "name": "name",
                    "type": "VARCHAR(100)",
                    "description": "User name"
                },
                {
                    "name": "scores",
                    "type": {
                        "type": "ARRAY",
                        "element": "DOUBLE"
                    }
                }
            ]
        }

        row_type: RowType = DataTypeParser.parse_data_type(row_json, AtomicInteger(0))
        self.assertEqual(row_type.fields[0].type.type, 'BIGINT')
        self.assertEqual(row_type.fields[1].type.type, 'VARCHAR(100)')

        complex_json = {
            "type": "ARRAY",
            "element": {
                "type": "MAP",
                "key": "STRING",
                "value": {
                    "type": "ROW",
                    "fields": [
                        {"name": "count", "type": "BIGINT"},
                        {"name": "percentage", "type": "DOUBLE"}
                    ]
                }
            }
        }

        complex_type: ArrayType = DataTypeParser.parse_data_type(complex_json, field_id)
        element_type: MapType = complex_type.element
        value_type: RowType = element_type.value
        self.assertEqual(value_type.fields[0].type.type, 'BIGINT')
        self.assertEqual(value_type.fields[1].type.type, 'DOUBLE')

    def test_api(self):
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
            rest_api = RESTApi(options)
            self.assertSetEqual(set(rest_api.list_databases()), {*test_databases})
            self.assertEqual(rest_api.get_database('default'), test_databases.get('default'))
            table = rest_api.get_table(Identifier.from_string('default.user'))
            self.assertEqual(table.id, str(test_tables['default.user'].uuid))

        finally:
            # Shutdown server
            server.shutdown()
            print("Server stopped")

    def test_ecs_loader_token(self):
        token = DLFToken(
            access_key_id='AccessKeyId',
            access_key_secret='AccessKeySecret',
            security_token='AQoDYXdzEJr...<remainder of security token>',
            expiration="2023-12-01T12:00:00Z"
        )
        token_json = JSON.to_json(token)
        role_name = 'test_role'
        config = ConfigResponse(defaults={"prefix": "mock-test"})
        server = RESTCatalogServer(
            data_path="/tmp/test_warehouse",
            auth_provider=None,
            config=config,
            warehouse="test_warehouse",
            role_name=role_name,
            token_json=token_json
        )
        try:
            # Start server
            server.start()
            ecs_metadata_url = f"http://localhost:{server.port}/ram/security-credential/"
            options = {
                CatalogOptions.DLF_TOKEN_LOADER: 'ecs',
                CatalogOptions.DLF_TOKEN_ECS_METADATA_URL: ecs_metadata_url
            }
            loader = DLFTokenLoaderFactory.create_token_loader(options)
            load_token = loader.load_token()
            self.assertEqual(load_token.access_key_id, token.access_key_id)
            self.assertEqual(load_token.access_key_secret, token.access_key_secret)
            self.assertEqual(load_token.security_token, token.security_token)
            self.assertEqual(load_token.expiration, token.expiration)
            options_with_role = {
                CatalogOptions.DLF_TOKEN_LOADER: 'ecs',
                CatalogOptions.DLF_TOKEN_ECS_METADATA_URL: ecs_metadata_url,
                CatalogOptions.DLF_TOKEN_ECS_ROLE_NAME: role_name,
            }
            loader = DLFTokenLoaderFactory.create_token_loader(options_with_role)
            token = loader.load_token()
            self.assertEqual(load_token.access_key_id, token.access_key_id)
            self.assertEqual(load_token.access_key_secret, token.access_key_secret)
            self.assertEqual(load_token.security_token, token.security_token)
            self.assertEqual(load_token.expiration, token.expiration)
        finally:
            # Shutdown server
            server.shutdown()
            print("Server stopped")
