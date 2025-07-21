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
import re
import uuid
from typing import Dict, List, Optional, Any, Union, Tuple
from dataclasses import dataclass
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse
import unittest

import pypaimon.api as api
from ..api.api_response import (ConfigResponse, ListDatabasesResponse, GetDatabaseResponse,
                                TableMetadata, Schema, GetTableResponse, ListTablesResponse,
                                TableSchema, RESTResponse, PagedList, DataField)
from ..api import RESTApi
from ..api.rest_json import JSON
from ..api.token_loader import DLFTokenLoaderFactory, DLFToken
from ..api.typedef import Identifier
from ..api.data_types import AtomicInteger, DataTypeParser, AtomicType, ArrayType, MapType, RowType


@dataclass
class ErrorResponse(RESTResponse):
    """Error response"""
    RESOURCE_TYPE_DATABASE = "database"
    RESOURCE_TYPE_TABLE = "table"
    RESOURCE_TYPE_VIEW = "view"
    RESOURCE_TYPE_FUNCTION = "function"
    RESOURCE_TYPE_COLUMN = "column"
    RESOURCE_TYPE_SNAPSHOT = "snapshot"
    RESOURCE_TYPE_TAG = "tag"
    RESOURCE_TYPE_BRANCH = "branch"
    RESOURCE_TYPE_DEFINITION = "definition"
    RESOURCE_TYPE_DIALECT = "dialect"

    resource_type: Optional[str]
    resource_name: Optional[str]
    message: str
    code: int


class ResourcePaths:
    """Resource path constants"""

    TABLES = "tables"
    VIEWS = "views"
    FUNCTIONS = "functions"
    SNAPSHOTS = "snapshots"
    ROLLBACK = "rollback"

    def __init__(self, prefix: str = ""):
        self.prefix = prefix.rstrip('/')

    def config(self) -> str:
        return "/v1/config"

    def databases(self) -> str:
        return f"/v1/{self.prefix}/databases"

    def tables(self) -> str:
        return f"{self.prefix}/tables"


# Exception classes
class CatalogException(Exception):
    """Base catalog exception"""


class DatabaseNotExistException(CatalogException):
    """Database not exist exception"""

    def __init__(self, database: str):
        self.database = database
        super().__init__(f"Database {database} does not exist")


class DatabaseAlreadyExistException(CatalogException):
    """Database already exist exception"""

    def __init__(self, database: str):
        self.database = database
        super().__init__(f"Database {database} already exists")


class DatabaseNoPermissionException(CatalogException):
    """Database no permission exception"""

    def __init__(self, database: str):
        self.database = database
        super().__init__(f"No permission to access database {database}")


class TableNotExistException(CatalogException):
    """Table not exist exception"""

    def __init__(self, identifier: Identifier):
        self.identifier = identifier
        super().__init__(f"Table {identifier.get_full_name()} does not exist")


class TableAlreadyExistException(CatalogException):
    """Table already exist exception"""

    def __init__(self, identifier: Identifier):
        self.identifier = identifier
        super().__init__(f"Table {identifier.get_full_name()} already exists")


class TableNoPermissionException(CatalogException):
    """Table no permission exception"""

    def __init__(self, identifier: Identifier):
        self.identifier = identifier
        super().__init__(f"No permission to access table {identifier.get_full_name()}")


class ViewNotExistException(CatalogException):
    """View not exist exception"""

    def __init__(self, identifier: Identifier):
        self.identifier = identifier
        super().__init__(f"View {identifier.get_full_name()} does not exist")


class ViewAlreadyExistException(CatalogException):
    """View already exist exception"""

    def __init__(self, identifier: Identifier):
        self.identifier = identifier
        super().__init__(f"View {identifier.get_full_name()} already exists")


class FunctionNotExistException(CatalogException):
    """Function not exist exception"""

    def __init__(self, identifier: Identifier):
        self.identifier = identifier
        super().__init__(f"Function {identifier.get_full_name()} does not exist")


class FunctionAlreadyExistException(CatalogException):
    """Function already exist exception"""

    def __init__(self, identifier: Identifier):
        self.identifier = identifier
        super().__init__(f"Function {identifier.get_full_name()} already exists")


class ColumnNotExistException(CatalogException):
    """Column not exist exception"""

    def __init__(self, column: str):
        self.column = column
        super().__init__(f"Column {column} does not exist")


class ColumnAlreadyExistException(CatalogException):
    """Column already exist exception"""

    def __init__(self, column: str):
        self.column = column
        super().__init__(f"Column {column} already exists")


class DefinitionNotExistException(CatalogException):
    """Definition not exist exception"""

    def __init__(self, identifier: Identifier, name: str):
        self.identifier = identifier
        self.name = name
        super().__init__(f"Definition {name} does not exist in {identifier.get_full_name()}")


class DefinitionAlreadyExistException(CatalogException):
    """Definition already exist exception"""

    def __init__(self, identifier: Identifier, name: str):
        self.identifier = identifier
        self.name = name
        super().__init__(f"Definition {name} already exists in {identifier.get_full_name()}")


class DialectNotExistException(CatalogException):
    """Dialect not exist exception"""

    def __init__(self, identifier: Identifier, dialect: str):
        self.identifier = identifier
        self.dialect = dialect
        super().__init__(f"Dialect {dialect} does not exist in {identifier.get_full_name()}")


class DialectAlreadyExistException(CatalogException):
    """Dialect already exist exception"""

    def __init__(self, identifier: Identifier, dialect: str):
        self.identifier = identifier
        self.dialect = dialect
        super().__init__(f"Dialect {dialect} already exists in {identifier.get_full_name()}")


# Constants
DEFAULT_MAX_RESULTS = 100
AUTHORIZATION_HEADER_KEY = "Authorization"

# REST API parameter constants
DATABASE_NAME_PATTERN = "databaseNamePattern"
TABLE_NAME_PATTERN = "tableNamePattern"
VIEW_NAME_PATTERN = "viewNamePattern"
FUNCTION_NAME_PATTERN = "functionNamePattern"
PARTITION_NAME_PATTERN = "partitionNamePattern"
MAX_RESULTS = "maxResults"
PAGE_TOKEN = "pageToken"

# Core options
PATH = "path"
TYPE = "type"
WAREHOUSE = "warehouse"
SNAPSHOT_CLEAN_EMPTY_DIRECTORIES = "snapshot.clean-empty-directories"

# Table types
FORMAT_TABLE = "FORMAT_TABLE"
OBJECT_TABLE = "OBJECT_TABLE"


class RESTCatalogServer:
    """Mock REST server for testing"""

    def __init__(self, data_path: str, auth_provider, config: ConfigResponse, warehouse: str,
                 role_name: str = None, token_json: str = None):
        self.logger = logging.getLogger(__name__)
        self.warehouse = warehouse
        self.config_response = config

        # Initialize resource paths
        prefix = config.defaults.get("prefix")
        self.resource_paths = ResourcePaths(prefix)
        self.database_uri = self.resource_paths.databases()

        # Initialize storage
        self.database_store: Dict[str, GetDatabaseResponse] = {}
        self.table_metadata_store: Dict[str, TableMetadata] = {}
        self.no_permission_databases: List[str] = []
        self.no_permission_tables: List[str] = []

        # Initialize mock catalog (simplified)
        self.data_path = data_path
        self.auth_provider = auth_provider
        self.role_name = role_name
        self.token_json = token_json

        # HTTP server setup
        self.server = None
        self.server_thread = None
        self.port = 0

    def start(self) -> None:
        """Start the mock server"""
        handler = self._create_request_handler()
        self.server = HTTPServer(('localhost', 0), handler)
        self.port = self.server.server_port

        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.daemon = True
        self.server_thread.start()

        self.logger.info(f"Mock REST server started on port {self.port}")

    def get_url(self) -> str:
        """Get server URL"""
        return f"http://localhost:{self.port}"

    def shutdown(self) -> None:
        """Shutdown the server"""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
        if self.server_thread:
            self.server_thread.join()

    def _create_request_handler(self):
        """Create HTTP request handler"""
        server_instance = self

        class RequestHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                self._handle_request('GET')

            def do_POST(self):
                self._handle_request('POST')

            def do_DELETE(self):
                self._handle_request('DELETE')

            def _handle_request(self, method: str):
                try:
                    # Parse request
                    parsed_url = urlparse(self.path)
                    resource_path = parsed_url.path
                    parameters = self._parse_query_params(parsed_url.query)

                    # Get request body
                    content_length = int(self.headers.get('Content-Length', 0))
                    data = self.rfile.read(content_length).decode('utf-8') if content_length > 0 else ""

                    # Get headers
                    headers = dict(self.headers)

                    # Handle authentication
                    auth_token = headers.get(AUTHORIZATION_HEADER_KEY.lower())
                    if not self._authenticate(auth_token, resource_path, parameters, method, data):
                        self._send_response(401, "Unauthorized")
                        return

                    # Route request
                    response, status_code = server_instance._route_request(
                        method, resource_path, parameters, data, headers
                    )

                    self._send_response(status_code, response)

                except Exception as e:
                    server_instance.logger.error(f"Request handling error: {e}")
                    self._send_response(500, str(e))

            def _parse_query_params(self, query: str) -> Dict[str, str]:
                """Parse query parameters"""
                if not query:
                    return {}

                params = {}
                for pair in query.split('&'):
                    if '=' in pair:
                        key, value = pair.split('=', 1)
                        params[key.strip()] = api.RESTUtil.decode_string(value.strip())
                return params

            def _authenticate(self, token: str, path: str, params: Dict[str, str],
                              method: str, data: str) -> bool:
                """Authenticate request"""
                # Simplified authentication - always return True for mock
                return True

            def _send_response(self, status_code: int, body: str):
                """Send HTTP response"""
                self.send_response(status_code)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(body.encode('utf-8'))

            def log_message(self, format, *args):
                """Override to use our logger"""
                server_instance.logger.debug(format % args)

        return RequestHandler

    def _route_request(self, method: str, resource_path: str, parameters: Dict[str, str],
                       data: str, headers: Dict[str, str]) -> Tuple[str, int]:
        """Route HTTP request to appropriate handler"""
        try:
            # Config endpoint
            if resource_path.startswith(self.resource_paths.config()):
                warehouse_param = parameters.get(WAREHOUSE)
                if warehouse_param == self.warehouse:
                    return self._mock_response(self.config_response, 200)

            # ecs role
            if resource_path == '/ram/security-credential/':
                return self._mock_response(self.role_name, 200)

            if resource_path == f'/ram/security-credential/{self.role_name}':
                return self._mock_response(self.token_json, 200)

            # Databases endpoint
            if resource_path == self.database_uri or resource_path.startswith(self.database_uri + "?"):
                return self._databases_api_handler(method, data, parameters)

            # Global tables endpoint
            if resource_path.startswith(self.resource_paths.tables()):
                return self._tables_handle(parameters)

            # Database-specific endpoints
            if resource_path.startswith(self.database_uri + "/"):
                return self._handle_database_resource(method, resource_path, parameters, data)

            return self._mock_response(ErrorResponse(None, None, "Not Found", 404), 404)

        except DatabaseNotExistException as e:
            response = ErrorResponse(
                ErrorResponse.RESOURCE_TYPE_DATABASE, e.database, str(e), 404
            )
            return self._mock_response(response, 404)
        except TableNotExistException as e:
            response = ErrorResponse(
                ErrorResponse.RESOURCE_TYPE_TABLE, e.identifier.get_table_name(), str(e), 404
            )
            return self._mock_response(response, 404)
        except DatabaseNoPermissionException as e:
            response = ErrorResponse(
                ErrorResponse.RESOURCE_TYPE_DATABASE, e.database, str(e), 403
            )
            return self._mock_response(response, 403)
        except TableNoPermissionException as e:
            response = ErrorResponse(
                ErrorResponse.RESOURCE_TYPE_TABLE, e.identifier.get_table_name(), str(e), 403
            )
            return self._mock_response(response, 403)
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            response = ErrorResponse(None, None, str(e), 500)
            return self._mock_response(response, 500)

    def _handle_database_resource(self, method: str, resource_path: str,
                                  parameters: Dict[str, str], data: str) -> Tuple[str, int]:
        """Handle database-specific resource requests"""
        # Extract database name and resource path
        path_parts = resource_path[len(self.database_uri) + 1:].split('/')
        database_name = api.RESTUtil.decode_string(path_parts[0])

        # Check database permissions
        if database_name in self.no_permission_databases:
            raise DatabaseNoPermissionException(database_name)

        if database_name not in self.database_store:
            raise DatabaseNotExistException(database_name)

        # Handle different resource types
        if len(path_parts) == 1:
            # Database operations
            return self._database_handle(method, data, database_name)

        elif len(path_parts) == 2:
            # Collection operations (tables, views, functions)
            resource_type = path_parts[1]

            if resource_type.startswith(ResourcePaths.TABLES):
                return self._tables_handle(method, data, database_name, parameters)

        elif len(path_parts) >= 3:
            # Individual resource operations
            resource_type = path_parts[1]
            resource_name = api.RESTUtil.decode_string(path_parts[2])
            identifier = Identifier.create(database_name, resource_name)

            if resource_type == ResourcePaths.TABLES:
                return self._handle_table_resource(method, path_parts, identifier, data, parameters)

        return self._mock_response(ErrorResponse(None, None, "Not Found", 404), 404)

    def _handle_table_resource(self, method: str, path_parts: List[str],
                               identifier: Identifier, data: str,
                               parameters: Dict[str, str]) -> Tuple[str, int]:
        """Handle table-specific resource requests"""
        # Check table permissions
        if identifier.get_full_name() in self.no_permission_tables:
            raise TableNoPermissionException(identifier)

        if len(path_parts) == 3:
            # Basic table operations
            return self._table_handle(method, data, identifier)
        return self._mock_response(ErrorResponse(None, None, "Not Found", 404), 404)

    def _databases_api_handler(self, method: str, data: str,
                               parameters: Dict[str, str]) -> Tuple[str, int]:
        """Handle databases API requests"""
        if method == "GET":
            database_name_pattern = parameters.get(DATABASE_NAME_PATTERN)
            databases = [
                db_name for db_name in self.database_store.keys()
                if not database_name_pattern or self._match_name_pattern(db_name, database_name_pattern)
            ]
            return self._generate_final_list_databases_response(parameters, databases)

        return self._mock_response(ErrorResponse(None, None, "Method Not Allowed", 405), 405)

    def _database_handle(self, method: str, data: str, database_name: str) -> Tuple[str, int]:
        """Handle individual database operations"""
        if database_name not in self.database_store:
            raise DatabaseNotExistException(database_name)

        database = self.database_store[database_name]

        if method == "GET":
            response = database
            return self._mock_response(response, 200)

        elif method == "DELETE":
            del self.database_store[database_name]
            return self._mock_response("", 200)
        return self._mock_response(ErrorResponse(None, None, "Method Not Allowed", 405), 405)

    def _tables_handle(self, method: str = None, data: str = None, database_name: str = None,
                       parameters: Dict[str, str] = None) -> Tuple[str, int]:
        """Handle tables operations"""
        if parameters is None:
            parameters = {}

        if database_name:
            # Database-specific tables
            if method == "GET":
                tables = self._list_tables(database_name, parameters)
                return self._generate_final_list_tables_response(parameters, tables)
        return self._mock_response(ErrorResponse(None, None, "Method Not Allowed", 405), 405)

    def _table_handle(self, method: str, data: str, identifier: Identifier) -> Tuple[str, int]:
        """Handle individual table operations"""
        if method == "GET":
            if identifier.is_system_table():
                # Handle system table
                schema = Schema(fields=[], options={PATH: f"/tmp/{identifier.get_full_name()}"})
                table_metadata = self._create_table_metadata(identifier, 1, schema, None, False)
            else:
                if identifier.get_full_name() not in self.table_metadata_store:
                    raise TableNotExistException(identifier)
                table_metadata = self.table_metadata_store[identifier.get_full_name()]

            schema = table_metadata.schema.to_schema()
            path = schema.options.pop(PATH, None)

            response = self.mock_table(identifier, table_metadata, path, schema)
            return self._mock_response(response, 200)
        #
        # elif method == "POST":
        #     # Alter table
        #     request_body = JSON.from_json(data, AlterTableRequest)
        #     self._alter_table_impl(identifier, request_body.get_changes())
        #     return self._mock_response("", 200)

        elif method == "DELETE":
            # Drop table
            if identifier.get_full_name() in self.table_metadata_store:
                del self.table_metadata_store[identifier.get_full_name()]
            if identifier.get_full_name() in self.table_latest_snapshot_store:
                del self.table_latest_snapshot_store[identifier.get_full_name()]
            if identifier.get_full_name() in self.table_partitions_store:
                del self.table_partitions_store[identifier.get_full_name()]

            return self._mock_response("", 200)

        return self._mock_response(ErrorResponse(None, None, "Method Not Allowed", 405), 405)

    # Utility methods
    def _mock_response(self, response: Union[RESTResponse, str], http_code: int) -> Tuple[str, int]:
        """Create mock response"""
        if isinstance(response, str):
            return response, http_code

        try:
            return JSON.to_json(response), http_code
        except Exception as e:
            self.logger.error(f"Failed to serialize response: {e}")
            return str(e), 500

    def _get_max_results(self, parameters: Dict[str, str]) -> int:
        """Get max results from parameters"""
        max_results_str = parameters.get(MAX_RESULTS)
        if max_results_str:
            try:
                max_results = int(max_results_str)
                return min(max_results, DEFAULT_MAX_RESULTS) if max_results > 0 else DEFAULT_MAX_RESULTS
            except ValueError:
                raise ValueError(f"Invalid maxResults value: {max_results_str}")
        return DEFAULT_MAX_RESULTS

    def _build_paged_entities(self, entities: List[Any], max_results: int,
                              page_token: Optional[str], desc: bool = False) -> PagedList:
        """Build paged entities"""
        # Sort entities
        sorted_entities = sorted(entities, key=self._get_paged_key, reverse=desc)

        # Apply pagination
        paged_entities = []
        for entity in sorted_entities:
            if len(paged_entities) < max_results:
                if not page_token or self._get_paged_key(entity) > page_token:
                    paged_entities.append(entity)
            else:
                break

        # Determine next page token
        next_page_token = None
        if len(paged_entities) == max_results and len(sorted_entities) > max_results:
            next_page_token = self._get_paged_key(paged_entities[-1])

        return PagedList(elements=paged_entities, next_page_token=next_page_token)

    def _get_paged_key(self, entity: Any) -> str:
        """Get paging key for entity"""
        if isinstance(entity, str):
            return entity
        elif hasattr(entity, 'get_name'):
            return entity.get_name()
        elif hasattr(entity, 'get_full_name'):
            return entity.get_full_name()
        elif hasattr(entity, 'name'):
            return entity.name
        else:
            return str(entity)

    def _match_name_pattern(self, name: str, pattern: str) -> bool:
        """Match name against SQL pattern"""
        if not pattern:
            raise ValueError("Pattern cannot be empty")
        regex_pattern = self._sql_pattern_to_regex(pattern)
        return re.match(regex_pattern, name) is not None

    def _sql_pattern_to_regex(self, pattern: str) -> str:
        """Convert SQL pattern to regex"""
        regex = []
        escaped = False

        for char in pattern:
            if escaped:
                regex.append(re.escape(char))
                escaped = False
            elif char == '\\':
                escaped = True
            elif char == '%':
                regex.append('.*')
            elif char == '_':
                regex.append('.')
            else:
                regex.append(re.escape(char))

        return '^' + ''.join(regex) + '$'

    def _create_table_metadata(self, identifier: Identifier, schema_id: int,
                               schema: Schema, uuid_str: str, is_external: bool) -> TableMetadata:
        """Create table metadata"""
        options = schema.options.copy()
        path = f"/tmp/{identifier.get_full_name()}"
        options[PATH] = path

        table_schema = TableSchema(
            id=schema_id,
            fields=schema.fields,
            highest_field_id=len(schema.fields) - 1,
            partition_keys=schema.partition_keys,
            primary_keys=schema.primary_keys,
            options=options,
            comment=schema.comment
        )

        return TableMetadata(
            schema=table_schema,
            is_external=is_external,
            uuid=uuid_str or str(uuid.uuid4())
        )

    # List methods
    def _list_tables(self, database_name: str, parameters: Dict[str, str]) -> List[str]:
        """List tables in database"""
        table_name_pattern = parameters.get(TABLE_NAME_PATTERN)
        tables = []

        for full_name, metadata in self.table_metadata_store.items():
            identifier = Identifier.from_string(full_name)
            if (identifier.get_database_name() == database_name and
                    (not table_name_pattern or self._match_name_pattern(identifier.get_table_name(),
                                                                        table_name_pattern))):
                tables.append(identifier.get_table_name())

        return tables

    # Response generation methods
    def _generate_final_list_databases_response(self, parameters: Dict[str, str],
                                                databases: List[str]) -> Tuple[str, int]:
        """Generate final list databases response"""
        if databases:
            max_results = self._get_max_results(parameters)
            page_token = parameters.get(PAGE_TOKEN)
            paged_dbs = self._build_paged_entities(databases, max_results, page_token)
            response = ListDatabasesResponse(
                databases=paged_dbs.elements,
                next_page_token=paged_dbs.next_page_token
            )
        else:
            response = ListDatabasesResponse(databases=[], next_page_token=None)

        return self._mock_response(response, 200)

    def _generate_final_list_tables_response(self, parameters: Dict[str, str],
                                             tables: List[str]) -> Tuple[str, int]:
        """Generate final list tables response"""
        if tables:
            max_results = self._get_max_results(parameters)
            page_token = parameters.get(PAGE_TOKEN)
            paged_tables = self._build_paged_entities(tables, max_results, page_token)
            response = ListTablesResponse(
                tables=paged_tables.elements,
                next_page_token=paged_tables.next_page_token
            )
        else:
            response = ListTablesResponse(tables=[], next_page_token=None)

        return self._mock_response(response, 200)

    def add_no_permission_database(self, database: str) -> None:
        """Add no permission database"""
        self.no_permission_databases.append(database)

    def add_no_permission_table(self, identifier: Identifier) -> None:
        """Add no permission table"""
        self.no_permission_tables.append(identifier.get_full_name())

    def mock_database(self, name: str, options: dict[str, str]) -> GetDatabaseResponse:
        return GetDatabaseResponse(
            id=str(uuid.uuid4()),
            name=name,
            location=f"{self.data_path}/{name}",
            options=options,
            owner="owner",
            created_at=1,
            created_by="created",
            updated_at=1,
            updated_by="updated"
        )

    def mock_table(self, identifier: Identifier, table_metadata: TableMetadata, path: str,
                   schema: Schema) -> GetTableResponse:
        return GetTableResponse(
            id=str(table_metadata.uuid),
            name=identifier.get_object_name(),
            path=path,
            is_external=table_metadata.is_external,
            schema_id=table_metadata.schema.id,
            schema=schema,
            owner="owner",
            created_at=1,
            created_by="created",
            updated_at=1,
            updated_by="updated"
        )


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

        # Create mock auth provider
        class MockAuthProvider:
            def merge_auth_header(self, headers, auth_param):
                return {AUTHORIZATION_HEADER_KEY: "Bearer test-token"}

        # Create server
        server = RESTCatalogServer(
            data_path="/tmp/test_warehouse",
            auth_provider=MockAuthProvider(),
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
            schema = TableSchema(len(data_fields), data_fields, len(data_fields), [], [], {}, "")
            test_tables = {
                "default.user": TableMetadata(uuid=str(uuid.uuid4()), is_external=True, schema=schema),
            }
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
            api = RESTApi(options)
            self.assertSetEqual(set(api.list_databases()), {*test_databases})
            self.assertEqual(api.get_database('default'), test_databases.get('default'))
            table = api.get_table(Identifier.from_string('default.user'))
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
                api.RESTCatalogOptions.DLF_TOKEN_LOADER: 'ecs',
                api.RESTCatalogOptions.DLF_TOKEN_ECS_METADATA_URL: ecs_metadata_url
            }
            loader = DLFTokenLoaderFactory.create_token_loader(options)
            load_token = loader.load_token()
            self.assertEqual(load_token.access_key_id, token.access_key_id)
            self.assertEqual(load_token.access_key_secret, token.access_key_secret)
            self.assertEqual(load_token.security_token, token.security_token)
            self.assertEqual(load_token.expiration, token.expiration)
            options_with_role = {
                api.RESTCatalogOptions.DLF_TOKEN_LOADER: 'ecs',
                api.RESTCatalogOptions.DLF_TOKEN_ECS_METADATA_URL: ecs_metadata_url,
                api.RESTCatalogOptions.DLF_TOKEN_ECS_ROLE_NAME: role_name,
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
