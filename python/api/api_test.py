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

import json
import logging
import os
import re
import tempfile
import time
import uuid
from collections import defaultdict
from typing import Dict, List, Optional, Any, Union, Tuple
from urllib.parse import parse_qs, unquote
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs


# Mock classes for Paimon entities
@dataclass
class Identifier:
    """Table/View/Function identifier"""
    database_name: str
    object_name: str
    branch_name: Optional[str] = None

    @classmethod
    def create(cls, database_name: str, object_name: str) -> 'Identifier':
        return cls(database_name, object_name)

    @classmethod
    def from_string(cls, full_name: str) -> 'Identifier':
        parts = full_name.split('.')
        if len(parts) == 2:
            return cls(parts[0], parts[1])
        elif len(parts) == 3:
            return cls(parts[0], parts[1], parts[2])
        else:
            raise ValueError(f"Invalid identifier format: {full_name}")

    def get_full_name(self) -> str:
        if self.branch_name:
            return f"{self.database_name}.{self.object_name}.{self.branch_name}"
        return f"{self.database_name}.{self.object_name}"

    def get_database_name(self) -> str:
        return self.database_name

    def get_table_name(self) -> str:
        return self.object_name

    def get_object_name(self) -> str:
        return self.object_name

    def get_branch_name(self) -> Optional[str]:
        return self.branch_name

    def is_system_table(self) -> bool:
        return self.object_name.startswith('$')


@dataclass
class Database:
    """Database entity"""
    name: str
    options: Dict[str, str] = field(default_factory=dict)
    comment: Optional[str] = None

    @classmethod
    def of(cls, name: str, options: Dict[str, str], comment: Optional[str]) -> 'Database':
        return cls(name, options, comment)


@dataclass
class Schema:
    """Table schema"""
    fields: List[Dict[str, Any]]
    partition_keys: List[str] = field(default_factory=list)
    primary_keys: List[str] = field(default_factory=list)
    options: Dict[str, str] = field(default_factory=dict)
    comment: Optional[str] = None

    def field_names(self) -> List[str]:
        return [field['name'] for field in self.fields]


@dataclass
class TableSchema:
    """Table schema with ID"""
    id: int
    fields: List[Dict[str, Any]]
    highest_field_id: int
    partition_keys: List[str]
    primary_keys: List[str]
    options: Dict[str, str]
    comment: Optional[str]

    def to_schema(self) -> Schema:
        return Schema(
            fields=self.fields,
            partition_keys=self.partition_keys,
            primary_keys=self.primary_keys,
            options=self.options,
            comment=self.comment
        )


@dataclass
class TableMetadata:
    """Table metadata"""
    schema: TableSchema
    is_external: bool
    uuid: str


@dataclass
class Snapshot:
    """Table snapshot"""
    id: int
    schema_id: int
    base_manifest_list: str
    delta_manifest_list: str
    changelog_manifest_list: Optional[str]
    commit_user: str
    commit_identifier: int
    commit_kind: str
    time_millis: int
    log_offsets: Dict[int, int]
    total_record_count: int
    delta_record_count: int
    changelog_record_count: int
    watermark: Optional[int]


@dataclass
class TableSnapshot:
    """Table snapshot with statistics"""
    snapshot: Snapshot
    record_count: int
    file_size_in_bytes: int
    file_count: int
    last_file_creation_time: int


@dataclass
class Partition:
    """Table partition"""
    spec: Dict[str, str]
    record_count: int
    file_size_in_bytes: int
    file_count: int
    last_file_creation_time: int
    done: bool


@dataclass
class PartitionStatistics:
    """Partition statistics"""
    spec: Dict[str, str]
    record_count: int
    file_size_in_bytes: int
    file_count: int
    last_file_creation_time: int


@dataclass
class RESTToken:
    """REST authentication token"""
    token: Dict[str, str]
    expire_at_millis: int


@dataclass
class PagedList:
    """Paged list result"""
    elements: List[Any]
    next_page_token: Optional[str]


# Request/Response classes
@dataclass
class CreateDatabaseRequest:
    """Create database request"""
    name: str
    options: Dict[str, str] = field(default_factory=dict)

    def get_name(self) -> str:
        return self.name

    def get_options(self) -> Dict[str, str]:
        return self.options


@dataclass
class AlterDatabaseRequest:
    """Alter database request"""
    removals: List[str] = field(default_factory=list)
    updates: Dict[str, str] = field(default_factory=dict)

    def get_removals(self) -> List[str]:
        return self.removals

    def get_updates(self) -> Dict[str, str]:
        return self.updates


@dataclass
class CreateTableRequest:
    """Create table request"""
    identifier: Identifier
    schema: Schema

    def get_identifier(self) -> Identifier:
        return self.identifier

    def get_schema(self) -> Schema:
        return self.schema


@dataclass
class AlterTableRequest:
    """Alter table request"""
    changes: List[Any]

    def get_changes(self) -> List[Any]:
        return self.changes


@dataclass
class RenameTableRequest:
    """Rename table request"""
    source: Identifier
    destination: Identifier

    def get_source(self) -> Identifier:
        return self.source

    def get_destination(self) -> Identifier:
        return self.destination


@dataclass
class CommitTableRequest:
    """Commit table request"""
    table_id: str
    snapshot: Snapshot
    statistics: List[PartitionStatistics]

    def get_table_id(self) -> str:
        return self.table_id

    def get_snapshot(self) -> Snapshot:
        return self.snapshot

    def get_statistics(self) -> List[PartitionStatistics]:
        return self.statistics


@dataclass
class AuthTableQueryRequest:
    """Auth table query request"""
    select_columns: Optional[List[str]] = None

    def select(self) -> Optional[List[str]]:
        return self.select_columns


@dataclass
class MarkDonePartitionsRequest:
    """Mark done partitions request"""
    partition_specs: List[Dict[str, str]]

    def get_partition_specs(self) -> List[Dict[str, str]]:
        return self.partition_specs


@dataclass
class AlterViewRequest:
    """Alter view request"""
    view_changes: List[Any]


@dataclass
class AlterFunctionRequest:
    """Alter function request"""
    changes: List[Any]


@dataclass
class RollbackTableRequest:
    """Rollback table request"""
    instant: Any

    def get_instant(self) -> Any:
        return self.instant


@dataclass
class CreateBranchRequest:
    """Create branch request"""
    branch: str
    from_tag: Optional[str] = None


# Response classes
class RESTResponse(ABC):
    """Base REST response"""
    pass


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


@dataclass
class ConfigResponse(RESTResponse):
    """Config response"""
    defaults: Dict[str, str] = field(default_factory=dict)

    def get_defaults(self) -> Dict[str, str]:
        return self.defaults


@dataclass
class ListDatabasesResponse(RESTResponse):
    """List databases response"""
    databases: List[str]
    next_page_token: Optional[str]


@dataclass
class GetDatabaseResponse(RESTResponse):
    """Get database response"""
    id: str
    name: str
    location: str
    options: Dict[str, str]
    owner: str
    created_at: int
    created_by: str
    updated_at: int
    updated_by: str


@dataclass
class AlterDatabaseResponse(RESTResponse):
    """Alter database response"""
    removed: List[str]
    updated: List[str]
    missing: List[str]


@dataclass
class ListTablesResponse(RESTResponse):
    """List tables response"""
    tables: List[str]
    next_page_token: Optional[str]


@dataclass
class ListTablesGloballyResponse(RESTResponse):
    """List tables globally response"""
    tables: List[Identifier]
    next_page_token: Optional[str]


@dataclass
class GetTableResponse(RESTResponse):
    """Get table response"""
    id: str
    name: str
    location: str
    is_external: bool
    schema_id: int
    schema: Schema
    owner: str
    created_at: int
    created_by: str
    updated_at: int
    updated_by: str

    def get_name(self) -> str:
        return self.name


@dataclass
class ListTableDetailsResponse(RESTResponse):
    """List table details response"""
    tables: List[GetTableResponse]
    next_page_token: Optional[str]


@dataclass
class GetTableTokenResponse(RESTResponse):
    """Get table token response"""
    token: Dict[str, str]
    expire_at_millis: int


@dataclass
class GetTableSnapshotResponse(RESTResponse):
    """Get table snapshot response"""
    snapshot: TableSnapshot


@dataclass
class ListSnapshotsResponse(RESTResponse):
    """List snapshots response"""
    snapshots: List[Snapshot]
    next_page_token: Optional[str]


@dataclass
class GetVersionSnapshotResponse(RESTResponse):
    """Get version snapshot response"""
    snapshot: Snapshot


@dataclass
class AuthTableQueryResponse(RESTResponse):
    """Auth table query response"""
    allowed_columns: List[str]


@dataclass
class CommitTableResponse(RESTResponse):
    """Commit table response"""
    success: bool


@dataclass
class ListPartitionsResponse(RESTResponse):
    """List partitions response"""
    partitions: List[Partition]
    next_page_token: Optional[str]


@dataclass
class ListBranchesResponse(RESTResponse):
    """List branches response"""
    branches: Optional[List[str]]


# Utility classes
class RESTUtil:
    """REST utilities"""

    @staticmethod
    def decode_string(encoded: str) -> str:
        """Decode URL-encoded string"""
        return unquote(encoded)

    @staticmethod
    def validate_prefix_sql_pattern(pattern: str) -> None:
        """Validate SQL pattern"""
        # Simple validation - in real implementation would be more comprehensive
        if not pattern:
            raise ValueError("Pattern cannot be empty")


class RESTApiParser:
    """REST API utilities"""

    @staticmethod
    def from_json(json_str: str, cls: type) -> Any:
        """Deserialize JSON to object"""
        data = json.loads(json_str)
        # Simple deserialization - in real implementation would use proper mapping
        return cls(**data) if hasattr(cls, '__dataclass_fields__') else data

    @staticmethod
    def to_json(obj: Any) -> str:
        """Serialize object to JSON"""
        if hasattr(obj, '__dict__'):
            return json.dumps(obj.__dict__, default=str, indent=2)
        return json.dumps(obj, default=str, indent=2)


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
        return f"/v1/config"

    def databases(self) -> str:
        return f"/v1/{self.prefix}/databases"

    def tables(self) -> str:
        return f"{self.prefix}/tables"

    def views(self) -> str:
        return f"{self.prefix}/views"

    def functions(self) -> str:
        return f"{self.prefix}/functions"

    def rename_table(self) -> str:
        return f"{self.prefix}/tables/rename"

    def rename_view(self) -> str:
        return f"{self.prefix}/views/rename"


class DataTokenStore:
    """Data token storage"""
    _tokens: Dict[str, RESTToken] = {}
    _lock = threading.Lock()

    @classmethod
    def put_data_token(cls, warehouse: str, table_name: str, token: RESTToken) -> None:
        """Store data token"""
        key = f"{warehouse}#{table_name}"
        with cls._lock:
            cls._tokens[key] = token

    @classmethod
    def get_data_token(cls, warehouse: str, table_name: str) -> Optional[RESTToken]:
        """Get data token"""
        key = f"{warehouse}#{table_name}"
        with cls._lock:
            return cls._tokens.get(key)

    @classmethod
    def remove_data_token(cls, warehouse: str, table_name: str) -> None:
        """Remove data token"""
        key = f"{warehouse}#{table_name}"
        with cls._lock:
            cls._tokens.pop(key, None)


# Exception classes
class CatalogException(Exception):
    """Base catalog exception"""
    pass


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

    def __init__(self, data_path: str, auth_provider, config: ConfigResponse, warehouse: str):
        self.logger = logging.getLogger(__name__)
        self.warehouse = warehouse
        self.config_response = config

        # Initialize resource paths
        prefix = self.config_response.get_defaults().get("prefix", "")
        self.resource_paths = ResourcePaths(prefix)
        self.database_uri = self.resource_paths.databases()

        # Initialize storage
        self.database_store: Dict[str, Database] = {}
        self.table_metadata_store: Dict[str, TableMetadata] = {}
        self.table_partitions_store: Dict[str, List[Partition]] = {}
        self.table_latest_snapshot_store: Dict[str, TableSnapshot] = {}
        self.table_with_snapshot_id_2_snapshot_store: Dict[str, TableSnapshot] = {}
        self.no_permission_databases: List[str] = []
        self.no_permission_tables: List[str] = []

        # Initialize mock catalog (simplified)
        self.data_path = data_path
        self.auth_provider = auth_provider

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
                        params[key.strip()] = RESTUtil.decode_string(value.strip())
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

            # Databases endpoint
            if resource_path == self.database_uri or resource_path.startswith(self.database_uri + "?"):
                return self._databases_api_handler(method, data, parameters)

            # Rename endpoints
            if resource_path == self.resource_paths.rename_table():
                return self._rename_table_handle(data)

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
        database_name = RESTUtil.decode_string(path_parts[0])

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
            resource_name = RESTUtil.decode_string(path_parts[2])
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

        elif len(path_parts) >= 4:
            operation = path_parts[3]

            # if operation == "token":
            #     return self._get_data_token_handle(identifier)
            # elif operation == "snapshot":
            #     return self._snapshot_handle(identifier)
            # elif operation == ResourcePaths.SNAPSHOTS:
            #     if len(path_parts) == 4:
            #         return self._list_snapshots(identifier)
            #     else:
            #         version = path_parts[4]
            #         return self._load_snapshot(identifier, version)
            # elif operation == "auth":
            #     return self._auth_table(identifier, data)
            # elif operation == "commit":
            #     return self._commit_table_handle(identifier, data)
            # elif operation == ResourcePaths.ROLLBACK:
            #     return self._rollback_table_handle(identifier, data)
            # elif operation.startswith("partitions"):
            #     if len(path_parts) == 5 and path_parts[4] == "mark":
            #         return self._mark_done_partitions_handle(identifier, data)
            #     else:
            #         return self._partitions_api_handle(method, parameters, identifier)
            # elif operation == "branches":
            #     return self._branch_api_handle(path_parts, method, data, identifier)

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

        elif method == "POST":
            request_body = RESTApiParser.from_json(data, CreateDatabaseRequest)
            database_name = request_body.get_name()

            if database_name in self.no_permission_databases:
                raise DatabaseNoPermissionException(database_name)

            if database_name in self.database_store:
                raise DatabaseAlreadyExistException(database_name)

            database = Database.of(database_name, request_body.get_options(), None)
            self.database_store[database_name] = database

            return self._mock_response("", 200)

        return self._mock_response(ErrorResponse(None, None, "Method Not Allowed", 405), 405)

    def _database_handle(self, method: str, data: str, database_name: str) -> Tuple[str, int]:
        """Handle individual database operations"""
        if database_name not in self.database_store:
            raise DatabaseNotExistException(database_name)

        database = self.database_store[database_name]

        if method == "GET":
            response = GetDatabaseResponse(
                id=str(uuid.uuid4()),
                name=database.name,
                location=f"{self.data_path}/{database_name}",
                options=database.options,
                owner="owner",
                created_at=1,
                created_by="created",
                updated_at=1,
                updated_by="updated"
            )
            return self._mock_response(response, 200)

        elif method == "DELETE":
            del self.database_store[database_name]
            return self._mock_response("", 200)

        elif method == "POST":
            request_body = RESTApiParser.from_json(data, AlterDatabaseRequest)

            # Apply changes
            new_options = database.options.copy()
            for key in request_body.get_removals():
                new_options.pop(key, None)
            new_options.update(request_body.get_updates())

            # Update database
            updated_database = Database.of(database_name, new_options, database.comment)
            self.database_store[database_name] = updated_database

            response = AlterDatabaseResponse(
                removed=request_body.get_removals(),
                updated=list(request_body.get_updates().keys()),
                missing=[]
            )
            return self._mock_response(response, 200)

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

            elif method == "POST":
                request_body = RESTApiParser.from_json(data, CreateTableRequest)
                identifier = request_body.get_identifier()
                schema = request_body.get_schema()

                if identifier.get_full_name() in self.table_metadata_store:
                    raise TableAlreadyExistException(identifier)

                # Create table metadata
                table_metadata = self._create_table_metadata(
                    identifier, 0, schema, str(uuid.uuid4()), False
                )
                self.table_metadata_store[identifier.get_full_name()] = table_metadata

                return self._mock_response("", 200)
        else:
            # Global tables
            tables = self._list_tables_globally(parameters)
            if tables:
                max_results = self._get_max_results(parameters)
                page_token = parameters.get(PAGE_TOKEN)
                paged_tables = self._build_paged_entities(tables, max_results, page_token)
                response = ListTablesGloballyResponse(
                    tables=paged_tables.elements,
                    next_page_token=paged_tables.next_page_token
                )
            else:
                response = ListTablesResponse(tables=[], next_page_token=None)

            return self._mock_response(response, 200)

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

            response = GetTableResponse(
                id=table_metadata.uuid,
                name=identifier.get_object_name(),
                location=path,
                is_external=table_metadata.is_external,
                schema_id=table_metadata.schema.id,
                schema=schema,
                owner="owner",
                created_at=1,
                created_by="created",
                updated_at=1,
                updated_by="updated"
            )
            return self._mock_response(response, 200)

        elif method == "POST":
            # Alter table
            request_body = RESTApiParser.from_json(data, AlterTableRequest)
            self._alter_table_impl(identifier, request_body.get_changes())
            return self._mock_response("", 200)

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
            return RESTApiParser.to_json(response), http_code
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
        RESTUtil.validate_prefix_sql_pattern(pattern)
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

    # Public API methods for test setup
    def set_table_snapshot(self, identifier: Identifier, snapshot: Snapshot,
                           record_count: int, file_size_in_bytes: int,
                           file_count: int, last_file_creation_time: int) -> None:
        """Set table snapshot for testing"""
        table_snapshot = TableSnapshot(
            snapshot=snapshot,
            record_count=record_count,
            file_size_in_bytes=file_size_in_bytes,
            file_count=file_count,
            last_file_creation_time=last_file_creation_time
        )
        self.table_latest_snapshot_store[identifier.get_full_name()] = table_snapshot

    def set_data_token(self, identifier: Identifier, token: RESTToken) -> None:
        """Set data token for testing"""
        DataTokenStore.put_data_token(self.warehouse, identifier.get_full_name(), token)

    def remove_data_token(self, identifier: Identifier) -> None:
        """Remove data token"""
        DataTokenStore.remove_data_token(self.warehouse, identifier.get_full_name())

    def add_no_permission_database(self, database: str) -> None:
        """Add no permission database"""
        self.no_permission_databases.append(database)

    def add_no_permission_table(self, identifier: Identifier) -> None:
        """Add no permission table"""
        self.no_permission_tables.append(identifier.get_full_name())

    def get_data_token(self, identifier: Identifier) -> Optional[RESTToken]:
        """Get data token"""
        return DataTokenStore.get_data_token(self.warehouse, identifier.get_full_name())


# Example usage
def main():
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
            "default": Database.of("default", {"env": "test"}, "default"),
            "test_db1": Database.of("test_db1", {"env": "test"}, "Test database 1"),
            "test_db2": Database.of("test_db2", {"env": "test"}, "Test database 2"),
            "prod_db": Database.of("prod_db", {"env": "prod"}, "Production database")
        }
        test_tables = {
            "default.user": TableMetadata(uuid=uuid.uuid4(), is_external=True, schema=TableSchema(1, [{"name": "int"}], 1, [], [], {}, "")),
        }
        server.table_metadata_store.update(test_tables)
        server.database_store.update(test_databases)
        from api import RESTApi
        options = {
            'uri': f"http://localhost:{server.port}",
            'warehouse': 'test_warehouse',
            'dlf.region': 'cn-hangzhou',
            "token.provider": "xxxx",
            'dlf.access-key-id': 'xxxx',
            'dlf.access-key-secret': 'xxxx'
        }
        api = RESTApi(options)
        print(api.list_databases())
        print(api.get_database('default'))
        print(api.get_table(Identifier.from_string('default.user')))

    finally:
        # Shutdown server
        server.shutdown()
        print("Server stopped")


if __name__ == "__main__":
    main()
