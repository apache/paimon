#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import logging
from typing import Callable, Dict, List, Optional, Union

import re

from pypaimon.api.api_request import (AlterDatabaseRequest, AlterFunctionRequest,
                                      AlterTableRequest, CommitTableRequest,
                                      CreateDatabaseRequest, CreateFunctionRequest,
                                      CreateTableRequest, CreateTagRequest,
                                      RenameTableRequest, RollbackTableRequest)
from pypaimon.api.api_response import (CommitTableResponse, ConfigResponse,
                                       GetDatabaseResponse, GetFunctionResponse,
                                       GetTableResponse,
                                       GetTableTokenResponse, GetTagResponse,
                                       ListDatabasesResponse,
                                       ListFunctionDetailsResponse,
                                       ListFunctionsGloballyResponse,
                                       ListFunctionsResponse,
                                       ListPartitionsResponse,
                                       ListTablesResponse, ListTagsResponse,
                                       PagedList,
                                       PagedResponse, GetTableSnapshotResponse,
                                       Partition)
from pypaimon.api.auth import AuthProviderFactory, RESTAuthFunction
from pypaimon.api.client import HttpClient
from pypaimon.api.resource_paths import ResourcePaths
from pypaimon.api.rest_util import RESTUtil
from pypaimon.api.typedef import T
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions
from pypaimon.common.identifier import Identifier
from pypaimon.schema.schema import Schema
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_commit import PartitionStatistics


class RESTApi:
    HEADER_PREFIX = "header."
    MAX_RESULTS = "maxResults"
    PAGE_TOKEN = "pageToken"
    DATABASE_NAME_PATTERN = "databaseNamePattern"
    TABLE_NAME_PATTERN = "tableNamePattern"
    TABLE_TYPE = "tableType"
    FUNCTION_NAME_PATTERN = "functionNamePattern"
    PARTITION_NAME_PATTERN = "partitionNamePattern"
    TAG_NAME_PREFIX = "tagNamePrefix"
    TOKEN_EXPIRATION_SAFE_TIME_MILLIS = 3_600_000

    # Function name validation pattern
    _FUNCTION_NAME_PATTERN = re.compile(r'^(?=.*[A-Za-z])[A-Za-z0-9._-]+$')

    def __init__(self, options: Union[Options, Dict[str, str]], config_required: bool = True):
        if isinstance(options, dict):
            options = Options(options)
        if not options:
            raise ValueError("Options cannot be None or empty")

        uri = options.get(CatalogOptions.URI)
        if not uri or not uri.strip():
            raise ValueError("URI cannot be empty")

        self.logger = logging.getLogger(self.__class__.__name__)
        self.client = HttpClient(uri)
        auth_provider = AuthProviderFactory.create_auth_provider(options)
        base_headers = RESTUtil.extract_prefix_map(options, self.HEADER_PREFIX)

        if config_required:
            warehouse = options.get(CatalogOptions.WAREHOUSE)
            if not warehouse or not warehouse.strip():
                raise ValueError("Warehouse name cannot be empty")

            query_params = {
                CatalogOptions.WAREHOUSE.key(): RESTUtil.encode_string(warehouse)
            }

            config_response = self.client.get_with_params(
                ResourcePaths.config(),
                query_params,
                ConfigResponse,
                RESTAuthFunction(base_headers, auth_provider),
            )
            options = config_response.merge(options)
            base_headers.update(
                RESTUtil.extract_prefix_map(options, self.HEADER_PREFIX)
            )

        self.rest_auth_function = RESTAuthFunction(base_headers, auth_provider)
        self.options = options
        self.resource_paths = ResourcePaths.for_catalog_properties(options)

    def __build_paged_query_params(
            self,
            max_results: Optional[int],
            page_token: Optional[str],
            name_patterns: Dict[str, str],
    ) -> Dict[str, str]:
        query_params = {}
        if max_results is not None and max_results > 0:
            query_params[RESTApi.MAX_RESULTS] = str(max_results)

        if page_token is not None and page_token.strip():
            query_params[RESTApi.PAGE_TOKEN] = page_token

        for key, value in name_patterns.items():
            if key and value and key.strip() and value.strip():
                query_params[key] = value

        return query_params

    def __list_data_from_page_api(
            self, page_api: Callable[[Dict[str, str]], PagedResponse[T]]
    ) -> List[T]:
        results = []
        query_params = {}
        page_token = None

        while True:
            if page_token:
                query_params[RESTApi.PAGE_TOKEN] = page_token
            elif RESTApi.PAGE_TOKEN in query_params:
                del query_params[RESTApi.PAGE_TOKEN]

            response = page_api(query_params)

            if response.data:
                results.extend(response.data())

            page_token = response.next_page_token

            if not page_token or not response.data:
                break

        return results

    def get_options(self) -> Options:
        return self.options

    def list_databases(self) -> List[str]:
        return self.__list_data_from_page_api(
            lambda query_params: self.client.get_with_params(
                self.resource_paths.databases(),
                query_params,
                ListDatabasesResponse,
                self.rest_auth_function,
            )
        )

    def list_databases_paged(
            self,
            max_results: Optional[int] = None,
            page_token: Optional[str] = None,
            database_name_pattern: Optional[str] = None,
    ) -> PagedList[str]:

        response = self.client.get_with_params(
            self.resource_paths.databases(),
            self.__build_paged_query_params(
                max_results,
                page_token,
                {self.DATABASE_NAME_PATTERN: database_name_pattern},
            ),
            ListDatabasesResponse,
            self.rest_auth_function,
        )

        databases = response.data() or []
        return PagedList(databases, response.get_next_page_token())

    def create_database(self, name: str, properties: Dict[str, str]) -> None:
        if not name or not name.strip():
            raise ValueError("Database name cannot be empty")

        request = CreateDatabaseRequest(name, properties)
        self.client.post(
            self.resource_paths.databases(), request, self.rest_auth_function
        )

    def get_database(self, name: str) -> GetDatabaseResponse:
        if not name or not name.strip():
            raise ValueError("Database name cannot be empty")

        return self.client.get(
            self.resource_paths.database(name),
            GetDatabaseResponse,
            self.rest_auth_function,
        )

    def drop_database(self, name: str) -> None:
        if not name or not name.strip():
            raise ValueError("Database name cannot be empty")

        self.client.delete(
            self.resource_paths.database(name),
            self.rest_auth_function)

    def alter_database(
            self,
            name: str,
            removals: Optional[List[str]] = None,
            updates: Optional[Dict[str, str]] = None,
    ):
        if not name or not name.strip():
            raise ValueError("Database name cannot be empty")
        removals = removals or []
        updates = updates or {}
        request = AlterDatabaseRequest(removals, updates)

        return self.client.post(
            self.resource_paths.database(name),
            request,
            self.rest_auth_function)

    def list_tables(self, database_name: str) -> List[str]:
        if not database_name or not database_name.strip():
            raise ValueError("Database name cannot be empty")

        return self.__list_data_from_page_api(
            lambda query_params: self.client.get_with_params(
                self.resource_paths.tables(database_name),
                query_params,
                ListTablesResponse,
                self.rest_auth_function,
            )
        )

    def list_tables_paged(
            self,
            database_name: str,
            max_results: Optional[int] = None,
            page_token: Optional[str] = None,
            table_name_pattern: Optional[str] = None,
            table_type: Optional[str] = None,
    ) -> PagedList[str]:
        if not database_name or not database_name.strip():
            raise ValueError("Database name cannot be empty")

        response = self.client.get_with_params(
            self.resource_paths.tables(database_name),
            self.__build_paged_query_params(
                max_results,
                page_token,
                {
                    self.TABLE_NAME_PATTERN: table_name_pattern,
                    self.TABLE_TYPE: table_type,
                },
            ),
            ListTablesResponse,
            self.rest_auth_function,
        )

        tables = response.data() or []
        return PagedList(tables, response.get_next_page_token())

    def create_table(self, identifier: Identifier, schema: Schema) -> None:
        database_name, _ = self.__validate_identifier(identifier)
        if not schema:
            raise ValueError("Schema cannot be None")

        request = CreateTableRequest(identifier, schema)
        return self.client.post(
            self.resource_paths.tables(database_name),
            request,
            self.rest_auth_function)

    def get_table(self, identifier: Identifier) -> GetTableResponse:
        database_name, table_name = self.__validate_identifier(identifier)

        return self.client.get(
            self.resource_paths.table(
                database_name,
                table_name),
            GetTableResponse,
            self.rest_auth_function,
        )

    def drop_table(self, identifier: Identifier) -> GetTableResponse:
        database_name, table_name = self.__validate_identifier(identifier)

        return self.client.delete(
            self.resource_paths.table(
                database_name,
                table_name),
            self.rest_auth_function,
        )

    def rename_table(self, source_identifier: Identifier, target_identifier: Identifier) -> None:
        if not source_identifier:
            raise ValueError("Source identifier cannot be None")
        if not target_identifier:
            raise ValueError("Target identifier cannot be None")
        self.__validate_identifier(source_identifier)
        self.__validate_identifier(target_identifier)

        request = RenameTableRequest(source_identifier, target_identifier)
        return self.client.post(
            self.resource_paths.rename_table(),
            request,
            self.rest_auth_function)

    def alter_table(self, identifier: Identifier, changes: List):
        database_name, table_name = self.__validate_identifier(identifier)
        if not changes:
            raise ValueError("Changes cannot be empty")

        request = AlterTableRequest(changes)
        return self.client.post(
            self.resource_paths.table(database_name, table_name),
            request,
            self.rest_auth_function)

    def load_table_token(self, identifier: Identifier) -> GetTableTokenResponse:
        database_name, table_name = self.__validate_identifier(identifier)

        return self.client.get(
            self.resource_paths.table_token(
                database_name,
                table_name),
            GetTableTokenResponse,
            self.rest_auth_function,
        )

    def commit_snapshot(
            self,
            identifier: Identifier,
            table_uuid: Optional[str],
            snapshot: Snapshot,
            statistics: List[PartitionStatistics]
    ) -> bool:
        """
        Commit snapshot for table.

        Args:
            identifier: Database name and table name
            table_uuid: UUID of the table to avoid wrong commit
            snapshot: Snapshot for committing
            statistics: Statistics for this snapshot incremental

        Returns:
            True if commit success

        Raises:
            NoSuchResourceException: Exception thrown on HTTP 404 means the table not exists
            ForbiddenException: Exception thrown on HTTP 403 means don't have the permission for this table
        """
        database_name, table_name = self.__validate_identifier(identifier)
        if not snapshot:
            raise ValueError("Snapshot cannot be None")
        if statistics is None:
            raise ValueError("Statistics cannot be None")

        request = CommitTableRequest(table_uuid, snapshot, statistics)
        response = self.client.post_with_response_type(
            self.resource_paths.commit_table(
                database_name, table_name),
            request,
            CommitTableResponse,
            self.rest_auth_function
        )
        return response.is_success()

    def rollback_to(self, identifier, instant, from_snapshot=None):
        """Rollback table to the given instant.

        Args:
            identifier: The table identifier.
            instant: The Instant (SnapshotInstant or TagInstant) to rollback to.
            from_snapshot: Optional snapshot ID. Success only occurs when the
                latest snapshot is this snapshot.

        Raises:
            NoSuchResourceException: If the table, snapshot or tag does not exist.
            ForbiddenException: If no permission to access this table.
        """
        database_name, table_name = self.__validate_identifier(identifier)
        request = RollbackTableRequest(instant=instant, from_snapshot=from_snapshot)
        self.client.post(
            self.resource_paths.rollback_table(database_name, table_name),
            request,
            self.rest_auth_function
        )

    def load_snapshot(self, identifier: Identifier) -> Optional['TableSnapshot']:
        """Load latest snapshot for table.

        Args:
            identifier: Database name and table name.

        Returns:
            TableSnapshot instance or None if snapshot not found.
        """
        database_name, table_name = self.__validate_identifier(identifier)
        response = self.client.get(
            self.resource_paths.table_snapshot(database_name, table_name),
            GetTableSnapshotResponse,
            self.rest_auth_function
        )
        if response is None:
            return None
        return response.get_snapshot()

    def list_partitions_paged(
            self,
            identifier: Identifier,
            max_results: Optional[int] = None,
            page_token: Optional[str] = None,
            partition_name_pattern: Optional[str] = None,
    ) -> PagedList[Partition]:
        database_name, table_name = self.__validate_identifier(identifier)

        response = self.client.get_with_params(
            self.resource_paths.partitions(database_name, table_name),
            self.__build_paged_query_params(
                max_results,
                page_token,
                {self.PARTITION_NAME_PATTERN: partition_name_pattern},
            ),
            ListPartitionsResponse,
            self.rest_auth_function,
        )

        partitions = response.data() or []
        return PagedList(partitions, response.get_next_page_token())

    # Tag CRUD wrappers — mirror Java RESTApi.java:1062-1123.
    def create_tag(
            self,
            identifier: Identifier,
            tag_name: str,
            snapshot_id: Optional[int] = None,
            time_retained: Optional[str] = None,
    ) -> None:
        database_name, table_name = self.__validate_identifier(identifier)
        request = CreateTagRequest(
            tag_name=tag_name,
            snapshot_id=snapshot_id,
            time_retained=time_retained,
        )
        self.client.post(
            self.resource_paths.tags(database_name, table_name),
            request,
            self.rest_auth_function,
        )

    def get_tag(self, identifier: Identifier, tag_name: str) -> GetTagResponse:
        database_name, table_name = self.__validate_identifier(identifier)
        return self.client.get(
            self.resource_paths.tag(database_name, table_name, tag_name),
            GetTagResponse,
            self.rest_auth_function,
        )

    def list_tags_paged(
            self,
            identifier: Identifier,
            max_results: Optional[int] = None,
            page_token: Optional[str] = None,
            tag_name_prefix: Optional[str] = None,
    ) -> PagedList[str]:
        database_name, table_name = self.__validate_identifier(identifier)
        response = self.client.get_with_params(
            self.resource_paths.tags(database_name, table_name),
            self.__build_paged_query_params(
                max_results,
                page_token,
                {self.TAG_NAME_PREFIX: tag_name_prefix},
            ),
            ListTagsResponse,
            self.rest_auth_function,
        )
        tags = response.data() or []
        return PagedList(tags, response.get_next_page_token())

    def delete_tag(self, identifier: Identifier, tag_name: str) -> None:
        database_name, table_name = self.__validate_identifier(identifier)
        self.client.delete(
            self.resource_paths.tag(database_name, table_name, tag_name),
            self.rest_auth_function,
        )

    @staticmethod
    def is_valid_function_name(name: str) -> bool:
        if not name:
            return False
        return RESTApi._FUNCTION_NAME_PATTERN.match(name) is not None

    @staticmethod
    def check_function_name(name: str) -> None:
        if not RESTApi.is_valid_function_name(name):
            raise IllegalArgumentError("Invalid function name: " + str(name))

    def list_functions(self, database_name: str) -> List[str]:
        return self.__list_data_from_page_api(
            lambda query_params: self.client.get_with_params(
                self.resource_paths.functions(database_name),
                query_params,
                ListFunctionsResponse,
                self.rest_auth_function,
            )
        )

    def list_functions_paged(
            self,
            database_name: str,
            max_results: Optional[int] = None,
            page_token: Optional[str] = None,
            function_name_pattern: Optional[str] = None,
    ) -> PagedList[str]:
        response = self.client.get_with_params(
            self.resource_paths.functions(database_name),
            self.__build_paged_query_params(
                max_results,
                page_token,
                {self.FUNCTION_NAME_PATTERN: function_name_pattern},
            ),
            ListFunctionsResponse,
            self.rest_auth_function,
        )
        functions = response.functions if response.functions else []
        return PagedList(functions, response.get_next_page_token())

    def list_function_details_paged(
            self,
            database_name: str,
            max_results: Optional[int] = None,
            page_token: Optional[str] = None,
            function_name_pattern: Optional[str] = None,
    ) -> PagedList[GetFunctionResponse]:
        response = self.client.get_with_params(
            self.resource_paths.function_details(database_name),
            self.__build_paged_query_params(
                max_results,
                page_token,
                {self.FUNCTION_NAME_PATTERN: function_name_pattern},
            ),
            ListFunctionDetailsResponse,
            self.rest_auth_function,
        )
        function_details = response.data() if response.data() else []
        return PagedList(function_details, response.get_next_page_token())

    def list_functions_paged_globally(
            self,
            database_name_pattern: Optional[str] = None,
            function_name_pattern: Optional[str] = None,
            max_results: Optional[int] = None,
            page_token: Optional[str] = None,
    ) -> PagedList:
        response = self.client.get_with_params(
            self.resource_paths.functions(),
            self.__build_paged_query_params(
                max_results,
                page_token,
                {
                    self.DATABASE_NAME_PATTERN: database_name_pattern,
                    self.FUNCTION_NAME_PATTERN: function_name_pattern,
                },
            ),
            ListFunctionsGloballyResponse,
            self.rest_auth_function,
        )
        functions = response.data() if response.data() else []
        return PagedList(functions, response.get_next_page_token())

    def get_function(self, identifier: Identifier) -> GetFunctionResponse:
        from pypaimon.api.rest_exception import NoSuchResourceException
        if not self.is_valid_function_name(identifier.get_object_name()):
            raise NoSuchResourceException(
                "FUNCTION",
                identifier.get_object_name(),
                "Invalid function name: " + identifier.get_object_name(),
            )
        return self.client.get(
            self.resource_paths.function(
                identifier.get_database_name(), identifier.get_object_name()),
            GetFunctionResponse,
            self.rest_auth_function,
        )

    def create_function(self, identifier: Identifier, function) -> None:
        self.check_function_name(identifier.get_object_name())
        request = CreateFunctionRequest(
            name=function.name(),
            input_params=function.input_params(),
            return_params=function.return_params(),
            deterministic=function.is_deterministic(),
            definitions=function.definitions(),
            comment=function.comment(),
            options=function.options(),
        )
        self.client.post(
            self.resource_paths.functions(identifier.get_database_name()),
            request,
            self.rest_auth_function,
        )

    def drop_function(self, identifier: Identifier) -> None:
        self.check_function_name(identifier.get_object_name())
        self.client.delete(
            self.resource_paths.function(
                identifier.get_database_name(), identifier.get_object_name()),
            self.rest_auth_function,
        )

    def alter_function(self, identifier: Identifier, changes: List) -> None:
        self.check_function_name(identifier.get_object_name())
        request = AlterFunctionRequest(changes=changes)
        self.client.post(
            self.resource_paths.function(
                identifier.get_database_name(), identifier.get_object_name()),
            request,
            self.rest_auth_function,
        )

    @staticmethod
    def __validate_identifier(identifier: Identifier):
        if not identifier:
            raise ValueError("Identifier cannot be None")

        database_name = identifier.get_database_name()
        if not database_name or not database_name.strip():
            raise ValueError("Database name cannot be empty")

        table_name = identifier.get_object_name()
        if not table_name or not table_name.strip():
            raise ValueError("Table name cannot be None")

        return database_name.strip(), table_name.strip()


from pypaimon.catalog.catalog_exception import IllegalArgumentError
