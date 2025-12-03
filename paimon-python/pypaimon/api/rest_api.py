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
from typing import Callable, Dict, List, Optional

from pypaimon.api.api_request import (AlterDatabaseRequest, CommitTableRequest,
                                      CreateDatabaseRequest,
                                      CreateTableRequest, RenameTableRequest)
from pypaimon.api.api_response import (CommitTableResponse, ConfigResponse,
                                       GetDatabaseResponse, GetTableResponse,
                                       GetTableTokenResponse,
                                       ListDatabasesResponse,
                                       ListTablesResponse, PagedList,
                                       PagedResponse)
from pypaimon.api.auth import AuthProviderFactory, RESTAuthFunction
from pypaimon.api.client import HttpClient
from pypaimon.api.resource_paths import ResourcePaths
from pypaimon.api.rest_util import RESTUtil
from pypaimon.api.typedef import T
from pypaimon.common.config import CatalogOptions
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
    TOKEN_EXPIRATION_SAFE_TIME_MILLIS = 3_600_000

    def __init__(self, options: Dict[str, str], config_required: bool = True):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.client = HttpClient(options.get(CatalogOptions.URI))
        auth_provider = AuthProviderFactory.create_auth_provider(options)
        base_headers = RESTUtil.extract_prefix_map(options, self.HEADER_PREFIX)

        if config_required:
            warehouse = options.get(CatalogOptions.WAREHOUSE)
            query_params = {}
            if warehouse:
                query_params[CatalogOptions.WAREHOUSE] = RESTUtil.encode_string(
                    warehouse)

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
            max_results: Optional[int],
            page_token: Optional[str],
            name_patterns: Dict[str, str],
    ) -> Dict[str, str]:
        query_params = {}
        if max_results is not None and max_results > 0:
            query_params[RESTApi.MAX_RESULTS] = str(max_results)

        if page_token is not None and page_token.strip():
            query_params[RESTApi.PAGE_TOKEN] = page_token

        for key, value in name_patterns:
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

    def get_options(self) -> Dict[str, str]:
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

    def create_database(self, name: str, options: Dict[str, str]) -> None:
        request = CreateDatabaseRequest(name, options)
        self.client.post(
            self.resource_paths.databases(), request, self.rest_auth_function
        )

    def get_database(self, name: str) -> GetDatabaseResponse:
        return self.client.get(
            self.resource_paths.database(name),
            GetDatabaseResponse,
            self.rest_auth_function,
        )

    def drop_database(self, name: str) -> None:
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
    ) -> PagedList[str]:
        response = self.client.get_with_params(
            self.resource_paths.tables(database_name),
            self.__build_paged_query_params(
                max_results, page_token, {self.TABLE_NAME_PATTERN: table_name_pattern}
            ),
            ListTablesResponse,
            self.rest_auth_function,
        )

        tables = response.data() or []
        return PagedList(tables, response.get_next_page_token())

    def create_table(self, identifier: Identifier, schema: Schema) -> None:
        request = CreateTableRequest(identifier, schema)
        return self.client.post(
            self.resource_paths.tables(identifier.get_database_name()),
            request,
            self.rest_auth_function)

    def get_table(self, identifier: Identifier) -> GetTableResponse:
        return self.client.get(
            self.resource_paths.table(
                identifier.get_database_name(),
                identifier.get_object_name()),
            GetTableResponse,
            self.rest_auth_function,
        )

    def drop_table(self, identifier: Identifier) -> GetTableResponse:
        return self.client.delete(
            self.resource_paths.table(
                identifier.get_database_name(),
                identifier.get_object_name()),
            self.rest_auth_function,
        )

    def rename_table(self, source_identifier: Identifier, target_identifier: Identifier) -> None:
        request = RenameTableRequest(source_identifier, target_identifier)
        return self.client.post(
            self.resource_paths.rename_table(),
            request,
            self.rest_auth_function)

    def load_table_token(self, identifier: Identifier) -> GetTableTokenResponse:
        return self.client.get(
            self.resource_paths.table_token(
                identifier.get_database_name(),
                identifier.get_object_name()),
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
        request = CommitTableRequest(table_uuid, snapshot, statistics)
        response = self.client.post_with_response_type(
            self.resource_paths.commit_table(
                identifier.get_database_name(), identifier.get_object_name()),
            request,
            CommitTableResponse,
            self.rest_auth_function
        )
        return response.is_success()
