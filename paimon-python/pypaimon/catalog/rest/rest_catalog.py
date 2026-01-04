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
from typing import Any, Callable, Dict, List, Optional, Union

from pypaimon.api.api_response import GetTableResponse, PagedList
from pypaimon.api.rest_api import RESTApi
from pypaimon.api.rest_exception import NoSuchResourceException, AlreadyExistsException
from pypaimon.catalog.catalog import Catalog
from pypaimon.catalog.catalog_context import CatalogContext
from pypaimon.catalog.catalog_environment import CatalogEnvironment
from pypaimon.catalog.catalog_exception import TableNotExistException, DatabaseAlreadyExistException, \
    TableAlreadyExistException, DatabaseNotExistException
from pypaimon.catalog.database import Database
from pypaimon.catalog.rest.property_change import PropertyChange
from pypaimon.catalog.rest.rest_token_file_io import RESTTokenFileIO
from pypaimon.catalog.rest.table_metadata import TableMetadata
from pypaimon.common.options.config import CatalogOptions
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.file_io import FileIO
from pypaimon.common.identifier import Identifier
from pypaimon.schema.schema import Schema
from pypaimon.schema.table_schema import TableSchema
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_commit import PartitionStatistics
from pypaimon.table.file_store_table import FileStoreTable


class RESTCatalog(Catalog):
    def __init__(self, context: CatalogContext, config_required: Optional[bool] = True):
        self.warehouse = context.options.get(CatalogOptions.WAREHOUSE)
        self.rest_api = RESTApi(context.options, config_required)
        self.context = CatalogContext.create(self.rest_api.options, context.hadoop_conf,
                                             context.prefer_io_loader, context.fallback_io_loader)
        self.data_token_enabled = self.rest_api.options.get(CatalogOptions.DATA_TOKEN_ENABLED)

    def catalog_loader(self):
        """
        Create and return a RESTCatalogLoader for this catalog.

        Returns:
            A RESTCatalogLoader instance configured with this catalog's context
        """
        from pypaimon.catalog.rest.rest_catalog_loader import RESTCatalogLoader
        return RESTCatalogLoader(self.context)

    def supports_version_management(self) -> bool:
        """
        Return whether this catalog supports version management for tables.

        Returns:
            True since REST catalogs support version management
        """
        return True

    def commit_snapshot(
            self,
            identifier: Identifier,
            table_uuid: Optional[str],
            snapshot: Snapshot,
            statistics: List[PartitionStatistics]
    ) -> bool:
        """
        Commit the Snapshot for table identified by the given Identifier.

        Args:
            identifier: Path of the table
            table_uuid: UUID of the table to avoid wrong commit
            snapshot: Snapshot to be committed
            statistics: Statistics information of this change

        Returns:
            True if commit was successful, False otherwise

        Raises:
            TableNotExistException: If the target table does not exist
        """
        try:
            return self.rest_api.commit_snapshot(identifier, table_uuid, snapshot, statistics)
        except NoSuchResourceException as e:
            raise TableNotExistException(identifier) from e
        except Exception as e:
            # Handle other exceptions that might be thrown by the API
            raise RuntimeError(f"Failed to commit snapshot for table {identifier.get_full_name()}: {e}") from e

    def list_databases(self) -> List[str]:
        return self.rest_api.list_databases()

    def list_databases_paged(self, max_results: Optional[int] = None, page_token: Optional[str] = None,
                             database_name_pattern: Optional[str] = None) -> PagedList[str]:
        return self.rest_api.list_databases_paged(max_results, page_token, database_name_pattern)

    def create_database(self, name: str, ignore_if_exists: bool, properties: Dict[str, str] = None):
        try:
            self.rest_api.create_database(name, properties)
        except AlreadyExistsException as e:
            if not ignore_if_exists:
                # Convert REST API exception to catalog exception
                raise DatabaseAlreadyExistException(name) from e

    def get_database(self, name: str) -> Database:
        response = self.rest_api.get_database(name)
        options = response.options
        options[Catalog.DB_LOCATION_PROP] = response.location
        response.put_audit_options_to(options)
        if response is not None:
            return Database(name, options)

    def drop_database(self, name: str, ignore_if_not_exists: bool = False):
        try:
            self.rest_api.drop_database(name)
        except NoSuchResourceException as e:
            if not ignore_if_not_exists:
                # Convert REST API exception to catalog exception
                raise DatabaseNotExistException(name) from e

    def alter_database(self, name: str, changes: List[PropertyChange]):
        set_properties, remove_keys = PropertyChange.get_set_properties_to_remove_keys(changes)
        self.rest_api.alter_database(name, list(remove_keys), set_properties)

    def list_tables(self, database_name: str) -> List[str]:
        return self.rest_api.list_tables(database_name)

    def list_tables_paged(
            self,
            database_name: str,
            max_results: Optional[int] = None,
            page_token: Optional[str] = None,
            table_name_pattern: Optional[str] = None
    ) -> PagedList[str]:
        return self.rest_api.list_tables_paged(
            database_name,
            max_results,
            page_token,
            table_name_pattern
        )

    def get_table(self, identifier: Union[str, Identifier]) -> FileStoreTable:
        if not isinstance(identifier, Identifier):
            identifier = Identifier.from_string(identifier)
        return self.load_table(
            identifier,
            lambda path: self.file_io_for_data(path, identifier),
            self.file_io_from_options,
            self.load_table_metadata,
        )

    def create_table(self, identifier: Union[str, Identifier], schema: Schema, ignore_if_exists: bool):
        if not isinstance(identifier, Identifier):
            identifier = Identifier.from_string(identifier)
        try:
            self.rest_api.create_table(identifier, schema)
        except AlreadyExistsException as e:
            if not ignore_if_exists:
                raise TableAlreadyExistException(identifier) from e

    def drop_table(self, identifier: Union[str, Identifier], ignore_if_not_exists: bool = False):
        if not isinstance(identifier, Identifier):
            identifier = Identifier.from_string(identifier)
        try:
            self.rest_api.drop_table(identifier)
        except NoSuchResourceException as e:
            if not ignore_if_not_exists:
                raise TableNotExistException(identifier) from e

    def load_table_metadata(self, identifier: Identifier) -> TableMetadata:
        response = self.rest_api.get_table(identifier)
        return self.to_table_metadata(identifier.get_database_name(), response)

    def to_table_metadata(self, db: str, response: GetTableResponse) -> TableMetadata:
        schema = TableSchema.from_schema(response.schema_id, response.get_schema())
        options: Dict[str, str] = dict(schema.options)
        options[CoreOptions.PATH.key()] = response.get_path()
        response.put_audit_options_to(options)

        identifier = Identifier.create(db, response.get_name())
        if identifier.get_branch_name() is not None:
            options[CoreOptions.BRANCH.key()] = identifier.get_branch_name()

        return TableMetadata(
            schema=schema.copy(options),
            is_external=response.get_is_external(),
            uuid=response.get_id()
        )

    def file_io_from_options(self, table_path: str) -> FileIO:
        return FileIO(table_path, self.context.options)

    def file_io_for_data(self, table_path: str, identifier: Identifier):
        return RESTTokenFileIO(identifier, table_path, self.context.options) \
            if self.data_token_enabled else self.file_io_from_options(table_path)

    def load_table(self,
                   identifier: Identifier,
                   internal_file_io: Callable[[str], Any],
                   external_file_io: Callable[[str], Any],
                   metadata_loader: Callable[[Identifier], TableMetadata],
                   ) -> FileStoreTable:
        metadata = metadata_loader(identifier)
        schema = metadata.schema
        data_file_io = external_file_io if metadata.is_external else internal_file_io
        catalog_env = CatalogEnvironment(
            identifier=identifier,
            uuid=metadata.uuid,
            catalog_loader=self.catalog_loader(),
            supports_version_management=True  # REST catalogs support version management
        )
        # Use the path from server response directly (do not trim scheme)
        table_path = schema.options.get(CoreOptions.PATH.key())
        table = self.create(data_file_io(table_path),
                            table_path,
                            schema,
                            catalog_env)
        return table

    @staticmethod
    def create(file_io: FileIO,
               table_path: str,
               table_schema: TableSchema,
               catalog_environment: CatalogEnvironment
               ) -> FileStoreTable:
        """Create FileStoreTable with dynamic options and catalog environment"""
        return FileStoreTable(file_io, catalog_environment.identifier, table_path, table_schema, catalog_environment)
