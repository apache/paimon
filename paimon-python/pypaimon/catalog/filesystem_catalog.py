################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
#################################################################################

from typing import List, Optional, Union

from pypaimon.catalog.catalog import Catalog
from pypaimon.catalog.catalog_environment import CatalogEnvironment
from pypaimon.catalog.catalog_exception import (DatabaseAlreadyExistException,
                                                DatabaseNotExistException,
                                                TableAlreadyExistException,
                                                TableNotExistException)
from pypaimon.catalog.database import Database
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.file_io import FileIO
from pypaimon.common.identifier import Identifier
from pypaimon.schema.schema_manager import SchemaManager
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_commit import PartitionStatistics
from pypaimon.table.file_store_table import FileStoreTable
from pypaimon.table.table import Table


class FileSystemCatalog(Catalog):
    def __init__(self, catalog_options: Options):
        if not catalog_options.contains(CatalogOptions.WAREHOUSE):
            raise ValueError(f"Paimon '{CatalogOptions.WAREHOUSE.key()}' path must be set")
        self.warehouse = catalog_options.get(CatalogOptions.WAREHOUSE)
        self.catalog_options = catalog_options
        self.file_io = FileIO(self.warehouse, self.catalog_options)

    def get_database(self, name: str) -> Database:
        if self.file_io.exists(self.get_database_path(name)):
            return Database(name, {})
        else:
            raise DatabaseNotExistException(name)

    def create_database(self, name: str, ignore_if_exists: bool, properties: Optional[dict] = None):
        try:
            self.get_database(name)
            if not ignore_if_exists:
                raise DatabaseAlreadyExistException(name)
        except DatabaseNotExistException:
            if properties and Catalog.DB_LOCATION_PROP in properties:
                raise ValueError("Cannot specify location for a database when using fileSystem catalog.")
            path = self.get_database_path(name)
            self.file_io.mkdirs(path)

    def get_table(self, identifier: Union[str, Identifier]) -> Table:
        if not isinstance(identifier, Identifier):
            identifier = Identifier.from_string(identifier)
        if self.catalog_options.contains(CoreOptions.SCAN_FALLBACK_BRANCH):
            raise ValueError(f"Unsupported CoreOption {CoreOptions.SCAN_FALLBACK_BRANCH}")
        table_path = self.get_table_path(identifier)
        table_schema = self.get_table_schema(identifier)

        # Create catalog environment for filesystem catalog
        # Filesystem catalog doesn't support version management by default
        catalog_environment = CatalogEnvironment(
            identifier=identifier,
            uuid=None,  # Filesystem catalog doesn't track table UUIDs
            catalog_loader=None,  # No catalog loader for filesystem
            supports_version_management=False
        )

        return FileStoreTable(self.file_io, identifier, table_path, table_schema, catalog_environment)

    def create_table(self, identifier: Union[str, Identifier], schema: 'Schema', ignore_if_exists: bool):
        if schema.options and schema.options.get(CoreOptions.AUTO_CREATE.key()):
            raise ValueError(f"The value of {CoreOptions.AUTO_CREATE.key()} property should be False.")

        if not isinstance(identifier, Identifier):
            identifier = Identifier.from_string(identifier)
        self.get_database(identifier.get_database_name())
        try:
            self.get_table(identifier)
            if not ignore_if_exists:
                raise TableAlreadyExistException(identifier)
        except TableNotExistException:
            if schema.options and CoreOptions.TYPE.key() in schema.options and schema.options.get(
                    CoreOptions.TYPE.key()) != "table":
                raise ValueError(f"Table Type: {schema.options.get(CoreOptions.TYPE.key())}")
            table_path = self.get_table_path(identifier)
            schema_manager = SchemaManager(self.file_io, table_path)
            schema_manager.create_table(schema)

    def get_table_schema(self, identifier: Identifier):
        table_path = self.get_table_path(identifier)
        table_schema = SchemaManager(self.file_io, table_path).latest()
        if table_schema is None:
            raise TableNotExistException(identifier)
        return table_schema

    def get_database_path(self, name) -> str:
        warehouse = self.warehouse.rstrip('/')
        return f"{warehouse}/{name}{Catalog.DB_SUFFIX}"

    def get_table_path(self, identifier: Identifier) -> str:
        db_path = self.get_database_path(identifier.get_database_name())
        return f"{db_path}/{identifier.get_table_name()}"

    def commit_snapshot(
            self,
            identifier: Identifier,
            table_uuid: Optional[str],
            snapshot: Snapshot,
            statistics: List[PartitionStatistics]
    ) -> bool:
        raise NotImplementedError("This catalog does not support commit catalog")
