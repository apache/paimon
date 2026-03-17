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
from pypaimon.catalog.catalog_exception import (
    DatabaseAlreadyExistException,
    DatabaseNotExistException,
    TableAlreadyExistException,
    TableNotExistException
)
from pypaimon.catalog.database import Database
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.file_io import FileIO
from pypaimon.common.identifier import Identifier
from pypaimon.schema.schema_change import SchemaChange
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
        self.file_io = FileIO.get(self.warehouse, self.catalog_options)

    def list_databases(self) -> list:
        statuses = self.file_io.list_status(self.warehouse)
        database_names = []
        for status in statuses:
            import pyarrow.fs as pafs
            is_directory = hasattr(status, 'type') and status.type == pafs.FileType.Directory
            name = status.base_name if hasattr(status, 'base_name') else ""
            if is_directory and name and name.endswith(Catalog.DB_SUFFIX):
                database_names.append(name[:-len(Catalog.DB_SUFFIX)])
        return sorted(database_names)

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

    def drop_database(self, name: str, ignore_if_not_exists: bool = False, cascade: bool = False):
        try:
            self.get_database(name)
        except DatabaseNotExistException:
            if not ignore_if_not_exists:
                raise
            return

        db_path = self.get_database_path(name)

        if cascade:
            for table_name in self.list_tables(name):
                table_path = f"{db_path}/{table_name}"
                self.file_io.delete(table_path, True)

        # Check if database still has tables
        remaining_tables = self.list_tables(name)
        if remaining_tables and not cascade:
            raise ValueError(
                f"Database {name} is not empty. "
                f"Use cascade=True to drop all tables first."
            )

        self.file_io.delete(db_path, True)

    def list_tables(self, database_name: str) -> list:
        try:
            self.get_database(database_name)
        except DatabaseNotExistException:
            raise

        db_path = self.get_database_path(database_name)
        statuses = self.file_io.list_status(db_path)
        table_names = []
        for status in statuses:
            import pyarrow.fs as pafs
            is_directory = hasattr(status, 'type') and status.type == pafs.FileType.Directory
            name = status.base_name if hasattr(status, 'base_name') else ""
            if is_directory and name and not name.startswith("."):
                table_names.append(name)
        return sorted(table_names)

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

    def alter_table(
        self,
        identifier: Union[str, Identifier],
        changes: List[SchemaChange],
        ignore_if_not_exists: bool = False
    ):
        if not isinstance(identifier, Identifier):
            identifier = Identifier.from_string(identifier)
        try:
            self.get_table(identifier)
        except TableNotExistException:
            if not ignore_if_not_exists:
                raise
            return

        table_path = self.get_table_path(identifier)
        schema_manager = SchemaManager(self.file_io, table_path)
        try:
            schema_manager.commit_changes(changes)
        except Exception as e:
            raise RuntimeError(f"Failed to alter table {identifier.get_full_name()}: {e}") from e

    def rename_table(self, source_identifier: Union[str, Identifier], target_identifier: Union[str, Identifier]):
        if not isinstance(source_identifier, Identifier):
            source_identifier = Identifier.from_string(source_identifier)
        if not isinstance(target_identifier, Identifier):
            target_identifier = Identifier.from_string(target_identifier)

        # Verify source table exists
        try:
            self.get_table(source_identifier)
        except TableNotExistException:
            raise

        # Verify target database exists
        self.get_database(target_identifier.get_database_name())

        # Verify target table does not exist
        try:
            self.get_table(target_identifier)
            raise TableAlreadyExistException(target_identifier)
        except TableNotExistException:
            pass

        source_path = self.get_table_path(source_identifier)
        target_path = self.get_table_path(target_identifier)
        self.file_io.rename(source_path, target_path)

    def drop_table(self, identifier: Union[str, Identifier], ignore_if_not_exists: bool = False):
        if not isinstance(identifier, Identifier):
            identifier = Identifier.from_string(identifier)
        
        # Check if table exists
        try:
            self.get_table(identifier)
        except TableNotExistException:
            if not ignore_if_not_exists:
                raise
            return
        
        # Delete the table directory
        table_path = self.get_table_path(identifier)
        self.file_io.delete(table_path, True)

    def commit_snapshot(
            self,
            identifier: Identifier,
            table_uuid: Optional[str],
            snapshot: Snapshot,
            statistics: List[PartitionStatistics]
    ) -> bool:
        raise NotImplementedError("This catalog does not support commit catalog")

    def load_snapshot(self, identifier: Identifier):
        """Load the snapshot of table identified by the given Identifier.

        Args:
            identifier: Path of the table

        Raises:
            NotImplementedError: FileSystemCatalog does not support version management
        """
        raise NotImplementedError("Filesystem catalog does not support load_snapshot")

    def list_partitions_paged(
            self,
            identifier: Union[str, Identifier],
            max_results: Optional[int] = None,
            page_token: Optional[str] = None,
            partition_name_pattern: Optional[str] = None,
    ):
        from pypaimon.api.api_response import Partition, PagedList
        from pypaimon.manifest.manifest_list_manager import ManifestListManager
        from pypaimon.manifest.manifest_file_manager import ManifestFileManager

        if not isinstance(identifier, Identifier):
            identifier = Identifier.from_string(identifier)

        table = self.get_table(identifier)
        snapshot = table.snapshot_manager().get_latest_snapshot()
        if snapshot is None:
            return PagedList(elements=[])

        # Read all manifest entries (ADD - DELETE merged)
        manifest_list_manager = ManifestListManager(table)
        manifest_file_manager = ManifestFileManager(table)
        manifest_files = manifest_list_manager.read_all(snapshot)
        entries = manifest_file_manager.read_entries_parallel(manifest_files, drop_stats=True)

        # Group entries by partition spec
        partition_map = {}  # spec_key -> aggregated stats
        for entry in entries:
            spec = {field.name: str(v) for field, v in
                    zip(entry.partition.fields, entry.partition.values)}
            spec_key = tuple(sorted(spec.items()))

            if spec_key not in partition_map:
                partition_map[spec_key] = {
                    'spec': spec,
                    'record_count': 0,
                    'file_size_in_bytes': 0,
                    'file_count': 0,
                    'last_file_creation_time': 0,
                    'buckets': set(),
                }
            stats = partition_map[spec_key]
            stats['record_count'] += entry.file.row_count
            stats['file_size_in_bytes'] += entry.file.file_size
            stats['file_count'] += 1
            if entry.file.creation_time is not None:
                ct = entry.file.creation_time.get_millisecond()
                if ct > stats['last_file_creation_time']:
                    stats['last_file_creation_time'] = ct
            stats['buckets'].add(entry.bucket)

        # Convert to Partition objects
        partitions = []
        for stats in partition_map.values():
            partitions.append(Partition(
                spec=stats['spec'],
                record_count=stats['record_count'],
                file_size_in_bytes=stats['file_size_in_bytes'],
                file_count=stats['file_count'],
                last_file_creation_time=stats['last_file_creation_time'],
                total_buckets=len(stats['buckets']),
            ))

        # Apply pattern filter
        if partition_name_pattern:
            import re
            regex = re.compile(partition_name_pattern.replace('*', '.*'))
            partitions = [
                p for p in partitions
                if regex.fullmatch(','.join(f'{k}={v}' for k, v in p.spec.items()))
            ]

        return PagedList(elements=partitions)
