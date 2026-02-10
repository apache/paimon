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
################################################################################

from typing import List, Optional

from pypaimon.catalog.catalog_environment import CatalogEnvironment
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.file_io import FileIO
from pypaimon.common.identifier import Identifier
from pypaimon.common.options.options import Options
from pypaimon.read.read_builder import ReadBuilder
from pypaimon.schema.schema_manager import SchemaManager
from pypaimon.schema.table_schema import TableSchema
from pypaimon.table.bucket_mode import BucketMode
from pypaimon.table.table import Table
from pypaimon.write.write_builder import BatchWriteBuilder, StreamWriteBuilder
from pypaimon.write.row_key_extractor import (DynamicBucketRowKeyExtractor,
                                              FixedBucketRowKeyExtractor,
                                              PostponeBucketRowKeyExtractor,
                                              RowKeyExtractor,
                                              UnawareBucketRowKeyExtractor)


class FileStoreTable(Table):
    def __init__(self, file_io: FileIO, identifier: Identifier, table_path: str,
                 table_schema: TableSchema, catalog_environment: Optional[CatalogEnvironment] = None):
        self.file_io = file_io
        self.identifier = identifier
        self.table_path = table_path
        self.catalog_environment = catalog_environment or CatalogEnvironment.empty()

        self.table_schema = table_schema
        self.fields = table_schema.fields
        self.field_names = [field.name for field in table_schema.fields]
        self.field_dict = {field.name: field for field in self.fields}
        self.primary_keys = table_schema.primary_keys
        self.primary_keys_fields = [self.field_dict[name] for name in self.primary_keys]
        self.partition_keys = table_schema.partition_keys
        self.partition_keys_fields = [self.field_dict[name] for name in self.partition_keys]
        self.trimmed_primary_keys = [pk for pk in self.primary_keys if pk not in self.partition_keys]
        self.trimmed_primary_keys_fields = [self.field_dict[name] for name in self.trimmed_primary_keys]

        self.options = CoreOptions(Options(table_schema.options))
        self.cross_partition_update = self.table_schema.cross_partition_update()
        self.is_primary_key_table = bool(self.primary_keys)
        self.total_buckets = self.options.bucket()

        self.schema_manager = SchemaManager(file_io, table_path)

    @classmethod
    def from_path(cls, table_path: str) -> 'FileStoreTable':
        """
        Create a FileStoreTable from a table path.
        This is useful for reading tables created by Java without going through a catalog.
        """
        file_io = FileIO(table_path, Options({}))
        schema_manager = SchemaManager(file_io, table_path)
        table_schema = schema_manager.latest()

        if table_schema is None:
            raise ValueError(f"No schema found at path: {table_path}")

        # Create a placeholder identifier
        identifier = Identifier("default", "table")

        return cls(file_io, identifier, table_path, table_schema)

    def current_branch(self) -> str:
        """Get the current branch name from options."""
        return self.options.branch()

    def snapshot_manager(self):
        """Get the snapshot manager for this table."""
        from pypaimon.snapshot.snapshot_manager import SnapshotManager
        return SnapshotManager(self)

    def tag_manager(self):
        """Get the tag manager for this table."""
        from pypaimon import TagManager
        return TagManager(self.file_io, self.table_path, self.current_branch())

    def create_tag(
            self,
            tag_name: str,
            snapshot_id: Optional[int] = None,
            ignore_if_exists: bool = False
    ) -> None:
        """
        Create a tag for a snapshot.
        
        Args:
            tag_name: Name for the tag
            snapshot_id: ID of the snapshot to tag. If None, uses the latest snapshot.
            ignore_if_exists: If True, don't raise error if tag already exists
            
        Raises:
            ValueError: If no snapshot exists or tag already exists (when ignore_if_exists=False)
        """

        snapshot_mgr = self.snapshot_manager()

        if snapshot_id is not None:
            snapshot = snapshot_mgr.get_snapshot_by_id(snapshot_id)
            if snapshot is None:
                raise ValueError(f"Snapshot with id {snapshot_id} doesn't exist.")
        else:
            snapshot = snapshot_mgr.get_latest_snapshot()
            if snapshot is None:
                raise ValueError("No snapshot exists in this table.")

        tag_mgr = self.tag_manager()
        tag_mgr.create_tag(snapshot, tag_name, ignore_if_exists)

    def delete_tag(self, tag_name: str) -> bool:
        """
        Delete a tag.
        
        Args:
            tag_name: Name of the tag to delete
            
        Returns:
            True if tag was deleted, False if tag didn't exist
        """
        tag_mgr = self.tag_manager()
        return tag_mgr.delete_tag(tag_name)

    def path_factory(self) -> 'FileStorePathFactory':
        from pypaimon.utils.file_store_path_factory import FileStorePathFactory

        # Get external paths
        external_paths = self._create_external_paths()

        # Get format identifier
        format_identifier = self.options.file_format()

        file_compression = self.options.file_compression()

        return FileStorePathFactory(
            root=str(self.table_path),
            partition_keys=self.partition_keys,
            default_part_value="__DEFAULT_PARTITION__",
            format_identifier=format_identifier,
            data_file_prefix="data-",
            changelog_file_prefix="changelog-",
            legacy_partition_name=True,
            file_suffix_include_compression=False,
            file_compression=file_compression,
            data_file_path_directory=None,
            external_paths=external_paths,
            index_file_in_data_file_dir=False,
        )

    def new_snapshot_commit(self):
        """Create a new SnapshotCommit instance using the catalog environment."""
        return self.catalog_environment.snapshot_commit(self.snapshot_manager())

    def bucket_mode(self) -> BucketMode:
        if self.is_primary_key_table:
            if self.options.bucket() == -2:
                return BucketMode.POSTPONE_MODE
            elif self.options.bucket() == -1:
                if self.cross_partition_update:
                    return BucketMode.CROSS_PARTITION
                else:
                    return BucketMode.HASH_DYNAMIC
            else:
                return BucketMode.HASH_FIXED
        else:
            if self.options.bucket() == -1:
                return BucketMode.BUCKET_UNAWARE
            else:
                return BucketMode.HASH_FIXED

    def new_read_builder(self) -> 'ReadBuilder':
        return ReadBuilder(self)

    def new_global_index_scan_builder(self) -> Optional['GlobalIndexScanBuilder']:
        if not self.options.global_index_enabled():
            return None

        from pypaimon.globalindex.global_index_scan_builder_impl import (
            GlobalIndexScanBuilderImpl
        )

        from pypaimon.index.index_file_handler import IndexFileHandler

        return GlobalIndexScanBuilderImpl(
            options=self.table_schema.options,
            row_type=self.fields,
            file_io=self.file_io,
            index_path_factory=self.path_factory().global_index_path_factory(),
            snapshot_manager=self.snapshot_manager(),
            index_file_handler=IndexFileHandler(table=self)
        )

    def new_batch_write_builder(self) -> BatchWriteBuilder:
        return BatchWriteBuilder(self)

    def new_stream_write_builder(self) -> StreamWriteBuilder:
        return StreamWriteBuilder(self)

    def create_row_key_extractor(self) -> RowKeyExtractor:
        bucket_mode = self.bucket_mode()
        if bucket_mode == BucketMode.HASH_FIXED:
            return FixedBucketRowKeyExtractor(self.table_schema)
        elif bucket_mode == BucketMode.BUCKET_UNAWARE:
            return UnawareBucketRowKeyExtractor(self.table_schema)
        elif bucket_mode == BucketMode.POSTPONE_MODE:
            return PostponeBucketRowKeyExtractor(self.table_schema)
        elif bucket_mode == BucketMode.HASH_DYNAMIC or bucket_mode == BucketMode.CROSS_PARTITION:
            return DynamicBucketRowKeyExtractor(self.table_schema)
        else:
            raise ValueError(f"Unsupported bucket mode: {bucket_mode}")

    def copy(self, options: dict) -> 'FileStoreTable':
        if CoreOptions.BUCKET.key() in options and int(options.get(CoreOptions.BUCKET.key())) != self.options.bucket():
            raise ValueError("Cannot change bucket number")
        new_options = CoreOptions.copy(self.options).options.to_map()
        for k, v in options.items():
            if v is None:
                new_options.pop(k)
            else:
                new_options[k] = v

        new_table_schema = self.table_schema.copy(new_options=new_options)

        time_travel_schema = self._try_time_travel(Options(new_options))
        if time_travel_schema is not None:
            new_table_schema = time_travel_schema

        return FileStoreTable(self.file_io, self.identifier, self.table_path, new_table_schema,
                              self.catalog_environment)

    def _try_time_travel(self, options: Options) -> Optional[TableSchema]:
        """
        Try to resolve time travel options and return the corresponding schema.
        
        Supports the following time travel options:
        - scan.tag-name: Travel to a specific tag
        
        Returns:
            The TableSchema at the time travel point, or None if no time travel option is set.
        """

        try:
            from pypaimon.snapshot.time_travel_util import TimeTravelUtil
            snapshot = TimeTravelUtil.try_travel_to_snapshot(options, self.tag_manager())
            if snapshot is None:
                return None
            return self.schema_manager.get_schema(snapshot.schema_id).copy(new_options=options.to_map())
        except Exception:
            return None

    def _create_external_paths(self) -> List[str]:
        from urllib.parse import urlparse
        from pypaimon.common.options.core_options import ExternalPathStrategy

        external_paths_str = self.options.data_file_external_paths()
        if not external_paths_str:
            return []

        strategy = self.options.data_file_external_paths_strategy()
        if strategy == ExternalPathStrategy.NONE:
            return []

        specific_fs = self.options.data_file_external_paths_specific_fs()

        paths = []
        for path_string in external_paths_str:
            if not path_string:
                continue

            # Parse and validate path
            parsed = urlparse(path_string)
            scheme = parsed.scheme
            if not scheme:
                raise ValueError(
                    f"External path must have a scheme (e.g., oss://, s3://, file://): {path_string}"
                )

            # Filter by specific filesystem if strategy is specific-fs
            if strategy == ExternalPathStrategy.SPECIFIC_FS:
                if not specific_fs:
                    raise ValueError(
                        f"data-file.external-paths.specific-fs must be set when "
                        f"strategy is {ExternalPathStrategy.SPECIFIC_FS}"
                    )
                if scheme.lower() != specific_fs.lower():
                    continue  # Skip paths that don't match the specific filesystem

            paths.append(path_string)

        if not paths:
            raise ValueError("No valid external paths found after filtering")

        return paths
