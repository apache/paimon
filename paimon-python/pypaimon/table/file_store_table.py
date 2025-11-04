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

from pathlib import Path
from typing import Optional

from pypaimon.catalog.catalog_environment import CatalogEnvironment
from pypaimon.common.core_options import CoreOptions
from pypaimon.common.file_io import FileIO
from pypaimon.common.identifier import Identifier
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
    def __init__(self, file_io: FileIO, identifier: Identifier, table_path: Path,
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

        self.options = table_schema.options
        self.cross_partition_update = self.table_schema.cross_partition_update()
        self.is_primary_key_table = bool(self.primary_keys)
        self.total_buckets = int(table_schema.options.get(CoreOptions.BUCKET, -1))

        self.schema_manager = SchemaManager(file_io, table_path)

    def current_branch(self) -> str:
        """Get the current branch name from options."""
        return self.options.get(CoreOptions.BRANCH, "main")

    def snapshot_manager(self):
        """Get the snapshot manager for this table."""
        from pypaimon.snapshot.snapshot_manager import SnapshotManager
        return SnapshotManager(self)

    def new_snapshot_commit(self):
        """Create a new SnapshotCommit instance using the catalog environment."""
        return self.catalog_environment.snapshot_commit(self.snapshot_manager())

    def bucket_mode(self) -> BucketMode:
        if self.is_primary_key_table:
            if int(self.options.get(CoreOptions.BUCKET, -1)) == -2:
                return BucketMode.POSTPONE_MODE
            elif int(self.options.get(CoreOptions.BUCKET, -1)) == -1:
                if self.cross_partition_update:
                    return BucketMode.CROSS_PARTITION
                else:
                    return BucketMode.HASH_DYNAMIC
            else:
                return BucketMode.HASH_FIXED
        else:
            if int(self.options.get(CoreOptions.BUCKET, -1)) == -1:
                return BucketMode.BUCKET_UNAWARE
            else:
                return BucketMode.HASH_FIXED

    def new_read_builder(self) -> 'ReadBuilder':
        return ReadBuilder(self)

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
        if CoreOptions.BUCKET in options and options.get(CoreOptions.BUCKET) != self.options.get(CoreOptions.BUCKET):
            raise ValueError("Cannot change bucket number")
        new_options = self.options.copy()
        for k, v in options.items():
            if v is None:
                new_options.pop(k)
            else:
                new_options[k] = v
        new_table_schema = self.table_schema.copy(new_options=new_options)
        return FileStoreTable(self.file_io, self.identifier, self.table_path, new_table_schema,
                              self.catalog_environment)

    def add_options(self, options: dict):
        for key, value in options.items():
            self.options[key] = value
