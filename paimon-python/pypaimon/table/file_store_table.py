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

from pypaimon.common.core_options import CoreOptions
from pypaimon.common.file_io import FileIO
from pypaimon.common.identifier import Identifier
from pypaimon.read.read_builder import ReadBuilder
from pypaimon.schema.schema_manager import SchemaManager
from pypaimon.schema.table_schema import TableSchema
from pypaimon.table.bucket_mode import BucketMode
from pypaimon.table.table import Table
from pypaimon.write.batch_write_builder import BatchWriteBuilder
from pypaimon.write.row_key_extractor import (FixedBucketRowKeyExtractor,
                                              RowKeyExtractor,
                                              UnawareBucketRowKeyExtractor)


class FileStoreTable(Table):
    def __init__(self, file_io: FileIO, identifier: Identifier, table_path: Path,
                 table_schema: TableSchema):
        self.file_io = file_io
        self.identifier = identifier
        self.table_path = table_path

        self.table_schema = table_schema
        self.fields = table_schema.fields
        self.primary_keys = table_schema.primary_keys
        self.partition_keys = table_schema.partition_keys
        self.options = table_schema.options

        self.schema_manager = SchemaManager(file_io, table_path)
        self.is_primary_key_table = bool(self.primary_keys)

    def bucket_mode(self) -> BucketMode:
        if self.is_primary_key_table:
            if self.primary_keys == self.partition_keys:
                return BucketMode.CROSS_PARTITION
            if self.options.get(CoreOptions.BUCKET, -1) == -1:
                return BucketMode.HASH_DYNAMIC
            else:
                return BucketMode.HASH_FIXED
        else:
            if self.options.get(CoreOptions.BUCKET, -1) == -1:
                return BucketMode.BUCKET_UNAWARE
            else:
                return BucketMode.HASH_FIXED

    def new_read_builder(self) -> 'ReadBuilder':
        return ReadBuilder(self)

    def new_batch_write_builder(self) -> BatchWriteBuilder:
        return BatchWriteBuilder(self)

    def create_row_key_extractor(self) -> RowKeyExtractor:
        bucket_mode = self.bucket_mode()
        if bucket_mode == BucketMode.HASH_FIXED:
            return FixedBucketRowKeyExtractor(self.table_schema)
        elif bucket_mode == BucketMode.BUCKET_UNAWARE:
            return UnawareBucketRowKeyExtractor(self.table_schema)
        elif bucket_mode == BucketMode.HASH_DYNAMIC or bucket_mode == BucketMode.CROSS_PARTITION:
            raise ValueError(f"Unsupported bucket mode {bucket_mode} yet")
        else:
            raise ValueError(f"Unsupported bucket mode: {bucket_mode}")
