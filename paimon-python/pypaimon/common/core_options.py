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

from enum import Enum

from pypaimon.common.memory_size import MemorySize


class CoreOptions(str, Enum):
    """Core options for paimon."""

    def __str__(self):
        return self.value

    # Basic options
    AUTO_CREATE = "auto-create"
    PATH = "path"
    TYPE = "type"
    BRANCH = "branch"
    BUCKET = "bucket"
    BUCKET_KEY = "bucket-key"
    WAREHOUSE = "warehouse"
    SCAN_MANIFEST_PARALLELISM = "scan.manifest.parallelism"
    # File format options
    FILE_FORMAT = "file.format"
    FILE_FORMAT_ORC = "orc"
    FILE_FORMAT_AVRO = "avro"
    FILE_FORMAT_PARQUET = "parquet"
    FILE_FORMAT_BLOB = "blob"
    FILE_COMPRESSION = "file.compression"
    FILE_COMPRESSION_PER_LEVEL = "file.compression.per.level"
    FILE_FORMAT_PER_LEVEL = "file.format.per.level"
    FILE_BLOCK_SIZE = "file.block-size"
    FILE_BLOB_AS_DESCRIPTOR = "blob-as-descriptor"
    # Scan options
    SCAN_FALLBACK_BRANCH = "scan.fallback-branch"
    INCREMENTAL_BETWEEN_TIMESTAMP = "incremental-between-timestamp"
    SOURCE_SPLIT_TARGET_SIZE = "source.split.target-size"
    SOURCE_SPLIT_OPEN_FILE_COST = "source.split.open-file-cost"
    # Commit options
    COMMIT_USER_PREFIX = "commit.user-prefix"
    ROW_TRACKING_ENABLED = "row-tracking.enabled"
    DATA_EVOLUTION_ENABLED = "data-evolution.enabled"

    @staticmethod
    def get_blob_as_descriptor(options: dict) -> bool:
        return options.get(CoreOptions.FILE_BLOB_AS_DESCRIPTOR, "false").lower() == 'true'


def parse_memory_size(size_str: str) -> int:
    return MemorySize.parse_bytes(size_str)


def get_split_target_size(options: dict, default_bytes: int = 128 * 1024 * 1024) -> int:
    if CoreOptions.SOURCE_SPLIT_TARGET_SIZE in options:
        size_str = options[CoreOptions.SOURCE_SPLIT_TARGET_SIZE]
        if isinstance(size_str, int):
            return size_str
        elif isinstance(size_str, str):
            return parse_memory_size(size_str)
        else:
            return default_bytes
    return default_bytes


def get_split_open_file_cost(options: dict, default_bytes: int = 4 * 1024 * 1024) -> int:
    """
    Get split open file cost from options, default to 4MB.
    
    Args:
        options: Table options dictionary
        default_bytes: Default size in bytes (4MB)
    
    Returns:
        Open file cost in bytes
    """
    if CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST in options:
        cost_str = options[CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST]
        if isinstance(cost_str, int):
            return cost_str
        elif isinstance(cost_str, str):
            return parse_memory_size(cost_str)
        else:
            return default_bytes
    return default_bytes
