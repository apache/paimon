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
from typing import List, Optional

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
    TARGET_FILE_SIZE = "target-file-size"
    BLOB_TARGET_FILE_SIZE = "blob.target-file-size"
    DATA_FILE_PREFIX = "data-file.prefix"
    # Scan options
    SCAN_FALLBACK_BRANCH = "scan.fallback-branch"
    INCREMENTAL_BETWEEN_TIMESTAMP = "incremental-between-timestamp"
    SOURCE_SPLIT_TARGET_SIZE = "source.split.target-size"
    SOURCE_SPLIT_OPEN_FILE_COST = "source.split.open-file-cost"
    # Commit options
    COMMIT_USER_PREFIX = "commit.user-prefix"
    ROW_TRACKING_ENABLED = "row-tracking.enabled"
    DATA_EVOLUTION_ENABLED = "data-evolution.enabled"
    # External paths options
    DATA_FILE_EXTERNAL_PATHS = "data-file.external-paths"
    DATA_FILE_EXTERNAL_PATHS_STRATEGY = "data-file.external-paths.strategy"
    DATA_FILE_EXTERNAL_PATHS_SPECIFIC_FS = "data-file.external-paths.specific-fs"

    @staticmethod
    def data_file_prefix(options: dict) -> str:
        return options.get(CoreOptions.DATA_FILE_PREFIX, "data-")

    @staticmethod
    def blob_as_descriptor(options: dict) -> bool:
        return options.get(CoreOptions.FILE_BLOB_AS_DESCRIPTOR, "false").lower() == 'true'

    @staticmethod
    def split_target_size(options: dict) -> int:
        """Get split target size from options, default to 128MB."""
        if CoreOptions.SOURCE_SPLIT_TARGET_SIZE in options:
            size_str = options[CoreOptions.SOURCE_SPLIT_TARGET_SIZE]
            return MemorySize.parse(size_str).get_bytes()
        return MemorySize.of_mebi_bytes(128).get_bytes()

    @staticmethod
    def split_open_file_cost(options: dict) -> int:
        """Get split open file cost from options, default to 4MB."""
        if CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST in options:
            cost_str = options[CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST]
            return MemorySize.parse(cost_str).get_bytes()
        return MemorySize.of_mebi_bytes(4).get_bytes()

    @staticmethod
    def target_file_size(options: dict, has_primary_key: bool = False) -> int:
        """Get target file size from options, default to 128MB for primary key table, 256MB for append-only table."""
        if CoreOptions.TARGET_FILE_SIZE in options:
            size_str = options[CoreOptions.TARGET_FILE_SIZE]
            return MemorySize.parse(size_str).get_bytes()
        return MemorySize.of_mebi_bytes(128 if has_primary_key else 256).get_bytes()

    @staticmethod
    def blob_target_file_size(options: dict) -> int:
        """Get blob target file size from options, default to target-file-size (256MB for append-only table)."""
        if CoreOptions.BLOB_TARGET_FILE_SIZE in options:
            size_str = options[CoreOptions.BLOB_TARGET_FILE_SIZE]
            return MemorySize.parse(size_str).get_bytes()
        return CoreOptions.target_file_size(options, has_primary_key=False)

    @staticmethod
    def data_file_external_paths(options: dict) -> Optional[List[str]]:
        external_paths_str = options.get(CoreOptions.DATA_FILE_EXTERNAL_PATHS)
        if not external_paths_str:
            return None
        return [path.strip() for path in external_paths_str.split(",") if path.strip()]

    @staticmethod
    def external_path_strategy(options: dict) -> 'ExternalPathStrategy':
        strategy_value = options.get(CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY, "none")
        if strategy_value is None:
            strategy_value = "none"

        strategy_str = strategy_value.lower() if isinstance(strategy_value, str) else str(strategy_value).lower()

        try:
            return ExternalPathStrategy(strategy_str)
        except ValueError:
            valid_values = [e.value for e in ExternalPathStrategy]
            raise ValueError(
                f"Could not parse value '{strategy_value}' for key '{CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY}'. "
                f"Expected one of: {valid_values}"
            )

    @staticmethod
    def external_specific_fs(options: dict) -> Optional[str]:
        return options.get(CoreOptions.DATA_FILE_EXTERNAL_PATHS_SPECIFIC_FS)

    @staticmethod
    def file_compression(options: dict) -> str:
        """Get file compression from options, default to 'zstd'."""
        compression = options.get(CoreOptions.FILE_COMPRESSION, "zstd")
        if compression is None:
            compression = "zstd"
        return compression

    @staticmethod
    def file_format(options: dict, default: Optional[str] = None) -> str:
        if default is None:
            default = CoreOptions.FILE_FORMAT_PARQUET
        file_format = options.get(CoreOptions.FILE_FORMAT, default)
        if file_format is None:
            file_format = default
        return file_format.lower() if file_format else file_format


class ExternalPathStrategy(str, Enum):
    """
    Strategy for selecting external paths.
    """
    NONE = "none"
    ROUND_ROBIN = "round-robin"
    SPECIFIC_FS = "specific-fs"
