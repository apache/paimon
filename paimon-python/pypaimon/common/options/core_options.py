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
from typing import Dict

from pypaimon.common.memory_size import MemorySize
from pypaimon.common.options import Options
from pypaimon.common.options.config_options import ConfigOptions
from pypaimon.common.options.config_option import ConfigOption


class ExternalPathStrategy(str, Enum):
    """
    Strategy for selecting external paths.
    """
    NONE = "none"
    ROUND_ROBIN = "round-robin"
    SPECIFIC_FS = "specific-fs"


class MergeEngine(str, Enum):
    """
    Specifies the merge engine for table with primary key.
    """
    DEDUPLICATE = "deduplicate"
    PARTIAL_UPDATE = "partial-update"
    AGGREGATE = "aggregation"
    FIRST_ROW = "first-row"


class CoreOptions:
    """Core options for Paimon tables."""
    # File format constants
    FILE_FORMAT_ORC: str = "orc"
    FILE_FORMAT_AVRO: str = "avro"
    FILE_FORMAT_PARQUET: str = "parquet"
    FILE_FORMAT_BLOB: str = "blob"
    FILE_FORMAT_LANCE: str = "lance"

    # Basic options
    AUTO_CREATE: ConfigOption[bool] = (
        ConfigOptions.key("auto-create")
        .boolean_type()
        .default_value(False)
        .with_description("Whether to auto create table.")
    )

    PATH: ConfigOption[str] = (
        ConfigOptions.key("path")
        .string_type()
        .no_default_value()
        .with_description("The file path of this table in the filesystem.")
    )

    TYPE: ConfigOption[str] = (
        ConfigOptions.key("type")
        .string_type()
        .default_value("primary-key")
        .with_description("Specify what type of table this is.")
    )

    BRANCH: ConfigOption[str] = (
        ConfigOptions.key("branch")
        .string_type()
        .default_value("main")
        .with_description("The branch name of this table.")
    )

    BUCKET: ConfigOption[int] = (
        ConfigOptions.key("bucket")
        .int_type()
        .default_value(-1)
        .with_description(
            "Bucket number for file store. If bucket is -1, the parallelism of "
            "sink determines the bucket number: write a record to the file corresponding to the "
            "hash value of one or more fields. When creating table, bucket number must be specified."
        )
    )

    BUCKET_KEY: ConfigOption[str] = (
        ConfigOptions.key("bucket-key")
        .string_type()
        .no_default_value()
        .with_description(
            "Specify the hash key by bucket-key. "
            "By default, if there is a primary key, the primary key will be used; "
            "if there is no primary key, the full row will be used. "
            "In this case, the sink parallelism must be set to the bucket number."
        )
    )

    SCAN_MANIFEST_PARALLELISM: ConfigOption[int] = (
        ConfigOptions.key("scan.manifest.parallelism")
        .int_type()
        .default_value(16)
        .with_description("The parallelism for scanning manifest files.")
    )

    # File format options
    FILE_FORMAT: ConfigOption[str] = (
        ConfigOptions.key("file.format")
        .string_type()
        .default_value(FILE_FORMAT_ORC)
        .with_description("Specify the message format of data files.")
    )

    FILE_COMPRESSION: ConfigOption[str] = (
        ConfigOptions.key("file.compression")
        .string_type()
        .default_value("lz4")
        .with_description("Default file compression format.")
    )

    FILE_COMPRESSION_PER_LEVEL: ConfigOption[Dict[str, str]] = (
        ConfigOptions.key("file.compression.per.level")
        .map_type()
        .default_value({})
        .with_description(
            "Define different compression policies for different level LSM data files, "
            "you can add the level and the corresponding compression type."
        )
    )

    FILE_FORMAT_PER_LEVEL: ConfigOption[Dict[str, str]] = (
        ConfigOptions.key("file.format.per.level")
        .map_type()
        .default_value({})
        .with_description(
            "Define different format types for different level LSM data files, "
            "you can add the level and the corresponding format type."
        )
    )

    FILE_BLOCK_SIZE: ConfigOption[MemorySize] = (
        ConfigOptions.key("file.block-size")
        .memory_type()
        .no_default_value()
        .with_description("Define the data block size.")
    )

    BLOB_AS_DESCRIPTOR: ConfigOption[bool] = (
        ConfigOptions.key("blob-as-descriptor")
        .boolean_type()
        .default_value(False)
        .with_description("Whether to use blob as descriptor.")
    )

    TARGET_FILE_SIZE: ConfigOption[MemorySize] = (
        ConfigOptions.key("target-file-size")
        .memory_type()
        .no_default_value()
        .with_description("The target file size for data files.")
    )

    BLOB_TARGET_FILE_SIZE: ConfigOption[MemorySize] = (
        ConfigOptions.key("blob.target-file-size")
        .memory_type()
        .default_value(MemorySize.of_mebi_bytes(256))
        .with_description("The target file size for blob files.")
    )
    DATA_FILE_PREFIX: ConfigOption[str] = (
        ConfigOptions.key("data-file.prefix")
        .string_type()
        .default_value("data-")
        .with_description("Specify the file name prefix of data files.")
    )
    # Scan options
    SCAN_FALLBACK_BRANCH: ConfigOption[str] = (
        ConfigOptions.key("scan.fallback-branch")
        .string_type()
        .no_default_value()
        .with_description("The fallback branch for scanning.")
    )

    INCREMENTAL_BETWEEN_TIMESTAMP: ConfigOption[str] = (
        ConfigOptions.key("incremental-between-timestamp")
        .string_type()
        .no_default_value()
        .with_description("The timestamp range for incremental reading.")
    )

    SOURCE_SPLIT_TARGET_SIZE: ConfigOption[MemorySize] = (
        ConfigOptions.key("source.split.target-size")
        .memory_type()
        .default_value(MemorySize.of_mebi_bytes(128))
        .with_description("The target size of a source split when scanning a table.")
    )

    SOURCE_SPLIT_OPEN_FILE_COST: ConfigOption[MemorySize] = (
        ConfigOptions.key("source.split.open-file-cost")
        .memory_type()
        .default_value(MemorySize.of_mebi_bytes(4))
        .with_description(
            "The estimated cost to open a file, used when scanning a table. "
            "It is used to avoid opening too many small files."
        )
    )
    DELETION_VECTORS_ENABLED: ConfigOption[bool] = (
        ConfigOptions.key("deletion-vectors.enabled")
        .boolean_type()
        .default_value(False)
        .with_description("Whether to enable deletion vectors.")
    )

    MERGE_ENGINE: ConfigOption[MergeEngine] = (
        ConfigOptions.key("merge-engine")
        .enum_type(MergeEngine)
        .default_value(MergeEngine.DEDUPLICATE)
        .with_description("Specify the merge engine for table with primary key. "
                          "Options: deduplicate, partial-update, aggregation, first-row.")
    )
    # Commit options
    COMMIT_USER_PREFIX: ConfigOption[str] = (
        ConfigOptions.key("commit.user-prefix")
        .string_type()
        .no_default_value()
        .with_description("The prefix for commit user.")
    )

    ROW_TRACKING_ENABLED: ConfigOption[bool] = (
        ConfigOptions.key("row-tracking.enabled")
        .boolean_type()
        .default_value(False)
        .with_description("Whether to enable row tracking.")
    )

    DATA_EVOLUTION_ENABLED: ConfigOption[bool] = (
        ConfigOptions.key("data-evolution.enabled")
        .boolean_type()
        .default_value(False)
        .with_description("Whether to enable data evolution.")
    )
    # External paths options
    DATA_FILE_EXTERNAL_PATHS: ConfigOption[str] = (
        ConfigOptions.key("data-file.external-paths")
        .string_type()
        .no_default_value()
        .with_description("External paths for data files, separated by comma.")
    )

    DATA_FILE_EXTERNAL_PATHS_STRATEGY: ConfigOption[str] = (
        ConfigOptions.key("data-file.external-paths.strategy")
        .string_type()
        .default_value(ExternalPathStrategy.NONE)
        .with_description("Strategy for selecting external paths. Options: none, round-robin, specific-fs.")
    )

    DATA_FILE_EXTERNAL_PATHS_SPECIFIC_FS: ConfigOption[str] = (
        ConfigOptions.key("data-file.external-paths.specific-fs")
        .string_type()
        .no_default_value()
        .with_description("Specific filesystem for external paths when using specific-fs strategy.")
    )

    def __init__(self, options: Options):
        self.options = options

    def set(self, key: ConfigOption, value):
        self.options.set(key, value)

    @staticmethod
    def copy(options: 'CoreOptions') -> 'CoreOptions':
        return CoreOptions.from_dict(dict(options.options.to_map()))

    @staticmethod
    def from_dict(options: dict) -> 'CoreOptions':
        return CoreOptions(Options(options))

    def path(self, default=None):
        return self.options.get(CoreOptions.PATH, default)

    def auto_create(self, default=None):
        return self.options.get(CoreOptions.AUTO_CREATE, default)

    def type(self, default=None):
        return self.options.get(CoreOptions.TYPE, default)

    def branch(self, default=None):
        return self.options.get(CoreOptions.BRANCH, default)

    def bucket(self, default=None):
        return self.options.get(CoreOptions.BUCKET, default)

    def bucket_key(self, default=None):
        return self.options.get(CoreOptions.BUCKET_KEY, default)

    def scan_manifest_parallelism(self, default=None):
        return self.options.get(CoreOptions.SCAN_MANIFEST_PARALLELISM, default)

    def file_format(self, default=None):
        return self.options.get(CoreOptions.FILE_FORMAT, default)

    def file_compression(self, default=None):
        return self.options.get(CoreOptions.FILE_COMPRESSION, default)

    def file_compression_per_level(self, default=None):
        return self.options.get(CoreOptions.FILE_COMPRESSION_PER_LEVEL, default)

    def file_format_per_level(self, default=None):
        return self.options.get(CoreOptions.FILE_FORMAT_PER_LEVEL, default)

    def file_block_size(self, default=None):
        return self.options.get(CoreOptions.FILE_BLOCK_SIZE, default)

    def blob_as_descriptor(self, default=None):
        return self.options.get(CoreOptions.BLOB_AS_DESCRIPTOR, default)

    def target_file_size(self, has_primary_key, default=None):
        return self.options.get(CoreOptions.TARGET_FILE_SIZE,
                                MemorySize.of_mebi_bytes(
                                    128 if has_primary_key else 256) if default is None else MemorySize.parse(
                                    default)).get_bytes()

    def blob_target_file_size(self, default=None):
        """
        Args:
            default: The standby value when the configuration item does not exist. If default is also None,
            then use the default value of the configuration item itself.
        """
        if self.options.contains(CoreOptions.BLOB_TARGET_FILE_SIZE):
            return self.options.get(CoreOptions.BLOB_TARGET_FILE_SIZE, None).get_bytes()
        elif default is not None:
            return MemorySize.parse(default).get_bytes()
        else:
            return self.target_file_size(has_primary_key=False)

    def data_file_prefix(self, default=None):
        return self.options.get(CoreOptions.DATA_FILE_PREFIX, default)

    def scan_fallback_branch(self, default=None):
        return self.options.get(CoreOptions.SCAN_FALLBACK_BRANCH, default)

    def incremental_between_timestamp(self, default=None):
        return self.options.get(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP, default)

    def source_split_target_size(self, default=None):
        return self.options.get(CoreOptions.SOURCE_SPLIT_TARGET_SIZE, default).get_bytes()

    def source_split_open_file_cost(self, default=None):
        return self.options.get(CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST, default).get_bytes()

    def commit_user_prefix(self, default=None):
        return self.options.get(CoreOptions.COMMIT_USER_PREFIX, default)

    def row_tracking_enabled(self, default=None):
        return self.options.get(CoreOptions.ROW_TRACKING_ENABLED, default)

    def data_evolution_enabled(self, default=None):
        return self.options.get(CoreOptions.DATA_EVOLUTION_ENABLED, default)

    def deletion_vectors_enabled(self, default=None):
        return self.options.get(CoreOptions.DELETION_VECTORS_ENABLED, default)

    def merge_engine(self, default=None):
        return self.options.get(CoreOptions.MERGE_ENGINE, default)

    def data_file_external_paths(self, default=None):
        external_paths_str = self.options.get(CoreOptions.DATA_FILE_EXTERNAL_PATHS, default)
        if not external_paths_str:
            return None
        return [path.strip() for path in external_paths_str.split(",") if path.strip()]

    def data_file_external_paths_strategy(self, default=None):
        return self.options.get(CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY, default)

    def data_file_external_paths_specific_fs(self, default=None):
        return self.options.get(CoreOptions.DATA_FILE_EXTERNAL_PATHS_SPECIFIC_FS, default)
