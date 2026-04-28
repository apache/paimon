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
import sys
from enum import Enum
from typing import Dict, Optional

from datetime import timedelta

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


class ChangelogProducer(str, Enum):
    """
    Available changelog producer modes.
    """
    NONE = "none"
    INPUT = "input"
    FULL_COMPACTION = "full-compaction"
    LOOKUP = "lookup"


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
    FILE_FORMAT_VORTEX: str = "vortex"

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

    DYNAMIC_BUCKET_TARGET_ROW_NUM: ConfigOption[int] = (
        ConfigOptions.key("dynamic-bucket.target-row-num")
        .int_type()
        .default_value(2000000)
        .with_description(
            "In dynamic bucket mode (bucket=-1), target row number per bucket; "
            "when exceeded, a new bucket is created (aligned with Java SimpleHashBucketAssigner)."
        )
    )

    DYNAMIC_BUCKET_MAX_BUCKETS: ConfigOption[int] = (
        ConfigOptions.key("dynamic-bucket.max-buckets")
        .int_type()
        .default_value(-1)
        .with_description(
            "In dynamic bucket mode, max buckets per partition. -1 means unlimited."
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
        .default_value("zstd")
        .with_description("Default file compression format. For faster read and write, it is recommended to use zstd.")
    )

    FILE_COMPRESSION_ZSTD_LEVEL: ConfigOption[int] = (
        ConfigOptions.key("file.compression.zstd-level")
        .int_type()
        .default_value(1)
        .with_description(
            "Default file compression zstd level. For higher compression rates, it can be configured to 9, "
            "but the read and write speed will significantly decrease."
        )
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

    METADATA_STATS_MODE: ConfigOption[str] = (
        ConfigOptions.key("metadata.stats-mode")
        .string_type()
        .default_value("truncate(16)")
        .with_description("The mode of metadata stats. Available modes: "
                          "'none' (no stats), 'counts' (null counts only), "
                          "'full' (exact min/max), 'truncate(length)' (truncated min/max).")
    )

    BLOB_AS_DESCRIPTOR: ConfigOption[bool] = (
        ConfigOptions.key("blob-as-descriptor")
        .boolean_type()
        .default_value(False)
        .with_description("Whether to return blob values as serialized BlobDescriptor bytes when reading.")
    )

    BLOB_DESCRIPTOR_FIELD: ConfigOption[str] = (
        ConfigOptions.key("blob-descriptor-field")
        .string_type()
        .no_default_value()
        .with_description(
            "Comma-separated BLOB field names that should be stored as serialized BlobDescriptor bytes "
            "inline in normal data files."
        )
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

    SCAN_TAG_NAME: ConfigOption[str] = (
        ConfigOptions.key("scan.tag-name")
        .string_type()
        .no_default_value()
        .with_description("Optional tag name used in case of 'from-snapshot' scan mode.")
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

    CHANGELOG_PRODUCER: ConfigOption[ChangelogProducer] = (
        ConfigOptions.key("changelog-producer")
        .enum_type(ChangelogProducer)
        .default_value(ChangelogProducer.NONE)
        .with_description("The changelog producer for streaming reads. "
                          "Options: none, input, full-compaction, lookup.")
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

    COMMIT_MAX_RETRIES: ConfigOption[int] = (
        ConfigOptions.key("commit.max-retries")
        .int_type()
        .default_value(10)
        .with_description("Maximum number of retries for commit operations.")
    )

    COMMIT_TIMEOUT: ConfigOption[timedelta] = (
        ConfigOptions.key("commit.timeout")
        .duration_type()
        .no_default_value()
        .with_description("Timeout for commit operations (e.g., '10s', '5m'). If not set, effectively unlimited.")
    )

    COMMIT_MIN_RETRY_WAIT: ConfigOption[timedelta] = (
        ConfigOptions.key("commit.min-retry-wait")
        .duration_type()
        .default_value(timedelta(milliseconds=10))
        .with_description("Minimum wait time between commit retries (e.g., '10ms', '100ms').")
    )

    COMMIT_MAX_RETRY_WAIT: ConfigOption[timedelta] = (
        ConfigOptions.key("commit.max-retry-wait")
        .duration_type()
        .default_value(timedelta(seconds=10))
        .with_description("Maximum wait time between commit retries (e.g., '1s', '10s').")
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

    # Global Index options
    GLOBAL_INDEX_ENABLED: ConfigOption[bool] = (
        ConfigOptions.key("global-index.enabled")
        .boolean_type()
        .default_value(True)
        .with_description("Whether to enable global index for scan.")
    )

    GLOBAL_INDEX_THREAD_NUM: ConfigOption[int] = (
        ConfigOptions.key("global-index.thread-num")
        .int_type()
        .no_default_value()
        .with_description(
            "The maximum number of concurrent scanner for global index. "
            "By default is the number of processors available."
        )
    )

    READ_BATCH_SIZE: ConfigOption[int] = (
        ConfigOptions.key("read.batch-size")
        .int_type()
        .default_value(1024)
        .with_description("Read batch size for any file format if it supports.")
    )

    ADD_COLUMN_BEFORE_PARTITION: ConfigOption[bool] = (
        ConfigOptions.key("add-column-before-partition")
        .boolean_type()
        .default_value(False)
        .with_description(
            "When adding a new column, if the table has partition keys, "
            "insert the new column before the first partition column by default."
        )
    )

    PARTITION_DEFAULT_NAME: ConfigOption[str] = (
        ConfigOptions.key("partition.default-name")
        .string_type()
        .default_value("__DEFAULT_PARTITION__")
        .with_description(
            "The default partition name in case the dynamic partition"
            " column value is null/empty string."
        )
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

    def dynamic_bucket_target_row_num(self, default=None):
        return self.options.get(CoreOptions.DYNAMIC_BUCKET_TARGET_ROW_NUM, default)

    def dynamic_bucket_max_buckets(self, default=None):
        return self.options.get(CoreOptions.DYNAMIC_BUCKET_MAX_BUCKETS, default)

    def scan_manifest_parallelism(self, default=None):
        return self.options.get(CoreOptions.SCAN_MANIFEST_PARALLELISM, default)

    def file_format(self, default=None):
        return self.options.get(CoreOptions.FILE_FORMAT, default)

    def file_compression(self, default=None):
        return self.options.get(CoreOptions.FILE_COMPRESSION, default)

    def file_compression_zstd_level(self, default=None):
        return self.options.get(CoreOptions.FILE_COMPRESSION_ZSTD_LEVEL, default)

    def file_compression_per_level(self, default=None):
        return self.options.get(CoreOptions.FILE_COMPRESSION_PER_LEVEL, default)

    def file_format_per_level(self, default=None):
        return self.options.get(CoreOptions.FILE_FORMAT_PER_LEVEL, default)

    def file_block_size(self, default=None):
        return self.options.get(CoreOptions.FILE_BLOCK_SIZE, default)

    def metadata_stats_enabled(self, default=None):
        return self.options.get(CoreOptions.METADATA_STATS_MODE, default).upper() != "NONE"

    def metadata_stats_mode(self, default=None):
        return self.options.get(CoreOptions.METADATA_STATS_MODE, default)

    def blob_as_descriptor(self, default=None):
        return self.options.get(CoreOptions.BLOB_AS_DESCRIPTOR, default)

    def blob_descriptor_fields(self, default=None):
        value = self.options.get(CoreOptions.BLOB_DESCRIPTOR_FIELD, default)
        if value is None:
            return set()
        if isinstance(value, str):
            return {field.strip() for field in value.split(",") if field.strip()}
        if isinstance(value, (list, set, tuple)):
            return {str(field).strip() for field in value if str(field).strip()}
        return set()

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

    def scan_tag_name(self, default=None):
        return self.options.get(CoreOptions.SCAN_TAG_NAME, default)

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

    def changelog_producer(self, default=None):
        return self.options.get(CoreOptions.CHANGELOG_PRODUCER, default)

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

    def commit_max_retries(self) -> int:
        return self.options.get(CoreOptions.COMMIT_MAX_RETRIES)

    def commit_timeout(self) -> int:
        timeout = self.options.get(CoreOptions.COMMIT_TIMEOUT)
        if timeout is None:
            return sys.maxsize
        return int(timeout.total_seconds() * 1000)

    def commit_min_retry_wait(self) -> int:
        wait = self.options.get(CoreOptions.COMMIT_MIN_RETRY_WAIT)
        return int(wait.total_seconds() * 1000)

    def commit_max_retry_wait(self) -> int:
        wait = self.options.get(CoreOptions.COMMIT_MAX_RETRY_WAIT)
        return int(wait.total_seconds() * 1000)

    def global_index_enabled(self, default=None):
        return self.options.get(CoreOptions.GLOBAL_INDEX_ENABLED, default)

    def global_index_thread_num(self) -> Optional[int]:
        return self.options.get(CoreOptions.GLOBAL_INDEX_THREAD_NUM)

    def read_batch_size(self, default=None) -> int:
        return self.options.get(CoreOptions.READ_BATCH_SIZE, default or 1024)

    def add_column_before_partition(self) -> bool:
        return self.options.get(CoreOptions.ADD_COLUMN_BEFORE_PARTITION, False)
