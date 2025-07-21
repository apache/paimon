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

    # File format options
    FILE_FORMAT = "file.format"
    FILE_FORMAT_ORC = "orc"
    FILE_FORMAT_AVRO = "avro"
    FILE_FORMAT_PARQUET = "parquet"
    FILE_COMPRESSION = "file.compression"
    FILE_COMPRESSION_PER_LEVEL = "file.compression.per.level"
    FILE_FORMAT_PER_LEVEL = "file.format.per.level"
    FILE_BLOCK_SIZE = "file.block-size"

    # File index options
    FILE_INDEX = "file-index"
    FILE_INDEX_IN_MANIFEST_THRESHOLD = "file-index.in-manifest-threshold"
    FILE_INDEX_READ_ENABLED = "file-index.read.enabled"

    # Manifest options
    MANIFEST_FORMAT = "manifest.format"
    MANIFEST_COMPRESSION = "manifest.compression"
    MANIFEST_TARGET_FILE_SIZE = "manifest.target-file-size"

    # Sort options
    SORT_SPILL_THRESHOLD = "sort-spill-threshold"
    SORT_SPILL_BUFFER_SIZE = "sort-spill-buffer-size"
    SPILL_COMPRESSION = "spill-compression"
    SPILL_COMPRESSION_ZSTD_LEVEL = "spill-compression.zstd-level"

    # Write options
    WRITE_ONLY = "write-only"
    TARGET_FILE_SIZE = "target-file-size"
    WRITE_BUFFER_SIZE = "write-buffer-size"

    # Level options
    NUM_LEVELS = "num-levels"

    # Commit options
    COMMIT_FORCE_COMPACT = "commit.force-compact"
    COMMIT_TIMEOUT = "commit.timeout"
    COMMIT_MAX_RETRIES = "commit.max-retries"

    # Compaction options
    COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT = "compaction.max-size-amplification-percent"

    # Field options
    DEFAULT_VALUE_SUFFIX = "default-value"
    FIELDS_PREFIX = "fields"
    FIELDS_SEPARATOR = ","

    # Aggregate options
    AGG_FUNCTION = "aggregate-function"
    DEFAULT_AGG_FUNCTION = "default-aggregate-function"

    # Other options
    IGNORE_RETRACT = "ignore-retract"
    NESTED_KEY = "nested-key"
    DISTINCT = "distinct"
    LIST_AGG_DELIMITER = "list-agg-delimiter"
    COLUMNS = "columns"

    # Row kind options
    ROWKIND_FIELD = "rowkind.field"

    # Scan options
    SCAN_MODE = "scan.mode"
    SCAN_TIMESTAMP = "scan.timestamp"
    SCAN_TIMESTAMP_MILLIS = "scan.timestamp-millis"
    SCAN_WATERMARK = "scan.watermark"
    SCAN_FILE_CREATION_TIME_MILLIS = "scan.file-creation-time-millis"
    SCAN_SNAPSHOT_ID = "scan.snapshot-id"
    SCAN_TAG_NAME = "scan.tag-name"
    SCAN_VERSION = "scan.version"
    SCAN_BOUNDED_WATERMARK = "scan.bounded.watermark"
    SCAN_MANIFEST_PARALLELISM = "scan.manifest.parallelism"
    SCAN_FALLBACK_BRANCH = "scan.fallback-branch"
    SCAN_MAX_SPLITS_PER_TASK = "scan.max-splits-per-task"
    SCAN_PLAN_SORT_PARTITION = "scan.plan.sort-partition"

    # Startup mode options
    INCREMENTAL_BETWEEN = "incremental-between"
    INCREMENTAL_BETWEEN_TIMESTAMP = "incremental-between-timestamp"

    # Stream scan mode options
    STREAM_SCAN_MODE = "stream-scan-mode"

    # Consumer options
    CONSUMER_ID = "consumer-id"
    CONSUMER_IGNORE_PROGRESS = "consumer-ignore-progress"

    # Changelog options
    CHANGELOG_PRODUCER = "changelog-producer"
    CHANGELOG_PRODUCER_ROW_DEDUPLICATE = "changelog-producer.row-deduplicate"
    CHANGELOG_PRODUCER_ROW_DEDUPLICATE_IGNORE_FIELDS = "changelog-producer.row-deduplicate-ignore-fields"
    CHANGELOG_LIFECYCLE_DECOUPLED = "changelog-lifecycle-decoupled"

    # Merge engine options
    MERGE_ENGINE = "merge-engine"
    IGNORE_DELETE = "ignore-delete"
    PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE = "partial-update.remove-record-on-delete"
    PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP = "partial-update.remove-record-on-sequence-group"

    # Lookup options
    FORCE_LOOKUP = "force-lookup"
    LOOKUP_WAIT = "lookup-wait"

    # Delete file options
    DELETE_FILE_THREAD_NUM = "delete-file.thread-num"

    # Commit user options
    COMMIT_USER_PREFIX = "commit.user-prefix"
