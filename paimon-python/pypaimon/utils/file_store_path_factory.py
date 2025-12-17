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
#  limitations under the License.
################################################################################
from typing import List, Optional, Tuple

from pypaimon.common.external_path_provider import ExternalPathProvider
from pypaimon.table.bucket_mode import BucketMode


class FileStorePathFactory:
    MANIFEST_PATH = "manifest"
    MANIFEST_PREFIX = "manifest-"
    MANIFEST_LIST_PREFIX = "manifest-list-"
    INDEX_MANIFEST_PREFIX = "index-manifest-"

    INDEX_PATH = "index"
    INDEX_PREFIX = "index-"

    STATISTICS_PATH = "statistics"
    STATISTICS_PREFIX = "stat-"

    BUCKET_PATH_PREFIX = "bucket-"

    def __init__(
        self,
        root: str,
        partition_keys: List[str],
        default_part_value: str,
        format_identifier: str,
        data_file_prefix: str,
        changelog_file_prefix: str,
        legacy_partition_name: bool,
        file_suffix_include_compression: bool,
        file_compression: str,
        data_file_path_directory: Optional[str] = None,
        external_paths: Optional[List[str]] = None,
        index_file_in_data_file_dir: bool = False,
    ):
        self._root = root.rstrip('/')
        self.partition_keys = partition_keys
        self.default_part_value = default_part_value
        self.format_identifier = format_identifier
        self.data_file_prefix = data_file_prefix
        self.changelog_file_prefix = changelog_file_prefix
        self.file_suffix_include_compression = file_suffix_include_compression
        self.file_compression = file_compression
        self.data_file_path_directory = data_file_path_directory
        self.external_paths = external_paths or []
        self.index_file_in_data_file_dir = index_file_in_data_file_dir
        self.legacy_partition_name = legacy_partition_name

    def root(self) -> str:
        return self._root

    def manifest_path(self) -> str:
        return f"{self._root}/{self.MANIFEST_PATH}"

    def index_path(self) -> str:
        return f"{self._root}/{self.INDEX_PATH}"

    def statistics_path(self) -> str:
        return f"{self._root}/{self.STATISTICS_PATH}"

    def data_file_path(self) -> str:
        if self.data_file_path_directory:
            return f"{self._root}/{self.data_file_path_directory}"
        return self._root

    def relative_bucket_path(self, partition: Tuple, bucket: int) -> str:
        bucket_name = str(bucket)
        if bucket == BucketMode.POSTPONE_BUCKET.value:
            bucket_name = "postpone"

        relative_parts = [f"{self.BUCKET_PATH_PREFIX}{bucket_name}"]

        # Add partition path
        if partition:
            partition_parts = []
            for i, field_name in enumerate(self.partition_keys):
                partition_parts.append(f"{field_name}={partition[i]}")
            if partition_parts:
                relative_parts = partition_parts + relative_parts

        # Add data file path directory if specified
        if self.data_file_path_directory:
            relative_parts = [self.data_file_path_directory] + relative_parts

        return "/".join(relative_parts)

    def bucket_path(self, partition: Tuple, bucket: int) -> str:
        relative_path = self.relative_bucket_path(partition, bucket)
        return f"{self._root}/{relative_path}"

    def create_external_path_provider(
        self, partition: Tuple, bucket: int
    ) -> Optional[ExternalPathProvider]:
        if not self.external_paths:
            return None

        relative_bucket_path = self.relative_bucket_path(partition, bucket)
        return ExternalPathProvider(self.external_paths, relative_bucket_path)
