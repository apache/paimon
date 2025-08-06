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
    WAREHOUSE = "warehouse"
    # File format options
    FILE_FORMAT = "file.format"
    FILE_FORMAT_ORC = "orc"
    FILE_FORMAT_AVRO = "avro"
    FILE_FORMAT_PARQUET = "parquet"
    FILE_COMPRESSION = "file.compression"
    FILE_COMPRESSION_PER_LEVEL = "file.compression.per.level"
    FILE_FORMAT_PER_LEVEL = "file.format.per.level"
    FILE_BLOCK_SIZE = "file.block-size"
    # Scan options
    SCAN_FALLBACK_BRANCH = "scan.fallback-branch"
    # Commit options
    COMMIT_USER_PREFIX = "commit.user-prefix"
