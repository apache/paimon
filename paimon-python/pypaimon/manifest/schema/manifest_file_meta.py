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

from dataclasses import dataclass

from pypaimon.manifest.schema.simple_stats import (PARTITION_STATS_SCHEMA,
                                                   SimpleStats)


@dataclass
class ManifestFileMeta:
    file_name: str
    file_size: int
    num_added_files: int
    num_deleted_files: int
    partition_stats: SimpleStats
    schema_id: int


MANIFEST_FILE_META_SCHEMA = {
    "type": "record",
    "name": "ManifestFileMeta",
    "fields": [
        {"name": "_VERSION", "type": "int"},
        {"name": "_FILE_NAME", "type": "string"},
        {"name": "_FILE_SIZE", "type": "long"},
        {"name": "_NUM_ADDED_FILES", "type": "long"},
        {"name": "_NUM_DELETED_FILES", "type": "long"},
        {"name": "_PARTITION_STATS", "type": PARTITION_STATS_SCHEMA},
        {"name": "_SCHEMA_ID", "type": "long"},
    ]
}
