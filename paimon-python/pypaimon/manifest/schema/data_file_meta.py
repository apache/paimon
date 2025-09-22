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
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from pypaimon.manifest.schema.simple_stats import (SIMPLE_STATS_SCHEMA,
                                                   SimpleStats)
from pypaimon.table.row.generic_row import GenericRow


@dataclass
class DataFileMeta:
    file_name: str
    file_size: int
    row_count: int
    min_key: GenericRow
    max_key: GenericRow
    key_stats: SimpleStats
    value_stats: SimpleStats
    min_sequence_number: int
    max_sequence_number: int
    schema_id: int
    level: int
    extra_files: List[str]

    creation_time: Optional[datetime] = None
    delete_row_count: Optional[int] = None
    embedded_index: Optional[bytes] = None
    file_source: Optional[str] = None
    value_stats_cols: Optional[List[str]] = None
    external_path: Optional[str] = None

    # not a schema field, just for internal usage
    file_path: str = None
    file_start_row: int = None
    file_end_row: int = None

    def set_file_path(self, table_path: Path, partition: GenericRow, bucket: int):
        path_builder = table_path
        partition_dict = partition.to_dict()
        for field_name, field_value in partition_dict.items():
            path_builder = path_builder / (field_name + "=" + str(field_value))
        path_builder = path_builder / ("bucket-" + str(bucket)) / self.file_name
        self.file_path = str(path_builder)


DATA_FILE_META_SCHEMA = {
    "type": "record",
    "name": "DataFileMeta",
    "fields": [
        {"name": "_FILE_NAME", "type": "string"},
        {"name": "_FILE_SIZE", "type": "long"},
        {"name": "_ROW_COUNT", "type": "long"},
        {"name": "_MIN_KEY", "type": "bytes"},
        {"name": "_MAX_KEY", "type": "bytes"},
        {"name": "_KEY_STATS", "type": SIMPLE_STATS_SCHEMA},
        {"name": "_VALUE_STATS", "type": "SimpleStats"},
        {"name": "_MIN_SEQUENCE_NUMBER", "type": "long"},
        {"name": "_MAX_SEQUENCE_NUMBER", "type": "long"},
        {"name": "_SCHEMA_ID", "type": "long"},
        {"name": "_LEVEL", "type": "int"},
        {"name": "_EXTRA_FILES", "type": {"type": "array", "items": "string"}},
        {"name": "_CREATION_TIME",
         "type": [
             "null",
             {"type": "long", "logicalType": "timestamp-millis"}],
         "default": None},
        {"name": "_DELETE_ROW_COUNT", "type": ["null", "long"], "default": None},
        {"name": "_EMBEDDED_FILE_INDEX", "type": ["null", "bytes"], "default": None},
        {"name": "_FILE_SOURCE", "type": ["null", "int"], "default": None},
        {"name": "_VALUE_STATS_COLS",
         "type": ["null", {"type": "array", "items": "string"}],
         "default": None},
    ]
}
