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
from typing import List, Optional
import time

from pypaimon.data.timestamp import Timestamp
from pypaimon.manifest.schema.simple_stats import (KEY_STATS_SCHEMA, VALUE_STATS_SCHEMA,
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

    creation_time: Optional[Timestamp] = None
    delete_row_count: Optional[int] = None
    embedded_index: Optional[bytes] = None
    file_source: Optional[int] = None
    value_stats_cols: Optional[List[str]] = None
    external_path: Optional[str] = None
    first_row_id: Optional[int] = None
    write_cols: Optional[List[str]] = None

    # not a schema field, just for internal usage
    file_path: str = None

    def get_creation_time(self) -> Optional[Timestamp]:
        return self.creation_time

    def creation_time_epoch_millis(self) -> Optional[int]:
        if self.creation_time is None:
            return None
        local_dt = self.creation_time.to_local_date_time()
        local_time_struct = local_dt.timetuple()
        local_timestamp = time.mktime(local_time_struct)
        utc_timestamp = time.mktime(time.gmtime(local_timestamp))
        tz_offset_seconds = int(local_timestamp - utc_timestamp)
        return int((local_timestamp - tz_offset_seconds) * 1000)

    def creation_time_as_datetime(self) -> Optional[datetime]:
        if self.creation_time is None:
            return None
        return self.creation_time.to_local_date_time()

    @classmethod
    def create(
        cls,
        file_name: str,
        file_size: int,
        row_count: int,
        min_key: GenericRow,
        max_key: GenericRow,
        key_stats: SimpleStats,
        value_stats: SimpleStats,
        min_sequence_number: int,
        max_sequence_number: int,
        schema_id: int,
        level: int,
        extra_files: List[str],
        creation_time: Optional[Timestamp] = None,
        delete_row_count: Optional[int] = None,
        embedded_index: Optional[bytes] = None,
        file_source: Optional[int] = None,
        value_stats_cols: Optional[List[str]] = None,
        external_path: Optional[str] = None,
        first_row_id: Optional[int] = None,
        write_cols: Optional[List[str]] = None,
        file_path: Optional[str] = None,
    ) -> 'DataFileMeta':
        if creation_time is None:
            creation_time = Timestamp.now()

        return cls(
            file_name=file_name,
            file_size=file_size,
            row_count=row_count,
            min_key=min_key,
            max_key=max_key,
            key_stats=key_stats,
            value_stats=value_stats,
            min_sequence_number=min_sequence_number,
            max_sequence_number=max_sequence_number,
            schema_id=schema_id,
            level=level,
            extra_files=extra_files,
            creation_time=creation_time,
            delete_row_count=delete_row_count,
            embedded_index=embedded_index,
            file_source=file_source,
            value_stats_cols=value_stats_cols,
            external_path=external_path,
            first_row_id=first_row_id,
            write_cols=write_cols,
            file_path=file_path,
        )

    def set_file_path(self, table_path: str, partition: GenericRow, bucket: int):
        path_builder = table_path.rstrip('/')
        partition_dict = partition.to_dict()
        for field_name, field_value in partition_dict.items():
            path_builder = f"{path_builder}/{field_name}={str(field_value)}"
        path_builder = f"{path_builder}/bucket-{str(bucket)}/{self.file_name}"
        self.file_path = path_builder

    def copy_without_stats(self) -> 'DataFileMeta':
        """Create a new DataFileMeta without value statistics."""
        return DataFileMeta(
            file_name=self.file_name,
            file_size=self.file_size,
            row_count=self.row_count,
            min_key=self.min_key,
            max_key=self.max_key,
            key_stats=self.key_stats,
            value_stats=SimpleStats.empty_stats(),
            min_sequence_number=self.min_sequence_number,
            max_sequence_number=self.max_sequence_number,
            schema_id=self.schema_id,
            level=self.level,
            extra_files=self.extra_files,
            creation_time=self.creation_time,
            delete_row_count=self.delete_row_count,
            embedded_index=self.embedded_index,
            file_source=self.file_source,
            value_stats_cols=[],
            external_path=self.external_path,
            first_row_id=self.first_row_id,
            write_cols=self.write_cols,
            file_path=self.file_path
        )

    def assign_first_row_id(self, first_row_id: int) -> 'DataFileMeta':
        """Create a new DataFileMeta with the assigned first_row_id."""
        return DataFileMeta(
            file_name=self.file_name,
            file_size=self.file_size,
            row_count=self.row_count,
            min_key=self.min_key,
            max_key=self.max_key,
            key_stats=self.key_stats,
            value_stats=self.value_stats,
            min_sequence_number=self.min_sequence_number,
            max_sequence_number=self.max_sequence_number,
            schema_id=self.schema_id,
            level=self.level,
            extra_files=self.extra_files,
            creation_time=self.creation_time,
            delete_row_count=self.delete_row_count,
            embedded_index=self.embedded_index,
            file_source=self.file_source,
            value_stats_cols=self.value_stats_cols,
            external_path=self.external_path,
            first_row_id=first_row_id,
            write_cols=self.write_cols,
            file_path=self.file_path
        )

    def assign_sequence_number(self, min_sequence_number: int, max_sequence_number: int) -> 'DataFileMeta':
        """Create a new DataFileMeta with the assigned sequence numbers."""
        return DataFileMeta(
            file_name=self.file_name,
            file_size=self.file_size,
            row_count=self.row_count,
            min_key=self.min_key,
            max_key=self.max_key,
            key_stats=self.key_stats,
            value_stats=self.value_stats,
            min_sequence_number=min_sequence_number,
            max_sequence_number=max_sequence_number,
            schema_id=self.schema_id,
            level=self.level,
            extra_files=self.extra_files,
            creation_time=self.creation_time,
            delete_row_count=self.delete_row_count,
            embedded_index=self.embedded_index,
            file_source=self.file_source,
            value_stats_cols=self.value_stats_cols,
            external_path=self.external_path,
            first_row_id=self.first_row_id,
            write_cols=self.write_cols,
            file_path=self.file_path
        )


DATA_FILE_META_SCHEMA = {
    "type": "record",
    "name": "DataFileMeta",
    "fields": [
        {"name": "_FILE_NAME", "type": "string"},
        {"name": "_FILE_SIZE", "type": "long"},
        {"name": "_ROW_COUNT", "type": "long"},
        {"name": "_MIN_KEY", "type": "bytes"},
        {"name": "_MAX_KEY", "type": "bytes"},
        {"name": "_KEY_STATS", "type": KEY_STATS_SCHEMA},
        {"name": "_VALUE_STATS", "type": VALUE_STATS_SCHEMA},
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
        {"name": "_EXTERNAL_PATH", "type": ["null", "string"], "default": None},
        {"name": "_FIRST_ROW_ID", "type": ["null", "long"], "default": None},
        {"name": "_WRITE_COLS",
         "type": ["null", {"type": "array", "items": "string"}],
         "default": None},
    ]
}
