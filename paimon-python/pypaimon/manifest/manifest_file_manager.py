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
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from typing import List

import fastavro

from datetime import datetime

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import (MANIFEST_ENTRY_SCHEMA,
                                                     ManifestEntry)
from pypaimon.manifest.schema.manifest_file_meta import ManifestFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.table.row.generic_row import (GenericRowDeserializer,
                                            GenericRowSerializer)
from pypaimon.table.row.binary_row import BinaryRow


class ManifestFileManager:
    """Writer for manifest files in Avro format using unified FileIO."""

    def __init__(self, table):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        manifest_path = table.table_path.rstrip('/')
        self.manifest_path = f"{manifest_path}/manifest"
        self.file_io = table.file_io
        self.partition_keys_fields = self.table.partition_keys_fields
        self.primary_keys_fields = self.table.primary_keys_fields
        self.trimmed_primary_keys_fields = self.table.trimmed_primary_keys_fields

    def read_entries_parallel(self, manifest_files: List[ManifestFileMeta], manifest_entry_filter=None,
                              drop_stats=True, max_workers=8) -> List[ManifestEntry]:

        def _process_single_manifest(manifest_file: ManifestFileMeta) -> List[ManifestEntry]:
            return self.read(manifest_file.file_name, manifest_entry_filter, drop_stats)

        def _entry_identifier(entry: ManifestEntry) -> tuple:
            return (
                tuple(entry.partition.values),
                entry.bucket,
                entry.file.level,
                entry.file.file_name,
                tuple(entry.file.extra_files) if entry.file.extra_files else (),
                entry.file.embedded_index,
                entry.file.external_path,
            )

        deleted_entry_keys = set()
        added_entries = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_results = executor.map(_process_single_manifest, manifest_files)
            for entries in future_results:
                for entry in entries:
                    if entry.kind == 0:  # ADD
                        added_entries.append(entry)
                    else:  # DELETE
                        deleted_entry_keys.add(_entry_identifier(entry))

        final_entries = [
            entry for entry in added_entries
            if _entry_identifier(entry) not in deleted_entry_keys
        ]
        return final_entries

    def read(self, manifest_file_name: str, manifest_entry_filter=None, drop_stats=True) -> List[ManifestEntry]:
        manifest_file_path = f"{self.manifest_path}/{manifest_file_name}"

        entries = []
        with self.file_io.new_input_stream(manifest_file_path) as input_stream:
            avro_bytes = input_stream.read()
        buffer = BytesIO(avro_bytes)
        reader = fastavro.reader(buffer)

        for record in reader:
            file_dict = dict(record['_FILE'])
            key_dict = dict(file_dict['_KEY_STATS'])
            key_stats = SimpleStats(
                min_values=BinaryRow(key_dict['_MIN_VALUES'], self.trimmed_primary_keys_fields),
                max_values=BinaryRow(key_dict['_MAX_VALUES'], self.trimmed_primary_keys_fields),
                null_counts=key_dict['_NULL_COUNTS'],
            )

            schema_fields = self.table.schema_manager.get_schema(file_dict['_SCHEMA_ID']).fields
            fields = self._get_value_stats_fields(file_dict, schema_fields)
            value_dict = dict(file_dict['_VALUE_STATS'])
            value_stats = SimpleStats(
                min_values=BinaryRow(value_dict['_MIN_VALUES'], fields),
                max_values=BinaryRow(value_dict['_MAX_VALUES'], fields),
                null_counts=value_dict['_NULL_COUNTS'],
            )
            # fastavro returns UTC-aware datetime for timestamp-millis, we need to convert properly
            from pypaimon.data.timestamp import Timestamp
            creation_time_value = file_dict['_CREATION_TIME']
            creation_time_ts = None
            if creation_time_value is not None:
                if isinstance(creation_time_value, datetime):
                    if creation_time_value.tzinfo:
                        epoch_millis = int(creation_time_value.timestamp() * 1000)
                        creation_time_ts = Timestamp.from_epoch_millis(epoch_millis)
                    else:
                        creation_time_ts = Timestamp.from_local_date_time(creation_time_value)
                elif isinstance(creation_time_value, (int, float)):
                    creation_time_ts = Timestamp.from_epoch_millis(int(creation_time_value))
                else:
                    raise ValueError(f"Unexpected creation_time type: {type(creation_time_value)}")

            file_meta = DataFileMeta(
                file_name=file_dict['_FILE_NAME'],
                file_size=file_dict['_FILE_SIZE'],
                row_count=file_dict['_ROW_COUNT'],
                min_key=GenericRowDeserializer.from_bytes(file_dict['_MIN_KEY'], self.trimmed_primary_keys_fields),
                max_key=GenericRowDeserializer.from_bytes(file_dict['_MAX_KEY'], self.trimmed_primary_keys_fields),
                key_stats=key_stats,
                value_stats=value_stats,
                min_sequence_number=file_dict['_MIN_SEQUENCE_NUMBER'],
                max_sequence_number=file_dict['_MAX_SEQUENCE_NUMBER'],
                schema_id=file_dict['_SCHEMA_ID'],
                level=file_dict['_LEVEL'],
                extra_files=file_dict['_EXTRA_FILES'],
                creation_time=creation_time_ts,
                delete_row_count=file_dict['_DELETE_ROW_COUNT'],
                embedded_index=file_dict['_EMBEDDED_FILE_INDEX'],
                file_source=file_dict['_FILE_SOURCE'],
                value_stats_cols=file_dict.get('_VALUE_STATS_COLS'),
                external_path=file_dict.get('_EXTERNAL_PATH'),
                first_row_id=file_dict['_FIRST_ROW_ID'] if '_FIRST_ROW_ID' in file_dict else None,
                write_cols=file_dict['_WRITE_COLS'] if '_WRITE_COLS' in file_dict else None,
            )
            entry = ManifestEntry(
                kind=record['_KIND'],
                partition=GenericRowDeserializer.from_bytes(record['_PARTITION'], self.partition_keys_fields),
                bucket=record['_BUCKET'],
                total_buckets=record['_TOTAL_BUCKETS'],
                file=file_meta
            )
            if manifest_entry_filter is not None and not manifest_entry_filter(entry):
                continue
            if drop_stats:
                entry = entry.copy_without_stats()
            entries.append(entry)
        return entries

    def _get_value_stats_fields(self, file_dict: dict, schema_fields: list) -> List:
        if file_dict['_VALUE_STATS_COLS'] is None:
            if '_WRITE_COLS' in file_dict:
                if file_dict['_WRITE_COLS'] is None:
                    fields = schema_fields
                else:
                    read_fields = file_dict['_WRITE_COLS']
                    fields = [self.table.field_dict[col] for col in read_fields]
            else:
                fields = schema_fields
        elif not file_dict['_VALUE_STATS_COLS']:
            fields = []
        else:
            fields = [self.table.field_dict[col] for col in file_dict['_VALUE_STATS_COLS']]
        return fields

    def write(self, file_name, entries: List[ManifestEntry]):
        avro_records = []
        for entry in entries:
            avro_record = {
                "_VERSION": 2,
                "_KIND": entry.kind,
                "_PARTITION": GenericRowSerializer.to_bytes(entry.partition),
                "_BUCKET": entry.bucket,
                "_TOTAL_BUCKETS": entry.total_buckets,
                "_FILE": {
                    "_FILE_NAME": entry.file.file_name,
                    "_FILE_SIZE": entry.file.file_size,
                    "_ROW_COUNT": entry.file.row_count,
                    "_MIN_KEY": GenericRowSerializer.to_bytes(entry.file.min_key),
                    "_MAX_KEY": GenericRowSerializer.to_bytes(entry.file.max_key),
                    "_KEY_STATS": {
                        "_MIN_VALUES": GenericRowSerializer.to_bytes(entry.file.key_stats.min_values),
                        "_MAX_VALUES": GenericRowSerializer.to_bytes(entry.file.key_stats.max_values),
                        "_NULL_COUNTS": entry.file.key_stats.null_counts,
                    },
                    "_VALUE_STATS": {
                        "_MIN_VALUES": GenericRowSerializer.to_bytes(entry.file.value_stats.min_values),
                        "_MAX_VALUES": GenericRowSerializer.to_bytes(entry.file.value_stats.max_values),
                        "_NULL_COUNTS": entry.file.value_stats.null_counts,
                    },
                    "_MIN_SEQUENCE_NUMBER": entry.file.min_sequence_number,
                    "_MAX_SEQUENCE_NUMBER": entry.file.max_sequence_number,
                    "_SCHEMA_ID": entry.file.schema_id,
                    "_LEVEL": entry.file.level,
                    "_EXTRA_FILES": entry.file.extra_files,
                    "_CREATION_TIME": entry.file.creation_time.get_millisecond() if entry.file.creation_time else None,
                    "_DELETE_ROW_COUNT": entry.file.delete_row_count,
                    "_EMBEDDED_FILE_INDEX": entry.file.embedded_index,
                    "_FILE_SOURCE": entry.file.file_source,
                    "_VALUE_STATS_COLS": entry.file.value_stats_cols,
                    "_EXTERNAL_PATH": entry.file.external_path,
                    "_FIRST_ROW_ID": entry.file.first_row_id,
                    "_WRITE_COLS": entry.file.write_cols,
                }
            }
            avro_records.append(avro_record)

        manifest_path = f"{self.manifest_path}/{file_name}"
        try:
            buffer = BytesIO()
            fastavro.writer(buffer, MANIFEST_ENTRY_SCHEMA, avro_records)
            avro_bytes = buffer.getvalue()
            with self.file_io.new_output_stream(manifest_path) as output_stream:
                output_stream.write(avro_bytes)
        except Exception as e:
            self.file_io.delete_quietly(manifest_path)
            raise RuntimeError(f"Failed to write manifest file: {e}") from e
