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
from typing import List, Tuple, Set

import fastavro

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
        self.manifest_path = table.table_path / "manifest"
        self.file_io = table.file_io
        self.partition_keys_fields = self.table.partition_keys_fields
        self.primary_keys_fields = self.table.primary_keys_fields
        self.trimmed_primary_keys_fields = self.table.trimmed_primary_keys_fields

    def read_entries_parallel(self, manifest_files: List[ManifestFileMeta], manifest_file_filter=None,
                              manifest_entry_filter=None, drop_stats=True, max_workers=8) -> List[ManifestEntry]:

        def _process_single_manifest(manifest_file: ManifestFileMeta) -> Tuple[List[ManifestEntry], Set[tuple]]:
            local_added = []
            local_deleted_keys = set()
            if manifest_file_filter and not manifest_file_filter(manifest_file):
                return local_added, local_deleted_keys
            manifest_entries = self.read(manifest_file.file_name, manifest_entry_filter, drop_stats)
            for entry in manifest_entries:
                if entry.kind == 0:
                    local_added.append(entry)
                else:
                    key = (tuple(entry.partition.values), entry.bucket, entry.file.file_name)
                    local_deleted_keys.add(key)
            local_final_added = [
                entry for entry in local_added
                if (tuple(entry.partition.values), entry.bucket, entry.file.file_name) not in local_deleted_keys
            ]
            return local_final_added, local_deleted_keys

        deleted_entry_keys = set()
        added_entries = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_results = executor.map(_process_single_manifest, manifest_files)
            for added, deleted_keys in future_results:
                added_entries.extend(added)
                deleted_entry_keys.update(deleted_keys)

        final_entries = [
            entry for entry in added_entries
            if (tuple(entry.partition.values), entry.bucket, entry.file.file_name) not in deleted_entry_keys
        ]
        return final_entries

    def read(self, manifest_file_name: str, manifest_entry_filter=None, drop_stats=True) -> List[ManifestEntry]:
        manifest_file_path = self.manifest_path / manifest_file_name

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

            value_dict = dict(file_dict['_VALUE_STATS'])
            if file_dict['_VALUE_STATS_COLS'] is None:
                if file_dict['_WRITE_COLS'] is None:
                    fields = self.table.table_schema.fields
                else:
                    read_fields = file_dict['_WRITE_COLS']
                    fields = [self.table.field_dict[col] for col in read_fields]
            elif not file_dict['_VALUE_STATS_COLS']:
                fields = []
            else:
                fields = [self.table.field_dict[col] for col in file_dict['_VALUE_STATS_COLS']]
            value_stats = SimpleStats(
                min_values=BinaryRow(value_dict['_MIN_VALUES'], fields),
                max_values=BinaryRow(value_dict['_MAX_VALUES'], fields),
                null_counts=value_dict['_NULL_COUNTS'],
            )
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
                creation_time=file_dict['_CREATION_TIME'],
                delete_row_count=file_dict['_DELETE_ROW_COUNT'],
                embedded_index=file_dict['_EMBEDDED_FILE_INDEX'],
                file_source=file_dict['_FILE_SOURCE'],
                value_stats_cols=file_dict.get('_VALUE_STATS_COLS'),
                external_path=file_dict.get('_EXTERNAL_PATH'),
                first_row_id=file_dict['_FIRST_ROW_ID'],
                write_cols=file_dict['_WRITE_COLS'],
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
                    "_CREATION_TIME": entry.file.creation_time,
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

        manifest_path = self.manifest_path / file_name
        try:
            buffer = BytesIO()
            fastavro.writer(buffer, MANIFEST_ENTRY_SCHEMA, avro_records)
            avro_bytes = buffer.getvalue()
            with self.file_io.new_output_stream(manifest_path) as output_stream:
                output_stream.write(avro_bytes)
        except Exception as e:
            self.file_io.delete_quietly(manifest_path)
            raise RuntimeError(f"Failed to write manifest file: {e}") from e
