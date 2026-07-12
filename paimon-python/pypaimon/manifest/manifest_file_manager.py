# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from typing import Callable, List, Optional

import fastavro

from datetime import datetime

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import (MANIFEST_ENTRY_SCHEMA,
                                                     ManifestEntry)
from pypaimon.manifest.schema.manifest_file_meta import ManifestFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.table.row.generic_row import (GenericRow,
                                            GenericRowDeserializer,
                                            GenericRowSerializer)
from pypaimon.table.row.binary_row import BinaryRow


class ManifestFileManager:
    """Writer for manifest files in Avro format using unified FileIO."""

    _AVRO_SYNC_INTERVAL = 16000

    def __init__(self, table):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        manifest_path = table.table_path.rstrip('/')
        self.manifest_path = f"{manifest_path}/manifest"
        self.file_io = table.file_io
        self.partition_keys_fields = self.table.partition_keys_fields
        self.primary_keys_fields = self.table.primary_keys_fields
        self.trimmed_primary_keys_fields = self.table.trimmed_primary_keys_fields
        from pypaimon.manifest import avro_codec
        self._codec = avro_codec(table.options.manifest_compression())

    def read_entries_parallel(self, manifest_files: List[ManifestFileMeta], manifest_entry_filter=None,
                              drop_stats=True, max_workers=8,
                              early_entry_filter: Optional[Callable[[int, int], bool]] = None,
                              early_record_filter: Optional[Callable[[dict], bool]] = None,
                              partition_filter=None,
                              ) -> List[ManifestEntry]:

        def _process_single_manifest(manifest_file: ManifestFileMeta) -> List[ManifestEntry]:
            return self.read(manifest_file.file_name, manifest_entry_filter, drop_stats,
                             early_entry_filter=early_entry_filter,
                             early_record_filter=early_record_filter,
                             partition_filter=partition_filter)

        def _entry_identifier(e: ManifestEntry) -> tuple:
            return (
                tuple(e.partition.values),
                e.bucket,
                e.file.level,
                e.file.file_name,
                tuple(e.file.extra_files) if e.file.extra_files else (),
                e.file.embedded_index,
                e.file.external_path,
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

    def read(self, manifest_file_name: str, manifest_entry_filter=None, drop_stats=True,
             early_entry_filter: Optional[Callable[[int, int], bool]] = None,
             early_record_filter: Optional[Callable[[dict], bool]] = None,
             partition_filter=None,
             ) -> List[ManifestEntry]:
        """
        early_entry_filter: ``(bucket, total_buckets) -> bool``, skip before deserializing _FILE.
        early_record_filter: ``(fastavro record dict) -> bool``, skip before constructing
            DataFileMeta. Separate from early_entry_filter because it operates on the full
            record (can inspect _FILE sub-fields) rather than just bucket/total_buckets.
        partition_filter: optional Predicate tested against the entry partition
            (deserialized before the _FILE block); non-matching entries skip full
            _FILE deserialization.
        """
        manifest_file_path = f"{self.manifest_path}/{manifest_file_name}"

        entries = []
        with self.file_io.new_input_stream(manifest_file_path) as input_stream:
            avro_bytes = input_stream.read()
        buffer = BytesIO(avro_bytes)
        reader = fastavro.reader(buffer)

        for record in reader:
            if early_entry_filter is not None:
                try:
                    bucket = record['_BUCKET']
                    total_buckets = record['_TOTAL_BUCKETS']
                except KeyError:
                    pass
                else:
                    if not early_entry_filter(bucket, total_buckets):
                        continue
            if early_record_filter is not None and not early_record_filter(record):
                continue
            partition = None
            if partition_filter is not None:
                partition = GenericRowDeserializer.from_bytes(
                    record['_PARTITION'], self.partition_keys_fields)
                if not partition_filter.test(partition):
                    continue
            file_dict = dict(record['_FILE'])
            key_dict = dict(file_dict['_KEY_STATS'])
            key_stats = SimpleStats(
                min_values=BinaryRow(key_dict['_MIN_VALUES'], self.trimmed_primary_keys_fields),
                max_values=BinaryRow(key_dict['_MAX_VALUES'], self.trimmed_primary_keys_fields),
                null_counts=key_dict['_NULL_COUNTS'],
            )

            schema_id = file_dict['_SCHEMA_ID']
            if schema_id == self.table.table_schema.id:
                schema_fields = self.table.table_schema.fields
            else:
                schema_fields = self.table.schema_manager.get_schema(schema_id).fields
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
            if partition is None:
                partition = GenericRowDeserializer.from_bytes(
                    record['_PARTITION'], self.partition_keys_fields)
            entry = ManifestEntry(
                kind=record['_KIND'],
                partition=partition,
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
                    # writeCols may contain metadata fields (e.g. _ROW_ID, _SEQUENCE_NUMBER)
                    data_field_dict = {f.name: f for f in schema_fields}
                    fields = [data_field_dict[col] for col in read_fields
                              if col in data_field_dict]
            else:
                fields = schema_fields
        elif not file_dict['_VALUE_STATS_COLS']:
            fields = []
        else:
            fields = [self.table.field_dict[col] for col in file_dict['_VALUE_STATS_COLS']]
        return fields

    def write(self, file_name, entries: List[ManifestEntry]):
        buf = BytesIO()
        fastavro.writer(
            buf, MANIFEST_ENTRY_SCHEMA, self._to_avro_records(entries),
            codec=self._codec)
        self._flush(file_name, buf.getvalue())

    def rolling_write(self, entries: List[ManifestEntry],
                      suggested_file_size: int,
                      name_prefix: str) -> List[ManifestFileMeta]:
        if not entries:
            return []

        from fastavro.write import Writer

        sync_interval = min(self._AVRO_SYNC_INTERVAL, suggested_file_size)
        result = []
        written_files = []
        chunk_start = 0
        buf = BytesIO()
        writer = Writer(buf, MANIFEST_ENTRY_SCHEMA,
                        sync_interval=sync_interval, codec=self._codec)
        try:
            for i, entry in enumerate(entries):
                writer.write(self._to_avro_record(entry))
                if buf.tell() >= suggested_file_size:
                    writer.flush()
                    avro_bytes = buf.getvalue()
                    file_name = f"{name_prefix}-{len(result)}"
                    self._flush(file_name, avro_bytes)
                    written_files.append(file_name)
                    result.append(self._build_meta(
                        file_name, entries[chunk_start:i + 1], len(avro_bytes)))
                    chunk_start = i + 1
                    buf = BytesIO()
                    writer = Writer(
                        buf, MANIFEST_ENTRY_SCHEMA,
                        sync_interval=sync_interval, codec=self._codec)

            if chunk_start < len(entries):
                writer.flush()
                avro_bytes = buf.getvalue()
                file_name = f"{name_prefix}-{len(result)}"
                self._flush(file_name, avro_bytes)
                written_files.append(file_name)
                result.append(self._build_meta(
                    file_name, entries[chunk_start:], len(avro_bytes)))
        except Exception:
            for fname in written_files:
                self.file_io.delete_quietly(f"{self.manifest_path}/{fname}")
            raise
        return result

    @staticmethod
    def _to_avro_record(entry: ManifestEntry) -> dict:
        return {
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

    def _to_avro_records(self, entries: List[ManifestEntry]) -> List[dict]:
        return [self._to_avro_record(e) for e in entries]

    def _flush(self, file_name: str, avro_bytes: bytes):
        manifest_path = f"{self.manifest_path}/{file_name}"
        try:
            with self.file_io.new_output_stream(manifest_path) as output_stream:
                output_stream.write(avro_bytes)
        except Exception as e:
            self.file_io.delete_quietly(manifest_path)
            raise RuntimeError(f"Failed to write manifest file: {e}") from e

    def _build_meta(self, file_name: str, entries: List[ManifestEntry],
                    file_size: int = None) -> ManifestFileMeta:
        added_file_count = 0
        deleted_file_count = 0
        schema_id = None
        for entry in entries:
            if entry.kind == 0:
                added_file_count += 1
            else:
                deleted_file_count += 1
            schema_id = entry.file.schema_id if schema_id is None else max(schema_id, entry.file.schema_id)
        if schema_id is None:
            schema_id = self.table.table_schema.id

        partition_columns = list(zip(*(entry.partition.values for entry in entries))) if entries else []
        partition_null_counts = [sum(1 for value in col if value is None) for col in partition_columns]
        partition_min_stats = [
            min((v for v in col if v is not None), default=None) for col in partition_columns
        ]
        partition_max_stats = [
            max((v for v in col if v is not None), default=None) for col in partition_columns
        ]

        min_row_id = None
        max_row_id = None
        for entry in entries:
            if entry.file.first_row_id is None:
                min_row_id = None
                max_row_id = None
                break
            file_range = entry.file.row_id_range()
            if min_row_id is None or file_range.from_ < min_row_id:
                min_row_id = file_range.from_
            if max_row_id is None or file_range.to > max_row_id:
                max_row_id = file_range.to

        if file_size is None:
            manifest_file_path = f"{self.manifest_path}/{file_name}"
            file_size = self.table.file_io.get_file_size(manifest_file_path)
        return ManifestFileMeta(
            file_name=file_name,
            file_size=file_size,
            num_added_files=added_file_count,
            num_deleted_files=deleted_file_count,
            partition_stats=SimpleStats(
                min_values=GenericRow(
                    values=partition_min_stats,
                    fields=self.table.partition_keys_fields
                ),
                max_values=GenericRow(
                    values=partition_max_stats,
                    fields=self.table.partition_keys_fields
                ),
                null_counts=partition_null_counts,
            ),
            schema_id=schema_id,
            min_row_id=min_row_id,
            max_row_id=max_row_id,
        )
