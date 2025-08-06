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

import uuid
from io import BytesIO
from typing import List

import fastavro

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import (MANIFEST_ENTRY_SCHEMA,
                                                     ManifestEntry)
from pypaimon.table.row.binary_row import (BinaryRow, BinaryRowDeserializer,
                                           BinaryRowSerializer)


class ManifestFileManager:
    """Writer for manifest files in Avro format using unified FileIO."""

    def __init__(self, table):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.manifest_path = table.table_path / "manifest"
        self.file_io = table.file_io
        self.partition_key_fields = self.table.table_schema.get_partition_key_fields()
        self.primary_key_fields = self.table.table_schema.get_primary_key_fields()
        self.trimmed_primary_key_fields = self.table.table_schema.get_trimmed_primary_key_fields()

    def read(self, manifest_file_name: str) -> List[ManifestEntry]:
        manifest_file_path = self.manifest_path / manifest_file_name

        entries = []
        with self.file_io.new_input_stream(manifest_file_path) as input_stream:
            avro_bytes = input_stream.read()
        buffer = BytesIO(avro_bytes)
        reader = fastavro.reader(buffer)

        for record in reader:
            file_info = dict(record['_FILE'])
            file_meta = DataFileMeta(
                file_name=file_info['_FILE_NAME'],
                file_size=file_info['_FILE_SIZE'],
                row_count=file_info['_ROW_COUNT'],
                min_key=BinaryRowDeserializer.from_bytes(file_info['_MIN_KEY'], self.trimmed_primary_key_fields),
                max_key=BinaryRowDeserializer.from_bytes(file_info['_MAX_KEY'], self.trimmed_primary_key_fields),
                key_stats=None,  # TODO
                value_stats=None,  # TODO
                min_sequence_number=file_info['_MIN_SEQUENCE_NUMBER'],
                max_sequence_number=file_info['_MAX_SEQUENCE_NUMBER'],
                schema_id=file_info['_SCHEMA_ID'],
                level=file_info['_LEVEL'],
                extra_files=None,  # TODO
            )
            entry = ManifestEntry(
                kind=record['_KIND'],
                partition=BinaryRowDeserializer.from_bytes(record['_PARTITION'], self.partition_key_fields),
                bucket=record['_BUCKET'],
                total_buckets=record['_TOTAL_BUCKETS'],
                file=file_meta
            )
            entries.append(entry)
        return entries

    def write(self, commit_messages: List['CommitMessage']) -> List[str]:
        avro_records = []
        for message in commit_messages:
            partition_bytes = BinaryRowSerializer.to_bytes(
                BinaryRow(list(message.partition()), self.table.table_schema.get_partition_key_fields()))
            for file in message.new_files():
                avro_record = {
                    "_KIND": 0,
                    "_PARTITION": partition_bytes,
                    "_BUCKET": message.bucket(),
                    "_TOTAL_BUCKETS": -1,  # TODO
                    "_FILE": {
                        "_FILE_NAME": file.file_name,
                        "_FILE_SIZE": file.file_size,
                        "_ROW_COUNT": file.row_count,
                        "_MIN_KEY": BinaryRowSerializer.to_bytes(file.min_key),
                        "_MAX_KEY": BinaryRowSerializer.to_bytes(file.max_key),
                        "_KEY_STATS": {
                            "_MIN_VALUES": None,
                            "_MAX_VALUES": None,
                            "_NULL_COUNTS": 0,
                        },
                        "_VALUE_STATS": {
                            "_MIN_VALUES": None,
                            "_MAX_VALUES": None,
                            "_NULL_COUNTS": 0,
                        },
                        "_MIN_SEQUENCE_NUMBER": 0,
                        "_MAX_SEQUENCE_NUMBER": 0,
                        "_SCHEMA_ID": 0,
                        "_LEVEL": 0,
                        "_EXTRA_FILES": [],
                    }
                }
                avro_records.append(avro_record)

        manifest_filename = f"manifest-{str(uuid.uuid4())}.avro"
        manifest_path = self.manifest_path / manifest_filename
        try:
            buffer = BytesIO()
            fastavro.writer(buffer, MANIFEST_ENTRY_SCHEMA, avro_records)
            avro_bytes = buffer.getvalue()
            with self.file_io.new_output_stream(manifest_path) as output_stream:
                output_stream.write(avro_bytes)
            return [str(manifest_filename)]
        except Exception as e:
            self.file_io.delete_quietly(manifest_path)
            raise RuntimeError(f"Failed to write manifest file: {e}") from e
