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

from io import BytesIO
from typing import List

import fastavro

from pypaimon.manifest.schema.manifest_file_meta import (
    MANIFEST_FILE_META_SCHEMA, ManifestFileMeta)
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.table.row.binary_row import BinaryRow
from pypaimon.table.row.generic_row import GenericRowSerializer


class ManifestListManager:
    """Manager for manifest list files in Avro format using unified FileIO."""

    def __init__(self, table):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        manifest_path = table.table_path.rstrip('/')
        self.manifest_path = f"{manifest_path}/manifest"
        self.file_io = self.table.file_io

    def read_all(self, snapshot: Snapshot) -> List[ManifestFileMeta]:
        manifest_files = []
        base_manifests = self.read(snapshot.base_manifest_list)
        manifest_files.extend(base_manifests)
        delta_manifests = self.read(snapshot.delta_manifest_list)
        manifest_files.extend(delta_manifests)
        return manifest_files

    def read_delta(self, snapshot: Snapshot) -> List[ManifestFileMeta]:
        return self.read(snapshot.delta_manifest_list)

    def read(self, manifest_list_name: str) -> List[ManifestFileMeta]:
        manifest_files = []

        manifest_list_path = f"{self.manifest_path}/{manifest_list_name}"
        with self.file_io.new_input_stream(manifest_list_path) as input_stream:
            avro_bytes = input_stream.read()
        buffer = BytesIO(avro_bytes)
        reader = fastavro.reader(buffer)
        for record in reader:
            stats_dict = dict(record['_PARTITION_STATS'])
            partition_stats = SimpleStats(
                min_values=BinaryRow(
                    stats_dict['_MIN_VALUES'],
                    self.table.partition_keys_fields
                ),
                max_values=BinaryRow(
                    stats_dict['_MAX_VALUES'],
                    self.table.partition_keys_fields
                ),
                null_counts=stats_dict['_NULL_COUNTS'],
            )
            manifest_file_meta = ManifestFileMeta(
                file_name=record['_FILE_NAME'],
                file_size=record['_FILE_SIZE'],
                num_added_files=record['_NUM_ADDED_FILES'],
                num_deleted_files=record['_NUM_DELETED_FILES'],
                partition_stats=partition_stats,
                schema_id=record['_SCHEMA_ID'],
            )
            manifest_files.append(manifest_file_meta)

        return manifest_files

    def write(self, file_name, manifest_file_metas: List[ManifestFileMeta]):
        avro_records = []
        for meta in manifest_file_metas:
            avro_record = {
                "_VERSION": 2,
                "_FILE_NAME": meta.file_name,
                "_FILE_SIZE": meta.file_size,
                "_NUM_ADDED_FILES": meta.num_added_files,
                "_NUM_DELETED_FILES": meta.num_deleted_files,
                "_PARTITION_STATS": {
                    "_MIN_VALUES": GenericRowSerializer.to_bytes(meta.partition_stats.min_values),
                    "_MAX_VALUES": GenericRowSerializer.to_bytes(meta.partition_stats.max_values),
                    "_NULL_COUNTS": meta.partition_stats.null_counts,
                },
                "_SCHEMA_ID": meta.schema_id,
            }
            avro_records.append(avro_record)

        list_path = f"{self.manifest_path}/{file_name}"
        try:
            buffer = BytesIO()
            fastavro.writer(buffer, MANIFEST_FILE_META_SCHEMA, avro_records)
            avro_bytes = buffer.getvalue()
            with self.file_io.new_output_stream(list_path) as output_stream:
                output_stream.write(avro_bytes)
        except Exception as e:
            self.file_io.delete_quietly(list_path)
            raise RuntimeError(f"Failed to write manifest list file: {e}") from e
