#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from io import BytesIO
from typing import List, Optional

import fastavro

from pypaimon.globalindex.global_index_meta import GlobalIndexMeta
from pypaimon.index.deletion_vector_meta import DeletionVectorMeta
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.table.row.generic_row import GenericRowDeserializer


class IndexManifestFile:
    """Index manifest file reader for reading index manifest entries."""

    DELETION_VECTORS_INDEX = "DELETION_VECTORS"

    def __init__(self, table):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        manifest_path = table.table_path.rstrip('/')
        self.manifest_path = f"{manifest_path}/manifest"
        self.file_io = table.file_io
        self.partition_keys_fields = self.table.partition_keys_fields

    def read(self, index_manifest_name: str) -> List[IndexManifestEntry]:
        index_manifest_path = f"{self.manifest_path}/{index_manifest_name}"

        if not self.file_io.exists(index_manifest_path):
            return []

        with self.file_io.new_input_stream(index_manifest_path) as input_stream:
            file_bytes = input_stream.read()
        return self._read_avro(file_bytes, index_manifest_path)

    def _read_avro(self, file_bytes: bytes, index_manifest_path: str) -> List[IndexManifestEntry]:
        entries = []
        try:
            buffer = BytesIO(file_bytes)
            reader = fastavro.reader(buffer)

            for record in reader:
                # Parse deletion vector ranges
                dv_list = record.get('_DELETIONS_VECTORS_RANGES') or []
                file_dv_dict = {}
                for dv_meta_record in dv_list:
                    dv_meta = DeletionVectorMeta(
                        data_file_name=dv_meta_record['f0'],
                        offset=dv_meta_record['f1'],
                        length=dv_meta_record['f2'],
                        cardinality=dv_meta_record.get('_CARDINALITY')
                    )
                    file_dv_dict[dv_meta.data_file_name] = dv_meta

                # Parse global index meta if present
                global_index_meta = self._parse_global_index_meta(
                    record.get('_GLOBAL_INDEX')
                )

                # Create IndexFileMeta
                index_file_meta = IndexFileMeta(
                    index_type=record['_INDEX_TYPE'],
                    file_name=record['_FILE_NAME'],
                    file_size=record['_FILE_SIZE'],
                    row_count=record['_ROW_COUNT'],
                    dv_ranges=file_dv_dict if file_dv_dict else None,
                    external_path=record.get('_EXTERNAL_PATH'),
                    global_index_meta=global_index_meta
                )

                # Create IndexManifestEntry
                entry = IndexManifestEntry(
                    kind=record['_KIND'],
                    partition=GenericRowDeserializer.from_bytes(
                        record['_PARTITION'],
                        self.partition_keys_fields
                    ),
                    bucket=record['_BUCKET'],
                    index_file=index_file_meta
                )
                entries.append(entry)

        except Exception as e:
            raise RuntimeError(
                f"Failed to read Avro index manifest file {index_manifest_path}: {e}"
            ) from e

        return entries

    def _parse_global_index_meta(self, global_index_record) -> Optional[GlobalIndexMeta]:
        """Parse global index meta from Avro record."""
        if global_index_record is None:
            return None

        return GlobalIndexMeta(
            row_range_start=global_index_record.get('_ROW_RANGE_START', 0),
            row_range_end=global_index_record.get('_ROW_RANGE_END', 0),
            index_field_id=global_index_record.get('_INDEX_FIELD_ID', 0),
            index_meta=global_index_record.get('_INDEX_META')
        )
