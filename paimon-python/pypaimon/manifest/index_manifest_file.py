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
from typing import List

import fastavro

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
        """Read index manifest entries from the specified index manifest file."""
        index_manifest_path = f"{self.manifest_path}/{index_manifest_name}"

        if not self.file_io.exists(index_manifest_path):
            return []

        entries = []
        try:
            with self.file_io.new_input_stream(index_manifest_path) as input_stream:
                avro_bytes = input_stream.read()

            buffer = BytesIO(avro_bytes)
            reader = fastavro.reader(buffer)

            for record in reader:
                dv_list = record['_DELETIONS_VECTORS_RANGES']
                file_dv_dict = {}
                for dv_meta_record in dv_list:
                    dv_meta = DeletionVectorMeta(
                        data_file_name=dv_meta_record['f0'],
                        offset=dv_meta_record['f1'],
                        length=dv_meta_record['f2'],
                        cardinality=dv_meta_record.get('_CARDINALITY')
                    )
                    file_dv_dict[dv_meta.data_file_name] = dv_meta

                # Create IndexFileMeta
                index_file_meta = IndexFileMeta(
                    index_type=record['_INDEX_TYPE'],
                    file_name=record['_FILE_NAME'],
                    file_size=record['_FILE_SIZE'],
                    row_count=record['_ROW_COUNT'],
                    dv_ranges=file_dv_dict,
                    external_path=record['_EXTERNAL_PATH']
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
            raise RuntimeError(f"Failed to read index manifest file {index_manifest_path}: {e}") from e

        return entries
