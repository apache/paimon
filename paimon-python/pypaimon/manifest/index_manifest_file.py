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

import uuid
from io import BytesIO
from typing import List, Optional

import fastavro

from pypaimon.globalindex.global_index_meta import GlobalIndexMeta
from pypaimon.index.deletion_vector_meta import DeletionVectorMeta
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.table.row.generic_row import (GenericRowDeserializer,
                                            GenericRowSerializer)
from pypaimon.utils.file_store_path_factory import FileStorePathFactory

# DV and global-index sub-schemas required by INDEX_MANIFEST_ENTRY_SCHEMA for
# Avro compatibility with Java; values are always null in data-evolution tables.
_DELETION_VECTOR_META_SCHEMA = {
    "type": "record",
    "name": "DeletionVectorMeta",
    "fields": [
        {"name": "f0", "type": "string"},
        {"name": "f1", "type": "long"},
        {"name": "f2", "type": "int"},
        {"name": "_CARDINALITY", "type": ["null", "long"], "default": None},
    ],
}

_GLOBAL_INDEX_META_SCHEMA = {
    "type": "record",
    "name": "GlobalIndexMeta",
    "fields": [
        {"name": "_ROW_RANGE_START", "type": "long"},
        {"name": "_ROW_RANGE_END", "type": "long"},
        {"name": "_INDEX_FIELD_ID", "type": "int"},
        {"name": "_EXTRA_FIELD_IDS",
         "type": ["null", {"type": "array", "items": "int"}], "default": None},
        {"name": "_INDEX_META", "type": ["null", "bytes"], "default": None},
    ],
}

INDEX_MANIFEST_ENTRY_SCHEMA = {
    "type": "record",
    "name": "IndexManifestEntry",
    "fields": [
        {"name": "_VERSION", "type": "int"},
        {"name": "_KIND", "type": "int"},
        {"name": "_PARTITION", "type": "bytes"},
        {"name": "_BUCKET", "type": "int"},
        {"name": "_INDEX_TYPE", "type": "string"},
        {"name": "_FILE_NAME", "type": "string"},
        {"name": "_FILE_SIZE", "type": "long"},
        {"name": "_ROW_COUNT", "type": "long"},
        {"name": "_DELETIONS_VECTORS_RANGES",
         "type": ["null", {"type": "array", "items": _DELETION_VECTOR_META_SCHEMA}],
         "default": None},
        {"name": "_EXTERNAL_PATH", "type": ["null", "string"], "default": None},
        {"name": "_GLOBAL_INDEX",
         "type": ["null", _GLOBAL_INDEX_META_SCHEMA], "default": None},
    ],
}

_INDEX_ENTRY_VERSION = 1


class IndexManifestFile:

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

        # Detect format: Avro files start with 'Obj' magic bytes
        if file_bytes.startswith(b'Obj'):
            return self._read_avro(file_bytes, index_manifest_path)
        else:
            # Fallback to JSON format (used by Python tests)
            return self._read_json(file_bytes, index_manifest_path)

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

    def _read_json(
        self, file_bytes: bytes, index_manifest_path: str
    ) -> List[IndexManifestEntry]:
        """Read JSON formatted index manifest (used by Python tests)."""
        import json

        entries = []
        try:
            data = json.loads(file_bytes.decode('utf-8'))
            for record in data:
                index_file_data = record.get('index_file', {})
                global_index_data = index_file_data.get('global_index_meta')

                global_index_meta = None
                if global_index_data:
                    global_index_meta = GlobalIndexMeta(
                        row_range_start=global_index_data.get('row_range_start', 0),
                        row_range_end=global_index_data.get('row_range_end', 0),
                        index_field_id=global_index_data.get('index_field_id', 0),
                        index_meta=global_index_data.get('index_meta')
                    )

                index_file_meta = IndexFileMeta(
                    index_type=index_file_data.get('index_type', ''),
                    file_name=index_file_data.get('file_name', ''),
                    file_size=index_file_data.get('file_size', 0),
                    row_count=index_file_data.get('row_count', 0),
                    dv_ranges=None,
                    external_path=index_file_data.get('external_path'),
                    global_index_meta=global_index_meta
                )

                from pypaimon.table.row.generic_row import GenericRow
                partition_values = record.get('partition', [])
                # Use partition key fields from table schema
                partition = GenericRow(partition_values, self.partition_keys_fields)

                entry = IndexManifestEntry(
                    kind=record.get('kind', 0),
                    partition=partition,
                    bucket=record.get('bucket', 0),
                    index_file=index_file_meta
                )
                entries.append(entry)

        except Exception as e:
            raise RuntimeError(
                f"Failed to read JSON index manifest file {index_manifest_path}: {e}"
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
            extra_field_ids=global_index_record.get('_EXTRA_FIELD_IDS'),
            index_meta=global_index_record.get('_INDEX_META')
        )

    def combine_deletes(
        self,
        previous_name: Optional[str],
        deletes: List[IndexManifestEntry],
    ) -> Optional[str]:
        if not deletes:
            return previous_name
        previous = self.read(previous_name) if previous_name else []
        delete_names = {e.index_file.file_name for e in deletes}
        survivors = [e for e in previous if e.index_file.file_name not in delete_names]
        if not survivors:
            return None
        return self.write(survivors)

    def write(self, entries: List[IndexManifestEntry]) -> str:
        file_name = f"{FileStorePathFactory.INDEX_MANIFEST_PREFIX}{uuid.uuid4()}"
        path = f"{self.manifest_path}/{file_name}"
        records = [self._to_avro_record(e) for e in entries]
        try:
            buffer = BytesIO()
            fastavro.writer(buffer, INDEX_MANIFEST_ENTRY_SCHEMA, records)
            with self.file_io.new_output_stream(path) as output_stream:
                output_stream.write(buffer.getvalue())
        except Exception as e:
            self.file_io.delete_quietly(path)
            raise RuntimeError(
                f"Exception occurs when writing records to {path}. Clean up."
            ) from e
        return file_name

    def _to_avro_record(self, entry: IndexManifestEntry) -> dict:
        index_file = entry.index_file
        dv_ranges = None
        if index_file.dv_ranges:
            dv_ranges = [
                {"f0": dv.data_file_name, "f1": dv.offset, "f2": dv.length,
                 "_CARDINALITY": dv.cardinality}
                for dv in index_file.dv_ranges.values()
            ]
        global_index = None
        if index_file.global_index_meta is not None:
            gim = index_file.global_index_meta
            global_index = {
                "_ROW_RANGE_START": gim.row_range_start,
                "_ROW_RANGE_END": gim.row_range_end,
                "_INDEX_FIELD_ID": gim.index_field_id,
                "_EXTRA_FIELD_IDS": gim.extra_field_ids,
                "_INDEX_META": gim.index_meta,
            }
        return {
            "_VERSION": _INDEX_ENTRY_VERSION,
            "_KIND": entry.kind,
            "_PARTITION": GenericRowSerializer.to_bytes(entry.partition),
            "_BUCKET": entry.bucket,
            "_INDEX_TYPE": index_file.index_type,
            "_FILE_NAME": index_file.file_name,
            "_FILE_SIZE": index_file.file_size,
            "_ROW_COUNT": index_file.row_count,
            "_DELETIONS_VECTORS_RANGES": dv_ranges,
            "_EXTERNAL_PATH": index_file.external_path,
            "_GLOBAL_INDEX": global_index,
        }
