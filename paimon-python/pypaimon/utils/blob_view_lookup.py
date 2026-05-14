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

import os
from typing import Dict, Iterable, Tuple

from pypaimon.common.identifier import Identifier
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.uri_reader import UriReader
from pypaimon.schema.schema_manager import SchemaManager
from pypaimon.table.row.blob import Blob, BlobDescriptor, BlobViewStruct
from pypaimon.table.special_fields import SpecialFields


class BlobViewLookup:
    """Resolve BlobViewStruct references by reading upstream blob descriptors."""

    def __init__(self, table):
        self._table = table
        self._table_cache = {}
        self._field_descriptor_cache: Dict[Tuple[str, int], Dict[int, BlobDescriptor]] = {}

    def preload(self, view_structs: Iterable[BlobViewStruct]) -> None:
        requests = {}
        for view_struct in view_structs:
            key = (view_struct.identifier.get_full_name(), view_struct.field_id)
            if key not in requests:
                requests[key] = (view_struct.identifier, set())
            requests[key][1].add(int(view_struct.row_id))

        for key, (identifier, row_ids) in requests.items():
            descriptors = self._field_descriptor_cache.setdefault(key, {})
            missing_row_ids = sorted(row_id for row_id in row_ids if row_id not in descriptors)
            if not missing_row_ids:
                continue
            descriptors.update(self._load_field_descriptors(identifier, key[1], missing_row_ids))

    def resolve_descriptor(self, view_struct: BlobViewStruct) -> BlobDescriptor:
        self.preload([view_struct])
        key = (view_struct.identifier.get_full_name(), view_struct.field_id)
        descriptors = self._field_descriptor_cache[key]
        descriptor = descriptors.get(int(view_struct.row_id))
        if descriptor is None:
            raise ValueError(
                "Cannot resolve BlobViewStruct {} because row id {} was not found "
                "in upstream table.".format(view_struct, view_struct.row_id)
            )
        return descriptor

    def resolve_data(self, view_struct: BlobViewStruct) -> bytes:
        descriptor = self.resolve_descriptor(view_struct)
        upstream_table = self._load_table(view_struct.identifier)
        uri_reader = self._create_uri_reader(upstream_table, descriptor)
        return Blob.from_descriptor(uri_reader, descriptor).to_data()

    def _load_field_descriptors(
            self,
            identifier: Identifier,
            field_id: int,
            row_ids: Iterable[int]) -> Dict[int, BlobDescriptor]:
        row_ids = list(row_ids)
        if not row_ids:
            return {}

        upstream_table = self._load_table(identifier)
        field = self._field_by_id(upstream_table, field_id)
        descriptor_table = upstream_table.copy({CoreOptions.BLOB_AS_DESCRIPTOR.key(): "true"})
        read_builder = descriptor_table.new_read_builder().with_projection(
            [field.name, SpecialFields.ROW_ID.name]
        )
        if SpecialFields.ROW_ID.name not in [data_field.name for data_field in read_builder.read_type()]:
            raise ValueError(
                "Cannot resolve blob view for table {} because row tracking is not readable."
                .format(identifier.get_full_name())
            )
        predicate_builder = read_builder.new_predicate_builder()
        if len(row_ids) == 1:
            predicate = predicate_builder.equal(SpecialFields.ROW_ID.name, row_ids[0])
        else:
            predicate = predicate_builder.is_in(SpecialFields.ROW_ID.name, row_ids)
        read_builder.with_filter(predicate)
        result = read_builder.new_read().to_arrow(read_builder.new_scan().plan().splits())

        if SpecialFields.ROW_ID.name not in result.schema.names:
            raise ValueError(
                "Cannot resolve blob view for table {} because row tracking is not readable."
                .format(identifier.get_full_name())
            )
        if field.name not in result.schema.names:
            raise ValueError(
                "Cannot resolve blob field {} in upstream table {}."
                .format(field_id, identifier.get_full_name())
            )

        row_ids = result.column(SpecialFields.ROW_ID.name).to_pylist()
        values = result.column(field.name).to_pylist()
        descriptors = {}
        for row_id, value in zip(row_ids, values):
            if value is None:
                continue
            descriptor = self._to_descriptor(value)
            descriptors[int(row_id)] = descriptor
        return descriptors

    def _load_table(self, identifier: Identifier):
        key = identifier.get_full_name()
        if key in self._table_cache:
            return self._table_cache[key]

        catalog_loader = self._table.catalog_environment.catalog_loader
        if catalog_loader is not None:
            catalog = catalog_loader.load()
            table = catalog.get_table(identifier)
        else:
            table = self._load_filesystem_table(identifier)

        self._table_cache[key] = table
        return table

    def _load_filesystem_table(self, identifier: Identifier):
        from pypaimon.table.file_store_table import FileStoreTable

        table_path = self._filesystem_table_path(identifier)
        schema_manager = SchemaManager(
            self._table.file_io,
            table_path,
            branch=identifier.get_branch_name_or_default(),
        )
        table_schema = schema_manager.latest()
        if table_schema is None:
            raise ValueError("Cannot find upstream table at path: {}".format(table_path))
        return FileStoreTable(self._table.file_io, identifier, table_path, table_schema)

    def _filesystem_table_path(self, identifier: Identifier) -> str:
        current_table_path = self._table.table_path.rstrip("/")
        current_db_path = os.path.dirname(current_table_path)
        warehouse = os.path.dirname(current_db_path)
        return "{}/{}.db/{}".format(
            warehouse.rstrip("/"),
            identifier.get_database_name(),
            identifier.get_table_name(),
        )

    @staticmethod
    def _field_by_id(table, field_id: int):
        for field in table.table_schema.fields:
            if field.id == field_id:
                return field
        raise ValueError(
            "Cannot find blob fieldId {} in upstream table {}."
            .format(field_id, table.identifier.get_full_name())
        )

    def _to_descriptor(self, value) -> BlobDescriptor:
        if hasattr(value, "as_py"):
            value = value.as_py()
        if isinstance(value, str):
            value = value.encode("utf-8")
        if isinstance(value, bytearray):
            value = bytes(value)
        if not isinstance(value, bytes):
            raise ValueError("Blob view upstream value must be serialized blob bytes.")
        if BlobViewStruct.is_blob_view_struct(value):
            return self.resolve_descriptor(BlobViewStruct.deserialize(value))
        if not BlobDescriptor.is_blob_descriptor(value):
            raise ValueError("Blob view upstream value is not a serialized BlobDescriptor.")
        return BlobDescriptor.deserialize(value)

    @staticmethod
    def _create_uri_reader(table, descriptor: BlobDescriptor) -> UriReader:
        uri_reader_factory = getattr(table.file_io, "uri_reader_factory", None)
        if uri_reader_factory is not None:
            return uri_reader_factory.create(descriptor.uri)
        return UriReader.from_file(table.file_io)
