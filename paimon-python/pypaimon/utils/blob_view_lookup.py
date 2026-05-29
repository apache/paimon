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
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Iterable, List, Tuple

from pypaimon.common.identifier import Identifier
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.uri_reader import UriReader
from pypaimon.schema.schema_manager import SchemaManager
from pypaimon.table.row.blob import Blob, BlobDescriptor, BlobViewStruct
from pypaimon.table.special_fields import SpecialFields
from pypaimon.utils.range import Range

_PRELOAD_THREAD_NUM = 100
_MIN_ROWS_PER_TASK = 100


class TableReferences:
    """Groups BlobViewStruct references by upstream table."""

    def __init__(self, identifier: Identifier):
        self.identifier: Identifier = identifier
        self.references_by_field: Dict[int, List[BlobViewStruct]] = {}
        self.row_ids: List[int] = []

    def add(self, view_struct: BlobViewStruct) -> None:
        self.references_by_field.setdefault(view_struct.field_id, []).append(view_struct)
        self.row_ids.append(int(view_struct.row_id))


class TableReadPlan:
    """A plan for reading blob descriptors from one upstream table."""

    def __init__(self, identifier: Identifier, upstream_table,
                 fields: List, row_ranges: List[Range]):
        self.identifier: Identifier = identifier
        self.upstream_table = upstream_table
        self.fields: List = fields
        self.row_ranges: List[Range] = row_ranges


class BlobViewLookup:
    """Resolve BlobViewStruct references by reading upstream blob descriptors."""

    def __init__(self, table):
        self._table = table
        self._table_cache: Dict[str, object] = {}
        self._uri_reader_cache: Dict[str, UriReader] = {}
        self._descriptor_cache: Dict[BlobViewStruct, BlobDescriptor] = {}

    def preload(self, view_structs: Iterable[BlobViewStruct]) -> None:
        unique_structs: List[BlobViewStruct] = []
        for view_struct in view_structs:
            if view_struct not in self._descriptor_cache:
                unique_structs.append(view_struct)
        if not unique_structs:
            return
        resolved: Dict[BlobViewStruct, BlobDescriptor] = self._preload_descriptors(unique_structs)
        self._descriptor_cache.update(resolved)

    def resolve_descriptor(self, view_struct: BlobViewStruct) -> BlobDescriptor:
        descriptor: BlobDescriptor = self._descriptor_cache.get(view_struct)
        if descriptor is None:
            self.preload([view_struct])
            descriptor = self._descriptor_cache.get(view_struct)
        if descriptor is None:
            raise ValueError(
                "Cannot resolve BlobViewStruct {} because row id {} was not found "
                "in upstream table.".format(view_struct, view_struct.row_id)
            )
        return descriptor

    def resolve_data(self, view_struct: BlobViewStruct) -> bytes:
        descriptor = self.resolve_descriptor(view_struct)
        upstream_table = self._load_table(view_struct.identifier)
        uri_reader = self._get_or_create_uri_reader(upstream_table, descriptor)
        return Blob.from_descriptor(uri_reader, descriptor).to_data()

    def _preload_descriptors(
        self, view_structs: List[BlobViewStruct]) -> Dict[BlobViewStruct, BlobDescriptor]:
        if not view_structs:
            return {}

        grouped: Dict[str, TableReferences] = self._group_by_table(view_structs)
        plans: List[TableReadPlan] = []
        for table_refs in grouped.values():
            plans.append(self._create_table_read_plan(table_refs))

        target_rows: int = self._target_rows_per_task(plans)
        tasks: List[Tuple[TableReadPlan, List[Tuple[int, int]]]] = []
        for plan in plans:
            for range_chunk in self._split_row_ranges(plan.row_ranges, target_rows):
                tasks.append((plan, range_chunk))

        if len(tasks) <= 1:
            resolved = {}
            for plan, range_chunk in tasks:
                resolved.update(self._load_descriptor_chunk(plan, range_chunk))
            return resolved

        resolved = {}
        with ThreadPoolExecutor(max_workers=min(_PRELOAD_THREAD_NUM, len(tasks))) as executor:
            futures = {
                executor.submit(self._load_descriptor_chunk, plan, range_chunk): (plan, range_chunk)
                for plan, range_chunk in tasks
            }
            for future in as_completed(futures):
                try:
                    resolved.update(future.result())
                except Exception as exc:
                    raise RuntimeError("Failed to preload blob descriptors.") from exc
        return resolved

    def _group_by_table(
        self, view_structs: List[BlobViewStruct]
    ) -> Dict[str, TableReferences]:
        grouped: Dict[str, TableReferences] = {}
        for view_struct in view_structs:
            key = view_struct.identifier.get_full_name()
            if key not in grouped:
                grouped[key] = TableReferences(view_struct.identifier)
            grouped[key].add(view_struct)
        return grouped

    def _create_table_read_plan(self, table_refs: TableReferences) -> TableReadPlan:
        upstream_table = self._load_table(table_refs.identifier)

        fields: List = []
        for field_id in table_refs.references_by_field:
            fields.append(self._field_by_id(upstream_table, field_id))

        row_ranges: List[Tuple[int, int]] = self._to_sorted_distinct_ranges(table_refs.row_ids)
        return TableReadPlan(table_refs.identifier, upstream_table, fields, row_ranges)

    def _load_descriptor_chunk(
        self, plan: TableReadPlan, row_ranges: List[Range]
    ) -> Dict[BlobViewStruct, BlobDescriptor]:
        identifier: Identifier = plan.identifier
        upstream_table = plan.upstream_table
        fields: List = plan.fields

        field_names: List[str] = [f.name for f in fields]
        projection: List[str] = field_names + [SpecialFields.ROW_ID.name]

        descriptor_table = upstream_table.copy({CoreOptions.BLOB_AS_DESCRIPTOR.key(): "true"})
        read_builder = descriptor_table.new_read_builder().with_projection(projection)

        if SpecialFields.ROW_ID.name not in [
            data_field.name for data_field in read_builder.read_type()
        ]:
            raise ValueError(
                "Cannot resolve blob view for table {} because row tracking is not readable."
                .format(identifier.get_full_name())
            )

        predicate_builder = read_builder.new_predicate_builder()
        range_predicates: List = []
        for r in row_ranges:
            if r.from_ == r.to:
                range_predicates.append(
                    predicate_builder.equal(SpecialFields.ROW_ID.name, r.from_))
            else:
                range_predicates.append(
                    predicate_builder.between(SpecialFields.ROW_ID.name, r.from_, r.to))
        if len(range_predicates) == 1:
            predicate = range_predicates[0]
        else:
            predicate = predicate_builder.or_predicates(range_predicates)
        read_builder.with_filter(predicate)
        result = read_builder.new_read().to_arrow(read_builder.new_scan().plan().splits())

        if SpecialFields.ROW_ID.name not in result.schema.names:
            raise ValueError(
                "Cannot resolve blob view for table {} because row tracking is not readable."
                .format(identifier.get_full_name())
            )

        row_id_values: List = result.column(SpecialFields.ROW_ID.name).to_pylist()
        resolved: Dict[BlobViewStruct, BlobDescriptor] = {}
        for field in fields:
            if field.name not in result.schema.names:
                continue
            values = result.column(field.name).to_pylist()
            for row_id, value in zip(row_id_values, values):
                if value is None:
                    continue
                descriptor = self._to_descriptor(value)
                view_struct = BlobViewStruct(
                    identifier.get_full_name(), field.id, int(row_id))
                resolved[view_struct] = descriptor
        return resolved

    @staticmethod
    def _to_sorted_distinct_ranges(row_ids: List[int]) -> List[Range]:
        return Range.to_ranges(row_ids)

    @staticmethod
    def _split_row_ranges(
        row_ranges: List[Range], target_rows_per_task: int
    ) -> List[List[Range]]:
        if not row_ranges:
            return []

        chunks: List[List[Range]] = []
        current_chunk: List[Range] = []
        current_chunk_rows: int = 0
        for r in row_ranges:
            next_from = r.from_
            while next_from <= r.to:
                if current_chunk_rows == target_rows_per_task:
                    chunks.append(current_chunk)
                    current_chunk = []
                    current_chunk_rows = 0
                remaining = target_rows_per_task - current_chunk_rows
                next_to = min(r.to, next_from + remaining - 1)
                current_chunk.append(Range(next_from, next_to))
                current_chunk_rows += next_to - next_from + 1
                next_from = next_to + 1
        if current_chunk:
            chunks.append(current_chunk)
        return chunks

    @staticmethod
    def _target_rows_per_task(plans: List[TableReadPlan]) -> int:
        total_rows: int = 0
        for plan in plans:
            for r in plan.row_ranges:
                total_rows += r.count()
        if total_rows <= 0:
            return _MIN_ROWS_PER_TASK
        target = (total_rows + _PRELOAD_THREAD_NUM - 1) // _PRELOAD_THREAD_NUM
        return max(_MIN_ROWS_PER_TASK, target)

    def _load_table(self, identifier: Identifier):
        key: str = identifier.get_full_name()
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
        current_table_path: str = self._table.table_path.rstrip("/")
        current_db_path: str = os.path.dirname(current_table_path)
        warehouse: str = os.path.dirname(current_db_path)
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

    def _get_or_create_uri_reader(self, table, descriptor: BlobDescriptor) -> UriReader:
        cache_key: str = table.identifier.get_full_name()
        if cache_key in self._uri_reader_cache:
            return self._uri_reader_cache[cache_key]
        uri_reader_factory = getattr(table.file_io, "uri_reader_factory", None)
        if uri_reader_factory is not None:
            uri_reader = uri_reader_factory.create(descriptor.uri)
        else:
            uri_reader = UriReader.from_file(table.file_io)
        self._uri_reader_cache[cache_key] = uri_reader
        return uri_reader
