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

from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from typing import TYPE_CHECKING, Dict, List, Set, Tuple

from pypaimon.common.identifier import Identifier
from pypaimon.common.uri_reader import FileUriReader, UriReader
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.table.row.blob import Blob, BlobDescriptor, BlobViewStruct
from pypaimon.table.special_fields import SpecialFields
from pypaimon.utils.range import Range

if TYPE_CHECKING:
    from pypaimon.schema.data_types import DataField

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

    def __init__(self, identifier: Identifier,
                 read_fields: List, row_ranges: List[Range]):
        self.identifier: Identifier = identifier
        self.read_fields: List = read_fields
        self.row_ranges: List[Range] = row_ranges


class BlobViewLookup:
    """Resolve BlobViewStruct references by reading upstream blob descriptors."""

    def __init__(self, table):
        self._table = table
        self._descriptor_cache: Dict[BlobViewStruct, BlobDescriptor] = {}
        self._uri_reader_cache: Dict[str, UriReader] = {}
        self._uri_reader_file_ios: Dict[str, object] = {}
        # A cached UriReader may depend on a table-scoped FileIO owned by the
        # catalog which produced it. Keep that catalog alive until close() so
        # the FileIO is never invalidated while the reader is still in use.
        self._uri_reader_catalogs: Dict[str, object] = {}
        self._null_value_cache: Set[BlobViewStruct] = set()
        self._closed = False

    def preload(self, view_structs: List[BlobViewStruct]):
        self._check_open()
        if not view_structs:
            return

        grouped: Dict[str, TableReferences] = self._group_by_table(view_structs)
        plans: List[TableReadPlan] = []
        for table_refs in grouped.values():
            plans.append(self._create_table_read_plan(table_refs))

        target_rows: int = self._target_rows_per_task(plans)
        tasks: List[Tuple[TableReadPlan, List[Range]]] = []
        for plan in plans:
            for range_chunk in self._split_row_ranges(plan.row_ranges, target_rows):
                tasks.append((plan, range_chunk))

        if len(tasks) <= 1:
            for plan, range_chunk in tasks:
                descriptors, null_values = self._load_descriptor_chunk(plan, range_chunk)
                self._descriptor_cache.update(descriptors)
                self._null_value_cache.update(null_values)
            return

        with ThreadPoolExecutor(max_workers=min(_PRELOAD_THREAD_NUM, len(tasks))) as executor:
            futures = {
                executor.submit(self._load_descriptor_chunk, plan, range_chunk): (plan, range_chunk)
                for plan, range_chunk in tasks
            }
            for future in as_completed(futures):
                try:
                    descriptors, null_values = future.result()
                    self._descriptor_cache.update(descriptors)
                    self._null_value_cache.update(null_values)
                except Exception as exc:
                    # Cancel remaining futures that have not started yet so a single
                    # failure can abort the rest of the preload work as early as possible.
                    for pending_future in futures:
                        pending_future.cancel()
                    raise RuntimeError("Failed to preload blob descriptors.") from exc

    def resolve_descriptor(self, view_struct: BlobViewStruct) -> BlobDescriptor:
        self._check_open()
        descriptor: BlobDescriptor = self._descriptor_cache.get(view_struct)
        if descriptor is None:
            if view_struct in self._null_value_cache:
                raise ValueError(
                    "BlobViewStruct {} resolves to a null blob value.".format(view_struct)
                )
            raise ValueError(
                "Cannot resolve BlobViewStruct {} because row id {} was not found "
                "in upstream table.".format(view_struct, view_struct.row_id)
            )
        return descriptor

    def resolve_blob(self, view_struct: BlobViewStruct) -> Blob:
        descriptor = self.resolve_descriptor(view_struct)
        uri_reader = self.resolve_uri_reader(view_struct)
        return Blob.from_descriptor(uri_reader, descriptor)

    def resolve_file_io(self, view_struct: BlobViewStruct):
        uri_reader = self.resolve_uri_reader(view_struct)
        if not isinstance(uri_reader, FileUriReader):
            raise ValueError(
                "Cannot resolve BlobViewStruct {} with parallel blob reads because "
                "upstream table {} does not use a file-backed UriReader.".format(
                    view_struct, view_struct.identifier.get_full_name())
            )
        return uri_reader._file_io

    def resolve_uri_reader(self, view_struct: BlobViewStruct) -> UriReader:
        self._check_open()
        table_key = view_struct.identifier.get_full_name()
        uri_reader = self._uri_reader_cache.get(table_key)
        if uri_reader is None:
            catalog = self._create_catalog()
            try:
                upstream_table = catalog.get_table(view_struct.identifier)
                uri_reader = UriReader.from_file(upstream_table.file_io)
            except Exception:
                self._close_catalog(catalog)
                raise
            self._uri_reader_cache[table_key] = uri_reader
            self._uri_reader_file_ios[table_key] = upstream_table.file_io
            self._uri_reader_catalogs[table_key] = catalog
        return uri_reader

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        catalogs = list(self._uri_reader_catalogs.values())
        file_ios = list(self._uri_reader_file_ios.values())
        self._uri_reader_catalogs.clear()
        self._uri_reader_file_ios.clear()
        self._uri_reader_cache.clear()
        self._descriptor_cache.clear()
        self._null_value_cache.clear()
        first_error = None
        catalog_file_io_ids = {
            id(file_io)
            for file_io in (getattr(catalog, "file_io", None) for catalog in catalogs)
            if file_io is not None
        }
        for catalog in catalogs:
            try:
                self._close_catalog(catalog)
            except Exception as error:
                if first_error is None:
                    first_error = error
        closed_file_ios = set(catalog_file_io_ids)
        for file_io in file_ios:
            if id(file_io) in closed_file_ios:
                continue
            closed_file_ios.add(id(file_io))
            try:
                self._close_file_io(file_io)
            except Exception as error:
                if first_error is None:
                    first_error = error
        if first_error is not None:
            raise first_error

    def _check_open(self) -> None:
        if self._closed:
            raise RuntimeError("BlobViewLookup is already closed.")

    def resolve_to_null(self, view_struct: BlobViewStruct) -> bool:
        self._check_open()
        if view_struct in self._null_value_cache:
            return True
        if view_struct not in self._descriptor_cache:
            raise ValueError(
                "Cannot resolve BlobViewStruct {} because row id {} was not found "
                "in upstream table.".format(view_struct, view_struct.row_id)
            )
        return False

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
        with self._catalog_scope() as catalog:
            upstream_table = catalog.get_table(table_refs.identifier)
            try:
                fields: List = []
                for field_id in table_refs.references_by_field:
                    fields.append(self._field_by_id(upstream_table, field_id))
            finally:
                self._close_distinct_table_file_io(upstream_table, catalog)

        read_fields = SpecialFields.row_type_with_row_id(fields)
        return TableReadPlan(
            table_refs.identifier, read_fields,
            Range.to_ranges(table_refs.row_ids))

    def _load_descriptor_chunk(
        self, plan: TableReadPlan, row_ranges: List[Range]
    ) -> Tuple[Dict[BlobViewStruct, BlobDescriptor], set]:
        identifier: Identifier = plan.identifier
        read_fields = plan.read_fields

        projection_field_names: List[str] = [f.name for f in read_fields]

        with self._catalog_scope() as catalog:
            upstream_table = catalog.get_table(identifier)
            try:
                descriptor_table = upstream_table.copy(
                    {CoreOptions.BLOB_AS_DESCRIPTOR.key(): "true"})
                read_builder = descriptor_table.new_read_builder().with_projection(
                    projection_field_names)

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
                            predicate_builder.between(
                                SpecialFields.ROW_ID.name, r.from_, r.to))
                if len(range_predicates) == 1:
                    predicate = range_predicates[0]
                else:
                    predicate = predicate_builder.or_predicates(range_predicates)
                read_builder.with_filter(predicate)
                result = read_builder.new_read().to_arrow(
                    read_builder.new_scan().plan().splits())
            finally:
                self._close_distinct_table_file_io(upstream_table, catalog)

        if SpecialFields.ROW_ID.name not in result.schema.names:
            raise ValueError(
                "Cannot resolve blob view for table {} because row tracking is not readable."
                .format(identifier.get_full_name())
            )

        row_id_values: List = result.column(SpecialFields.ROW_ID.name).to_pylist()
        resolved: Dict[BlobViewStruct, BlobDescriptor] = {}
        null_values: set = set()
        for field in read_fields:
            if field.name == SpecialFields.ROW_ID.name:
                continue
            if field.name not in result.schema.names:
                continue
            values = result.column(field.name).to_pylist()
            for row_id, value in zip(row_id_values, values):
                view_struct = BlobViewStruct(
                    identifier.get_full_name(), field.id, int(row_id))
                if value is None:
                    null_values.add(view_struct)
                    continue
                descriptor = BlobDescriptor.deserialize(value)
                resolved[view_struct] = descriptor
        return resolved, null_values

    @staticmethod
    def _split_row_ranges(
        row_ranges: List[Range], target_rows_per_task: int
    ) -> List[List[Range]]:
        """
        Split row ranges into multiple chunks for parallel task processing.
        """
        if not row_ranges:
            return []

        chunks: List[List[Range]] = []
        current_chunk: List[Range] = []
        current_chunk_rows: int = 0

        for r in row_ranges:
            next_from = r.from_
            # Process current range until all rows are allocated
            while next_from <= r.to:
                # If current chunk is full, save it and start a new one
                if current_chunk_rows == target_rows_per_task:
                    chunks.append(current_chunk)
                    current_chunk = []
                    current_chunk_rows = 0

                # Calculate remaining capacity in current chunk
                remaining = target_rows_per_task - current_chunk_rows
                # Determine the end position for this allocation (don't exceed range boundary)
                next_to = min(r.to, next_from + remaining - 1)

                # Add the allocated range to current chunk
                current_chunk.append(Range(next_from, next_to))
                current_chunk_rows += next_to - next_from + 1

                # Move to next unallocated position
                next_from = next_to + 1

        # Don't forget the last chunk if it has any ranges
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

        return max(_MIN_ROWS_PER_TASK, (total_rows + _PRELOAD_THREAD_NUM - 1) // _PRELOAD_THREAD_NUM)

    def _load_table(self, identifier: Identifier):
        # Kept as a small compatibility helper for callers/tests which only
        # need table metadata. Internal read paths use _catalog_scope directly
        # so a catalog stays alive for the entire table operation.
        with self._catalog_scope() as catalog:
            return catalog.get_table(identifier)

    def _create_catalog(self):
        catalog_environment = self._table.catalog_environment
        catalog_loader = catalog_environment.catalog_loader
        dependency_context = catalog_environment.dependency_read_context()
        if dependency_context is catalog_environment.catalog_context():
            return catalog_loader.load()
        from pypaimon.catalog.catalog_factory import CatalogFactory
        return CatalogFactory.create_from_context(
            dependency_context, config_required=False)

    @contextmanager
    def _catalog_scope(self):
        catalog = self._create_catalog()
        try:
            yield catalog
        finally:
            self._close_catalog(catalog)

    @staticmethod
    def _close_catalog(catalog) -> None:
        file_io = getattr(catalog, "file_io", None)
        try:
            close = getattr(catalog, "close", None)
            if callable(close):
                close()
        finally:
            BlobViewLookup._close_file_io(file_io)

    @staticmethod
    def _close_distinct_table_file_io(table, catalog) -> None:
        table_file_io = getattr(table, "file_io", None)
        if table_file_io is not getattr(catalog, "file_io", None):
            BlobViewLookup._close_file_io(table_file_io)

    @staticmethod
    def _close_file_io(file_io) -> None:
        close = getattr(file_io, "close", None)
        if callable(close):
            close()

    @staticmethod
    def _field_by_id(table, field_id: int) -> 'DataField':
        for field in table.table_schema.fields:
            if field.id == field_id:
                return field
        raise ValueError(
            "Cannot find blob fieldId {} in upstream table {}."
            .format(field_id, table.identifier.get_full_name())
        )
