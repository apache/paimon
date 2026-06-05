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
from typing import Dict, List, Tuple, Set

from pypaimon.common.identifier import Identifier
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.table.row.blob import BlobDescriptor, BlobViewStruct
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
                 read_fields: List, row_ranges: List[Range]):
        self.identifier: Identifier = identifier
        self.upstream_table = upstream_table
        self.read_fields: List = read_fields
        self.row_ranges: List[Range] = row_ranges


class BlobViewLookup:
    """Resolve BlobViewStruct references by reading upstream blob descriptors."""

    def __init__(self, table):
        self._table = table
        self._descriptor_cache: Dict[BlobViewStruct, BlobDescriptor] = {}
        self._null_value_cache: Set[BlobViewStruct] = set()

    def preload(self, view_structs: List[BlobViewStruct]):
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

    def resolve_to_null(self, view_struct: BlobViewStruct) -> bool:
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
        upstream_table = self._load_table(table_refs.identifier)

        fields: List = []
        for field_id in table_refs.references_by_field:
            fields.append(self._field_by_id(upstream_table, field_id))

        read_fields = SpecialFields.row_type_with_row_id(fields)
        return TableReadPlan(
            table_refs.identifier, upstream_table, read_fields,
            Range.to_ranges(table_refs.row_ids))

    def _load_descriptor_chunk(
        self, plan: TableReadPlan, row_ranges: List[Range]
    ) -> Tuple[Dict[BlobViewStruct, BlobDescriptor], set]:
        identifier: Identifier = plan.identifier
        upstream_table = plan.upstream_table
        read_fields = plan.read_fields

        projection_field_names: List[str] = [f.name for f in read_fields]

        descriptor_table = upstream_table.copy({CoreOptions.BLOB_AS_DESCRIPTOR.key(): "true"})
        read_builder = descriptor_table.new_read_builder().with_projection(projection_field_names)

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
        catalog = self._table.catalog_environment.catalog_loader.load()
        return catalog.get_table(identifier)

    @staticmethod
    def _field_by_id(table, field_id: int) -> 'DataField':
        for field in table.table_schema.fields:
            if field.id == field_id:
                return field
        raise ValueError(
            "Cannot find blob fieldId {} in upstream table {}."
            .format(field_id, table.identifier.get_full_name())
        )
