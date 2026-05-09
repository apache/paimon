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

from typing import List, Optional

from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read.table_read import TableRead
from pypaimon.read.table_scan import TableScan
from pypaimon.schema.data_types import DataField
from pypaimon.table.special_fields import SpecialFields
from pypaimon.utils.projection import Projection, _is_row_type


class ReadBuilder:
    """Implementation of ReadBuilder for native Python reading."""

    def __init__(self, table):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self._predicate: Optional[Predicate] = None
        # ``_projection`` is the user-facing list of names from
        # :meth:`with_projection`. ``_nested_paths`` is the canonical
        # integer-path form (length-1 for top level, longer for nested
        # ROW children) used by every consumer in this module.
        # ``with_nested_projection`` populates only the latter; the two
        # never both hold meaningful state simultaneously.
        self._projection: Optional[List[str]] = None
        self._nested_paths: Optional[List[List[int]]] = None
        self._limit: Optional[int] = None

    def with_filter(self, predicate: Predicate) -> 'ReadBuilder':
        self._predicate = predicate
        return self

    def with_projection(self, projection: List[str]) -> 'ReadBuilder':
        """Project to the given column names.

        Names containing a dot (e.g. ``"struct.subfield"``) walk into ROW
        children and are translated into a nested projection. Top-level-
        only callers see the same observable behaviour as before — the
        dotted form is opt-in. Unknown names are silently skipped to
        preserve the pre-existing contract.
        """
        self._projection = projection
        if projection and any('.' in name for name in projection):
            self._nested_paths = self._resolve_dotted_paths(projection)
        else:
            self._nested_paths = None
        return self

    def with_limit(self, limit: int) -> 'ReadBuilder':
        self._limit = limit
        return self

    def new_scan(self) -> TableScan:
        return TableScan(
            table=self.table,
            predicate=self._predicate,
            limit=self._limit
        )

    def new_read(self) -> TableRead:
        return TableRead(
            table=self.table,
            predicate=self._predicate,
            read_type=self.read_type(),
            nested_name_paths=self._nested_name_paths(),
        )

    def _nested_name_paths(self) -> Optional[List[List[str]]]:
        """Resolve the current nested-projection state into a parallel list
        of name paths against the underlying table schema. Returns ``None``
        if the user only requested top-level projection (or no projection).
        """
        if not self._nested_paths:
            return None
        table_fields = self.table.fields
        if self.table.options.row_tracking_enabled():
            table_fields = SpecialFields.row_type_with_row_tracking(table_fields)
        return Projection.of(self._nested_paths).to_name_paths(table_fields)

    def new_predicate_builder(self) -> PredicateBuilder:
        return PredicateBuilder(self.read_type())

    def read_type(self) -> List[DataField]:
        table_fields = self.table.fields

        if not self._projection and not self._nested_paths:
            return table_fields

        if self.table.options.row_tracking_enabled():
            table_fields = SpecialFields.row_type_with_row_tracking(table_fields)

        if self._nested_paths:
            return Projection.of(self._nested_paths).project(table_fields)

        field_map = {field.name: field for field in table_fields}
        return [field_map[name] for name in self._projection if name in field_map]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_dotted_paths(self, names: List[str]) -> List[List[int]]:
        """Translate dotted-name projection entries into integer paths
        against the current table schema. Names without dots produce
        length-1 paths.
        """
        table_fields = self.table.fields
        if self.table.options.row_tracking_enabled():
            table_fields = SpecialFields.row_type_with_row_tracking(table_fields)
        top_index = {f.name: i for i, f in enumerate(table_fields)}

        paths: List[List[int]] = []
        for name in names:
            if '.' not in name:
                if name not in top_index:
                    # Silently skip unknown names — preserves the
                    # pre-existing contract from the plain top-level path.
                    continue
                paths.append([top_index[name]])
                continue
            parts = name.split('.')
            top = parts[0]
            if top not in top_index:
                continue
            path = [top_index[top]]
            current_field = table_fields[path[0]]
            ok = True
            for part in parts[1:]:
                if not _is_row_type(current_field.type):
                    ok = False
                    break
                child_fields = current_field.type.fields
                child_idx = next(
                    (i for i, f in enumerate(child_fields) if f.name == part),
                    -1)
                if child_idx < 0:
                    ok = False
                    break
                path.append(child_idx)
                current_field = child_fields[child_idx]
            if ok:
                paths.append(path)
        return paths
