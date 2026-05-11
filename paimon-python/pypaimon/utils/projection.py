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

"""Column projection utilities.

A projection maps a source row type to a flat list of ``DataField``: the
columns the user wants to read. Two flavours:

* :class:`TopLevelProjection` selects fields by their top-level index.
* :class:`NestedProjection` accepts paths that walk into ROW children, e.g.
  ``[[1, 0], [1, 2]]`` means "the 0th and 2nd children of the field at top
  level index 1". The result is flattened into top-level fields whose
  names are the underscore-joined original path (``a_b`` for ``a.b``,
  with a ``__N`` suffix on collisions) and whose IDs are inherited from
  the leaf so schema-evolution remapping by field ID still works.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Sequence

from pypaimon.schema.data_types import DataField, RowType


class Projection(ABC):
    """Abstract base for column projection."""

    @abstractmethod
    def project(self, row_type) -> List[DataField]:
        """Apply the projection and return the resulting flat fields."""

    @abstractmethod
    def is_nested(self) -> bool:
        """Whether any path goes deeper than the top level."""

    @abstractmethod
    def to_top_level_indexes(self) -> List[int]:
        """Top-level positions touched by this projection.

        For nested projections, returns unique top-level indexes in path
        order. Useful for fallback paths that can only push down at the
        top level.
        """

    @abstractmethod
    def to_nested_indexes(self) -> List[List[int]]:
        """Return the projection as a list of paths, one per output field."""

    @abstractmethod
    def to_name_paths(self, row_type) -> List[List[str]]:
        """Translate integer paths to field-name paths against ``row_type``.

        For a path ``[1, 0]`` against a row whose top-level field at index 1
        is a struct ``mv_col`` with sub-fields ``[LATEST_VERSION, ...]``,
        returns ``[["mv_col", "LATEST_VERSION"], ...]``. Used by format
        readers to push nested projection down to the underlying engine
        (e.g. PyArrow's ``ds.field(*name_path)``).
        """

    # ------------------------------------------------------------------
    # Factories
    # ------------------------------------------------------------------

    @staticmethod
    def empty() -> "Projection":
        """The empty projection: no columns selected."""
        return _EmptyProjection()

    @staticmethod
    def of(indexes_or_paths) -> "Projection":
        """Build a projection from either ``int[]`` or ``int[][]``.

        Empty input returns :func:`empty`. The input must be uniformly
        shaped — either all integers or all sequences of integers; mixing
        the two raises ``TypeError`` so the failure is reported at the
        ``of`` call site rather than as an opaque error deep in
        ``project``.
        """
        if not indexes_or_paths:
            return _EmptyProjection()
        first_is_path = isinstance(indexes_or_paths[0], (list, tuple))
        for entry in indexes_or_paths[1:]:
            entry_is_path = isinstance(entry, (list, tuple))
            if entry_is_path != first_is_path:
                raise TypeError(
                    "Projection.of expects either all top-level indexes "
                    "or all nested paths; got a mix")
        if first_is_path:
            return NestedProjection([list(p) for p in indexes_or_paths])
        return TopLevelProjection(list(indexes_or_paths))

    @staticmethod
    def range(start_inclusive: int, end_exclusive: int) -> "Projection":
        """Top-level projection over a contiguous index range."""
        if end_exclusive <= start_inclusive:
            return _EmptyProjection()
        return TopLevelProjection(list(range(start_inclusive, end_exclusive)))


class _EmptyProjection(Projection):

    def project(self, row_type) -> List[DataField]:
        return []

    def is_nested(self) -> bool:
        return False

    def to_top_level_indexes(self) -> List[int]:
        return []

    def to_nested_indexes(self) -> List[List[int]]:
        return []

    def to_name_paths(self, row_type) -> List[List[str]]:
        return []


class TopLevelProjection(Projection):
    """Single-level projection: pick fields by their top-level index."""

    def __init__(self, indexes: Sequence[int]):
        self.indexes = list(indexes)

    def project(self, row_type) -> List[DataField]:
        fields = _row_fields(row_type)
        return [fields[i] for i in self.indexes]

    def is_nested(self) -> bool:
        return False

    def to_top_level_indexes(self) -> List[int]:
        return list(self.indexes)

    def to_nested_indexes(self) -> List[List[int]]:
        return [[i] for i in self.indexes]

    def to_name_paths(self, row_type) -> List[List[str]]:
        fields = _row_fields(row_type)
        return [[fields[i].name] for i in self.indexes]


class NestedProjection(Projection):
    """Projection over paths that may walk into ROW children.

    Each path navigates from a top-level field through successive ROW
    children. A path of length 1 is equivalent to a top-level selection.
    """

    def __init__(self, paths: Sequence[Sequence[int]]):
        if not paths:
            raise ValueError("NestedProjection requires at least one path")
        self.paths = [list(p) for p in paths]
        for p in self.paths:
            if len(p) == 0:
                raise ValueError(
                    "Each projection path must have at least one index")
        self._has_nested = any(len(p) > 1 for p in self.paths)

    def is_nested(self) -> bool:
        return self._has_nested

    def to_top_level_indexes(self) -> List[int]:
        # Preserve order, deduplicate.
        seen = set()
        out: List[int] = []
        for p in self.paths:
            top = p[0]
            if top not in seen:
                seen.add(top)
                out.append(top)
        return out

    def to_nested_indexes(self) -> List[List[int]]:
        return [list(p) for p in self.paths]

    def to_name_paths(self, row_type) -> List[List[str]]:
        fields = _row_fields(row_type)
        result: List[List[str]] = []
        for path in self.paths:
            field = fields[path[0]]
            names = [field.name]
            for idx in path[1:]:
                child_type = field.type
                if not is_row_type(child_type):
                    raise ValueError(
                        "Nested projection step expected a ROW type but got %s "
                        "for field '%s'" % (child_type, field.name))
                child_fields = _row_fields(child_type)
                field = child_fields[idx]
                names.append(field.name)
            result.append(names)
        return result

    def project(self, row_type) -> List[DataField]:
        fields = _row_fields(row_type)
        out: List[DataField] = []
        seen_names = set()
        dup_count = 0
        for path in self.paths:
            field = fields[path[0]]
            name_parts = [field.name]
            for idx in path[1:]:
                child_type = field.type
                if not is_row_type(child_type):
                    raise ValueError(
                        "Nested projection step expected a ROW type but got %s "
                        "for field '%s'" % (child_type, field.name))
                child_fields = _row_fields(child_type)
                field = child_fields[idx]
                name_parts.append(field.name)
            base_name = "_".join(name_parts)
            final_name = base_name
            while final_name in seen_names:
                final_name = "%s__%d" % (base_name, dup_count)
                dup_count += 1
            seen_names.add(final_name)
            # Keep the leaf field's ID so downstream schema-evolution
            # remapping by field ID still works after rename.
            out.append(DataField(
                id=field.id,
                name=final_name,
                type=field.type,
                description=getattr(field, 'description', None),
                default_value=getattr(field, 'default_value', None),
            ))
        return out


def _row_fields(row_type) -> List[DataField]:
    """Return the field list of a row-like type. Accepts a RowType, a plain
    list of DataField, or anything else with a ``.fields`` attribute.
    """
    if isinstance(row_type, list):
        return row_type
    fields: Optional[List[DataField]] = getattr(row_type, 'fields', None)
    if fields is None:
        raise ValueError(
            "Projection target must be a RowType or have a .fields attribute, "
            "got %s" % type(row_type).__name__)
    return list(fields)


def is_row_type(data_type) -> bool:
    if isinstance(data_type, RowType):
        return True
    return getattr(data_type, 'fields', None) is not None
