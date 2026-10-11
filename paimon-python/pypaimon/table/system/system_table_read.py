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

"""Read pipeline for system tables.

Mirrors the user-facing surface of :class:`TableRead` (``to_arrow``,
``to_pandas``, ``to_iterator``, ``to_record_batch_iterator``,
``to_duckdb``) so callers reach for the same API as a regular data
table. The implementation operates purely on the in-memory PyArrow
tables produced by :class:`SystemSplit` — no manifest reads, no data
files.
"""

from typing import Any, Iterator, List, Optional, TYPE_CHECKING

import pandas
import pyarrow

from pypaimon.common.predicate import Predicate
from pypaimon.read.split import Split
from pypaimon.schema.data_types import (
    AtomicType, DataField, PyarrowFieldParser)
from pypaimon.table.system.system_table_scan import SystemSplit

if TYPE_CHECKING:  # pragma: no cover - type-only import
    from pypaimon.table.system.system_table import SystemTable


_PREDICATE_NOT_SUPPORTED = (
    "string-match predicate pushdown (starts_with / ends_with / contains / "
    "like) on system tables is not supported yet; filter the resulting "
    "PyArrow / pandas table on the client side"
)

# Value comparisons (equal / greater-than / is-in / ...) have no PyArrow
# compute kernel for nested columns such as ``$files.write_cols`` or
# ``$table_indexes.dv_ranges``; only null checks work there.
_COMPARISON_NEEDS_SCALAR = ("isNull", "isNotNull")


class SystemTableRead:
    """In-memory read pipeline backed by a single SystemSplit."""

    def __init__(
        self,
        system_table: "SystemTable",
        read_type: List[DataField],
        predicate: Optional[Predicate],
        limit: Optional[int],
    ):
        self.system_table = system_table
        self.read_type = read_type
        self.predicate = predicate
        self.limit = limit

    def to_arrow(self, splits: List[Split]) -> pyarrow.Table:
        return self._materialise(splits)

    def to_pandas(self, splits: List[Split]) -> pandas.DataFrame:
        return self.to_arrow(splits).to_pandas()

    def to_iterator(self, splits: List[Split]) -> Iterator[dict]:
        arrow_table = self.to_arrow(splits)
        column_names = arrow_table.schema.names
        for batch in arrow_table.to_batches():
            py_columns = [batch.column(i).to_pylist()
                          for i in range(batch.num_columns)]
            for row_idx in range(batch.num_rows):
                yield {column_names[c]: py_columns[c][row_idx]
                       for c in range(batch.num_columns)}

    def to_record_batch_iterator(
        self, splits: List[Split]
    ) -> Iterator[pyarrow.RecordBatch]:
        for batch in self.to_arrow(splits).to_batches():
            yield batch

    def to_duckdb(
        self,
        splits: List[Split],
        table_name: str,
        connection: Optional[Any] = None,
    ):
        import duckdb

        con = connection or duckdb.connect(database=":memory:")
        con.register(table_name, self.to_arrow(splits))
        return con

    def _materialise(self, splits: List[Split]) -> pyarrow.Table:
        from pypaimon.read.push_down_utils import predicate_supports_arrow_filter

        predicate = self.predicate
        if predicate is not None and not isinstance(predicate, Predicate):
            # Preserve the public error contract for non-Predicate filter
            # objects (e.g. ``with_filter(object())``): predicate_supports_
            # arrow_filter dereferences ``.method`` and would otherwise leak
            # an internal AttributeError.
            raise NotImplementedError(_PREDICATE_NOT_SUPPORTED)
        if predicate is not None and not predicate_supports_arrow_filter(
            predicate
        ):
            raise NotImplementedError(_PREDICATE_NOT_SUPPORTED)
        if predicate is not None:
            self._reject_non_scalar_filter_fields(predicate)

        if not splits:
            return self._empty_table()

        arrow_predicate = (
            self.predicate.to_arrow() if self.predicate is not None else None
        )

        projected_names = [f.name for f in self.read_type]
        slices: List[pyarrow.Table] = []
        for split in splits:
            if not isinstance(split, SystemSplit):
                raise TypeError(
                    "SystemTableRead expects SystemSplit but received "
                    + type(split).__name__
                )
            arrow_table = split.arrow_table()
            # Filter on the full schema before projection so a predicate may
            # reference columns that are not part of the projection.
            if arrow_predicate is not None:
                arrow_table = arrow_table.filter(arrow_predicate)
            if projected_names:
                arrow_table = arrow_table.select(projected_names)
            slices.append(arrow_table)

        combined = pyarrow.concat_tables(slices)
        if self.limit is not None and combined.num_rows > self.limit:
            combined = combined.slice(0, self.limit)
        return combined

    def _reject_non_scalar_filter_fields(self, predicate: Predicate) -> None:
        """Reject value comparisons on nested system-table columns.

        System tables expose list/map/row columns (e.g. ``$files.write_cols``,
        ``$table_indexes.dv_ranges``). PyArrow has no equality / comparison /
        is-in kernel for those types, so ``arrow_table.filter`` would raise an
        internal ``ArrowNotImplementedError``. Surface the documented
        ``NotImplementedError`` instead. Null checks are left alone because the
        ``is_null`` / ``is_valid`` kernels do accept nested input.
        """
        field_types = {
            f.name: f.type for f in self.system_table.row_type().fields
        }
        pending: List[Predicate] = [predicate]
        while pending:
            current = pending.pop()
            if current.method in ("and", "or"):
                pending.extend(current.literals or [])
                continue
            if current.method in _COMPARISON_NEEDS_SCALAR:
                continue
            field_type = field_types.get(current.field)
            if field_type is not None and not isinstance(field_type, AtomicType):
                raise NotImplementedError(
                    "predicate pushdown of "
                    + repr(current.method)
                    + " on the non-scalar system-table column "
                    + repr(current.field)
                    + " (type "
                    + type(field_type).__name__
                    + ") is not supported; filter the resulting PyArrow / "
                    + "pandas table on the client side"
                )

    def _empty_table(self) -> pyarrow.Table:
        target_schema = PyarrowFieldParser.from_paimon_schema(self.read_type)
        return pyarrow.Table.from_arrays(
            [pyarrow.array([], type=f.type) for f in target_schema],
            schema=target_schema,
        )
