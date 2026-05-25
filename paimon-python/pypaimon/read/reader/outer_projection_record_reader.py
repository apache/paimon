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

"""Outer-projection wrapper for nested-field reads.

Sits above a reader whose rows still carry full ROW sub-structures, and
emits flat rows whose slots are the values reached by walking each
nested name path. Used on the primary-key merge-read path: the inner
reader hands the merge function complete ROW columns (so deduplicate /
partial-update / aggregation see the original sub-structure), and this
wrapper extracts the user-visible flat columns afterwards.
"""

from typing import Any, List, Optional

from pypaimon.read.reader.iface.record_iterator import RecordIterator
from pypaimon.read.reader.iface.record_reader import RecordReader
from pypaimon.table.row.internal_row import InternalRow
from pypaimon.table.row.offset_row import OffsetRow


class OuterProjectionRecordReader(RecordReader[InternalRow]):
    """Wraps an InternalRow reader and projects nested name paths into flat rows."""

    def __init__(
        self,
        inner: RecordReader[InternalRow],
        inner_top_names: List[str],
        name_paths: List[List[str]],
    ):
        if not name_paths:
            raise ValueError("name_paths must be non-empty")
        for path in name_paths:
            if not path:
                raise ValueError("each name path must contain at least one name")
        name_to_top_idx = {name: i for i, name in enumerate(inner_top_names)}
        self._specs: List[_PathSpec] = []
        for path in name_paths:
            top_name = path[0]
            if top_name not in name_to_top_idx:
                raise ValueError(
                    "path top-level field %r not found in inner row schema %r"
                    % (top_name, inner_top_names))
            self._specs.append(_PathSpec(name_to_top_idx[top_name], list(path[1:])))
        self._inner = inner
        self._flat_arity = len(name_paths)

    def read_batch(self) -> Optional[RecordIterator[InternalRow]]:
        inner_batch = self._inner.read_batch()
        if inner_batch is None:
            return None
        return _OuterProjectionIterator(inner_batch, self._specs, self._flat_arity)

    def close(self) -> None:
        self._inner.close()


class _OuterProjectionIterator(RecordIterator[InternalRow]):
    """Per-batch iterator that materialises one flat OffsetRow per inner row."""

    def __init__(
        self,
        inner: RecordIterator[InternalRow],
        specs: List["_PathSpec"],
        flat_arity: int,
    ):
        self._inner = inner
        self._specs = specs
        self._flat_arity = flat_arity
        self._reused_row = OffsetRow(None, 0, flat_arity)

    def next(self) -> Optional[InternalRow]:
        inner_row = self._inner.next()
        if inner_row is None:
            return None
        flat = tuple(_extract(inner_row, spec) for spec in self._specs)
        self._reused_row.replace(flat)
        # Inherit the inner row's RowKind so downstream consumers (e.g. the
        # to_arrow path) keep the same +I/-D/-U/+U classification.
        self._reused_row.set_row_kind_byte(inner_row.get_row_kind().value)
        return self._reused_row


class _PathSpec:
    """Pre-resolved name path: top-level slot index plus sub-field names."""

    __slots__ = ("top_idx", "sub_names")

    def __init__(self, top_idx: int, sub_names: List[str]):
        self.top_idx = top_idx
        self.sub_names = sub_names


def _extract(row: InternalRow, spec: _PathSpec) -> Any:
    cur = row.get_field(spec.top_idx)
    for name in spec.sub_names:
        if cur is None:
            return None
        cur = _step_into(cur, name)
    return cur


def _step_into(value: Any, name: str) -> Any:
    """Take one step into a ROW sub-structure by sub-field name.

    Upstream materialises nested ROW values as plain Python dicts (e.g.
    polars row-by-row iteration produces a dict for each struct slot),
    so dict access is the only supported form here. Anything else is
    rejected loudly to surface schema/wiring mismatches early.
    """
    if isinstance(value, dict):
        return value.get(name)
    if isinstance(value, InternalRow):
        # Defensive: if the upstream reader handed us a wrapped sub-row,
        # we cannot index it by name without its schema, so fail fast
        # rather than guessing the slot.
        raise TypeError(
            "Cannot step into InternalRow by name %r without sub-schema; "
            "expected a dict from the polars row materialisation" % (name,))
    raise TypeError(
        "Cannot index nested ROW step %r into value of type %s"
        % (name, type(value).__name__))
