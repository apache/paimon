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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Bounded temporal alignment for multimodal table scans."""

from bisect import bisect_left
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.multimodal.query import ScanQuery
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.table.special_fields import SpecialFields


_ROW_ID = SpecialFields.ROW_ID.name


@dataclass(frozen=True)
class _MatchSpec:
    query: ScanQuery
    method: str
    tolerance: object = None
    on: Optional[str] = None


def exact(query, *, on=None):
    """Match a secondary scan at exactly the anchor timestamp."""
    return _match_spec(query, "exact", None, on)


def backward(query, *, tolerance, on=None):
    """Match the latest secondary row at or before the anchor timestamp."""
    return _match_spec(query, "backward", tolerance, on)


def forward(query, *, tolerance, on=None):
    """Match the earliest secondary row at or after the anchor timestamp."""
    return _match_spec(query, "forward", tolerance, on)


def nearest(query, *, tolerance, on=None):
    """Match the closest secondary row; equal-distance ties choose earlier."""
    return _match_spec(query, "nearest", tolerance, on)


def align(anchor, *, on, by, sources):
    """Build a batch-streaming, episode-local temporal alignment.

    The anchor scan defines output rows. Each named secondary source
    contributes at most one row according to its match policy. All scans are
    pinned to their current snapshots when this object is constructed.
    """
    if not isinstance(on, str) or not on:
        raise ValueError("on must be a non-empty column name.")
    if isinstance(by, str):
        by = (by,)
    else:
        try:
            by = tuple(by)
        except TypeError as error:
            raise ValueError("by must be a column name or sequence.") from error
    if not by:
        raise ValueError("align requires at least one grouping column in by.")
    if (any(not isinstance(name, str) or not name for name in by)
            or len(set(by)) != len(by)):
        raise ValueError("by must contain unique, non-empty column names.")
    if not isinstance(sources, Mapping):
        raise TypeError("sources must be a mapping of names to match specs.")
    if not sources:
        raise ValueError("align requires at least one named secondary source.")
    return AlignedScan(anchor, on, by, sources)


class AlignedScan:
    """Executable result of :func:`align`."""

    def __init__(self, anchor, on, by, sources):
        self._anchor = _pin_scan(_require_scan(anchor, "anchor"))
        self._on = on
        self._by = by
        self._sources = {
            name: _PinnedSource(name, _pin_match_spec(spec, name), on, by)
            for name, spec in sources.items()
        }
        self._anchor_schema = _query_schema(self._anchor)
        self._anchor_table_schema = _table_schema(self._anchor)
        self._validate_anchor()
        self.schema = self._output_schema()

    def to_arrow_batch_reader(self, *, batch_size=1024):
        """Plan scalar timestamps once, then fetch selected rows in batches."""
        if not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer.")

        anchor_rows = _metadata_rows(self._anchor, self._on, self._by)
        anchor_rows.sort(key=_metadata_sort_key(self._on, self._by))
        for source in self._sources.values():
            source.plan()

        def batches():
            for start in range(0, len(anchor_rows), batch_size):
                rows = anchor_rows[start:start + batch_size]
                yield self._build_batch(rows)

        return pa.RecordBatchReader.from_batches(self.schema, batches())

    def to_arrow(self):
        reader = self.to_arrow_batch_reader()
        try:
            return reader.read_all()
        finally:
            reader.close()

    def to_pandas(self):
        return self.to_arrow().to_pandas()

    def to_list(self):
        return self.to_arrow().to_pylist()

    def _validate_anchor(self):
        _require_columns(
            self._anchor_table_schema, self._by + (self._on,), "anchor")
        anchor_type = self._anchor_table_schema.field(self._on).type
        _delta_type(anchor_type)
        for source in self._sources.values():
            if source.time_type != anchor_type:
                raise TypeError(
                    "Anchor and source %r temporal columns must have the same "
                    "type; got %s and %s."
                    % (source.name, anchor_type, source.time_type)
                )

    def _output_schema(self):
        fields = list(self._anchor_schema)
        names = set(self._anchor_schema.names)
        anchor_time_type = self._anchor_table_schema.field(self._on).type
        for source in self._sources.values():
            for field in source.payload_schema:
                output = pa.field(
                    source.output_name(field.name), field.type, nullable=True)
                if output.name in names:
                    raise ValueError(
                        "Duplicate aligned column %r." % output.name)
                fields.append(output)
                names.add(output.name)
            audit = [
                pa.field(
                    source.output_name("valid"), pa.bool_(), nullable=False),
                pa.field(
                    source.output_name("matched_time"),
                    source.time_type,
                    nullable=True,
                ),
                pa.field(
                    source.output_name("time_delta"),
                    _delta_type(anchor_time_type),
                    nullable=True,
                ),
            ]
            for field in audit:
                if field.name in names:
                    raise ValueError(
                        "Duplicate aligned column %r." % field.name)
                fields.append(field)
                names.add(field.name)
        return pa.schema(fields)

    def _build_batch(self, anchor_rows):
        anchor_ids = [row[_ROW_ID] for row in anchor_rows]
        anchor = _fetch_rows(self._anchor, anchor_ids)
        arrays = [anchor[name] for name in self._anchor_schema.names]

        for source in self._sources.values():
            matches = [source.match(row) for row in anchor_rows]
            matched_ids = [match[0] for match in matches if match is not None]
            unique_ids = list(dict.fromkeys(matched_ids))
            values = _fetch_rows(source.spec.query, unique_ids)
            positions = {
                row_id: index for index, row_id in enumerate(unique_ids)
            }
            take = pa.array([
                None if match is None else positions[match[0]]
                for match in matches
            ], type=pa.int64())
            for field in source.payload_schema:
                arrays.append(pc.take(values[field.name], take))

            arrays.extend([
                pa.array(
                    [match is not None for match in matches], type=pa.bool_()),
                pa.array(
                    [None if match is None else match[1] for match in matches],
                    type=source.time_type,
                ),
                pa.array(
                    [None if match is None else match[2] for match in matches],
                    type=_delta_type(
                        self._anchor_table_schema.field(self._on).type),
                ),
            ])

        table = pa.Table.from_arrays(
            arrays, schema=self.schema).combine_chunks()
        return table.to_batches(max_chunksize=len(anchor_rows))[0]


class _PinnedSource:

    def __init__(self, name, spec, anchor_on, by):
        if not name or "__" in name:
            raise ValueError(
                "Aligned source names must be non-empty and cannot contain "
                "'__'.")
        self.name = name
        self.spec = spec
        self.anchor_on = anchor_on
        self.on = spec.on or anchor_on
        self.by = by
        table_schema = _table_schema(spec.query)
        _require_columns(table_schema, by + (self.on,), "source %r" % name)
        self.time_type = table_schema.field(self.on).type
        _delta_type(self.time_type)
        schema = _query_schema(spec.query)
        self.payload_schema = pa.schema([
            field for field in schema
            if field.name not in set(by + (self.on,))
        ])
        self._index = None

    def output_name(self, field):
        return "%s__%s" % (self.name, field)

    def plan(self):
        rows = _metadata_rows(self.spec.query, self.on, self.by)
        groups = {}
        for row in rows:
            key = tuple(row[name] for name in self.by)
            groups.setdefault(key, []).append((row[self.on], row[_ROW_ID]))

        self._index = {}
        for key, values in groups.items():
            values.sort()
            times = [value[0] for value in values]
            if len(times) != len(set(times)):
                raise ValueError(
                    "Source %r has duplicate timestamps within group %r."
                    % (self.name, key)
                )
            self._index[key] = (times, [value[1] for value in values])

    def match(self, anchor_row):
        key = tuple(anchor_row[name] for name in self.by)
        values = self._index.get(key)
        if values is None:
            return None
        times, row_ids = values
        target = anchor_row[self.anchor_on]
        index = _match_index(times, target, self.spec.method)
        if index is None:
            return None
        delta = times[index] - target
        if (self.spec.tolerance is not None
                and abs(delta) > self.spec.tolerance):
            return None
        return row_ids[index], times[index], delta


def _match_spec(query, method, tolerance, on):
    _require_scan(query, method)
    if method != "exact" and tolerance is None:
        raise ValueError("%s requires a non-null tolerance." % method)
    if tolerance is not None:
        try:
            zero = timedelta(0) if isinstance(tolerance, timedelta) else 0
            negative = tolerance < zero
        except TypeError:
            negative = False
        if negative:
            raise ValueError("tolerance must be non-negative.")
    return _MatchSpec(query, method, tolerance, on)


def _pin_match_spec(spec, name):
    if not isinstance(spec, _MatchSpec):
        raise TypeError(
            "Source %r must use exact(), backward(), forward(), or nearest()."
            % name
        )
    return _MatchSpec(
        _pin_scan(spec.query), spec.method, spec.tolerance, spec.on)


def _require_scan(query, label):
    if type(query) is not ScanQuery:
        raise TypeError("%s must be a MultimodalTable.scan() query." % label)
    return query


def _pin_scan(query):
    table = query._table
    options = table.options
    if not options.row_tracking_enabled(False):
        raise ValueError("align requires 'row-tracking.enabled' = 'true'.")
    empty = False
    if (options.scan_snapshot_id() is None
            and options.scan_tag_name() is None):
        snapshot = table.snapshot_manager().get_latest_snapshot()
        if snapshot is None:
            empty = True
        else:
            table = table.copy({
                CoreOptions.SCAN_SNAPSHOT_ID.key(): str(snapshot.id),
            })
    pinned = ScanQuery(table)
    pinned._predicate = query._predicate
    pinned._projection = query._projection
    pinned._limit = query._limit
    pinned._include_row_id = query._include_row_id
    pinned._temporal_empty = empty
    return pinned


def _query_schema(query):
    table = query._table.copy({CoreOptions.BLOB_AS_DESCRIPTOR.key(): "true"})
    builder = query._configured_read_builder(table)
    return PyarrowFieldParser.from_paimon_schema(builder.read_type())


def _table_schema(query):
    return PyarrowFieldParser.from_paimon_schema(query._table.fields)


def _metadata_rows(query, on, by):
    columns = list(dict.fromkeys(by + (on, _ROW_ID)))
    if getattr(query, "_temporal_empty", False):
        return []
    table = query._table.copy({CoreOptions.BLOB_AS_DESCRIPTOR.key(): "true"})
    builder = table.new_read_builder()
    if query._predicate is not None:
        builder = builder.with_filter(query._predicate)
        for name in query._predicate_fields():
            if name not in columns:
                columns.append(name)
    builder = builder.with_projection(columns)
    if query._limit is not None:
        builder = builder.with_limit(query._limit)
    arrow = builder.new_read().to_arrow(builder.new_scan().plan().splits())
    rows = arrow.select(list(by) + [on, _ROW_ID]).to_pylist()
    for row in rows:
        if row[on] is None or any(row[name] is None for name in by):
            raise ValueError("Temporal keys cannot be null: %r." % row)
    return rows


def _fetch_rows(query, row_ids):
    schema = _query_schema(query)
    if not row_ids:
        return pa.Table.from_arrays(
            [pa.array([], type=field.type) for field in schema], schema=schema)

    table = query._table.copy({CoreOptions.BLOB_AS_DESCRIPTOR.key(): "true"})
    visible = schema.names
    projection = list(dict.fromkeys(visible + [_ROW_ID]))
    for name in query._predicate_fields():
        if name not in projection:
            projection.append(name)
    builder = table.new_read_builder().with_projection(projection)
    row_id_predicate = builder.new_predicate_builder().is_in(_ROW_ID, row_ids)
    predicates = [row_id_predicate]
    if query._predicate is not None:
        predicates.insert(0, query._predicate)
    builder = builder.with_filter(PredicateBuilder.and_predicates(predicates))
    arrow = builder.new_read().to_arrow(builder.new_scan().plan().splits())

    found = arrow[_ROW_ID].to_pylist()
    positions = {}
    for index, row_id in enumerate(found):
        if row_id in positions:
            raise RuntimeError("Duplicate row id %r in aligned scan." % row_id)
        positions[row_id] = index
    missing = [row_id for row_id in row_ids if row_id not in positions]
    if missing:
        raise RuntimeError(
            "Aligned row ids disappeared from pinned snapshot: %r." % missing)
    take = pa.array([positions[row_id] for row_id in row_ids], type=pa.int64())
    return arrow.select(visible).take(take)


def _metadata_sort_key(on, by):
    def key(row):
        return tuple(row[name] for name in by) + (row[on], row[_ROW_ID])
    return key


def _match_index(times, target, method):
    position = bisect_left(times, target)
    if method == "exact":
        if position < len(times) and times[position] == target:
            return position
        return None
    if method == "backward":
        if position < len(times) and times[position] == target:
            return position
        return position - 1 if position else None
    if method == "forward":
        return position if position < len(times) else None
    if method == "nearest":
        if position == 0:
            return 0
        if position == len(times):
            return len(times) - 1
        before = position - 1
        if target - times[before] <= times[position] - target:
            return before
        return position
    raise ValueError("Unknown temporal match method %r." % method)


def _delta_type(data_type):
    if pa.types.is_timestamp(data_type):
        return pa.duration(data_type.unit)
    if pa.types.is_integer(data_type):
        return pa.int64()
    if pa.types.is_floating(data_type):
        return pa.float64()
    raise TypeError(
        "Temporal columns must be integer, floating point, or timestamp; "
        "got %s."
        % data_type
    )


def _require_columns(schema, columns, label):
    missing = [name for name in columns if name not in schema.names]
    if missing:
        raise ValueError(
            "%s is missing temporal columns %r." % (label, missing))
