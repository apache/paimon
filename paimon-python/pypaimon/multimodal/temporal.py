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

"""Temporal alignment for multimodal table scans."""

from bisect import bisect_left, bisect_right
from datetime import timedelta
from fractions import Fraction
import json
import math
from numbers import Integral, Real

import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.catalog.table_query_auth import TableQueryAuthResult
from pypaimon.common.options.core_options import CoreOptions, StartupMode
from pypaimon.common.predicate_json_parser import (
    _apply_predicate_transform,
    _collect_all_field_refs_from_transform,
)
from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.multimodal.query import ScanQuery
from pypaimon.read.query_auth_split import QueryAuthSplit, resolve_auth_result
from pypaimon.read.reader.format_pyarrow_reader import _DecodedRowGroupCache
from pypaimon.read.table_read import _ClosableArrowBatchReader
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.snapshot.time_travel_util import TimeTravelUtil
from pypaimon.table.special_fields import SpecialFields
from pypaimon.table.source.global_index_live_row_filter import (
    table_at_snapshot,
)
from pypaimon.utils.range import Range


_ROW_ID = SpecialFields.ROW_ID.name
_MAX_INT64 = (1 << 63) - 1
_TIME_KEY = object()
_TEMPORAL_ROW_GROUP_CACHE_MAX_SIZE = 64 * 1024 * 1024


def join_asof(left, right, *, on, by, direction="backward", tolerance=None,
              right_on=None, suffix="_right") -> "TemporalAlignment":
    """Join each left row with at most one time-aligned right row."""
    return TemporalAlignment(left, on=on, by=by).join_asof(
        right,
        direction=direction,
        tolerance=tolerance,
        right_on=right_on,
        suffix=suffix,
    )


def interpolate(left, right, *, on, by, tolerance=None,
                right_on=None, suffix="_right") -> "TemporalAlignment":
    """Linearly interpolate numeric right values at each left timestamp."""
    return TemporalAlignment(left, on=on, by=by).interpolate(
        right,
        tolerance=tolerance,
        right_on=right_on,
        suffix=suffix,
    )


def join_window(left, right, *, on, by, preceding, aggregations,
                following=None, closed="both", right_on=None,
                suffix="_right") -> "TemporalAlignment":
    """Join and aggregate right values in each left row's time window."""
    return TemporalAlignment(left, on=on, by=by).join_window(
        right,
        preceding=preceding,
        following=following,
        aggregations=aggregations,
        closed=closed,
        right_on=right_on,
        suffix=suffix,
    )


def _normalize_temporal_keys(on, by):
    if not isinstance(on, str) or not on:
        raise ValueError("on must be a non-empty column name.")
    if isinstance(by, str):
        by = (by,)
    else:
        try:
            by = tuple(by)
        except TypeError as error:
            raise ValueError(
                "by must be a column name or sequence.") from error
    if not by:
        raise ValueError(
            "Temporal alignment requires a grouping column in by.")
    if (any(not isinstance(name, str) or not name for name in by)
            or len(set(by)) != len(by)):
        raise ValueError("by must contain unique, non-empty column names.")
    return on, by


class TemporalAlignment:
    """Lazy, chainable alignment of table scans by time."""

    def __init__(self, left, *, on, by):
        on, by = _normalize_temporal_keys(on, by)
        self._anchor = _pin_scan_to_snapshot(_require_scan(left, "left"))
        self._on = on
        self._by = by
        self._sources = ()
        self._anchor_schema = _query_schema(self._anchor)
        self._anchor_table_schema = _table_schema(self._anchor)
        self._validate_anchor()
        self.schema = self._output_schema()

    def join_asof(self, right, *, direction="backward", tolerance=None,
                  right_on=None, suffix="_right") -> "TemporalAlignment":
        """Append a right-side as-of join without materializing this scan."""
        position = len(self._sources) + 1
        label = "right source %d" % position
        source = _AsOfJoinRight(
            label,
            right,
            self._on,
            self._by,
            direction,
            tolerance,
            right_on,
            suffix,
        )
        return self._append(source)

    def interpolate(self, right, *, tolerance=None, right_on=None,
                    suffix="_right") -> "TemporalAlignment":
        """Append linear interpolation of numeric right-side values."""
        position = len(self._sources) + 1
        source = _LinearInterpolationRight(
            "right source %d" % position,
            right,
            self._on,
            self._by,
            tolerance,
            right_on,
            suffix,
        )
        return self._append(source)

    def join_window(self, right, *, preceding, aggregations,
                    following=None, closed="both", right_on=None,
                    suffix="_right") -> "TemporalAlignment":
        """Append a right-side window join with aggregation."""
        position = len(self._sources) + 1
        source = _WindowJoinRight(
            "right source %d" % position,
            right,
            self._on,
            self._by,
            preceding,
            following,
            aggregations,
            closed,
            right_on,
            suffix,
        )
        return self._append(source)

    def _append(self, source):
        result = object.__new__(TemporalAlignment)
        result._anchor = self._anchor
        result._on = self._on
        result._by = self._by
        result._sources = self._sources + (source,)
        result._anchor_schema = self._anchor_schema
        result._anchor_table_schema = self._anchor_table_schema
        result._validate_anchor()
        result.schema = result._output_schema()
        return result

    def to_arrow_batch_reader(self, *, batch_size=1024):
        """Index right-side timestamps, then stream aligned rows in batches."""
        if (isinstance(batch_size, bool)
                or not isinstance(batch_size, int)
                or batch_size <= 0):
            raise ValueError("batch_size must be a positive integer.")

        anchor_metadata = _metadata_batches(
            self._anchor, self._on, self._by, batch_size)
        row_group_cache = _DecodedRowGroupCache(
            _TEMPORAL_ROW_GROUP_CACHE_MAX_SIZE)
        anchor_fetcher = _RowIdFetcher(self._anchor, row_group_cache)
        source_fetchers = []
        for source in self._sources:
            source.plan()
            source_fetchers.append(
                _RowIdFetcher(
                    source.query, row_group_cache, source._fetch_names))
        schema = self._output_schema(anchor_fetcher.schema, source_fetchers)
        self.schema = schema

        def batches():
            try:
                for metadata in anchor_metadata:
                    rows = _metadata_rows(
                        metadata, self._on,
                        self._anchor_table_schema.field(self._on).type)
                    yield from self._build_batches(
                        rows, anchor_fetcher, source_fetchers, schema)
            finally:
                anchor_metadata.close()

        batch_iterator = batches()
        reader = pa.ipc.RecordBatchReader.from_batches(schema, batch_iterator)
        return _ClosableArrowBatchReader(reader, batch_iterator)

    def to_arrow(self):
        reader = self.to_arrow_batch_reader()
        try:
            return reader.read_all()
        finally:
            close = getattr(reader, "close", None)
            if close is not None:
                close()

    def to_pandas(self):
        return self.to_arrow().to_pandas()

    def to_list(self):
        return _arrow_rows(self.to_arrow())

    @property
    def resolved_snapshots(self):
        """Return the table snapshots pinned by this alignment."""
        snapshots = {
            "left": _resolved_snapshot(self._anchor),
        }
        snapshots.update({
            "right_%d" % position: _resolved_snapshot(source.query)
            for position, source in enumerate(self._sources, 1)
        })
        return snapshots

    def _validate_anchor(self):
        _require_columns(
            self._anchor_table_schema, self._by + (self._on,), "anchor")
        anchor_type = self._anchor_table_schema.field(self._on).type
        _delta_type(anchor_type)
        for name in self._by:
            _validate_group_type(
                name, self._anchor_table_schema.field(name).type)
        for source in self._sources:
            if source.time_type != anchor_type:
                raise TypeError(
                    "Left and %s temporal columns must have the same "
                    "type; got %s and %s."
                    % (source.label, anchor_type, source.time_type)
                )
            for name in self._by:
                anchor_group_type = self._anchor_table_schema.field(name).type
                source_group_type = source.table_schema.field(name).type
                if source_group_type != anchor_group_type:
                    raise TypeError(
                        "Left and %s grouping column %r must have "
                        "the same type; got %s and %s."
                        % (source.label, name, anchor_group_type,
                           source_group_type)
                    )

    def _output_schema(self, anchor_schema=None, source_fetchers=None):
        if anchor_schema is None:
            anchor_schema = self._anchor_schema
        fields = list(anchor_schema)
        names = set(anchor_schema.names)
        for position, source in enumerate(self._sources):
            payload_schema = (
                source.payload_schema if source_fetchers is None
                else source_fetchers[position].schema
            )
            for field in source.output_fields(
                    payload_schema,
                    effective=source_fetchers is not None):
                output_name = field.name
                if output_name in names:
                    output_name += source.suffix
                if output_name in names:
                    raise ValueError(
                        "%s column %r conflicts after applying suffix %r."
                        % (source.label, field.name, source.suffix)
                    )
                output = pa.field(
                    output_name, field.type, nullable=True,
                    metadata=field.metadata)
                fields.append(output)
                names.add(output.name)
        return pa.schema(fields, metadata=anchor_schema.metadata)

    def _build_batch(
            self, anchor_rows, anchor_fetcher, source_fetchers, schema):
        anchor_ids = [row[_ROW_ID] for row in anchor_rows]
        anchor = anchor_fetcher.fetch(anchor_ids)
        anchor.validate()
        arrays = [anchor[name] for name in self._anchor_schema.names]

        for source, fetcher in zip(self._sources, source_fetchers):
            arrays.extend(source.build_arrays(anchor_rows, fetcher))

        if arrays:
            table = pa.Table.from_arrays(
                arrays, schema=schema).combine_chunks()
            return table.to_batches(max_chunksize=len(anchor_rows))[0]
        batch = pa.RecordBatch.from_struct_array(pa.array(
            [{}] * len(anchor_rows), type=pa.struct([])))
        return batch.replace_schema_metadata(schema.metadata)

    def _build_batches(
            self, anchor_rows, anchor_fetcher, source_fetchers, schema):
        try:
            yield self._build_batch(
                anchor_rows, anchor_fetcher, source_fetchers, schema)
        except pa.ArrowInvalid as error:
            if len(anchor_rows) < 2 or "offset" not in str(error).lower():
                raise
            middle = len(anchor_rows) // 2
            yield from self._build_batches(
                anchor_rows[:middle], anchor_fetcher,
                source_fetchers, schema)
            yield from self._build_batches(
                anchor_rows[middle:], anchor_fetcher,
                source_fetchers, schema)


class _AsOfJoinRight:

    def __init__(self, label, query, anchor_on, by, direction, tolerance,
                 right_on, suffix):
        _validate_join_options(direction, tolerance, right_on, suffix)
        self.label = label
        self.query = _pin_scan_to_snapshot(_require_scan(query, label))
        self.direction = direction
        self.suffix = suffix
        self.anchor_on = anchor_on
        self.on = anchor_on if right_on is None else right_on
        self.by = by
        self.table_schema = _table_schema(self.query)
        _require_columns(
            self.table_schema, by + (self.on,), label)
        self.time_type = self.table_schema.field(self.on).type
        _delta_type(self.time_type)
        _validate_tolerance(tolerance, self.time_type)
        self._tolerance_key = _time_tolerance_key(tolerance, self.time_type)
        schema, paths = _query_schema_and_paths(self.query)
        projection = self.query._effective_projection()
        excluded = {(name,) for name in by}
        if projection is None:
            excluded.add((self.on,))
        self.payload_schema = pa.schema([
            field for field, path in zip(schema, paths)
            if tuple(path) not in excluded
        ])
        self._fetch_names = None
        self._index = None

    def plan(self):
        metadata = _metadata_table(self.query, self.on, self.by)
        self._times = metadata[self.on].combine_chunks()
        self._time_keys = _time_search_keys(self._times, self.time_type)
        self._row_ids = metadata[_ROW_ID].combine_chunks()
        self._index = {}
        group_columns = [metadata[name].combine_chunks() for name in self.by]
        previous = None
        start = 0
        for position in range(len(metadata)):
            key = tuple(column[position].as_py() for column in group_columns)
            if position and key != previous:
                self._index[previous] = (start, position)
                start = position
            previous = key
        if len(metadata):
            self._index[previous] = (start, len(metadata))

    def match(self, anchor_row):
        key = tuple(anchor_row[name] for name in self.by)
        bounds = self._index.get(key)
        if bounds is None:
            return None
        target_key = anchor_row[_TIME_KEY]
        index = _match_index(
            self._time_keys, target_key, self.direction, *bounds)
        if index is None:
            return None
        matched_key = _python_scalar(self._time_keys[index])
        if (self._tolerance_key is not None
                and abs(matched_key - target_key) > self._tolerance_key):
            return None
        return self._row_ids[index].as_py()

    @staticmethod
    def output_field(field, effective=True):
        return field

    def output_fields(self, payload_schema, effective=True):
        return [
            self.output_field(payload_schema.field(name), effective)
            for name in self.payload_schema.names
        ]

    def build_arrays(self, anchor_rows, fetcher):
        matches = [self.match(row) for row in anchor_rows]
        matched_ids = [match for match in matches if match is not None]
        unique_ids = list(dict.fromkeys(matched_ids))
        values = fetcher.fetch(unique_ids)
        positions = {
            row_id: index for index, row_id in enumerate(unique_ids)
        }
        take = pa.array([
            None if match is None else positions[match]
            for match in matches
        ], type=pa.int64())
        arrays = []
        for field in self.payload_schema:
            array = pc.take(values[field.name], take)
            array.validate()
            arrays.append(array)
        return arrays


class _LinearInterpolationRight(_AsOfJoinRight):

    def __init__(self, label, query, anchor_on, by, tolerance,
                 right_on, suffix):
        super().__init__(
            label, query, anchor_on, by, "nearest", tolerance,
            right_on, suffix)

    @staticmethod
    def output_field(field, effective=True):
        try:
            output_type = _linear_output_type(field.type)
        except TypeError:
            if effective:
                raise
            output_type = field.type
        return pa.field(
            field.name, output_type, nullable=True,
            metadata=field.metadata)

    def match(self, anchor_row):
        key = tuple(anchor_row[name] for name in self.by)
        bounds = self._index.get(key)
        if bounds is None:
            return None
        start, end = bounds
        target = anchor_row[_TIME_KEY]
        position = bisect_left(self._time_keys, target, start, end)
        if position < end and self._time_keys[position] == target:
            exact = bisect_right(
                self._time_keys, target, position, end) - 1
            row_id = self._row_ids[exact].as_py()
            return row_id, row_id, 0.0, 0, 1
        if position == start or position == end:
            return None

        before = position - 1
        after = position
        before_time = _python_scalar(self._time_keys[before])
        after_time = _python_scalar(self._time_keys[after])
        if (self._tolerance_key is not None
                and max(target - before_time, after_time - target)
                > self._tolerance_key):
            return None
        return (
            self._row_ids[before].as_py(),
            self._row_ids[after].as_py(),
            *_linear_weight(target, before_time, after_time, self.time_type),
        )

    def build_arrays(self, anchor_rows, fetcher):
        matches = [self.match(row) for row in anchor_rows]
        matched_ids = []
        for match in matches:
            if match is not None:
                matched_ids.extend(match[:2])
        unique_ids = list(dict.fromkeys(matched_ids))
        values = fetcher.fetch(unique_ids)
        positions = {
            row_id: index for index, row_id in enumerate(unique_ids)
        }
        before = pa.array([
            None if match is None else positions[match[0]]
            for match in matches
        ], type=pa.int64())
        after = pa.array([
            None if match is None else positions[match[1]]
            for match in matches
        ], type=pa.int64())
        weights = pa.array([
            None if match is None else match[2]
            for match in matches
        ], type=pa.float64())
        ratios = [
            None if match is None else match[3:5]
            for match in matches
        ]

        arrays = []
        for field in self.payload_schema:
            array = _interpolate_array(
                pc.take(values[field.name], before),
                pc.take(values[field.name], after),
                weights,
                ratios,
            )
            array.validate()
            arrays.append(array)
        return arrays


class _WindowJoinRight(_AsOfJoinRight):

    _SUPPORTED_AGGREGATIONS = {
        "count", "first", "last", "max", "mean", "min",
    }

    def __init__(self, label, query, anchor_on, by, preceding, following,
                 aggregations, closed, right_on, suffix):
        super().__init__(
            label, query, anchor_on, by, "nearest", None,
            right_on, suffix)
        self._preceding_key = _window_bound_key(
            "preceding", preceding, self.time_type)
        if following is None:
            following = (
                timedelta(0) if pa.types.is_timestamp(self.time_type) else 0)
        self._following_key = _window_bound_key(
            "following", following, self.time_type)
        if closed not in ("both", "left", "neither", "right"):
            raise ValueError(
                "closed must be 'both', 'left', 'right', or 'neither'.")
        self.closed = closed
        self.aggregations = _normalize_aggregations(
            aggregations, self.payload_schema, self.label,
            self._SUPPORTED_AGGREGATIONS)
        source_names = {
            specification[1] for specification in self.aggregations
        }
        self._fetch_names = tuple(
            field.name for field in self.payload_schema
            if field.name in source_names)
        self.payload_schema = pa.schema([
            field for field in self.payload_schema
            if field.name in source_names
        ], metadata=self.payload_schema.metadata)

    def output_fields(self, payload_schema, effective=True):
        fields = []
        for output_name, source_name, aggregation in self.aggregations:
            source = payload_schema.field(source_name)
            try:
                output_type = _aggregate_output_type(
                    source.type, aggregation)
            except TypeError:
                if effective:
                    raise
                output_type = source.type
            fields.append(pa.field(
                output_name, output_type, nullable=True,
                metadata=source.metadata))
        return fields

    def match(self, anchor_row):
        key = tuple(anchor_row[name] for name in self.by)
        bounds = self._index.get(key)
        if bounds is None:
            return []
        target = anchor_row[_TIME_KEY]
        start, end = bounds
        left = target - self._preceding_key
        right = target + self._following_key
        if pa.types.is_integer(self.time_type):
            first_key = (
                math.ceil(left)
                if self.closed in ("both", "left")
                else math.floor(left) + 1
            )
            last_key = (
                math.floor(right)
                if self.closed in ("both", "right")
                else math.ceil(right) - 1
            )
            if first_key > last_key:
                return []
            first = bisect_left(
                self._time_keys, first_key, start, end)
            last = bisect_right(
                self._time_keys, last_key, first, end)
        else:
            first = (
                bisect_left(self._time_keys, left, start, end)
                if self.closed in ("both", "left")
                else bisect_right(self._time_keys, left, start, end)
            )
            last = (
                bisect_right(self._time_keys, right, first, end)
                if self.closed in ("both", "right")
                else bisect_left(self._time_keys, right, first, end)
            )
        return [self._row_ids[index].as_py()
                for index in range(first, last)]

    def build_arrays(self, anchor_rows, fetcher):
        matches = [self.match(row) for row in anchor_rows]
        unique_ids = list(dict.fromkeys(
            row_id for match in matches for row_id in match))
        values = fetcher.fetch(unique_ids)
        positions = {
            row_id: index for index, row_id in enumerate(unique_ids)
        }
        indices = [
            [positions[row_id] for row_id in match]
            for match in matches
        ]
        arrays = []
        for _, source_name, aggregation in self.aggregations:
            effective = fetcher.schema.field(source_name)
            output_type = _aggregate_output_type(
                effective.type, aggregation)
            arrays.append(pa.array([
                _aggregate_values(
                    values[source_name], row_indices, aggregation)
                for row_indices in indices
            ], type=output_type))
        return arrays


def _normalize_aggregations(aggregations, schema, label, supported):
    if not isinstance(aggregations, dict) or not aggregations:
        raise ValueError("aggregations must be a non-empty dict.")
    normalized = []
    missing = []
    for output_name, specification in aggregations.items():
        if not isinstance(output_name, str) or not output_name:
            raise ValueError(
                "Aggregation output names must be non-empty strings.")
        if isinstance(specification, str):
            source_name = output_name
            aggregation = specification
        elif isinstance(specification, tuple) and len(specification) == 2:
            source_name, aggregation = specification
        else:
            raise ValueError(
                "Aggregation %r must be an operation or a "
                "(source column, operation) pair." % output_name)
        if not isinstance(source_name, str) or not source_name:
            raise ValueError(
                "Aggregation source columns must be non-empty strings.")
        if source_name not in schema.names:
            missing.append(source_name)
        if not isinstance(aggregation, str) or aggregation not in supported:
            raise ValueError(
                "Unsupported aggregation %r for output %r; expected one of "
                "%r." % (aggregation, output_name, sorted(supported)))
        normalized.append((output_name, source_name, aggregation))
    if missing:
        raise ValueError(
            "%s is missing aggregation columns %r." % (label, missing))
    return tuple(normalized)


def _aggregate_output_type(data_type, aggregation):
    if aggregation == "count":
        return pa.int64()
    if aggregation in ("first", "last"):
        return data_type
    if not (pa.types.is_integer(data_type)
            or pa.types.is_floating(data_type)):
        raise TypeError(
            "Window %s aggregation requires an integer or floating-point "
            "scalar column; got %s." % (aggregation, data_type))
    if aggregation == "mean":
        return pa.float64()
    return data_type


def _aggregate_values(values, indices, aggregation):
    if not indices:
        return 0 if aggregation == "count" else None
    selected = pc.take(values, pa.array(indices, type=pa.int64()))
    if aggregation == "count":
        return pc.count(selected).as_py()
    if aggregation == "mean":
        items = [item for item in selected.to_pylist()
                 if item is not None]
        if not items:
            return None
        if pa.types.is_integer(values.type):
            return sum(items) / len(items)
        if not all(math.isfinite(item) for item in items):
            return pc.mean(selected).as_py()
        try:
            return math.fsum(items) / len(items)
        except OverflowError:
            pass
        scale = max(abs(item) for item in items)
        if scale == 0:
            return 0.0
        return (math.fsum(item / scale for item in items) / len(items)) * scale
    if aggregation == "min":
        return pc.min(selected).as_py()
    if aggregation == "max":
        return pc.max(selected).as_py()
    items = selected.to_pylist()
    if aggregation == "first":
        return next((item for item in items if item is not None), None)
    return next((item for item in reversed(items) if item is not None), None)


def _window_bound_key(name, value, data_type):
    if isinstance(value, bool) or not isinstance(value, (Real, timedelta)):
        raise TypeError(
            "%s must be numeric or datetime.timedelta." % name)
    if (isinstance(value, Real) and not isinstance(value, Integral)
            and not math.isfinite(value)):
        raise ValueError("%s must be finite." % name)
    zero = timedelta(0) if isinstance(value, timedelta) else 0
    if value < zero:
        raise ValueError("%s must be non-negative." % name)
    _validate_tolerance(value, data_type)
    if pa.types.is_integer(data_type) and not isinstance(value, Integral):
        try:
            exact = Fraction(value)
        except TypeError:
            exact = Fraction(float(value))
        if exact.denominator == 1:
            return exact.numerator
        return exact
    return _time_tolerance_key(value, data_type)


def _validate_join_options(direction, tolerance, right_on, suffix):
    if direction not in ("backward", "forward", "nearest"):
        raise ValueError(
            "direction must be 'backward', 'forward', or 'nearest'.")
    if right_on is not None and (
            not isinstance(right_on, str) or not right_on):
        raise ValueError("right_on must be a non-empty column name.")
    if not isinstance(suffix, str):
        raise TypeError("suffix must be a string.")
    if tolerance is not None:
        if isinstance(tolerance, bool) or not isinstance(
                tolerance, (Real, timedelta)):
            raise TypeError("tolerance must be numeric or datetime.timedelta.")
        if (isinstance(tolerance, Real)
                and not isinstance(tolerance, Integral)
                and not math.isfinite(tolerance)):
            raise ValueError("tolerance must be finite.")
        zero = timedelta(0) if isinstance(tolerance, timedelta) else 0
        if tolerance < zero:
            raise ValueError("tolerance must be non-negative.")


def _linear_output_type(data_type):
    if pa.types.is_integer(data_type):
        return pa.float64()
    if pa.types.is_floating(data_type):
        return data_type
    if pa.types.is_fixed_size_list(data_type):
        return pa.list_(
            _linear_output_type(data_type.value_type), data_type.list_size)
    raise TypeError(
        "Linear interpolation requires integer or floating-point scalars "
        "or fixed-size lists; got %s." % data_type)


def _linear_weight(target, before, after, data_type):
    if pa.types.is_integer(data_type) or pa.types.is_timestamp(data_type):
        numerator = target - before
        denominator = after - before
        return numerator / denominator, numerator, denominator
    if pa.types.is_floating(data_type):
        ratios = [float(value).as_integer_ratio()
                  for value in (target, before, after)]
        common_denominator = max(
            denominator for unused, denominator in ratios)
        target, before, after = [
            numerator * (common_denominator // denominator)
            for numerator, denominator in ratios
        ]
        numerator = target - before
        denominator = after - before
        weight = numerator / denominator
        return weight, numerator, denominator
    weight = float(target - before) / (after - before)
    numerator, denominator = weight.as_integer_ratio()
    return weight, numerator, denominator


def _interpolate_array(before, after, weights, ratios):
    if isinstance(before, pa.ChunkedArray):
        before = before.combine_chunks()
    if isinstance(after, pa.ChunkedArray):
        after = after.combine_chunks()
    data_type = before.type
    output_type = _linear_output_type(data_type)
    if pa.types.is_fixed_size_list(data_type):
        size = data_type.list_size
        repeated = pa.array([
            weight for weight in weights.to_pylist() for unused in range(size)
        ], type=pa.float64())
        repeated_ratios = [
            ratio for ratio in ratios for unused in range(size)
        ]
        values = _interpolate_array(
            before.values.slice(before.offset * size, len(before) * size),
            after.values.slice(after.offset * size, len(after) * size),
            repeated,
            repeated_ratios,
        )
        mask = pc.or_(before.is_null(), after.is_null())
        result = pa.FixedSizeListArray.from_arrays(values, size)
        return pc.if_else(mask, pa.scalar(None, type=result.type), result)

    if pa.types.is_integer(data_type):
        result = []
        for start, end, ratio in zip(
                before.to_pylist(), after.to_pylist(), ratios):
            if start is None or end is None or ratio is None:
                result.append(None)
                continue
            numerator, denominator = ratio
            result.append((
                start * (denominator - numerator) + end * numerator
            ) / denominator)
        return pa.array(result, type=pa.float64())

    start = pc.cast(before, pa.float64())
    end = pc.cast(after, pa.float64())
    result = pc.add(
        pc.multiply(start, pc.subtract(1.0, weights)),
        pc.multiply(end, weights),
    )
    result = pc.if_else(pc.equal(start, end), start, result)
    result = pc.if_else(pc.equal(weights, 0.0), start, result)
    if result.type != output_type:
        result = pc.cast(result, output_type)
    return result


def _require_scan(query, label):
    if (type(query) is not ScanQuery
            or getattr(query, "_result_factory", None) is not None):
        raise TypeError("%s must be a MultimodalTable.scan() query." % label)
    return query


def _pin_scan_to_snapshot(query):
    table = query._table
    options = table.options
    if not options.row_tracking_enabled(False):
        raise ValueError(
            "Temporal alignment requires 'row-tracking.enabled' = 'true'.")
    if (options.scan_mode() == StartupMode.INCREMENTAL
            or options.options.contains(
                CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP)):
        raise ValueError(
            "Temporal alignment does not support incremental scans; "
            "inputs must "
            "represent a complete point-in-time snapshot.")
    # Validate the original scan configuration before replacing it with a
    # pinned snapshot. Otherwise an invalid or unsupported scan mode can be
    # silently converted into a latest-full scan.
    table.new_read_builder().new_scan()
    snapshot = TimeTravelUtil.try_travel_to_snapshot(
        options.options, table.tag_manager(), table.snapshot_manager())
    if snapshot is None:
        snapshot = table.snapshot_manager().get_latest_snapshot()
    empty = snapshot is None
    tag_name = (
        options.scan_tag_name()
        if options.options.contains_key(CoreOptions.SCAN_TAG_NAME.key())
        else None
    )
    if snapshot is not None and tag_name is None:
        table = table_at_snapshot(table, snapshot)
    pinned = ScanQuery(table)
    pinned._predicate = query._predicate
    pinned._projection = query._projection
    pinned._limit = query._limit
    pinned._include_row_id = query._include_row_id
    pinned._temporal_empty = empty
    pinned._temporal_snapshot_id = (
        None if snapshot is None else snapshot.id)
    pinned._temporal_tag_name = tag_name
    return pinned


def _resolved_snapshot(query):
    resolved = {
        "table": query._table.identifier.get_full_name(),
        "snapshot_id": query._temporal_snapshot_id,
    }
    tag_name = getattr(query, "_temporal_tag_name", None)
    if tag_name is not None:
        resolved["tag_name"] = tag_name
    return resolved


def _query_schema(query):
    return _query_schema_and_paths(query)[0]


def _query_schema_and_paths(query):
    table = query._table.copy_without_time_travel({
        CoreOptions.BLOB_AS_DESCRIPTOR.key(): "true",
    })
    builder = query._configured_read_builder(table)
    schema = PyarrowFieldParser.from_paimon_schema(builder.read_type())
    paths = builder._nested_name_paths()
    if paths is None:
        paths = [[field.name] for field in schema]
    return schema, paths


def _table_schema(query):
    return PyarrowFieldParser.from_paimon_schema(query._table.fields)


def _metadata_table(query, on, by):
    read_builder, splits, key_columns, output_columns = (
        _metadata_builders(query, on, by))
    if getattr(query, "_temporal_empty", False):
        return _empty_metadata(query, key_columns, output_columns)
    arrow = read_builder.new_read().to_arrow(splits)
    metadata = arrow.select(output_columns).combine_chunks()
    _validate_metadata(query, metadata, key_columns)
    sort_keys = [(name, "ascending") for name in output_columns]
    return metadata.take(pc.sort_indices(metadata, sort_keys=sort_keys))


def _metadata_batches(query, on, by, batch_size):
    read_builder, splits, key_columns, output_columns = (
        _metadata_builders(query, on, by))
    if getattr(query, "_temporal_empty", False):
        return
    reader = read_builder.new_read()._to_managed_arrow_batch_reader(splits)
    try:
        for batch in reader:
            metadata = pa.Table.from_batches([batch]).select(output_columns)
            _validate_metadata(query, metadata, key_columns)
            for start in range(0, len(metadata), batch_size):
                yield metadata.slice(start, batch_size)
    finally:
        reader.close()


def _metadata_builders(query, on, by):
    _validate_pinned_tag(query)
    key_columns = list(dict.fromkeys(by + (on,)))
    output_columns = key_columns + [_ROW_ID]
    table = query._table.copy_without_time_travel({
        CoreOptions.BLOB_AS_DESCRIPTOR.key(): "true",
    })
    plan_builder = table.new_read_builder()
    if query._predicate is not None:
        plan_builder = plan_builder.with_filter(query._predicate)
    plan_builder = plan_builder.with_projection(key_columns)
    if query._limit is not None:
        plan_builder = plan_builder.with_limit(query._limit)
    splits, masking = _plan_with_internal_row_id(
        plan_builder,
        query._temporal_snapshot_id,
        getattr(query, "_temporal_empty", False),
    )
    key_masking = {
        name: masking[name] for name in key_columns if name in masking
    }
    dependencies = _mask_dependencies(
        key_masking, key_columns, _table_schema(query))
    splits = _with_active_masking(splits, key_columns)
    read_projection = list(dict.fromkeys(
        output_columns + dependencies))
    read_builder = table.new_read_builder().with_projection(read_projection)
    if query._predicate is not None:
        read_builder = read_builder.with_filter(query._predicate)
    if query._limit is not None:
        read_builder = read_builder.with_limit(query._limit)

    physical_schema = PyarrowFieldParser.from_paimon_schema(
        read_builder.read_type())
    effective_schema = _effective_masked_schema(
        physical_schema, key_masking)
    for name in key_columns:
        physical_type = physical_schema.field(name).type
        effective_type = effective_schema.field(name).type
        if effective_type != physical_type:
            raise TypeError(
                "Temporal key %r must preserve its type after column "
                "masking; got %s instead of %s."
                % (name, effective_type, physical_type)
            )
    return read_builder, splits, key_columns, output_columns


def _empty_metadata(query, key_columns, output_columns):
    return pa.Table.from_arrays([
        pa.array([], type=_table_schema(query).field(name).type)
        for name in key_columns
    ] + [pa.array([], type=pa.int64())], names=output_columns)


def _validate_metadata(query, metadata, key_columns):
    for name in key_columns:
        column = metadata[name]
        if column.null_count:
            raise ValueError("Temporal key %r cannot be null." % name)
        if pa.types.is_floating(column.type):
            for scalar in column:
                if not math.isfinite(scalar.as_py()):
                    raise ValueError(
                        "Temporal key %r must be finite." % name)


class _RowIdFetcher:

    def __init__(self, query, row_group_cache, output_names=None):
        _validate_pinned_tag(query)
        query_schema, query_paths = _query_schema_and_paths(query)
        visible_projection = query._effective_projection()
        if output_names is None:
            self._schema = query_schema
        else:
            output_names = set(output_names)
            selected = [
                (field, path)
                for field, path in zip(query_schema, query_paths)
                if field.name in output_names
            ]
            self._schema = pa.schema(
                [field for field, unused in selected],
                metadata=query_schema.metadata,
            )
            visible_projection = [
                ".".join(path) for unused, path in selected
            ]
        table = query._table.copy_without_time_travel({
            CoreOptions.BLOB_AS_DESCRIPTOR.key(): "true",
        })
        plan_builder = table.new_read_builder()
        if visible_projection is not None:
            plan_projection = visible_projection
            projected = table.new_read_builder().with_projection(
                visible_projection)
            projected_paths = projected._nested_name_paths()
            if projected_paths is not None:
                plan_projection = list(dict.fromkeys(
                    path[0] for path in projected_paths))
            plan_builder = plan_builder.with_projection(plan_projection)
        if query._predicate is not None:
            plan_builder = plan_builder.with_filter(query._predicate)
        self._splits, masking = _plan_with_internal_row_id(
            plan_builder,
            query._temporal_snapshot_id,
            getattr(query, "_temporal_empty", False),
        )

        read_projection = (
            [field.name for field in table.fields]
            if not visible_projection else list(visible_projection)
        )
        projected_builder = table.new_read_builder().with_projection(
            list(dict.fromkeys(read_projection + [_ROW_ID])))
        projected_schema = PyarrowFieldParser.from_paimon_schema(
            projected_builder.read_type())
        projected_paths = projected_builder._nested_name_paths()
        if projected_paths is not None:
            for field, path in zip(projected_schema, projected_paths):
                if field.name in masking and field.name != path[0]:
                    raise ValueError(
                        "Temporal alignment cannot safely apply column "
                        "masking to nested projection %r."
                        % ".".join(path)
                    )
        if projected_paths is None:
            active_targets = projected_schema.names
        else:
            active_targets = list(dict.fromkeys(
                path[0] for path in projected_paths))
        dependencies = _mask_dependencies(
            masking, active_targets, _table_schema(query))
        read_projection = list(dict.fromkeys(
            read_projection + dependencies + [_ROW_ID]))
        visible_builder = table.new_read_builder().with_projection(
            read_projection)
        self._fetch_schema = PyarrowFieldParser.from_paimon_schema(
            visible_builder.read_type())
        self._name_paths = visible_builder._nested_name_paths()
        self._row_id_name = _ROW_ID
        if self._name_paths is not None:
            self._row_id_name = next(
                field.name
                for field, path in zip(
                    self._fetch_schema, self._name_paths)
                if path == [_ROW_ID]
            )
        if self._name_paths is None:
            builder = visible_builder
        else:
            top_level = list(dict.fromkeys(
                path[0] for path in self._name_paths))
            builder = table.new_read_builder().with_projection(top_level)
        if query._predicate is not None:
            builder = builder.with_filter(query._predicate)
        self._read = builder.new_read()
        self._read._parquet_row_group_cache = row_group_cache
        physical_schema = PyarrowFieldParser.from_paimon_schema(
            builder.read_type())
        effective_schema = _effective_masked_schema(
            physical_schema, masking)
        visible_paths = (
            None if self._name_paths is None
            else self._name_paths[:len(self._schema)]
        )
        self._schema = _project_effective_schema(
            self._schema, visible_paths, effective_schema, masking)
        self._fetch_schema = _project_effective_schema(
            self._fetch_schema, self._name_paths,
            effective_schema, masking)
        self._split_ranges = [
            self._row_ranges(split) for split in self._splits]
        self._range_intervals = sorted(
            (row_range.from_, row_range.to, split_index)
            for split_index, ranges in enumerate(self._split_ranges)
            for row_range in ranges
        )
        self._range_starts = [
            interval[0] for interval in self._range_intervals]
        self._range_max_ends = []
        max_end = -1
        for _, end, _ in self._range_intervals:
            max_end = max(max_end, end)
            self._range_max_ends.append(max_end)

    @property
    def schema(self):
        return self._schema

    @staticmethod
    def _row_ranges(split):
        if isinstance(split, QueryAuthSplit):
            split = split.split
        if isinstance(split, IndexedSplit):
            ranges = split.row_ranges()
        else:
            ranges = [
                data_file.row_id_range()
                for data_file in split.files
                if data_file.row_id_range() is not None
            ]
        return Range.sort_and_merge_overlap(ranges, True)

    def fetch(self, row_ids):
        if not row_ids:
            return pa.Table.from_arrays(
                [pa.array([], type=field.type) for field in self._schema],
                schema=self._schema,
            )

        wanted = Range.sort_and_merge_overlap(
            [Range(row_id, row_id) for row_id in set(row_ids)], True)
        selected_splits = []
        for split_index in self._find_splits(wanted):
            original = self._splits[split_index]
            auth_result = None
            split = original
            if isinstance(split, QueryAuthSplit):
                auth_result = split.auth_result
                split = split.split
            if isinstance(split, IndexedSplit):
                split = split.data_split()
            allowed = Range.and_(wanted, self._split_ranges[split_index])
            if not allowed:
                continue
            indexed = IndexedSplit(
                split,
                allowed,
                exact_merged_row_count=sum(r.count() for r in allowed),
            )
            if auth_result is not None:
                indexed = QueryAuthSplit(indexed, auth_result)
            selected_splits.append(indexed)

        arrow = self._project_fetch(self._read.to_arrow(selected_splits))
        found = arrow[self._row_id_name].to_pylist()
        positions = {}
        for index, row_id in enumerate(found):
            if row_id in positions:
                raise RuntimeError(
                    "Duplicate row id %r in aligned scan." % row_id)
            positions[row_id] = index
        missing = [row_id for row_id in row_ids if row_id not in positions]
        if missing:
            raise RuntimeError(
                "Aligned row ids disappeared from pinned snapshot: %r."
                % missing
            )
        take = pa.array(
            [positions[row_id] for row_id in row_ids], type=pa.int64())
        visible = pa.Table.from_arrays(
            [arrow.column(index) for index in range(len(self._schema))],
            schema=self._schema,
        )
        return visible.take(take)

    def _find_splits(self, ranges):
        split_indices = set()
        for row_range in ranges:
            right = bisect_right(self._range_starts, row_range.to)
            left = bisect_left(
                self._range_max_ends, row_range.from_, 0, right)
            for position in range(left, right):
                _, end, split_index = self._range_intervals[position]
                if end >= row_range.from_:
                    split_indices.add(split_index)
        return sorted(split_indices)

    def _project_fetch(self, arrow):
        if self._name_paths is None:
            return arrow
        arrays = []
        for path in self._name_paths:
            array = arrow[path[0]]
            for name in path[1:]:
                index = array.type.get_field_index(name)
                if index < 0:
                    raise KeyError("Nested field %r does not exist." % name)
                array = array.flatten()[index]
            arrays.append(array)
        return pa.Table.from_arrays(arrays, schema=self._fetch_schema)


def _arrow_rows(table):
    if hasattr(table, "to_pylist"):
        return table.to_pylist()
    columns = table.to_pydict()
    return [
        {name: columns[name][index] for name in table.column_names}
        for index in range(table.num_rows)
    ]


def _metadata_rows(table, on, time_type):
    rows = _arrow_rows(table)
    if pa.types.is_timestamp(time_type):
        times = table[on].combine_chunks()
        for index, row in enumerate(rows):
            row[_TIME_KEY] = times[index].value
    else:
        for row in rows:
            row[_TIME_KEY] = row[on]
    return rows


def _validate_pinned_tag(query):
    tag_name = getattr(query, "_temporal_tag_name", None)
    if tag_name is None:
        return
    tag = query._table.tag_manager().get(tag_name)
    if tag is None or tag.id != query._temporal_snapshot_id:
        raise RuntimeError(
            "Tag %r changed after temporal alignment was created." % tag_name)


def _plan_with_internal_row_id(
        builder, expected_snapshot_id, empty=False):
    scan = builder.new_scan()
    if empty:
        auth_result = resolve_auth_result(
            getattr(scan, "_query_auth_fn", None), scan._read_type)
        masking = _masking_rules([auth_result])
        if _ROW_ID in masking:
            raise ValueError(
                "Temporal alignment cannot use a query that masks _ROW_ID.")
        return [], masking

    auth_results = []
    query_auth = getattr(scan, "_query_auth_fn", None)
    if query_auth is not None:
        def capture_auth(select):
            result = query_auth(select)
            auth_results.append(result)
            return result

        scan._query_auth_fn = capture_auth
    plan = scan.plan()
    if plan.snapshot_id != expected_snapshot_id:
        raise RuntimeError(
            "Temporal input changed from snapshot %r to %r during planning."
            % (expected_snapshot_id, plan.snapshot_id)
        )
    splits = plan.splits()
    auth_results.extend(
        split.auth_result for split in splits
        if isinstance(split, QueryAuthSplit)
    )
    masking = _masking_rules(auth_results)
    if _ROW_ID in masking:
        raise ValueError(
            "Temporal alignment cannot use a query that masks _ROW_ID.")
    return splits, masking


def _masking_rules(auth_results):
    masking = None
    for auth_result in auth_results:
        if auth_result is None:
            continue
        current = dict(
            getattr(auth_result, "column_masking", None) or {})
        if masking is None:
            masking = current
        elif current != masking:
            raise RuntimeError(
                "Column masking rules changed during query planning.")
    parsed = {}
    for name, rule in (masking or {}).items():
        if not rule:
            continue
        transform = json.loads(rule)
        if transform is not None:
            parsed[name] = transform
    return parsed


def _with_active_masking(splits, targets):
    active = set(targets)
    result = []
    for split in splits:
        if not isinstance(split, QueryAuthSplit):
            result.append(split)
            continue
        auth = split.auth_result
        masking = getattr(auth, "column_masking", None) or {}
        restricted = {
            name: rule for name, rule in masking.items() if name in active
        }
        if restricted == masking:
            result.append(split)
            continue
        auth = TableQueryAuthResult(
            filter=getattr(auth, "filter", None),
            column_masking=restricted or None,
        )
        result.append(
            QueryAuthSplit(split.split, auth)
            if auth.has_restrictions else split.split
        )
    return result


def _mask_dependencies(masking, targets, table_schema):
    dependencies = set()
    ordered_targets = list(dict.fromkeys(targets))
    readable = set(ordered_targets)
    pending = list(ordered_targets)
    while pending:
        target = pending.pop(0)
        transform = masking.get(target)
        if transform is None:
            continue
        for name in _collect_all_field_refs_from_transform(transform):
            if name not in table_schema.names:
                raise ValueError(
                    "Column masking for %r refers to unknown field %r."
                    % (target, name)
                )
            if name not in readable:
                readable.add(name)
                dependencies.add(name)
                pending.append(name)
    return [
        name for name in table_schema.names if name in dependencies
    ]


def _effective_masked_schema(schema, masking):
    if not masking:
        return schema
    batch = pa.RecordBatch.from_arrays([
        pa.array([], type=field.type) for field in schema
    ], schema=schema)
    fields = []
    for field in schema:
        transform = masking.get(field.name)
        if transform is None:
            fields.append(field)
            continue
        masked = _apply_predicate_transform(
            transform, batch, null_type=field.type)
        fields.append(pa.field(
            field.name, masked.type, nullable=True,
            metadata=field.metadata))
    return pa.schema(fields, metadata=schema.metadata)


def _project_effective_schema(
        schema, name_paths, effective_schema, masking):
    if not masking:
        return schema
    paths = name_paths or [(field.name,) for field in schema]
    fields = []
    for field, path in zip(schema, paths):
        target = path[0]
        if not masking.get(target):
            fields.append(field)
            continue
        masked_field = effective_schema.field(target)
        masked_type = masked_field.type
        for name in path[1:]:
            if not pa.types.is_struct(masked_type):
                raise TypeError(
                    "Column masking for %r no longer produces the struct "
                    "required by nested projection %r."
                    % (target, ".".join(path))
                )
            index = masked_type.get_field_index(name)
            if index < 0:
                raise TypeError(
                    "Column masking for %r does not produce nested field %r."
                    % (target, name)
                )
            masked_type = masked_type[index].type
        fields.append(pa.field(
            field.name, masked_type, nullable=True,
            metadata=field.metadata))
    return pa.schema(fields, metadata=schema.metadata)


def _match_index(times, target, method, start=0, end=None):
    end = len(times) if end is None else end
    if start >= end:
        return None
    position = bisect_left(times, target, start, end)
    if method == "backward":
        position = bisect_right(times, target, start, end)
        return position - 1 if position > start else None
    if method == "forward":
        return position if position < end else None
    if method == "nearest":
        if position < end and times[position] == target:
            return bisect_right(times, target, position, end) - 1
        if position == start:
            return start
        if position == end:
            return end - 1
        before = position - 1
        before_value = _python_scalar(times[before])
        after_value = _python_scalar(times[position])
        if target - before_value <= after_value - target:
            return before
        return position
    raise ValueError("Unknown temporal match method %r." % method)


def _time_search_keys(values, data_type):
    if pa.types.is_timestamp(data_type):
        values = pc.cast(values, pa.int64())
    return values.to_numpy(zero_copy_only=False)


def _time_tolerance_key(tolerance, data_type):
    if tolerance is None or not pa.types.is_timestamp(data_type):
        return tolerance
    return pa.scalar(tolerance, type=pa.duration(data_type.unit)).value


def _python_scalar(value):
    item = getattr(value, "item", None)
    return item() if item is not None else value


def _validate_tolerance(tolerance, data_type):
    if tolerance is None:
        return
    if pa.types.is_timestamp(data_type):
        if not isinstance(tolerance, timedelta):
            raise TypeError(
                "Timestamp alignment tolerance must be datetime.timedelta.")
        return
    if isinstance(tolerance, timedelta):
        raise TypeError("Numeric alignment tolerance must be numeric.")
    if pa.types.is_integer(data_type) and tolerance > _MAX_INT64:
        raise ValueError(
            "Integer alignment tolerance cannot exceed int64 maximum.")


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


def _validate_group_type(name, data_type):
    if pa.types.is_nested(data_type) or pa.types.is_null(data_type):
        raise TypeError(
            "Grouping column %r must have a scalar type; got %s."
            % (name, data_type)
        )


def _require_columns(schema, columns, label):
    missing = [name for name in columns if name not in schema.names]
    if missing:
        raise ValueError(
            "%s is missing temporal columns %r." % (label, missing))
