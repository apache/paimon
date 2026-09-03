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
import math
from numbers import Integral, Real

import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.multimodal.query import ScanQuery
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.snapshot.time_travel_util import TimeTravelUtil
from pypaimon.table.special_fields import SpecialFields
from pypaimon.table.source.global_index_live_row_filter import (
    table_at_snapshot,
)
from pypaimon.utils.range import Range


_ROW_ID = SpecialFields.ROW_ID.name
_MAX_INT64 = (1 << 63) - 1


def join_asof(left, right, *, on, by, direction="backward", tolerance=None,
              right_on=None, suffix="_right"):
    """Join each left row with at most one time-aligned right row."""
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
            "join_asof requires at least one grouping column in by.")
    if (any(not isinstance(name, str) or not name for name in by)
            or len(set(by)) != len(by)):
        raise ValueError("by must contain unique, non-empty column names.")
    return _AsOfJoin(left, on, by).join_asof(
        right,
        direction=direction,
        tolerance=tolerance,
        right_on=right_on,
        suffix=suffix,
    )


class _AsOfJoin:
    """Lazy, chainable result of :func:`join_asof`."""

    def __init__(self, left, on, by):
        self._anchor = _pin_scan_to_snapshot(_require_scan(left, "left"))
        self._on = on
        self._by = by
        self._sources = ()
        self._anchor_schema = _query_schema(self._anchor)
        self._anchor_table_schema = _table_schema(self._anchor)
        self._validate_anchor()
        self.schema = self._output_schema()

    def join_asof(self, right, *, direction="backward", tolerance=None,
                  right_on=None, suffix="_right"):
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

        result = object.__new__(_AsOfJoin)
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
        """Plan scalar timestamps once, then fetch selected rows in batches."""
        if (isinstance(batch_size, bool)
                or not isinstance(batch_size, int)
                or batch_size <= 0):
            raise ValueError("batch_size must be a positive integer.")

        anchor_metadata = _metadata_table(self._anchor, self._on, self._by)
        anchor_fetcher = _RowIdFetcher(self._anchor)
        source_fetchers = []
        for source in self._sources:
            source.plan()
            source_fetchers.append(_RowIdFetcher(source.query))

        def batches():
            for start in range(0, len(anchor_metadata), batch_size):
                rows = _arrow_rows(anchor_metadata.slice(start, batch_size))
                yield self._build_batch(
                    rows, anchor_fetcher, source_fetchers)

        return pa.ipc.RecordBatchReader.from_batches(self.schema, batches())

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

    def _output_schema(self):
        fields = list(self._anchor_schema)
        names = set(self._anchor_schema.names)
        for source in self._sources:
            for field in source.payload_schema:
                output_name = field.name
                if output_name in names:
                    output_name += source.suffix
                if output_name in names:
                    raise ValueError(
                        "%s column %r conflicts after applying suffix %r."
                        % (source.label, field.name, source.suffix)
                    )
                output = pa.field(output_name, field.type, nullable=True)
                fields.append(output)
                names.add(output.name)
        return pa.schema(fields)

    def _build_batch(self, anchor_rows, anchor_fetcher, source_fetchers):
        anchor_ids = [row[_ROW_ID] for row in anchor_rows]
        anchor = anchor_fetcher.fetch(anchor_ids)
        arrays = [anchor[name] for name in self._anchor_schema.names]

        for source, fetcher in zip(self._sources, source_fetchers):
            matches = [source.match(row) for row in anchor_rows]
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
            for field in source.payload_schema:
                arrays.append(pc.take(values[field.name], take))

        table = pa.Table.from_arrays(
            arrays, schema=self.schema).combine_chunks()
        return table.to_batches(max_chunksize=len(anchor_rows))[0]


class _AsOfJoinRight:

    def __init__(self, label, query, anchor_on, by, direction, tolerance,
                 right_on, suffix):
        _validate_join_options(direction, tolerance, right_on, suffix)
        self.label = label
        self.query = _pin_scan_to_snapshot(_require_scan(query, label))
        self.direction = direction
        self.tolerance = tolerance
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
        schema = _query_schema(self.query)
        self.payload_schema = pa.schema([
            field for field in schema
            if field.name not in set(by + (self.on,))
        ])
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
            if (position > start
                    and self._time_keys[position]
                    == self._time_keys[position - 1]):
                raise ValueError(
                    "%s has duplicate timestamps within group %r."
                    % (self.label, key)
                )
            previous = key
        if len(metadata):
            self._index[previous] = (start, len(metadata))

    def match(self, anchor_row):
        key = tuple(anchor_row[name] for name in self.by)
        bounds = self._index.get(key)
        if bounds is None:
            return None
        target = anchor_row[self.anchor_on]
        target_key = _time_search_key(target, self.time_type)
        index = _match_index(
            self._time_keys, target_key, self.direction, *bounds)
        if index is None:
            return None
        matched_time = self._times[index].as_py()
        delta = matched_time - target
        if self.tolerance is not None and abs(delta) > self.tolerance:
            return None
        return self._row_ids[index].as_py()


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
            "join_asof requires 'row-tracking.enabled' = 'true'.")
    snapshot = TimeTravelUtil.try_travel_to_snapshot(
        options.options, table.tag_manager(), table.snapshot_manager())
    if snapshot is None:
        snapshot = table.snapshot_manager().get_latest_snapshot()
    empty = snapshot is None
    if snapshot is not None:
        table = table_at_snapshot(table, snapshot)
    pinned = ScanQuery(table)
    pinned._predicate = query._predicate
    pinned._projection = query._projection
    pinned._limit = query._limit
    pinned._include_row_id = query._include_row_id
    pinned._temporal_empty = empty
    return pinned


def _query_schema(query):
    table = query._table.copy_without_time_travel({
        CoreOptions.BLOB_AS_DESCRIPTOR.key(): "true",
    })
    builder = query._configured_read_builder(table)
    return PyarrowFieldParser.from_paimon_schema(builder.read_type())


def _table_schema(query):
    return PyarrowFieldParser.from_paimon_schema(query._table.fields)


def _metadata_table(query, on, by):
    key_columns = list(dict.fromkeys(by + (on,)))
    output_columns = key_columns + [_ROW_ID]
    if getattr(query, "_temporal_empty", False):
        return pa.Table.from_arrays([
            pa.array([], type=_table_schema(query).field(name).type)
            for name in key_columns
        ] + [pa.array([], type=pa.int64())], names=output_columns)
    table = query._table.copy_without_time_travel({
        CoreOptions.BLOB_AS_DESCRIPTOR.key(): "true",
    })
    builder = table.new_read_builder()
    if query._predicate is not None:
        builder = builder.with_filter(query._predicate)
    builder = builder.with_projection(output_columns)
    if query._limit is not None:
        builder = builder.with_limit(query._limit)
    splits = _plan_with_visible_row_ids(builder)
    arrow = builder.new_read().to_arrow(splits)
    metadata = arrow.select(output_columns).combine_chunks()
    schema = _table_schema(query)
    for name in key_columns:
        column = metadata[name]
        if column.null_count:
            raise ValueError("Temporal key %r cannot be null." % name)
        if pa.types.is_floating(schema.field(name).type):
            for scalar in column:
                if not math.isfinite(scalar.as_py()):
                    raise ValueError(
                        "Temporal key %r must be finite." % name)
    sort_keys = [(name, "ascending") for name in output_columns]
    return metadata.take(pc.sort_indices(metadata, sort_keys=sort_keys))


class _RowIdFetcher:

    def __init__(self, query):
        self._schema = _query_schema(query)
        table = query._table.copy_without_time_travel({
            CoreOptions.BLOB_AS_DESCRIPTOR.key(): "true",
        })
        projection = query._effective_projection()
        if projection is None:
            projection = [field.name for field in table.fields]
        projection = list(dict.fromkeys(projection + [_ROW_ID]))
        visible_builder = table.new_read_builder().with_projection(projection)
        self._fetch_schema = PyarrowFieldParser.from_paimon_schema(
            visible_builder.read_type())
        self._name_paths = visible_builder._nested_name_paths()
        if self._name_paths is None:
            builder = visible_builder
        else:
            top_level = list(dict.fromkeys(
                path[0] for path in self._name_paths))
            builder = table.new_read_builder().with_projection(top_level)
        if query._predicate is not None:
            builder = builder.with_filter(query._predicate)
        self._read = builder.new_read()
        self._splits = _plan_with_visible_row_ids(builder)
        self._split_ranges = [
            self._row_ranges(split) for split in self._splits]
        self._range_index = _SplitRangeIndex(self._split_ranges)

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
        for split_index in self._range_index.find(wanted):
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
        found = arrow[_ROW_ID].to_pylist()
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
        return arrow.select(self._schema.names).take(take)

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


class _SplitRangeIndex:

    def __init__(self, ranges_by_split):
        self._intervals = sorted(
            (row_range.from_, row_range.to, split_index)
            for split_index, ranges in enumerate(ranges_by_split)
            for row_range in ranges
        )
        self._starts = [interval[0] for interval in self._intervals]
        self._max_ends = []
        max_end = -1
        for _, end, _ in self._intervals:
            max_end = max(max_end, end)
            self._max_ends.append(max_end)

    def find(self, ranges):
        split_indices = set()
        for row_range in ranges:
            right = bisect_right(self._starts, row_range.to)
            left = bisect_left(
                self._max_ends, row_range.from_, 0, right)
            for position in range(left, right):
                _, end, split_index = self._intervals[position]
                if end >= row_range.from_:
                    split_indices.add(split_index)
        return sorted(split_indices)


def _arrow_rows(table):
    if hasattr(table, "to_pylist"):
        return table.to_pylist()
    columns = table.to_pydict()
    return [
        {name: columns[name][index] for name in table.column_names}
        for index in range(table.num_rows)
    ]


def _plan_with_visible_row_ids(builder):
    splits = builder.new_scan().plan().splits()
    for split in splits:
        if not isinstance(split, QueryAuthSplit):
            continue
        masking = getattr(split.auth_result, "column_masking", None)
        if masking and _ROW_ID in masking:
            raise ValueError(
                "Temporal alignment cannot use a query that masks _ROW_ID.")
    return splits


def _match_index(times, target, method, start=0, end=None):
    end = len(times) if end is None else end
    if start >= end:
        return None
    position = bisect_left(times, target, start, end)
    if method == "backward":
        if position < end and times[position] == target:
            return position
        return position - 1 if position > start else None
    if method == "forward":
        return position if position < end else None
    if method == "nearest":
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


def _time_search_key(value, data_type):
    if pa.types.is_timestamp(data_type):
        return pa.scalar(value, type=data_type).value
    return value


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
