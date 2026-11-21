# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import math
import re

import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.common.options.config_options import ConfigOptions
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.schema.data_types import AtomicType, PyarrowFieldParser
from pypaimon.table.row.generic_row import GenericRow


_DENSE_STORE = ConfigOptions.key('metadata.stats-dense-store').boolean_type().default_value(True)
_KEEP_FIRST = ConfigOptions.key('metadata.stats-keep-first-n-columns').int_type().default_value(-1)
_PER_LEVEL = 'metadata.stats-mode.per.level'
_CHANGELOG_MODE = ConfigOptions.key('changelog-file.stats-mode').string_type().no_default_value()
_TRUNCATE = re.compile(r'truncate\(([0-9]+)\)')


def _unquote_map_token(value):
    value = str(value).strip()
    if len(value) >= 2 and value[0] in "'\"" and value[-1] == value[0]:
        quote = value[0]
        inner = value[1:-1]
        if quote not in inner.replace(quote * 2, ''):
            return inner.replace(quote * 2, quote)
    return value


def _mode_at_level(options, level):
    mode_map = options.get(_PER_LEVEL)
    if mode_map is None:
        prefix = _PER_LEVEL + '.'
        entries = ((key[len(prefix):], value) for key, value in options.items() if key.startswith(prefix))
    elif isinstance(mode_map, dict):
        entries = mode_map.items()
    else:
        # Stats modes and level numbers contain neither commas nor colons.
        entries = (_unquote_map_token(entry).split(':', 1) for entry in mode_map.split(',') if entry.strip())
    result = None
    for key, mode in entries:
        if int(_unquote_map_token(key)) == level:
            result = _unquote_map_token(mode)
    return result


def _parse_mode(value):
    mode = value.lower()
    if mode in ('none', 'counts', 'full'):
        return mode, None
    match = _TRUNCATE.fullmatch(mode)
    if match is not None:
        length = int(match.group(1))
        if 0 < length <= 2147483647:
            return 'truncate', length
    raise ValueError('Invalid metadata stats mode: {!r}'.format(value))


def _truncate_bounds(minimum, maximum, length):
    if maximum is None:
        return None, None
    if isinstance(minimum, (str, bytes)):
        minimum = minimum[:length]
    if not isinstance(maximum, (str, bytes)) or len(maximum) <= length:
        return minimum, maximum
    prefix = maximum[:length]
    is_text = isinstance(prefix, str)
    ceiling = 0x10FFFF if is_text else 0xFF
    for i in range(len(prefix) - 1, -1, -1):
        code = ord(prefix[i]) if is_text else prefix[i]
        if code < ceiling:
            # Java advances across the reserved UTF-16 surrogate range.
            next_code = 0xE000 if is_text and code == 0xD7FF else code + 1
            upper = chr(next_code) if is_text else bytes([next_code])
            return minimum, prefix[:i] + upper
    # A truncated upper bound cannot be represented. Drop both bounds as Java does.
    return None, None


class _ParquetBounds:
    """Reuse bounds from the writer's in-memory footer for matching scalar leaves."""

    def __init__(self, metadata):
        schema = metadata.schema
        self._indices = {}
        for index in range(metadata.num_columns):
            path = schema.column(index).path
            # A literal dotted column name can collide with a nested leaf path.
            self._indices[path] = None if path in self._indices else index
        self._row_groups = [metadata.row_group(i) for i in range(metadata.num_row_groups)]

    def bounds(self, name, column_type):
        index = self._indices.get(name)
        if index is None or not (
            pa.types.is_integer(column_type) or pa.types.is_boolean(column_type)
            or column_type in (pa.float32(), pa.float64())
            or pa.types.is_string(column_type) or pa.types.is_binary(column_type)
            or pa.types.is_fixed_size_binary(column_type)
        ):
            return None
        minimum, maximum = None, None
        for group in self._row_groups:
            statistics = group.column(index).statistics
            if statistics is None:
                return None
            if not statistics.has_min_max:
                if statistics.null_count == group.num_rows:
                    continue
                # PyArrow can omit bounds for long values or unsupported statistics.
                return None
            low, high = statistics.min, statistics.max
            if low is None or high is None:
                return None
            minimum = low if minimum is None else min(minimum, low)
            maximum = high if maximum is None else max(maximum, high)
        # Parquet normalizes zero signs. Some writers also use a finite sentinel for
        # the other bound of an all-infinity column. Consult the input in either case.
        if minimum is None or (pa.types.is_floating(column_type) and (
                minimum == 0.0 or maximum == 0.0 or not math.isfinite(minimum) or not math.isfinite(maximum))):
            return None
        return minimum, maximum


def column_stats(data, name, bounds=True, parquet_bounds=None):
    column = data.column(name)
    stats = dict(min_values=None, max_values=None, null_counts=column.null_count)
    if (not bounds or column.null_count == len(column) or pa.types.is_nested(column.type)
            or pa.types.is_large_binary(column.type)):
        return stats
    if pa.types.is_floating(column.type) and pc.any(pc.is_nan(column)).as_py():
        # File readers can disagree on where NaN sorts. Preserve only the null count.
        stats['bounds_unavailable'] = True
        return stats
    if parquet_bounds is not None:
        file_bounds = parquet_bounds.bounds(name, column.type)
        if file_bounds is not None:
            stats['min_values'], stats['max_values'] = file_bounds
            return stats
    try:
        min_max = pc.min_max(column)
        minimum, maximum = min_max['min'].as_py(), min_max['max'].as_py()
    except (pa.ArrowNotImplementedError, OverflowError):
        # Some Arrow dates and timestamps fall outside Python's datetime range.
        stats['bounds_unavailable'] = True
        return stats
    if pa.types.is_timestamp(column.type) and (minimum != minimum or maximum != maximum):
        # pandas treats the minimum signed i64 timestamp as NaT.
        stats['bounds_unavailable'] = True
        return stats
    if pa.types.is_floating(column.type) and (minimum == 0.0 or maximum == 0.0):
        # Arrow min_max treats both zeros as equal. Inspect their IEEE bits without
        # copying the values or losing the validity bitmap of sliced arrays.
        bits = column.type.bit_width
        int_type = pa.int32() if bits == 32 else pa.int64()
        chunks = column.chunks if isinstance(column, pa.ChunkedArray) else (column,)
        if minimum == 0.0:
            minimum = -0.0 if any(pc.index(c.view(int_type), -(1 << (bits - 1))).as_py() >= 0
                                  for c in chunks) else 0.0
        if maximum == 0.0:
            maximum = 0.0 if any(pc.index(c.view(int_type), 0).as_py() >= 0
                                 for c in chunks) else -0.0
    stats['min_values'], stats['max_values'] = minimum, maximum
    return stats


def build_stats(fields, values):
    if not fields:
        return SimpleStats.empty_stats()
    return SimpleStats(
        GenericRow([value[0] for value in values], fields),
        GenericRow([value[1] for value in values], fields),
        [value[2] for value in values],
    )


class ValueStatsCollector:
    """Resolve Java's value stats policy for the columns of one file."""

    def __init__(self, options, fields, manifest_fields, *, level=None, changelog=False,
                 table_mode=None, file_format=None):
        options = options.options
        raw_options = options.to_map()
        mode = table_mode if table_mode is not None else options.get(CoreOptions.METADATA_STATS_MODE)
        if level is not None:
            level_mode = _mode_at_level(raw_options, level)
            if level_mode is not None:
                mode = level_mode
        if changelog:
            mode = options.get(_CHANGELOG_MODE, mode)
        default_mode = _parse_mode(mode)
        keep_first = options.get(_KEEP_FIRST)
        dense = options.get(_DENSE_STORE)
        self.fields = []
        self.modes = {}
        for index, field in enumerate(fields):
            override = raw_options.get('fields.{}.stats-mode'.format(field.name))
            if override is not None:
                field_mode = _parse_mode(override)
            elif 0 <= keep_first <= index:
                field_mode = ('none', None)
            else:
                field_mode = default_mode
            if (file_format == CoreOptions.FILE_FORMAT_ROW and field_mode[0] in ('full', 'truncate')
                    and isinstance(field.type, AtomicType)
                    and field.type.type.upper().startswith('TIMESTAMP')
                    and PyarrowFieldParser.from_paimon_type(field.type).unit == 'ns'):
                # The Python ROW encoder stores timestamp values at microsecond precision.
                field_mode = ('counts', None)
            self.modes[field.name] = field_mode
            if field_mode[0] != 'none':
                self.fields.append(field)
        self.stored_fields = self.fields if dense else fields
        names = [field.name for field in self.stored_fields]
        self.columns = None if names == [field.name for field in manifest_fields] else names

    def needs_bounds(self):
        return any(mode in ('full', 'truncate') for mode, _ in self.modes.values())

    def collect_column(self, data, name, parquet_bounds=None):
        return column_stats(data, name, bounds=self.modes[name][0] != 'counts', parquet_bounds=parquet_bounds)

    def collect(self, data, known=None, parquet_metadata=None):
        known = known or {}
        parquet_bounds = (_ParquetBounds(parquet_metadata)
                          if parquet_metadata is not None and parquet_metadata.num_rows == data.num_rows else None)
        return self.finish({
            field.name: (known[field.name] if field.name in known else
                         self.collect_column(data, field.name, parquet_bounds))
            for field in self.fields
        })

    def finish(self, stats):
        values = []
        for field in self.stored_fields:
            mode, length = self.modes[field.name]
            if mode == 'none':
                values.append((None, None, None))
                continue
            column = stats[field.name]
            minimum, maximum = column['min_values'], column['max_values']
            if mode == 'counts' or column.get('bounds_unavailable'):
                minimum, maximum = None, None
            elif mode == 'truncate':
                minimum, maximum = _truncate_bounds(minimum, maximum, length)
            values.append((minimum, maximum, column['null_counts']))
        return build_stats(self.stored_fields, values)
