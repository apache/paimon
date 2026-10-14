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

"""Java cast rules shared by old-file readers."""

import pyarrow as pa

from pypaimon.casting.numeric_cast import _narrow_integer, cast_numeric
from pypaimon.casting.temporal_cast import cast_temporal
from pypaimon.casting.temporal_string import temporal_to_string
from pypaimon.table.row.generic_row import _parse_type_precision_scale


def _truncate_timestamp(array, target_type, source_paimon_type, target_paimon_type):
    if (not pa.types.is_timestamp(array.type) or not pa.types.is_timestamp(target_type)
            or array.type.tz != target_type.tz
            or source_paimon_type is None or target_paimon_type is None):
        return None
    source_precision = _parse_type_precision_scale(source_paimon_type)[0]
    target_precision = _parse_type_precision_scale(target_paimon_type)[0]
    if source_precision <= target_precision:
        return None
    nanos_per_tick = {'s': 1000000000, 'ms': 1000000, 'us': 1000, 'ns': 1}
    quantum = 10 ** (9 - target_precision)
    values = [None if value is None else
              (value * nanos_per_tick[array.type.unit] // quantum * quantum) // nanos_per_tick[target_type.unit]
              for value in array.cast(pa.int64()).to_pylist()]
    # Flooring a nanosecond boundary can exceed Arrow's int64 storage even
    # though Java's timestamp representation can hold it. Match Native's
    # lenient overflow handling when rebuilding the Arrow result.
    values = [value if value is None or -(1 << 63) <= value < (1 << 63) else None for value in values]
    return pa.array(values, type=pa.int64()).cast(target_type)


def _timestamp_to_bigint(array):
    ticks = {'s': 1, 'ms': 1000, 'us': 1000000, 'ns': 1000000000}[array.type.unit]
    values = []
    for value in array.cast(pa.int64()).to_pylist():
        if value is None:
            values.append(None)
        elif array.type.tz is None:
            values.append(value // ticks)
        else:
            # Java's LTZ rule round-trips Timestamp through LocalDateTime.
            # Timestamp stores its epoch day as an int in that conversion.
            millis = value * 1000 if array.type.unit == 's' else value // (ticks // 1000)
            if not -(1 << 63) <= millis < (1 << 63):
                values.append(None)
                continue
            day = _narrow_integer(millis // 86400000, 32)
            values.append((day * 86400000 + millis % 86400000) // 1000)
    return pa.array(values, type=pa.int64())


def _integer_to_timestamp(array, target_type):
    ticks = {'s': 1, 'ms': 1000, 'us': 1000000, 'ns': 1000000000}[target_type.unit]
    values = []
    for value in array.to_pylist():
        if value is None:
            values.append(None)
            continue
        # NumericPrimitiveToTimestamp performs value.longValue() * 1000
        # before conversion. Java's signed long multiplication wraps.
        millis = _narrow_integer(value * 1000, 64)
        value = millis // 1000 if target_type.unit == 's' else millis * (ticks // 1000)
        values.append(value if -(1 << 63) <= value < (1 << 63) else None)
    return pa.array(values, type=pa.int64()).cast(target_type)


def cast_array_for_schema_evolution(array, target_type, source_paimon_type=None, target_paimon_type=None):
    """Materialize existing type changes using Java numeric and timestamp casts.

    Declared Paimon types are needed for precision changes within one Arrow
    timestamp unit. INTEGER/BIGINT to TIMESTAMP and TIMESTAMP to BIGINT use
    epoch seconds, including pre-epoch flooring and Java's millis multiplication.
    Numeric narrowing preserves low bits, while floating-point values truncate
    and saturate to int/long before byte/short narrowing.
    DECIMAL conversion uses Java decimal text and HALF_UP, returning NULL on
    precision overflow after rounding.
    LTZ conversions use the process local zone, including Java gap/overlap rules.
    """
    if pa.types.is_string(target_type):
        strings = temporal_to_string(array, source_paimon_type, target_paimon_type)
        if strings is not None:
            return strings
    converted = cast_temporal(array, target_type, source_paimon_type, target_paimon_type)
    if converted is not None:
        return converted
    timestamp = _truncate_timestamp(array, target_type, source_paimon_type, target_paimon_type)
    if timestamp is not None:
        return timestamp
    if array.type == target_type:
        return array
    if pa.types.is_signed_integer(array.type) and pa.types.is_decimal(target_type):
        required = {8: 3, 16: 5, 32: 10, 64: 19}[array.type.bit_width] + target_type.scale
        if target_type.precision < required:
            # Preserve PyPaimon's existing ALTER TABLE capability boundary.
            # Authorization CASTs separately use Java's per-value NULL overflow.
            return array.cast(target_type, safe=False)
    numeric = cast_numeric(array, target_type, target_paimon_type)
    if numeric is not None:
        return numeric
    if pa.types.is_timestamp(array.type) and target_type == pa.int64():
        return _timestamp_to_bigint(array)
    if (array.type in (pa.int32(), pa.int64())
            and pa.types.is_timestamp(target_type) and target_type.tz is None):
        return _integer_to_timestamp(array, target_type)
    return array.cast(target_type, safe=False)
