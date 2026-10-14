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

import math

import pyarrow as pa

from pypaimon.execution.python.casting.decimal_cast import cast_decimal
from pypaimon.execution.python.casting.temporal_cast import cast_temporal
from pypaimon.execution.python.casting.temporal_string import temporal_to_string
from pypaimon.table.row.generic_row import _parse_type_precision_scale


def _narrow_integer(value, bits):
    half = 1 << (bits - 1)
    return (value + half) % (half * 2) - half


def _numeric_to_integer(array, target_type):
    bits = target_type.bit_width
    floating = pa.types.is_floating(array.type)
    intermediate_bits = 64 if bits == 64 else 32
    lower = -(1 << (intermediate_bits - 1))
    upper = (1 << (intermediate_bits - 1)) - 1
    values = []
    for value in array.to_pylist():
        if value is None:
            values.append(None)
            continue
        # Number.byteValue/shortValue first convert floats to int; Java's
        # float-to-int/long conversion saturates before the final narrowing.
        if floating:
            # Java's floating-point narrowing maps NaN to zero.
            if math.isnan(value):
                value = 0
            else:
                value = lower if value <= lower else upper if value >= upper else int(value)
        else:
            # BigDecimal.longValue truncates towards zero and retains low bits.
            value = int(value)
        values.append(_narrow_integer(value, bits))
    return pa.array(values, type=target_type)


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
    values = []
    for value in array.cast(pa.int64()).to_pylist():
        if value is None:
            values.append(None)
            continue
        nanos = value * nanos_per_tick[array.type.unit] // quantum * quantum
        millis, remainder = divmod(nanos, 1000000)
        # Java stores signed-long milliseconds before converting storage units.
        millis = _narrow_integer(millis, 64)
        values.append((millis * 1000000 + remainder) // nanos_per_tick[target_type.unit])
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
    decimals = cast_decimal(array, target_type)
    if decimals is not None:
        return decimals
    if pa.types.is_timestamp(array.type) and target_type == pa.int64():
        return _timestamp_to_bigint(array)
    if (array.type in (pa.int32(), pa.int64())
            and pa.types.is_timestamp(target_type) and target_type.tz is None):
        return _integer_to_timestamp(array, target_type)
    if (pa.types.is_signed_integer(target_type)
            and (pa.types.is_floating(array.type) or pa.types.is_decimal(array.type))):
        return _numeric_to_integer(array, target_type)
    try:
        return array.cast(target_type, safe=False)
    except pa.ArrowNotImplementedError:
        # PyArrow 6 cannot cast a map or list as a whole when only the child
        # binary type changes. Child arrays themselves still cast.
        rebuilt = _cast_nested_container(array, target_type)
        if rebuilt is None:
            raise
        return rebuilt


def _cast_child(array, target_type):
    if array.type == target_type:
        return array
    try:
        return array.cast(target_type, safe=False)
    except pa.ArrowNotImplementedError:
        return _cast_nested_container(array, target_type)


def _cast_nested_container(array, target_type):
    """Rebuild a list or map after casting its children."""
    if isinstance(array, pa.ChunkedArray):
        chunks = [_cast_nested_container(chunk, target_type) for chunk in array.chunks]
        if any(chunk is None for chunk in chunks):
            return None
        return pa.chunked_array(chunks, type=target_type)
    if pa.types.is_list(array.type) and pa.types.is_list(target_type):
        values = _cast_child(array.values, target_type.value_type)
        if values is None:
            return None
        return pa.ListArray.from_arrays(_offsets_with_nulls(array), values)
    if pa.types.is_large_list(array.type) and pa.types.is_large_list(target_type):
        values = _cast_child(array.values, target_type.value_type)
        if values is None:
            return None
        return pa.LargeListArray.from_arrays(_offsets_with_nulls(array), values)
    if pa.types.is_map(array.type) and pa.types.is_map(target_type):
        keys = _cast_child(array.keys, target_type.key_type)
        items = _cast_child(array.items, target_type.item_type)
        if keys is None or items is None:
            return None
        # PyArrow 6 MapArray.from_arrays has no mask argument. A null offset
        # entry is how that version represents a null map.
        return pa.MapArray.from_arrays(_offsets_with_nulls(array), keys, items)
    return None


def _value_offsets(array):
    """Offsets for a list or map, including a sliced array.

    PyArrow 6's ``MapArray`` has no ``offsets`` attribute. The offset buffer is
    still ``buffers()[1]``, and a slice starts at ``array.offset``. Copy that
    range into a new array: ``from_arrays`` rejects a sliced offset buffer when
    the map also has a null bitmap. The values still index the child arrays.
    """
    return _offsets_from_buffer(array)


def _offsets_from_buffer(array):
    offset_type = pa.int64() if pa.types.is_large_list(array.type) else pa.int32()
    width = 8 if offset_type == pa.int64() else 4
    start = array.offset
    count = len(array) + 1
    raw = array.buffers()[1]
    sliced = None if raw is None else raw.slice(start * width, count * width)
    return pa.Array.from_buffers(offset_type, count, [None, sliced])


def _offsets_with_nulls(array):
    offsets = _value_offsets(array)
    if array.null_count == 0:
        return offsets
    # Keep the numeric offset under each null so neighboring rows still slice
    # the child arrays at the right index. PyArrow 6 reads that null bit as a
    # null list or map.
    nulls = array.is_null().to_pylist()
    nulls.append(False)
    return pa.array(offsets.to_pylist(), type=offsets.type, mask=pa.array(nulls))
