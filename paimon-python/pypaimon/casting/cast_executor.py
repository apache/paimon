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

"""The existing Python scalar CAST subset with Java's original executor."""

import re

import pyarrow as pa

from pypaimon.casting.cast_text import format_date, parse_date, parse_timestamp
from pypaimon.casting.floating_cast import parse_java_float
from pypaimon.casting.numeric_cast import cast_numeric, _numeric_to_integer
from pypaimon.casting.numeric_utils import java_number_string
from pypaimon.casting.schema_evolution import _timestamp_to_bigint
from pypaimon.casting.temporal_cast import cast_temporal
from pypaimon.casting.temporal_string import temporal_to_string, timestamp_object_to_string
from pypaimon.casting.temporal_utils import _signed
from pypaimon.table.row.generic_row import _parse_type_precision_scale


def _text(t):
    return pa.types.is_string(t) or pa.types.is_large_string(t)


def _binary(t):
    return pa.types.is_binary(t) or pa.types.is_large_binary(t)


def _primitive(t):
    return pa.types.is_signed_integer(t) or pa.types.is_floating(t)


def _numeric(t):
    return _primitive(t) or pa.types.is_decimal(t)


def _number(t):
    return _primitive(t) or pa.types.is_date(t) or pa.types.is_time(t)


def _integer_object(t):
    return t == pa.int32() or pa.types.is_date(t) or pa.types.is_time(t)


def supports_cast(source, target):
    if _numeric(source) and _numeric(target):
        return True
    if pa.types.is_boolean(source) and (_primitive(target) or _text(target)):
        return True
    if pa.types.is_signed_integer(source) and pa.types.is_boolean(target):
        return True
    if _text(target) and (_numeric(source) or _text(source) or _binary(source)
                          or pa.types.is_date(source) or pa.types.is_time(source) or pa.types.is_timestamp(source)):
        return True
    if _binary(target) and (_text(source) or _binary(source)):
        return True
    if _text(source) and (_numeric(target) or pa.types.is_boolean(target)
                          or pa.types.is_date(target) or pa.types.is_timestamp(target)):
        return True
    if (source == pa.int64() or pa.types.is_date(source)) and pa.types.is_timestamp(target):
        return True
    if pa.types.is_timestamp(source) and (pa.types.is_timestamp(target) or pa.types.is_date(target)
                                          or pa.types.is_time(target) or target == pa.int64()):
        return True
    return False


def _number_input(array):
    if pa.types.is_date(array.type):
        return array.cast(pa.date32(), safe=False).cast(pa.int32())
    if pa.types.is_time(array.type):
        return array.cast(pa.time32('ms'), safe=False).cast(pa.int32())
    return array


def _timestamp_parts(array):
    ticks = array.cast(pa.int64()).to_pylist()
    unit = array.type.unit
    return [None if value is None else
            (_signed(value * 1000, 64), 0) if unit == 's' else
            (value, 0) if unit == 'ms' else
            (value // 1000, value % 1000 * 1000) if unit == 'us' else
            (value // 1000000, value % 1000000) for value in ticks]


def _timestamp_storage(values, target):
    quantum = {'s': 1000000000, 'ms': 1000000, 'us': 1000, 'ns': 1}[target.unit]
    ticks = []
    for value in values:
        if value is None:
            ticks.append(None)
            continue
        millis, nanos = value
        full = millis * 1000000 + nanos
        if full % quantum or not -(1 << 63) <= full // quantum < (1 << 63):
            raise ValueError('Java timestamp result cannot be represented by the Arrow target')
        ticks.append(full // quantum)
    return pa.array(ticks, type=pa.int64()).cast(target)


def _fit_width(array, target):
    width = re.fullmatch(r'\s*(CHAR|VARCHAR|BINARY|VARBINARY)(?:\s*\(\s*([0-9]+)\s*\))?'
                         r'\s*(?:(?:NOT\s+)?NULL)?\s*', target, flags=re.IGNORECASE)
    if width is None:
        return array
    kind, length = width.group(1).upper(), int(width.group(2) or 1)
    if not 1 <= length <= (1 << 31) - 1:
        raise ValueError('Cast width must be between 1 and 2147483647')
    pad = kind in ('CHAR', 'BINARY')
    padding = ' ' if kind == 'CHAR' else b'\x00'
    return pa.array([None if value is None else
                     (value[:length] + padding * max(0, length - len(value)) if pad else value[:length])
                     for value in array.to_pylist()], type=array.type)


def cast_array(array, source_type, target_type):
    # Keep the transport parser independent of value execution.
    from pypaimon.common.predicate_json_parser import _paimon_type_to_arrow

    source, target = _paimon_type_to_arrow(source_type), _paimon_type_to_arrow(target_type)
    if pa.types.is_boolean(target) and (pa.types.is_floating(source) or pa.types.is_decimal(source)):
        raise ValueError("Java has no FLOAT/DOUBLE/DECIMAL to BOOLEAN cast")
    if not supports_cast(source, target):
        raise pa.ArrowNotImplementedError('CAST is outside the existing Python/Java scalar rules: %s -> %s'
                                          % (source_type, target_type))
    if array.null_count == len(array):
        return pa.nulls(len(array), type=target)
    current = array.type

    def require(valid):
        if not valid:
            raise ValueError('CAST source no longer has the runtime class its original Java executor expects')

    if _numeric(source) and _text(target):
        require(_numeric(current) or _text(current) or pa.types.is_boolean(current)
                or pa.types.is_date(current) or pa.types.is_time(current) or pa.types.is_timestamp(current))
        value = _number_input(array)
        if pa.types.is_timestamp(value.type):
            result = timestamp_object_to_string(value)
        elif pa.types.is_floating(value.type):
            result = pa.array([None if v is None else java_number_string(v, value.type == pa.float32())
                               for v in value.to_pylist()], type=target)
        elif pa.types.is_decimal(value.type):
            result = pa.array([None if v is None else format(v, 'f') for v in value.to_pylist()], type=target)
        else:
            result = value.cast(target, safe=False)
    elif _numeric(source) and _numeric(target):
        require(pa.types.is_decimal(current) if pa.types.is_decimal(source) else _number(current))
        value = _number_input(array)
        if pa.types.is_decimal(target) and _primitive(source):
            value = (_numeric_to_integer(value, pa.int64()) if pa.types.is_signed_integer(source)
                     else value.cast(pa.float64(), safe=False))
        result = cast_numeric(value, target)
    elif pa.types.is_signed_integer(source) and pa.types.is_boolean(target):
        require(_number(current))
        result = _numeric_to_integer(_number_input(array), pa.int64()).cast(target, safe=False)
    elif pa.types.is_boolean(source):
        require(pa.types.is_boolean(current))
        result = array.cast(target, safe=False)
    elif _text(source):
        require(_text(current))
        if pa.types.is_floating(target):
            result = pa.array([None if v is None else parse_java_float(v, target == pa.float32())
                               for v in array.to_pylist()], type=target)
        elif pa.types.is_date(target):
            result = pa.array([None if v is None else parse_date(v) for v in array.to_pylist()],
                              type=pa.int32()).cast(target)
        elif pa.types.is_timestamp(target):
            precision = _parse_type_precision_scale(target_type)[0]
            values = [None if v is None else parse_timestamp(v, precision, target.tz is not None)
                      for v in array.to_pylist()]
            result = _timestamp_storage(values, target)
        else:
            result = cast_numeric(array, target)
            if result is None:
                result = array.cast(target, safe=False)
    elif _binary(source):
        require(_binary(current))
        if _text(target):
            # Arrow may relabel binary buffers as UTF8 without validating them.
            # Java BinaryString preserves these bytes; an Arrow string cannot.
            for value in array.to_pylist():
                if value is not None:
                    value.decode("utf8")
        result = array.cast(target, safe=False)
        if _text(target):
            return result  # BinaryToString does not trim/pad character width.
    elif pa.types.is_date(source):
        if pa.types.is_timestamp(target):
            require(_number(current))
            values = _number_input(array)
            if target.tz is not None:
                integers = _numeric_to_integer(values, pa.int32()).cast(pa.date32())
                result = cast_temporal(integers, target, source_type, target_type, strict=True)
            else:
                values = _numeric_to_integer(values, pa.int64()).to_pylist()
                result = _timestamp_storage(
                    [None if v is None else (_signed(v * 86400000, 64), 0) for v in values], target)
        else:
            require(_integer_object(current))
            value = _number_input(array).cast(pa.int32())
            result = (pa.array([None if v is None else format_date(v) for v in value.to_pylist()], type=target)
                      if _text(target) else value.cast(target))
    elif pa.types.is_time(source):
        require(_integer_object(current))
        value = _number_input(array).cast(pa.int32()).cast(pa.time32('ms'))
        result = (temporal_to_string(value, source_type, 'STRING') if _text(target) else value.cast(target))
    elif pa.types.is_timestamp(source):
        require(pa.types.is_timestamp(current))
        if _text(target):
            result = temporal_to_string(array, source_type, 'STRING')
        elif target == pa.int64():
            # The original timestamp kind selects the executor even after rebind.
            value = array.cast(pa.timestamp(array.type.unit, source.tz))
            result = _timestamp_to_bigint(value)
        else:
            result = cast_temporal(array, target, source_type, target_type, strict=True)
            if result is None:
                precision = _parse_type_precision_scale(target_type)[0]
                truncate = _parse_type_precision_scale(source_type)[0] > precision
                values = []
                for value in _timestamp_parts(array):
                    if value is None:
                        values.append(None)
                        continue
                    millis, nanos = value
                    if truncate:
                        if precision <= 3:
                            quantum = 10 ** (3 - precision)
                            millis, nanos = _signed(millis // quantum * quantum, 64), 0
                        else:
                            quantum = 10 ** (9 - precision)
                            nanos = nanos // quantum * quantum
                    values.append((millis, nanos))
                result = _timestamp_storage(values, target)
    else:  # BIGINT -> timestamp uses Number.longValue in epoch seconds.
        require(_number(current))
        value = _numeric_to_integer(_number_input(array), pa.int64())
        if target.tz is not None:
            result = cast_temporal(value, target, source_type, target_type, strict=True)
        else:
            values = [None if v is None else (_signed(v * 1000, 64), 0) for v in value.to_pylist()]
            result = _timestamp_storage(values, target)
    if pa.types.is_timestamp(target) and result.null_count > array.null_count:
        raise ValueError('Java timestamp result cannot be represented by the Arrow target')
    return _fit_width(result, target_type)
