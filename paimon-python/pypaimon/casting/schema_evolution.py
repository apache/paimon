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

from decimal import Decimal, localcontext, ROUND_HALF_UP

import pyarrow as pa

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
    values = [None if value is None else
              (value * nanos_per_tick[array.type.unit] // quantum * quantum) // nanos_per_tick[target_type.unit]
              for value in array.cast(pa.int64()).to_pylist()]
    # Flooring a nanosecond boundary can exceed Arrow's int64 storage even
    # though Java's timestamp representation can hold it. Match Native's
    # lenient overflow handling when rebuilding the Arrow result.
    values = [value if value is None or -(1 << 63) <= value < (1 << 63) else None for value in values]
    return pa.array(values, type=pa.int64()).cast(target_type)


def cast_array_for_schema_evolution(array, target_type, source_paimon_type=None, target_paimon_type=None):
    """Materialize existing type changes using Java numeric and timestamp casts.

    Declared Paimon types are needed for precision changes within one Arrow
    timestamp unit. Numeric narrowing preserves low bits, while floating-point
    values truncate and saturate to int/long before byte/short narrowing.
    DECIMAL scale reduction uses HALF_UP and returns NULL on precision overflow.
    """
    timestamp = _truncate_timestamp(array, target_type, source_paimon_type, target_paimon_type)
    if timestamp is not None:
        return timestamp
    if array.type == target_type:
        return array
    if (pa.types.is_signed_integer(target_type)
            and (pa.types.is_floating(array.type) or pa.types.is_decimal(array.type))):
        return _numeric_to_integer(array, target_type)
    if (pa.types.is_decimal(array.type) and pa.types.is_decimal(target_type)
            and array.type.scale > target_type.scale):
        values = []
        with localcontext() as context:
            context.prec = max(array.type.precision, target_type.precision) + 1
            quantum = Decimal(1).scaleb(-target_type.scale)
            for value in array.to_pylist():
                if value is None:
                    values.append(None)
                    continue
                rounded = value.quantize(quantum, rounding=ROUND_HALF_UP)
                values.append(rounded if len(rounded.as_tuple().digits) <= target_type.precision else None)
        return pa.array(values, type=target_type)
    return array.cast(target_type, safe=False)
