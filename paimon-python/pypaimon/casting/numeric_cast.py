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

"""Existing Java numeric and Boolean CAST rules shared by readers and auth."""

import math
import re

import pyarrow as pa

from pypaimon.casting.decimal_cast import cast_decimal


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


def _parse_integer(text, bits):
    if not text:
        raise ValueError('Empty integer value')
    negative = text[0] == '-'
    index = int(text[0] in ('+', '-'))
    if index == len(text):
        raise ValueError('Invalid integer value: %s' % text)
    limit = 1 << (bits - 1)
    if not negative:
        limit -= 1
    value = 0
    while index < len(text) and text[index] != '.':
        digit = text[index]
        if digit < '0' or digit > '9':
            raise ValueError('Invalid integer value: %s' % text)
        value = value * 10 + ord(digit) - ord('0')
        if value > limit:
            raise ValueError('Integer value overflow: %s' % text)
        index += 1
    if index < len(text) and any(digit < '0' or digit > '9' for digit in text[index + 1:]):
        raise ValueError('Invalid integer value: %s' % text)
    return -value if negative else value


def _parse_boolean(text):
    lowered = text.lower()
    if lowered in ('t', 'true', 'y', 'yes', '1'):
        return True
    if lowered in ('f', 'false', 'n', 'no', '0'):
        return False
    raise ValueError("Cannot parse '%s' as BOOLEAN" % text)


def cast_numeric(array, target, target_paimon_type=None):
    """Return None outside the existing numeric/Boolean CAST subset.

    Invalid text is an error, decimal precision overflow is NULL, and numeric
    narrowing follows Number/BigDecimal conversion rather than Arrow storage.
    The caller retains the original Java executor when rebinding field refs.
    """
    source = array.type
    text = pa.types.is_string(source) or pa.types.is_large_string(source)
    integer = pa.types.is_signed_integer(source)
    numeric = integer or pa.types.is_floating(source) or pa.types.is_decimal(source)
    numeric_target = pa.types.is_signed_integer(target) or pa.types.is_floating(target) or pa.types.is_decimal(target)
    if pa.types.is_boolean(target) and text:
        return pa.array([None if value is None else _parse_boolean(value) for value in array.to_pylist()],
                        type=target)
    if pa.types.is_boolean(target) and numeric:
        if not integer:
            raise ValueError('Java has no FLOAT/DOUBLE/DECIMAL to BOOLEAN cast')
        return pa.array([None if value is None else value != 0 for value in array.to_pylist()], type=target)
    if pa.types.is_signed_integer(target) and text:
        return pa.array([None if value is None else _parse_integer(value, target.bit_width)
                         for value in array.to_pylist()], type=target)
    if pa.types.is_decimal(source) and (pa.types.is_string(target) or pa.types.is_large_string(target)):
        values = [None if value is None else format(value, 'f') for value in array.to_pylist()]
        # Arrow carries no declared CHAR/VARCHAR length. Authorization callers
        # apply widths themselves; old-file reads pass the logical target here.
        width = re.fullmatch(r'(CHAR|VARCHAR)(?:\(\s*([0-9]+)\s*\))?(?:\s+(?:NOT\s+)?NULL)?',
                             str(target_paimon_type).strip().upper())
        if width is not None:
            kind, length = width.group(1), int(width.group(2) or 1)
            values = [None if value is None else (value[:length].ljust(length) if kind == 'CHAR' else value[:length])
                      for value in values]
        return pa.array(values, type=target)
    if pa.types.is_decimal(target) and (numeric or text):
        return cast_decimal(array, target)
    if pa.types.is_boolean(source) and pa.types.is_decimal(target):
        # PyPaimon does not support this pair; keep the existing capability
        # boundary instead of introducing another Java CAST category here.
        return None
    if numeric_target and (numeric or pa.types.is_boolean(source)):
        if pa.types.is_signed_integer(target) and numeric:
            return _numeric_to_integer(array, target)
        if pa.types.is_decimal(source):
            # DecimalToNumericPrimitive first obtains a double, then casts to
            # FLOAT if requested; direct decimal-to-float can double-round differently.
            def decimal_double(value):
                if value is None:
                    return None
                if source.precision > 18:
                    return float(value)
                digits = value.as_tuple()
                unscaled = 0
                for digit in digits.digits:
                    unscaled = unscaled * 10 + digit
                if digits.sign:
                    unscaled = -unscaled
                return float(unscaled) / (10 ** source.scale)

            return pa.array([decimal_double(value) for value in array.to_pylist()], type=target)
        return array.cast(target, safe=False)
    return None
