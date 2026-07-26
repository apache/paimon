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

"""Render a row as the string Java produces when casting it to STRING.

Mirrors ``RowToStringCastRule`` and the per type ``*ToStringCastRule`` rules of
``paimon-common``, so system tables show the same text in both languages.
"""

import math
import struct
from decimal import Decimal
from typing import Any, Optional

from pypaimon.table.row.generic_row import _parse_type_precision_scale


def cast_row_to_string(row) -> Optional[str]:
    """Render ``row`` as ``{v1, v2}``, with ``null`` for null fields.

    An empty row renders as ``{}``, which is what an unpartitioned table has.
    """
    if row is None:
        return None
    fields = getattr(row, "fields", None) or []
    parts = []
    for i in range(len(fields)):
        value = row.get_field(i)
        parts.append("null" if value is None
                     else cast_value_to_string(value, fields[i].type))
    return "{" + ", ".join(parts) + "}"


def cast_value_to_string(value: Any, data_type) -> str:
    """Cast a single field value to string the way the Java cast rules do."""
    type_name = _type_name(data_type)

    if type_name in ("BOOLEAN", "BOOL"):
        return "true" if value else "false"
    if type_name in ("FLOAT", "REAL"):
        return _format_java_float(value, True)
    if type_name == "DOUBLE":
        return _format_java_float(value, False)
    if (type_name.startswith("BINARY") or type_name.startswith("VARBINARY")
            or type_name == "BYTES"):
        return bytes(value).decode("utf-8", "replace")
    if type_name == "DATE":
        return value.isoformat()
    # TIMESTAMP has to be tested before TIME, it starts with it
    if type_name.startswith("TIMESTAMP"):
        precision, _ = _parse_type_precision_scale(data_type)
        return _format_timestamp(value, precision)
    if type_name.startswith("TIME"):
        precision, _ = _parse_type_precision_scale(data_type)
        return _format_time(value, precision)
    return str(value)


def _type_name(data_type) -> str:
    name = getattr(data_type, "type", None)
    return (name if isinstance(name, str) else str(data_type)).upper().strip()


def _format_timestamp(value, precision: int) -> str:
    """Format as ``yyyy-MM-dd HH:mm:ss[.fraction]``.

    The fraction is padded to nine digits and then stripped of trailing zeros
    down to ``precision`` digits, as ``DateTimeUtils.formatTimestamp`` does.
    Python datetimes are microsecond resolution, so the last three digits of a
    TIMESTAMP(7..9) value are always zero.
    """
    text = "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}".format(
        value.year, value.month, value.day,
        value.hour, value.minute, value.second)
    fraction = "{:09d}".format(value.microsecond * 1000)
    while len(fraction) > precision and fraction.endswith("0"):
        fraction = fraction[:-1]
    return (text + "." + fraction) if fraction else text


def _format_time(value, precision: int) -> str:
    """Format as ``HH:mm:ss[.fraction]``.

    ``DateTimeUtils.formatTimestampMillis`` emits at most ``precision`` digits
    of the millisecond part and stops early once nothing but zeros is left, but
    always keeps one digit when precision allows any.
    """
    text = "{:02d}:{:02d}:{:02d}".format(value.hour, value.minute, value.second)
    if precision <= 0:
        return text
    digits = "{:03d}".format(value.microsecond // 1000)[:min(precision, 3)]
    return text + "." + (digits.rstrip("0") or digits[:1])


def _format_java_float(value, is_float32: bool) -> str:
    """Format like Java ``Float.toString`` / ``Double.toString``.

    Finds the shortest decimal that round trips, then lays it out with Java's
    rules: plain decimal when ``1 <= D <= 7`` or ``-2 <= D <= 0`` (writing
    ``value = 0.<digits> x 10^D``), otherwise ``d.dddEexp`` with an upper case
    ``E``, a sign only when negative, and no zero padded exponent. The shortest
    form differs from a pre JDK 19 JVM only at the denormal extremes
    (``1.0E-45`` vs ``1.4E-45``), which partition stats never hold.
    """
    if value != value:
        return "NaN"
    if value == math.inf:
        return "Infinity"
    if value == -math.inf:
        return "-Infinity"
    sign = "-" if math.copysign(1.0, value) < 0 else ""
    if value == 0.0:
        return sign + "0.0"
    digits, decimal_exp = _shortest_digits(abs(value), is_float32)
    n = len(digits)
    if 0 < decimal_exp <= 7:
        if decimal_exp >= n:
            body = digits + "0" * (decimal_exp - n) + ".0"
        else:
            body = digits[:decimal_exp] + "." + digits[decimal_exp:]
    elif -3 < decimal_exp <= 0:
        body = "0." + "0" * (-decimal_exp) + digits
    else:
        body = digits[0] + "." + (digits[1:] or "0") + "E" + str(decimal_exp - 1)
    return sign + body


def _shortest_digits(magnitude, is_float32: bool):
    """Return ``(digits, D)`` with ``magnitude = 0.<digits> x 10^D``.

    ``digits`` carries no leading or trailing zeros. ``repr`` already yields the
    shortest float64 form; for float32 the shortest ``%g`` that round trips
    through four bytes is used, so a widened ``0.1f`` renders as ``0.1``.
    """
    if is_float32:
        packed = struct.pack("<f", magnitude)
        text = repr(magnitude)
        for precision in range(1, 10):
            candidate = "%.{}g".format(precision) % magnitude
            if struct.pack("<f", float(candidate)) == packed:
                text = candidate
                break
    else:
        text = repr(magnitude)
    _, digit_tuple, exp = Decimal(text).as_tuple()
    digits = list(digit_tuple)
    while len(digits) > 1 and digits[-1] == 0:
        digits.pop()
        exp += 1
    digit_str = "".join(map(str, digits))
    return digit_str, exp + len(digit_str)
