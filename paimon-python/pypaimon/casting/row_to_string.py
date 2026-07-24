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

import struct
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
        return _format_float(value)
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


def _format_float(value) -> str:
    """Format like ``Float.toString``: shortest text that round trips as float32.

    ``repr`` would print the float64 the reader widened the value to, turning
    ``0.1f`` into ``0.10000000149011612``.
    """
    try:
        packed = struct.pack("<f", value)
    except (OverflowError, struct.error):
        return repr(value)
    text = repr(value)
    for digits in range(1, 10):
        candidate = "%.{}g".format(digits) % value
        if struct.pack("<f", float(candidate)) == packed:
            text = candidate
            break
    # Java always prints a decimal point for a finite float
    if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
        return text + ".0"
    return text
