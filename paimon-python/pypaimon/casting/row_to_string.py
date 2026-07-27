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

Only the types whose output provably matches Java are rendered. A row holding
one of the types left out is not rendered at all, so the caller keeps the NULL
it would have emitted anyway:

- FLOAT and DOUBLE go through ``Float/Double.toString``, which up to JDK 18 is
  the legacy ``FloatingDecimal`` and does not produce the shortest round
  tripping decimal, while JDK 19+ does: ``2.68873286E11`` against
  ``2.6887329E11`` for the same float.
- TIMESTAMP WITH LOCAL TIME ZONE is formatted in ``TimeZone.getDefault()``, so
  one manifest reads ``2024-01-02 03:04:05`` on a UTC JVM and
  ``2024-01-02 06:04:05`` on a Europe/Moscow one.
- BINARY, VARBINARY and BYTES are decoded as UTF-8, and malformed input leaves
  the JDK decoder and Python disagreeing on how many replacement characters to
  emit: ``ED A0 80`` is one in Java and three here.
"""

from typing import Any, Optional

from pypaimon.table.row.generic_row import _parse_type_precision_scale

_UNSUPPORTED_TYPES = ("FLOAT", "REAL", "DOUBLE", "TIMESTAMP_LTZ",
                      "BINARY", "VARBINARY", "BYTES")


def cast_row_to_string(row) -> Optional[str]:
    """Render ``row`` as ``{v1, v2}``, with ``null`` for null fields.

    An empty row renders as ``{}``, which is what an unpartitioned table has.
    Returns None when a field has a type this module does not render.
    """
    if row is None:
        return None
    fields = getattr(row, "fields", None) or []
    # on the declared type, not on the value, so every manifest of a table
    # answers the same way no matter which stats happen to be null
    if any(_is_unsupported(field.type) for field in fields):
        return None
    parts = []
    for i in range(len(fields)):
        value = row.get_field(i)
        parts.append("null" if value is None
                     else cast_value_to_string(value, fields[i].type))
    return "{" + ", ".join(parts) + "}"


def cast_value_to_string(value: Any, data_type) -> str:
    """Cast a single field value to string the way the Java cast rules do."""
    type_name = _type_name(data_type)

    if _is_unsupported(data_type):
        raise ValueError(
            "{} has no portable string form, see the module docstring".format(
                type_name))
    if type_name in ("BOOLEAN", "BOOL"):
        return "true" if value else "false"
    if type_name.startswith("DECIMAL") or type_name.startswith("NUMERIC"):
        # Java Decimal.toString is BigDecimal.toPlainString, which never uses
        # scientific notation, while str(Decimal) does below 1e-6
        return "{:f}".format(value)
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


def _is_unsupported(data_type) -> bool:
    type_name = _type_name(data_type)
    # the other spelling of TIMESTAMP_LTZ, see data_types.py
    if "WITH LOCAL TIME ZONE" in type_name:
        return True
    # first token only, so "FLOAT NOT NULL" is caught as well
    head = type_name.split("(", 1)[0].split()
    return bool(head) and head[0] in _UNSUPPORTED_TYPES


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

    Digit by digit off the millisecond part, exactly as
    ``DateTimeUtils.formatTimestampMillis`` does: it stops only once nothing
    but zeros is left, so a truncated non zero tail keeps the zero before it
    (``.10`` for 101 ms at precision 2, not ``.1``).
    """
    text = "{:02d}:{:02d}:{:02d}".format(value.hour, value.minute, value.second)
    if precision <= 0:
        return text
    millis = value.microsecond // 1000
    digits = []
    while precision > 0:
        digits.append(str(millis // 100))
        millis = millis % 100 * 10
        if millis == 0:
            break
        precision -= 1
    return text + "." + "".join(digits)
