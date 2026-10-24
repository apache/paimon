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

"""Validate a scalar column default before it is stored.

The stored value is still the original string. Scalar checks follow the Java
string casts used by ``DefaultValueUtils`` for booleans, numbers, dates, times
and timestamps. ARRAY, MAP and ROW defaults are accepted by Java's
``StringToArrayCastRule``, ``StringToMapCastRule`` and ``StringToRowCastRule``;
Python does not parse those forms yet and rejects them explicitly.
"""

import re
from calendar import monthrange
from datetime import time
from typing import Optional

from pypaimon.schema.data_types import AtomicType

_TRUE = {"t", "true", "y", "yes", "1"}
_FALSE = {"f", "false", "n", "no", "0"}
_INT_MIN = -2147483648
_INT_MAX = 2147483647
_INT_RANGES = {
    "TINYINT": (-128, 127),
    "SMALLINT": (-32768, 32767),
    "INT": (_INT_MIN, _INT_MAX),
    "INTEGER": (_INT_MIN, _INT_MAX),
    "BIGINT": (-9223372036854775808, 9223372036854775807),
}
_INTEGRAL = set(_INT_RANGES)
_APPROXIMATE = {"FLOAT", "DOUBLE"}
_CHARACTER = {"STRING", "CHAR", "VARCHAR"}
_BINARY = {"BINARY", "VARBINARY", "BYTES"}
_UNSIGNED_INTEGER_RE = re.compile(r"[0-9]+")
# StringUtils.isNumeric guards the epoch fast path of DATE, TIME and TIMESTAMP.
# It allows a leading '-' but not '+', and only ASCII digits after it.
_JAVA_NUMERIC_RE = re.compile(r"-?[0-9]+")
_INTEGRAL_RE = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]*)")
# BigDecimal(String) reads digits with Character.digit, so Unicode digits are
# valid here, unlike the other numeric casts.
_DECIMAL_RE = re.compile(
    r"[+-]?(?:(\d+)(?:\.(\d*))?|\.(\d+))(?:[eE]([+-]?\d+))?")
_DECIMAL_SCALE_RE = re.compile(r"\(\s*\d+\s*(?:,\s*(\d+)\s*)?\)")
# Decimal.fromBigDecimal rescales with BigDecimal.setScale, which needs
# BigInteger.TEN.pow(raise). pow rejects any raise where
# bitLength(10) * raise / 32 exceeds BigInteger.MAX_MAG_LENGTH (2^26 + 1).
_MAX_TEN_POWER = 8 * ((1 << 26) + 1)
# Float.parseFloat / Double.parseDouble accept hexadecimal literals and type
# suffixes, but not Python's underscores or case-insensitive special values.
_FLOAT_RE = re.compile(
    r"[+-]?(?:NaN|Infinity|"
    r"(?:(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?|"
    r"0[xX](?:[0-9a-fA-F]+(?:\.[0-9a-fA-F]*)?|\.[0-9a-fA-F]+)"
    r"[pP][+-]?[0-9]+)[fFdD]?)")
_JAVA_TRIM_CHARS = "".join(chr(i) for i in range(33))
# DateTimeUtils.DEFAULT_TIMESTAMP_FORMATTER: the time is optional, a space
# requires hour:minute:second, and 'T' requires hour:minute with optional
# seconds. appendFraction has a minimum width of 0, so a bare '.' is allowed.
# 'yyyy' is SignStyle.EXCEEDS_PAD: exactly four digits, or '+' and five to
# nineteen digits. The formatter only accepts ASCII digits, so use [0-9], not
# \d. Anchor with \Z, not $, so a trailing newline does not slip through.
_TIMESTAMP_DATE = r"^(\+[0-9]{5,19}|[0-9]{4})-([0-9]{1,2})-([0-9]{1,2})"
_TIMESTAMP_SPACE_RE = re.compile(
    _TIMESTAMP_DATE
    + r"(?: ([0-9]{1,2}):([0-9]{1,2}):([0-9]{1,2})(?:\.([0-9]{0,9}))?)?\Z")
_TIMESTAMP_T_RE = re.compile(
    _TIMESTAMP_DATE
    + r"T([0-9]{1,2}):([0-9]{1,2})(?::([0-9]{1,2})(?:\.([0-9]{0,9}))?)?\Z")
_MAX_YEAR = 999999999


def validate_default_value(data_type, default_value: Optional[str]) -> None:
    """Raise if ``default_value`` cannot be cast to ``data_type``.

    ``None`` clears the default and is always accepted.
    """
    if default_value is None:
        return
    if not isinstance(data_type, AtomicType):
        raise RuntimeError(
            "Unsupported default value `{}` for type {}. Python currently "
            "validates scalar defaults only; ARRAY, MAP and ROW defaults "
            "are not supported.".format(default_value, data_type))
    try:
        _convert(data_type, _unquote(default_value))
    except Exception as cause:
        raise RuntimeError(
            "Unsupported default value `{}` for type {}".format(
                default_value, data_type)
        ) from cause


def _unquote(value: str) -> str:
    if len(value) >= 2 and value.startswith("'") and value.endswith("'"):
        return value[1:-1]
    return value


def _convert(data_type, text: str) -> None:
    root = _type_root(data_type.type)
    if root in _CHARACTER or root in _BINARY:
        return
    if root == "BOOLEAN" or root == "BOOL":
        if text.lower() not in _TRUE and text.lower() not in _FALSE:
            raise ValueError("Cannot parse {!r} as BOOLEAN.".format(text))
        return
    if root in _INTEGRAL:
        _parse_integral(text, root)
        return
    if root in _APPROXIMATE:
        if _FLOAT_RE.fullmatch(text.strip(_JAVA_TRIM_CHARS)) is None:
            raise ValueError("Invalid floating-point value {}".format(text))
        return
    if root == "DECIMAL" or root == "NUMERIC":
        _parse_decimal(text, _decimal_scale(data_type.type))
        return
    if root == "DATE":
        _parse_date(text)
        return
    if root == "TIME":
        _parse_time(text)
        return
    if root == "TIMESTAMP":
        if _JAVA_NUMERIC_RE.fullmatch(text):
            _parse_integral(text, "BIGINT")
            return
        _parse_timestamp(text)
        return
    if root == "TIMESTAMP_LTZ":
        _parse_timestamp(text)
        return
    raise RuntimeError(
        "Default value do not support the type of {}".format(data_type))


def _type_root(type_name: str) -> str:
    match = re.match(r"^([A-Za-z_]+)", type_name.strip())
    if match is None:
        raise ValueError("Unrecognized type {}".format(type_name))
    root = match.group(1).upper()
    # Java writes TIMESTAMP_LTZ as 'TIMESTAMP(p) WITH LOCAL TIME ZONE'.
    if root == "TIMESTAMP" and "WITH LOCAL TIME ZONE" in type_name.upper():
        return "TIMESTAMP_LTZ"
    return root


def _parse_integral(text: str, root: str) -> int:
    if _INTEGRAL_RE.fullmatch(text) is None:
        raise ValueError("Invalid integer {}".format(text))
    sign = -1 if text[0] == "-" else 1
    body = text[1:] if text[0] in "+-" else text
    # BinaryStringUtils truncates fractions, including '.5' and '10.'. It
    # checks the original bytes, so whitespace and Unicode digits are invalid.
    number = sign * int(body.split(".", 1)[0] or "0")
    low, high = _INT_RANGES[root]
    if number < low or number > high:
        raise ValueError("Value {} is out of range for {}".format(number, root))
    return number


def _decimal_scale(type_name: str) -> int:
    match = _DECIMAL_SCALE_RE.search(type_name)
    if match is None or match.group(1) is None:
        return 0
    return int(match.group(1))


def _parse_decimal(text: str, target_scale: int) -> None:
    match = _DECIMAL_RE.fullmatch(text)
    if match is None:
        raise ValueError("Invalid decimal {}".format(text))
    integer, fraction, only_fraction, exponent = match.groups()
    fraction = fraction or only_fraction or ""
    # BigDecimal(String) requires both the exponent and the resulting scale
    # (fraction digits minus exponent) to fit in an int. Leading zeros are
    # stripped first so a long zero-padded exponent stays within Python's
    # int conversion limit.
    sign = "-" if exponent and exponent[0] == "-" else ""
    exponent_digits = (exponent or "0").lstrip("+-").lstrip("0") or "0"
    if len(exponent_digits) > 10:
        raise ValueError("Exponent of {} is out of range".format(text))
    exponent = int(sign + exponent_digits)
    scale = len(fraction) - exponent
    if not (_INT_MIN <= exponent <= _INT_MAX and _INT_MIN <= scale <= _INT_MAX):
        raise ValueError("Exponent of {} is out of range".format(text))
    # Rescaling a zero never builds a power of ten. Otherwise an out-of-range
    # result becomes a null default, but one Java cannot rescale at all fails.
    if not any(int(digit) for digit in (integer or "") + fraction):
        return
    if abs(scale - target_scale) >= _MAX_TEN_POWER:
        raise ValueError(
            "Cannot rescale {} to scale {}".format(text, target_scale))


def _parse_date(text: str) -> None:
    if _JAVA_NUMERIC_RE.fullmatch(text):
        _parse_integral(text, "INT")
        return
    # DateTimeUtils.parseDate also accepts year-month and ignores a timestamp's
    # time suffix. Missing month/day components default to January/the first.
    space = text.find(" ")
    if space > 0:
        text = text[:space]
    parts = [part.strip(_JAVA_TRIM_CHARS) for part in text.split("-")]
    if len(parts) > 3 or any(
            _UNSIGNED_INTEGER_RE.fullmatch(part) is None for part in parts):
        raise ValueError("Cannot parse {!r} as DATE.".format(text))
    values = [_parse_integral(part, "INT") for part in parts]
    year, month, day = values + [1] * (3 - len(values))
    # Java DATE permits year zero, unlike datetime.
    if not (0 <= year <= 9999 and 1 <= month <= 12
            and 1 <= day <= monthrange(year, month)[1]):
        raise ValueError("Cannot parse {!r} as DATE.".format(text))


def _parse_time(text: str) -> None:
    if _JAVA_NUMERIC_RE.fullmatch(text):
        _parse_integral(text, "INT")
        return
    # DateTimeUtils.parseTime splits on ':' and '.' and only requires each
    # component to be a non-negative int. It does not bound hour/minute/second,
    # so '25:00:00' and '12:00:99' are accepted, and an optional '[+-]hh[:mm]'
    # timezone suffix may follow. Mirror that instead of building a real time.
    # Every separator is searched in the whole string, offset included, so in
    # '12:30+01:00' the second ':' is the offset's and the minute is '30+01'.
    end = len(text)
    timezone = text.find("-")
    if timezone < 0:
        timezone = text.find("+")
    if timezone >= 0:
        end = timezone
        colon3 = text.find(":", timezone)
        if colon3 < 0:
            _check_java_int(text[timezone + 1:])
        else:
            _check_java_int(text[timezone + 1:colon3])
            _check_java_int(text[colon3 + 1:])
    colon1 = text.find(":")
    if colon1 < 0:
        _check_java_int(_java_substring(text, 0, end))
        return
    _check_java_int(_java_substring(text, 0, colon1))
    colon2 = text.find(":", colon1 + 1)
    if colon2 < 0:
        _check_java_int(_java_substring(text, colon1 + 1, end))
        return
    _check_java_int(_java_substring(text, colon1 + 1, colon2))
    dot = text.find(".", colon2)
    if dot < 0:
        _check_java_int(_java_substring(text, colon2 + 1, end))
        return
    _check_java_int(_java_substring(text, colon2 + 1, dot))
    # parseFraction reads digits and treats anything else as zero, so the
    # fractional part never fails and may be empty or arbitrarily long.
    _java_substring(text, dot + 1, end)


def _java_substring(text: str, begin: int, end: int) -> str:
    """Match ``String.substring``, which throws instead of returning ''."""
    if begin > end:
        raise ValueError("Cannot parse {!r} as TIME.".format(text))
    return text[begin:end]


def _check_java_int(text: str) -> None:
    """Match ``DateTimeUtils.isInteger``: non-negative and fits in an int."""
    stripped = text.strip(_JAVA_TRIM_CHARS)
    if _UNSIGNED_INTEGER_RE.fullmatch(stripped) is None:
        raise ValueError("Cannot parse {!r} as an integer.".format(text))
    if int(stripped) > 2147483647:
        raise ValueError("Value {} is out of range for INT".format(stripped))


def _parse_timestamp(text: str) -> None:
    match = _TIMESTAMP_SPACE_RE.match(text) or _TIMESTAMP_T_RE.match(text)
    if match is None:
        raise ValueError("Cannot parse {!r} as TIMESTAMP.".format(text))
    year, month, day, hour, minute, second, fraction = match.groups()
    year, month, day = int(year), int(month), int(day)
    _check_timestamp_date(year, month, day)
    if hour is None:
        return
    hour, minute, second = int(hour), int(minute), int(second or "0")
    # The SMART resolver reads 24:00 as midnight of the following day.
    if hour == 24 and minute == 0 and second == 0 and not int(fraction or "0"):
        if (year, month, day) == (_MAX_YEAR, 12, 31):
            raise ValueError("Year {} is out of range.".format(year + 1))
        return
    time(hour, minute, second)


def _check_timestamp_date(year: int, month: int, day: int) -> None:
    """Bound a timestamp date the way the Java formatter's SMART resolver does.

    Unlike DATE, which validates the calendar exactly, ``LocalDate`` resolved in
    SMART mode clamps the day to the end of the month. ``2024-02-31`` becomes
    February 29 instead of failing, but month 0/13 and day 0/32 still fail.
    Year 0 is rejected because the formatter parses ``yyyy`` as an era year.
    """
    if year < 1 or year > _MAX_YEAR:
        raise ValueError("Year {} is out of range.".format(year))
    if not 1 <= month <= 12:
        raise ValueError("Month {} is out of range.".format(month))
    if not 1 <= day <= 31:
        raise ValueError("Day {} is out of range.".format(day))
