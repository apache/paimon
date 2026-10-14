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

"""Java BinaryStringUtils date/timestamp text, including SMART resolution."""

import re

from pypaimon.casting.numeric_cast import _parse_integer
from pypaimon.casting.numeric_utils import java_decimal_digit
from pypaimon.casting.temporal_utils import (
    _div, _rem, _signed, _java_date_parts, _ymd_to_epoch_day,
    _epoch_day_to_java_date, _local_to_utc_seconds,
)


_TRIM = ''.join(chr(i) for i in range(33))
_DIGITS = re.compile(r'[0-9]+\Z')


def _numeric(text):
    text = text[1:] if text.startswith('-') else text
    return bool(text) and all(java_decimal_digit(ch) is not None for ch in text)


def format_date(value):
    year, month, day = _java_date_parts(value)
    return '-'.join(''.join(chr(48 + _rem(_div(value, divisor), 10)) for divisor in divisors)
                    for value, divisors in [(year, (1000, 100, 10, 1)), (month, (10, 1)), (day, (10, 1))])


def _days_in_month(year, month):
    leap = year % 400 == 0 or year % 4 == 0 and year % 100 != 0
    return 29 if month == 2 and leap else 28 if month == 2 else 30 if month in (4, 6, 9, 11) else (
        31 if 1 <= month <= 12 else 0)


def parse_date(text):
    if _numeric(text):
        return _parse_integer(text, 32)
    position = text.find(' ')
    date = text[:position] if position > 0 else text
    components = date.split('-')
    if len(components) > 3:
        raise ValueError('Invalid Java DATE: %s' % text)
    values = [0, 1, 1]
    for i, component in enumerate(components):
        component = component.strip(_TRIM)
        if _DIGITS.fullmatch(component) is None or len(component.lstrip('0')) > 10:
            raise ValueError('Invalid Java DATE: %s' % text)
        values[i] = int(component.lstrip('0') or '0')
        if values[i] > (1 << 31) - 1:
            raise ValueError('Invalid Java DATE: %s' % text)
    year, month, day = values
    if not 0 <= year <= 9999 or not 1 <= day <= _days_in_month(year, month):
        raise ValueError('Invalid Java DATE: %s' % text)
    return _epoch_day_to_java_date(_ymd_to_epoch_day(year, month, day))


def _component(text):
    if not text:
        return None
    if len(text) > 21 or _DIGITS.fullmatch(text) is None:
        raise ValueError('Invalid Java timestamp component')
    first = int(text[:2])
    if len(text) > 2 and int(text[2:]) != first:
        raise ValueError('Conflicting Java timestamp fields')
    return first


def _time_components(text, space):
    parts = text.split(':')
    if not (3 if space else 2) <= len(parts) <= 3:
        raise ValueError('Invalid Java timestamp time')
    values = [_component(parts[0]), _component(parts[1]), None, None]
    if len(parts) == 3:
        second, dot, fraction = parts[2].partition('.')
        values[2] = _component(second)
        if dot:
            if len(fraction) > 9 or fraction and _DIGITS.fullmatch(fraction) is None:
                raise ValueError('Invalid Java timestamp fraction')
            values[3] = int(fraction or '0') * 10 ** (9 - len(fraction))
    return values


def parse_timestamp(text, precision, local):
    if not local and _numeric(text):
        ticks = _parse_integer(text, 64)
        if precision == 0:
            return _signed(ticks * 1000, 64), 0
        if precision == 3:
            return ticks, 0
        if precision in (6, 9):
            divisor, factor = (1000, 1000) if precision == 6 else (1000000, 1)
            return ticks // divisor, ticks % divisor * factor
        raise ValueError('Unsupported numeric timestamp precision')
    position = next((i for i, ch in enumerate(text) if ch in (' ', 'T')), len(text))
    date, rest = text[:position], text[position:]
    parts = date.split('-')
    if len(parts) != 3:
        raise ValueError('Invalid Java timestamp date: %s' % text)
    if not parts[1] and parts[2]:
        raise ValueError('Java timestamp day needs its month')
    year_text = parts[0]
    digits = year_text[1:] if year_text.startswith('+') else year_text
    if (_DIGITS.fullmatch(digits) is None or len(digits) > 19
            or (len(digits) <= 4 if year_text.startswith('+') else len(digits) != 4)):
        raise ValueError('Invalid Java timestamp year')
    year = int(digits)
    complete_date = bool(parts[1]) and bool(parts[2])
    month, day = _component(parts[1]), _component(parts[2])
    month, day = 1 if month is None else month, 1 if day is None else day
    if not 1 <= year <= 999999999 or not 1 <= month <= 12 or not 1 <= day <= 31:
        raise ValueError('Invalid Java timestamp date')
    epoch_day = _ymd_to_epoch_day(year, month, min(day, _days_in_month(year, month)))
    values = [None] * 4
    if rest.startswith(' '):
        other, separator, tail = rest[1:].partition('T')
        values = _time_components(other, True)
        rest = separator + tail
    if rest.startswith('T'):
        other = _time_components(rest[1:], False)
        for i, value in enumerate(other):
            if values[i] is not None and value is not None and values[i] != value:
                raise ValueError('Conflicting Java timestamp time')
            if value is not None:
                values[i] = value
        rest = ''
    if rest:
        raise ValueError('Invalid Java timestamp suffix')
    if values[0] == 24 and (values[1] is None and values[2] is not None
                            or values[2] is None and values[3] is not None):
        raise ValueError("Java timestamp requires a minute for 24-hour time with seconds")
    hour, minute, second, nanos = [0 if v is None else v for v in values]
    if (not 0 <= hour <= 24 or not 0 <= minute <= 59 or not 0 <= second <= 59
            or hour == 24 and (minute != 0 or second != 0 or nanos != 0)):
        raise ValueError('Invalid Java timestamp time')
    seconds = (epoch_day + (hour == 24 and complete_date)) * 86400 + hour % 24 * 3600 + minute * 60 + second
    nanos = nanos // 10 ** (9 - precision) * 10 ** (9 - precision)
    millis = _signed((_local_to_utc_seconds(seconds) if local else seconds) * 1000, 64)
    return _signed(millis + nanos // 1000000, 64), nanos % 1000000
