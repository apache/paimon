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

"""Java DateTimeUtils text shared by schema evolution and row rendering."""

import pyarrow as pa

from pypaimon.casting.temporal_utils import _epoch_day_to_date, _local_offset, _signed
from pypaimon.table.row.generic_row import _parse_type_precision_scale


def format_timestamp(value, precision, nanos=None):
    """Keep significant fraction digits, with declared precision as a minimum."""
    return format_timestamp_parts(value.year, value.month, value.day, value.hour, value.minute,
                                  value.second, value.microsecond * 1000 if nanos is None else nanos, precision)


def format_timestamp_parts(year, month, day, hour, minute, second, nanos, precision):
    # DateTimeUtils.int4 writes four individual digits, even for years outside
    # 0000..9999. Preserve Java's signed remainder rather than Python's modulo.
    digits = []
    for divisor in (1000, 100, 10, 1):
        part = abs(year) // divisor
        part = -(part % 10) if year < 0 else part % 10
        digits.append(chr(ord('0') + part))
    text = "{}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}".format(
        ''.join(digits), month, day, hour, minute, second)
    fraction = "{:09d}".format(nanos)
    while len(fraction) > precision and fraction.endswith("0"):
        fraction = fraction[:-1]
    return text + "." + fraction if fraction else text


def format_time(millis, precision):
    """Java TIME has millisecond storage, independent of declared precision."""
    if millis < 0:
        millis %= 86400000
    seconds, fraction = divmod(millis, 1000)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    text = "{:02d}:{:02d}:{:02d}".format(hours % 100, minutes, seconds)
    if precision <= 0:
        return text
    digits = []
    while precision > 0:
        digits.append(str(fraction // 100))
        fraction = fraction % 100 * 10
        if fraction == 0:
            break
        precision -= 1
    return text + "." + "".join(digits)


def temporal_to_string(array, source_type=None, target_type=None):
    """Return None for non-temporal input; preserve NULLs, slices and nanos.

    LTZ uses the process local zone, as Java TimeZone.getDefault() does.
    Character length is applied after formatting, using the target Paimon type.
    """
    timestamp = pa.types.is_timestamp(array.type)
    if not timestamp and not pa.types.is_time(array.type):
        return None
    unit = array.type.unit
    precision = (_parse_type_precision_scale(source_type)[0] if source_type is not None
                 else {'s': 0, 'ms': 3, 'us': 6, 'ns': 9}[unit])
    if timestamp:
        raw = array.cast(pa.int64()).to_pylist()
        ticks = {'s': 1, 'ms': 1000, 'us': 1000000, 'ns': 1000000000}[unit]
        source_name = str(source_type).upper() if source_type is not None else ''
        local_zone = (array.type.tz is not None if source_type is None else
                      source_name.startswith('TIMESTAMP_LTZ') or 'WITH LOCAL TIME ZONE' in source_name)
    else:
        raw = array.cast(pa.time32('ms'), safe=False).cast(pa.int32()).to_pylist()
    name = str(target_type).upper() if target_type is not None else 'STRING'
    length = (_parse_type_precision_scale(target_type)[0] or 1) if name.startswith(('CHAR', 'VARCHAR')) else None
    values = []
    for value in raw:
        if value is None:
            values.append(None)
            continue
        if timestamp:
            millis = value * 1000 if unit == 's' else value // (ticks // 1000)
            # Arrow seconds may exceed Java's signed millisecond storage.
            millis = (millis + (1 << 63)) % (1 << 64) - (1 << 63)
            seconds = millis // 1000
            if local_zone:
                millis += _local_offset(seconds) * 1000
                millis = (millis + (1 << 63)) % (1 << 64) - (1 << 63)
                seconds = millis // 1000
            sub_milli = value % (ticks // 1000) * (1000000000 // ticks) if ticks > 1000 else 0
            nanos = millis % 1000 * 1000000 + sub_milli
            day, second_of_day = divmod(seconds, 86400)
            day = (day + (1 << 31)) % (1 << 32) - (1 << 31)
            year, month, day = _epoch_day_to_date(day)
            hour, second_of_day = divmod(second_of_day, 3600)
            minute, second = divmod(second_of_day, 60)
            text = format_timestamp_parts(year, month, day, hour, minute, second,
                                          nanos, precision)
        else:
            text = format_time(value, precision)
        if length is not None:
            text = text[:length]
            if name.startswith('CHAR'):
                text = text.ljust(length)
        values.append(text)
    return pa.array(values, type=pa.string())


def timestamp_object_to_string(array):
    """Java Timestamp.toString: ISO wall time, regardless of the current LTZ zone."""
    values = []
    unit = array.type.unit
    for value in array.cast(pa.int64()).to_pylist():
        if value is None:
            values.append(None)
            continue
        millis = (_signed(value * 1000, 64) if unit == 's' else value if unit == 'ms'
                  else value // (1000 if unit == 'us' else 1000000))
        sub = value % (1000 if unit == 'us' else 1000000) * (1000 if unit == 'us' else 1) if unit in ('us', 'ns') else 0
        nanos = millis % 1000 * 1000000 + sub
        day, second_of_day = divmod(millis // 1000, 86400)
        year, month, day = _epoch_day_to_date(_signed(day, 32))
        year_text = ('%04d' % year if 0 <= year <= 9999 else '-%04d' % -year if year < 0 else '+%d' % year)
        hour, second_of_day = divmod(second_of_day, 3600)
        minute, second = divmod(second_of_day, 60)
        text = '%s-%02d-%02dT%02d:%02d' % (year_text, month, day, hour, minute)
        if second or nanos:
            text += ':%02d' % second
        if nanos:
            precision = 3 if nanos % 1000000 == 0 else 6 if nanos % 1000 == 0 else 9
            text += '.' + ('%09d' % nanos)[:precision]
        values.append(text)
    return pa.array(values, type=pa.string())
