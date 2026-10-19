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

"""Java TypeUtils conversions for private overwrite and truncate specs."""

import re
import struct
from calendar import monthrange
from datetime import date, datetime, timedelta


_UNREPRESENTABLE_PARTITION_VALUE = object()
_JAVA_WHITESPACE = ''.join(chr(value) for value in range(33))


def _partition_spec_value(value, data_type):
    """TypeUtils.castFromString for the scalar types accepted as partitions.

    Partition specs are not SQL casts: integer text must be integral and
    DECIMAL uses HALF_UP with precision overflow represented as NULL.
    Already typed Python values retain the existing public API contract.
    """
    if not isinstance(value, str):
        return value
    import pyarrow as pa

    from pypaimon.casting.decimal_cast import cast_decimal
    from pypaimon.casting.numeric_utils import java_floating_string
    from pypaimon.casting.temporal_utils import _local_to_utc_seconds, _signed
    from pypaimon.schema.data_types import PyarrowFieldParser
    from pypaimon.table.row.generic_row import _parse_type_precision_scale
    from unicodedata import decimal

    target = PyarrowFieldParser.from_paimon_type(data_type)
    if pa.types.is_integer(target) or pa.types.is_decimal(target):
        # Integer.valueOf and BigDecimal scan UTF-16 chars. BMP decimal
        # digits are accepted; a supplementary-plane digit is two surrogates.
        value = ''.join(str(decimal(char)) if ord(char) <= 0xffff and char.isdecimal() else char for char in value)
    if pa.types.is_string(target) or pa.types.is_binary(target):
        result = value.encode('utf-8') if pa.types.is_binary(target) else value
        match = re.fullmatch(r'(CHAR|VARCHAR|VARBINARY)\((\d+)\)', data_type.type.upper())
        if match is not None and len(result) > int(match.group(2)):
            raise ValueError('Partition value exceeds the declared type length')
        return result
    if pa.types.is_integer(target):
        if re.fullmatch(r'[+-]?[0-9]+', value) is None:
            raise ValueError('Partition integer text must be integral')
        result = int(value)
        if not -(1 << (target.bit_width - 1)) <= result < (1 << (target.bit_width - 1)):
            raise ValueError('Partition integer value is out of range')
        return result
    if pa.types.is_boolean(target):
        lowered = value.lower()
        if lowered in ('t', 'true', 'y', 'yes', '1'):
            return True
        if lowered in ('f', 'false', 'n', 'no', '0'):
            return False
        raise ValueError('Invalid BOOLEAN partition value')
    if pa.types.is_floating(target):
        text = value.strip(_JAVA_WHITESPACE)
        if re.fullmatch(r'[+-]?(?:NaN|Infinity|(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)'
                        r'(?:[eE][+-]?[0-9]+)?[fFdD]?|0[xX](?:[0-9a-fA-F]+(?:\.[0-9a-fA-F]*)?'
                        r'|\.[0-9a-fA-F]+)'
                        r'[pP][+-]?[0-9]+[fFdD]?)', text) is None:
            raise ValueError('Invalid floating partition value')
        text = text[:-1] if text[-1:] in ('f', 'F', 'd', 'D') else text
        result = float.fromhex(text) if 'p' in text.lower() else float(text)
        if target == pa.float32():
            try:
                single = struct.unpack('>f', struct.pack('>f', result))[0]
            except OverflowError:
                single = float('-inf') if result < 0 else float('inf')
            if result != single and java_floating_string(result) != java_floating_string(single, True):
                raise ValueError('Partition value cannot be cast to FLOAT due to precision loss')
            return single
        return result
    if pa.types.is_decimal(target):
        return cast_decimal(pa.array([value]), target)[0].as_py()
    if pa.types.is_date32(target):
        if re.fullmatch(r'-?[0-9]+', value):
            days = int(value)
            if not -(1 << 31) <= days < (1 << 31):
                raise ValueError('DATE partition value is out of range')
            try:
                return date(1970, 1, 1) + timedelta(days=days)
            except OverflowError:
                # Pure Python cannot write partition dates outside datetime's
                # range. A valid Java spec outside that range cannot match one.
                return _UNREPRESENTABLE_PARTITION_VALUE
        text = value.split(' ', 1)[0] if value.find(' ') > 0 else value
        parts = text.split('-')
        parts = [part.strip(_JAVA_WHITESPACE) for part in parts]
        if not 1 <= len(parts) <= 3 or any(re.fullmatch(r'[0-9]+', part) is None for part in parts):
            raise ValueError('Invalid DATE partition value')
        return date(*([int(part) for part in parts] + [1] * (3 - len(parts))))
    if pa.types.is_time(target):
        if re.fullmatch(r'-?[0-9]+', value):
            millis = int(value)
        else:
            # Java parseTime permits partial clocks and adds a signed offset.
            match = re.fullmatch(r'[\x00-\x20]*([0-9]+)[\x00-\x20]*(?::[\x00-\x20]*([0-9]+)[\x00-\x20]*'
                                 r'(?::[\x00-\x20]*([0-9]+)(?:\.([^+\-]*))?[\x00-\x20]*)?)?'
                                 r'(?:([+-])[\x00-\x20]*([0-9]+)[\x00-\x20]*'
                                 r'(?::[\x00-\x20]*([0-9]+)[\x00-\x20]*)?)?', value)
            if match is None:
                raise ValueError('Invalid TIME partition value')
            hours, minutes, seconds, fraction, sign, zone_hour, zone_minute = match.groups()
            if zone_minute is not None and seconds is None:
                raise ValueError('Invalid TIME partition value')
            fraction = (fraction or '').strip(_JAVA_WHITESPACE)
            digits = (fraction + '000')[:3]
            millis = sum((int(char) if '0' <= char <= '9' else 0) * factor
                         for char, factor in zip(digits, (100, 10, 1)))
            if len(fraction) > 3 and fraction[3] >= '5':
                millis += 1
            components = [int(item or 0) for item in (hours, minutes, seconds, zone_hour, zone_minute)]
            if any(item >= 1 << 31 for item in components):
                raise ValueError('TIME partition component is out of range')
            millis += ((int(hours) * 60 + int(minutes or 0)) * 60 + int(seconds or 0)) * 1000
            if sign is not None:
                millis += (1 if sign == '+' else -1) * (int(zone_hour) * 60 + int(zone_minute or 0)) * 60000
            millis = _signed(millis, 32)
        if not -(1 << 31) <= millis < (1 << 31):
            raise ValueError('TIME partition value is out of range')
        if not 0 <= millis < 86400000:
            return _UNREPRESENTABLE_PARTITION_VALUE
        return (datetime(1970, 1, 1) + timedelta(milliseconds=millis)).time()
    if pa.types.is_timestamp(target):
        precision = _parse_type_precision_scale(data_type)[0]
        if target.tz is None and re.fullmatch(r'-?[0-9]+', value):
            if precision not in (0, 3, 6, 9):
                raise ValueError('Numeric TIMESTAMP partition values require precision 0, 3, 6 or 9')
            ticks = int(value)
            if not -(1 << 63) <= ticks < (1 << 63):
                raise ValueError('TIMESTAMP partition value is out of range')
            # Java first converts numeric seconds to signed-long millis.
            # The multiplication wraps before it constructs the timestamp.
            storage_type = target
            if precision == 0:
                ticks = _signed(ticks * 1000, 64)
                # A wrapped product need not retain whole-second alignment.
                # Timestamp.fromEpochMillis keeps that millisecond remainder.
                storage_type = pa.timestamp('ms')
            try:
                return pa.scalar(ticks, type=storage_type).as_py()
            except OverflowError:
                return _UNREPRESENTABLE_PARTITION_VALUE
        match = re.fullmatch(r'(\+?[0-9]+)-([0-9]*)-([0-9]*)'
                             r'(?: ([0-9]*):([0-9]*):([0-9]*)(?:\.([0-9]{0,9}))?)?'
                             r'(?:T([0-9]*):([0-9]*)(?::([0-9]*)(?:\.([0-9]{0,9}))?)?)?', value)
        if match is None:
            raise ValueError('Invalid TIMESTAMP partition value')
        year_text, month_text, day_text, hour, minute, second, fraction, t_hour, t_minute, t_second, t_fraction = (
            match.groups())
        year_digits = year_text.lstrip('+')
        if ((not year_text.startswith('+') and len(year_digits) != 4)
                or year_text.startswith('+') and not 5 <= len(year_digits) <= 19
                or not 1 <= int(year_digits) <= 999999999
                or not month_text and day_text):
            raise ValueError('Invalid TIMESTAMP partition value')

        def component(text):
            if not text:
                return None
            # DateTimeFormatter's adjacent optional [MM][M] patterns can
            # repeat a component, provided both parsed values agree.
            if len(text) > 21 or len(text) > 2 and int(text[:2]) != int(text[2:]):
                raise ValueError('Conflicting TIMESTAMP partition components')
            return int(text[:2])

        clock = [component(text) for text in (hour, minute, second)] + [
            None if fraction is None else int(fraction.ljust(9, '0'))]
        other = [component(text) for text in (t_hour, t_minute, t_second)] + [
            None if t_fraction is None else int(t_fraction.ljust(9, '0'))]
        for index, (left, right) in enumerate(zip(clock, other)):
            if left is not None and right is not None and left != right:
                raise ValueError('Conflicting TIMESTAMP partition components')
            if right is not None:
                clock[index] = right
        if clock[0] == 24 and (clock[1] is None and clock[2] is not None
                               or clock[2] is None and clock[3] is not None):
            raise ValueError('Invalid TIMESTAMP partition value')
        hour, minute, second, nanos = [item or 0 for item in clock]
        year, month, day = int(year_digits), component(month_text), component(day_text)
        month = 1 if month is None else month
        day = 1 if day is None else day
        if (not 1 <= month <= 12 or not 1 <= day <= 31 or not 0 <= hour <= 24
                or hour == 24 and (minute or second or nanos) or not 0 <= minute <= 59 or not 0 <= second <= 59):
            raise ValueError('Invalid TIMESTAMP partition value')
        if year > 9999:
            return _UNREPRESENTABLE_PARTITION_VALUE
        # DateTimeFormatter's default SMART resolver clamps day-of-month and
        # accepts exactly midnight as 24:00:00 on the following date.
        whole = datetime(year, month, min(day, monthrange(year, month)[1]), hour % 24, minute, second)
        if hour == 24 and month_text and day_text:
            whole += timedelta(days=1)
        nanos = nanos // 10 ** (9 - precision) * 10 ** (9 - precision)
        epoch = whole - datetime(1970, 1, 1)
        seconds = epoch.days * 86400 + epoch.seconds
        if target.tz is not None:
            seconds = _local_to_utc_seconds(seconds)
        ticks_per_second = {'s': 1, 'ms': 1000, 'us': 1000000, 'ns': 1000000000}[target.unit]
        return pa.scalar(seconds * ticks_per_second + nanos // (1000000000 // ticks_per_second), type=target).as_py()
    raise ValueError('Unsupported partition spec type %s' % data_type)
