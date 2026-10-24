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

"""Java DateTimeUtils text shared by partition paths and row rendering."""


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
