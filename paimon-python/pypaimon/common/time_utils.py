################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
################################################################################


def parse_duration(text: str) -> int:
    if text is None:
        raise ValueError("text cannot be None")

    trimmed = text.strip().lower()
    if not trimmed:
        raise ValueError("argument is an empty- or whitespace-only string")

    pos = 0
    while pos < len(trimmed) and trimmed[pos].isdigit():
        pos += 1

    number_str = trimmed[:pos]
    unit_str = trimmed[pos:].strip()

    if not number_str:
        raise ValueError("text does not start with a number")

    try:
        value = int(number_str)
    except ValueError:
        raise ValueError(
            f"The value '{number_str}' cannot be re represented as 64bit number (numeric overflow)."
        )

    if not unit_str:
        result_ms = value
    elif unit_str in ('ns', 'nano', 'nanosecond', 'nanoseconds'):
        result_ms = value / 1_000_000
    elif unit_str in ('µs', 'micro', 'microsecond', 'microseconds'):
        result_ms = value / 1_000
    elif unit_str in ('ms', 'milli', 'millisecond', 'milliseconds'):
        result_ms = value
    elif unit_str in ('s', 'sec', 'second', 'seconds'):
        result_ms = value * 1000
    elif unit_str in ('m', 'min', 'minute', 'minutes'):
        result_ms = value * 60 * 1000
    elif unit_str in ('h', 'hour', 'hours'):
        result_ms = value * 60 * 60 * 1000
    elif unit_str in ('d', 'day', 'days'):
        result_ms = value * 24 * 60 * 60 * 1000
    else:
        supported_units = (
            'DAYS: (d | day | days), '
            'HOURS: (h | hour | hours), '
            'MINUTES: (m | min | minute | minutes), '
            'SECONDS: (s | sec | second | seconds), '
            'MILLISECONDS: (ms | milli | millisecond | milliseconds), '
            'MICROSECONDS: (µs | micro | microsecond | microseconds), '
            'NANOSECONDS: (ns | nano | nanosecond | nanoseconds)'
        )
        raise ValueError(
            f"Time interval unit label '{unit_str}' does not match any of the recognized units: "
            f"{supported_units}"
        )

    result_ms_int = int(round(result_ms))

    if result_ms_int < 0:
        raise ValueError(f"Duration cannot be negative: {text}")

    return result_ms_int
