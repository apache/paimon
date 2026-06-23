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

import calendar
from datetime import datetime, timedelta
from typing import List


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


# Unit label -> nanoseconds. Mirrors the aliases accepted by ``parse_duration``
# (and Java ``TimeUtils.parseDuration``); a bare number with no unit is treated
# as milliseconds, matching ``parse_duration``.
_UNIT_TO_NANOS = {
    'ns': 1, 'nano': 1, 'nanosecond': 1, 'nanoseconds': 1,
    'µs': 1_000, 'micro': 1_000, 'microsecond': 1_000, 'microseconds': 1_000,
    'ms': 1_000_000, 'milli': 1_000_000, 'millisecond': 1_000_000, 'milliseconds': 1_000_000,
    's': 1_000_000_000, 'sec': 1_000_000_000, 'second': 1_000_000_000, 'seconds': 1_000_000_000,
    'm': 60_000_000_000, 'min': 60_000_000_000, 'minute': 60_000_000_000, 'minutes': 60_000_000_000,
    'h': 3_600_000_000_000, 'hour': 3_600_000_000_000, 'hours': 3_600_000_000_000,
    'd': 86_400_000_000_000, 'day': 86_400_000_000_000, 'days': 86_400_000_000_000,
}


def parse_duration_nanos(text: str) -> int:
    """Parse a duration string to an integer nanosecond count.

    This is the full-precision integer counterpart of :func:`parse_duration`
    (which returns rounded milliseconds): it accepts the same unit aliases and
    the same "bare number means milliseconds" convention, but keeps sub-millisecond
    units exactly (``"1ns"`` -> ``1``, ``"500micro"`` -> ``500_000``) instead of
    rounding them to zero. Use it where sub-millisecond precision must not be
    silently dropped. ``parse_duration`` is intentionally left untouched so its
    millisecond contract (relied on by option parsing) does not change.
    """
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
        nanos_per_unit = 1_000_000  # bare number is milliseconds
    elif unit_str in _UNIT_TO_NANOS:
        nanos_per_unit = _UNIT_TO_NANOS[unit_str]
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

    return value * nanos_per_unit


# ---------------------------------------------------------------------------
# Codecs for Java temporal types as serialized by Jackson's JavaTimeModule.
#
# These mirror the on-disk JSON shapes that Paimon's Java side reads/writes so
# that a tag file written by pypaimon is interoperable with Java and vice versa
# (see ``org.apache.paimon.tag.Tag`` and ``TagTest``):
#   - ``LocalDateTime`` -> JSON array ``[year, month, day, hour, minute,
#     second, nanoOfSecond]``
#   - ``Duration``      -> JSON number of (fractional) seconds, e.g. ``86400.0``
#
# Resolution note: Python's ``datetime`` / ``timedelta`` are microsecond-based,
# so a Java create-time or duration finer than a microsecond is truncated to
# microseconds on read. Tag granularity is coarse, so this is not a concern in
# practice.
# ---------------------------------------------------------------------------

_NANOS_PER_MICRO = 1000


def local_datetime_to_json_array(dt: datetime) -> List[int]:
    """Encode a naive ``datetime`` as Java ``LocalDateTime`` array form.

    Python ``datetime`` only has microsecond resolution, so the emitted
    nanoOfSecond is ``microsecond * 1000`` (never finer than microseconds).
    """
    return [
        dt.year,
        dt.month,
        dt.day,
        dt.hour,
        dt.minute,
        dt.second,
        dt.microsecond * _NANOS_PER_MICRO,
    ]


def json_array_to_local_datetime(arr: List[int]) -> datetime:
    """Decode a Java ``LocalDateTime`` array form into a naive ``datetime``.

    Jackson omits trailing zero components, so the array may be shorter than 7;
    missing components default to 0. A Java nanoOfSecond finer than a
    microsecond is truncated to microseconds (Python's resolution limit).
    """
    padded = list(arr) + [0] * (7 - len(arr))
    year, month, day, hour, minute, second, nano = padded[:7]
    return datetime(
        year, month, day, hour, minute, second, nano // _NANOS_PER_MICRO
    )


def duration_to_json_seconds(td: timedelta):
    """Encode a ``timedelta`` as Java ``Duration`` decimal-seconds number."""
    return td.total_seconds()


def json_seconds_to_duration(seconds) -> timedelta:
    """Decode a Java ``Duration`` decimal-seconds number into a ``timedelta``."""
    return timedelta(seconds=seconds)


def duration_to_iso8601(td: timedelta) -> str:
    """Render a non-negative ``timedelta`` like ``java.time.Duration.toString()``.

    Examples: 1 day -> ``PT24H`` (Duration has no day unit), 30 min ->
    ``PT30M``, 5 s -> ``PT5S``, zero -> ``PT0S``. Matches what Paimon's Java
    ``$tags`` system table surfaces for ``time_retained``. Retentions only ever
    come from ``parse_duration``, which rejects negatives, so only the
    non-negative form is supported.
    """
    total_micros = (
        td.days * 86_400_000_000 + td.seconds * 1_000_000 + td.microseconds
    )
    if total_micros <= 0:
        return "PT0S"

    total_seconds, micros = divmod(total_micros, 1_000_000)
    hours, rem = divmod(total_seconds, 3600)
    minutes, secs = divmod(rem, 60)

    buf = "PT"
    if hours != 0:
        buf += "{}H".format(hours)
    if minutes != 0:
        buf += "{}M".format(minutes)
    if secs != 0 or micros != 0 or buf == "PT":
        frac = ""
        if micros != 0:
            frac = "." + ("%06d" % micros).rstrip("0")
        buf += "{}{}S".format(secs, frac)
    return buf


def local_datetime_to_millis(dt: datetime) -> int:
    """Convert a naive ``LocalDateTime`` to epoch millis (treated as UTC).

    The tag create-time is timezone-less (Java ``LocalDateTime``); interpreting
    it as UTC keeps the millis self-consistent with the ``$tags`` system table,
    whose ``create_time`` mirrors Java ``Timestamp.fromLocalDateTime`` (also
    zone-less wall-clock arithmetic). Uses integer math with floored
    sub-millisecond truncation (matching Java ``Instant.toEpochMilli``), so it
    is exact and correct for pre-epoch instants too.
    """
    return calendar.timegm(dt.timetuple()) * 1000 + dt.microsecond // 1000
