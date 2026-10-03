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

"""``metadata.stats-mode`` handling for the write path.

Mirrors Java ``org.apache.paimon.statistics.*SimpleColStatsCollector``:
each value column's collected ``(min, max, null_count)`` is converted
according to the configured mode before it is written into the manifest,
so a Paimon/Spark/Flink reader can data-skip files pypaimon wrote.

- ``none``     -> no stats at all (min/max/null_count all dropped)
- ``counts``   -> only the null count is kept
- ``truncate(N)`` -> null count plus a *truncated* min/max: a min <= the
  real min and a max >= the real max, so the bound stays sound for
  pruning. Only strings and byte strings are truncated; other types keep
  their full min/max. Matches ``TruncateSimpleColStatsCollector``.
- ``full``     -> the full min/max/null_count (pypaimon's existing
  behaviour when ``metadata.stats-mode=full``).

Key stats are always collected in full regardless of this option (the LSM
needs exact key bounds); only *value* stats are converted here.
"""

import re
from typing import Optional, Tuple

NONE = "none"
COUNTS = "counts"
TRUNCATE = "truncate"
FULL = "full"

# Highest Unicode code point; mirrors Java ``Character.MAX_CODE_POINT``.
_MAX_CODE_POINT = 0x10FFFF
_TRUNCATE_PATTERN = re.compile(r"truncate\((\d+)\)")


def parse_stats_mode(raw: Optional[str]) -> Tuple[str, Optional[int]]:
    """Parse ``metadata.stats-mode`` into ``(kind, length)``.

    ``length`` is the truncation length for ``truncate(N)`` and ``None``
    otherwise. Mirrors Java ``SimpleColStatsCollector.from`` (case
    insensitive; ``truncate(N)`` requires ``N > 0``). Raises ``ValueError``
    on an unrecognized mode, matching Java's ``IllegalArgumentException``.
    """
    if raw is None:
        return NONE, None
    s = raw.strip().lower()
    if s == NONE:
        return NONE, None
    if s == COUNTS:
        return COUNTS, None
    if s == FULL:
        return FULL, None
    match = _TRUNCATE_PATTERN.fullmatch(s)
    if match is not None:
        length = int(match.group(1))
        if length <= 0:
            raise ValueError("Truncate length should be larger than zero.")
        return TRUNCATE, length
    raise ValueError("Unexpected metadata.stats-mode: {!r}".format(raw))


def value_stats_enabled(kind: str) -> bool:
    """Whether any per-column value stats are recorded for this mode.

    ``none`` records nothing (the column is left out of the manifest's
    value-stats columns); every other mode records at least the null count.
    """
    return kind != NONE


def truncate_min(value, length: int):
    """A truncated value <= ``value`` (a sound lower bound).

    Strings keep their first ``length`` code points; byte strings keep
    their first ``length`` bytes. Dropping a suffix only makes the value
    smaller, so the result is always <= the original. Non-(str/bytes)
    values are returned unchanged (numbers etc. are never truncated),
    matching ``TruncateSimpleColStatsCollector.truncateMin``.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value[:length]
    if isinstance(value, (bytes, bytearray)):
        data = bytes(value)
        return data if len(data) <= length else data[:length]
    return value


def truncate_max(value, length: int):
    """A truncated value >= ``value`` (a sound upper bound), or ``None``.

    Truncate to ``length`` units, then increment from the end to stay >=
    the original: for strings bump the last code point that is not already
    the maximum; for byte strings bump the last byte that is not ``0xFF``.
    Returns the original untouched when it already fits within ``length``.
    Returns ``None`` when every position is already at its ceiling (no
    sound upper bound exists) -- the caller then drops both min and max,
    matching ``TruncateSimpleColStatsCollector.truncateMax``.
    """
    if value is None:
        return None
    if isinstance(value, str):
        if len(value) <= length:
            return value
        chars = list(value[:length])
        for i in range(length - 1, -1, -1):
            nxt = ord(chars[i]) + 1
            # Skip surrogates: they are unencodable in UTF-8, so bumping an
            # earlier position (a still-sound, if looser, upper bound) is
            # preferred over emitting an invalid stat.
            if nxt <= _MAX_CODE_POINT and not (0xD800 <= nxt <= 0xDFFF):
                return "".join(chars[:i]) + chr(nxt)
        return None
    if isinstance(value, (bytes, bytearray)):
        data = bytes(value)
        if len(data) <= length:
            return data
        truncated = bytearray(data[:length])
        for i in range(length - 1, -1, -1):
            if truncated[i] != 0xFF:
                truncated[i] += 1
                return bytes(truncated[:i + 1])
        return None
    return value


def convert_col_stats(kind: str, length: Optional[int],
                      min_value, max_value, null_count):
    """Convert one column's full ``(min, max, null_count)`` per the mode.

    Returns the ``(min, max, null_count)`` triple to record. Mirrors the
    ``convert`` of each Java collector: ``none`` drops everything;
    ``counts`` keeps only the null count; ``truncate(N)`` truncates min/max
    and -- crucially -- drops *both* min and max when no sound upper bound
    exists (``truncate_max`` returned ``None``); ``full`` is a pass-through.
    """
    if kind == NONE:
        return None, None, None
    if kind == COUNTS:
        return None, None, null_count
    if kind == FULL:
        return min_value, max_value, null_count
    truncated_max = truncate_max(max_value, length)
    if truncated_max is None:
        return None, None, null_count
    return truncate_min(min_value, length), truncated_max, null_count
