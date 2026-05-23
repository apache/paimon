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

"""
Time extractor to extract datetime from partition values.

This mirrors the Java implementation at:
  org.apache.paimon.partition.PartitionTimeExtractor
"""

import re
from datetime import datetime, time as dt_time
from typing import Dict, List, Optional


# Default timestamp formats (lenient parsing, mirrors Java TIMESTAMP_FORMATTER)
_DEFAULT_TIMESTAMP_FORMATS = [
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
]

# Java DateTimeFormatter → Python strptime token mapping.
# Note: Python's %f is microseconds (6 digits); Java's SSS is milliseconds (3 digits).
# This works for parsing because strptime(%f) accepts 1-6 digits, but the semantic
# precision differs. Callers should be aware of this when formatting output.
_JAVA_TO_PYTHON_TOKENS = {
    "yyyy": "%Y",
    "yy": "%y",
    "MM": "%m",
    "dd": "%d",
    "HH": "%H",
    "mm": "%M",
    "SSS": "%f",
    "SS": "%f",
    "ss": "%S",
}

# Regex that matches Java tokens (longest first) or quoted literals
_JAVA_TOKEN_RE = re.compile(
    r"'([^']*)'|(" + "|".join(re.escape(k) for k in sorted(_JAVA_TO_PYTHON_TOKENS, key=len, reverse=True)) + r")"
)


def _java_to_python_format(java_fmt: str) -> str:
    """Convert Java DateTimeFormatter pattern to Python strptime format.

    Uses regex tokenization to avoid overlapping-replacement issues with
    sequential str.replace().
    """
    def _replace_token(m):
        if m.group(1) is not None:
            return m.group(1)
        return _JAVA_TO_PYTHON_TOKENS[m.group(2)]

    # Replace known tokens; leave unmatched characters (literals like '-', ' ', 'T') as-is
    result = _JAVA_TOKEN_RE.sub(_replace_token, java_fmt)
    return result


class PartitionTimeExtractor:
    """
    Extracts a datetime from partition key values.

    Supports two modes:
    1. No pattern: uses the first partition value directly as a timestamp string.
    2. With pattern: replaces $<key> placeholders with partition values, then parses.

    The optional ``formatter`` is a strptime-compatible format string used to parse
    the resulting timestamp string.
    """

    def __init__(self, pattern: Optional[str] = None, formatter: Optional[str] = None):
        self._pattern = pattern
        self._formatter = formatter

    def extract(self, partition_keys: List[str], partition_values: List[object]) -> datetime:
        """
        Extract a datetime from the given partition keys and values.

        Args:
            partition_keys: Ordered list of partition column names.
            partition_values: Ordered list of partition column values (stringifiable).

        Returns:
            The extracted datetime.

        Raises:
            ValueError: If the timestamp string cannot be parsed.
        """
        timestamp_string = self._build_timestamp_string(partition_keys, partition_values)
        return self._to_local_datetime(timestamp_string)

    def extract_from_spec(self, spec: Dict[str, str]) -> datetime:
        """
        Extract a datetime from a partition spec dictionary.

        Args:
            spec: Partition specification {key: value}.

        Returns:
            The extracted datetime.
        """
        keys = list(spec.keys())
        values = list(spec.values())
        return self.extract(keys, values)

    def _build_timestamp_string(self, partition_keys: List[str], partition_values: List[object]) -> str:
        if self._pattern is None:
            return str(partition_values[0])
        result = self._pattern
        for i, key in enumerate(partition_keys):
            result = re.sub(r'\$' + re.escape(key), str(partition_values[i]), result)
        return result

    def _to_local_datetime(self, timestamp_string: str) -> datetime:
        if self._formatter is not None:
            py_format = _java_to_python_format(self._formatter)
            return self._parse_with_formatter(timestamp_string, py_format)
        return self._parse_default(timestamp_string)

    @staticmethod
    def _parse_with_formatter(timestamp_string: str, formatter: str) -> datetime:
        """Parse using the converted Python strptime pattern.

        Mirrors Java behavior: tries LocalDateTime.parse first, then falls back
        to LocalDate.parse (date-only) with the same formatter pattern.
        """
        try:
            return datetime.strptime(timestamp_string, formatter)
        except ValueError:
            # Fallback: strip time directives (%H, %M, %S, %f) and surrounding
            # literals to get a date-only format, mirroring Java's LocalDate.parse fallback.
            date_only_format = re.split(r'(?=%H|%M|%S|%f)', formatter)[0].rstrip()
            # Also strip any trailing non-directive separators (e.g. trailing space or 'T')
            date_only_format = date_only_format.rstrip(' T-:')
            if date_only_format and date_only_format != formatter:
                parsed_date = datetime.strptime(timestamp_string, date_only_format).date()
                return datetime.combine(parsed_date, dt_time.min)
            raise

    @staticmethod
    def _parse_default(timestamp_string: str) -> datetime:
        """Parse using default formats (lenient)."""
        for fmt in _DEFAULT_TIMESTAMP_FORMATS:
            try:
                return datetime.strptime(timestamp_string, fmt)
            except ValueError:
                continue
        raise ValueError(
            f"Cannot parse timestamp string '{timestamp_string}' with any default format. "
            f"Tried: {_DEFAULT_TIMESTAMP_FORMATS}"
        )
