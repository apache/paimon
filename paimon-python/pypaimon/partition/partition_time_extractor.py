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

# Java DateTimeFormatter → Python strptime conversion (order matters: longest first)
_JAVA_TO_PYTHON_PATTERNS = [
    ("yyyy", "%Y"),
    ("yy", "%y"),
    ("MM", "%m"),
    ("dd", "%d"),
    ("HH", "%H"),
    ("mm", "%M"),
    ("SSS", "%f"),
    ("SS", "%f"),
    ("ss", "%S"),
]


def _java_to_python_format(java_fmt: str) -> str:
    """Convert Java DateTimeFormatter pattern to Python strptime format.

    Handles:
    - Standard tokens: yyyy, MM, dd, HH, mm, ss, SSS
    - Quoted literals: 'T' → T (strips single quotes)
    """
    import re
    # Strip Java quoted literals: 'T' → T, '' → '
    result = re.sub(r"'([^']*)'", r"\1", java_fmt)
    for java_pat, py_pat in _JAVA_TO_PYTHON_PATTERNS:
        result = result.replace(java_pat, py_pat)
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
        """Parse using the converted Python strptime pattern."""
        try:
            return datetime.strptime(timestamp_string, formatter)
        except ValueError:
            parsed_date = datetime.strptime(timestamp_string, formatter).date()
            return datetime.combine(parsed_date, dt_time.min)

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
