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
# limitations under the License.
################################################################################

"""Daft version compatibility checks."""

from __future__ import annotations

from importlib.metadata import version

_daft_version: tuple[int, ...] | None = None


def _parse_daft_version() -> tuple[int, ...]:
    global _daft_version
    if _daft_version is not None:
        return _daft_version
    raw = version("daft")
    parts = []
    for seg in raw.split(".")[:3]:
        digits = ""
        for c in seg:
            if c.isdigit():
                digits += c
            else:
                break
        if digits:
            parts.append(int(digits))
    _daft_version = tuple(parts)
    return _daft_version


def has_file_range_reads() -> bool:
    """True if the installed Daft supports File offset/length (>= 0.7.11)."""
    return _parse_daft_version() >= (0, 7, 11)


def require_file_range_reads(feature: str = "BLOB columns") -> None:
    """Raise if Daft is too old for File offset/length support (requires >= 0.7.11)."""
    if not has_file_range_reads():
        v = ".".join(str(x) for x in _parse_daft_version())
        raise NotImplementedError(
            f"{feature} require daft >= 0.7.11 (File offset/length support), "
            f"but found {v}. "
            f"Please upgrade: pip install 'daft>=0.7.11'"
        )
