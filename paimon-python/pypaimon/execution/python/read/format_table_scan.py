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

import math
import re
import struct
from dataclasses import replace
from typing import Dict, List, Optional
from unicodedata import normalize

import pyarrow
import pyarrow.fs as pafs

from pypaimon.common.file_io import FileIO
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.execution.python.write.partition_spec import _partition_spec_value, _UNREPRESENTABLE_PARTITION_VALUE
from pypaimon.read.plan import Plan
from pypaimon.read.push_down_utils import _NULL_PARTITION_LITERAL
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.table.format.format_data_split import FormatDataSplit
from pypaimon.utils.file_store_path_factory import _escape_partition_component


def _unescape_path_name(path: str) -> str:
    result = []
    i = 0
    while i < len(path):
        if path[i] == '%' and i + 2 < len(path):
            digits = path[i + 1:i + 3]
            if re.fullmatch(r'[+-]?[\da-fA-FＡ-Ｆａ-ｆ]+', digits) and all(ord(c) <= 0xffff for c in digits):
                code = int(normalize('NFKC', digits), 16)
                if code >= 0:
                    result.append(chr(code))
                    i += 3
                    continue
        result.append(path[i])
        i += 1
    return ''.join(result)


def _is_data_file_name(name: str) -> bool:
    if name is None:
        return False
    return not name.startswith(".") and not name.startswith("_")


def _is_reserved_dir_name(name: str) -> bool:
    if not name:
        return True
    if name.startswith(".") or name.startswith("_"):
        return True
    return False


def _list_data_files_recursive(
    file_io: FileIO,
    path: str,
    partition_keys: List[str],
    partition_only_value: bool,
    default_part_value: str,
    rel_path_parts: Optional[List[str]] = None,
    raw_path_parts: Optional[List[str]] = None,
    legacy_directory: bool = False,
) -> List[FormatDataSplit]:
    splits: List[FormatDataSplit] = []
    rel_path_parts = rel_path_parts or []
    raw_path_parts = raw_path_parts or []
    try:
        infos = file_io.list_status(path)
    except FileNotFoundError:
        # The directory vanished after its parent listed it.
        return splits
    if not infos:
        return splits
    path_rstrip = path.rstrip("/")
    for info in infos:
        name = info.path.split("/")[-1] if "/" in info.path else info.path
        full_path = f"{path_rstrip}/{name}" if path_rstrip else name
        if info.path.startswith("/") or info.path.startswith("file:"):
            full_path = info.path
        if info.type == pafs.FileType.Directory:
            if len(rel_path_parts) >= len(partition_keys):
                continue
            if _is_reserved_dir_name(name) and not (
                    partition_only_value and name == _escape_partition_component(default_part_value)):
                continue
            raw_value = name
            legacy = legacy_directory
            if not partition_only_value:
                if '=' not in name:
                    continue
                key, raw_value = name.rsplit('=', 1)
                partition_key = partition_keys[len(rel_path_parts)]
                if _unescape_path_name(key) != partition_key or not raw_value:
                    # Earlier versions did not escape values, so pt=a=b holds the value 'a=b', and pt= the
                    # value ''. Java skips such a directory, and this version never writes one.
                    if not name.startswith(partition_key + '='):
                        continue
                    raw_value = name[len(partition_key) + 1:]
                    legacy = True
            part_value = raw_value if legacy else _unescape_path_name(raw_value)
            if not legacy and part_value == default_part_value:
                part_value = None
            splits.extend(_list_data_files_recursive(
                file_io,
                full_path,
                partition_keys,
                partition_only_value,
                default_part_value,
                rel_path_parts + [part_value],
                raw_path_parts + [raw_value],
                legacy,
            ))
        elif info.type == pafs.FileType.File and _is_data_file_name(name):
            size = getattr(info, "size", None) or 0
            part_spec: Optional[Dict[str, str]] = None
            legacy_spec: Optional[Dict[str, str]] = None
            if partition_keys and len(rel_path_parts) >= len(partition_keys):
                part_spec = dict(zip(partition_keys, rel_path_parts))
                if legacy_directory or raw_path_parts != rel_path_parts:
                    legacy_spec = dict(zip(partition_keys, raw_path_parts))
            splits.append(
                FormatDataSplit(
                    file_path=full_path,
                    file_size=size,
                    partition=part_spec,
                    legacy_partition=legacy_spec,
                    legacy_directory=legacy_directory,
                )
            )
    return splits


def _partition_filter_value(value, data_type, legacy_ltz=False, directory=False):
    parsed = _partition_spec_value(value, data_type, legacy_ltz=legacy_ltz, directory=directory)
    if (parsed is _UNREPRESENTABLE_PARTITION_VALUE or parsed is _NULL_PARTITION_LITERAL
            or value is not None and parsed is None):
        raise ValueError(f"Partition value '{value}' cannot be represented as {data_type} in Python")
    scalar = pyarrow.scalar(parsed, type=PyarrowFieldParser.from_paimon_type(data_type))
    converted = scalar.as_py()
    floating = scalar.is_valid and pyarrow.types.is_floating(scalar.type)
    if converted != parsed and not (floating and math.isnan(converted) and math.isnan(parsed)):
        raise ValueError(f"Partition value '{value}' cannot be represented exactly as {data_type}")
    if floating:
        # Java Float/Double.compare distinguishes signed zero and equates NaNs.
        return (b'NaN' if math.isnan(converted)
                else struct.pack('>f' if scalar.type.bit_width == 32 else '>d', converted))
    return scalar


def _earlier_partition_text(value, data_type):
    """The name that earlier versions gave the directory of a value: str() of it, None for null."""
    parsed = _partition_spec_value(value, data_type)
    if parsed is None or parsed is _UNREPRESENTABLE_PARTITION_VALUE or parsed is _NULL_PARTITION_LITERAL:
        return None
    return str(pyarrow.scalar(parsed, type=PyarrowFieldParser.from_paimon_type(data_type)).as_py())


def plan(self) -> Plan:
    options = CoreOptions.from_dict(self.table.options())
    partition_only_value = self.table.options().get(
        "format-table.partition-path-only-value", "false"
    ).lower() == "true"
    splits = _list_data_files_recursive(
        self.table.file_io,
        self.table.location(),
        self.table.partition_keys,
        partition_only_value,
        options.options.get(CoreOptions.PARTITION_DEFAULT_NAME),
    )
    if self.partition_filter:
        types = {f.name: f.type for f in self.table.fields if f.name in self.table.partition_keys}
        try:
            values = {k: _partition_filter_value(v, types[k]) for k, v in self.partition_filter.items()}
        except (ValueError, TypeError, OverflowError):
            return Plan(_splits=[])
        earlier_texts = {k: _earlier_partition_text(v, types[k]) for k, v in self.partition_filter.items()}
        legacy_ltz = options.options.get(CoreOptions.PARTITION_GENERATE_LEGACY_NAME)
        # The files of a partition share its values, so each partition is matched once.
        matches = {}

        def match(partition):
            key = tuple(partition.get(k) for k in values)
            if key not in matches:
                try:
                    matches[key] = all(
                        _partition_filter_value(
                            partition.get(k), types[k], legacy_ltz=legacy_ltz, directory=True) == v
                        for k, v in values.items()
                    )
                except (ValueError, TypeError, OverflowError):
                    matches[key] = False
            return matches[key]

        filtered = []
        for s in splits:
            directory_matches = bool(s.partition) and not s.legacy_directory and match(s.partition)
            # Files of earlier versions keep their partition values, which can differ from the directory's: a
            # directory named by str() of the value, such as pt=a%3Ab for 'a%3Ab' or pt=inf for infinity, which
            # does not parse as this version's name, or above the partition depth, where earlier versions
            # stopped the path at a null value. The read keeps their rows by those values.
            raw = s.legacy_partition if s.legacy_partition is not None else s.partition
            earlier = not s.partition or all(
                text is not None and raw.get(k) == text for k, text in earlier_texts.items()) or (
                s.legacy_partition is not None and match(s.legacy_partition))
            if directory_matches or earlier:
                filtered.append(replace(s, row_filter=self.partition_filter, directory_matches=directory_matches))
        splits = filtered
    return Plan(_splits=splits)
