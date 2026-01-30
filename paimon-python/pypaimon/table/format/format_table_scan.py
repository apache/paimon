# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Dict, List, Optional

import pyarrow.fs as pafs

from pypaimon.common.file_io import FileIO
from pypaimon.read.plan import Plan
from pypaimon.table.format.format_data_split import FormatDataSplit
from pypaimon.table.format.format_table import FormatTable


def _is_data_file_name(name: str) -> bool:
    """Match Java FormatTableScan.isDataFileName: exclude hidden/metadata."""
    if name is None:
        return False
    return not name.startswith(".") and not name.startswith("_")


def _is_reserved_dir_name(name: str) -> bool:
    """Skip metadata/reserved dirs (not treated as partition levels)."""
    if not name:
        return True
    if name.startswith(".") or name.startswith("_"):
        return True
    if name.lower() in ("schema", "_schema"):
        return True
    return False


def _list_data_files_recursive(
    file_io: FileIO,
    path: str,
    partition_keys: List[str],
    partition_only_value: bool,
    rel_path_parts: Optional[List[str]] = None,
) -> List[FormatDataSplit]:
    """List data files under path, building partition spec from dir names."""
    splits: List[FormatDataSplit] = []
    rel_path_parts = rel_path_parts or []
    try:
        infos = file_io.list_status(path)
    except Exception:
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
            if _is_reserved_dir_name(name):
                continue
            part_value = name
            if not partition_only_value and "=" in name:
                part_value = name.split("=", 1)[1]
            child_parts = rel_path_parts + [part_value]
            if len(child_parts) <= len(partition_keys):
                sub_splits = _list_data_files_recursive(
                    file_io,
                    full_path,
                    partition_keys,
                    partition_only_value,
                    child_parts,
                )
                splits.extend(sub_splits)
        elif info.type == pafs.FileType.File and _is_data_file_name(name):
            size = getattr(info, "size", None) or 0
            part_spec: Optional[Dict[str, str]] = None
            if partition_keys and len(rel_path_parts) >= len(partition_keys):
                part_spec = dict(
                    zip(
                        partition_keys,
                        rel_path_parts[: len(partition_keys)],
                    )
                )
            splits.append(
                FormatDataSplit(
                    file_path=full_path,
                    file_size=size if size is not None else 0,
                    partition=part_spec,
                )
            )
    return splits


class FormatTableScan:

    def __init__(
        self,
        table: FormatTable,
        partition_filter: Optional[Dict[str, str]] = None,
        limit: Optional[int] = None,
    ):
        self.table = table
        self.partition_filter = partition_filter  # optional equality filter
        self.limit = limit

    def plan(self) -> Plan:
        partition_only_value = self.table.options().get(
            "format-table.partition-path-only-value", "false"
        ).lower() == "true"
        splits = _list_data_files_recursive(
            self.table.file_io,
            self.table.location(),
            self.table.partition_keys,
            partition_only_value,
        )
        if self.partition_filter:
            filtered = []
            for s in splits:
                match = s.partition and all(
                    s.partition.get(k) == v
                    for k, v in self.partition_filter.items()
                )
                if match:
                    filtered.append(s)
            splits = filtered
        if self.limit is not None and self.limit <= 0:
            splits = []
        elif self.limit is not None and len(splits) > self.limit:
            splits = splits[: self.limit]
        return Plan(_splits=splits)
