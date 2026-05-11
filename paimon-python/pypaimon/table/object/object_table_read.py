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

from typing import List, Optional
from urllib.parse import urlparse

import pandas
import pyarrow
import pyarrow.fs as pafs

from pypaimon.table.object.object_split import ObjectSplit

# PyArrow schema matching Java ObjectTable.SCHEMA
OBJECT_TABLE_ARROW_SCHEMA = pyarrow.schema([
    pyarrow.field("path", pyarrow.string(), nullable=False),
    pyarrow.field("name", pyarrow.string(), nullable=False),
    pyarrow.field("length", pyarrow.int64(), nullable=False),
    pyarrow.field("mtime", pyarrow.int64(), nullable=False),
    pyarrow.field("atime", pyarrow.int64(), nullable=False),
    pyarrow.field("owner", pyarrow.string(), nullable=True),
])


def _normalize_path(path_str: str) -> str:
    """Extract the filesystem path from a URI or plain path."""
    parsed = urlparse(path_str)
    if parsed.scheme:
        return parsed.path
    return path_str


def _list_files_recursive(file_io, directory: str) -> list:
    """Recursively list all files under a directory using FileIO.

    Returns a list of (file_path, file_info) tuples.
    """
    results = []
    try:
        infos = file_io.list_status(directory)
    except Exception:
        return results

    directory_rstrip = directory.rstrip("/")
    for info in infos:
        file_name = info.path.split("/")[-1] if "/" in info.path else info.path
        full_path = info.path
        if not full_path.startswith("/") and "://" not in full_path:
            full_path = "{}/{}".format(directory_rstrip, file_name)

        if info.type == pafs.FileType.Directory:
            sub_results = _list_files_recursive(file_io, full_path)
            results.extend(sub_results)
        elif info.type == pafs.FileType.File:
            results.append((full_path, info))

    return results


def _compute_relative_path(prefix: str, file_path: str) -> str:
    """Compute relative path by removing the location prefix.

    Matches Java ObjectTableImpl.toRow() logic.
    """
    norm_prefix = _normalize_path(prefix).rstrip("/")
    norm_file = _normalize_path(file_path)

    if norm_file.startswith(norm_prefix):
        relative = norm_file[len(norm_prefix):]
        if relative.startswith("/"):
            relative = relative[1:]
        return relative

    return norm_file.split("/")[-1] if "/" in norm_file else norm_file


class ObjectTableRead:
    """Read implementation for ObjectTable.

    Recursively lists all files under the location directory and returns
    their metadata (path, name, length, mtime, atime, owner) as a PyArrow Table.
    """

    def __init__(
        self,
        table,
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ):
        self.table = table
        self.projection = projection
        self.limit = limit

    def to_arrow(self, splits: List[ObjectSplit]) -> pyarrow.Table:
        paths = []
        names = []
        lengths = []
        mtimes = []
        atimes = []
        owners = []

        for split in splits:
            file_entries = _list_files_recursive(split.file_io, split.location)
            for file_path, file_info in file_entries:
                if self.limit is not None and len(paths) >= self.limit:
                    break

                relative_path = _compute_relative_path(split.location, file_path)
                file_name = file_path.split("/")[-1] if "/" in file_path else file_path
                file_size = getattr(file_info, "size", 0) or 0
                modification_time = getattr(file_info, "mtime_ns", 0) or 0
                if modification_time > 0:
                    modification_time = modification_time // 1_000_000
                access_time = 0
                owner = None

                paths.append(relative_path)
                names.append(file_name)
                lengths.append(file_size)
                mtimes.append(modification_time)
                atimes.append(access_time)
                owners.append(owner)

            if self.limit is not None and len(paths) >= self.limit:
                break

        result = pyarrow.table(
            {
                "path": pyarrow.array(paths, type=pyarrow.string()),
                "name": pyarrow.array(names, type=pyarrow.string()),
                "length": pyarrow.array(lengths, type=pyarrow.int64()),
                "mtime": pyarrow.array(mtimes, type=pyarrow.int64()),
                "atime": pyarrow.array(atimes, type=pyarrow.int64()),
                "owner": pyarrow.array(owners, type=pyarrow.string()),
            },
            schema=OBJECT_TABLE_ARROW_SCHEMA,
        )

        if self.projection:
            existing = [c for c in self.projection if c in result.column_names]
            if existing:
                result = result.select(existing)

        return result

    def to_pandas(self, splits: List[ObjectSplit]) -> pandas.DataFrame:
        return self.to_arrow(splits).to_pandas()
