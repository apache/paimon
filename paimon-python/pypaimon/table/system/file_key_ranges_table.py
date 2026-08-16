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

"""The ``$file_key_ranges`` system table."""

from typing import List

import pyarrow

from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.schema.data_types import AtomicType, DataField, RowType
from pypaimon.table.system.files_table import (
    _render_key,
    _render_partition,
    _stringify_path,
)
from pypaimon.table.system.system_table import SystemTable


TABLE_TYPE = RowType(False, [
    DataField(0, "partition", AtomicType("STRING", nullable=True)),
    DataField(1, "bucket", AtomicType("INT", nullable=False)),
    DataField(2, "file_path", AtomicType("STRING", nullable=False)),
    DataField(3, "file_format", AtomicType("STRING", nullable=False)),
    DataField(4, "schema_id", AtomicType("BIGINT", nullable=False)),
    DataField(5, "level", AtomicType("INT", nullable=False)),
    DataField(6, "record_count", AtomicType("BIGINT", nullable=False)),
    DataField(7, "file_size_in_bytes", AtomicType("BIGINT", nullable=False)),
    DataField(8, "min_key", AtomicType("STRING", nullable=True)),
    DataField(9, "max_key", AtomicType("STRING", nullable=True)),
    DataField(10, "first_row_id", AtomicType("BIGINT", nullable=True)),
])


class FileKeyRangesTable(SystemTable):
    """The ``$file_key_ranges`` system table."""

    def system_table_name(self) -> str:
        return "file_key_ranges"

    def row_type(self) -> RowType:
        return TABLE_TYPE

    def primary_keys(self) -> List[str]:
        return ["file_path"]

    def _build_arrow_table(self) -> pyarrow.Table:
        snapshot = self.base_table.snapshot_manager().get_latest_snapshot()
        if snapshot is None:
            return _empty_table()

        manifest_list_manager = ManifestListManager(self.base_table)
        manifest_files = manifest_list_manager.read_all(snapshot)
        manifest_file_manager = ManifestFileManager(self.base_table)
        entries = manifest_file_manager.read_entries_parallel(
            manifest_files, drop_stats=False)

        file_format = self.base_table.options.file_format()
        path_factory = self.base_table.path_factory()
        rows = {
            "partition": [],
            "bucket": [],
            "file_path": [],
            "file_format": [],
            "schema_id": [],
            "level": [],
            "record_count": [],
            "file_size_in_bytes": [],
            "min_key": [],
            "max_key": [],
            "first_row_id": [],
        }

        for entry in entries:
            meta = entry.file
            bucket_path = path_factory.bucket_path(
                tuple(entry.partition.values), int(entry.bucket))
            rows["partition"].append(_render_partition(entry.partition))
            rows["bucket"].append(int(entry.bucket))
            rows["file_path"].append(_stringify_path(
                meta.external_path
                or meta.file_path
                or "%s/%s" % (bucket_path, meta.file_name)))
            rows["file_format"].append(file_format)
            rows["schema_id"].append(int(meta.schema_id))
            rows["level"].append(int(meta.level))
            rows["record_count"].append(int(meta.row_count))
            rows["file_size_in_bytes"].append(int(meta.file_size))
            rows["min_key"].append(_render_key(meta.min_key))
            rows["max_key"].append(_render_key(meta.max_key))
            rows["first_row_id"].append(
                None if meta.first_row_id is None
                else int(meta.first_row_id))

        return pyarrow.table({
            "partition": pyarrow.array(
                rows["partition"], type=pyarrow.string()),
            "bucket": pyarrow.array(rows["bucket"], type=pyarrow.int32()),
            "file_path": pyarrow.array(
                rows["file_path"], type=pyarrow.string()),
            "file_format": pyarrow.array(
                rows["file_format"], type=pyarrow.string()),
            "schema_id": pyarrow.array(
                rows["schema_id"], type=pyarrow.int64()),
            "level": pyarrow.array(rows["level"], type=pyarrow.int32()),
            "record_count": pyarrow.array(
                rows["record_count"], type=pyarrow.int64()),
            "file_size_in_bytes": pyarrow.array(
                rows["file_size_in_bytes"], type=pyarrow.int64()),
            "min_key": pyarrow.array(rows["min_key"], type=pyarrow.string()),
            "max_key": pyarrow.array(rows["max_key"], type=pyarrow.string()),
            "first_row_id": pyarrow.array(
                rows["first_row_id"], type=pyarrow.int64()),
        })


def _empty_table() -> pyarrow.Table:
    return pyarrow.table({
        "partition": pyarrow.array([], type=pyarrow.string()),
        "bucket": pyarrow.array([], type=pyarrow.int32()),
        "file_path": pyarrow.array([], type=pyarrow.string()),
        "file_format": pyarrow.array([], type=pyarrow.string()),
        "schema_id": pyarrow.array([], type=pyarrow.int64()),
        "level": pyarrow.array([], type=pyarrow.int32()),
        "record_count": pyarrow.array([], type=pyarrow.int64()),
        "file_size_in_bytes": pyarrow.array([], type=pyarrow.int64()),
        "min_key": pyarrow.array([], type=pyarrow.string()),
        "max_key": pyarrow.array([], type=pyarrow.string()),
        "first_row_id": pyarrow.array([], type=pyarrow.int64()),
    })
