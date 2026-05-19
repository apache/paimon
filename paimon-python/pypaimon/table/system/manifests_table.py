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

"""The ``$manifests`` system table — manifest list for the latest snapshot."""

from typing import List, Optional

import pyarrow

from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.schema.data_types import AtomicType, DataField, RowType
from pypaimon.table.system.system_table import SystemTable


TABLE_TYPE = RowType(False, [
    DataField(0, "file_name", AtomicType("STRING", nullable=False)),
    DataField(1, "file_size", AtomicType("BIGINT", nullable=False)),
    DataField(2, "num_added_files", AtomicType("BIGINT", nullable=False)),
    DataField(3, "num_deleted_files", AtomicType("BIGINT", nullable=False)),
    DataField(4, "schema_id", AtomicType("BIGINT", nullable=False)),
    DataField(5, "min_partition_stats", AtomicType("STRING", nullable=True)),
    DataField(6, "max_partition_stats", AtomicType("STRING", nullable=True)),
    DataField(7, "min_row_id", AtomicType("BIGINT", nullable=True)),
    DataField(8, "max_row_id", AtomicType("BIGINT", nullable=True)),
])


class ManifestsTable(SystemTable):
    """The ``$manifests`` system table for the latest snapshot."""

    def system_table_name(self) -> str:
        return "manifests"

    def row_type(self) -> RowType:
        return TABLE_TYPE

    def primary_keys(self) -> List[str]:
        return ["file_name"]

    def _build_arrow_table(self) -> pyarrow.Table:
        snapshot = self.base_table.snapshot_manager().get_latest_snapshot()
        if snapshot is None:
            return self._empty_table()

        manifest_list_manager = ManifestListManager(self.base_table)
        manifests = manifest_list_manager.read_all(snapshot)

        file_names: List[str] = []
        file_sizes: List[int] = []
        num_added: List[int] = []
        num_deleted: List[int] = []
        schema_ids: List[int] = []
        min_partition_stats: List[Optional[str]] = []
        max_partition_stats: List[Optional[str]] = []
        min_row_ids: List[Optional[int]] = []
        max_row_ids: List[Optional[int]] = []

        for meta in manifests:
            file_names.append(meta.file_name)
            file_sizes.append(int(meta.file_size))
            num_added.append(int(meta.num_added_files))
            num_deleted.append(int(meta.num_deleted_files))
            schema_ids.append(int(meta.schema_id))
            # TODO: render min/max_partition_stats by casting partition
            # rows to their string form. pypaimon
            # has SimpleStats but no shared partition-row-to-string
            # helper yet; emit NULL to preserve the column shape.
            min_partition_stats.append(None)
            max_partition_stats.append(None)
            min_row_ids.append(
                None if meta.min_row_id is None else int(meta.min_row_id))
            max_row_ids.append(
                None if meta.max_row_id is None else int(meta.max_row_id))

        return pyarrow.table({
            "file_name": pyarrow.array(file_names, type=pyarrow.string()),
            "file_size": pyarrow.array(file_sizes, type=pyarrow.int64()),
            "num_added_files": pyarrow.array(num_added, type=pyarrow.int64()),
            "num_deleted_files": pyarrow.array(num_deleted, type=pyarrow.int64()),
            "schema_id": pyarrow.array(schema_ids, type=pyarrow.int64()),
            "min_partition_stats": pyarrow.array(
                min_partition_stats, type=pyarrow.string()),
            "max_partition_stats": pyarrow.array(
                max_partition_stats, type=pyarrow.string()),
            "min_row_id": pyarrow.array(min_row_ids, type=pyarrow.int64()),
            "max_row_id": pyarrow.array(max_row_ids, type=pyarrow.int64()),
        })

    @staticmethod
    def _empty_table() -> pyarrow.Table:
        return pyarrow.table({
            "file_name": pyarrow.array([], type=pyarrow.string()),
            "file_size": pyarrow.array([], type=pyarrow.int64()),
            "num_added_files": pyarrow.array([], type=pyarrow.int64()),
            "num_deleted_files": pyarrow.array([], type=pyarrow.int64()),
            "schema_id": pyarrow.array([], type=pyarrow.int64()),
            "min_partition_stats": pyarrow.array([], type=pyarrow.string()),
            "max_partition_stats": pyarrow.array([], type=pyarrow.string()),
            "min_row_id": pyarrow.array([], type=pyarrow.int64()),
            "max_row_id": pyarrow.array([], type=pyarrow.int64()),
        })
