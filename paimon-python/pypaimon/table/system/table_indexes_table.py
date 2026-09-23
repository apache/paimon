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

"""The ``$table_indexes`` system table."""

from typing import Dict, List, Optional

import pyarrow

from pypaimon.index.index_file_handler import IndexFileHandler
from pypaimon.schema.data_types import (ArrayType, AtomicType, DataField,
                                        RowType)
from pypaimon.table.system.files_table import _render_partition
from pypaimon.table.system.system_table import SystemTable


_DV_RANGE_ROW_TYPE = RowType(False, [
    DataField(0, "f0", AtomicType("STRING", nullable=False)),
    DataField(1, "f1", AtomicType("INT", nullable=False)),
    DataField(2, "f2", AtomicType("INT", nullable=False)),
    DataField(3, "_CARDINALITY", AtomicType("BIGINT", nullable=True)),
])
_DV_RANGES_TYPE = pyarrow.list_(pyarrow.struct([
    pyarrow.field("f0", pyarrow.string(), nullable=False),
    pyarrow.field("f1", pyarrow.int32(), nullable=False),
    pyarrow.field("f2", pyarrow.int32(), nullable=False),
    pyarrow.field("_CARDINALITY", pyarrow.int64(), nullable=True),
]))

TABLE_TYPE = RowType(False, [
    DataField(0, "partition", AtomicType("STRING", nullable=True)),
    DataField(1, "bucket", AtomicType("INT", nullable=False)),
    DataField(2, "index_type", AtomicType("STRING", nullable=False)),
    DataField(3, "file_name", AtomicType("STRING", nullable=False)),
    DataField(4, "file_size", AtomicType("BIGINT", nullable=False)),
    DataField(5, "row_count", AtomicType("BIGINT", nullable=False)),
    DataField(
        6,
        "dv_ranges",
        ArrayType(nullable=True,
                  element_type=_DV_RANGE_ROW_TYPE)),
    DataField(7, "row_range_start", AtomicType("BIGINT", nullable=True)),
    DataField(8, "row_range_end", AtomicType("BIGINT", nullable=True)),
    DataField(9, "index_field_id", AtomicType("INT", nullable=True)),
    DataField(10, "index_field_name", AtomicType("STRING", nullable=True)),
])


class TableIndexesTable(SystemTable):
    """The ``$table_indexes`` system table."""

    def system_table_name(self) -> str:
        return "table_indexes"

    def row_type(self) -> RowType:
        return TABLE_TYPE

    def primary_keys(self) -> List[str]:
        return ["file_name"]

    def _build_arrow_table(self) -> pyarrow.Table:
        snapshot = self.base_table.snapshot_manager().get_latest_snapshot()
        entries = IndexFileHandler(self.base_table).scan(snapshot)

        rows = {
            "partition": [],
            "bucket": [],
            "index_type": [],
            "file_name": [],
            "file_size": [],
            "row_count": [],
            "dv_ranges": [],
            "row_range_start": [],
            "row_range_end": [],
            "index_field_id": [],
            "index_field_name": [],
        }

        for entry in entries:
            index_file = entry.index_file
            global_meta = index_file.global_index_meta
            rows["partition"].append(_render_partition(entry.partition))
            rows["bucket"].append(int(entry.bucket))
            rows["index_type"].append(index_file.index_type)
            rows["file_name"].append(index_file.file_name)
            rows["file_size"].append(int(index_file.file_size))
            rows["row_count"].append(int(index_file.row_count))
            rows["dv_ranges"].append(_render_dv_ranges(index_file.dv_ranges))
            rows["row_range_start"].append(
                None if global_meta is None
                else int(global_meta.row_range_start))
            rows["row_range_end"].append(
                None if global_meta is None
                else int(global_meta.row_range_end))
            rows["index_field_id"].append(
                None if global_meta is None
                else int(global_meta.index_field_id))
            rows["index_field_name"].append(
                None if global_meta is None
                else _index_field_names(self.base_table, global_meta))

        return pyarrow.table({
            "partition": pyarrow.array(
                rows["partition"], type=pyarrow.string()),
            "bucket": pyarrow.array(rows["bucket"], type=pyarrow.int32()),
            "index_type": pyarrow.array(
                rows["index_type"], type=pyarrow.string()),
            "file_name": pyarrow.array(
                rows["file_name"], type=pyarrow.string()),
            "file_size": pyarrow.array(
                rows["file_size"], type=pyarrow.int64()),
            "row_count": pyarrow.array(
                rows["row_count"], type=pyarrow.int64()),
            "dv_ranges": pyarrow.array(
                rows["dv_ranges"], type=_DV_RANGES_TYPE),
            "row_range_start": pyarrow.array(
                rows["row_range_start"], type=pyarrow.int64()),
            "row_range_end": pyarrow.array(
                rows["row_range_end"], type=pyarrow.int64()),
            "index_field_id": pyarrow.array(
                rows["index_field_id"], type=pyarrow.int32()),
            "index_field_name": pyarrow.array(
                rows["index_field_name"], type=pyarrow.string()),
        })


def _render_dv_ranges(dv_ranges) -> Optional[List[Dict[str, object]]]:
    if not dv_ranges:
        return None
    rendered = []
    for meta in dv_ranges.values():
        rendered.append({
            "f0": meta.data_file_name,
            "f1": int(meta.offset),
            "f2": int(meta.length),
            "_CARDINALITY": (
                None if meta.cardinality is None else int(meta.cardinality)
            ),
        })
    return rendered


def _index_field_names(table, global_meta) -> Optional[str]:
    field_by_id = {field.id: field.name for field in table.fields}
    field_ids = [global_meta.index_field_id]
    if global_meta.extra_field_ids:
        field_ids.extend(global_meta.extra_field_ids)

    names = []
    for field_id in field_ids:
        name = field_by_id.get(field_id)
        if name is None:
            return None
        names.append(name)
    return ",".join(names)
