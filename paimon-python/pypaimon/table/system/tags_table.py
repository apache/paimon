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

"""The ``$tags`` system table — every tag plus its snapshot metadata."""

from typing import List, Optional

import pyarrow

from pypaimon.schema.data_types import AtomicType, DataField, RowType
from pypaimon.table.system.system_table import SystemTable


TABLE_TYPE = RowType(False, [
    DataField(0, "tag_name", AtomicType("STRING", nullable=False)),
    DataField(1, "snapshot_id", AtomicType("BIGINT", nullable=False)),
    DataField(2, "schema_id", AtomicType("BIGINT", nullable=False)),
    DataField(3, "commit_time", AtomicType("TIMESTAMP(3)", nullable=False)),
    DataField(4, "record_count", AtomicType("BIGINT", nullable=True)),
    DataField(5, "create_time", AtomicType("TIMESTAMP(3)", nullable=True)),
    DataField(6, "time_retained", AtomicType("STRING", nullable=True)),
])


_TIMESTAMP_TYPE = pyarrow.timestamp("ms")


class TagsTable(SystemTable):
    """The ``$tags`` system table."""

    def system_table_name(self) -> str:
        return "tags"

    def row_type(self) -> RowType:
        return TABLE_TYPE

    def primary_keys(self) -> List[str]:
        return ["tag_name"]

    def _build_arrow_table(self) -> pyarrow.Table:
        tag_manager = self.base_table.tag_manager()

        names: List[str] = []
        snapshot_ids: List[int] = []
        schema_ids: List[int] = []
        commit_times: List[int] = []
        record_counts: List[Optional[int]] = []
        create_times: List[Optional[int]] = []
        time_retained: List[Optional[str]] = []

        for name in tag_manager.list_tags():
            tag = tag_manager.get(name)
            if tag is None:
                continue
            names.append(name)
            snapshot_ids.append(int(tag.id))
            schema_ids.append(int(tag.schema_id))
            commit_times.append(int(tag.time_millis))
            record_counts.append(
                None if tag.total_record_count is None
                else int(tag.total_record_count))
            # TODO: surface create_time and time_retained once the Tag
            # dataclass carries them.
            create_times.append(None)
            time_retained.append(None)

        return pyarrow.table({
            "tag_name": pyarrow.array(names, type=pyarrow.string()),
            "snapshot_id": pyarrow.array(snapshot_ids, type=pyarrow.int64()),
            "schema_id": pyarrow.array(schema_ids, type=pyarrow.int64()),
            "commit_time": pyarrow.array(commit_times, type=_TIMESTAMP_TYPE),
            "record_count": pyarrow.array(record_counts, type=pyarrow.int64()),
            "create_time": pyarrow.array(create_times, type=_TIMESTAMP_TYPE),
            "time_retained": pyarrow.array(time_retained, type=pyarrow.string()),
        })
