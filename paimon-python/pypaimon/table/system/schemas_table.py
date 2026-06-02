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

"""The ``$schemas`` system table — every committed table schema."""

import json
from typing import Any, List, Optional

import pyarrow

from pypaimon.schema.data_types import AtomicType, DataField, RowType
from pypaimon.table.system.system_table import SystemTable


TABLE_TYPE = RowType(False, [
    DataField(0, "schema_id", AtomicType("BIGINT", nullable=False)),
    DataField(1, "fields", AtomicType("STRING", nullable=False)),
    DataField(2, "partition_keys", AtomicType("STRING", nullable=False)),
    DataField(3, "primary_keys", AtomicType("STRING", nullable=False)),
    DataField(4, "options", AtomicType("STRING", nullable=False)),
    DataField(5, "comment", AtomicType("STRING", nullable=True)),
    DataField(6, "update_time", AtomicType("TIMESTAMP(3)", nullable=False)),
])


_TIMESTAMP_TYPE = pyarrow.timestamp("ms")


def _to_json(value: Any) -> str:
    """Compact JSON encoding for the four serialised columns.

    Field ordering follows the source object (insertion order for dicts
    and lists is preserved by ``json.dumps``); bit-exact equality with
    any external encoder is not promised — callers compare decoded
    structures, not raw strings.
    """
    return json.dumps(value, separators=(',', ':'), ensure_ascii=False)


class SchemasTable(SystemTable):
    """The ``$schemas`` system table."""

    def system_table_name(self) -> str:
        return "schemas"

    def row_type(self) -> RowType:
        return TABLE_TYPE

    def primary_keys(self) -> List[str]:
        return ["schema_id"]

    def _build_arrow_table(self) -> pyarrow.Table:
        schemas = self.base_table.schema_manager.list_all()

        schema_ids: List[int] = []
        fields_jsons: List[str] = []
        partition_keys_jsons: List[str] = []
        primary_keys_jsons: List[str] = []
        options_jsons: List[str] = []
        comments: List[Optional[str]] = []
        update_times: List[int] = []

        for schema in schemas:
            schema_ids.append(int(schema.id))
            fields_jsons.append(
                _to_json([field.to_dict() for field in schema.fields]))
            partition_keys_jsons.append(_to_json(list(schema.partition_keys)))
            primary_keys_jsons.append(_to_json(list(schema.primary_keys)))
            options_jsons.append(_to_json(dict(schema.options)))
            comments.append(schema.comment)
            update_times.append(int(schema.time_millis))

        return pyarrow.table({
            "schema_id": pyarrow.array(schema_ids, type=pyarrow.int64()),
            "fields": pyarrow.array(fields_jsons, type=pyarrow.string()),
            "partition_keys": pyarrow.array(
                partition_keys_jsons, type=pyarrow.string()),
            "primary_keys": pyarrow.array(
                primary_keys_jsons, type=pyarrow.string()),
            "options": pyarrow.array(options_jsons, type=pyarrow.string()),
            "comment": pyarrow.array(comments, type=pyarrow.string()),
            "update_time": pyarrow.array(update_times, type=_TIMESTAMP_TYPE),
        })
