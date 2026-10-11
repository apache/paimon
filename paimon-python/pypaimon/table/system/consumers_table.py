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

"""The ``$consumers`` system table — streaming consumer progress."""

from typing import List

import pyarrow

from pypaimon.schema.data_types import AtomicType, DataField, RowType
from pypaimon.table.system.system_table import SystemTable


TABLE_TYPE = RowType(False, [
    DataField(0, "consumer_id", AtomicType("STRING", nullable=False)),
    DataField(1, "next_snapshot_id", AtomicType("BIGINT", nullable=False)),
])


class ConsumersTable(SystemTable):
    """The ``$consumers`` system table: one ``(consumer_id,
    next_snapshot_id)`` row per streaming consumer, so the consumption
    progress persisted under ``{table}/consumer/`` is queryable the same
    way Java's ``ConsumersTable`` exposes it (needed under a REST catalog,
    where the internal consumer API is otherwise the only view).
    """

    def system_table_name(self) -> str:
        return "consumers"

    def row_type(self) -> RowType:
        return TABLE_TYPE

    def primary_keys(self) -> List[str]:
        return ["consumer_id"]

    def _build_arrow_table(self) -> pyarrow.Table:
        # consumers() maps consumer_id -> next_snapshot; sort by id so the
        # output is deterministic. Build with explicit column types so an
        # empty table still carries the declared (string, int64) schema
        # rather than pyarrow's null-type inference.
        consumers = self.base_table.consumer_manager().consumers()
        consumer_ids = sorted(consumers.keys())
        next_snapshot_ids = [consumers[cid] for cid in consumer_ids]
        return pyarrow.table({
            "consumer_id": pyarrow.array(consumer_ids, type=pyarrow.string()),
            "next_snapshot_id": pyarrow.array(
                next_snapshot_ids, type=pyarrow.int64()),
        })
