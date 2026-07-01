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

from typing import Callable, Dict, List, Optional

import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.common.predicate_json_parser import (
    extract_referenced_fields,
    parse_predicate_to_batch_filter,
)
from pypaimon.schema.data_types import DataField


class TableQueryAuthResult:

    def __init__(self, filter: Optional[List[str]], column_masking: Optional[Dict[str, str]]):
        self.filter = [f for f in filter if f] if filter else filter
        self.column_masking = (
            {k: v for k, v in column_masking.items() if k and v}
            if column_masking else column_masking
        )

    def convert_plan(self, plan):
        from pypaimon.read.query_auth_split import QueryAuthSplit
        from pypaimon.read.plan import Plan

        if not self.filter and not self.column_masking:
            return plan
        auth_splits = [QueryAuthSplit(split, self) for split in plan.splits()]
        return Plan(auth_splits, snapshot_id=plan.snapshot_id)

    def extract_row_filter(self) -> Optional[Callable[[pa.RecordBatch], pa.Array]]:
        if not self.filter:
            return None
        filters = [parse_predicate_to_batch_filter(json_str) for json_str in self.filter]
        if len(filters) == 1:
            return filters[0]

        def combined(batch: pa.RecordBatch) -> pa.Array:
            result = filters[0](batch)
            for f in filters[1:]:
                result = pc.and_(result, f(batch))
            return result
        return combined

    def get_extra_fields_for_filter(
            self,
            read_fields: List[DataField],
            table_fields: List[DataField],
    ) -> List[DataField]:
        if not self.filter:
            return []
        read_field_names = {f.name for f in read_fields}
        extra = []
        for json_str in self.filter:
            referenced = extract_referenced_fields(json_str)
            for name in referenced:
                if name not in read_field_names:
                    field = next((f for f in table_fields if f.name == name), None)
                    if field:
                        extra.append(field)
                        read_field_names.add(name)
        return extra
