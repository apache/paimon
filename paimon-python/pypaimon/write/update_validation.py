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

from typing import Any, Dict, List, Mapping, Optional, Sequence

import pyarrow as pa

from pypaimon.common.predicate import Predicate
from pypaimon.table.row.internal_row import InternalRow
from pypaimon.write.row_utils import require_columns


def _authorize_update(table):
    """Require the same whole-table grant as plan_all_files for fast paths."""
    from pypaimon.catalog.catalog_exception import TableNoPermissionException
    from pypaimon.read.auth_support import authorize

    query_auth = table.catalog_environment.table_query_auth(table.options, table.identifier)
    if authorize(table, query_auth, None) is not None:
        raise TableNoPermissionException(table.identifier)


def predicate_fields(predicate: Predicate) -> set:
    if predicate.field is not None:
        return {predicate.field}
    fields = set()
    for child in predicate.literals or []:
        fields.update(predicate_fields(child))
    return fields


def _validate_predicate_update(
        self, assignments: Mapping[str, Any], read_columns: Optional[Sequence[str]],
        has_callable: bool, has_array: bool):
    if not self.table.options.data_evolution_enabled():
        raise ValueError("update_by_predicate requires 'data-evolution.enabled' = 'true'.")
    if not self.table.options.row_tracking_enabled():
        raise ValueError("update_by_predicate requires 'row-tracking.enabled' = 'true'.")
    if not assignments:
        raise ValueError('assignments must not be empty.')
    if read_columns and (not has_callable):
        raise ValueError('read_columns requires a callable assignment.')
    if has_callable:
        if has_array:
            raise ValueError('Callable assignments cannot be combined with Arrow array assignments.')
        if not read_columns:
            raise ValueError('Callable assignments require read_columns.')
        for col in read_columns:
            if col not in self.table.field_names:
                raise ValueError(f'Read column {col} is not in table schema.')
    partition_keys = set(self.table.partition_keys)
    for col in assignments:
        if col not in self.table.field_names:
            raise ValueError(f'Column {col} is not in table schema.')
        if col in partition_keys:
            raise ValueError(f"update_by_predicate does not support updating partition column '{col}'.")


def _validate_inputs(self, data: pa.Table, upsert_keys: List[str], update_cols: Optional[List[str]]):
    """Validate inputs before processing."""
    if not self.table.options.data_evolution_enabled():
        raise ValueError("upsert_by_arrow_with_key requires 'data-evolution.enabled' = 'true'.")
    if not self.table.options.row_tracking_enabled():
        raise ValueError("upsert_by_arrow_with_key requires 'row-tracking.enabled' = 'true'.")
    if not upsert_keys:
        raise ValueError('upsert_keys must not be empty.')
    for key in upsert_keys:
        if key not in self.table.field_names:
            raise ValueError(f"upsert_key '{key}' is not in table schema fields: {self.table.field_names}")
        if key not in data.column_names:
            raise ValueError(f"upsert_key '{key}' is not in input data columns: {data.column_names}")
    partition_keys = self.table.partition_keys
    if partition_keys:
        missing_in_data = [pk for pk in partition_keys if pk not in data.column_names]
        if missing_in_data:
            raise ValueError(
                'For partitioned tables, input data must contain all partition key '
                f'columns. Missing: {missing_in_data}')
    if update_cols is not None:
        for col in update_cols:
            if col not in self.table.field_names:
                raise ValueError(
                    f"Column '{col}' in update_cols is not in table schema fields: {self.table.field_names}")
    if data.num_rows == 0:
        raise ValueError('Input data is empty.')


def _validate_row_inputs(self, values_by_name: Dict[str, Any],
                         upsert_keys: List[str], update_cols: Optional[List[str]]):
    if not self.table.options.data_evolution_enabled():
        raise ValueError("upsert_by_key requires 'data-evolution.enabled' = 'true'.")
    if not self.table.options.row_tracking_enabled():
        raise ValueError("upsert_by_key requires 'row-tracking.enabled' = 'true'.")
    if not upsert_keys:
        raise ValueError('upsert_keys must not be empty.')
    for key in upsert_keys:
        if key not in self.table.field_names:
            raise ValueError(f"upsert_key '{key}' is not in table schema fields: {self.table.field_names}")
    unknown_fields = [field_name for field_name in values_by_name if field_name not in self.table.field_names]
    if unknown_fields:
        raise ValueError(
            f'upsert_by_key got row field(s) {unknown_fields} '
            f'that are not in table schema fields: {self.table.field_names}')
    require_columns(values_by_name, upsert_keys, 'upsert_by_key')
    require_columns(values_by_name, self.table.partition_keys, 'upsert_by_key')
    if update_cols is not None:
        for col in update_cols:
            if col not in self.table.field_names:
                raise ValueError(
                    f"Column '{col}' in update_cols is not in table schema fields: {self.table.field_names}")


def _normalize_rows(rows) -> List:
    if isinstance(rows, InternalRow):
        return [rows]
    return list(rows)
