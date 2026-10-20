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

from typing import List, Optional

import pyarrow as pa

from pypaimon.execution.dispatch import prepare_native_write_operation
from pypaimon.execution.python.write.table_upsert_by_key import PythonTableUpsertMixin
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.row_utils import row_to_named_values


class TableUpsertByKey(PythonTableUpsertMixin):
    """Upsert append-table rows using one or more user-selected key columns."""

    def __init__(self, table, commit_user: str, commit_identifier: int):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.commit_user = commit_user
        self.commit_identifier = commit_identifier

    def upsert(self, data: pa.Table, upsert_keys: List[str],
               update_cols: Optional[List[str]] = None) -> List[CommitMessage]:
        """
        Upsert rows into an append-only table by the specified key columns.

        Execution is driven partition-by-partition:
          1. Group input rows by their partition values.
          2. For each partition, scan only that partition to build the
             key -> _ROW_ID map.
          3. Split the partition's input rows into matched (update) and
             unmatched (append).
          4. Perform updates and appends.

        This keeps memory usage proportional to a single partition's key set
        and avoids scanning the entire table.

        Args:
            data: Input Arrow table containing rows to upsert.
                  Must contain all upsert_keys columns.
            upsert_keys: One or more column names used together as a composite
                         match key.
            update_cols: Columns to update for matched rows.
                         If None, all columns in the table schema are updated.

        Returns:
            List of CommitMessages to be committed.
        """
        self._validate_inputs(data, upsert_keys, update_cols)

        # Determine which columns to update
        if update_cols is None or len(update_cols) == len(self.table.field_names):
            effective_update_cols = None  # means all columns
        else:
            effective_update_cols = update_cols

        columns = (list(effective_update_cols) if effective_update_cols
                   else list(self.table.field_names))
        native = self._create_native_upsert(data, upsert_keys, columns)
        if native is not None:
            return native.upsert()

        return self._upsert_arrow(data, upsert_keys, effective_update_cols)

    def upsert_rows(self, rows, upsert_keys: List[str],
                    update_cols: Optional[List[str]] = None) -> List[CommitMessage]:
        row_list = self._normalize_rows(rows)
        if not row_list:
            raise ValueError("rows must not be empty.")

        row_items = [
            (row, row_to_named_values(row, self.table.table_schema.fields))
            for row in row_list
        ]
        for _, values_by_name in row_items:
            self._validate_row_inputs(values_by_name, upsert_keys, update_cols)

        if update_cols is None or len(update_cols) == len(self.table.field_names):
            effective_update_cols = None
        else:
            effective_update_cols = update_cols

        columns = (list(effective_update_cols) if effective_update_cols is not None
                   else list(self.table.field_names))
        native = self._create_native_upsert(
            [values for _, values in row_items], upsert_keys, columns)
        if native is not None:
            return native.upsert()

        return self._upsert_rows(row_items, upsert_keys, effective_update_cols)

    def _create_native_upsert(self, data, upsert_keys, columns):
        return prepare_native_write_operation(
            'upsert', self.table, self.commit_user, data, upsert_keys, columns)
