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

from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.commit_identifier import validate_commit_identifier


class TableUpsertByKey:
    """Upsert append-table rows using one or more user-selected key columns."""

    def __init__(self, table, commit_user: str, commit_identifier: int):
        commit_identifier = validate_commit_identifier(commit_identifier)
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
        from pypaimon.write.update_dispatch import UpsertOperations
        return UpsertOperations.upsert(self, data, upsert_keys, update_cols)

    def upsert_rows(self, rows, upsert_keys: List[str], update_cols: Optional[List[str]] = None) -> List[CommitMessage]:
        from pypaimon.write.update_dispatch import UpsertOperations
        return UpsertOperations.upsert_rows(self, rows, upsert_keys, update_cols)

    def _create_native_upsert(self, data, upsert_keys, columns):
        from pypaimon.write.update_dispatch import UpsertOperations
        return UpsertOperations._create_native_upsert(self, data, upsert_keys, columns)

    def _new_row_id_updater(self):
        from pypaimon.execution.python.write.table_upsert_by_key import PythonTableUpsertMixin
        return PythonTableUpsertMixin._new_row_id_updater(self)

    @staticmethod
    def _normalize_rows(rows):
        from pypaimon.write.update_validation import _normalize_rows
        return _normalize_rows(rows)

    def _group_rows_by_partition(self, row_items):
        from pypaimon.execution.python.write.table_upsert_by_key import PythonTableUpsertMixin
        return PythonTableUpsertMixin._group_rows_by_partition(self, row_items)

    def _upsert_row_partition(self, row_items, upsert_keys, partition_spec, update_cols):
        from pypaimon.execution.python.write.table_upsert_by_key import PythonTableUpsertMixin
        return PythonTableUpsertMixin._upsert_row_partition(self, row_items, upsert_keys, partition_spec, update_cols)

    @staticmethod
    def _dedup_row_items_last_write_wins(row_items, input_key_tuples, partition_spec):
        from pypaimon.execution.python.write.table_upsert_by_key import PythonTableUpsertMixin
        return PythonTableUpsertMixin._dedup_row_items_last_write_wins(row_items, input_key_tuples, partition_spec)

    def _group_by_partition(self, data):
        from pypaimon.execution.python.write.table_upsert_by_key import PythonTableUpsertMixin
        return PythonTableUpsertMixin._group_by_partition(self, data)

    def _upsert_partition(self, partition_data, upsert_keys, partition_spec, update_cols):
        from pypaimon.execution.python.write.table_upsert_by_key import PythonTableUpsertMixin
        return PythonTableUpsertMixin._upsert_partition(self, partition_data, upsert_keys, partition_spec, update_cols)

    @staticmethod
    def _dedup_last_write_wins(partition_data, input_key_tuples, partition_spec):
        from pypaimon.execution.python.write.table_upsert_by_key import PythonTableUpsertMixin
        return PythonTableUpsertMixin._dedup_last_write_wins(partition_data, input_key_tuples, partition_spec)

    def _validate_inputs(self, data, upsert_keys, update_cols):
        from pypaimon.write.update_validation import _validate_inputs
        return _validate_inputs(self, data, upsert_keys, update_cols)

    def _validate_row_inputs(self, values_by_name, upsert_keys, update_cols):
        from pypaimon.write.update_validation import _validate_row_inputs
        return _validate_row_inputs(self, values_by_name, upsert_keys, update_cols)

    def _build_key_to_row_ids_map(self, match_keys, partition_spec, input_key_set):
        from pypaimon.execution.python.write.table_upsert_by_key import PythonTableUpsertMixin
        return PythonTableUpsertMixin._build_key_to_row_ids_map(self, match_keys, partition_spec, input_key_set)

    def _do_updates(self, data, matched_indices, input_key_tuples, key_to_row_ids, update_cols):
        from pypaimon.execution.python.write.table_upsert_by_key import PythonTableUpsertMixin
        return PythonTableUpsertMixin._do_updates(
            self, data, matched_indices, input_key_tuples, key_to_row_ids, update_cols)

    def _do_appends(self, data, new_indices):
        from pypaimon.execution.python.write.table_upsert_by_key import PythonTableUpsertMixin
        return PythonTableUpsertMixin._do_appends(self, data, new_indices)

    def _append_rows(self, row_items):
        from pypaimon.execution.python.write.table_upsert_by_key import PythonTableUpsertMixin
        return PythonTableUpsertMixin._append_rows(self, row_items)

    def _append_row_column_names(self, row_items):
        from pypaimon.execution.python.write.table_upsert_by_key import PythonTableUpsertMixin
        return PythonTableUpsertMixin._append_row_column_names(self, row_items)

    def _upsert_arrow(self, data, upsert_keys, effective_update_cols):
        from pypaimon.execution.python.write.table_upsert_by_key import PythonTableUpsertMixin
        return PythonTableUpsertMixin._upsert_arrow(self, data, upsert_keys, effective_update_cols)

    def _upsert_rows(self, row_items, upsert_keys, effective_update_cols):
        from pypaimon.execution.python.write.table_upsert_by_key import PythonTableUpsertMixin
        return PythonTableUpsertMixin._upsert_rows(self, row_items, upsert_keys, effective_update_cols)
