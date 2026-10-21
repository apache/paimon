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

"""Select update execution before mutations, preserving public owner hooks."""

from typing import Any, Iterable, List, Mapping, Optional, Sequence

import pyarrow as pa

from pypaimon.common.predicate import Predicate
from pypaimon.execution.dispatch import prepare_native_write_operation
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.table.special_fields import SpecialFields
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.row_utils import row_to_named_values
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
from pypaimon.write.table_upsert_by_key import TableUpsertByKey
from pypaimon.write.update_validation import _authorize_update


class TableUpdateOperations:

    def _new_row_id_updater(self, commit_identifier: int, _precomputed_files_info=None) -> TableUpdateByRowId:
        _authorize_update(self.table)
        if _precomputed_files_info is None:
            native = prepare_native_write_operation('update_by_row_id', self.table, self.commit_user, commit_identifier)
            if native is not None:
                return native
        return TableUpdateByRowId(self.table, self.commit_user, commit_identifier,
                                  _precomputed_files_info=_precomputed_files_info)

    def new_shard_updator(self, shard_num: int, total_shard_count: int):
        from pypaimon.write.table_update import StreamTableUpdate
        """Create a shard updater for scan and rewrite style updates."""
        if total_shard_count <= 0 or shard_num < 0 or shard_num >= total_shard_count:
            raise ValueError('Shard index must be smaller than a positive shard count')
        if self.update_cols == []:
            raise ValueError('Shard update columns must not be empty')
        native = prepare_native_write_operation(
            'shard_update',
            self.table,
            self.commit_user,
            self.projection,
            self.update_cols,
            shard_num,
            total_shard_count,
            stream=isinstance(
                self,
                StreamTableUpdate))
        if native is not None:
            return native
        return self._new_python_shard_updator(shard_num, total_shard_count)

    def _update_by_arrow_with_row_id(self, table: pa.Table, commit_identifier: int) -> List[CommitMessage]:
        cols = self.update_cols if self.update_cols is not None else [
            c for c in table.column_names if c != SpecialFields.ROW_ID.name]
        if table.num_rows and cols and (SpecialFields.ROW_ID.name in table.column_names):
            native = prepare_native_write_operation('update', self.table, self.commit_user, cols)
            if native is not None:
                return native.update_by_arrow_with_row_id(table)
        return self._new_row_id_updater(commit_identifier).update_columns(table, cols)

    def _update_by_arrow_batches_with_row_id(
            self, tables: Iterable[pa.Table], commit_identifier: int) -> List[CommitMessage]:
        from pypaimon.execution.python.write.table_update import PythonTableUpdateMixin
        return PythonTableUpdateMixin._update_by_arrow_batches_with_row_id(self, tables, commit_identifier)

    def _upsert_by_arrow_with_key(
            self, table: pa.Table, upsert_keys: List[str], commit_identifier: int) -> List[CommitMessage]:
        """Shared implementation for ``upsert_by_arrow_with_key``.

        For each row in the input Arrow table:

        * If one or more rows with the same composite ``upsert_keys`` value
          already exist → update all of them in-place.
        * Otherwise → append as a new row.

        The public method lives on the concrete subclasses so each can
        expose the signature appropriate to its mode (batch vs stream).

        Args:
            table: Input Arrow table containing rows to upsert.
            upsert_keys: One or more column names forming the composite match key.
            commit_identifier: Identifier to tag the produced commit messages with.

        Returns:
            List of :class:`CommitMessage` objects to be committed.
        """
        return TableUpsertByKey(self.table, self.commit_user, commit_identifier).upsert(
            table, upsert_keys, self.update_cols)

    def _upsert_by_key(self, rows, upsert_keys: List[str], commit_identifier: int) -> List[CommitMessage]:
        return TableUpsertByKey(self.table, self.commit_user, commit_identifier).upsert_rows(
            rows, upsert_keys, self.update_cols)

    def _merge_into(self, source: Any, on, when_matched: Sequence, when_not_matched: Sequence,
                    commit_identifier: int) -> List[CommitMessage]:
        from pypaimon.table.data_evolution_merge_into import merge_into
        return merge_into(self.table, source, on=on, when_matched=when_matched, when_not_matched=when_not_matched,
                          commit_user=self.commit_user, commit_identifier=commit_identifier)

    def _update_by_predicate(
            self, predicate: Optional[Predicate], assignments: Mapping[str, Any],
            commit_identifier: int, read_columns: Optional[Sequence[str]] = None) -> List[CommitMessage]:
        """Shared implementation for SQL-like ``UPDATE ... WHERE ...``.

        ``predicate`` identifies the target rows. Assignment values may be
        literals or callables receiving one matched logical file group as an
        Arrow table.
        """
        has_callable = any((callable(value) for value in assignments.values()))
        has_array = any((isinstance(value, (pa.Array, pa.ChunkedArray)) for value in assignments.values()))
        read_columns = tuple(read_columns or ())
        self._validate_predicate_update(assignments, read_columns, has_callable, has_array)
        if self.table.options.native_write_enabled():
            native = prepare_native_write_operation(
                'predicate_update',
                self.table,
                self.commit_user,
                predicate,
                list(assignments))
            if native is not None:
                return native.update(assignments, read_columns)
        return self._execute_predicate_update(
            predicate, assignments, commit_identifier, read_columns, has_callable, has_array)

    def _delete_by_predicate(self, predicate: Optional[Predicate], commit_identifier: int) -> List[CommitMessage]:
        native = prepare_native_write_operation('predicate_delete', self.table, self.commit_user, predicate)
        if native is not None:
            return native.delete()
        return self._execute_predicate_delete(predicate, commit_identifier)

    def _delete_by_row_id(self, row_ids: Sequence[int], commit_identifier: int) -> List[CommitMessage]:
        if len(row_ids):
            native = prepare_native_write_operation('delete', self.table, self.commit_user)
            if native is not None:
                return native.delete_by_row_id(row_ids)
        return self._delete_rows_with_python(row_ids)

    def _matched_delete_row_ids(self, predicate: Optional[Predicate]) -> List[int]:
        from pypaimon.execution.python.write.table_update import PythonTableUpdateMixin
        return PythonTableUpdateMixin._matched_delete_row_ids(self, predicate)

    def update_by_arrow_batches_with_row_id(self, tables: Iterable[pa.Table]) -> List[CommitMessage]:
        """Apply row-id updates from batches using one target-file index.

        For each updated column, batches must target disjoint ``first_row_id``
        file groups. Conflicting overlap is rejected and all files staged by
        earlier batches are aborted.
        """
        native = prepare_native_write_operation('update', self.table, self.commit_user, self.update_cols)
        if native is not None:
            return native.update_by_arrow_batches_with_row_id(tables)
        return self._update_by_arrow_batches_with_row_id(tables, BATCH_COMMIT_IDENTIFIER)


class UpsertOperations:

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
        if update_cols is None or len(update_cols) == len(self.table.field_names):
            effective_update_cols = None
        else:
            effective_update_cols = update_cols
        columns = list(effective_update_cols) if effective_update_cols else list(self.table.field_names)
        native = self._create_native_upsert(data, upsert_keys, columns)
        if native is not None:
            return native.upsert()
        return self._upsert_arrow(data, upsert_keys, effective_update_cols)

    def upsert_rows(self, rows, upsert_keys: List[str], update_cols: Optional[List[str]] = None) -> List[CommitMessage]:
        row_list = self._normalize_rows(rows)
        if not row_list:
            raise ValueError('rows must not be empty.')
        row_items = [(row, row_to_named_values(row, self.table.table_schema.fields)) for row in row_list]
        for _, values_by_name in row_items:
            self._validate_row_inputs(values_by_name, upsert_keys, update_cols)
        if update_cols is None or len(update_cols) == len(self.table.field_names):
            effective_update_cols = None
        else:
            effective_update_cols = update_cols
        columns = list(effective_update_cols) if effective_update_cols is not None else list(self.table.field_names)
        native = self._create_native_upsert([values for _, values in row_items], upsert_keys, columns)
        if native is not None:
            return native.upsert()
        return self._upsert_rows(row_items, upsert_keys, effective_update_cols)

    def _create_native_upsert(self, data, upsert_keys, columns):
        return prepare_native_write_operation('upsert', self.table, self.commit_user, data, upsert_keys, columns)
