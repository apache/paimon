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
from pypaimon.write.backend_dispatch import (
    create_python_merge_into_operation, create_python_update_operation, create_python_upsert_operation,
    prepare_native_write_operation,
)
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.table.special_fields import SpecialFields
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.row_utils import row_to_named_values
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
from pypaimon.write.table_upsert_by_key import TableUpsertByKey
from pypaimon.write.update_validation import _authorize_update


class TableUpdateOperations:

    def __init__(self, context):
        self._context = context

    def _new_row_id_updater(self, commit_identifier: int, _precomputed_files_info=None) -> TableUpdateByRowId:
        context = self._context
        # The public constructor and builders share one selection boundary,
        # including complete plans transported to distributed workers.
        return TableUpdateByRowId(context.table, context.commit_user, commit_identifier,
                                  _precomputed_files_info=_precomputed_files_info)

    def new_shard_updator(self, shard_num: int, total_shard_count: int):
        context = self._context
        from pypaimon.write.table_update import StreamTableUpdate
        """Create a shard updater for scan and rewrite style updates."""
        if total_shard_count <= 0 or shard_num < 0 or shard_num >= total_shard_count:
            raise ValueError('Shard index must be smaller than a positive shard count')
        if context.update_cols == []:
            raise ValueError('Shard update columns must not be empty')
        _authorize_update(context.table)
        native = prepare_native_write_operation(
            'shard_update',
            context.table,
            context.commit_user,
            context.projection,
            context.update_cols,
            shard_num,
            total_shard_count,
            stream=isinstance(
                context,
                StreamTableUpdate))
        if native is not None:
            return native
        from pypaimon.write.table_update import ShardTableUpdator
        return ShardTableUpdator._from_python(
            context.table, context.projection, context.update_cols, context.commit_user, shard_num, total_shard_count)

    def _update_by_arrow_with_row_id(self, table: pa.Table, commit_identifier: int) -> List[CommitMessage]:
        context = self._context
        cols = context.update_cols if context.update_cols is not None else [
            c for c in table.column_names if c != SpecialFields.ROW_ID.name]
        operation = None
        if table.num_rows and cols and (SpecialFields.ROW_ID.name in table.column_names):
            operation = prepare_native_write_operation('update', context.table, context.commit_user, cols)
        if operation is None:
            operation = create_python_update_operation(context, commit_identifier)
        return operation.update_by_arrow_with_row_id(table)

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
        context = self._context
        return TableUpsertByKey(context.table, context.commit_user, commit_identifier).upsert(
            table, upsert_keys, context.update_cols)

    def _upsert_by_key(self, rows, upsert_keys: List[str], commit_identifier: int) -> List[CommitMessage]:
        context = self._context
        return TableUpsertByKey(context.table, context.commit_user, commit_identifier).upsert_rows(
            rows, upsert_keys, context.update_cols)

    def _merge_into(self, source: Any, on, when_matched: Sequence, when_not_matched: Sequence,
                    commit_identifier: int) -> List[CommitMessage]:
        context = self._context
        from pypaimon.table.data_evolution_merge_into import merge_into
        return merge_into(context.table, source, on=on, when_matched=when_matched, when_not_matched=when_not_matched,
                          commit_user=context.commit_user, commit_identifier=commit_identifier)

    def _update_by_predicate(
            self, predicate: Optional[Predicate], assignments: Mapping[str, Any],
            commit_identifier: int, read_columns: Optional[Sequence[str]] = None) -> List[CommitMessage]:
        """Shared implementation for SQL-like ``UPDATE ... WHERE ...``.

        ``predicate`` identifies the target rows. Assignment values may be
        literals or callables receiving one matched logical file group as an
        Arrow table.
        """
        context = self._context
        has_callable = any((callable(value) for value in assignments.values()))
        has_array = any((isinstance(value, (pa.Array, pa.ChunkedArray)) for value in assignments.values()))
        read_columns = tuple(read_columns or ())
        context._validate_predicate_update(assignments, read_columns, has_callable, has_array)
        operation = None
        if context.table.options.native_write_enabled():
            operation = prepare_native_write_operation(
                'predicate_update',
                context.table,
                context.commit_user,
                predicate,
                list(assignments))
        if operation is None:
            operation = create_python_update_operation(
                context, commit_identifier, predicate, has_callable=has_callable, has_array=has_array)
        return operation.update(assignments, read_columns)

    def _delete_by_predicate(self, predicate: Optional[Predicate], commit_identifier: int) -> List[CommitMessage]:
        context = self._context
        operation = prepare_native_write_operation('predicate_delete', context.table, context.commit_user, predicate)
        if operation is None:
            operation = create_python_update_operation(context, commit_identifier, predicate)
        return operation.delete()

    def _delete_by_row_id(self, row_ids: Sequence[int], commit_identifier: int) -> List[CommitMessage]:
        context = self._context
        operation = None
        if len(row_ids):
            operation = prepare_native_write_operation('delete', context.table, context.commit_user)
        if operation is None:
            operation = create_python_update_operation(context, commit_identifier)
        return operation.delete_by_row_id(row_ids)

    def update_by_arrow_batches_with_row_id(self, tables: Iterable[pa.Table]) -> List[CommitMessage]:
        """Apply row-id updates from batches using one target-file index.

        For each updated column, batches must target disjoint ``first_row_id``
        file groups. Conflicting overlap is rejected. Already prepared files
        are retained because they may have been published by the caller.
        """
        context = self._context
        operation = prepare_native_write_operation('update', context.table, context.commit_user, context.update_cols)
        if operation is None:
            operation = create_python_update_operation(context, BATCH_COMMIT_IDENTIFIER)
        return operation.update_by_arrow_batches_with_row_id(tables)


class UpsertOperations:

    def __init__(self, context):
        self._context = context

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
        context = self._context
        context._validate_inputs(data, upsert_keys, update_cols)
        if update_cols is None or len(update_cols) == len(context.table.field_names):
            effective_update_cols = None
        else:
            effective_update_cols = update_cols
        columns = list(effective_update_cols) if effective_update_cols else list(context.table.field_names)
        operation = context._create_native_upsert(data, upsert_keys, columns)
        if operation is None:
            operation = create_python_upsert_operation(context, data, upsert_keys, effective_update_cols)
        return operation.upsert()

    def upsert_rows(self, rows, upsert_keys: List[str], update_cols: Optional[List[str]] = None) -> List[CommitMessage]:
        context = self._context
        row_list = context._normalize_rows(rows)
        if not row_list:
            raise ValueError('rows must not be empty.')
        row_items = [(row, row_to_named_values(row, context.table.table_schema.fields)) for row in row_list]
        for _, values_by_name in row_items:
            context._validate_row_inputs(values_by_name, upsert_keys, update_cols)
        if update_cols is None or len(update_cols) == len(context.table.field_names):
            effective_update_cols = None
        else:
            effective_update_cols = update_cols
        columns = list(effective_update_cols) if effective_update_cols is not None else list(context.table.field_names)
        operation = context._create_native_upsert([values for _, values in row_items], upsert_keys, columns)
        if operation is None:
            operation = create_python_upsert_operation(
                context, row_items, upsert_keys, effective_update_cols, rows=True)
        return operation.upsert()

    def _create_native_upsert(self, data, upsert_keys, columns):
        context = self._context
        return prepare_native_write_operation('upsert', context.table, context.commit_user, data, upsert_keys, columns)


def merge_into(
        target_table, source, *, on, when_matched=(), when_not_matched=(),
        commit_user, commit_identifier=BATCH_COMMIT_IDENTIFIER):
    """Select MERGE execution before consuming source input or staging files."""
    if target_table.options.native_write_enabled():
        from pypaimon.write.backend_dispatch import prepare_native_merge_into

        native = prepare_native_merge_into(
            target_table, source, on, when_matched, when_not_matched,
            commit_user, commit_identifier,
        )
        if native is not None:
            return native.prepare_commit()

    return create_python_merge_into_operation(
        target_table, source, on=on, when_matched=when_matched,
        when_not_matched=when_not_matched, commit_user=commit_user,
        commit_identifier=commit_identifier,
    ).prepare_commit()
