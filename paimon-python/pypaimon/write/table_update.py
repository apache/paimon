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

from typing import Any, Iterable, List, Mapping, Optional, Sequence

import pyarrow as pa

from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.execution.dispatch import prepare_native_write_operation
from pypaimon.execution.python.write.table_update import (
    PythonTableUpdateMixin, ShardTableUpdator, SingleWriter)
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.table.special_fields import SpecialFields
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
from pypaimon.write.table_upsert_by_key import TableUpsertByKey
from pypaimon.write.update_validation import _authorize_update, predicate_fields

__all__ = ["TableUpdate", "BatchTableUpdate", "StreamTableUpdate", "ShardTableUpdator", "SingleWriter"]


class TableUpdate(PythonTableUpdateMixin):
    """Public batch and stream update configuration and execution entry points."""

    def __init__(self, table, commit_user):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.commit_user = commit_user
        self.update_cols = None
        self.projection = None

    def _new_row_id_updater(
            self, commit_identifier: int, _precomputed_files_info=None
    ) -> TableUpdateByRowId:
        _authorize_update(self.table)
        if _precomputed_files_info is None:
            native = prepare_native_write_operation(
                'update_by_row_id', self.table, self.commit_user, commit_identifier)
            if native is not None:
                return native
        return TableUpdateByRowId(
            self.table, self.commit_user, commit_identifier,
            _precomputed_files_info=_precomputed_files_info)

    def with_update_type(self, update_cols: List[str]):
        update_cols = list(dict.fromkeys(update_cols))
        for col in update_cols:
            if col not in self.table.field_names:
                raise ValueError(f"Column {col} is not in table schema.")
        if len(update_cols) == len(self.table.field_names):
            update_cols = None
        self.update_cols = update_cols
        return self

    def with_read_projection(self, projection: List[str]):
        self.projection = projection

    def new_predicate_builder(self) -> PredicateBuilder:
        return self.table.new_read_builder().new_predicate_builder()

    def new_shard_updator(self, shard_num: int, total_shard_count: int):
        """Create a shard updater for scan and rewrite style updates."""
        if total_shard_count <= 0 or shard_num < 0 or shard_num >= total_shard_count:
            raise ValueError('Shard index must be smaller than a positive shard count')
        native = prepare_native_write_operation(
            'shard_update', self.table, self.commit_user, self.projection,
            self.update_cols, shard_num, total_shard_count,
            stream=isinstance(self, StreamTableUpdate))
        if native is not None:
            return native
        return self._new_python_shard_updator(shard_num, total_shard_count)

    def _update_by_arrow_with_row_id(
            self, table: pa.Table, commit_identifier: int
    ) -> List[CommitMessage]:
        cols = self.update_cols if self.update_cols is not None else [
            c for c in table.column_names if c != SpecialFields.ROW_ID.name]
        if table.num_rows and cols and SpecialFields.ROW_ID.name in table.column_names:
            native = prepare_native_write_operation(
                'update', self.table, self.commit_user, cols)
            if native is not None:
                return native.update_by_arrow_with_row_id(table)
        return self._new_row_id_updater(commit_identifier).update_columns(table, cols)

    def _update_by_arrow_batches_with_row_id(
            self, tables: Iterable[pa.Table], commit_identifier: int
    ) -> List[CommitMessage]:
        updater = None
        try:
            for table in tables:
                cols = self.update_cols if self.update_cols is not None else [
                    c for c in table.column_names
                    if c != SpecialFields.ROW_ID.name
                ]
                if updater is None:
                    updater = self._new_row_id_updater(commit_identifier)
                updater.update_columns(table, cols)
            return [] if updater is None else updater.commit_messages
        except Exception:
            # Never delete files from CommitMessage on failure.
            # A commit can succeed even when its response raises an exception.
            raise

    def _upsert_by_arrow_with_key(
            self,
            table: pa.Table,
            upsert_keys: List[str],
            commit_identifier: int,
    ) -> List[CommitMessage]:
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
        return TableUpsertByKey(
            self.table, self.commit_user, commit_identifier
        ).upsert(table, upsert_keys, self.update_cols)

    def _upsert_by_key(
            self,
            rows,
            upsert_keys: List[str],
            commit_identifier: int,
    ) -> List[CommitMessage]:
        return TableUpsertByKey(
            self.table, self.commit_user, commit_identifier
        ).upsert_rows(rows, upsert_keys, self.update_cols)

    def _merge_into(
            self,
            source: Any,
            on,
            when_matched: Sequence,
            when_not_matched: Sequence,
            commit_identifier: int,
    ) -> List[CommitMessage]:
        from pypaimon.table.data_evolution_merge_into import merge_into

        return merge_into(
            self.table,
            source,
            on=on,
            when_matched=when_matched,
            when_not_matched=when_not_matched,
            commit_user=self.commit_user,
            commit_identifier=commit_identifier,
        )

    def _update_by_predicate(
            self,
            predicate: Optional[Predicate],
            assignments: Mapping[str, Any],
            commit_identifier: int,
            read_columns: Optional[Sequence[str]] = None,
    ) -> List[CommitMessage]:
        """Shared implementation for SQL-like ``UPDATE ... WHERE ...``.

        ``predicate`` identifies the target rows. Assignment values may be
        literals or callables receiving one matched logical file group as an
        Arrow table.
        """
        has_callable = any(callable(value) for value in assignments.values())
        has_array = any(
            isinstance(value, (pa.Array, pa.ChunkedArray))
            for value in assignments.values()
        )
        read_columns = tuple(read_columns or ())
        self._validate_predicate_update(
            assignments, read_columns, has_callable, has_array
        )

        if self.table.options.native_write_enabled():
            native = prepare_native_write_operation(
                'predicate_update', self.table, self.commit_user,
                predicate, list(assignments))
            if native is not None:
                return native.update(assignments, read_columns)
        return self._execute_predicate_update(
            predicate, assignments, commit_identifier, read_columns,
            has_callable, has_array)

    def _delete_by_predicate(
            self, predicate: Optional[Predicate], commit_identifier: int,
    ) -> List[CommitMessage]:
        native = prepare_native_write_operation(
            'predicate_delete', self.table, self.commit_user, predicate)
        if native is not None:
            return native.delete()
        return self._execute_predicate_delete(predicate, commit_identifier)

    def _delete_by_row_id(
            self, row_ids: Sequence[int], commit_identifier: int,
    ) -> List[CommitMessage]:
        if len(row_ids):
            native = prepare_native_write_operation(
                'delete', self.table, self.commit_user)
            if native is not None:
                return native.delete_by_row_id(row_ids)
        return self._delete_rows_with_python(row_ids)

    def _matched_delete_row_ids(
            self, predicate: Optional[Predicate]) -> List[int]:
        scan_table = self._matched_update_scan_table()
        read_builder = scan_table.new_read_builder()
        if predicate is not None:
            read_builder.with_filter(predicate)
            read_builder.with_projection(
                list(scan_table.field_names) + [SpecialFields.ROW_ID.name]
            )
        else:
            read_builder.with_projection([SpecialFields.ROW_ID.name])

        scan = read_builder.new_scan()
        splits = scan.plan_all_files().splits()
        if (splits
                and self.table.options.native_write_enabled()
                and not any(isinstance(split, QueryAuthSplit)
                            for split in splits)):
            from pypaimon.execution.dispatch import try_native_predicate_row_ids
            row_ids = try_native_predicate_row_ids(scan_table, predicate, splits)
            if row_ids is not None:
                return row_ids
        matched = read_builder.new_read().to_arrow(splits)
        if matched.num_rows == 0:
            return []
        return matched[SpecialFields.ROW_ID.name].to_pylist()

    _predicate_fields = staticmethod(predicate_fields)


class BatchTableUpdate(TableUpdate):
    """Batch-mode table update; commit messages always use
    :data:`BATCH_COMMIT_IDENTIFIER`."""

    def new_update_by_row_id(
            self, _precomputed_files_info=None) -> TableUpdateByRowId:
        """Create a row-id updater using this batch update's commit user."""
        return self._new_row_id_updater(
            BATCH_COMMIT_IDENTIFIER, _precomputed_files_info)

    def update_by_arrow_with_row_id(self, table: pa.Table) -> List[CommitMessage]:
        """Apply column updates keyed by ``_ROW_ID`` to existing rows."""
        return self._update_by_arrow_with_row_id(table, BATCH_COMMIT_IDENTIFIER)

    def update_by_arrow_batches_with_row_id(
            self, tables: Iterable[pa.Table]
    ) -> List[CommitMessage]:
        """Apply row-id updates from batches using one target-file index.

        For each updated column, batches must target disjoint ``first_row_id``
        file groups. Conflicting overlap is rejected and all files staged by
        earlier batches are aborted.
        """
        native = prepare_native_write_operation(
            'update', self.table, self.commit_user, self.update_cols)
        if native is not None:
            return native.update_by_arrow_batches_with_row_id(tables)
        return self._update_by_arrow_batches_with_row_id(
            tables, BATCH_COMMIT_IDENTIFIER)

    def upsert_by_arrow_with_key(
            self, table: pa.Table, upsert_keys: List[str]
    ) -> List[CommitMessage]:
        """Upsert rows into an append-only table by one or more key columns."""
        return self._upsert_by_arrow_with_key(
            table, upsert_keys, BATCH_COMMIT_IDENTIFIER
        )

    def upsert_by_key(
            self, rows, upsert_keys: List[str]
    ) -> List[CommitMessage]:
        """Upsert rows into an append-only table by key columns."""
        return self._upsert_by_key(
            rows, upsert_keys, BATCH_COMMIT_IDENTIFIER
        )

    def update_by_predicate(
            self,
            predicate: Optional[Predicate],
            assignments: Mapping[str, Any],
            read_columns: Optional[Sequence[str]] = None,
    ) -> List[CommitMessage]:
        """Update rows using literal or Arrow callable assignments."""
        return self._update_by_predicate(
            predicate,
            assignments,
            BATCH_COMMIT_IDENTIFIER,
            read_columns,
        )

    def delete_by_predicate(
            self,
            predicate: Optional[Predicate],
    ) -> List[CommitMessage]:
        """Delete rows matching ``predicate`` using deletion vectors."""
        return self._delete_by_predicate(predicate, BATCH_COMMIT_IDENTIFIER)

    def delete_by_row_id(self, row_ids: Sequence[int]) -> List[CommitMessage]:
        """Delete rows by ``_ROW_ID`` using deletion vectors."""
        return self._delete_by_row_id(row_ids, BATCH_COMMIT_IDENTIFIER)

    def _delete_by_row_id(
            self, row_ids: Sequence[int], commit_identifier: int
    ) -> List[CommitMessage]:
        return super()._delete_by_row_id(row_ids, commit_identifier)

    def merge_into(
            self,
            source: Any,
            *,
            on,
            when_matched: Sequence = (),
            when_not_matched: Sequence = (),
    ) -> List[CommitMessage]:
        """Prepare batch MERGE INTO commit messages."""
        return self._merge_into(
            source,
            on,
            when_matched,
            when_not_matched,
            BATCH_COMMIT_IDENTIFIER,
        )


class StreamTableUpdate(TableUpdate):
    """Stream-mode table update; the same instance may drive many rounds,
    each tagged with its own ``commit_identifier``."""

    def new_update_by_row_id(
            self, commit_identifier: int,
            _precomputed_files_info=None) -> TableUpdateByRowId:
        """Create a row-id updater for a stream commit identifier."""
        return self._new_row_id_updater(
            commit_identifier, _precomputed_files_info)

    def update_by_arrow_with_row_id(
            self, table: pa.Table, commit_identifier: int
    ) -> List[CommitMessage]:
        """Apply column updates keyed by ``_ROW_ID`` to existing rows,
        tagging the produced commit messages with ``commit_identifier``."""
        return self._update_by_arrow_with_row_id(table, commit_identifier)

    def upsert_by_arrow_with_key(
            self,
            table: pa.Table,
            upsert_keys: List[str],
            commit_identifier: int,
    ) -> List[CommitMessage]:
        """Upsert rows into an append-only table by one or more key columns,
        tagging the produced commit messages with ``commit_identifier``."""
        return self._upsert_by_arrow_with_key(
            table, upsert_keys, commit_identifier
        )

    def upsert_by_key(
            self,
            rows,
            upsert_keys: List[str],
            commit_identifier: int,
    ) -> List[CommitMessage]:
        """Upsert rows into an append-only table by key columns,
        tagging the produced commit messages with ``commit_identifier``."""
        return self._upsert_by_key(
            rows, upsert_keys, commit_identifier
        )

    def update_by_predicate(
            self,
            predicate: Optional[Predicate],
            assignments: Mapping[str, Any],
            commit_identifier: int,
            read_columns: Optional[Sequence[str]] = None,
    ) -> List[CommitMessage]:
        """Update rows using literal or Arrow callable assignments,
        tagging the produced commit messages with ``commit_identifier``."""
        return self._update_by_predicate(
            predicate, assignments, commit_identifier, read_columns
        )

    def delete_by_predicate(
            self,
            predicate: Optional[Predicate],
            commit_identifier: int,
    ) -> List[CommitMessage]:
        """Delete rows matching ``predicate`` using deletion vectors,
        tagging the produced commit messages with ``commit_identifier``."""
        return self._delete_by_predicate(predicate, commit_identifier)

    def delete_by_row_id(
            self,
            row_ids: Sequence[int],
            commit_identifier: int,
    ) -> List[CommitMessage]:
        """Delete rows by ``_ROW_ID`` using deletion vectors,
        tagging the produced commit messages with ``commit_identifier``."""
        return self._delete_by_row_id(row_ids, commit_identifier)

    def merge_into(
            self,
            source: Any,
            *,
            on,
            when_matched: Sequence = (),
            when_not_matched: Sequence = (),
            commit_identifier: int,
    ) -> List[CommitMessage]:
        """Prepare stream MERGE INTO commit messages."""
        return self._merge_into(
            source,
            on,
            when_matched,
            when_not_matched,
            commit_identifier,
        )
