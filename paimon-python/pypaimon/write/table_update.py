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

from typing import TYPE_CHECKING, Any, Iterable, List, Mapping, Optional, Sequence, Tuple

import pyarrow
import pyarrow as pa

from pypaimon.common.predicate import Predicate
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.utils.range import Range

if TYPE_CHECKING:
    from pypaimon.execution.python.write.writer.data_writer import DataWriter
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
from pypaimon.write.update_validation import predicate_fields

__all__ = ["TableUpdate", "BatchTableUpdate", "StreamTableUpdate", "ShardTableUpdator", "SingleWriter"]


class TableUpdate:
    """Public batch and stream update configuration and execution entry points."""

    def __init__(self, table, commit_user):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.commit_user = commit_user
        self.update_cols = None
        self.projection = None

    def _new_row_id_updater(self, commit_identifier: int, _precomputed_files_info=None) -> TableUpdateByRowId:
        from pypaimon.write.update_dispatch import TableUpdateOperations
        return TableUpdateOperations._new_row_id_updater(self, commit_identifier, _precomputed_files_info)

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
        from pypaimon.write.update_dispatch import TableUpdateOperations
        return TableUpdateOperations.new_shard_updator(self, shard_num, total_shard_count)

    def _update_by_arrow_with_row_id(self, table: pa.Table, commit_identifier: int) -> List[CommitMessage]:
        from pypaimon.write.update_dispatch import TableUpdateOperations
        return TableUpdateOperations._update_by_arrow_with_row_id(self, table, commit_identifier)

    def _update_by_arrow_batches_with_row_id(
            self, tables: Iterable[pa.Table], commit_identifier: int) -> List[CommitMessage]:
        from pypaimon.write.update_dispatch import TableUpdateOperations
        return TableUpdateOperations._update_by_arrow_batches_with_row_id(self, tables, commit_identifier)

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
        from pypaimon.write.update_dispatch import TableUpdateOperations
        return TableUpdateOperations._upsert_by_arrow_with_key(self, table, upsert_keys, commit_identifier)

    def _upsert_by_key(self, rows, upsert_keys: List[str], commit_identifier: int) -> List[CommitMessage]:
        from pypaimon.write.update_dispatch import TableUpdateOperations
        return TableUpdateOperations._upsert_by_key(self, rows, upsert_keys, commit_identifier)

    def _merge_into(self, source: Any, on, when_matched: Sequence, when_not_matched: Sequence,
                    commit_identifier: int) -> List[CommitMessage]:
        from pypaimon.write.update_dispatch import TableUpdateOperations
        return TableUpdateOperations._merge_into(self, source, on, when_matched, when_not_matched, commit_identifier)

    def _update_by_predicate(
            self, predicate: Optional[Predicate], assignments: Mapping[str, Any],
            commit_identifier: int, read_columns: Optional[Sequence[str]] = None) -> List[CommitMessage]:
        """Shared implementation for SQL-like ``UPDATE ... WHERE ...``.

        ``predicate`` identifies the target rows. Assignment values may be
        literals or callables receiving one matched logical file group as an
        Arrow table.
        """
        from pypaimon.write.update_dispatch import TableUpdateOperations
        return TableUpdateOperations._update_by_predicate(self, predicate, assignments, commit_identifier, read_columns)

    def _delete_by_predicate(self, predicate: Optional[Predicate], commit_identifier: int) -> List[CommitMessage]:
        from pypaimon.write.update_dispatch import TableUpdateOperations
        return TableUpdateOperations._delete_by_predicate(self, predicate, commit_identifier)

    def _delete_by_row_id(self, row_ids: Sequence[int], commit_identifier: int) -> List[CommitMessage]:
        from pypaimon.write.update_dispatch import TableUpdateOperations
        return TableUpdateOperations._delete_by_row_id(self, row_ids, commit_identifier)

    def _matched_delete_row_ids(self, predicate: Optional[Predicate]) -> List[int]:
        from pypaimon.write.update_dispatch import TableUpdateOperations
        return TableUpdateOperations._matched_delete_row_ids(self, predicate)

    _predicate_fields = staticmethod(predicate_fields)

    @staticmethod
    def _predicate_update_file_groups(splits):
        from pypaimon.execution.python.write.table_update import PythonTableUpdateMixin
        return PythonTableUpdateMixin._predicate_update_file_groups(splits)

    def _matched_update_scan_table(self):
        from pypaimon.execution.python.write.table_update import PythonTableUpdateMixin
        return PythonTableUpdateMixin._matched_update_scan_table(self)

    def _validate_predicate_update(self, assignments, read_columns, has_callable, has_array):
        from pypaimon.write.update_validation import _validate_predicate_update
        return _validate_predicate_update(self, assignments, read_columns, has_callable, has_array)

    def _build_predicate_update_table(self, assignments, matched):
        from pypaimon.execution.python.write.table_update import PythonTableUpdateMixin
        return PythonTableUpdateMixin._build_predicate_update_table(self, assignments, matched)

    @staticmethod
    def _assignment_to_array(value, data_type, row_count):
        from pypaimon.execution.python.write.table_update import PythonTableUpdateMixin
        return PythonTableUpdateMixin._assignment_to_array(value, data_type, row_count)

    def _delete_by_partition_filter(self, partition_filter):
        from pypaimon.execution.python.write.table_update import PythonTableUpdateMixin
        return PythonTableUpdateMixin._delete_by_partition_filter(self, partition_filter)

    @staticmethod
    def _partition_delete_message(messages, partition, bucket, snapshot_id):
        from pypaimon.execution.python.write.table_update import PythonTableUpdateMixin
        return PythonTableUpdateMixin._partition_delete_message(messages, partition, bucket, snapshot_id)

    def _partition_index_entries(self, snapshot, partition_filter):
        from pypaimon.execution.python.write.table_update import PythonTableUpdateMixin
        return PythonTableUpdateMixin._partition_index_entries(self, snapshot, partition_filter)

    def _partition_only_delete_filter(self, predicate):
        from pypaimon.execution.python.write.table_update import PythonTableUpdateMixin
        return PythonTableUpdateMixin._partition_only_delete_filter(self, predicate)

    def _rewrite_predicate_to_partition_indices(self, predicate, partition_index):
        from pypaimon.execution.python.write.table_update import PythonTableUpdateMixin
        return PythonTableUpdateMixin._rewrite_predicate_to_partition_indices(self, predicate, partition_index)

    def _execute_predicate_update(self, predicate, assignments, commit_identifier,
                                  read_columns, has_callable, has_array):
        from pypaimon.execution.python.write.table_update import PythonTableUpdateMixin
        return PythonTableUpdateMixin._execute_predicate_update(
            self, predicate, assignments, commit_identifier, read_columns, has_callable, has_array)

    def _execute_predicate_delete(self, predicate, commit_identifier):
        from pypaimon.execution.python.write.table_update import PythonTableUpdateMixin
        return PythonTableUpdateMixin._execute_predicate_delete(self, predicate, commit_identifier)

    def _new_python_shard_updator(self, shard_num, total_shard_count):
        from pypaimon.execution.python.write.table_update import PythonTableUpdateMixin
        return PythonTableUpdateMixin._new_python_shard_updator(self, shard_num, total_shard_count)

    def _delete_rows_with_python(self, row_ids):
        from pypaimon.execution.python.write.table_update import PythonTableUpdateMixin
        return PythonTableUpdateMixin._delete_rows_with_python(self, row_ids)


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

    def update_by_arrow_batches_with_row_id(self, tables: Iterable[pa.Table]) -> List[CommitMessage]:
        """Apply row-id updates from batches using one target-file index.

        For each updated column, batches must target disjoint ``first_row_id``
        file groups. Conflicting overlap is rejected and all files staged by
        earlier batches are aborted.
        """
        from pypaimon.write.update_dispatch import TableUpdateOperations
        return TableUpdateOperations.update_by_arrow_batches_with_row_id(self, tables)

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


class ShardTableUpdator:

    def __init__(self, table, projection: Optional[List[str]],
                 write_cols: List[str], commit_user, shard_num: int, total_shard_count: int):
        from pypaimon.execution.python.write.table_update import ShardTableUpdator
        return ShardTableUpdator.__init__(self, table, projection, write_cols,
                                          commit_user, shard_num, total_shard_count)

    @staticmethod
    def compute_from_files(files: List[DataFileMeta]) -> List[Range]:
        from pypaimon.execution.python.write.table_update import ShardTableUpdator
        return ShardTableUpdator.compute_from_files(files)

    def arrow_reader(self) -> pyarrow.ipc.RecordBatchReader:
        from pypaimon.execution.python.write.table_update import ShardTableUpdator
        return ShardTableUpdator.arrow_reader(self)

    def prepare_commit(self) -> List[CommitMessage]:
        from pypaimon.execution.python.write.table_update import ShardTableUpdator
        return ShardTableUpdator.prepare_commit(self)

    def close(self):
        from pypaimon.execution.python.write.table_update import ShardTableUpdator
        return ShardTableUpdator.close(self)

    def update_by_arrow_batch(self, data: pa.RecordBatch):
        from pypaimon.execution.python.write.table_update import ShardTableUpdator
        return ShardTableUpdator.update_by_arrow_batch(self, data)

    def _init_writer(self):
        from pypaimon.execution.python.write.table_update import ShardTableUpdator
        return ShardTableUpdator._init_writer(self)


class SingleWriter:

    def __init__(self, writer: 'DataWriter', partition, first_row_id: int, row_count: int):
        from pypaimon.execution.python.write.table_update import SingleWriter
        return SingleWriter.__init__(self, writer, partition, first_row_id, row_count)

    def capacity(self) -> int:
        from pypaimon.execution.python.write.table_update import SingleWriter
        return SingleWriter.capacity(self)

    def write(self, data: pa.RecordBatch):
        from pypaimon.execution.python.write.table_update import SingleWriter
        return SingleWriter.write(self, data)

    def partition(self) -> Tuple:
        from pypaimon.execution.python.write.table_update import SingleWriter
        return SingleWriter.partition(self)

    def end(self) -> DataFileMeta:
        from pypaimon.execution.python.write.table_update import SingleWriter
        return SingleWriter.end(self)
