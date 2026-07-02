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

import logging
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa

from pypaimon.read.table_read import TableRead
from pypaimon.table.row.blob import Blob
from pypaimon.table.row.internal_row import InternalRow
from pypaimon.table.special_fields import SpecialFields
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.row_utils import require_columns, row_to_named_values
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
from pypaimon.write.table_write import StreamTableWrite

# Composite key is represented as a tuple of values
_KeyTuple = Tuple[Any, ...]

logger = logging.getLogger(__name__)


class TableUpsertByKey:
    """
    Table upsert by one or more user-specified key columns for append-only tables.

    For each row in the input Arrow table:
    - If one or more rows with the same upsert_keys composite value already exist →
      update all of them (in-place rewrite).
    - If no matching row exists → append as a new row.

    All upsert_keys must be columns present in both the input data and the table schema.
    """

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

        all_commit_messages: List[CommitMessage] = []

        # Process each partition independently
        for partition_spec, partition_data in self._group_by_partition(data):
            msgs = self._upsert_partition(
                partition_data, upsert_keys, partition_spec, effective_update_cols
            )
            all_commit_messages.extend(msgs)

        return all_commit_messages

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

        commit_messages: List[CommitMessage] = []
        for partition_spec, partition_items in self._group_rows_by_partition(row_items):
            commit_messages.extend(
                self._upsert_row_partition(
                    partition_items,
                    upsert_keys,
                    partition_spec,
                    effective_update_cols,
                )
            )
        return commit_messages

    @staticmethod
    def _normalize_rows(rows) -> List:
        if isinstance(rows, InternalRow):
            return [rows]
        return list(rows)

    def _group_rows_by_partition(
            self,
            row_items: List[Tuple[Any, Dict[str, Any]]],
    ) -> List[Tuple[Dict[str, Any], List[Tuple[Any, Dict[str, Any]]]]]:
        if not self.table.partition_keys:
            return [({}, row_items)]

        partition_to_items: Dict[Tuple[Any, ...], List[Tuple[Any, Dict[str, Any]]]] = {}
        for item in row_items:
            _, values_by_name = item
            part_tuple = tuple(
                values_by_name[key] for key in self.table.partition_keys
            )
            partition_to_items.setdefault(part_tuple, []).append(item)

        return [
            (dict(zip(self.table.partition_keys, part_tuple)), items)
            for part_tuple, items in partition_to_items.items()
        ]

    def _upsert_row_partition(
            self,
            row_items: List[Tuple[Any, Dict[str, Any]]],
            upsert_keys: List[str],
            partition_spec: Dict[str, Any],
            update_cols: Optional[List[str]],
    ) -> List[CommitMessage]:
        partition_key_set = set(self.table.partition_keys)
        match_keys = [k for k in upsert_keys if k not in partition_key_set]
        input_key_tuples = [
            tuple(values_by_name[k] for k in match_keys)
            for _, values_by_name in row_items
        ]
        for key_tuple in input_key_tuples:
            for value in key_tuple:
                if isinstance(value, Blob):
                    raise ValueError("Blob values are not supported as upsert keys.")

        row_items, input_key_tuples = self._dedup_row_items_last_write_wins(
            row_items, input_key_tuples, partition_spec)

        key_to_row_ids = self._build_key_to_row_ids_map(
            match_keys, partition_spec, set(input_key_tuples)
        )

        matched_items: List[Tuple[Any, Dict[str, Any]]] = []
        matched_row_ids: List[List[int]] = []
        new_items: List[Tuple[Any, Dict[str, Any]]] = []
        for item, key_tuple in zip(row_items, input_key_tuples):
            if key_tuple in key_to_row_ids:
                matched_items.append(item)
                matched_row_ids.append(key_to_row_ids[key_tuple])
            else:
                new_items.append(item)

        commit_messages: List[CommitMessage] = []
        if matched_items:
            cols_to_update = (
                list(update_cols)
                if update_cols is not None
                else list(self.table.field_names)
            )
            for _, values_by_name in matched_items:
                require_columns(values_by_name, cols_to_update, "upsert_by_key")
            commit_messages.extend(TableUpdateByRowId(
                self.table, self.commit_user, self.commit_identifier,
            ).update_rows_columns(
                [row for row, _ in matched_items],
                matched_row_ids,
                cols_to_update,
            ))

        if new_items:
            commit_messages.extend(self._append_rows(new_items))

        return commit_messages

    @staticmethod
    def _dedup_row_items_last_write_wins(
            row_items: List[Tuple[Any, Dict[str, Any]]],
            input_key_tuples: List[_KeyTuple],
            partition_spec: Dict[str, Any],
    ) -> Tuple[List[Tuple[Any, Dict[str, Any]]], List[_KeyTuple]]:
        key_to_last_idx: Dict[_KeyTuple, int] = {}
        for i, key_tuple in enumerate(input_key_tuples):
            key_to_last_idx[key_tuple] = i

        if len(key_to_last_idx) == len(input_key_tuples):
            return row_items, input_key_tuples

        original_count = len(input_key_tuples)
        dedup_indices = sorted(key_to_last_idx.values())
        logger.warning(
            "Deduplicated input rows from %d to %d in partition %s "
            "(kept last occurrence).",
            original_count, len(dedup_indices), partition_spec,
        )
        return (
            [row_items[i] for i in dedup_indices],
            [input_key_tuples[i] for i in dedup_indices],
        )

    # ------------------------------------------------------------------
    # Partition grouping
    # ------------------------------------------------------------------

    def _group_by_partition(
            self, data: pa.Table
    ) -> List[Tuple[Dict[str, Any], pa.Table]]:
        """
        Split *data* into ``(partition_spec, partition_rows)`` pairs.

        For non-partitioned tables a single pair ``({}, data)`` is returned.
        Iteration order matches first-appearance order of each partition value
        in the input — dict preserves insertion order on Python 3.7+.
        """
        partition_keys = self.table.partition_keys
        if not partition_keys:
            return [({}, data)]

        part_columns = [data[k].to_pylist() for k in partition_keys]

        partition_to_indices: Dict[Tuple[Any, ...], List[int]] = {}
        for i in range(data.num_rows):
            part_tuple = tuple(col[i] for col in part_columns)
            partition_to_indices.setdefault(part_tuple, []).append(i)

        return [
            (
                dict(zip(partition_keys, part_tuple)),
                data.take(pa.array(indices, type=pa.int64())),
            )
            for part_tuple, indices in partition_to_indices.items()
        ]

    # ------------------------------------------------------------------
    # Per-partition upsert
    # ------------------------------------------------------------------

    def _upsert_partition(
            self,
            partition_data: pa.Table,
            upsert_keys: List[str],
            partition_spec: Dict[str, Any],
            update_cols: Optional[List[str]],
    ) -> List[CommitMessage]:
        """
        Full upsert cycle scoped to a single partition.

        Partition key columns are stripped from *upsert_keys* before matching
        because all rows within this partition share the same partition values.

        The scan reads partition data in batches and filters against the input
        key set on-the-fly, so only matching key → _ROW_ID pairs are kept in
        memory (instead of the entire partition's key set).
        """
        # Partition columns are constant inside one partition – drop them
        # from the key set used for matching.
        partition_key_set = set(self.table.partition_keys)
        match_keys = [k for k in upsert_keys if k not in partition_key_set]

        # 1. Build the composite key tuple for every input row.
        key_columns = [partition_data[k].to_pylist() for k in match_keys]
        input_key_tuples: List[_KeyTuple] = [
            tuple(col[i] for col in key_columns)
            for i in range(partition_data.num_rows)
        ]

        # 2. Deduplicate – keep the LAST occurrence of every repeated key.
        partition_data, input_key_tuples = self._dedup_last_write_wins(
            partition_data, input_key_tuples, partition_spec,
        )

        # 3. Scan partition once, keeping key → [_ROW_ID, ...] for keys that
        #    appear in the input (memory ∝ matched existing rows, not the
        #    whole partition).
        key_to_row_ids = self._build_key_to_row_ids_map(
            match_keys, partition_spec, set(input_key_tuples),
        )

        # 4. Partition input rows into matched (update) vs unmatched (append).
        matched_indices: List[int] = []
        new_indices: List[int] = []
        for i, key_tuple in enumerate(input_key_tuples):
            (matched_indices if key_tuple in key_to_row_ids else new_indices).append(i)

        logger.info(
            "Upserting partition %s: %d matched, %d new",
            partition_spec, len(matched_indices), len(new_indices),
        )
        total_updates = sum(
            len(key_to_row_ids[input_key_tuples[i]]) for i in matched_indices)
        if total_updates > len(matched_indices):
            logger.info(
                "Upsert fan-out in partition %s: %d input rows expand to "
                "%d row updates", partition_spec,
                len(matched_indices), total_updates,
            )

        commit_messages: List[CommitMessage] = []
        if matched_indices:
            commit_messages.extend(self._do_updates(
                partition_data, matched_indices,
                input_key_tuples, key_to_row_ids, update_cols,
            ))
        if new_indices:
            commit_messages.extend(self._do_appends(partition_data, new_indices))
        return commit_messages

    @staticmethod
    def _dedup_last_write_wins(
            partition_data: pa.Table,
            input_key_tuples: List[_KeyTuple],
            partition_spec: Dict[str, Any],
    ) -> Tuple[pa.Table, List[_KeyTuple]]:
        """Collapse duplicate-key rows in ``partition_data`` to the last
        occurrence of each key, preserving relative order. No-op when the
        input has no duplicates."""
        key_to_last_idx: Dict[_KeyTuple, int] = {}
        for i, key_tuple in enumerate(input_key_tuples):
            key_to_last_idx[key_tuple] = i  # last write wins

        if len(key_to_last_idx) == len(input_key_tuples):
            return partition_data, input_key_tuples

        original_count = len(input_key_tuples)
        dedup_indices = sorted(key_to_last_idx.values())
        logger.warning(
            "Deduplicated input from %d to %d rows in partition %s "
            "(kept last occurrence).",
            original_count, len(dedup_indices), partition_spec,
        )
        return (
            partition_data.take(dedup_indices),
            [input_key_tuples[i] for i in dedup_indices],
        )

    def _validate_inputs(self, data: pa.Table, upsert_keys: List[str],
                         update_cols: Optional[List[str]]):
        """Validate inputs before processing."""
        if not self.table.options.data_evolution_enabled():
            raise ValueError(
                "upsert_by_arrow_with_key requires 'data-evolution.enabled' = 'true'."
            )

        if not self.table.options.row_tracking_enabled():
            raise ValueError(
                "upsert_by_arrow_with_key requires 'row-tracking.enabled' = 'true'."
            )

        if not upsert_keys:
            raise ValueError("upsert_keys must not be empty.")

        for key in upsert_keys:
            if key not in self.table.field_names:
                raise ValueError(
                    f"upsert_key '{key}' is not in table schema fields: {self.table.field_names}"
                )
            if key not in data.column_names:
                raise ValueError(
                    f"upsert_key '{key}' is not in input data columns: {data.column_names}"
                )

        # For partitioned tables, input data must contain partition columns
        partition_keys = self.table.partition_keys
        if partition_keys:
            missing_in_data = [pk for pk in partition_keys if pk not in data.column_names]
            if missing_in_data:
                raise ValueError(
                    f"For partitioned tables, input data must contain all partition key "
                    f"columns. Missing: {missing_in_data}"
                )

        if update_cols is not None:
            for col in update_cols:
                if col not in self.table.field_names:
                    raise ValueError(
                        f"Column '{col}' in update_cols is not in table schema fields: "
                        f"{self.table.field_names}"
                    )

        if data.num_rows == 0:
            raise ValueError("Input data is empty.")

        # NOTE: duplicate-key checking is deferred to _upsert_partition so
        # that partition columns can be stripped first.  The same non-partition
        # key may legally appear in different partitions.

    def _validate_row_inputs(
            self,
            values_by_name: Dict[str, Any],
            upsert_keys: List[str],
            update_cols: Optional[List[str]]):
        if not self.table.options.data_evolution_enabled():
            raise ValueError(
                "upsert_by_key requires 'data-evolution.enabled' = 'true'."
            )

        if not self.table.options.row_tracking_enabled():
            raise ValueError(
                "upsert_by_key requires 'row-tracking.enabled' = 'true'."
            )

        if not upsert_keys:
            raise ValueError("upsert_keys must not be empty.")

        for key in upsert_keys:
            if key not in self.table.field_names:
                raise ValueError(
                    f"upsert_key '{key}' is not in table schema fields: {self.table.field_names}"
                )

        unknown_fields = [
            field_name
            for field_name in values_by_name
            if field_name not in self.table.field_names
        ]
        if unknown_fields:
            raise ValueError(
                f"upsert_by_key got row field(s) {unknown_fields} "
                f"that are not in table schema fields: {self.table.field_names}"
            )

        require_columns(values_by_name, upsert_keys, "upsert_by_key")
        require_columns(values_by_name, self.table.partition_keys, "upsert_by_key")

        if update_cols is not None:
            for col in update_cols:
                if col not in self.table.field_names:
                    raise ValueError(
                        f"Column '{col}' in update_cols is not in table schema fields: "
                        f"{self.table.field_names}"
                    )

    def _build_key_to_row_ids_map(
            self,
            match_keys: List[str],
            partition_spec: Optional[Dict[str, Any]],
            input_key_set: set,
    ) -> Dict[_KeyTuple, List[int]]:
        """
        Scan the partition in batches and collect key → [_ROW_ID, ...] for
        rows whose composite key is in *input_key_set*.

        The partition spec (if any) is pushed down as an ``and`` of per-key
        equality predicates so non-matching partitions are pruned by the
        scanner. The match-key filter itself is applied in Python on each
        batch — this keeps memory proportional to the input key set rather
        than the entire partition.

        Args:
            match_keys:     Column names used as the composite match key
                            (partition columns already stripped).
            partition_spec:  Dict of partition_key → value to restrict the scan.
                             Pass an empty dict (or None) for non-partitioned tables.
            input_key_set:   Set of composite key tuples from the input data.
        """
        read_builder = self.table.new_read_builder()

        if partition_spec:
            predicate_builder = read_builder.new_predicate_builder()
            sub_predicates = [
                predicate_builder.equal(k, v)
                for k, v in partition_spec.items()
            ]
            partition_predicate = predicate_builder.and_predicates(sub_predicates)
            read_builder = read_builder.with_filter(partition_predicate)

        scan = read_builder.new_scan()
        splits = scan.plan().splits()
        if not splits:
            return {}

        # Read only the key columns + _ROW_ID
        key_fields = [self.table.field_dict[k] for k in match_keys]
        read_type = key_fields + [SpecialFields.ROW_ID]

        table_read = TableRead(
            table=self.table, predicate=None, read_type=read_type
        )

        # Stream batches and filter against input_key_set on-the-fly
        key_to_row_ids: Dict[_KeyTuple, List[int]] = {}
        row_id_col = SpecialFields.ROW_ID.name
        for batch in table_read.to_arrow_batch_reader(splits):
            batch_key_cols = [batch.column(k).to_pylist() for k in match_keys]
            batch_row_ids = batch.column(row_id_col).to_pylist()
            for j, row_id in enumerate(batch_row_ids):
                key_tuple = tuple(col[j] for col in batch_key_cols)
                if key_tuple in input_key_set:
                    key_to_row_ids.setdefault(key_tuple, []).append(row_id)

        return key_to_row_ids

    def _do_updates(
            self,
            data: pa.Table,
            matched_indices: List[int],
            input_key_tuples: List[_KeyTuple],
            key_to_row_ids: Dict[_KeyTuple, List[int]],
            update_cols: Optional[List[str]]
    ) -> List[CommitMessage]:
        """Update matched rows in-place via :class:`TableUpdateByRowId`."""
        expanded_input_indices: List[int] = []
        row_ids: List[int] = []
        for i in matched_indices:
            for row_id in key_to_row_ids[input_key_tuples[i]]:
                expanded_input_indices.append(i)
                row_ids.append(row_id)

        update_data = data.take(expanded_input_indices).append_column(
            SpecialFields.ROW_ID.name, pa.array(row_ids, type=pa.int64()),
        )

        cols_to_update = list(update_cols) if update_cols else list(self.table.field_names)
        return TableUpdateByRowId(
            self.table, self.commit_user, self.commit_identifier,
        ).update_columns(update_data, cols_to_update)

    def _do_appends(
            self,
            data: pa.Table,
            new_indices: List[int],
    ) -> List[CommitMessage]:
        """
        Append rows that have no matching upsert key.

        New rows are always written with *all* columns — ``update_cols``
        only restricts which columns are rewritten for *matched* rows.

        :class:`StreamTableWrite` is used so the produced commit messages
        carry this upsert's ``commit_identifier``; in batch mode the
        identifier is :data:`BATCH_COMMIT_IDENTIFIER`, so the on-disk
        result is identical to using :class:`BatchTableWrite`.
        """
        new_data = data.take(new_indices)

        # Reorder columns to match table schema order
        all_ordered_cols = [c for c in self.table.field_names if c in new_data.column_names]
        new_data = new_data.select(all_ordered_cols)

        table_write = StreamTableWrite(self.table, self.commit_user)
        try:
            table_write.with_write_type(all_ordered_cols)
            table_write.write_arrow(new_data)
            return table_write.prepare_commit(self.commit_identifier)
        finally:
            table_write.close()

    def _append_rows(
            self,
            row_items: List[Tuple[Any, Dict[str, Any]]],
    ) -> List[CommitMessage]:
        all_ordered_cols = self._append_row_column_names(row_items)

        table_write = StreamTableWrite(self.table, self.commit_user)
        try:
            table_write.with_write_type(all_ordered_cols)
            for row, _ in row_items:
                table_write.write_row(row)
            return table_write.prepare_commit(self.commit_identifier)
        finally:
            table_write.close()

    def _append_row_column_names(
            self,
            row_items: List[Tuple[Any, Dict[str, Any]]],
    ) -> List[str]:
        first_field_names = set(row_items[0][1])
        for _, values_by_name in row_items[1:]:
            field_names = set(values_by_name)
            if field_names == first_field_names:
                continue

            missing_fields = [
                name for name in self.table.field_names
                if name in first_field_names and name not in field_names
            ]
            extra_fields = [
                name for name in self.table.field_names
                if name in field_names and name not in first_field_names
            ]
            raise ValueError(
                "upsert_by_key requires appended rows in the same batch to "
                "have the same field set. Compared with the first appended "
                f"row, missing fields: {missing_fields}; "
                f"extra fields: {extra_fields}."
            )
        return [
            name for name in self.table.field_names if name in first_field_names
        ]
