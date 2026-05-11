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

import logging
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa

from pypaimon.read.table_read import TableRead
from pypaimon.table.special_fields import SpecialFields
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
from pypaimon.write.table_write import StreamTableWrite

# Composite key is represented as a tuple of values
_KeyTuple = Tuple[Any, ...]

logger = logging.getLogger(__name__)


class TableUpsertByKey:
    """
    Table upsert by one or more user-specified key columns for append-only tables.

    For each row in the input Arrow table:
    - If a row with the same upsert_keys composite value already exists → update that row
      (in-place rewrite).
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

        # 3. Scan partition once, keeping only key → _ROW_ID pairs that
        #    appear in the input (memory ∝ |input|, not |partition|).
        key_to_row_id = self._build_key_to_row_id_map(
            match_keys, partition_spec, set(input_key_tuples),
        )

        # 4. Partition input rows into matched (update) vs unmatched (append).
        matched_indices: List[int] = []
        new_indices: List[int] = []
        for i, key_tuple in enumerate(input_key_tuples):
            (matched_indices if key_tuple in key_to_row_id else new_indices).append(i)

        logger.info(
            "Upserting partition %s: %d matched, %d new",
            partition_spec, len(matched_indices), len(new_indices),
        )

        commit_messages: List[CommitMessage] = []
        if matched_indices:
            commit_messages.extend(self._do_updates(
                partition_data, matched_indices,
                input_key_tuples, key_to_row_id, update_cols,
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

    def _build_key_to_row_id_map(
            self,
            match_keys: List[str],
            partition_spec: Optional[Dict[str, Any]],
            input_key_set: set,
    ) -> Dict[_KeyTuple, int]:
        """
        Scan the partition in batches and collect key → _ROW_ID only for
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
        key_to_row_id: Dict[_KeyTuple, int] = {}
        row_id_col = SpecialFields.ROW_ID.name
        for batch in table_read.to_arrow_batch_reader(splits):
            batch_key_cols = [batch.column(k).to_pylist() for k in match_keys]
            batch_row_ids = batch.column(row_id_col).to_pylist()
            for j, row_id in enumerate(batch_row_ids):
                key_tuple = tuple(col[j] for col in batch_key_cols)
                if key_tuple in input_key_set:
                    key_to_row_id[key_tuple] = row_id

        return key_to_row_id

    def _do_updates(
            self,
            data: pa.Table,
            matched_indices: List[int],
            input_key_tuples: List[_KeyTuple],
            key_to_row_id: Dict[_KeyTuple, int],
            update_cols: Optional[List[str]]
    ) -> List[CommitMessage]:
        """Update matched rows by rewriting them in-place via
        :class:`TableUpdateByRowId`."""
        matched_data = data.take(matched_indices)
        row_id_array = pa.array(
            [key_to_row_id[input_key_tuples[i]] for i in matched_indices],
            type=pa.int64(),
        )
        update_data = matched_data.append_column(
            SpecialFields.ROW_ID.name, row_id_array,
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
