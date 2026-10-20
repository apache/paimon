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

"""Public global index build API and shared request validation."""

from typing import Dict, List, Optional, Sequence, Union

import pyarrow as pa

from pypaimon.table.row.generic_row import GenericRow
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.utils.range import Range

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.globalindex.index_types import BTREE_IDENTIFIER, BITMAP_IDENTIFIER
from pypaimon.globalindex.full_text_options import FULL_TEXT_IDENTIFIER
from pypaimon.globalindex.vindex_options import VINDEX_IDENTIFIERS
from pypaimon.write.commit_message import CommitMessage


def create_global_index(
    table,
    index_column: Union[str, Sequence[str]],
    index_type: str = BTREE_IDENTIFIER,
    partition_filter: Optional[Predicate] = None,
    partitions: Optional[Union[Dict[str, object], Sequence[Dict[str, object]]]] = None,
    options: Optional[Dict[str, object]] = None,
    *,
    execution="local",
    concurrency=None,
    ray_remote_args=None,
) -> int:
    """Build and commit global index files for a table.

    Returns the number of index files added to the table snapshot.
    """

    builder = GlobalIndexBuilder(
        table,
        index_column,
        index_type=index_type,
        partition_filter=partition_filter,
        partitions=partitions,
        options=options,
    )
    messages = builder.build(
        execution=execution, concurrency=concurrency, ray_remote_args=ray_remote_args)
    if not messages:
        return 0

    write_builder = table.new_batch_write_builder()
    commit = write_builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    return sum(len(message.index_adds) for message in messages)


_SORTED_INDEX_IDENTIFIERS = (BTREE_IDENTIFIER, BITMAP_IDENTIFIER)
_GENERIC_INDEX_IDENTIFIERS = tuple(VINDEX_IDENTIFIERS) + (FULL_TEXT_IDENTIFIER,)


class GlobalIndexBuilder:
    """Build global index files with the configured execution backend."""

    def __init__(
        self,
        table,
        index_column: Union[str, Sequence[str]],
        index_type: str = BTREE_IDENTIFIER,
        partition_filter: Optional[Predicate] = None,
        partitions: Optional[Union[Dict[str, object], Sequence[Dict[str, object]]]] = None,
        options: Optional[Dict[str, object]] = None,
    ):
        self._table = table
        self._index_columns = _normalize_index_columns(index_column)
        self._index_type = index_type.lower().strip()
        self._partition_filter = partition_filter
        self._partitions = partitions
        self._user_options = dict(options or {})
        self._options = _merged_options(table, options)
        self._core_options = CoreOptions(self._options)

        if (
            self._index_type not in _SORTED_INDEX_IDENTIFIERS
            and self._index_type not in _GENERIC_INDEX_IDENTIFIERS
        ):
            raise ValueError(
                "Python global index build currently supports %s and %s, got '%s'."
                % (
                    _SORTED_INDEX_IDENTIFIERS,
                    _GENERIC_INDEX_IDENTIFIERS,
                    index_type,
                )
            )
        if len(self._index_columns) != 1:
            raise ValueError(
                "Python global index build currently supports one column, got %s."
                % self._index_columns
            )
        if not self._table.options.row_tracking_enabled():
            raise ValueError(
                "Table '%s' must enable 'row-tracking.enabled=true' before "
                "creating global index." % self._table.identifier
            )
        for column in self._index_columns:
            if column not in self._table.field_dict:
                raise ValueError(
                    "Column '%s' does not exist in table '%s'."
                    % (column, self._table.identifier)
                )
        self._validate_unaware_bucket_table()
        if self._index_type in _GENERIC_INDEX_IDENTIFIERS:
            self._validate_generic_index_table()

    def build(self, *, execution="local", concurrency=None, ray_remote_args=None) -> List[CommitMessage]:
        if execution not in ("local", "ray"):
            raise ValueError("execution must be 'local' or 'ray'.")
        if execution == "local" and (concurrency is not None or ray_remote_args is not None):
            raise ValueError("Ray options require execution='ray'.")
        if execution == "ray":
            from pypaimon.ray.vector_index_build import validate_build_options
            concurrency, ray_remote_args = validate_build_options(
                self, concurrency, ray_remote_args)
        partition_filter = self._resolve_partition_filter()
        if execution == "local":
            from pypaimon.execution.native.write.native_index_build import build_native_global_index
            messages = build_native_global_index(self, partition_filter)
            if messages is not None:
                return messages
        from pypaimon.execution.python.globalindex.create_global_index import build_global_index
        return build_global_index(
            self, partition_filter, execution=execution, concurrency=concurrency,
            ray_remote_args=ray_remote_args)

    def _snapshot_for_plan(self, plan):
        from pypaimon.execution.python.globalindex.create_global_index import PythonGlobalIndexBuilder
        return PythonGlobalIndexBuilder._snapshot_for_plan(self, plan)

    def _build_sorted_index(
        self, splits, unindexed_ranges, index_field, table_read, index_path: str
    ) -> List[CommitMessage]:
        from pypaimon.execution.python.globalindex.create_global_index import PythonGlobalIndexBuilder
        return PythonGlobalIndexBuilder._build_sorted_index(
            self, splits, unindexed_ranges, index_field, table_read, index_path)

    def _create_sorted_index_writer(self, index_path: str, key_serializer):
        from pypaimon.execution.python.globalindex.create_global_index import PythonGlobalIndexBuilder
        return PythonGlobalIndexBuilder._create_sorted_index_writer(self, index_path, key_serializer)

    def _build_generic_index(
        self, splits, unindexed_ranges, index_field, table_read, index_path: str
    ) -> List[CommitMessage]:
        from pypaimon.execution.python.globalindex.create_global_index import PythonGlobalIndexBuilder
        return PythonGlobalIndexBuilder._build_generic_index(
            self, splits, unindexed_ranges, index_field, table_read, index_path)

    def _build_generic_shard(
        self, index_split, index_range, index_field, table_read, index_path: str, file_name=None
    ) -> Optional[CommitMessage]:
        from pypaimon.execution.python.globalindex.create_global_index import PythonGlobalIndexBuilder
        return PythonGlobalIndexBuilder._build_generic_shard(
            self, index_split, index_range, index_field, table_read, index_path, file_name)

    def _delete_writer_output(self, writer, index_path: str) -> None:
        from pypaimon.execution.python.globalindex.create_global_index import PythonGlobalIndexBuilder
        return PythonGlobalIndexBuilder._delete_writer_output(self, writer, index_path)

    def _delete_uncommitted_indexes(self, messages) -> None:
        from pypaimon.execution.python.globalindex.create_global_index import PythonGlobalIndexBuilder
        return PythonGlobalIndexBuilder._delete_uncommitted_indexes(self, messages)

    def _create_generic_index_writer(self, index_path: str, index_field):
        from pypaimon.execution.python.globalindex.create_global_index import PythonGlobalIndexBuilder
        return PythonGlobalIndexBuilder._create_generic_index_writer(self, index_path, index_field)

    def _validate_unaware_bucket_table(self) -> None:
        # Global index files live in bucket 0 and are keyed by the table-wide
        # _ROW_ID, so every index type needs an unaware-bucket table. Check the
        # table options: caller options only tune the index build.
        bucket = self._table.options.bucket()
        if bucket != -1:
            raise ValueError(
                "Global index only supports unaware-bucket tables "
                "(bucket = -1), but table '%s' has bucket = %s."
                % (self._table.identifier, bucket)
            )

    def _validate_generic_index_table(self) -> None:
        if (self._table.options.deletion_vectors_enabled()
                and not (self._index_type in VINDEX_IDENTIFIERS
                         and self._table.options.data_evolution_enabled())):
            raise ValueError(
                "Global index build with deletion vectors requires a native "
                "vector index on a data-evolution table. Table '%s', index type '%s'."
                % (self._table.identifier, self._index_type)
            )

    def _resolve_partition_filter(self) -> Optional[Predicate]:
        if self._partition_filter is not None:
            return self._partition_filter
        if self._partitions is None:
            return None

        partitions = self._partitions
        if isinstance(partitions, dict):
            partitions = [partitions]

        predicate_builder = PredicateBuilder(self._table.partition_keys_fields)
        partition_predicates = []
        for partition in partitions:
            sub_predicates = []
            for key, value in partition.items():
                if key not in self._table.partition_keys:
                    raise ValueError(
                        "Partition spec key '%s' is not a partition column. "
                        "Partition keys are: %s"
                        % (key, list(self._table.partition_keys))
                    )
                if value is None:
                    sub_predicates.append(predicate_builder.is_null(key))
                else:
                    sub_predicates.append(predicate_builder.equal(key, value))
            if sub_predicates:
                partition_predicates.append(
                    predicate_builder.and_predicates(sub_predicates))
        return predicate_builder.or_predicates(partition_predicates)


def _normalize_index_columns(index_column: Union[str, Sequence[str]]) -> List[str]:
    if isinstance(index_column, str):
        return [c.strip() for c in index_column.split(",") if c.strip()]
    return [str(c).strip() for c in index_column if str(c).strip()]


def _merged_options(table, options: Optional[Dict[str, object]]) -> Options:
    merged = dict(table.options.options.to_map())
    if options:
        merged.update(options)
    return Options(merged)


def _extract_sorted_rows(
    table: pa.Table,
    index_column: str,
    row_id_column: str,
    key_serializer,
    row_range: Optional[Range] = None,
):
    from pypaimon.execution.python.globalindex.create_global_index import _extract_sorted_rows as execute
    return execute(table, index_column, row_id_column, key_serializer, row_range)


def _write_vector_batch(writer, batch, index_column, row_range):
    from pypaimon.execution.python.globalindex.create_global_index import _write_vector_batch as execute
    return execute(writer, batch, index_column, row_range)


def _extract_index_rows(
    table: Union[pa.Table, pa.RecordBatch],
    index_column: str,
    row_id_column: str,
    row_range: Optional[Range] = None,
):
    from pypaimon.execution.python.globalindex.create_global_index import _extract_index_rows as execute
    return execute(table, index_column, row_id_column, row_range)


def _chunks(rows, size):
    from pypaimon.execution.python.globalindex.create_global_index import _chunks as execute
    return execute(rows, size)


def _to_index_manifest_entries(
    table,
    partition: GenericRow,
    row_range: Range,
    index_field_id: int,
    index_type: str,
    result_entries,
) -> List[IndexManifestEntry]:
    from pypaimon.execution.python.globalindex.create_global_index import _to_index_manifest_entries as execute
    return execute(table, partition, row_range, index_field_id, index_type, result_entries)
