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

from collections.abc import Mapping
from typing import Dict, List, Optional, Sequence, Union

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.common.options.vector_index_options import VectorIndexOptions
from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.globalindex.index_types import BTREE_IDENTIFIER, BITMAP_IDENTIFIER
from pypaimon.globalindex.full_text_options import FULL_TEXT_IDENTIFIER
from pypaimon.globalindex.vindex_options import VINDEX_IDENTIFIERS, is_multi_vector_type
from pypaimon.write.commit_message import CommitMessage


def create_global_index(
    table,
    index_column: Union[str, Sequence[str]],
    index_type: str = BTREE_IDENTIFIER,
    partition_filter: Optional[Predicate] = None,
    partitions: Optional[Union[Dict[str, object], Sequence[Dict[str, object]]]] = None,
    options: Optional[Dict[str, object]] = None,
    *,
    extra_fields=None,
    execution="local",
    concurrency=None,
    ray_remote_args=None,
) -> int:
    """Build and commit global index files for a table.

    ``extra_fields`` maps scalar columns to ``bitmap`` for centroid-sharded
    vector indexes. It can be a mapping or a JSON object string.
    Returns the number of index files added to the table snapshot.
    """

    builder = GlobalIndexBuilder(
        table,
        index_column,
        index_type=index_type,
        partition_filter=partition_filter,
        partitions=partitions,
        options=options,
        extra_fields=extra_fields,
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
        *,
        extra_fields=None,
    ):
        self._table = table
        self._index_columns = _normalize_index_columns(index_column)
        self._index_type = index_type.lower().strip()
        self._partition_filter = partition_filter
        self._partitions = partitions
        self._user_options = dict(options or {})
        if self._index_type in VINDEX_IDENTIFIERS:
            self._user_options = _vector_procedure_options(table, self._user_options)
        if extra_fields is not None:
            if self._user_options.get("extra_fields") is not None:
                raise ValueError("Specify extra_fields only once.")
            self._user_options["extra_fields"] = (
                dict(extra_fields) if isinstance(extra_fields, Mapping) else extra_fields)
        self._options = _merged_options(table, self._user_options)

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
        column = self._index_columns[0]
        self._centroid_sharding = False
        if self._index_type in VINDEX_IDENTIFIERS:
            self._options, layout = self._resolve_vector_build_options(column)
            self._centroid_sharding = layout == "centroid"
        else:
            self._options = CoreOptions(self._options).global_index_options(self._index_type, column)
        self._core_options = CoreOptions(self._options)
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
        self._multi_vector = (self._index_type in VINDEX_IDENTIFIERS
                              and is_multi_vector_type(self._table.field_dict[column].type))
        if self._user_options.get("extra_fields") is not None and not self._centroid_sharding:
            raise ValueError("extra_fields requires a centroid-sharded vector index.")

    def _resolve_vector_build_options(self, column):
        # Keep flat procedure overrides separate from the table's field JSON.
        # A procedure can also replace a field JSON definition in the defaults.
        table_values = dict(self._table.options.options.to_map())
        field_keys = CoreOptions._field_index_option_keys(self._index_type, column)
        for key, value in self._user_options.items():
            if value is None:
                table_values.pop(key, None)
            elif key in field_keys:
                table_values[key] = value
        defaults = VectorIndexOptions.resolve(Options(table_values), self._index_type, column).options.to_map()
        # Rust treats only values introduced by a field declaration as field defaults.
        declared = {key: value for key, value in defaults.items() if value != table_values.get(key)}
        explicit_values = {key: value for key, value in self._user_options.items() if value is not None}
        explicit = VectorIndexOptions.resolve(
            Options(explicit_values), self._index_type, column, default_metric=False).options.to_map()
        field_key = "fields.%s.sharding" % column
        index_key = self._index_type + ".sharding"
        layout = next((str(value).strip() for value in (
            explicit.get(field_key), explicit.get(index_key), explicit.get("sharding"),
            declared.get(field_key), declared.get(index_key), declared.get("sharding"),
            table_values.get(field_key), table_values.get(index_key),
        ) if value is not None), "range")
        if layout not in ("range", "centroid"):
            raise ValueError("%s must be range or centroid, got %s" % (index_key, layout))
        return Options({**defaults, **explicit}), layout

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
        from pypaimon.globalindex.index_dispatch import build_global_index
        return build_global_index(
            self, partition_filter, execution=execution, concurrency=concurrency,
            ray_remote_args=ray_remote_args)

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
            from pypaimon.read.push_down_utils import rewrite_predicate_indices

            self._table.new_read_builder().with_partition_filter(self._partition_filter)
            return rewrite_predicate_indices(self._partition_filter, self._table.partition_keys_fields)
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


def _vector_procedure_options(table, options):
    # A caller can echo the effective table options. Unchanged values keep
    # their precedence as schema options, as in the Rust vector builder.
    table_values = table.options.options.to_map()
    keys = {key for key, value in table_values.items()
            if value is not None and not key.startswith(('scan.', 'incremental-'))
            and (key not in options or options[key] is not None)}
    if keys and keys <= options.keys():
        return {key: value for key, value in options.items()
                if value is None or value != table_values.get(key)}
    return options
