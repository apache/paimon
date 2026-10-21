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

"""Drop global index files from Python."""

from typing import Dict, List, Optional, Sequence, Union

from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.globalindex.index_types import BTREE_IDENTIFIER
from pypaimon.write.commit_message import CommitMessage


def drop_global_index(
    table,
    index_column: Union[str, Sequence[str]],
    index_type: str = BTREE_IDENTIFIER,
    partition_filter: Optional[Predicate] = None,
    partitions: Optional[Union[Dict[str, object], Sequence[Dict[str, object]]]] = None,
    dry_run: bool = False,
) -> int:
    """Drop global index metadata for a table.

    Returns the number of matched index files. When ``dry_run`` is true, no
    commit is created.
    """

    dropper = GlobalIndexDropper(
        table,
        index_column,
        index_type=index_type,
        partition_filter=partition_filter,
        partitions=partitions,
    )
    messages = dropper.build()
    dropped = sum(len(message.index_deletes) for message in messages)
    if dry_run or not messages:
        return dropped

    write_builder = table.new_batch_write_builder()
    commit = write_builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    return dropped


class GlobalIndexDropper:
    """Small Python dropper for global index manifest entries."""

    def __init__(
        self,
        table,
        index_column: Union[str, Sequence[str]],
        index_type: str = BTREE_IDENTIFIER,
        partition_filter: Optional[Predicate] = None,
        partitions: Optional[Union[Dict[str, object], Sequence[Dict[str, object]]]] = None,
    ):
        self._table = table
        self._index_columns = _normalize_index_columns(index_column)
        self._index_type = index_type.lower().strip()
        self._partition_filter = partition_filter
        self._partitions = partitions

        if not self._index_columns:
            raise ValueError("At least one index column is required.")
        for column in self._index_columns:
            if column not in self._table.field_dict:
                raise ValueError(
                    "Column '%s' does not exist in table '%s'."
                    % (column, self._table.identifier)
                )

    def build(self) -> List[CommitMessage]:
        partition_filter = self._resolve_partition_filter()
        from pypaimon.globalindex.index_dispatch import build_global_index_drop
        return build_global_index_drop(self, partition_filter)

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
