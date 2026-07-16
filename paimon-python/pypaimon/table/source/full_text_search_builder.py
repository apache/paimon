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

"""Builder to build full-text search."""

from abc import ABC, abstractmethod
from typing import Optional

from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.table.source.full_text_read import FullTextRead, DataEvolutionFullTextRead
from pypaimon.table.source.full_text_scan import FullTextScan, DataEvolutionFullTextScan
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.index.pk.primary_key_index_definition import PrimaryKeyIndexFamily
from pypaimon.index.pk.primary_key_index_definitions import PrimaryKeyIndexDefinitions


class FullTextSearchBuilder(ABC):
    """Builder to build full-text search."""

    @abstractmethod
    def with_limit(self, limit: int) -> 'FullTextSearchBuilder':
        """The top k results to return."""
        pass

    @abstractmethod
    def with_query(self, field_name: str, query: str) -> 'FullTextSearchBuilder':
        """The full-text query string to search against the given field."""
        pass

    @abstractmethod
    def with_partition_filter(self, partition_filter) -> 'FullTextSearchBuilder':
        """Partition predicate used to prune index manifest entries."""
        pass

    @abstractmethod
    def new_full_text_scan(self) -> FullTextScan:
        """Create full-text scan to scan index files."""
        pass

    @abstractmethod
    def new_full_text_read(self) -> FullTextRead:
        """Create full-text read to read index files."""
        pass

    def execute_local(self) -> GlobalIndexResult:
        """Execute full-text index search in local."""
        return self.new_full_text_read().read_plan(self.new_full_text_scan().scan())


class FullTextSearchBuilderImpl(FullTextSearchBuilder):
    """Implementation for FullTextSearchBuilder."""

    def __init__(self, table: 'FileStoreTable'):
        self._table = table
        self._limit: int = 0
        self._field_name: Optional[str] = None
        self._query: Optional[str] = None
        self._partition_filter = None

    def with_limit(self, limit: int) -> 'FullTextSearchBuilder':
        self._limit = limit
        return self

    def with_query(self, field_name: str, query: str) -> 'FullTextSearchBuilder':
        self._field_name = field_name
        self._query = query
        return self

    def with_partition_filter(self, partition_filter) -> 'FullTextSearchBuilder':
        if partition_filter is None:
            self._partition_filter = None
            return self
        partition_keys = list(self._table.partition_keys or [])
        if not partition_keys:
            raise ValueError(
                "with_partition_filter called on a non-partitioned table")
        from pypaimon.read.push_down_utils import _get_all_fields
        referenced = _get_all_fields(partition_filter)
        extras = referenced - set(partition_keys)
        if extras:
            raise ValueError(
                "Partition filter must reference only partition keys "
                "(%s); got non-partition field(s): %s"
                % (partition_keys, sorted(extras)))
        rebuilt = self._rebuild_leaf_indices_by_name(
            partition_filter,
            {name: idx for idx, name in enumerate(partition_keys)},
        )
        if self._partition_filter is None:
            self._partition_filter = rebuilt
        else:
            self._partition_filter = PredicateBuilder.and_predicates(
                [self._partition_filter, rebuilt])
        return self

    @classmethod
    def _rebuild_leaf_indices_by_name(cls, predicate, name_to_idx):
        if predicate.method in ('and', 'or'):
            return predicate.new_literals(
                [cls._rebuild_leaf_indices_by_name(c, name_to_idx)
                 for c in (predicate.literals or [])])
        return predicate.new_index(name_to_idx[predicate.field])

    def new_full_text_scan(self) -> FullTextScan:
        definition = self._primary_key_full_text_definition()
        if definition is not None:
            from pypaimon.table.source.primary_key_full_text_scan import PrimaryKeyFullTextScan
            return PrimaryKeyFullTextScan(
                self._table, definition, partition_filter=self._partition_filter)
        return DataEvolutionFullTextScan(
            self._table,
            self._text_columns(),
            partition_filter=self._partition_filter,
        )

    def new_full_text_read(self) -> FullTextRead:
        if self._limit <= 0:
            raise ValueError("Limit must be positive, set via with_limit()")
        definition = self._primary_key_full_text_definition()
        if definition is not None:
            from pypaimon.table.source.primary_key_full_text_read import PrimaryKeyFullTextRead
            return PrimaryKeyFullTextRead(
                self._table, self._limit, self._text_columns(), self._query,
                definition=definition, partition_filter=self._partition_filter)
        return DataEvolutionFullTextRead(
            self._table,
            self._limit,
            self._text_columns(),
            self._query,
            partition_filter=self._partition_filter,
        )

    def _text_columns(self):
        if self._query is None:
            raise ValueError("Query must be set via with_query()")
        if self._field_name is None:
            raise ValueError("Field name must be set via with_query()")
        field_dict = {f.name: f for f in self._table.fields}
        if self._field_name not in field_dict:
            raise ValueError(
                f"Text column '{self._field_name}' not found in table schema")
        return [field_dict[self._field_name]]

    def _primary_key_full_text_definition(self):
        text_column = self._text_columns()[0]
        core = CoreOptions(Options(dict(self._table.table_schema.options)))
        if (core.data_evolution_enabled()
                or not core.primary_key_full_text_index_columns()):
            return None
        for definition in PrimaryKeyIndexDefinitions.create(
                self._table.table_schema).definitions:
            if (definition.family == PrimaryKeyIndexFamily.FULL_TEXT
                    and definition.field_id == text_column.id):
                return definition
        return None
