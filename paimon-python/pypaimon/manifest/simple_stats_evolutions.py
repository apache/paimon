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

from typing import Callable, Dict, List, Optional

from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.manifest.simple_stats_evolution import SimpleStatsEvolution
from pypaimon.schema.data_types import AtomicType, DataField


class SimpleStatsEvolutions:
    """Converters to create col stats array serializer."""

    def __init__(self, schema_fields: Callable[[int], List[DataField]], table_schema_id: int):
        self.schema_fields = schema_fields
        self.table_schema_id = table_schema_id
        self.table_fields = schema_fields(table_schema_id)
        self.evolutions: Dict[int, SimpleStatsEvolution] = {}

    def get_or_create(self, data_schema_id: int) -> SimpleStatsEvolution:
        """Get or create SimpleStatsEvolution for given schema id."""
        if data_schema_id in self.evolutions:
            return self.evolutions[data_schema_id]

        if self.table_schema_id == data_schema_id:
            evolution = SimpleStatsEvolution(self.schema_fields(data_schema_id), None, None)
        else:
            data_fields = self.schema_fields(data_schema_id)
            index_cast_mapping = self._create_index_cast_mapping(self.table_fields, data_fields)
            index_mapping = index_cast_mapping.get('index_mapping')
            cast_mapping = index_cast_mapping.get('cast_mapping')
            evolution = SimpleStatsEvolution(data_fields, index_mapping, cast_mapping)

        self.evolutions[data_schema_id] = evolution
        return evolution

    def filter_unsafe_filter(self, data_schema_id: int,
                             predicate: Optional[Predicate]) -> Optional[Predicate]:
        """Keep evolution-safe conjuncts, like Java's filterUnsafeFilter.

        Integer narrowing can wrap and precision changes can truncate values.
        Comparing current literals to those files' original bounds is unsafe.
        An OR must stay intact: dropping one of its branches could lose rows.
        """
        if predicate is None or data_schema_id == self.table_schema_id:
            return predicate
        source_fields = {field.id: field for field in self.schema_fields(data_schema_id)}

        def safe(part):
            if part.method in ('and', 'or'):
                return all(safe(child) for child in part.literals)
            if part.index is None or not 0 <= part.index < len(self.table_fields):
                return False
            target = self.table_fields[part.index]
            source = source_fields.get(target.id)
            if source is None:
                # Missing fields remain NULL in evolved stats.
                return True
            source_type, target_type = source.type, target.type
            if isinstance(source_type, AtomicType) and isinstance(target_type, AtomicType):
                if source_type.type == target_type.type:
                    return True
                widths = {'TINYINT': 8, 'SMALLINT': 16, 'INT': 32, 'BIGINT': 64}
                source_width = widths.get(source_type.type)
                target_width = widths.get(target_type.type)
                if source_width and target_width and source_width < target_width:
                    # Java also rejects literals that cannot be represented in
                    # the old type. Stats values need no conversion in Python.
                    bound = 1 << (source_width - 1)
                    return all(value is not None and -bound <= value < bound
                               for value in (part.literals or []))
                return False
            return source_type == target_type

        def conjuncts(part):
            if part.method == 'and':
                return [child for item in part.literals for child in conjuncts(item)]
            return [part]

        return PredicateBuilder.and_predicates([part for part in conjuncts(predicate) if safe(part)])

    @staticmethod
    def _create_index_cast_mapping(table_fields: List[DataField],
                                   data_fields: List[DataField]) -> Dict[str, Optional[List[int]]]:
        """
        Create index and cast mapping between table fields and data fields.
        This is a simplified implementation.
        """
        # Create a mapping from field IDs to indices in data_fields.
        # Field IDs are stable across schema evolution (including column renames),
        # while field names may change.
        data_field_map = {field.id: i for i, field in enumerate(data_fields)}

        index_mapping = []
        for table_field in table_fields:
            if table_field.id in data_field_map:
                index_mapping.append(data_field_map[table_field.id])
            else:
                index_mapping.append(-1)  # Field not found in data schema

        return {
            'index_mapping': index_mapping,
            'cast_mapping': None  # Simplified - no casting for now
        }
