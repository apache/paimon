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

from typing import Callable, Dict, List, Optional

from pypaimon.manifest.simple_stats_evolution import SimpleStatsEvolution
from pypaimon.schema.data_types import DataField


class SimpleStatsEvolutions:
    """Converters to create col stats array serializer."""

    def __init__(self, schema_fields: Callable[[int], List[DataField]], table_schema_id: int):
        self.schema_fields = schema_fields
        self.table_schema_id = table_schema_id
        self.table_data_fields = schema_fields(table_schema_id)
        self.table_fields = None
        self.evolutions: Dict[int, SimpleStatsEvolution] = {}

    def get_or_create(self, data_schema_id: int) -> SimpleStatsEvolution:
        """Get or create SimpleStatsEvolution for given schema id."""
        if data_schema_id in self.evolutions:
            return self.evolutions[data_schema_id]

        if self.table_schema_id == data_schema_id:
            evolution = SimpleStatsEvolution(self.schema_fields(data_schema_id), None, None)
        else:
            # TODO support schema evolution
            if self.table_fields is None:
                self.table_fields = self.table_data_fields

            data_fields = self.schema_fields(data_schema_id)
            index_cast_mapping = self._create_index_cast_mapping(self.table_fields, data_fields)
            index_mapping = index_cast_mapping.get('index_mapping')
            cast_mapping = index_cast_mapping.get('cast_mapping')

            evolution = SimpleStatsEvolution(data_fields, index_mapping, cast_mapping)

        self.evolutions[data_schema_id] = evolution
        return evolution

    def _create_index_cast_mapping(self, table_fields: List[DataField],
                                   data_fields: List[DataField]) -> Dict[str, Optional[List[int]]]:
        """
        Create index and cast mapping between table fields and data fields.
        This is a simplified implementation.
        """
        # Create a mapping from field names to indices in data_fields
        data_field_map = {field.name: i for i, field in enumerate(data_fields)}

        index_mapping = []
        for table_field in table_fields:
            if table_field.name in data_field_map:
                index_mapping.append(data_field_map[table_field.name])
            else:
                index_mapping.append(-1)  # Field not found in data schema

        return {
            'index_mapping': index_mapping,
            'cast_mapping': None  # Simplified - no casting for now
        }
