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

from typing import List, Optional, Dict, Any
import threading

from pypaimon.schema.data_types import DataField
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.projected_row import ProjectedRow


class SimpleStatsEvolution:
    """Converter for array of SimpleColStats."""

    def __init__(self, data_fields: List[DataField], index_mapping: Optional[List[int]],
                 cast_field_getters: Optional[List[Any]]):
        self.field_names = [field.name for field in data_fields]
        self.index_mapping = index_mapping
        self.cast_field_getters = cast_field_getters
        self.index_mappings: Dict[tuple, List[int]] = {}
        self._lock = threading.Lock()

        # Create empty values for optimization
        self.empty_values = GenericRow([None] * len(self.field_names), data_fields)
        self.empty_null_counts = [0] * len(self.field_names)

    def evolution(self, stats: SimpleStats, row_count: Optional[int],
                  stats_fields: Optional[List[str]]) -> 'SimpleStats':
        min_values = stats.min_values
        max_values = stats.max_values
        null_counts = stats.null_counts

        if stats_fields is not None and not stats_fields:
            # Optimize for empty dense fields
            min_values = self.empty_values
            max_values = self.empty_values
            null_counts = self.empty_null_counts
        elif stats_fields is not None:
            # Apply dense field mapping
            dense_index_mapping = self._get_dense_index_mapping(stats_fields)
            min_values = self._project_row(min_values, dense_index_mapping)
            max_values = self._project_row(max_values, dense_index_mapping)
            null_counts = self._project_array(null_counts, dense_index_mapping)

        if self.index_mapping is not None:
            min_values = self._project_row(min_values, self.index_mapping)
            max_values = self._project_row(max_values, self.index_mapping)

            if row_count is None:
                raise RuntimeError("Schema Evolution for stats needs row count.")

            null_counts = self._evolve_null_counts(null_counts, self.index_mapping, row_count)

        return SimpleStats(min_values, max_values, null_counts)

    def _get_dense_index_mapping(self, dense_fields: List[str]) -> List[int]:
        """
        Get dense index mapping similar to Java:
        fieldNames.stream().mapToInt(denseFields::indexOf).toArray()
        """
        dense_fields_tuple = tuple(dense_fields)

        if dense_fields_tuple not in self.index_mappings:
            with self._lock:
                # Double-check locking
                if dense_fields_tuple not in self.index_mappings:
                    mapping = []
                    for field_name in self.field_names:
                        try:
                            index = dense_fields.index(field_name)
                            mapping.append(index)
                        except ValueError:
                            mapping.append(-1)  # Field not found
                    self.index_mappings[dense_fields_tuple] = mapping

        return self.index_mappings[dense_fields_tuple]

    def _project_row(self, row: Any, index_mapping: List[int]) -> Any:
        """Project row based on index mapping using ProjectedRow."""
        projected_row = ProjectedRow.from_index_mapping(index_mapping)
        return projected_row.replace_row(row)

    def _project_array(self, array: List[Any], index_mapping: List[int]) -> List[Any]:
        """Project array based on index mapping."""
        if not array:
            return [0] * len(index_mapping)

        projected = []
        for mapped_index in index_mapping:
            if mapped_index >= 0 and mapped_index < len(array):
                projected.append(array[mapped_index])
            else:
                projected.append(0)  # Default value for missing fields

        return projected

    def _evolve_null_counts(self, null_counts: List[Any], index_mapping: List[int],
                            not_found_value: int) -> List[Any]:
        """Evolve null counts with schema evolution mapping."""
        evolved = []
        for mapped_index in index_mapping:
            if mapped_index >= 0 and mapped_index < len(null_counts):
                evolved.append(null_counts[mapped_index])
            else:
                evolved.append(not_found_value)  # Use row count for missing fields

        return evolved
