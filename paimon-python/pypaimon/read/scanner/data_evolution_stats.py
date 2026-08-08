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

from typing import Callable, Dict, List, Tuple

from pypaimon.common.predicate import Predicate
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.manifest.simple_stats_evolutions import SimpleStatsEvolutions
from pypaimon.schema.data_types import DataField
from pypaimon.table.row.generic_row import GenericRow


_KNOWN = 0
_MISSING = 1
_UNKNOWN = 2


class _FileLayout:

    def __init__(self, data_fields, stats_field_ids):
        self.data_fields = {field.id: field for field in data_fields}
        self.stats_field_ids = stats_field_ids


class DataEvolutionGroupStatsFilter:
    """Conservatively filters logical row-id groups by merged column stats."""

    def __init__(
        self,
        predicate: Predicate,
        table_fields: List[DataField],
        schema_fields: Callable[[int], List[DataField]],
        stats_evolutions: SimpleStatsEvolutions,
    ):
        self.predicate = predicate
        self.table_fields = table_fields
        self.schema_fields = schema_fields
        self.stats_evolutions = stats_evolutions
        self._layout_cache: Dict[Tuple, _FileLayout] = {}

    def may_match(self, files: List[DataFileMeta]) -> bool:
        if not files:
            return True
        try:
            stats, states, row_count = self._group_stats(files)
            return self._predicate_may_match(
                self.predicate, stats, states, row_count)
        except Exception:
            # Stats pruning is optional. Unknown schemas, corrupt stats, and
            # incompatible types must retain the complete logical group.
            return True

    def _group_stats(self, files):
        group_start = min(file.non_null_row_id_range().from_ for file in files)
        group_end = max(file.non_null_row_id_range().to for file in files)
        row_count = group_end - group_start + 1

        normal_files = []
        special_field_ids = set()
        for file in files:
            layout = self._layout(file)
            if DataFileMeta.is_blob_file(file.file_name) \
                    or DataFileMeta.is_vector_file(file.file_name):
                special_field_ids.update(layout.data_fields)
            else:
                normal_files.append((file, layout))
        normal_files.sort(key=lambda item: item[0].max_sequence_number,
                          reverse=True)

        min_values = []
        max_values = []
        null_counts = []
        states = []
        evolved_stats = {}
        for field_index, field in enumerate(self.table_fields):
            providers = [
                (file, layout)
                for file, layout in normal_files
                if field.id in layout.data_fields
            ]
            if not providers:
                if field.id in special_field_ids:
                    self._append_unknown(
                        min_values, max_values, null_counts, states)
                else:
                    min_values.append(None)
                    max_values.append(None)
                    null_counts.append(row_count)
                    states.append(_MISSING)
                continue

            file, layout = providers[0]
            latest_sequence = file.max_sequence_number
            if sum(1 for candidate, _ in providers
                   if candidate.max_sequence_number == latest_sequence) > 1:
                self._append_unknown(
                    min_values, max_values, null_counts, states)
                continue

            file_range = file.non_null_row_id_range()
            source_field = layout.data_fields[field.id]
            # Partial-file stats do not describe the complete logical group.
            if (file_range.from_ != group_start
                    or file_range.to != group_end
                    or source_field.type != field.type
                    or field.id not in layout.stats_field_ids):
                self._append_unknown(
                    min_values, max_values, null_counts, states)
                continue

            stats = evolved_stats.get(id(file))
            if stats is None:
                stats_fields = (
                    file.value_stats_cols
                    if file.value_stats_cols is not None
                    else file.write_cols
                )
                stats = self.stats_evolutions.get_or_create(
                    file.schema_id).evolution(
                        file.value_stats, file.row_count, stats_fields)
                evolved_stats[id(file)] = stats

            min_values.append(stats.min_values.get_field(field_index))
            max_values.append(stats.max_values.get_field(field_index))
            null_counts.append(
                stats.null_counts[field_index]
                if (stats.null_counts is not None
                    and field_index < len(stats.null_counts))
                else None
            )
            states.append(_KNOWN)

        return (
            SimpleStats(
                GenericRow(min_values, self.table_fields),
                GenericRow(max_values, self.table_fields),
                null_counts,
            ),
            states,
            row_count,
        )

    @staticmethod
    def _append_unknown(min_values, max_values, null_counts, states):
        min_values.append(None)
        max_values.append(None)
        null_counts.append(None)
        states.append(_UNKNOWN)

    def _layout(self, file):
        key = (
            file.schema_id,
            tuple(file.write_cols) if file.write_cols is not None else None,
            (tuple(file.value_stats_cols)
             if file.value_stats_cols is not None else None),
        )
        layout = self._layout_cache.get(key)
        if layout is not None:
            return layout

        schema_fields = self.schema_fields(file.schema_id)
        fields_by_name = {field.name: field for field in schema_fields}
        data_fields = (
            schema_fields
            if file.write_cols is None
            else [fields_by_name[name] for name in file.write_cols
                  if name in fields_by_name]
        )
        stats_fields = (
            data_fields
            if file.value_stats_cols is None
            else [fields_by_name[name] for name in file.value_stats_cols
                  if name in fields_by_name]
        )
        layout = _FileLayout(data_fields, {field.id for field in stats_fields})
        self._layout_cache[key] = layout
        return layout

    def _predicate_may_match(self, predicate, stats, states, row_count):
        if predicate.method == 'and':
            return all(
                self._predicate_may_match(child, stats, states, row_count)
                for child in predicate.literals
            )
        if predicate.method == 'or':
            return any(
                self._predicate_may_match(child, stats, states, row_count)
                for child in predicate.literals
            )

        index = predicate.index
        if index is None or index < 0 or index >= len(states):
            return True
        if states[index] == _UNKNOWN:
            return True
        if (states[index] == _MISSING
                or (stats.null_counts[index] is not None
                    and stats.null_counts[index] == row_count)):
            tester = Predicate.testers.get(predicate.method)
            return True if tester is None else tester.test_by_value(
                None, predicate.literals)
        return predicate.test_by_simple_stats(stats, row_count)
