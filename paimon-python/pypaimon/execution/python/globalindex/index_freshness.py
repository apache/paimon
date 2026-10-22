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

"""Freshness checks for global indexes on data-evolution tables."""

import logging

from pypaimon.common.options.core_options import CoreOptions, GlobalIndexColumnUpdateAction
from pypaimon.table.special_fields import SpecialFields

logger = logging.getLogger(__name__)


def key_indexes_are_fresh(table, snapshot, match_keys, partition_predicate):
    """Check whether IGNORE may have left indexed match keys stale."""
    if table.options.global_index_column_update_action() != GlobalIndexColumnUpdateAction.IGNORE:
        return True
    if snapshot is None or not match_keys:
        return True
    from pypaimon.execution.python.globalindex.data_evolution_global_index_scanner import (
        is_supported_scalar_index,
    )
    from pypaimon.index.data_evolution_index_source_meta import DataEvolutionIndexSourceMeta
    from pypaimon.index.index_file_handler import IndexFileHandler
    from pypaimon.read.push_down_utils import trim_and_transform_predicate
    from pypaimon.utils.range import Range

    try:
        key_ids = {table.field_dict[key].id for key in match_keys}
        partition_filter = trim_and_transform_predicate(
            partition_predicate, table.field_names, table.partition_keys)

        def relevant_index(entry):
            meta = entry.index_file.global_index_meta
            return (is_supported_scalar_index(entry.index_file)
                    and bool(key_ids.intersection(
                        [meta.index_field_id] + (meta.extra_field_ids or [])))
                    and (partition_filter is None or partition_filter.test(entry.partition)))

        indexes = IndexFileHandler(table).scan(snapshot, relevant_index)
        if not indexes:
            return True
        baselines = []
        for index in indexes:
            meta = index.index_file.global_index_meta
            source = DataEvolutionIndexSourceMeta.deserialize(meta.source_meta)
            if source.scan_snapshot_id > snapshot.id or meta.row_range_start > meta.row_range_end:
                return False
            baselines.append((index, source.scan_snapshot_id))

        # Inspect active file metadata before either key stats or an index can
        # hide the files containing changed keys. Both plans use this snapshot.
        builder = table.copy_without_time_travel({
            CoreOptions.GLOBAL_INDEX_ENABLED.key(): 'false',
        }).new_read_builder().with_filter(partition_predicate)
        files_by_group = {}
        for split in builder.new_scan().plan_all().splits():
            files = files_by_group.setdefault((tuple(split.partition.values), split.bucket), {})
            files.update((file.file_name, file) for file in split.files)

        schema = table.table_schema
        known_names = set(table.field_names) | SpecialFields.SYSTEM_FIELD_NAMES
        for index, baseline in baselines:
            meta = index.index_file.global_index_meta
            indexed_keys = key_ids.intersection([meta.index_field_id] + (meta.extra_field_ids or []))
            index_range = Range(meta.row_range_start, meta.row_range_end)
            for file in files_by_group.get((tuple(index.partition.values), index.bucket), {}).values():
                row_range = file.row_id_range()
                if row_range is None:
                    return False
                if not index_range.overlaps(row_range):
                    continue
                # Historical schemas are outside this optimization's proof.
                if file.schema_id != schema.id:
                    return False
                if file.write_cols is not None and (
                        any(name not in known_names for name in file.write_cols)
                        or len(file.write_cols) != len(set(file.write_cols))):
                    return False
                fields = schema.data_file_fields(file.write_cols)
                sequences = file.write_cols_sequences
                for position, field in enumerate(fields):
                    if field.id not in indexed_keys:
                        continue
                    sequence = (sequences[position]
                                if sequences is not None and len(sequences) == len(fields)
                                else file.max_sequence_number)
                    if sequence <= 0 or sequence > file.max_sequence_number or sequence > baseline:
                        return False
        return True
    except Exception:
        logger.debug("Cannot verify upsert key index freshness; scanning without indexes.", exc_info=True)
        return False
