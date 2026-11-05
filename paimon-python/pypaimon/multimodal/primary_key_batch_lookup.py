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

"""Share physical-position reads across primary-key vector query results."""

from copy import copy

import pyarrow as pa

from pypaimon.execution.python.deletionvectors.deletion_vector import DeletionVector
from pypaimon.execution.python.index.pk.primary_key_index_source_policy import should_read
from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.table.source.primary_key_scored_result import (
    PrimaryKeyScoredResult, _partition_bytes, _ranges,
)
from pypaimon.utils.arrow_utils import zero_column_batch


def _take(rows, indices):
    if not rows.num_columns:
        return pa.Table.from_batches([zero_column_batch(len(indices), rows.schema.metadata)])
    return rows.take(pa.array(indices, type=pa.int64()))


def read_primary_key_batch(query, results):
    """Return shared lookup tables, or None when position mapping is unavailable."""
    table = query._table
    if (not table.is_primary_key_table or query._predicate is not None
            or table.options.query_auth_enabled or table.options.native_read_enabled()
            or not all(isinstance(result, PrimaryKeyScoredResult) for result in results)
            or len({result.snapshot_id for result in results}) != 1):
        return None
    projection = query._effective_projection()
    if projection and not query._configured_read_builder()._output_fields():
        return None

    groups = {}
    selections = []
    for result in results:
        selected = []
        for split in result.splits:
            if not isinstance(split, IndexedSplit) or len(split.files) != 1:
                return None
            data = split.data_split()
            file = data.files[0]
            if not should_read(file):
                return None
            key = (_partition_bytes(data.partition), data.bucket, file.file_name)
            positions = {position for r in split.row_ranges()
                         for position in range(r.from_, r.to + 1)}
            if key not in groups:
                groups[key] = (data, set())
            source, union = groups[key]
            if source.files != data.files or source.data_deletion_files != data.data_deletion_files:
                return None
            union.update(positions)
            selected.append((key, positions))
        selections.append(selected)

    # Preserve per-query read parallelism when every file is selected once.
    if len(groups) == sum(len(selected) for selected in selections):
        return None

    lookup = copy(query)
    # A shared file may contain more rows than any individual query's Top-K.
    lookup._limit = None
    reader = lookup._configured_read_builder().new_read()
    shared = {}
    for key, (data, positions) in groups.items():
        ordered = sorted(positions)
        deletion = data.data_deletion_files[0] if data.data_deletion_files else None
        if deletion is not None:
            dv = DeletionVector.read(table.file_io, deletion)
            ordered = [position for position in ordered if not dv.is_deleted(position)]
        rows = reader.to_arrow([IndexedSplit(data, _ranges(ordered))])
        # Compacted source files are key-sorted. DVs preserve that physical
        # order; a tombstone or merge that removes additional rows prevents a
        # reliable ordinal mapping and requires the individual lookup path.
        if rows.num_rows != len(ordered):
            return None
        shared[key] = (rows, {position: index for index, position in enumerate(ordered)})

    empty = _take(next(iter(shared.values()))[0], [])
    output = []
    for selected in selections:
        parts = []
        for key, positions in selected:
            rows, offsets = shared[key]
            indices = [offsets[position] for position in sorted(positions) if position in offsets]
            parts.append(_take(rows, indices))
        if not parts:
            rows = empty
        elif not empty.num_columns:
            rows = _take(empty, range(sum(part.num_rows for part in parts)))
        else:
            rows = pa.concat_tables(parts)
        if query._limit is not None:
            rows = (_take(rows, range(min(query._limit, rows.num_rows)))
                    if not rows.num_columns else rows.slice(0, query._limit))
        output.append(rows)
    return output
