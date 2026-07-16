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

from concurrent.futures import wait

from pypaimon.index.pk.primary_key_index_source_meta import PrimaryKeyIndexSourceMeta
from pypaimon.table.source.primary_key_scored_result import (
    PrimaryKeyScoredResult, PrimaryKeySearchPosition, _partition_bytes)
from pypaimon.table.source.primary_key_vector_scan import PrimaryKeyVectorScanPlan
from pypaimon.table.source.vector_search_read import DataEvolutionVectorRead
from pypaimon.table.source.vector_search_read import (
    _check_vector_dimension, _compute_score, _raw_search_metric, _to_vector_list)
from pypaimon.read.split import DataSplit
from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.deletionvectors.deletion_vector import DeletionVector
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


class PrimaryKeyVectorRead(DataEvolutionVectorRead):
    """Search PK vector payloads and localize payload row ids to data files."""

    def read_plan(self, plan):
        if not isinstance(plan, PrimaryKeyVectorScanPlan):
            raise ValueError("Primary-key vector read requires a PrimaryKeyVectorScanPlan.")
        futures = []
        contexts = []
        deleted_positions = _deleted_positions(self._table, plan)
        for split in plan.splits():
            for payload in split.payloads:
                source_meta = PrimaryKeyIndexSourceMeta.from_index_file(payload)
                row_count = sum(source.row_count for source in source_meta.source_files)
                include_row_ids = _include_row_ids(
                    split, source_meta.source_files, deleted_positions)
                future = self._eval(0, row_count - 1, [payload],
                                    self._query_vector, self._limit,
                                    include_row_ids)
                futures.append(future)
                contexts.append((split, source_meta))
        wait(futures)

        candidates = []
        for future, context in zip(futures, contexts):
            result = future.result()
            if result is None:
                continue
            score_getter = result.score_getter()
            for local_row_id in result.results():
                source, row_position = _localize(
                    context[1].source_files, local_row_id)
                if row_position in deleted_positions.get(
                        (id(context[0]), source.file_name), set()):
                    continue
                if not _allowed(context[0], source.file_name, row_position):
                    continue
                candidates.append(PrimaryKeySearchPosition(
                    _partition_bytes(context[0].data_split.partition),
                    context[0].data_split.bucket, source.file_name,
                    row_position, score_getter(local_row_id)))
        candidates.extend(self._raw_candidates(plan))
        candidates.sort(
            key=lambda position: (
                -position.score,
                position.partition_bytes,
                position.bucket,
                position.data_file_name,
                position.row_position))
        source_splits = [split.data_split for split in plan.splits()]
        return PrimaryKeyScoredResult(
            plan.snapshot_id, source_splits, candidates[:self._limit])

    def _raw_candidates(self, plan):
        metric = _raw_search_metric(
            self._table, self._vector_column, self._options,
            self._table.options.primary_key_vector_index_type(
                self._vector_column.name))
        result = []
        read_builder = self._table.new_read_builder().with_projection(
            [self._vector_column.name])
        reader = read_builder.new_read()
        for split in plan.splits():
            uncovered = set(split.uncovered_data_files)
            for file_index, data_file in enumerate(split.data_split.files):
                if data_file.file_name not in uncovered:
                    continue
                deletion_file = None
                if split.data_split.data_deletion_files is not None:
                    deletion_file = split.data_split.data_deletion_files[file_index]
                single = DataSplit([data_file], split.data_split.partition,
                                   split.data_split.bucket, False,
                                   [deletion_file] if deletion_file is not None else None)
                ranges = split.row_ranges_by_file.get(data_file.file_name)
                read_split = IndexedSplit(single, list(ranges), None) if ranges else single
                arrow = reader.to_arrow([read_split])
                vectors = arrow.column(self._vector_column.name).to_pylist()
                positions = _live_positions(self._table, data_file.row_count,
                                            deletion_file)
                positions = [position for position in positions
                             if _allowed(split, data_file.file_name, position)]
                if len(vectors) != len(positions):
                    raise ValueError("Raw vector row count does not match physical positions.")
                for row_position, stored in zip(positions, vectors):
                    if stored is None:
                        continue
                    stored = _to_vector_list(stored)
                    _check_vector_dimension(self._query_vector, stored)
                    result.append(PrimaryKeySearchPosition(
                        _partition_bytes(split.data_split.partition),
                        split.data_split.bucket, data_file.file_name,
                        row_position,
                        _compute_score(self._query_vector, stored, metric)))
        return result


def _localize(source_files, local_row_id):
    offset = 0
    for source in source_files:
        end = offset + source.row_count
        if offset <= local_row_id < end:
            return source, local_row_id - offset
        offset = end
    raise ValueError("Vector index returned out-of-range local row id %s." % local_row_id)


def _live_positions(table, row_count, deletion_file):
    deleted = set()
    if deletion_file is not None:
        deleted = set(DeletionVector.read(table.file_io, deletion_file).bit_map())
    return [position for position in range(row_count) if position not in deleted]


def _deleted_positions(table, plan):
    result = {}
    for split in plan.splits():
        deletions = split.data_split.data_deletion_files or []
        for index, data_file in enumerate(split.data_split.files):
            deletion = deletions[index] if index < len(deletions) else None
            if deletion is not None:
                result[(id(split), data_file.file_name)] = set(
                    DeletionVector.read(table.file_io, deletion).bit_map())
    return result


def _allowed(split, file_name, position):
    ranges = split.row_ranges_by_file.get(file_name)
    return ranges is None or any(row_range.contains(position) for row_range in ranges)


def _include_row_ids(split, source_files, deleted_positions):
    """Map live, pre-filtered physical positions to payload-local row ids."""
    include = RoaringBitmap64()
    offset = 0
    filtered = False
    for source in source_files:
        deleted = deleted_positions.get(
            (id(split), source.file_name), set())
        ranges = split.row_ranges_by_file.get(source.file_name)
        if deleted or ranges is not None:
            filtered = True
        for position in range(source.row_count):
            if position not in deleted and _allowed(
                    split, source.file_name, position):
                include.add(offset + position)
        offset += source.row_count
    return include if filtered else None
