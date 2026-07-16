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
from pypaimon.table.source.full_text_read import (
    DataEvolutionFullTextRead, _search_raw_full_text)
from pypaimon.table.source.primary_key_full_text_scan import PrimaryKeyFullTextScanPlan
from pypaimon.table.source.primary_key_scored_result import (
    PrimaryKeyScoredResult, PrimaryKeySearchPosition, _partition_bytes)
from pypaimon.table.source.primary_key_vector_read import _localize
from pypaimon.table.source.primary_key_vector_read import (
    _allowed, _deleted_positions, _live_positions)
from pypaimon.read.split import DataSplit
from pypaimon.globalindex.indexed_split import IndexedSplit


class PrimaryKeyFullTextRead(DataEvolutionFullTextRead):
    """Search PK full-text payloads and localize hits to data-file positions."""

    def __init__(self, table, limit, text_column, query, definition,
                 partition_filter=None):
        self._definition = definition
        super().__init__(table, limit, text_column, query, partition_filter)

    def read_plan(self, plan):
        if not isinstance(plan, PrimaryKeyFullTextScanPlan):
            raise ValueError("Primary-key full-text read requires a PrimaryKeyFullTextScanPlan.")
        futures = []
        contexts = []
        for split in plan.splits():
            for payload in split.payloads:
                source_meta = PrimaryKeyIndexSourceMeta.from_index_file(payload)
                row_count = sum(source.row_count for source in source_meta.source_files)
                futures.append(self._eval(0, row_count - 1, [payload], None))
                contexts.append((split, source_meta))
        wait(futures)

        candidates = []
        deleted_positions = _deleted_positions(self._table, plan)
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
        candidates.sort(key=lambda position: (-position.score,
                                               position.partition_bytes,
                                               position.bucket,
                                               position.data_file_name,
                                               position.row_position))
        return PrimaryKeyScoredResult(
            plan.snapshot_id, [split.data_split for split in plan.splits()],
            candidates[:self._limit])

    def _raw_candidates(self, plan):
        result = []
        column = self._text_columns[0]
        reader = self._table.new_read_builder().with_projection(
            [column.name]).new_read()
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
                texts = arrow.column(column.name).to_pylist()
                positions = _live_positions(self._table, data_file.row_count,
                                            deletion_file)
                positions = [position for position in positions
                             if _allowed(split, data_file.file_name, position)]
                if len(texts) != len(positions):
                    raise ValueError("Raw full-text row count does not match physical positions.")
                index_bytes = self._build_raw_index(positions, texts, 0)
                if index_bytes is None:
                    continue
                scored = _search_raw_full_text(
                    index_bytes, 0, self._query, data_file.row_count)
                getter = scored.score_getter()
                for row_position in scored.results():
                    result.append(PrimaryKeySearchPosition(
                        _partition_bytes(split.data_split.partition),
                        split.data_split.bucket, data_file.file_name,
                        row_position, getter(row_position)))
        return result
