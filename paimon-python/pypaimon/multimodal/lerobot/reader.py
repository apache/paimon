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

"""Indexed frame-row reader used by :class:`PaimonDatasetReader`."""

import os

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.multimodal.table import _time_travel_table
from pypaimon.read.query_auth_split import QueryAuthSplit


class _PaimonTableFrameReader:
    """Read logical frame rows from one indexed Paimon table."""

    def __init__(self, frames_table, *, columns):
        self._table, self.snapshot_id, splits = _indexed_read_table(
            frames_table, columns)
        snapshot = self._table.snapshot_manager().get_snapshot_by_id(
            self.snapshot_id)
        self.num_rows = snapshot.next_row_id
        self.file_io = self._table.file_io
        self._locator = _FrameLocator(self._table, snapshot, splits)

    def read_indices(self, indices, columns):
        splits, needs_filter = self._locator.locate(indices)
        builder = self._table.new_read_builder().with_projection(columns)
        if needs_filter:
            builder = builder.with_filter(
                _index_predicate(self._table, indices))
        return builder.new_read().to_arrow(splits)

    def close(self):
        self._locator.close()


class _FrameLocator:
    """Locate LeRobot frame rows in one fixed Paimon snapshot."""

    def __init__(self, table, snapshot, splits):
        self._table = table
        self._snapshot = snapshot
        self._scanner = None
        self._scanner_initialized = False
        self._process_id = os.getpid()
        self._set_splits(splits)

    def _set_splits(self, splits):
        from pypaimon.read.datasource.torch_dataset import (
            SplitRangeIndex,
            row_ranges_for_split,
        )

        self._splits = splits
        self._split_ranges = [
            row_ranges_for_split(split) for split in splits
        ]
        self._split_range_index = SplitRangeIndex(self._split_ranges)

    def locate(self, indices):
        """Return narrowed splits and whether rows still need filtering."""
        self._ensure_process()
        predicate = _index_predicate(self._table, indices)
        try:
            scanner = self._index_scanner(predicate)
        except Exception as error:
            raise RuntimeError(
                "Failed to open the Paimon global index for LeRobot frame "
                "lookups.") from error
        if scanner is None:
            raise RuntimeError(
                "PaimonLeRobotDataset requires a readable global index on "
                "the frame 'index' column.")
        try:
            evaluation = scanner.scan_with_coverage(predicate)
            if evaluation is None:
                raise RuntimeError(
                    "The Paimon global index could not evaluate the LeRobot "
                    "frame index predicate.")
            unindexed = scanner.unindexed_ranges(
                predicate,
                search_mode=self._table.options.scalar_index_search_mode(),
                contributing_field_ids=evaluation.contributing_field_ids,
            )
            ranges = evaluation.result.results().to_range_list() + unindexed
            from pypaimon.read.datasource.torch_dataset import (
                select_indexed_splits,
            )
            from pypaimon.utils.range import Range
            return select_indexed_splits(
                self._splits,
                self._split_ranges,
                self._split_range_index,
                Range.sort_and_merge_overlap(ranges, True),
            ), bool(unindexed)
        except RuntimeError:
            raise
        except Exception as error:
            raise RuntimeError(
                "Failed to query the Paimon global index for LeRobot "
                "frames.") from error

    def _ensure_process(self):
        process_id = os.getpid()
        if process_id == self._process_id:
            return
        self._scanner = None
        self._scanner_initialized = False
        self._set_splits(self._splits)
        self._process_id = process_id

    def _index_scanner(self, predicate):
        if not self._scanner_initialized:
            from pypaimon.globalindex import DataEvolutionGlobalIndexScanner
            self._scanner = DataEvolutionGlobalIndexScanner.create(
                self._table,
                predicate=predicate,
                snapshot=self._snapshot,
            )
            self._scanner_initialized = True
        return self._scanner

    def close(self):
        scanner = self._scanner
        self._scanner = None
        self._scanner_initialized = False
        if scanner is not None and self._process_id == os.getpid():
            scanner.close()

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_scanner"] = None
        state["_scanner_initialized"] = False
        state["_process_id"] = None
        state["_split_ranges"] = None
        state["_split_range_index"] = None
        return state

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


def _indexed_read_table(raw_table, projection):
    read_table = raw_table.copy({
        CoreOptions.BLOB_AS_DESCRIPTOR.key(): "true"
    })
    plan = read_table.new_read_builder().with_projection(
        projection).new_scan().plan()
    splits = plan.splits()
    if any(
            isinstance(split, QueryAuthSplit)
            and (
                getattr(split.auth_result, "filter", None)
                or getattr(split.auth_result, "column_masking", None)
            )
            for split in splits):
        raise ValueError(
            "PaimonLeRobotDataset does not support query authorization "
            "filters or column masking.")
    if plan.snapshot_id is None:
        raise ValueError("Paimon LeRobot frames table has no snapshot.")
    if read_table.options.scan_tag_name() is None:
        read_table = _time_travel_table(
            read_table, snapshot_id=plan.snapshot_id)
    return read_table, plan.snapshot_id, splits


def _index_predicate(table, indices):
    return table.new_read_builder().new_predicate_builder().is_in(
        "index", indices)
