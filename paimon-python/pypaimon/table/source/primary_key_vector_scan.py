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

from dataclasses import dataclass

from pypaimon.common.options.options import Options
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.index.index_file_handler import IndexFileHandler
from pypaimon.index.pk.primary_key_index_source_meta import PrimaryKeyIndexSourceMeta
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.read.split import DataSplit
from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.deletionvectors.deletion_vector import DeletionVector
from pypaimon.snapshot.time_travel_util import TimeTravelUtil
from pypaimon.table.source.vector_search_scan import VectorSearchScan, VectorSearchScanPlan
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.utils.range import Range


@dataclass(frozen=True)
class PrimaryKeyVectorSearchSplit:
    data_split: DataSplit
    payloads: tuple
    uncovered_data_files: tuple
    row_ranges_by_file: dict


class PrimaryKeyVectorScan(VectorSearchScan):
    """Plan source-backed vector payloads with their current PK data files."""

    def __init__(self, table, vector_column, filter_=None,
                 partition_filter=None, options=None, index_type=None):
        self._table = table
        self._vector_column = vector_column
        self._filter = filter_
        self._partition_filter = partition_filter
        self._options = dict(options or {})
        self._index_type = index_type

    def scan(self):
        snapshot = TimeTravelUtil.try_travel_to_snapshot(
            Options(self._table.table_schema.options), self._table.tag_manager(),
            self._table.snapshot_manager())
        if snapshot is None:
            snapshot = self._table.snapshot_manager().get_latest_snapshot()
        if snapshot is None:
            return PrimaryKeyVectorScanPlan(0, [])

        pin_options = {
            CoreOptions.SCAN_MODE.key(): "from-snapshot",
            CoreOptions.SCAN_SNAPSHOT_ID.key(): str(snapshot.id)}
        for option in (CoreOptions.SCAN_TAG_NAME,
                       CoreOptions.SCAN_WATERMARK,
                       CoreOptions.SCAN_TIMESTAMP,
                       CoreOptions.SCAN_TIMESTAMP_MILLIS):
            if option.key() in self._table.table_schema.options:
                pin_options[option.key()] = None
        scan_table = self._table.copy(pin_options)
        builder = scan_table.new_read_builder()
        if self._partition_filter is not None:
            builder = builder.with_partition_filter(self._partition_filter)
        if self._filter is not None:
            builder = builder.with_filter(self._filter)
        source_splits = []
        for split in builder.new_scan().plan().splits():
            split = split.split if isinstance(split, QueryAuthSplit) else split
            if isinstance(split, IndexedSplit):
                data_split = split.data_split()
                if len(data_split.files) != 1:
                    raise ValueError("Primary-key pre-filter split must contain one data file.")
                source_splits.append((data_split, {
                    data_split.files[0].file_name: tuple(split.row_ranges())}))
            elif isinstance(split, DataSplit):
                source_splits.append((split, {}))

        if self._filter is not None:
            source_splits = [
                (split, _residual_row_ranges(
                    scan_table, self._filter, split, ranges))
                for split, ranges in source_splits
            ]

        index_type = self._index_type
        if index_type is None:
            index_type = CoreOptions(
                Options(dict(scan_table.table_schema.options))
            ).primary_key_vector_index_type(self._vector_column.name)
        entries = IndexFileHandler(table=scan_table).scan(
            snapshot,
            lambda entry: _matches(entry, self._vector_column.id, index_type,
                                   self._partition_filter))
        return PrimaryKeyVectorScanPlan(
            snapshot.id, _bucket_splits(source_splits, entries))


class PrimaryKeyVectorScanPlan(VectorSearchScanPlan):
    def __init__(self, snapshot_id, splits):
        super().__init__(splits)
        self.snapshot_id = snapshot_id


def _matches(entry, field_id, index_type, partition_filter):
    meta = entry.index_file.global_index_meta
    return (entry.kind == 0 and entry.index_file.index_type == index_type
            and meta is not None and meta.source_meta is not None
            and meta.index_field_id == field_id
            and (partition_filter is None or partition_filter.test(entry.partition)))


def _bucket_splits(source_splits, entries):
    payloads_by_bucket = {}
    for entry in entries:
        key = (_partition_key(entry.partition), entry.bucket)
        payloads_by_bucket.setdefault(key, []).append(entry.index_file)
    combined = {}
    ranges_by_bucket = {}
    for split, ranges in source_splits:
        key = (_partition_key(split.partition), split.bucket)
        ranges_by_bucket.setdefault(key, {}).update(ranges)
        previous = combined.get(key)
        if previous is None:
            combined[key] = split
            continue
        files = list(previous.files) + list(split.files)
        deletions = None
        if (previous.data_deletion_files is not None
                or split.data_deletion_files is not None):
            deletions = list(previous.data_deletion_files or [None] * len(previous.files))
            deletions.extend(split.data_deletion_files or [None] * len(split.files))
        combined[key] = DataSplit(files, previous.partition, previous.bucket,
                                  False, deletions)

    result = []
    for split in combined.values():
        active = {data_file.file_name: data_file for data_file in split.files
                  if _should_read_source(data_file)}
        sources_by_level = {}
        for data_file in active.values():
            sources_by_level.setdefault(data_file.level, []).append(
                (data_file.file_name, data_file.row_count))
        for sources in sources_by_level.values():
            sources.sort(key=lambda source: source[0])
        payloads_by_level = {}
        for payload in payloads_by_bucket.get(
                (_partition_key(split.partition), split.bucket), []):
            try:
                source_meta = PrimaryKeyIndexSourceMeta.from_index_file(payload)
                sources = [(source.file_name, source.row_count)
                           for source in source_meta.source_files]
                source_row_count = sum(source.row_count
                                       for source in source_meta.source_files)
                global_meta = payload.global_index_meta
                if (payload.row_count == source_row_count
                        and global_meta.row_range_start == 0
                        and global_meta.row_range_end == source_row_count - 1
                        and sources_by_level.get(source_meta.data_level) == sources):
                    payloads_by_level.setdefault(
                        source_meta.data_level, []).append(payload)
            except (ValueError, OverflowError, TypeError, AttributeError):
                pass
        current = []
        covered = set()
        for level in sorted(payloads_by_level):
            level_payloads = payloads_by_level[level]
            if len(level_payloads) != 1:
                continue
            current.append(level_payloads[0])
            covered.update(name for name, _ in sources_by_level[level])
        key = (_partition_key(split.partition), split.bucket)
        result.append(PrimaryKeyVectorSearchSplit(
            split, tuple(current), tuple(name for name in active if name not in covered),
            dict(ranges_by_bucket.get(key, {}))))
    return result


def _should_read_source(data_file):
    # FileSource.COMPACT = 1. Match Java PrimaryKeyIndexSourcePolicy.
    return data_file.file_source == 1 and data_file.level > 0


def _residual_row_ranges(table, predicate, split, candidate_ranges):
    """Evaluate the residual predicate on physical rows before ANN search."""
    from pypaimon.read.push_down_utils import (
        predicate_field_names, rewrite_predicate_indices)

    field_names = predicate_field_names(predicate)
    fields = [field for field in table.fields if field.name in field_names]
    residual = rewrite_predicate_indices(predicate, fields)
    reader = table.new_read_builder().with_projection(
        [field.name for field in fields]).new_read()
    result = {}
    deletions = split.data_deletion_files or []
    for index, data_file in enumerate(split.files):
        ranges = candidate_ranges.get(data_file.file_name)
        if ranges is not None:
            ranges = tuple(Range.sort_and_merge_overlap(list(ranges), True))
            if not ranges:
                result[data_file.file_name] = tuple()
                continue
        deletion_file = deletions[index] if index < len(deletions) else None
        deleted = set()
        if deletion_file is not None:
            deleted = set(DeletionVector.read(
                table.file_io, deletion_file).bit_map())

        def physical_positions():
            range_index = 0
            for position in range(data_file.row_count):
                while (ranges is not None and range_index < len(ranges)
                       and ranges[range_index].to < position):
                    range_index += 1
                if position in deleted:
                    continue
                if (ranges is None or (range_index < len(ranges)
                                       and ranges[range_index].contains(position))):
                    yield position

        single = DataSplit(
            [data_file], split.partition, split.bucket, True,
            [deletion_file] if deletion_file is not None else None)
        read_split = IndexedSplit(single, list(ranges), None) \
            if ranges is not None else single
        positions = physical_positions()
        matched = []
        for batch in reader.to_arrow([read_split]).to_batches():
            for row in batch.to_pylist():
                try:
                    position = next(positions)
                except StopIteration:
                    raise ValueError(
                        "Residual filter row count does not match physical positions.")
                if residual.test(GenericRow(
                        [row[field.name] for field in fields], fields)):
                    if matched and matched[-1].to + 1 == position:
                        matched[-1] = Range(matched[-1].from_, position)
                    else:
                        matched.append(Range(position, position))
        try:
            next(positions)
            raise ValueError(
                "Residual filter row count does not match physical positions.")
        except StopIteration:
            pass
        result[data_file.file_name] = tuple(matched)
    return result


def _partition_key(partition):
    return repr(tuple(getattr(partition, "values", ())))
