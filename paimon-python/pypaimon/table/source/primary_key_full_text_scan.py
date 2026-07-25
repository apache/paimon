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

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.index.index_file_handler import IndexFileHandler
from pypaimon.index.pk.primary_key_index_source_meta import (
    PrimaryKeyIndexSourceMeta)
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.read.split import DataSplit
from pypaimon.snapshot.time_travel_util import TimeTravelUtil
from pypaimon.table.source.full_text_scan import FullTextScan, FullTextScanPlan


@dataclass(frozen=True)
class PrimaryKeyFullTextSearchSplit:
    data_split: DataSplit
    payloads: tuple
    uncovered_data_files: tuple
    row_ranges_by_file: dict


class PrimaryKeyFullTextScan(FullTextScan):
    """Plan source-backed full-text payloads with current PK data files."""

    def __init__(self, table, definition, partition_filter=None):
        self._table = table
        self._definition = definition
        self._partition_filter = partition_filter

    def scan(self):
        snapshot = TimeTravelUtil.try_travel_to_snapshot(
            Options(self._table.table_schema.options), self._table.tag_manager(),
            self._table.snapshot_manager())
        if snapshot is None:
            snapshot = self._table.snapshot_manager().get_latest_snapshot()
        if snapshot is None:
            return PrimaryKeyFullTextScanPlan(0, [])

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
        source_splits = []
        for split in builder.new_scan().plan().splits():
            split = split.split if isinstance(split, QueryAuthSplit) else split
            if isinstance(split, IndexedSplit):
                split = split.data_split()
            if isinstance(split, DataSplit):
                source_splits.append(split)

        entries = IndexFileHandler(table=scan_table).scan(
            snapshot, lambda entry: _matches(
                entry, self._definition.field_id,
                self._definition.index_type, self._partition_filter))
        return PrimaryKeyFullTextScanPlan(
            snapshot.id, _full_text_bucket_splits(source_splits, entries))


def _matches(entry, field_id, index_type, partition_filter):
    meta = entry.index_file.global_index_meta
    return (entry.kind == 0 and entry.index_file.index_type == index_type
            and meta is not None and meta.source_meta is not None
            and meta.index_field_id == field_id
            and (partition_filter is None
                 or partition_filter.test(entry.partition)))


def _partition_key(partition):
    return tuple(partition.values)


def _full_text_bucket_splits(source_splits, entries):
    payloads_by_bucket = {}
    for entry in entries:
        key = (_partition_key(entry.partition), entry.bucket)
        payloads_by_bucket.setdefault(key, []).append(entry.index_file)

    buckets = {}
    for split in source_splits:
        if split.bucket < 0:
            continue
        key = (_partition_key(split.partition), split.bucket)
        bucket = buckets.setdefault(key, {
            "partition": split.partition, "files": [],
            "deletions": [], "names": set(), "has_deletions": False})
        deletions = split.data_deletion_files
        if deletions is not None and len(deletions) != len(split.files):
            raise ValueError(
                "Deletion files must align with data files in a full-text bucket split.")
        bucket["has_deletions"] |= deletions is not None
        for index, data_file in enumerate(split.files):
            if not _should_read_source(data_file):
                continue
            if data_file.file_name in bucket["names"]:
                raise ValueError(
                    "Data file %s appears more than once in full-text bucket planning."
                    % data_file.file_name)
            bucket["names"].add(data_file.file_name)
            bucket["files"].append(data_file)
            bucket["deletions"].append(
                None if deletions is None else deletions[index])

    result = []
    for key, bucket in buckets.items():
        if not bucket["files"]:
            continue
        payloads, covered = _current_payloads(
            bucket["files"], payloads_by_bucket.get(key, []))
        data_split = DataSplit(
            bucket["files"], bucket["partition"], key[1], False,
            bucket["deletions"] if bucket["has_deletions"] else None)
        result.append(PrimaryKeyFullTextSearchSplit(
            data_split, tuple(payloads),
            tuple(data_file.file_name for data_file in bucket["files"]
                  if data_file.file_name not in covered), {}))
    return result


def _current_payloads(active_files, active_payloads):
    sources_by_level = {}
    for data_file in active_files:
        sources_by_level.setdefault(data_file.level, []).append(
            (data_file.file_name, data_file.row_count))
    for sources in sources_by_level.values():
        sources.sort(key=lambda source: source[0])

    payloads_by_level = {}
    for payload in active_payloads:
        try:
            source_meta = PrimaryKeyIndexSourceMeta.from_index_file(payload)
            sources = [(source.file_name, source.row_count)
                       for source in source_meta.source_files]
            sources.sort(key=lambda source: source[0])
            source_row_count = sum(source.row_count
                                   for source in source_meta.source_files)
            meta = payload.global_index_meta
            valid_archive = (payload.row_count == source_row_count
                             and meta.row_range_start == 0
                             and meta.row_range_end == source_row_count - 1)
            if (valid_archive
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
        payload = level_payloads[0]
        current.append(payload)
        source_meta = PrimaryKeyIndexSourceMeta.from_index_file(payload)
        covered.update(source.file_name for source in source_meta.source_files)
    return current, covered


def _should_read_source(data_file):
    # FileSource.COMPACT = 1. Match Java PrimaryKeyIndexSourcePolicy.
    return data_file.file_source == 1 and data_file.level > 0


class PrimaryKeyFullTextScanPlan(FullTextScanPlan):
    def __init__(self, snapshot_id, splits):
        super().__init__(splits)
        self.snapshot_id = snapshot_id
