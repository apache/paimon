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
from pypaimon.snapshot.time_travel_util import TimeTravelUtil
from pypaimon.table.source.vector_search_scan import VectorSearchScan, VectorSearchScanPlan


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

        builder = self._table.new_read_builder()
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

        index_type = self._index_type
        if index_type is None:
            index_type = CoreOptions(
                Options(dict(self._table.table_schema.options))
            ).primary_key_vector_index_type(self._vector_column.name)
        entries = IndexFileHandler(table=self._table).scan(
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
        active = {data_file.file_name: data_file for data_file in split.files}
        current = []
        covered = set()
        for payload in payloads_by_bucket.get(
                (_partition_key(split.partition), split.bucket), []):
            source_meta = PrimaryKeyIndexSourceMeta.from_index_file(payload)
            source_row_count = sum(source.row_count
                                   for source in source_meta.source_files)
            global_meta = payload.global_index_meta
            if (payload.row_count == source_row_count
                    and global_meta.row_range_start == 0
                    and global_meta.row_range_end == source_row_count - 1
                    and all(source.file_name in active
                            and active[source.file_name].row_count == source.row_count
                            for source in source_meta.source_files)):
                names = {source.file_name for source in source_meta.source_files}
                if not names.intersection(covered):
                    current.append(payload)
                    covered.update(names)
        key = (_partition_key(split.partition), split.bucket)
        result.append(PrimaryKeyVectorSearchSplit(
            split, tuple(current), tuple(name for name in active if name not in covered),
            dict(ranges_by_bucket.get(key, {}))))
    return result


def _partition_key(partition):
    return repr(tuple(getattr(partition, "values", ())))
