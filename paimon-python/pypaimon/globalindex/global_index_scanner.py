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

"""Scanner for shard-based global indexes."""

from concurrent.futures import ThreadPoolExecutor
from typing import Collection, Optional

from pypaimon.globalindex.global_index_evaluator import GlobalIndexEvaluator
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.global_index_reader import GlobalIndexReader, _map_future
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.common.predicate import Predicate
from pypaimon.globalindex.global_index_coverage import GlobalIndexCoverage
from pypaimon.read.push_down_utils import _get_all_fields
from pypaimon.schema.data_types import DataField
from pypaimon.utils.range import Range


class GlobalIndexScanner:
    """Scanner for shard-based global indexes."""

    def __init__(
        self,
        fields: list,
        file_io,
        index_path: str,
        index_files: Collection['IndexFileMeta'],
        thread_num: Optional[int] = None,
        options: Optional[CoreOptions] = None,
        table=None,
        snapshot=None,
        partition_filter=None,
    ):
        self._options = options or CoreOptions(Options.from_none())
        self._executor = ThreadPoolExecutor(
            max_workers=thread_num or 32
        )
        self._fields = fields
        self._coverage = (
            GlobalIndexCoverage(table, snapshot, partition_filter, index_files)
            if table is not None
            else None
        )
        self._evaluator = self._create_evaluator(
            fields, file_io, index_path, index_files
        )

    def _create_evaluator(self, fields, file_io, index_path, index_files):
        index_metas = {}
        extra_index_metas = {}
        for index_file in index_files:
            global_index_meta = index_file.global_index_meta
            if global_index_meta is None:
                continue

            index_type = index_file.index_type
            index_field_id = global_index_meta.index_field_id
            field_ids = [index_field_id]
            if global_index_meta.extra_field_ids is not None:
                field_ids.extend(global_index_meta.extra_field_ids)

            io_meta = GlobalIndexIOMeta(
                file_name=index_file.file_name,
                file_size=index_file.file_size,
                metadata=global_index_meta.index_meta,
                external_path=index_file.external_path,
            )
            range_key = Range(
                global_index_meta.row_range_start,
                global_index_meta.row_range_end)
            group = index_metas.get(index_field_id)
            if group is None:
                group = _IndexMetaFileGroup(index_field_id, field_ids)
                index_metas[index_field_id] = group
                for extra_field_id in field_ids[1:]:
                    extra_index_metas.setdefault(extra_field_id, []).append(group)
            elif group.field_ids != tuple(field_ids):
                raise ValueError(
                    "Primary field %s owns multiple indexes with different "
                    "columns %s and %s; a primary column can own at most one "
                    "index." % (index_field_id, list(group.field_ids), field_ids)
                )
            group.add_file(index_type, range_key, io_meta)

        executor = self._executor
        options = self._options

        def readers_function(field: DataField) -> Collection[GlobalIndexReader]:
            group = index_metas.get(field.id)
            if group is not None:
                return _create_readers(
                    file_io, index_path, group.metas, field, executor, options)

            extra_groups = extra_index_metas.get(field.id)
            if not extra_groups:
                return []
            union_coverage = Range.sort_and_merge_overlap(
                [
                    range_key
                    for group in extra_groups
                    for range_key in group.coverage_ranges
                ],
                True,
            )
            readers = []
            for group in extra_groups:
                pad_ranges = _exclude_ranges(union_coverage, group.coverage_ranges)
                readers.extend(
                    _create_readers(
                        file_io, index_path, group.metas, field, executor,
                        options, pad_ranges=pad_ranges))
            return readers

        return GlobalIndexEvaluator(fields, readers_function)

    @staticmethod
    def create(table, index_files=None, partition_filter=None, predicate=None,
               snapshot=None) -> Optional['GlobalIndexScanner']:
        """Create a GlobalIndexScanner.

        Can be called in two ways:
        1. create(table, index_files) - with explicit index files
        2. create(table, partition_filter=..., predicate=..., snapshot=...) -
           scan index files from snapshot. ``snapshot`` may be passed in by the
           caller to avoid a duplicate ``get_latest_snapshot`` REST round-trip
           (the caller usually already fetched it for manifest scanning).
        """
        from pypaimon.index.index_file_handler import IndexFileHandler

        if index_files is not None:
            if len(index_files) == 0:
                return None
            core_options = _core_options(table)
            return GlobalIndexScanner(
                fields=table.fields,
                file_io=table.file_io,
                index_path=table.path_factory().global_index_path_factory().index_path(),
                index_files=index_files,
                thread_num=core_options.global_index_thread_num(),
                options=core_options,
                table=table,
                snapshot=_resolve_snapshot(table, snapshot),
                partition_filter=partition_filter,
            )

        # Scan index files from snapshot using partition_filter and predicate
        filter_field_names = _get_all_fields(predicate)
        filter_field_ids = set()
        if predicate is not None:
            for field_item in table.fields:
                if field_item.name in filter_field_names:
                    filter_field_ids.add(field_item.id)

        def index_file_filter(entry):
            if partition_filter is not None:
                if not partition_filter.test(entry.partition):
                    return False
            global_index_meta = entry.index_file.global_index_meta
            if global_index_meta is None:
                return False
            if global_index_meta.index_field_id in filter_field_ids:
                return True
            if global_index_meta.extra_field_ids is not None:
                return any(
                    field_id in filter_field_ids
                    for field_id in global_index_meta.extra_field_ids
                )
            return False

        if snapshot is None:
            snapshot = _resolve_snapshot(table, None)
        index_file_handler = IndexFileHandler(table=table)
        entries = index_file_handler.scan(snapshot, index_file_filter)
        scanned_index_files = [entry.index_file for entry in entries]

        if len(scanned_index_files) == 0:
            return None
        core_options = _core_options(table)
        return GlobalIndexScanner(
            fields=table.fields,
            file_io=table.file_io,
            index_path=table.path_factory().global_index_path_factory().index_path(),
            index_files=scanned_index_files,
            thread_num=core_options.global_index_thread_num(),
            options=core_options,
            table=table,
            snapshot=snapshot,
            partition_filter=partition_filter,
        )

    def scan(self, predicate: Optional[Predicate]) -> Optional[GlobalIndexResult]:
        """Scan the global index with the given predicate."""
        return self._evaluator.evaluate(predicate)

    def unindexed_rows(self, predicate: Optional[Predicate]) -> GlobalIndexResult:
        """Return coarse row ids not covered by global indexes."""
        if self._coverage is None:
            return GlobalIndexResult.create_empty()
        return GlobalIndexResult.from_ranges(
            self._coverage.unindexed_ranges(self._fields, predicate))

    def close(self):
        """Close the scanner and release resources."""
        self._evaluator.close()
        self._executor.shutdown(wait=False)

    def __enter__(self) -> 'GlobalIndexScanner':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


class _IndexMetaFileGroup:
    def __init__(self, index_field_id, field_ids):
        self.index_field_id = index_field_id
        self.field_ids = tuple(field_ids)
        self.metas = {}
        self.coverage_ranges = []

    def add_file(self, index_type, range_key, io_meta):
        self.coverage_ranges.append(range_key)
        self.metas.setdefault(index_type, {}).setdefault(range_key, []).append(io_meta)


class _PaddingGlobalIndexReader(GlobalIndexReader):
    def __init__(self, wrapped, padding):
        self._wrapped = wrapped
        self._padding = padding

    def _pad(self, future):
        return _map_future(
            future,
            lambda result: None if result is None else result.or_(self._padding))

    def visit_equal(self, field_ref, literal):
        return self._pad(self._wrapped.visit_equal(field_ref, literal))

    def visit_not_equal(self, field_ref, literal):
        return self._pad(self._wrapped.visit_not_equal(field_ref, literal))

    def visit_less_than(self, field_ref, literal):
        return self._pad(self._wrapped.visit_less_than(field_ref, literal))

    def visit_less_or_equal(self, field_ref, literal):
        return self._pad(self._wrapped.visit_less_or_equal(field_ref, literal))

    def visit_greater_than(self, field_ref, literal):
        return self._pad(self._wrapped.visit_greater_than(field_ref, literal))

    def visit_greater_or_equal(self, field_ref, literal):
        return self._pad(self._wrapped.visit_greater_or_equal(field_ref, literal))

    def visit_is_null(self, field_ref):
        return self._pad(self._wrapped.visit_is_null(field_ref))

    def visit_is_not_null(self, field_ref):
        return self._pad(self._wrapped.visit_is_not_null(field_ref))

    def visit_in(self, field_ref, literals):
        return self._pad(self._wrapped.visit_in(field_ref, literals))

    def visit_not_in(self, field_ref, literals):
        return self._pad(self._wrapped.visit_not_in(field_ref, literals))

    def visit_starts_with(self, field_ref, literal):
        return self._pad(self._wrapped.visit_starts_with(field_ref, literal))

    def visit_ends_with(self, field_ref, literal):
        return self._pad(self._wrapped.visit_ends_with(field_ref, literal))

    def visit_contains(self, field_ref, literal):
        return self._pad(self._wrapped.visit_contains(field_ref, literal))

    def visit_like(self, field_ref, literal):
        return self._pad(self._wrapped.visit_like(field_ref, literal))

    def visit_between(self, field_ref, from_v, to_v):
        return self._pad(self._wrapped.visit_between(field_ref, from_v, to_v))

    def visit_not_between(self, field_ref, from_v, to_v):
        return self._pad(self._wrapped.visit_not_between(field_ref, from_v, to_v))

    def close(self):
        self._wrapped.close()


def _resolve_snapshot(table, snapshot):
    if snapshot is not None:
        return snapshot
    snapshot_manager = table.snapshot_manager()
    if snapshot_manager is None:
        return None
    try:
        from pypaimon.snapshot.time_travel_util import TimeTravelUtil
        table_options = getattr(table.table_schema, "options", {})
        scan_keys = getattr(TimeTravelUtil, "SCAN_KEYS", None)
        if scan_keys is None:
            from pypaimon.snapshot.time_travel_util import SCAN_KEYS as scan_keys
        has_time_travel = any(key in table_options for key in scan_keys)
        resolved = TimeTravelUtil.try_travel_to_snapshot(
            Options(table.table_schema.options),
            table.tag_manager(),
            snapshot_manager,
        )
        if resolved is not None and (
                has_time_travel or getattr(resolved, "next_row_id", None) is not None):
            return resolved
    except Exception:
        pass
    return snapshot_manager.get_latest_snapshot()


def _core_options(table):
    options = getattr(table, "options", None)
    if options is None:
        return CoreOptions(Options.from_none())
    return options


def _exclude_ranges(base_ranges, excluded_ranges):
    excluded_ranges = Range.sort_and_merge_overlap(excluded_ranges, True)
    result = []
    for base_range in Range.sort_and_merge_overlap(base_ranges, True):
        result.extend(base_range.exclude(excluded_ranges))
    return Range.sort_and_merge_overlap(result, True)


def _create_readers(
        file_io, index_path, index_type_metas, field, executor=None,
        options=None, pad_ranges=None):
    """Create readers for a specific field, dispatched by index_type.

    Unknown indexTypes raise — a silent skip would make
    ``VectorSearchReadImpl._pre_filter`` return ``None`` and the vector search
    would then return unfiltered results that violate the user predicate.
    """
    if index_type_metas is None:
        return []

    from pypaimon.globalindex.offset_global_index_reader import (
        OffsetGlobalIndexReader,
    )
    from pypaimon.globalindex.union_global_index_reader import (
        UnionGlobalIndexReader,
    )

    readers = []
    for index_type, range_metas in index_type_metas.items():
        offset_readers = []
        for range_key, io_metas in range_metas.items():
            inner_readers = _create_inner_readers(
                index_type, file_io, index_path, field, io_metas, executor, options)
            for inner in inner_readers:
                offset_readers.append(
                    OffsetGlobalIndexReader(
                        inner, range_key.from_, range_key.to))
        if offset_readers:
            reader = UnionGlobalIndexReader(offset_readers)
            if pad_ranges:
                padding = GlobalIndexResult.from_ranges(pad_ranges)
                reader = _PaddingGlobalIndexReader(reader, padding)
            readers.append(reader)
    return readers


def _create_inner_readers(
        index_type, file_io, index_path, field, io_metas, executor=None, options=None):
    """Build per-file (or per-shard) readers for a single indexType/range."""
    core_options = options or CoreOptions(Options.from_none())
    if index_type == 'btree':
        from pypaimon.globalindex.btree.lazy_filtered_btree_reader import LazyFilteredBTreeReader
        from pypaimon.globalindex.key_serializer import create_serializer
        key_serializer = create_serializer(field.type)
        return [LazyFilteredBTreeReader(
            key_serializer=key_serializer,
            file_io=file_io,
            index_path=index_path,
            io_metas=io_metas,
            executor=executor,
            fallback_scan_max_size=core_options.btree_index_fallback_scan_max_size(),
        )]

    if index_type == 'bitmap':
        from pypaimon.globalindex.bitmap.lazy_filtered_bitmap_reader import LazyFilteredBitmapReader
        from pypaimon.globalindex.key_serializer import create_serializer
        key_serializer = create_serializer(field.type)
        return [LazyFilteredBitmapReader(
            key_serializer=key_serializer,
            file_io=file_io,
            index_path=index_path,
            io_metas=io_metas,
            executor=executor,
            fallback_scan_max_size=core_options.bitmap_index_fallback_scan_max_size(),
        )]

    from pypaimon.globalindex.tantivy import (
        TANTIVY_FULLTEXT_IDENTIFIER,
        TantivyFullTextGlobalIndexReader,
    )
    if index_type == TANTIVY_FULLTEXT_IDENTIFIER:
        return [
            TantivyFullTextGlobalIndexReader(file_io, index_path, [io_meta])
            for io_meta in io_metas
        ]

    raise ValueError(
        "Unsupported global-index type in scanner: '%s'" % index_type)
