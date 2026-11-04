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

"""Scanner for shard-based global indexes on data-evolution tables."""

from concurrent.futures import ThreadPoolExecutor
import threading
from typing import Collection, List, Optional

from pypaimon.execution.python.globalindex.global_index_evaluator import (
    GlobalIndexEvaluation,
    GlobalIndexEvaluator,
)
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.global_index_reader import GlobalIndexReader
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.common.predicate import Predicate
from pypaimon.execution.python.globalindex.data_evolution_global_index_coverage import DataEvolutionGlobalIndexCoverage
from pypaimon.read.push_down_utils import _get_all_fields
from pypaimon.schema.data_types import DataField
from pypaimon.utils.range import Range


_SUPPORTED_SCALAR_INDEX_TYPES = frozenset(('btree', 'bitmap'))


class CompositeIndexReadError(RuntimeError):
    """A selected composite index is unreadable; falling back may hide corruption."""


class DataEvolutionGlobalIndexScanner:
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
        composite_files=None,
    ):
        index_files = _supported_scalar_index_files(index_files)
        self._options = options or CoreOptions(Options.from_none())
        self._executor = ThreadPoolExecutor(
            max_workers=thread_num or 32
        )
        self._fields = fields
        self._file_io = file_io
        self._index_path = index_path
        self._composite_groups = _group_composite_files(composite_files or [])
        self._composite_readers = {}
        self._composite_reader_lock = threading.Lock()
        self._composite_reader_locks = {}
        self._coverage = (
            DataEvolutionGlobalIndexCoverage(table, snapshot, partition_filter, index_files)
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
            from pypaimon.execution.python.globalindex.union_global_index_reader import UnionGlobalIndexReader

            groups = []
            group = index_metas.get(field.id)
            if group is not None:
                groups.append(group)

            extra_groups = extra_index_metas.get(field.id)
            if extra_groups:
                groups.extend(
                    extra_group
                    for extra_group in extra_groups
                    if extra_group not in groups
                )
            if not groups:
                return []
            # Like Java, union every reader for the field. Missing coverage
            # belongs to raw fallback, not to the matches returned by an index.
            readers = []
            for group in groups:
                readers.extend(
                    _create_readers(
                        file_io, index_path, group.metas, field, executor, options))
            return [UnionGlobalIndexReader(readers)]

        return GlobalIndexEvaluator(fields, readers_function)

    @staticmethod
    def create(table, index_files=None, partition_filter=None, predicate=None,
               snapshot=None, allow_composite=False) -> Optional['DataEvolutionGlobalIndexScanner']:
        """Create a DataEvolutionGlobalIndexScanner.

        Can be called in two ways:
        1. create(table, index_files) - with explicit index files
        2. create(table, partition_filter=..., predicate=..., snapshot=...) -
           scan index files from snapshot. ``snapshot`` may be passed in by the
           caller to avoid a duplicate ``get_latest_snapshot`` REST round-trip
           (the caller usually already fetched it for manifest scanning).
        """
        from pypaimon.index.index_file_handler import IndexFileHandler

        if index_files is not None:
            index_files = list(index_files)
            scalar_files = _supported_scalar_index_files(index_files)
            composite_files = [file for file in index_files
                               if allow_composite and _is_composite_btree(file)]
            if not scalar_files and not composite_files:
                return None
            core_options = _core_options(table)
            return DataEvolutionGlobalIndexScanner(
                fields=table.fields,
                file_io=table.file_io,
                index_path=table.path_factory().global_index_path_factory().index_path(),
                index_files=scalar_files,
                composite_files=composite_files,
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
            if not (is_supported_scalar_index(entry.index_file)
                    or allow_composite and _is_composite_btree(entry.index_file)):
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
        return DataEvolutionGlobalIndexScanner(
            fields=table.fields,
            file_io=table.file_io,
            index_path=table.path_factory().global_index_path_factory().index_path(),
            index_files=scanned_index_files,
            composite_files=[file for file in scanned_index_files
                             if allow_composite and _is_composite_btree(file)],
            thread_num=core_options.global_index_thread_num(),
            options=core_options,
            table=table,
            snapshot=snapshot,
            partition_filter=partition_filter,
        )

    def scan(self, predicate: Optional[Predicate]) -> Optional[GlobalIndexResult]:
        """Scan the global index with the given predicate."""
        evaluation = self.scan_with_coverage(predicate)
        return evaluation.result if evaluation is not None else None

    def scan_with_coverage(
        self, predicate: Optional[Predicate]
    ) -> Optional[GlobalIndexEvaluation]:
        if not self._composite_groups or predicate is None:
            return self._evaluator.evaluate_with_contributing_fields(predicate)
        return self._evaluate(predicate)

    def _evaluate(self, predicate):
        from pypaimon.execution.python.globalindex.btree.composite_btree import (
            _conjuncts,
            plan_composite,
        )

        fields_by_id = {field.id: field for field in self._fields}
        candidates = []
        for field_ids, files in self._composite_groups.items():
            if any(field_id not in fields_by_id for field_id in field_ids):
                continue
            fields = [fields_by_id[field_id] for field_id in field_ids]
            plan = plan_composite(fields, predicate)
            if plan is None:
                continue
            if plan.bound_columns == 1 and self._coverage is not None:
                composite_ranges = Range.sort_and_merge_overlap(list(files), True)
                scalar_ranges = self._coverage.indexed_ranges([field_ids[0]])
                if Range.and_(composite_ranges, scalar_ranges) == composite_ranges:
                    continue
            try:
                selected = self._select_composite_files(files, fields, plan)
            except Exception as error:
                raise CompositeIndexReadError("Cannot read composite index metadata") from error
            if selected is None:
                continue
            candidates.append((plan.bound_columns, plan.equal_columns,
                               -len(field_ids), field_ids, fields, files, selected, plan))
        if candidates:
            _, _, _, field_ids, fields, files, selected, plan = max(
                candidates, key=lambda candidate: candidate[:3])
            if predicate.method == 'and':
                key_fields = {field.name for field in fields}
                remaining = []
                for child in _conjuncts(predicate):
                    if any(child is used for used in plan.covered_predicates):
                        continue
                    referenced_fields = _get_all_fields(child)
                    # Key residuals stay in the final row filter.
                    if referenced_fields and referenced_fields <= key_fields:
                        continue
                    remaining.append(child)
                if remaining:
                    rest = (remaining[0] if len(remaining) == 1
                            else Predicate('and', None, None, remaining))
                    if not self._may_use_composite(rest):
                        rest_future = self._evaluator._visit_async(rest)
                        try:
                            composite = self._read_composite(
                                field_ids, fields, files, selected, plan)
                        except BaseException:
                            try:
                                rest_future.result()
                            except BaseException:
                                pass
                            raise
                        return self._combine_evaluations(
                            [composite, rest_future.result()], 'and')
                    composite = self._read_composite(
                        field_ids, fields, files, selected, plan)
                    return self._combine_evaluations(
                        [composite, self._evaluate(rest)], 'and')
            return self._read_composite(field_ids, fields, files, selected, plan)

        if predicate.method in ('and', 'or'):
            scalar_children = []
            composite_children = []
            for child in predicate.literals:
                if self._may_use_composite(child):
                    composite_children.append(child)
                else:
                    scalar_children.append(child)
            if not composite_children:
                return self._evaluator.evaluate_with_contributing_fields(predicate)
            scalar_future = None
            if scalar_children:
                scalar = (scalar_children[0] if len(scalar_children) == 1
                          else Predicate(predicate.method, None, None, scalar_children))
                scalar_future = self._evaluator._visit_async(scalar)
            evaluations = [self._evaluate(child) for child in composite_children]
            if scalar_future is not None:
                evaluations.append(scalar_future.result())
            return self._combine_evaluations(evaluations, predicate.method)
        return self._evaluator.evaluate_with_contributing_fields(predicate)

    def _may_use_composite(self, predicate):
        from pypaimon.execution.python.globalindex.btree.composite_btree import plan_composite

        fields_by_id = {field.id: field for field in self._fields}
        predicate_fields = _get_all_fields(predicate)
        for field_ids in self._composite_groups:
            if (all(field_id in fields_by_id for field_id in field_ids)
                    and fields_by_id[field_ids[0]].name in predicate_fields):
                fields = [fields_by_id[field_id] for field_id in field_ids]
                if plan_composite(fields, predicate) is not None:
                    return True
        return (predicate.method in ('and', 'or')
                and any(self._may_use_composite(child) for child in predicate.literals))

    def _combine_evaluations(self, evaluations, method):
        if method == 'or' and any(item is None for item in evaluations):
            return None
        present = [item for item in evaluations if item is not None]
        if not present:
            return None
        result = present[0].result
        ranges = self._evaluation_ranges(present[0])
        field_ids = set(present[0].contributing_field_ids)
        for item in present[1:]:
            result = (result.or_(item.result) if method == 'or'
                      else result.and_(item.result))
            ranges = Range.and_(ranges, self._evaluation_ranges(item))
            field_ids.update(item.contributing_field_ids)
        if len(present) != len(evaluations):
            result = GlobalIndexResult.create(result.results(), is_exact=False)
        return GlobalIndexEvaluation(result, frozenset(field_ids), tuple(ranges))

    def _evaluation_ranges(self, evaluation):
        if evaluation.indexed_ranges is not None:
            return list(evaluation.indexed_ranges)
        if self._coverage is None:
            return []
        return self._coverage.indexed_ranges(evaluation.contributing_field_ids)

    def _select_composite_files(self, files, fields, plan):
        from pypaimon.execution.python.globalindex.btree.composite_btree import (
            compare_key_bound,
        )
        from pypaimon.execution.python.globalindex.btree.composite_key import (
            CompositeKeySerializer,
        )
        from pypaimon.globalindex.sorted_index_file_meta import SortedIndexFileMeta

        if not plan.intervals:
            return []
        serializer = CompositeKeySerializer(fields)
        selected = []
        budget = self._options.btree_index_fallback_scan_max_size()
        for row_range, metas in files.items():
            shard = []
            for meta in metas:
                if meta.metadata is None:
                    return None
                index_meta = SortedIndexFileMeta.deserialize(meta.metadata)
                if index_meta.first_key is not None and plan.intervals:
                    first = serializer.deserialize(index_meta.first_key)
                    last = serializer.deserialize(index_meta.last_key)
                    if not any(compare_key_bound(last, lower, serializer._comparators) > 0
                               and compare_key_bound(first, upper,
                                                     serializer._comparators) < 0
                               for lower, upper in plan.intervals):
                        continue
                if plan.intervals:
                    shard.append((row_range, meta))
            if not plan.point_lookup and sum(meta.file_size for _, meta in shard) > budget:
                return None
            selected.extend(shard)
        return selected

    def _read_composite(self, field_ids, fields, files, selected, plan):
        from pypaimon.execution.python.globalindex.btree.composite_btree import (
            CompositeBTreeIndexReader,
        )
        from pypaimon.execution.python.globalindex.btree.composite_key import (
            CompositeKeySerializer,
        )

        serializer = CompositeKeySerializer(fields)
        futures = []
        for row_range, meta in selected:
            def read(range_=row_range, io_meta=meta):
                key = (field_ids, io_meta.external_path or io_meta.file_name)
                with self._composite_reader_lock:
                    reader = self._composite_readers.get(key)
                    reader_lock = (self._composite_reader_locks.setdefault(
                        key, threading.Lock()) if reader is None else None)
                if reader is None:
                    with reader_lock:
                        with self._composite_reader_lock:
                            reader = self._composite_readers.get(key)
                        if reader is None:
                            reader = CompositeBTreeIndexReader(
                                serializer, self._file_io, self._index_path, io_meta)
                            with self._composite_reader_lock:
                                self._composite_readers[key] = reader
                return reader.query(plan).offset(range_.from_)
            futures.append(self._executor.submit(read))
        result = GlobalIndexResult.create_empty()
        failure = None
        for future in futures:
            try:
                result = result.or_(future.result())
            except Exception as error:
                if failure is None:
                    failure = error
        if failure is not None:
            raise CompositeIndexReadError("Cannot read composite BTree file") from failure
        result = GlobalIndexResult.create(result.results(), is_exact=False)
        return GlobalIndexEvaluation(
            result, frozenset(field_ids[:plan.bound_columns]),
            tuple(Range.sort_and_merge_overlap(list(files), True)))

    def unindexed_rows(self, predicate: Optional[Predicate],
                       search_mode=None,
                       contributing_field_ids=None, indexed_ranges=None) -> GlobalIndexResult:
        """Return coarse row ids not covered by global indexes."""
        return GlobalIndexResult.from_ranges(self.unindexed_ranges(
            predicate,
            search_mode=search_mode,
            contributing_field_ids=contributing_field_ids,
            indexed_ranges=indexed_ranges,
        ))

    def unindexed_ranges(self, predicate: Optional[Predicate],
                         search_mode=None,
                         contributing_field_ids=None, indexed_ranges=None) -> List[Range]:
        """Return row ranges not covered by global indexes."""
        if self._coverage is None:
            return []
        if indexed_ranges is not None:
            return self._coverage.unindexed_ranges_from_indexed(
                indexed_ranges, search_mode=search_mode)
        if contributing_field_ids is not None:
            return self._coverage.unindexed_ranges(
                contributing_field_ids, search_mode=search_mode)
        return self._coverage.unindexed_ranges(
            self._fields, predicate, search_mode=search_mode)

    def close(self):
        """Close the scanner and release resources."""
        self._evaluator.close()
        for reader in self._composite_readers.values():
            reader.close()
        self._executor.shutdown(wait=False)

    def __enter__(self) -> 'DataEvolutionGlobalIndexScanner':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


class _IndexMetaFileGroup:
    def __init__(self, index_field_id, field_ids):
        self.index_field_id = index_field_id
        self.field_ids = tuple(field_ids)
        self.metas = {}

    def add_file(self, index_type, range_key, io_meta):
        self.metas.setdefault(index_type, {}).setdefault(range_key, []).append(io_meta)


def _resolve_snapshot(table, snapshot):
    if snapshot is not None:
        return snapshot
    if hasattr(table, "_read_snapshot"):
        return table._read_snapshot
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


def is_supported_scalar_index(index_file):
    meta = index_file.global_index_meta
    return (
        meta is not None
        and index_file.index_type in _SUPPORTED_SCALAR_INDEX_TYPES
        # Composite BTree keys cannot use scalar readers or their coverage.
        and not (index_file.index_type == 'btree' and meta.extra_field_ids)
    )


def _supported_scalar_index_files(index_files):
    return [index_file for index_file in index_files
            if is_supported_scalar_index(index_file)]


def _is_composite_btree(index_file):
    meta = index_file.global_index_meta
    return (index_file.index_type == 'btree' and meta is not None
            and bool(meta.extra_field_ids)
            and len(set((meta.index_field_id,) + tuple(meta.extra_field_ids)))
            == 1 + len(meta.extra_field_ids))


def _group_composite_files(index_files):
    groups = {}
    for index_file in index_files:
        if not _is_composite_btree(index_file):
            continue
        meta = index_file.global_index_meta
        field_ids = (meta.index_field_id,) + tuple(meta.extra_field_ids)
        row_range = Range(meta.row_range_start, meta.row_range_end)
        io_meta = GlobalIndexIOMeta(
            file_name=index_file.file_name,
            file_size=index_file.file_size,
            metadata=meta.index_meta,
            external_path=index_file.external_path,
        )
        groups.setdefault(field_ids, {}).setdefault(row_range, []).append(io_meta)
    return groups


def _core_options(table):
    options = getattr(table, "options", None)
    if options is None:
        return CoreOptions(Options.from_none())
    return options


def _create_readers(
        file_io, index_path, index_type_metas, field, executor=None,
        options=None):
    """Create readers for a specific field, dispatched by index_type.

    Unknown indexTypes raise — a silent skip would make
    ``DataEvolutionVectorRead._pre_filter`` return ``None`` and the vector search
    would then return unfiltered results that violate the user predicate.
    """
    if index_type_metas is None:
        return []

    from pypaimon.execution.python.globalindex.offset_global_index_reader import (
        OffsetGlobalIndexReader,
    )
    from pypaimon.execution.python.globalindex.union_global_index_reader import (
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
            readers.append(UnionGlobalIndexReader(offset_readers))
    return readers


def _create_inner_readers(
        index_type, file_io, index_path, field, io_metas, executor=None, options=None):
    """Build per-file (or per-shard) readers for a single indexType/range."""
    core_options = options or CoreOptions(Options.from_none())
    core_options = CoreOptions(core_options.global_index_options(index_type, field.name))
    if index_type == 'btree':
        from pypaimon.execution.python.globalindex.btree.lazy_filtered_btree_reader import LazyFilteredBTreeReader
        from pypaimon.execution.python.globalindex.key_serializer import create_serializer
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
        from pypaimon.execution.python.globalindex.bitmap.lazy_filtered_bitmap_reader import LazyFilteredBitmapReader
        from pypaimon.execution.python.globalindex.key_serializer import create_serializer
        key_serializer = create_serializer(field.type)
        return [LazyFilteredBitmapReader(
            key_serializer=key_serializer,
            file_io=file_io,
            index_path=index_path,
            io_metas=io_metas,
            executor=executor,
            fallback_scan_max_size=core_options.bitmap_index_fallback_scan_max_size(),
        )]

    raise ValueError(
        "Unsupported global-index type in scanner: '%s'" % index_type)
