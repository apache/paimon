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

"""Base reader for sorted global index files with manifest pruning."""

import threading
from concurrent.futures import Executor, Future
from typing import Callable, Dict, List, Optional

from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.global_index_reader import FieldRef, GlobalIndexReader, _completed_future
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.globalindex.key_serializer import KeySerializer
from pypaimon.globalindex.sorted_file_meta_selector import SortedFileMetaSelector
from pypaimon.globalindex.sorted_index_file_meta import SortedIndexFileMeta
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


_LONG_MAX_VALUE = (1 << 63) - 1


class SortedFileGlobalIndexReader(GlobalIndexReader):
    """Shared lazy reader/candidate pruning logic for sorted global indexes."""

    def __init__(
        self,
        key_serializer: KeySerializer,
        file_io,
        index_path: str,
        io_metas: List[GlobalIndexIOMeta],
        executor: Executor,
        fallback_scan_max_size: int,
    ):
        self._key_serializer = key_serializer
        self._file_io = file_io
        self._index_path = index_path
        self._executor = executor
        self._fallback_scan_max_size = fallback_scan_max_size

        files = []
        for io_meta in io_metas:
            idx_meta = SortedIndexFileMeta.deserialize(io_meta.metadata)
            files.append((io_meta, idx_meta))

        self._selector = SortedFileMetaSelector(files, key_serializer)
        self._reader_cache: Dict[str, object] = {}
        self._cache_lock = threading.Lock()

    def open_reader(self, meta: GlobalIndexIOMeta):
        raise NotImplementedError

    def _get_or_create_reader(self, meta: GlobalIndexIOMeta):
        key = meta.external_path or meta.file_name
        with self._cache_lock:
            reader = self._reader_cache.get(key)
            if reader is not None:
                return reader
            reader = self.open_reader(meta)
            self._reader_cache[key] = reader
            return reader

    def _visit_parallel(
        self,
        selector_fn: Callable[[], Optional[List[GlobalIndexIOMeta]]],
        visitor_fn: Callable[[object], Optional[GlobalIndexResult]],
    ) -> 'Future[Optional[GlobalIndexResult]]':
        selected = selector_fn()
        if selected is None:
            return _completed_future(None)
        if not selected:
            return _completed_future(GlobalIndexResult.create_empty())

        task_futures: List[Future] = []
        for meta in selected:
            task_futures.append(self._executor.submit(
                lambda m=meta: visitor_fn(self._get_or_create_reader(m))))

        all_done: Future = Future()
        remaining = [len(task_futures)]
        lock = threading.Lock()

        def on_done(_):
            with lock:
                remaining[0] -= 1
                if remaining[0] == 0:
                    try:
                        result: Optional[GlobalIndexResult] = None
                        for future in task_futures:
                            current = future.result()
                            if current is None:
                                continue
                            if result is None:
                                result = current
                            else:
                                result = result.or_(current)
                        all_done.set_result(result)
                    except Exception as e:
                        all_done.set_exception(e)

        for future in task_futures:
            future.add_done_callback(on_done)

        return all_done

    def _visit_fallback_parallel(
        self,
        selector_fn: Callable[[], Optional[List[GlobalIndexIOMeta]]],
        visitor_fn: Callable[[object], Optional[GlobalIndexResult]],
    ) -> 'Future[Optional[GlobalIndexResult]]':
        selected = selector_fn()
        if selected is None:
            return _completed_future(None)
        if not selected:
            return _completed_future(GlobalIndexResult.create_empty())
        if not self._fallback_scan_enabled(selected):
            return _completed_future(None)
        return self._visit_parallel(lambda: selected, visitor_fn)

    def _fallback_scan_enabled(self, files: List[GlobalIndexIOMeta]) -> bool:
        if self._fallback_scan_max_size <= 0:
            return False
        total_size = 0
        for file in files:
            if _LONG_MAX_VALUE - total_size < file.file_size:
                return False
            total_size += file.file_size
            if total_size > self._fallback_scan_max_size:
                return False
        return True

    def visit_equal(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        return self._visit_parallel(
            lambda: self._selector.select_equal(literal),
            lambda reader: reader.visit_equal(literal))

    def visit_not_equal(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        return self._visit_parallel(
            lambda: self._selector.select_not_equal(literal),
            lambda reader: reader.visit_not_equal(literal))

    def visit_less_than(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        if not self._can_fallback_scan(literal):
            return _completed_future(None)
        return self._visit_fallback_parallel(
            lambda: self._selector.select_less_than(literal),
            lambda reader: reader.visit_less_than(literal))

    def visit_less_or_equal(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        if not self._can_fallback_scan(literal):
            return _completed_future(None)
        return self._visit_fallback_parallel(
            lambda: self._selector.select_less_or_equal(literal),
            lambda reader: reader.visit_less_or_equal(literal))

    def visit_greater_than(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        if not self._can_fallback_scan(literal):
            return _completed_future(None)
        return self._visit_fallback_parallel(
            lambda: self._selector.select_greater_than(literal),
            lambda reader: reader.visit_greater_than(literal))

    def visit_greater_or_equal(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        if not self._can_fallback_scan(literal):
            return _completed_future(None)
        return self._visit_fallback_parallel(
            lambda: self._selector.select_greater_or_equal(literal),
            lambda reader: reader.visit_greater_or_equal(literal))

    def visit_is_null(self, field_ref: FieldRef) -> 'Future[Optional[GlobalIndexResult]]':
        return self._visit_parallel(
            lambda: self._selector.select_is_null(),
            lambda reader: reader.visit_is_null())

    def visit_is_not_null(self, field_ref: FieldRef) -> 'Future[Optional[GlobalIndexResult]]':
        return self._visit_parallel(
            lambda: self._selector.select_is_not_null(),
            lambda reader: reader.visit_is_not_null())

    def visit_in(self, field_ref: FieldRef, literals) -> 'Future[Optional[GlobalIndexResult]]':
        return self._visit_parallel(
            lambda: self._selector.select_in(literals),
            lambda reader: reader.visit_in(literals))

    def visit_not_in(self, field_ref: FieldRef, literals) -> 'Future[Optional[GlobalIndexResult]]':
        return self._visit_parallel(
            lambda: self._selector.select_not_in(literals),
            lambda reader: reader.visit_not_in(literals))

    def visit_starts_with(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        if not self._is_string_type(field_ref) or literal is None:
            return _completed_future(None)
        return self._visit_parallel(
            lambda: self._selector.select_starts_with(literal),
            lambda reader: reader.visit_starts_with(literal))

    def visit_ends_with(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        if not self._can_fallback_string_scan(field_ref, literal):
            return _completed_future(None)
        return self._visit_fallback_parallel(
            lambda: self._selector.select_ends_with(literal),
            lambda reader: reader.visit_ends_with(literal))

    def visit_contains(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        if not self._can_fallback_string_scan(field_ref, literal):
            return _completed_future(None)
        return self._visit_fallback_parallel(
            lambda: self._selector.select_contains(literal),
            lambda reader: reader.visit_contains(literal))

    def visit_like(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        if not self._is_string_type(field_ref) or literal is None:
            return _completed_future(None)

        optimized = self._try_optimize_like(literal)
        if optimized is not None:
            method, optimized_literal = optimized
            if method == "equal":
                return self.visit_equal(field_ref, optimized_literal)
            if method == "starts_with":
                return self.visit_starts_with(field_ref, optimized_literal)
            if method == "ends_with":
                return self.visit_ends_with(field_ref, optimized_literal)
            if method == "contains":
                return self.visit_contains(field_ref, optimized_literal)

        if not self._can_fallback_string_scan(field_ref, literal):
            return _completed_future(None)
        return self._visit_fallback_parallel(
            lambda: self._selector.select_like(literal),
            lambda reader: reader.visit_like(literal))

    def visit_between(self, field_ref: FieldRef, from_v, to_v) -> 'Future[Optional[GlobalIndexResult]]':
        if not self._can_fallback_scan(from_v) or to_v is None:
            return _completed_future(None)
        return self._visit_fallback_parallel(
            lambda: self._selector.select_between(from_v, to_v),
            lambda reader: reader.visit_between(from_v, to_v))

    def visit_not_between(self, field_ref: FieldRef, from_v, to_v) -> 'Future[Optional[GlobalIndexResult]]':
        if not self._can_fallback_scan(from_v) or to_v is None:
            return _completed_future(None)
        return self._visit_fallback_parallel(
            lambda: self._selector.select_or([
                self._selector.select_less_than(from_v),
                self._selector.select_greater_than(to_v),
            ]),
            lambda reader: GlobalIndexResult.create(
                RoaringBitmap64.or_(
                    reader.less_than(from_v),
                    reader.greater_than(to_v))))

    def _can_fallback_string_scan(self, field_ref: FieldRef, literal) -> bool:
        return (self._fallback_scan_max_size > 0
                and literal is not None
                and self._is_string_type(field_ref))

    def _can_fallback_scan(self, literal) -> bool:
        return self._fallback_scan_max_size > 0 and literal is not None

    @staticmethod
    def _is_string_type(field_ref: FieldRef) -> bool:
        data_type = str(getattr(field_ref, "data_type", "")).upper()
        if data_type.endswith(" NOT NULL"):
            data_type = data_type[:-9].rstrip()
        return (data_type == "STRING"
                or data_type == "CHAR"
                or data_type.startswith("CHAR(")
                or data_type == "VARCHAR"
                or data_type.startswith("VARCHAR("))

    @staticmethod
    def _try_optimize_like(literal):
        pattern = str(literal)
        if "_" in pattern:
            return None
        if "%" not in pattern and pattern:
            return "equal", pattern
        if (pattern.startswith("%")
                and pattern.endswith("%")
                and pattern.count("%") == 2
                and pattern[1:-1]):
            return "contains", pattern[1:-1]
        if pattern.startswith("%") and pattern.count("%") == 1 and pattern[1:]:
            return "ends_with", pattern[1:]
        if pattern.endswith("%") and pattern.count("%") == 1 and pattern[:-1]:
            return "starts_with", pattern[:-1]
        return None

    def close(self) -> None:
        with self._cache_lock:
            for reader in self._reader_cache.values():
                try:
                    reader.close()
                except Exception:
                    pass
            self._reader_cache.clear()
