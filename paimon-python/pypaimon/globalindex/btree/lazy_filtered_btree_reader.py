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

"""A GlobalIndexReader that manages multiple BTree files with lazy reader creation and file pruning."""

import threading
from concurrent.futures import Executor, Future
from typing import Callable, Dict, List, Optional

from pypaimon.globalindex.btree.btree_file_meta_selector import BTreeFileMetaSelector
from pypaimon.globalindex.btree.btree_index_meta import BTreeIndexMeta
from pypaimon.globalindex.btree.btree_index_reader import BTreeIndexReader
from pypaimon.globalindex.btree.key_serializer import KeySerializer
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.global_index_reader import FieldRef, GlobalIndexReader, _completed_future
from pypaimon.globalindex.global_index_result import GlobalIndexResult


class LazyFilteredBTreeReader(GlobalIndexReader):
    """Manages multiple BTree index files for one range.

    Uses BTreeFileMetaSelector to prune files before I/O, and lazily
    creates/caches BTreeIndexReader instances only for files that are
    actually visited.
    """

    def __init__(
        self,
        key_serializer: KeySerializer,
        file_io,
        index_path: str,
        io_metas: List[GlobalIndexIOMeta],
        executor: Executor,
    ):
        self._key_serializer = key_serializer
        self._file_io = file_io
        self._index_path = index_path
        self._executor = executor

        files = []
        for io_meta in io_metas:
            idx_meta = BTreeIndexMeta.deserialize(io_meta.metadata)
            files.append((io_meta, idx_meta))

        self._selector = BTreeFileMetaSelector(files, key_serializer)
        self._reader_cache: Dict[str, BTreeIndexReader] = {}
        self._cache_lock = threading.Lock()

    def _get_or_create_reader(self, meta: GlobalIndexIOMeta) -> BTreeIndexReader:
        key = meta.external_path or meta.file_name
        with self._cache_lock:
            reader = self._reader_cache.get(key)
            if reader is not None:
                return reader
            reader = BTreeIndexReader(
                key_serializer=self._key_serializer,
                file_io=self._file_io,
                index_path=self._index_path,
                io_meta=meta,
            )
            self._reader_cache[key] = reader
            return reader

    def _visit_parallel(
        self,
        selector_fn: Callable[[], Optional[List[GlobalIndexIOMeta]]],
        visitor_fn: Callable[[BTreeIndexReader], Optional[GlobalIndexResult]],
    ) -> 'Future[Optional[GlobalIndexResult]]':
        selected = selector_fn()
        if selected is None:
            selected = [io_meta for io_meta, _ in self._selector._files]
        if not selected:
            return _completed_future(GlobalIndexResult.create_empty())

        # Single-level submit: reader creation + query in one task.
        # This avoids the nested submission deadlock when using a
        # semaphore-limited executor.
        task_futures: List[Future] = []
        for meta in selected:
            task_futures.append(self._executor.submit(
                lambda m=meta: visitor_fn(self._get_or_create_reader(m))))

        # Union all results once every task future is done
        all_done: Future = Future()
        remaining = [len(task_futures)]
        lock = threading.Lock()

        def on_done(_):
            with lock:
                remaining[0] -= 1
                if remaining[0] == 0:
                    try:
                        result: Optional[GlobalIndexResult] = None
                        for f in task_futures:
                            current = f.result()
                            if current is None:
                                continue
                            if result is None:
                                result = current
                            else:
                                result = result.or_(current)
                        all_done.set_result(result if result is not None
                                            else GlobalIndexResult.create_empty())
                    except Exception as e:
                        all_done.set_exception(e)

        for f in task_futures:
            f.add_done_callback(on_done)

        return all_done

    # ---- visit methods -------------------------------------------------------

    def visit_equal(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        return self._visit_parallel(
            lambda: self._selector.select_equal(literal),
            lambda r: r.visit_equal(literal))

    def visit_not_equal(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        return self._visit_parallel(
            lambda: self._selector.select_not_equal(literal),
            lambda r: r.visit_not_equal(literal))

    def visit_less_than(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        return self._visit_parallel(
            lambda: self._selector.select_less_than(literal),
            lambda r: r.visit_less_than(literal))

    def visit_less_or_equal(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        return self._visit_parallel(
            lambda: self._selector.select_less_or_equal(literal),
            lambda r: r.visit_less_or_equal(literal))

    def visit_greater_than(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        return self._visit_parallel(
            lambda: self._selector.select_greater_than(literal),
            lambda r: r.visit_greater_than(literal))

    def visit_greater_or_equal(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        return self._visit_parallel(
            lambda: self._selector.select_greater_or_equal(literal),
            lambda r: r.visit_greater_or_equal(literal))

    def visit_is_null(self, field_ref: FieldRef) -> 'Future[Optional[GlobalIndexResult]]':
        return self._visit_parallel(
            lambda: self._selector.select_is_null(),
            lambda r: r.visit_is_null())

    def visit_is_not_null(self, field_ref: FieldRef) -> 'Future[Optional[GlobalIndexResult]]':
        return self._visit_parallel(
            lambda: self._selector.select_is_not_null(),
            lambda r: r.visit_is_not_null())

    def visit_in(self, field_ref: FieldRef, literals) -> 'Future[Optional[GlobalIndexResult]]':
        return self._visit_parallel(
            lambda: self._selector.select_in(literals),
            lambda r: r.visit_in(literals))

    def visit_not_in(self, field_ref: FieldRef, literals) -> 'Future[Optional[GlobalIndexResult]]':
        return self._visit_parallel(
            lambda: self._selector.select_not_in(literals),
            lambda r: r.visit_not_in(literals))

    def visit_starts_with(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        return self._visit_parallel(
            lambda: self._selector.select_starts_with(literal),
            lambda r: r.visit_starts_with(literal))

    def visit_ends_with(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        return self._visit_parallel(
            lambda: self._selector.select_ends_with(literal),
            lambda r: r.visit_ends_with(literal))

    def visit_contains(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        return self._visit_parallel(
            lambda: self._selector.select_contains(literal),
            lambda r: r.visit_contains(literal))

    def visit_like(self, field_ref: FieldRef, literal) -> 'Future[Optional[GlobalIndexResult]]':
        return self._visit_parallel(
            lambda: self._selector.select_like(literal),
            lambda r: r.visit_like(literal))

    def visit_between(self, field_ref: FieldRef, from_v, to_v) -> 'Future[Optional[GlobalIndexResult]]':
        return self._visit_parallel(
            lambda: self._selector.select_between(from_v, to_v),
            lambda r: r.visit_between(from_v, to_v))

    def close(self) -> None:
        with self._cache_lock:
            for reader in self._reader_cache.values():
                try:
                    reader.close()
                except Exception:
                    pass
            self._reader_cache.clear()
