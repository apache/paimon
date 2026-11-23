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

"""Coordinate search execution and transport without coupling the backends.

The selected backend consumes ordinary snapshot-and-split plans, including
transported plans. An execution error never triggers another search.
"""

from contextlib import contextmanager
from copy import copy
from functools import partial
from threading import local


_search_execution = local()


@contextmanager
def _python_search_execution():
    """Retain Python selection through hooks delegating to public methods."""
    previous = getattr(_search_execution, 'python', False)
    _search_execution.python = True
    try:
        yield
    finally:
        _search_execution.python = previous


def _python_search_request(request):
    if getattr(_search_execution, 'python', False):
        request = copy(request)
        request._table = request._table.copy_without_time_travel({
            'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    return request


def _python_vector_scan(execution):
    from pypaimon.execution.python.read.search.factories import new_vector_scan
    return new_vector_scan(_python_search_request(execution))


def _python_vector_read(builder, batch):
    from pypaimon.execution.python.read.search.factories import new_batch_vector_read, new_vector_read
    builder = _python_search_request(builder)
    return new_batch_vector_read(builder) if batch else new_vector_read(builder)


def _python_full_text_scan(execution):
    from pypaimon.execution.python.read.search.factories import new_full_text_scan
    return new_full_text_scan(_python_search_request(execution))


def _python_full_text_read(execution):
    from pypaimon.execution.python.read.search.factories import new_full_text_read
    return new_full_text_read(_python_search_request(execution))


class _SearchScan:
    def __init__(self, native, python_factory):
        self._native_scan = native
        self._python_factory = python_factory
        self._python_executor = None

    def scan(self):
        if self._native_scan.available():
            return self._native_scan.scan()
        if self._python_executor is None:
            self._python_executor = self._python_factory()
        return self._python_executor.scan()


class _SearchRead:
    def __init__(self, native, python_factory):
        self._native_reader = native
        self._python_factory = python_factory
        self._python_executor = None

    def _python_reader(self):
        if self._python_executor is None:
            self._python_executor = self._python_factory()
        return self._python_executor

    def _reader_for(self, plan=None, splits=()):
        return self._native_reader

    def read_plan(self, plan, executor=None):
        reader = self._reader_for(plan)
        return reader.read_plan(plan) if executor is None else reader.read_plan(plan, executor=executor)

    def read_batch_plan(self, plan, executor=None):
        reader = self._reader_for(plan)
        return reader.read_batch_plan(plan) if executor is None else reader.read_batch_plan(plan, executor=executor)


class _SplitSearchRead(_SearchRead):
    def read(self, splits):
        return self._native_reader.read(splits)


class _BatchSearchRead(_SearchRead):
    def read_batch(self, splits):
        return self._native_reader.read_batch(splits)


class _FilesystemVectorScan:
    def __init__(self, builder, batch):
        self._builder = builder
        self._batch = batch

    def scan(self):
        from pypaimon.execution.native.read.native_search_plan import native_vector_scan
        from pypaimon.execution.native.read.native_vector_search import (
            _filesystem_vector_search_view, _has_centroid_indexes,
        )

        request = copy(self._builder)
        request._table, entries = _filesystem_vector_search_view(request._table, request._partition_filter)
        if _has_centroid_indexes(entries, [request._vector_column]):
            scan = native_vector_scan(request, self._batch)
            if scan is not None:
                return scan.scan()
        return _python_vector_scan(request).scan(partition_entries=entries)


class _FilesystemVectorRead(_SearchRead):
    def __init__(self, native_factory, python_factory):
        super().__init__(None, python_factory)
        self._native_factory = native_factory

    def _reader_for(self, plan=None, splits=()):
        from pypaimon.table.source.vector_search_split import IndexVectorSearchSplit

        if plan is not None:
            splits = plan.splits()
        native = any(getattr(split, '_native_vector_layout', False) for split in splits) or any(
            file.global_index_meta is not None and file.global_index_meta.is_centroid_index()
            for split in splits if isinstance(split, IndexVectorSearchSplit) for file in split.vector_index_files)
        if native:
            if self._native_reader is None:
                self._native_reader = self._native_factory()
            if self._native_reader is not None:
                return self._native_reader
        return self._python_reader()

    def read(self, splits):
        splits = list(splits)
        return self._reader_for(splits=splits).read(splits)

    def read_batch(self, splits):
        splits = list(splits)
        return self._reader_for(splits=splits).read_batch(splits)


def python_search_reader(reader):
    """Select Python explicitly for backend parity and transported-plan checks."""
    return reader._python_reader() if isinstance(reader, _SearchRead) else reader


def _native_requested(builder):
    if getattr(_search_execution, 'python', False):
        return False
    table = getattr(builder, '_table', None)
    enabled = getattr(getattr(table, 'options', None), 'native_read_enabled', None)
    return callable(enabled) and enabled()


def _filesystem_vector_layout_dispatch(builder):
    from pypaimon.execution.native.read.native_vector_search import (
        _filesystem_vector_search_supported, _multi_vector_requested,
    )
    return (not getattr(builder, '_native_hybrid_route', False)
            and not _multi_vector_requested(builder) and _filesystem_vector_search_supported(builder._table))


def create_vector_scan(builder, execution, batch=False):
    python_factory = partial(_python_vector_scan, execution)
    if not _native_requested(builder):
        return python_factory()
    from pypaimon.execution.native.read.native_search_plan import native_vector_scan
    if _filesystem_vector_layout_dispatch(builder):
        return _FilesystemVectorScan(execution, batch)
    native = native_vector_scan(builder, batch)
    return python_factory() if native is None else _SearchScan(native, python_factory)


def create_vector_read(builder, batch=False):
    python_factory = partial(_python_vector_read, builder, batch)
    if not _native_requested(builder):
        return python_factory()
    from pypaimon.execution.native.read.native_search_plan import native_vector_read
    if _filesystem_vector_layout_dispatch(builder):
        return _FilesystemVectorRead(partial(native_vector_read, builder, batch), python_factory)
    native = native_vector_read(builder, batch)
    if native is None:
        return python_factory()
    reader_type = _BatchSearchRead if batch else _SplitSearchRead
    return reader_type(native, python_factory)


def create_full_text_scan(builder, execution):
    python_factory = partial(_python_full_text_scan, execution)
    if not _native_requested(builder):
        return python_factory()
    from pypaimon.execution.native.read.native_search_plan import native_full_text_scan
    native = native_full_text_scan(builder)
    return python_factory() if native is None else _SearchScan(native, python_factory)


def create_full_text_read(builder, execution):
    python_factory = partial(_python_full_text_read, execution)
    if not _native_requested(builder):
        return python_factory()
    from pypaimon.execution.native.read.native_search_plan import native_full_text_read
    native = native_full_text_read(builder)
    return python_factory() if native is None else _SplitSearchRead(native, python_factory)


def execute_vector_search(builder, queries, batch=False):
    if _native_requested(builder) and not _filesystem_vector_layout_dispatch(builder):
        from pypaimon.execution.native.read.native_vector_search import try_native_vector_search
        result = try_native_vector_search(builder, queries, batch)
        if result is not None:
            return result
    reader = (builder.new_batch_vector_search_read() if batch
              else builder.new_vector_search_read())
    plan = builder.new_vector_search_scan().scan()
    return reader.read_batch_plan(plan) if batch else reader.read_plan(plan)


def execute_full_text_search(builder):
    if _native_requested(builder):
        from pypaimon.execution.native.read.native_full_text_search import try_native_full_text_search
        result = try_native_full_text_search(builder)
        if result is not None:
            return result
    return builder.new_full_text_read().read_plan(builder.new_full_text_scan().scan())


def _standard_hybrid_builder(builder):
    from pypaimon.table.source.hybrid_search_builder import HybridSearchBuilderImpl
    # Instance and subclass hooks own their execution protocol. Native cannot
    # replace them, and copying their owner would change observable side effects.
    return type(builder) is HybridSearchBuilderImpl and not any(name in vars(builder) for name in (
        'route_builders', 'to_route_result', 'rank', '_new_vector_search_builder', '_new_full_text_search_builder'))


def _hybrid_search_request(builder):
    from pypaimon.catalog.table_query_auth import reject_search_under_query_auth
    from pypaimon.execution.native.read.native_vector_search import (
        _filesystem_vector_search_supported, _filesystem_vector_search_view, _has_centroid_indexes,
    )
    from pypaimon.globalindex.vindex_options import is_multi_vector_type

    reject_search_under_query_auth(builder._table)
    if not _filesystem_vector_search_supported(builder._table):
        return builder, True
    builder._validate_search()
    try:
        fields = [builder._table.field_dict[route.field_name] for route in builder._routes if route.is_vector()]
    except KeyError as exc:
        raise ValueError("Vector column '%s' not found in table schema" % exc.args[0]) from exc
    if not fields:
        return builder, False
    if any(is_multi_vector_type(field.type) for field in fields):
        return builder, True
    request = copy(builder)
    request._table, entries = _filesystem_vector_search_view(builder._table, builder._partition_filter)
    return request, _has_centroid_indexes(entries, fields)


def execute_hybrid_search(builder, executor=None):
    standard = _standard_hybrid_builder(builder)
    if standard and _native_requested(builder):
        from pypaimon.execution.native.read.native_hybrid_search import try_native_hybrid_search
        builder, native = _hybrid_search_request(builder)
        if native:
            result = (try_native_hybrid_search(builder) if executor is None else
                      try_native_hybrid_search(builder, executor=executor))
            if result is not None:
                return result
    from pypaimon.execution.python.read.search.hybrid_search import execute
    return execute(builder, executor)


def hybrid_route_builders(builder):
    from pypaimon.catalog.table_query_auth import reject_search_under_query_auth
    # Authorization precedes runtime discovery and snapshot resolution, as in
    # Java's routeBuilders(). Selecting a backend must not hide a rejection.
    reject_search_under_query_auth(builder._table)
    if _native_requested(builder):
        from pypaimon.execution.native.read.native_hybrid_search import try_native_hybrid_routes
        builder, native = _hybrid_search_request(builder)
        if native:
            routes = try_native_hybrid_routes(builder)
            if routes is not None:
                return routes
    from pypaimon.execution.python.read.search.hybrid_search import route_builders
    return route_builders(builder)


def rank_hybrid_results(builder, route_results):
    builder._validate_search()
    if _native_requested(builder):
        from pypaimon.execution.native.runtime import native_runtime_available
        if native_runtime_available():
            from pypaimon.execution.native.read.native_hybrid_search import rank_hybrid_results as rank
            return rank(builder, route_results)
    from pypaimon.execution.python.read.search.hybrid_search import rank
    return rank(builder, route_results)
