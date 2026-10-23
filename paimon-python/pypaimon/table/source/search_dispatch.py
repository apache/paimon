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

    def read_plan(self, plan, executor=None):
        return (self._native_reader.read_plan(plan) if executor is None else
                self._native_reader.read_plan(plan, executor=executor))

    def read_batch_plan(self, plan, executor=None):
        return (self._native_reader.read_batch_plan(plan) if executor is None else
                self._native_reader.read_batch_plan(plan, executor=executor))


class _SplitSearchRead(_SearchRead):
    def read(self, splits):
        return self._native_reader.read(splits)


class _BatchSearchRead(_SearchRead):
    def read_batch(self, splits):
        return self._native_reader.read_batch(splits)


def python_search_reader(reader):
    """Select Python explicitly for backend parity and transported-plan checks."""
    return reader._python_reader() if isinstance(reader, _SearchRead) else reader


def _native_requested(builder):
    table = getattr(builder, '_table', None)
    enabled = getattr(getattr(table, 'options', None), 'native_read_enabled', None)
    return callable(enabled) and enabled()


def create_vector_scan(builder, python_factory, batch=False):
    if not _native_requested(builder):
        return python_factory()
    from pypaimon.execution.native.read.native_search_plan import native_vector_scan
    native = native_vector_scan(builder, batch)
    return python_factory() if native is None else _SearchScan(native, python_factory)


def create_vector_read(builder, python_factory, batch=False):
    if not _native_requested(builder):
        return python_factory()
    from pypaimon.execution.native.read.native_search_plan import native_vector_read
    native = native_vector_read(builder, batch)
    if native is None:
        return python_factory()
    reader_type = _BatchSearchRead if batch else _SplitSearchRead
    return reader_type(native, python_factory)


def create_full_text_scan(builder, python_factory):
    if not _native_requested(builder):
        return python_factory()
    from pypaimon.execution.native.read.native_search_plan import native_full_text_scan
    native = native_full_text_scan(builder)
    return python_factory() if native is None else _SearchScan(native, python_factory)


def create_full_text_read(builder, python_factory):
    if not _native_requested(builder):
        return python_factory()
    from pypaimon.execution.native.read.native_search_plan import native_full_text_read
    native = native_full_text_read(builder)
    return python_factory() if native is None else _SplitSearchRead(native, python_factory)


def execute_vector_search(builder, queries, batch=False):
    if _native_requested(builder):
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


def execute_hybrid_search(builder, executor=None):
    from pypaimon.table.source.hybrid_search_builder import HybridSearchBuilderImpl
    # Custom implementations execute through their public route/rank hooks;
    # only the default implementation owns the native configuration protocol.
    if isinstance(builder, HybridSearchBuilderImpl) and _native_requested(builder):
        from pypaimon.execution.native.read.native_hybrid_search import try_native_hybrid_search
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
