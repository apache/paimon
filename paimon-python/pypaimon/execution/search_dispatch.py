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

A live compatible plan executes in Rust. Transported plans and explicit split
reads select Python lazily. An execution error never triggers another search.
"""

from copy import copy


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

    def _reader_for_plan(self, plan):
        reader = copy(self._python_reader())
        if plan.snapshot() is not None:
            reader._table = reader._table._copy_with_snapshot(plan.snapshot())
        return reader

    def read_plan(self, plan):
        native = self._native_reader.resolve_plan(plan)
        if native is not None:
            return self._native_reader.read_plan(native)
        return self._reader_for_plan(plan).read_plan(plan)

    def read_batch_plan(self, plan):
        native = self._native_reader.resolve_plan(plan)
        if native is not None:
            return self._native_reader.read_batch_plan(native)
        return self._reader_for_plan(plan).read_batch_plan(plan)


class _SplitSearchRead(_SearchRead):
    def read(self, splits):
        return self._python_reader().read(splits)


class _BatchSearchRead(_SearchRead):
    def read_batch(self, splits):
        return self._python_reader().read_batch(splits)


def python_search_reader(reader):
    """Select Python explicitly for Ray split workers or plan comparisons."""
    return reader._python_reader() if isinstance(reader, _SearchRead) else reader


def _native_requested(builder):
    enabled = getattr(getattr(builder._table, 'options', None), 'native_read_enabled', None)
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


def execute_hybrid_search(builder):
    if _native_requested(builder):
        from pypaimon.execution.native.read.native_hybrid_search import try_native_hybrid_search
        result = try_native_hybrid_search(builder)
        if result is not None:
            return result
    from pypaimon.execution.python.read.search.hybrid_search import execute_local
    return execute_local(builder)
