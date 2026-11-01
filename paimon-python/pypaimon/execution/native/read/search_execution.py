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

"""Adapt opaque Rust search tasks to the shared scheduling contract."""

from concurrent.futures import CancelledError
from threading import Event, Lock

from pypaimon.execution.native.table import _native_table
from pypaimon.table.source.search_execution import SearchTask


def _execute_task(context, payload):
    # Core restores the captured schema/snapshot and owns file readers. Only
    # ordinary table configuration and opaque bytes cross the worker boundary.
    table, shared = context
    return _native_table(table)._execute_search_task(payload, shared)


def native_task_executor(table, executor):
    return _NativeTaskExecutor(table, executor)


class _NativeTaskExecutor:
    """One cancellation scope shared by all Core callbacks for a query."""

    def __init__(self, table, executor):
        self._table = table
        self._executor = executor.with_concurrency(executor.concurrency)
        self._cancelled = Event()
        self._failure_lock = Lock()
        self._failure = None

    def cancel(self):
        with self._failure_lock:
            self._cancelled.set()

    def _fail(self, failure):
        # Save the cause before waking sibling callbacks: their synthetic
        # cancellation may reach the Rust boundary before this callback closes.
        with self._failure_lock:
            if (self._failure is None and not self._cancelled.is_set()
                    and not isinstance(failure, GeneratorExit)):
                self._failure = failure
            # Once Core or another callback cancels the query, late failures
            # and cleanup errors must not replace its existing primary error.
            self._cancelled.set()

    def raise_if_failed(self):
        with self._failure_lock:
            failure = self._failure
        if failure is not None:
            raise failure

    def __call__(self, shared, payloads):
        try:
            if self._cancelled.is_set():
                raise CancelledError('Search query cancelled')
            with self._executor.cancellation_scope(self._cancelled):
                yield from self._execute(shared, payloads)
        except BaseException as failure:
            self._fail(failure)
            raise

    def _execute(self, shared, payloads):
        context = (self._table, shared)
        tasks = [SearchTask(_execute_task, context, payload) for payload in payloads]
        results = self._executor.map(tasks, ordered=True)
        completed = 0
        failed = False
        try:
            for ordinal, value in results:
                if self._cancelled.is_set():
                    raise CancelledError('Search query cancelled')
                if ordinal != completed or completed >= len(tasks):
                    raise ValueError('Search task executor did not preserve task order')
                completed += 1
                yield value
            if completed != len(tasks):
                raise ValueError('Search task executor returned an incomplete result sequence')
        except BaseException as failure:
            failed = True
            self._fail(failure)
            raise
        finally:
            close = getattr(results, 'close', None)
            if close is not None:
                try:
                    close()
                except BaseException as failure:
                    self._fail(failure)
                    if not failed:
                        raise
