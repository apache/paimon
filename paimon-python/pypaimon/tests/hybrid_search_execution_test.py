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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for local hybrid-search route execution."""

import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

from pypaimon.table.source.hybrid_search_builder import (
    HybridSearchBuilderImpl,
    HybridSearchRouteBuilder,
)


class _CallableSearchBuilder:

    def __init__(self, execute):
        self._execute = execute

    def execute_local(self):
        return self._execute()


def _execution_builder(executions):
    route_builders = [
        HybridSearchRouteBuilder(
            "route-%d" % index, _CallableSearchBuilder(execute))
        for index, execute in enumerate(executions)
    ]
    builder = HybridSearchBuilderImpl(table=None)
    builder.route_builders = lambda: route_builders
    builder.to_route_result = (
        lambda route_builder, result: (route_builder.route, result))
    builder.rank = lambda route_results: route_results
    return builder


class HybridSearchExecutionTest(unittest.TestCase):

    def test_executes_routes_concurrently_and_preserves_order(self):
        started = threading.Barrier(2)

        def execute(index, delay):
            started.wait(timeout=2.0)
            time.sleep(delay)
            return "result-%d" % index

        builder = _execution_builder([
            lambda: execute(0, 0.03),
            lambda: execute(1, 0.0),
        ])

        self.assertEqual(
            [("route-0", "result-0"), ("route-1", "result-1")],
            builder.execute_local(),
        )

    def test_single_route_avoids_executor(self):
        builder = _execution_builder([lambda: "result"])

        with mock.patch(
                "pypaimon.table.source.hybrid_search_builder."
                "ThreadPoolExecutor") as executor:
            self.assertEqual(
                [("route-0", "result")], builder.execute_local())

        executor.assert_not_called()

    def test_caps_route_workers(self):
        builder = _execution_builder([
            lambda index=index: index for index in range(6)
        ])
        worker_counts = []

        def new_executor(*args, **kwargs):
            worker_counts.append(kwargs["max_workers"])
            return ThreadPoolExecutor(*args, **kwargs)

        with mock.patch(
                "pypaimon.table.source.hybrid_search_builder."
                "ThreadPoolExecutor", side_effect=new_executor):
            builder.execute_local()

        self.assertEqual([4], worker_counts)

    def test_propagates_failure_after_started_routes_finish(self):
        started = threading.Barrier(2)
        failed = threading.Event()
        release = threading.Event()
        finished = threading.Event()
        outcome = {}

        def fail():
            started.wait(timeout=2.0)
            failed.set()
            raise RuntimeError("route failed")

        def block():
            started.wait(timeout=2.0)
            release.wait(timeout=2.0)
            finished.set()
            return "result"

        builder = _execution_builder([fail, block])

        def execute():
            try:
                builder.execute_local()
            except BaseException as error:
                outcome["error"] = error

        caller = threading.Thread(target=execute)
        caller.start()
        self.assertTrue(failed.wait(timeout=2.0))
        self.assertTrue(caller.is_alive())
        release.set()
        caller.join(timeout=2.0)

        self.assertFalse(caller.is_alive())
        self.assertTrue(finished.is_set())
        self.assertIsInstance(outcome.get("error"), RuntimeError)
        self.assertEqual("route failed", str(outcome["error"]))


if __name__ == "__main__":
    unittest.main()
