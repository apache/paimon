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

"""Top-level pytest hooks for pypaimon tests."""

import os
import sys
import types

import pytest


_NATIVE_PLAN_ENV = "PYPAIMON_TEST_NATIVE_PLAN"
_native_plan_count = 0
_force_native_for_test = False


def _skip_package_init() -> bool:
    return os.environ.get("PYPAIMON_SKIP_PACKAGE_INIT", "").lower() in (
        "1",
        "true",
        "yes",
    )


# This hook must live outside the ``pypaimon`` package. Pytest imports a
# package-scoped conftest through ``pypaimon.__init__``, which is too late to
# bypass optional top-level imports for dependency-light unit test runs.
if _skip_package_init() and "pypaimon" not in sys.modules:
    _pypaimon = types.ModuleType("pypaimon")
    _pypaimon.__path__ = [os.path.join(os.path.dirname(__file__), "pypaimon")]
    sys.modules["pypaimon"] = _pypaimon


def _native_plan_enabled():
    return os.environ.get(_NATIVE_PLAN_ENV) == "1"


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "python_plan: keep Python planner assertions on the Python lane")
    if not _native_plan_enabled():
        return

    from pypaimon.read.table_scan import TableScan

    original = TableScan._try_native_plan

    def tracked(self):
        global _native_plan_count
        plan = original(self)
        if plan is not None and _force_native_for_test:
            _native_plan_count += 1
        return plan

    TableScan._try_native_plan = tracked


@pytest.fixture(autouse=True)
def enable_native_plan(request, monkeypatch):
    global _force_native_for_test
    if (not _native_plan_enabled()
            or request.node.get_closest_marker("python_plan") is not None
            or request.path.name in (
                "native_plan_test.py", "native_plan_integration_test.py")):
        yield
        return

    from pypaimon.common.options.core_options import CoreOptions

    original = CoreOptions.native_plan_enabled

    def enabled(self, default=None):
        return original(self, True if default is None else default)

    monkeypatch.setattr(CoreOptions, "native_plan_enabled", enabled)
    _force_native_for_test = True
    try:
        yield
    finally:
        _force_native_for_test = False


def pytest_sessionfinish(session, exitstatus):
    if _native_plan_enabled() and exitstatus == 0 and _native_plan_count == 0:
        session.exitstatus = pytest.ExitCode.TESTS_FAILED


def pytest_terminal_summary(terminalreporter):
    if _native_plan_enabled():
        terminalreporter.write_line(
            "native plans exercised: %d" % _native_plan_count)
