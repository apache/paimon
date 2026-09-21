# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

import pytest


_NATIVE_PLAN_ENV = "PYPAIMON_TEST_NATIVE_PLAN"
_NATIVE_READ_ENV = "PYPAIMON_TEST_NATIVE_READ"
_native_plan_count = 0
_native_read_count = 0
_force_native_for_test = False
_force_native_read_for_test = False


def pytest_addoption(parser):
    parser.addoption(
        "--robomind-agilex-input",
        help="Downloaded RoboMIND AgileX directory for the optional sample test.",
    )


def _native_plan_enabled():
    return os.environ.get(_NATIVE_PLAN_ENV) == "1"


def _native_read_enabled():
    return os.environ.get(_NATIVE_READ_ENV) == "1"


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "python_plan: keep Python planner assertions on the Python lane")
    config.addinivalue_line(
        "markers", "python_read: keep Python reader assertions on the Python lane")
    config.addinivalue_line(
        "markers", "native_plan: exercise the real Rust planner in the Rust main CI job")
    if _native_plan_enabled():
        from pypaimon.read.table_scan import TableScan

        original_plan = TableScan._try_native_plan

        def tracked_plan(self):
            global _native_plan_count
            plan = original_plan(self)
            if plan is not None and _force_native_for_test:
                _native_plan_count += 1
            return plan

        TableScan._try_native_plan = tracked_plan

    if _native_read_enabled():
        from pypaimon.read.table_read import TableRead

        original_read = TableRead._try_native_batches

        def tracked_read(self, splits, *args, **kwargs):
            global _native_read_count
            batches = original_read(self, splits, *args, **kwargs)
            if batches is not None and splits and _force_native_read_for_test:
                _native_read_count += 1
            return batches

        TableRead._try_native_batches = tracked_read


@pytest.fixture(autouse=True)
def enable_native_plan_and_read(request, monkeypatch):
    global _force_native_for_test, _force_native_read_for_test
    python_plan = request.node.get_closest_marker("python_plan") is not None
    python_read = request.node.get_closest_marker("python_read") is not None
    native_plan_test = request.path.name in (
        "native_plan_test.py", "native_plan_integration_test.py",
        "native_plan_capabilities_test.py")
    force_plan = _native_plan_enabled() and not python_plan and not native_plan_test
    force_read = (_native_read_enabled() and not python_plan and not python_read
                  and not native_plan_test)
    if not (force_plan or force_read):
        yield
        return

    from pypaimon.common.options.core_options import CoreOptions

    if force_plan:
        original_plan = CoreOptions.native_plan_enabled

        def plan_enabled(self, default=None):
            return original_plan(self, True if default is None else default)

        monkeypatch.setattr(CoreOptions, "native_plan_enabled", plan_enabled)
    if force_read:
        original_read = CoreOptions.native_read_enabled

        def read_enabled(self, default=None):
            return original_read(self, True if default is None else default)

        monkeypatch.setattr(CoreOptions, "native_read_enabled", read_enabled)
    _force_native_for_test = force_plan
    _force_native_read_for_test = force_read
    try:
        yield
    finally:
        _force_native_for_test = False
        _force_native_read_for_test = False


def pytest_sessionfinish(session, exitstatus):
    if exitstatus == 0:
        if ((_native_plan_enabled() and _native_plan_count == 0)
                or (_native_read_enabled() and _native_read_count == 0)):
            session.exitstatus = pytest.ExitCode.TESTS_FAILED


def pytest_terminal_summary(terminalreporter):
    if _native_plan_enabled():
        terminalreporter.write_line(
            "native plans exercised: %d" % _native_plan_count)
    if _native_read_enabled():
        terminalreporter.write_line(
            "native reads exercised: %d" % _native_read_count)
