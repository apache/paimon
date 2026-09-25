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
_NATIVE_WRITE_ENV = "PYPAIMON_TEST_NATIVE_WRITE"
_NATIVE_COMMIT_ENV = "PYPAIMON_TEST_NATIVE_COMMIT"
_native_plan_count = 0
_native_read_count = 0
_native_write_count = 0
_native_commit_count = 0
_force_native_for_test = False
_force_native_read_for_test = False
_force_native_write_for_test = False
_force_native_commit_for_test = False


def pytest_addoption(parser):
    parser.addoption(
        "--robomind-agilex-input",
        help="Downloaded RoboMIND AgileX directory for the optional sample test.",
    )


@pytest.fixture
def native_rest_catalog(tmp_path):
    """A local REST catalog for native writer and committer integration tests."""
    import uuid

    from pypaimon import CatalogFactory
    from pypaimon.api.api_response import ConfigResponse
    from pypaimon.api.auth import BearTokenAuthProvider
    from pypaimon.tests.rest.rest_server import RESTCatalogServer

    token = str(uuid.uuid4())
    server = RESTCatalogServer(
        data_path=str(tmp_path), auth_provider=BearTokenAuthProvider(token),
        config=ConfigResponse(defaults={'prefix': 'native-test'}), warehouse='warehouse')
    server.start()
    try:
        catalog = CatalogFactory.create({
            'metastore': 'rest', 'uri': server.get_url(), 'warehouse': 'warehouse',
            'token.provider': 'bear', 'token': token, 'data-token.enabled': 'false'})
        catalog.create_database('default', True)
        yield catalog
    finally:
        server.shutdown()


def _native_plan_enabled():
    return os.environ.get(_NATIVE_PLAN_ENV) == "1"


def _native_read_enabled():
    return os.environ.get(_NATIVE_READ_ENV) == "1"


def _native_write_enabled():
    return os.environ.get(_NATIVE_WRITE_ENV) == "1"


def _native_commit_enabled():
    return os.environ.get(_NATIVE_COMMIT_ENV) == "1"


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "python_plan: keep Python planner assertions on the Python lane")
    config.addinivalue_line(
        "markers", "python_read: keep Python reader assertions on the Python lane")
    config.addinivalue_line(
        "markers", "native_plan: exercise the real Rust planner in the Rust main CI job")
    config.addinivalue_line(
        "markers", "python_write: keep Python writer assertions on the Python lane")
    config.addinivalue_line(
        "markers", "python_commit: keep Python committer assertions on the Python lane")
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

    if _native_write_enabled():
        from pypaimon.write.native_write import NativeTableWrite

        original_write = NativeTableWrite.write_arrow_batch

        def tracked_write(self, data):
            global _native_write_count
            native = self._native_writer is not None
            result = original_write(self, data)
            if native and data.num_rows and _force_native_write_for_test:
                _native_write_count += 1
            return result

        NativeTableWrite.write_arrow_batch = tracked_write

    if _native_commit_enabled():
        from pypaimon.write.table_commit import TableCommit

        original_prepare = TableCommit._prepare_native_commit

        def tracked_prepare(self, messages):
            global _native_commit_count
            prepared = original_prepare(self, messages)
            if prepared is not None and _force_native_commit_for_test:
                _native_commit_count += 1
            return prepared

        TableCommit._prepare_native_commit = tracked_prepare


def pytest_collection_modifyitems(items):
    if _native_plan_enabled():
        return
    skip_native = pytest.mark.skip(reason="native plan tests run in the Rust Plan job")
    for item in items:
        if item.get_closest_marker("native_plan") is not None:
            item.add_marker(skip_native)


@pytest.fixture(autouse=True)
def enable_native_backends(request, monkeypatch):
    global _force_native_for_test, _force_native_read_for_test
    global _force_native_write_for_test, _force_native_commit_for_test
    python_plan = request.node.get_closest_marker("python_plan") is not None
    python_read = request.node.get_closest_marker("python_read") is not None
    python_write = request.node.get_closest_marker("python_write") is not None
    python_commit = request.node.get_closest_marker("python_commit") is not None
    native_plan_test = request.path.name in (
        "native_plan_test.py", "native_plan_integration_test.py",
        "native_plan_capabilities_test.py")
    force_plan = _native_plan_enabled() and not python_plan and not native_plan_test
    force_read = (_native_read_enabled() and not python_plan and not python_read
                  and not native_plan_test)
    force_write = _native_write_enabled() and not python_write
    force_commit = _native_commit_enabled() and not python_commit
    if not (force_plan or force_read or force_write or force_commit):
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
    if force_write:
        original_write = CoreOptions.native_write_enabled

        def write_enabled(self, default=None):
            return original_write(self, True if default is None else default)

        monkeypatch.setattr(CoreOptions, "native_write_enabled", write_enabled)
    if force_commit:
        original_commit = CoreOptions.native_commit_enabled

        def commit_enabled(self, default=None):
            return original_commit(self, True if default is None else default)

        monkeypatch.setattr(CoreOptions, "native_commit_enabled", commit_enabled)
    _force_native_for_test = force_plan
    _force_native_read_for_test = force_read
    _force_native_write_for_test = force_write
    _force_native_commit_for_test = force_commit
    try:
        yield
    finally:
        _force_native_for_test = False
        _force_native_read_for_test = False
        _force_native_write_for_test = False
        _force_native_commit_for_test = False


def pytest_sessionfinish(session, exitstatus):
    if exitstatus == 0:
        if ((_native_plan_enabled() and _native_plan_count == 0)
                or (_native_read_enabled() and _native_read_count == 0)
                or (_native_write_enabled() and _native_write_count == 0)
                or (_native_commit_enabled() and _native_commit_count == 0)):
            session.exitstatus = pytest.ExitCode.TESTS_FAILED


def pytest_terminal_summary(terminalreporter):
    if _native_plan_enabled():
        terminalreporter.write_line(
            "native plans exercised: %d" % _native_plan_count)
    if _native_read_enabled():
        terminalreporter.write_line(
            "native reads exercised: %d" % _native_read_count)
    if _native_write_enabled():
        terminalreporter.write_line(
            "native writes exercised: %d" % _native_write_count)
    if _native_commit_enabled():
        terminalreporter.write_line(
            "native commits exercised: %d" % _native_commit_count)
