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

"""Storage and table metadata must not eagerly load Python write engines."""

import subprocess
import sys
from types import ModuleType
from unittest.mock import patch

import pytest


def test_native_table_preparation_does_not_load_python_write_implementations():
    source = """
import importlib.abc
import sys
from types import SimpleNamespace

class RejectPythonWriters(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith('pypaimon.execution.python'):
            raise AssertionError('Native preparation loaded a Python writer: ' + fullname)

sys.meta_path.insert(0, RejectPythonWriters())
from pypaimon.catalog.catalog_environment import CatalogEnvironment
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.filesystem.hdfs_native_file_io import HdfsNativeFileIO
from pypaimon.table.file_store_table import FileStoreTable
from pypaimon.execution.native.table import _resolved_schema_file_io_options

file_io = LocalFileIO()
table = SimpleNamespace(file_io=file_io, catalog_environment=CatalogEnvironment.empty())
assert _resolved_schema_file_io_options(table) == {}
assert FileStoreTable.__module__ == 'pypaimon.table.file_store_table'
"""
    result = subprocess.run([sys.executable, '-c', source], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            universal_newlines=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('link', ['direct', 'cause', 'context'])
def test_loaded_fork_error_is_propagated_without_loading_native(link):
    from pypaimon.write.backend_dispatch import propagate_fork_safety_error

    class ForkSafetyError(RuntimeError):
        pass

    sdk = ModuleType('pypaimon_rust')
    sdk.ForkSafetyError = ForkSafetyError
    failure = ForkSafetyError('inherited SDK state')
    wrapped = failure if link == 'direct' else RuntimeError('prefetch failed')
    if link == 'cause':
        wrapped.__cause__ = failure
    elif link == 'context':
        wrapped.__context__ = failure
    with patch.dict(sys.modules, {'pypaimon_rust': sdk, 'pypaimon.execution.native.runtime': None}):
        with pytest.raises(ForkSafetyError) as caught:
            propagate_fork_safety_error(wrapped)
    assert caught.value is failure


def test_loaded_fork_error_check_terminates_on_cyclic_exception_chain():
    from pypaimon.write.backend_dispatch import propagate_fork_safety_error

    sdk = ModuleType('pypaimon_rust')
    sdk.ForkSafetyError = type('ForkSafetyError', (RuntimeError,), {})
    first, second = ValueError('first'), RuntimeError('second')
    first.__cause__, second.__context__ = second, first
    with patch.dict(sys.modules, {'pypaimon_rust': sdk, 'pypaimon.execution.native.runtime': None}):
        assert propagate_fork_safety_error(first) is None


def test_python_streaming_prefetch_failure_never_loads_native():
    source = """
import importlib.abc
import sys
from types import SimpleNamespace
from unittest.mock import Mock
import pytest

class BlockNative(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith(('pypaimon.execution.native', 'pypaimon_rust')):
            raise AssertionError('Python prefetch loaded Native: ' + fullname)

sys.meta_path.insert(0, BlockNative())
from pypaimon.execution.python.read.streaming_table_scan import AsyncStreamingTableScan

scan = object.__new__(AsyncStreamingTableScan)
failure = OSError('prefetch unavailable')
scan._snapshot_manager = SimpleNamespace(find_next_scannable=Mock(side_effect=failure))
scan.follow_up_scanner = SimpleNamespace(should_scan=Mock())
scan._lookahead_size = 4
with pytest.raises(OSError) as caught:
    scan._fetch_plan_with_lookahead(1)
assert caught.value is failure
scan._snapshot_manager.find_next_scannable.assert_called_once_with(
    1, scan.follow_up_scanner.should_scan, lookahead_size=4)
assert not any(name.startswith(('pypaimon.execution.native', 'pypaimon_rust')) for name in sys.modules)
"""
    result = subprocess.run([sys.executable, '-c', source], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            universal_newlines=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
