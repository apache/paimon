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


def test_native_read_schema_and_commit_path_adapters_do_not_load_python_casts():
    source = """
import importlib.abc
import sys
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
import pyarrow as pa

class BlockPythonCasts(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith(('pypaimon.execution.python', 'pypaimon_rust')):
            raise AssertionError('Shared Native adapter loaded execution: ' + fullname)

sys.meta_path.insert(0, BlockPythonCasts())
from pypaimon.schema.arrow_schema import read_arrow_schema
from pypaimon.schema.data_types import ArrayType, AtomicType, DataField, RowType
from pypaimon.execution.native.read.table_read import NativeTableRead
from pypaimon.execution.native.write.native_commit import native_messages_supported
from pypaimon.utils.file_store_path_factory import FileStorePathFactory, canonical_data_file_path

fields = [DataField(0, 'nested', RowType(False, [
    DataField(1, 'values', ArrayType(False, AtomicType('INT', nullable=False)))]))]
schema = read_arrow_schema(fields)
assert schema.field('nested').nullable
assert schema.field('nested').type.field('values').nullable
assert schema.field('nested').type.field('values').type.value_field.nullable
assert pa.array([{'values': [1, None]}, None], type=schema.field('nested').type).to_pylist() == [
    {'values': [1, None]}, None]

factory = FileStorePathFactory(
    '/warehouse', ['amount', 'created'], '__DEFAULT_PARTITION__', 'parquet', 'data-', 'changelog-',
    False, False, 'zstd', external_paths=['file:///external/a', 'file:///external/b'],
    external_path_strategy='entropy-inject', index_file_in_data_file_dir=True,
    partition_types=[AtomicType('DECIMAL(10,2)'), AtomicType('TIMESTAMP(6)')])
table = SimpleNamespace(path_factory=lambda: factory)
partition = (Decimal('2.30'), datetime(2024, 1, 2, 3, 4, 5, 123456))
path = canonical_data_file_path(table, partition, 0, 'data.parquet')
assert '/amount=2.30/created=2024-01-02 03%3A04%3A05.123456/bucket-0/' in path
index = SimpleNamespace(file_name='index-1', external_path=None)
assert factory.bucket_index_path(partition, 0, index) == path.rsplit('/', 1)[0] + '/index-1'
index.external_path = '/previous-external-root/index-1'
assert factory.bucket_index_path(partition, 0, index) == index.external_path
file = SimpleNamespace(external_path=None, file_path=path, file_name='data.parquet')
message = SimpleNamespace(partition=partition, bucket=0, new_files=[file], changelog_files=[],
                          compact_before=[], compact_after=[], compact_changelog_files=[],
                          compact_index_adds=[], compact_index_deletes=[])
assert native_messages_supported(table, [message])
file.file_path = path + '.wrong'
assert not native_messages_supported(table, [message])
assert not any(name.startswith(('pypaimon.execution.python', 'pypaimon_rust')) for name in sys.modules)
"""
    result = subprocess.run([sys.executable, '-c', source], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            universal_newlines=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr


def test_all_native_adapters_import_without_python_execution_or_sdk_initialization():
    source = """
import importlib
import importlib.abc
import pkgutil
import sys

class BlockExecution(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith(('pypaimon.execution.python', 'pypaimon_rust')):
            raise AssertionError('Native adapter initialized another executor: ' + fullname)

sys.meta_path.insert(0, BlockExecution())
from pypaimon.execution import native
for module in pkgutil.walk_packages(native.__path__, native.__name__ + '.'):
    importlib.import_module(module.name)
assert not any(name.startswith(('pypaimon.execution.python', 'pypaimon_rust')) for name in sys.modules)
"""
    result = subprocess.run([sys.executable, '-c', source], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            universal_newlines=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
