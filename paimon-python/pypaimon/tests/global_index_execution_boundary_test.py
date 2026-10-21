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

"""Shared index contracts and Native builds do not load Python index executors."""

import importlib
from pathlib import Path
import pickle
import subprocess
import sys
import textwrap

import pytest


def test_native_index_read_build_and_drop_do_not_import_python_index_execution():
    script = r"""
import importlib.abc
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

class RejectPythonIndexes(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith('pypaimon.execution.python'):
            raise AssertionError('Python index execution imported: ' + fullname)
        if fullname in ('paimon_vindex', 'paimon_ftindex', 'lumina'):
            raise AssertionError('Python index SDK imported: ' + fullname)

sys.meta_path.insert(0, RejectPythonIndexes())
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.globalindex import GlobalIndexBuilder, GlobalIndexDropper
from pypaimon.globalindex.global_index_reader import GlobalIndexReader
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.globalindex.global_index_meta import GlobalIndexMeta
from pypaimon.globalindex.vindex_options import VINDEX_IDENTIFIERS
from pypaimon.schema.data_types import ArrayType, AtomicType, DataField
from pypaimon.execution.native.write import native_index_build, native_index_drop

runtime = ModuleType('pypaimon_rust')
runtime.__path__ = []
ffi = ModuleType('pypaimon_rust.datafusion')
ffi.GlobalIndexBuildBuilder = SimpleNamespace(supports_index_type=lambda kind: True)
ffi.GlobalIndexDropBuilder = SimpleNamespace(supports_index_type=lambda kind: True)
sys.modules['pypaimon_rust'] = runtime
sys.modules['pypaimon_rust.datafusion'] = ffi

native = Mock()
for name in ('with_index_column', 'with_index_columns', 'with_index_type', 'with_options'):
    getattr(native, name).return_value = native
native.build.return_value = []
native_table = Mock()
native_table.new_global_index_build_builder.return_value = native
native_table.new_global_index_drop_builder.return_value = native
options = CoreOptions(Options({
    'bucket': '-1', 'row-tracking.enabled': 'true', 'write.native.enabled': 'true',
}))
table = SimpleNamespace(
    options=options, identifier='db.table', is_primary_key_table=False,
    field_dict={'embedding': DataField(0, 'embedding', ArrayType('ARRAY', AtomicType('FLOAT')))},
)
# An empty Native result is a successful build/drop, never a Python fallback.
with patch.object(native_index_build, 'native_runtime_available', return_value=True), \
        patch.object(native_index_build, '_rest_catalog_supported', return_value=True), \
        patch.object(native_index_build, '_native_table', return_value=native_table), \
        patch.object(native_index_build, 'from_native_commit_messages', side_effect=lambda table, messages: messages), \
        patch('pypaimon.execution.native.runtime.native_runtime_available', return_value=True), \
        patch('pypaimon.execution.native.write.native_commit._rest_catalog_supported', return_value=True), \
        patch('pypaimon.execution.native.table._native_table', return_value=native_table), \
        patch('pypaimon.execution.native.write.native_commit.from_native_commit_messages',
              side_effect=lambda table, messages: messages):
    for kind in ('btree', 'bitmap', 'full-text') + VINDEX_IDENTIFIERS:
        assert GlobalIndexBuilder(table, 'embedding', index_type=kind).build() == []
        assert GlobalIndexDropper(table, 'embedding', index_type=kind).build() == []
assert native.build.call_count == 2 * (3 + len(VINDEX_IDENTIFIERS))

# The low-level vindex API also selects Rust before opening a Python SDK reader.
import io
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.vector_search import VectorSearch
from pypaimon.globalindex.vindex_reader import VindexVectorGlobalIndexReader
from pypaimon.execution.native.read import vindex_reader

class RustReader:
    failure_phase = None

    def __init__(self, input_, file_path, file_size, options):
        assert input_.pread_many([(0, 1)]) == [b'x']
        assert (file_path, file_size, options) == ('/indexes/file', 1, {'vindex.read.parallelism': '1'})
        if self.failure_phase == 'open':
            raise failure

    def metric(self):
        if self.failure_phase == 'metric':
            raise failure
        return 'inner_product'

    def search(self, vectors, limit, filter_bytes=None, options=None):
        assert limit == 2 and filter_bytes is None and options == {'ivf.nprobe': '3'}
        if self.failure_phase == 'search':
            raise failure
        return [{7: .5} for _ in vectors]

    def close(self):
        pass

with patch.object(vindex_reader, '_native_reader_type', return_value=RustReader):
    for phase in (None, 'open', 'metric', 'search'):
        RustReader.failure_phase = phase
        failure = OSError('Native ' + str(phase) + ' failed')
        stream = io.BytesIO(b'x')
        reader = VindexVectorGlobalIndexReader(
            SimpleNamespace(new_input_stream=lambda path: stream),
            '/indexes', [GlobalIndexIOMeta('file', 1)], {'vindex.read.parallelism': 1})
        try:
            if phase is None:
                assert reader.vector_metric() == 'inner_product'
                result = reader.visit_vector_search(VectorSearch([1., 2.], 2, 'embedding',
                                                               options={'ivf.nprobe': 3})).result()
                assert list(result.results()) == [7] and result.score_getter()(7) == .5
            else:
                try:
                    if phase == 'search':
                        reader.visit_vector_search(VectorSearch([1., 2.], 2, 'embedding',
                                                                options={'ivf.nprobe': 3}))
                    else:
                        reader.vector_metric()
                except OSError as error:
                    assert error is failure
                else:
                    raise AssertionError('Native error was hidden')
        finally:
            reader.close()
            reader.close()
        assert stream.closed
assert not any(name.startswith('pypaimon.execution.python') for name in sys.modules)
"""
    result = subprocess.run(
        [sys.executable, '-c', textwrap.dedent(script)],
        cwd=str(Path(__file__).resolve().parents[2]),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('public_module,new_module,name', [
    ('globalindex',
     'execution.python.globalindex.global_index_evaluator', 'GlobalIndexEvaluator'),
    ('globalindex',
     'execution.python.globalindex.data_evolution_global_index_scanner', 'DataEvolutionGlobalIndexScanner'),
    ('globalindex', 'execution.python.globalindex.key_serializer', 'KeySerializer'),
    ('globalindex', 'execution.python.globalindex.memory_slice_input', 'MemorySliceInput'),
])
def test_public_index_exports_resolve_to_canonical_class(public_module, new_module, name):
    public_module, new_module = 'pypaimon.' + public_module, 'pypaimon.' + new_module
    canonical = getattr(importlib.import_module(new_module), name)
    assert getattr(importlib.import_module(public_module), name) is canonical
    public_pickle = ('c%s\n%s\n.' % (public_module, name)).encode('ascii')
    assert pickle.loads(public_pickle) is canonical
