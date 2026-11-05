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

from pathlib import Path
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
        if fullname == 'ray' or fullname.startswith('pypaimon.ray'):
            raise AssertionError('Ray integration imported for a local index: ' + fullname)
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
def copy_table(values):
    copied = SimpleNamespace(**vars(table))
    copied.options = CoreOptions(Options(dict(table.options.options.to_map(), **values)))
    return copied
table.copy_without_time_travel = copy_table
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
from pypaimon.globalindex.batch_vector_search import BatchVectorSearch
from pypaimon.globalindex.vindex_reader import VindexVectorGlobalIndexReader
from pypaimon.execution.native.read import vindex_reader

reader_options = {
    'vindex.read.parallelism': 1,
    'global-index.ivf-pq.index-column': 'embedding',
    'pk-vector.index.columns': 'embedding',
    'fields.embedding.pk-vector.index.type': 'ivf-pq',
    'fields.embedding.index-options': '{"nlist":64}',
    'opaque.enabled': True,
}
wire_options = dict(reader_options, **{'vindex.read.parallelism': '1', 'opaque.enabled': 'true'})
query_options = {'ivf.nprobe': 3, 'fields.embedding.index-options': '{"refine_factor":4}',
                 'index_type': 'ignored', 'opaque.query.option': 'preserved'}

class RustReader:
    failure_phase = None

    def __init__(self, input_, file_path, file_size, options):
        assert input_.pread_many([(0, 1)]) == [b'x']
        assert (file_path, file_size, options) == ('/indexes/file', 1, wire_options)
        if self.failure_phase == 'open':
            raise failure

    def metric(self):
        if self.failure_phase == 'metric':
            raise failure
        return 'inner_product'

    def search(self, vectors, limit, filter_bytes=None, options=None):
        assert limit == 2 and filter_bytes is None
        assert options == dict(query_options, **{'ivf.nprobe': '3'})
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
            '/indexes', [GlobalIndexIOMeta('file', 1)], reader_options)
        try:
            if phase is None:
                assert reader.vector_metric() == 'inner_product'
                result = reader.visit_vector_search(VectorSearch([1., 2.], 2, 'embedding',
                                                               options=query_options)).result()
                assert list(result.results()) == [7] and result.score_getter()(7) == .5
                results = reader.visit_batch_vector_search(BatchVectorSearch(
                    [[1., 2.], [2., 1.]], 2, 'embedding', options=query_options)).result()
                assert [list(result.results()) for result in results] == [[7], [7]]
            else:
                try:
                    if phase == 'search':
                        reader.visit_vector_search(VectorSearch([1., 2.], 2, 'embedding',
                                                                options=query_options))
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


def test_local_python_index_build_does_not_import_ray_integration():
    script = r"""
import importlib.abc
import sys
from types import SimpleNamespace
from unittest.mock import Mock

class RejectRay(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'ray' or fullname.startswith('pypaimon.ray'):
            raise AssertionError('Ray integration imported for a local index: ' + fullname)

sys.meta_path.insert(0, RejectRay())
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.globalindex.create_global_index import GlobalIndexBuilder
from pypaimon.globalindex.vindex_options import VINDEX_IDENTIFIERS
from pypaimon.schema.data_types import ArrayType, AtomicType, DataField

read_builder = Mock()
read_builder.new_scan.return_value.plan_all.return_value.splits.return_value = []
table = SimpleNamespace(
    options=CoreOptions(Options({
        'bucket': '-1', 'row-tracking.enabled': 'true', 'write.native.enabled': 'false',
    })),
    identifier='db.table', is_primary_key_table=False,
    field_dict={'embedding': DataField(0, 'embedding', ArrayType('ARRAY', AtomicType('FLOAT')))},
    new_read_builder=lambda: read_builder,
)
for kind in ('btree', 'bitmap', 'full-text') + VINDEX_IDENTIFIERS:
    builder = GlobalIndexBuilder(table, 'embedding', index_type=kind)
    assert builder.build() == []
    try:
        builder.build(execution='local', concurrency=1)
    except ValueError as error:
        assert "execution='ray'" in str(error)
    else:
        raise AssertionError('Local index build accepted Ray-only options')
assert not any(name == 'ray' or name.startswith('pypaimon.ray') for name in sys.modules)
"""
    result = subprocess.run(
        [sys.executable, '-c', textwrap.dedent(script)],
        cwd=str(Path(__file__).resolve().parents[2]),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.skipif(sys.version_info < (3, 7), reason='Python 3.6 retains eager package exports')
def test_public_index_exports_are_backend_independent_contracts():
    script = r"""
import importlib.abc
import pickle
import sys

class RejectExecution(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'pypaimon.execution' or fullname.startswith('pypaimon.execution.'):
            raise AssertionError('Public index export imported execution: ' + fullname)

sys.meta_path.insert(0, RejectExecution())
import pypaimon.globalindex as indexes

for name in indexes.__all__:
    exported = getattr(indexes, name)
    # Existing public import paths still resolve in transported references.
    reference = ('cpypaimon.globalindex\n%s\n.' % name).encode('ascii')
    assert pickle.loads(reference) is exported
for name in ('GlobalIndexEvaluator', 'DataEvolutionGlobalIndexScanner',
             'KeySerializer', 'MemorySliceInput', 'OffsetGlobalIndexReader',
             'SortedFileGlobalIndexReader', 'SortedFileMetaSelector'):
    assert name not in indexes.__all__ and not hasattr(indexes, name), name
assert not any(name == 'pypaimon.execution' or name.startswith('pypaimon.execution.')
               for name in sys.modules)
"""
    result = subprocess.run(
        [sys.executable, '-c', textwrap.dedent(script)],
        cwd=str(Path(__file__).resolve().parents[2]),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
