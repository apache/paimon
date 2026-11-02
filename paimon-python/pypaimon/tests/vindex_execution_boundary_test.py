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

"""The low-level public reader delegates backend selection and input ownership."""

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from pathlib import Path
import subprocess
import sys
import textwrap
import threading
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.vector_search import VectorSearch
from pypaimon.globalindex.vindex_reader import VindexVectorGlobalIndexReader


_NATIVE_FACTORY = 'pypaimon.execution.native.read.vindex_reader.create_native_vindex_reader'
_PYTHON_READER = ('pypaimon.execution.python.globalindex.vindex.'
                  'vindex_vector_global_index_reader.PythonVindexVectorGlobalIndexReader')


def _reader(reader_type=VindexVectorGlobalIndexReader):
    return reader_type(SimpleNamespace(), '/indexes', [GlobalIndexIOMeta('index', 1)], {'option': 'value'})


def test_constructing_and_closing_public_reader_does_not_import_an_execution_backend():
    script = r"""
import importlib.abc
import sys
from types import SimpleNamespace

class RejectBackends(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith(('pypaimon.execution.native', 'pypaimon.execution.python')):
            raise AssertionError('Constructing a public reader imported ' + fullname)

sys.meta_path.insert(0, RejectBackends())
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.vindex_reader import VindexVectorGlobalIndexReader
reader = VindexVectorGlobalIndexReader(SimpleNamespace(), '/indexes', [GlobalIndexIOMeta('index', 1)])
assert reader._reader is None and reader._index_input is None
with reader as entered:
    assert entered is reader
reader.close()
assert not any(name.startswith(('pypaimon.execution.native', 'pypaimon.execution.python')) for name in sys.modules)
"""
    result = subprocess.run(
        [sys.executable, '-c', textwrap.dedent(script)],
        cwd=str(Path(__file__).resolve().parents[2]),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_unavailable_native_reader_selects_python_once_and_preserves_public_load_hook():
    class Reader(VindexVectorGlobalIndexReader):
        def _ensure_loaded(self):
            self.loaded = getattr(self, 'loaded', 0) + 1
            return super()._ensure_loaded()

    backend = Mock()
    backend.vector_metric.return_value = 'cosine'
    backend._run_search.return_value = ['scores']
    reader = _reader(Reader)
    with patch(_NATIVE_FACTORY, return_value=None) as native, patch(_PYTHON_READER, return_value=backend) as python:
        assert reader.vector_metric() == 'cosine'
        assert reader.visit_vector_search(VectorSearch([1., 2.], 3, 'embedding')).result() == 'scores'
        assert reader.loaded == 2
        assert reader._reader is backend._reader
        assert reader._index_input is backend._index_input
        native.assert_called_once_with(reader._file_io, '/indexes', [reader._io_meta], {'option': 'value'})
        python.assert_called_once_with(reader._file_io, '/indexes', [reader._io_meta], {'option': 'value'})
        backend._ensure_loaded.assert_called_once_with()
        reader.close()
        reader.close()
        assert reader._reader is None and reader._index_input is None
        backend.close.assert_called_once_with()


@pytest.mark.parametrize('phase', ['prepare', 'load', 'query', 'metric', 'close'])
def test_native_reader_errors_never_select_python(phase):
    failure = OSError('Native ' + phase + ' failed')
    backend = Mock()
    method = {'load': '_ensure_loaded', 'query': '_run_search', 'metric': 'vector_metric', 'close': 'close'}
    if phase in method:
        getattr(backend, method[phase]).side_effect = failure
    reader = _reader()
    with patch(_NATIVE_FACTORY, return_value=backend,
               side_effect=failure if phase == 'prepare' else None), \
            patch(_PYTHON_READER, side_effect=AssertionError('Native error selected Python')) as python:
        with pytest.raises(OSError) as raised:
            if phase == 'close':
                reader._ensure_loaded()
                reader.close()
            elif phase == 'metric':
                reader.vector_metric()
            else:
                reader.visit_vector_search(VectorSearch([1., 2.], 3, 'embedding'))
        assert raised.value is failure
        python.assert_not_called()
        reader.close()


def test_concurrent_queries_open_one_backend():
    entered = threading.Event()
    release = threading.Event()
    backend = Mock()
    backend.vector_metric.return_value = 'l2'

    def load():
        entered.set()
        assert release.wait(5), 'Reader initialization was not released'

    backend._ensure_loaded.side_effect = load
    reader = _reader()
    with patch(_NATIVE_FACTORY, return_value=backend) as create, ThreadPoolExecutor(2) as pool:
        first = pool.submit(reader.vector_metric)
        try:
            assert entered.wait(5), 'Reader initialization did not start'
            second = pool.submit(reader.vector_metric)
            release.set()
            assert first.result(5) == second.result(5) == 'l2'
            create.assert_called_once()
            backend._ensure_loaded.assert_called_once_with()
        finally:
            release.set()
            reader.close()
    backend.close.assert_called_once_with()


def test_close_waits_for_initialization_and_releases_the_opened_backend():
    entered = threading.Event()
    release = threading.Event()
    backend = Mock()

    def load():
        entered.set()
        assert release.wait(5), 'Reader initialization was not released'

    backend._ensure_loaded.side_effect = load
    reader = _reader()
    with patch(_NATIVE_FACTORY, return_value=backend), ThreadPoolExecutor(2) as pool:
        loading = pool.submit(reader._ensure_loaded)
        try:
            assert entered.wait(5), 'Reader initialization did not start'
            closing = pool.submit(reader.close)
            with pytest.raises(FutureTimeoutError):
                closing.result(.05)
            release.set()
            loading.result(5)
            closing.result(5)
        finally:
            release.set()
            reader.close()
    backend.close.assert_called_once_with()
    assert reader._reader is None and reader._index_input is None


def _call_without_deadlock(operation):
    results = []
    errors = []

    def run():
        try:
            results.append(operation())
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(3)
    assert not thread.is_alive(), 'Reentrant reader close deadlocked'
    if errors:
        raise errors[0]
    return results[0]


@pytest.mark.parametrize('phase', ['prepare', 'load'])
@pytest.mark.parametrize('cleanup_fails', [False, True])
def test_close_reentered_during_load_cancels_candidate_without_deadlock(phase, cleanup_fails):
    reader = _reader()
    candidate = Mock()
    replacement = Mock()
    replacement.vector_metric.return_value = 'cosine'
    cleanup_error = OSError('candidate cleanup failed')
    if cleanup_fails:
        candidate.close.side_effect = cleanup_error
    if phase == 'load':
        candidate._ensure_loaded.side_effect = reader.close

    def create(*args):
        if phase == 'prepare':
            reader.close()
        return candidate

    with patch(_NATIVE_FACTORY, side_effect=create) as factory, \
            patch(_PYTHON_READER, side_effect=AssertionError('Closed Native reader selected Python')) as python:
        with pytest.raises(RuntimeError, match='closed during initialization') as raised:
            _call_without_deadlock(reader.vector_metric)
        if cleanup_fails:
            assert raised.value.__cause__ is cleanup_error
        candidate.close.assert_called_once_with()
        if phase == 'prepare':
            candidate._ensure_loaded.assert_not_called()
        else:
            candidate._ensure_loaded.assert_called_once_with()
        assert reader._reader is None and reader._index_input is None
        python.assert_not_called()
        factory.side_effect = None
        factory.return_value = replacement
        assert reader.vector_metric() == 'cosine'
        reader.close()
        replacement.close.assert_called_once_with()


def test_backend_close_can_reenter_public_close_and_reader_can_reopen():
    reader = _reader()
    backend = Mock()
    backend.close.side_effect = reader.close
    replacement = Mock()
    replacement.vector_metric.return_value = 'l2'

    with patch(_NATIVE_FACTORY, side_effect=[backend, replacement]):
        reader._ensure_loaded()
        _call_without_deadlock(reader.close)
        backend.close.assert_called_once_with()
        assert reader._reader is None and reader._index_input is None
        assert reader.vector_metric() == 'l2'
        reader.close()
        replacement.close.assert_called_once_with()
