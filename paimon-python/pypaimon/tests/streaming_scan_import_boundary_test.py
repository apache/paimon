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

"""Importing a public streaming class loads only its own execution backend."""

import subprocess
import sys
import textwrap

import pytest

pytestmark = pytest.mark.skipif(
    sys.version_info < (3, 7), reason='Module-level lazy exports require Python 3.7')


def _run(source, *args):
    result = subprocess.run(
        [sys.executable, '-c', textwrap.dedent(source)] + list(args),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        universal_newlines=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('export, backend, forbidden', [
    ('StreamTableScan', 'native', 'python'),
    ('AsyncStreamingTableScan', 'python', 'native'),
])
def test_public_streaming_class_imports_only_its_backend(export, backend, forbidden):
    _run("""
        import importlib
        import importlib.abc
        import inspect
        import pickle
        import sys

        export, backend, forbidden = sys.argv[1:]
        blocked = 'pypaimon.execution.' + forbidden

        class RejectOtherBackend(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname == blocked or fullname.startswith(blocked + '.'):
                    raise AssertionError('Unexpected backend import: ' + fullname)

        sys.meta_path.insert(0, RejectOtherBackend())
        if backend == 'native':
            from pypaimon.read.streaming_table_scan import StreamTableScan as public_class
        else:
            from pypaimon.read.streaming_table_scan import AsyncStreamingTableScan as public_class
        module = importlib.import_module('pypaimon.read.streaming_table_scan')
        direct = importlib.import_module(
            'pypaimon.execution.' + backend + '.read.streaming_table_scan')
        canonical_class = getattr(direct, export)
        assert public_class is canonical_class
        assert getattr(module, export) is canonical_class
        assert inspect.signature(public_class) == inspect.signature(canonical_class)
        assert pickle.loads(pickle.dumps(public_class)) is canonical_class
        assert not any(name == blocked or name.startswith(blocked + '.')
                       for name in sys.modules)
    """, export, backend, forbidden)


def test_streaming_exports_are_discoverable_without_loading_either_backend():
    _run("""
        import importlib
        import importlib.abc
        import sys

        class RejectBackends(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname.startswith(('pypaimon.execution.python',
                                        'pypaimon.execution.native')):
                    raise AssertionError('Unexpected backend import: ' + fullname)

        sys.meta_path.insert(0, RejectBackends())
        module = importlib.import_module('pypaimon.read.streaming_table_scan')
        exports = ['AsyncStreamingTableScan', 'StreamTableScan']
        assert module.__all__ == exports
        assert all(name in dir(module) for name in exports)
        assert all(name not in module.__dict__ for name in exports)
        try:
            module.unknown_export
        except AttributeError as error:
            assert 'unknown_export' in str(error)
            assert 'pypaimon.read.streaming_table_scan' in str(error)
        else:
            raise AssertionError('An unknown export must raise AttributeError')
    """)
