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

"""Public streaming classes do not load an execution backend on import."""

import subprocess
import sys
import textwrap

import pytest


def _run(source, *args):
    result = subprocess.run(
        [sys.executable, '-c', textwrap.dedent(source)] + list(args),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        universal_newlines=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('export', ['StreamTableScan', 'AsyncStreamingTableScan'])
def test_public_streaming_class_imports_without_either_backend(export):
    _run("""
        import importlib
        import importlib.abc
        import pickle
        import sys

        export = sys.argv[1]

        class RejectBackends(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname.startswith(('pypaimon.execution.python', 'pypaimon.execution.native')):
                    raise AssertionError('Unexpected execution import: ' + fullname)

        sys.meta_path.insert(0, RejectBackends())
        module = importlib.import_module('pypaimon.read.streaming_table_scan')
        public_class = getattr(module, export)
        assert public_class.__module__ == module.__name__
        assert pickle.loads(pickle.dumps(public_class)) is public_class
    """, export)


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
        assert all(getattr(module, name).__module__ == module.__name__ for name in exports)
        try:
            module.unknown_export
        except AttributeError as error:
            assert 'unknown_export' in str(error)
            assert 'pypaimon.read.streaming_table_scan' in str(error)
        else:
            raise AssertionError('An unknown export must raise AttributeError')
    """)
