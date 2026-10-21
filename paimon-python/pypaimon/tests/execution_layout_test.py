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

"""Execution packages hold paired Paimon backends, not common adapters."""

from pathlib import Path
import subprocess
import sys

import pytest


def test_execution_package_contains_only_backend_implementations():
    root = Path(__file__).resolve().parents[1] / 'execution'
    modules = {path.name for path in root.iterdir() if path.is_file() and path.suffix == '.py'}
    packages = {path.name for path in root.iterdir()
                if path.is_dir() and any(path.glob('*.py'))}
    assert modules == {'__init__.py'}
    assert packages == {'python', 'native'}


@pytest.mark.skipif(sys.version_info < (3, 7), reason='Python 3.6 retains eager package exports')
def test_common_adapters_do_not_load_either_execution_backend():
    source = r"""
import importlib
import importlib.abc
import sys

class RejectExecution(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'pypaimon.execution' or fullname.startswith('pypaimon.execution.'):
            raise AssertionError('A common adapter imported an execution backend: ' + fullname)

sys.meta_path.insert(0, RejectExecution())
modules = [
    'pypaimon.read.read_dispatch', 'pypaimon.read.read_support',
    'pypaimon.read.read_explain', 'pypaimon.table.source.search_dispatch',
    'pypaimon.write.backend_dispatch', 'pypaimon.write.write_dispatch',
    'pypaimon.write.update_dispatch', 'pypaimon.write.input_adapter',
    'pypaimon.write.merge_condition', 'pypaimon.write.merge_transform',
    'pypaimon.globalindex.index_dispatch', 'pypaimon.globalindex.vindex_input',
    'pypaimon.table.system.system_table_read',
]
for name in modules:
    importlib.import_module(name)
assert not any(name == 'pypaimon.execution' or name.startswith('pypaimon.execution.')
               for name in sys.modules)
"""
    result = subprocess.run([sys.executable, '-c', source],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            universal_newlines=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
