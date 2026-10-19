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

"""Native CI instrumentation preserves the public mutation API."""

import importlib.util
from pathlib import Path
from unittest.mock import Mock

import pytest

from pypaimon.write.table_commit import BatchTableCommit, TableCommit

pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]


@pytest.mark.parametrize('operation,args', [
    ('truncate_table', ()), ('truncate_partitions', ([{'pt': 'a'}],)),
])
@pytest.mark.parametrize('fail', [False, True])
def test_native_commit_tracking_forwards_batch_truncate(monkeypatch, operation, args, fail):
    # Load the actual CI hook in isolation. Its patches are scoped to this test
    # and must not replace the surrounding pytest session's counters.
    path = Path(__file__).resolve().parents[2] / 'conftest.py'
    spec = importlib.util.spec_from_file_location('native_instrumentation', str(path))
    instrumentation = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(instrumentation)
    for key in ('PLAN', 'READ', 'WRITE', 'UPDATE'):
        monkeypatch.setenv('PYPAIMON_TEST_NATIVE_' + key, '0')
    monkeypatch.setenv('PYPAIMON_TEST_NATIVE_COMMIT', '1')
    backend = Mock()
    calls = []

    def prepare(self, messages, *, batch=False):
        calls.append((messages, batch))
        return backend, []

    monkeypatch.setattr(TableCommit, '_prepare_native_commit', prepare)
    instrumentation.pytest_configure(Mock())
    instrumentation._force_native_commit_for_test = True
    commit = object.__new__(BatchTableCommit)
    commit.batch_committed = False
    commit.file_store_commit = Mock()
    error = RuntimeError('snapshot may already have published')
    if fail:
        getattr(backend, operation).side_effect = error
        with pytest.raises(RuntimeError) as raised:
            getattr(commit, operation)(*args)
        assert raised.value is error
    else:
        getattr(commit, operation)(*args)
    assert calls == [([], True)]
    assert instrumentation._native_commit_count == 1
    getattr(backend, operation).assert_called_once_with(*args)
    commit.file_store_commit.truncate_table.assert_not_called()
    commit.file_store_commit.drop_partitions.assert_not_called()
