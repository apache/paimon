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

"""Ray coordination uses public hooks without selecting either file backend."""

from pathlib import Path
import pickle
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pyarrow as pa

from pypaimon import Schema
from pypaimon.write.ray_datasink import PaimonDatasink, PaimonWriteResult


def test_ray_datasink_configuration_and_pickle_do_not_load_execution():
    source = r'''
import importlib.abc
import pickle
import sys
from types import SimpleNamespace

class BlockExecution(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith(('pypaimon.execution.python', 'pypaimon.execution.native',
                                'pypaimon.execution.datasink', 'pypaimon_rust')):
            raise AssertionError('Ray configuration loaded execution: ' + fullname)

sys.meta_path.insert(0, BlockExecution())
from pypaimon.write.ray_datasink import PaimonDatasink, PaimonWriteResult

class Identifier:
    def get_full_name(self):
        return 'default.data'

table = SimpleNamespace(identifier=Identifier())
sink = PaimonDatasink(table, static_partition={'p': 'value'})
assert sink._is_overwrite()
restored = pickle.loads(pickle.dumps(sink))
assert type(restored) is PaimonDatasink
assert PaimonDatasink.__module__ == 'pypaimon.write.ray_datasink'
assert restored.__getstate__().keys() == sink.__getstate__().keys()
assert restored.static_partition == {'p': 'value'}
result = PaimonWriteResult(7, 11)
assert pickle.loads(pickle.dumps(result)) == result
assert type(result).__module__ == 'pypaimon.write.ray_datasink'
'''
    result = subprocess.run(
        [sys.executable, '-c', source],
        cwd=str(Path(__file__).resolve().parents[3]),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        universal_newlines=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr


def test_ray_worker_write_retains_public_hook_and_serializable_row_count():
    class HookedSink(PaimonDatasink):
        def _is_overwrite(self):
            self.overwrite_checks += 1
            return True

    table = Mock()
    table.identifier.get_full_name.return_value = 'default.data'
    table.table_schema = Schema.from_pyarrow_schema(pa.schema([('id', pa.int64())]))
    builder = table.new_batch_write_builder.return_value
    builder.overwrite.return_value = builder
    writer = builder.new_write.return_value
    writer.prepare_commit.return_value = []
    sink = HookedSink(table, static_partition={'p': 'value'})
    sink.overwrite_checks = 0
    messages = sink.write([pa.table({'id': [1, 2, 3]})], None)

    assert sink.overwrite_checks == 1
    builder.overwrite.assert_called_once_with({'p': 'value'})
    writer.write_arrow.assert_called_once()
    assert writer.write_arrow.call_args[0][0].to_pydict() == {'id': [1, 2, 3]}
    writer.prepare_commit.assert_called_once_with()
    writer.close.assert_called_once_with()
    writer.abort.assert_not_called()
    restored = pickle.loads(pickle.dumps(messages))
    assert restored == []
    assert restored.row_count == 3
    assert sink._writer_builder is None


def test_ray_commit_retains_public_hooks_and_reports_committed_snapshot():
    class HookedSink(PaimonDatasink):
        def on_write_start(self, schema=None):
            self.starts += 1
            return super().on_write_start(schema)

        @staticmethod
        def _extract_row_count(write_result, write_returns):
            return 23

    table = Mock()
    table.identifier.get_full_name.return_value = 'default.data'
    builder = table.new_batch_write_builder.return_value
    commit = builder.new_commit.return_value
    callbacks = []
    commit.add_commit_callback.side_effect = callbacks.append

    def publish(messages):
        for callback in callbacks:
            callback.call(SimpleNamespace(snapshot=SimpleNamespace(id=7)))

    commit.commit.side_effect = publish
    message = Mock()
    message.is_empty.return_value = False
    sink = HookedSink(table)
    sink.starts = 0
    sink.on_write_complete([[message]])

    assert sink.starts == 1
    assert sink.commit_result == PaimonWriteResult(23, 7)
    commit.commit.assert_called_once_with([message])
    commit.close.assert_called_once_with()
    table.snapshot_manager.assert_not_called()


def test_ray_coordination_can_run_without_importing_either_backend():
    source = r'''
import importlib.abc
import sys
from types import SimpleNamespace
from unittest.mock import Mock

class BlockBackends(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith(('pypaimon.execution.python', 'pypaimon.execution.native', 'pypaimon_rust')):
            raise AssertionError('Ray coordination selected a backend directly: ' + fullname)

sys.meta_path.insert(0, BlockBackends())
import pyarrow as pa
from pypaimon import Schema
from pypaimon.write.ray_datasink import PaimonDatasink, PaimonWriteResult

table = Mock()
table.identifier.get_full_name.return_value = 'default.data'
table.table_schema = Schema.from_pyarrow_schema(pa.schema([('id', pa.int64())]))
builder = table.new_batch_write_builder.return_value
writer = builder.new_write.return_value
message = Mock()
message.is_empty.return_value = False
writer.prepare_commit.return_value = [message]
commit = builder.new_commit.return_value
callbacks = []
commit.add_commit_callback.side_effect = callbacks.append
def publish(messages):
    for callback in callbacks:
        callback.call(SimpleNamespace(snapshot=SimpleNamespace(id=19)))
commit.commit.side_effect = publish

sink = PaimonDatasink(table)
messages = sink.write([pa.table({'id': [1, 2]})], None)
assert messages == [message]
sink.on_write_complete([messages])
assert sink.commit_result == PaimonWriteResult(2, 19)
sink.on_write_failed(RuntimeError('late scheduler failure'))
assert commit.commit.call_count == 1
writer.abort.assert_not_called()
assert not any(name.startswith(('pypaimon.execution.python', 'pypaimon.execution.native', 'pypaimon_rust'))
               for name in sys.modules)
'''
    result = subprocess.run(
        [sys.executable, '-c', source],
        cwd=str(Path(__file__).resolve().parents[3]),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        universal_newlines=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
