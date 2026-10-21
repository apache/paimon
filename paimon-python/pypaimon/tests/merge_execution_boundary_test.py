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

"""Local MERGE execution must not import distributed Ray orchestration."""

import importlib.util
import subprocess
import sys

import pytest


@pytest.mark.parametrize('engine', ['python', 'native'])
def test_merge_execution_does_not_load_ray_or_the_other_backend(tmp_path, engine):
    if sys.version_info < (3, 10):
        pytest.skip('MERGE condition expressions require Python 3.10')
    if engine == 'python' and importlib.util.find_spec('datafusion') is None:
        pytest.skip('Python MERGE conditions require the datafusion extra')
    source = r"""
import importlib.abc
import sys
from unittest.mock import Mock, patch
import pyarrow as pa

engine, warehouse = sys.argv[1:]
blocked = 'pypaimon.execution.' + ('native' if engine == 'python' else 'python')
class BlockExecution(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if (fullname == 'ray' or fullname.startswith('ray.')
                or fullname.startswith('pypaimon.ray')
                or fullname == blocked or fullname.startswith(blocked + '.')
                or (engine == 'python' and fullname.startswith('pypaimon_rust'))):
            raise AssertionError('Unexpected execution import: ' + fullname)
sys.meta_path.insert(0, BlockExecution())
from pypaimon import CatalogFactory, Schema
from pypaimon.table.data_evolution_merge_into import merge_into, WhenMatched, WhenNotMatched
from pypaimon.write.merge_types import source_col

catalog = CatalogFactory.create({'warehouse': warehouse})
catalog.create_database('default', True)
schema = pa.schema([('id', pa.int64()), ('value', pa.int32())])
catalog.create_table('default.data', Schema.from_pyarrow_schema(schema, options={
    'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
    'write.native.enabled': str(engine == 'native').lower(), 'commit.native.enabled': 'false',
    'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false',
}), False)
table = catalog.get_table('default.data')
data = pa.table({'id': [1, 2], 'value': [9, 7]}, schema=schema)
matched = [WhenMatched.update({'value': source_col('value')}, condition='s.value > t.value')]
if engine == 'python':
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    try:
        writer.write_arrow(pa.table({'id': [1], 'value': [3]}, schema=schema))
        builder.new_commit().commit(writer.prepare_commit())
    finally:
        writer.close()
    builder = table.new_batch_write_builder()
    messages = merge_into(table, data, on=['id'], when_matched=matched,
                          when_not_matched=[WhenNotMatched('*')], commit_user=builder.commit_user)
    builder.new_commit().commit(messages)
    read = table.new_read_builder()
    assert read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id').to_pylist() == [
        {'id': 1, 'value': 9}, {'id': 2, 'value': 7}]
else:
    core = Mock()
    builder = core.new_batch_write_builder.return_value
    builder._with_commit_user.return_value = builder
    writer = builder.new_update.return_value
    writer.merge_into.return_value = []
    def assignment(batch):
        assert batch.column_names == ['s.id', 's.value', 't.id', 't.value']
        return batch.column('s.value')
    matched = [WhenMatched.update({'value': assignment}, condition='s.value > t.value')]
    with patch('pypaimon.execution.native.write.native_merge_into._native_row_id_table', return_value=core), \
            patch('pypaimon.execution.native.write.native_merge_into.from_native_commit_messages',
                  side_effect=lambda table, messages: messages):
        assert merge_into(table, data, on=['id'], when_matched=matched, commit_user='test') == []
    writer.merge_into.assert_called_once()
    callback = writer.merge_into.call_args[1]['when_matched'][0]['assignments'][0][2]
    assert callback(pa.table({'t.id': [1], 't.value': [3], 's.value': [9]})).to_pylist() == [9]
    assert writer.merge_into.call_args[1]['when_matched'][0]['condition'] == {
        'sql': '\"s.value\" > \"t.value\"'}
assert not any(name.startswith('pypaimon.ray') or name == blocked
               or name.startswith(blocked + '.') for name in sys.modules)
"""
    result = subprocess.run(
        [sys.executable, '-c', source, engine, str(tmp_path / 'warehouse')],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=45)
    assert result.returncode == 0, result.stdout + result.stderr
