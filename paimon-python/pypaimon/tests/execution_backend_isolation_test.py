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

"""Whole public Python workflows remain independent of Native execution."""

from pathlib import Path
import subprocess
import sys
import textwrap


def test_python_read_update_and_index_roundtrip_never_loads_native(tmp_path):
    source = r"""
import importlib.abc
import sys
import pyarrow as pa

class BlockNative(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith(('pypaimon.execution.native', 'pypaimon_rust')):
            raise AssertionError('Python workflow loaded Native execution: ' + fullname)

sys.meta_path.insert(0, BlockNative())
from pypaimon import CatalogFactory, Schema

catalog = CatalogFactory.create({'warehouse': sys.argv[1]})
catalog.create_database('default', True)
catalog.create_table('default.records', Schema.from_pyarrow_schema(
    pa.schema([('id', pa.int64()), ('value', pa.string())]), options={
        'bucket': '-1', 'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false',
        'write.native.enabled': 'false', 'commit.native.enabled': 'false',
    }), False)
table = catalog.get_table('default.records')
builder = table.new_batch_write_builder()
writer, commit = builder.new_write(), builder.new_commit()
try:
    writer.write_arrow(pa.table({'id': [1, 2], 'value': ['old', 'kept']}))
    commit.commit(writer.prepare_commit())
finally:
    writer.close()
    commit.close()

builder = table.new_batch_write_builder()
commit = builder.new_commit()
try:
    messages = builder.new_update().update_by_arrow_with_row_id(
        pa.table({'_ROW_ID': [0], 'value': ['changed']}))
    commit.commit(messages)
finally:
    commit.close()

read = table.new_read_builder()
rows = read.new_read().to_arrow(read.new_scan().plan().splits()).to_pylist()
assert sorted(rows, key=lambda row: row['id']) == [
    {'id': 1, 'value': 'changed'}, {'id': 2, 'value': 'kept'}]
assert table.create_global_index('value', 'btree') > 0
filtered = table.new_read_builder()
filtered.with_filter(filtered.new_predicate_builder().equal('value', 'changed'))
assert filtered.new_read().to_arrow(filtered.new_scan().plan().splits()).to_pylist() == [
    {'id': 1, 'value': 'changed'}]
assert table.drop_global_index('value', 'btree', dry_run=True) > 0
assert table.drop_global_index('value', 'btree') > 0
assert not any(name.startswith(('pypaimon.execution.native', 'pypaimon_rust')) for name in sys.modules)
"""
    result = subprocess.run(
        [sys.executable, '-c', textwrap.dedent(source), str(tmp_path / 'warehouse')],
        cwd=str(Path(__file__).resolve().parents[2]),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=45)
    assert result.returncode == 0, result.stdout + result.stderr
