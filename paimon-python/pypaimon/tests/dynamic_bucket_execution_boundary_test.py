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

"""Shared distributed bucket assignment does not load Python writer execution."""

from pathlib import Path
import subprocess
import sys

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


def test_shared_hash_assignment_restores_nonempty_snapshot_without_python_execution(tmp_path):
    warehouse = str(tmp_path / 'warehouse')
    catalog = CatalogFactory.create({'warehouse': warehouse})
    catalog.create_database('default', True)
    catalog.create_table('default.records', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int64()), ('value', pa.string()), ('p', pa.string())]),
        primary_keys=['id', 'p'], partition_keys=['p'], options={
            'bucket': '-1', 'dynamic-bucket.target-row-num': '1', 'write.native.enabled': 'true',
        }), False)
    table = catalog.get_table('default.records').copy({
        'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
    })
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table({'id': [1], 'value': ['old'], 'p': ['p']}))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()

    source = r'''
import importlib.abc
import sys
from types import SimpleNamespace
from unittest.mock import patch
import pyarrow as pa

class BlockPythonExecution(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith('pypaimon.execution.python'):
            raise AssertionError('Shared assignment loaded Python execution: ' + fullname)

sys.meta_path.insert(0, BlockPythonExecution())
from pypaimon import CatalogFactory
from pypaimon.index.dynamic_bucket import HashBucketAssigner, resolve_bucket_snapshot
from pypaimon.write.input_routing import DynamicBucketInputRouter

catalog = CatalogFactory.create({'warehouse': sys.argv[1]})
table = catalog.get_table('default.records')
snapshot = resolve_bucket_snapshot(table)
assert snapshot is not None and snapshot.index_manifest is not None
rows = pa.record_batch([pa.array([2, 1], type=pa.int64()), pa.array(['other', 'new']), pa.array(['p', 'p'])],
                       names=['id', 'value', 'p'])
router = DynamicBucketInputRouter(table.table_schema)
partitions, partition_hashes, key_hashes = router.extract_hashes_batch(rows)
# Keep the Rust metadata boundary deterministic. The assignment reads the
# actual nonempty HASH file produced above and restores the original key.
plan = SimpleNamespace(splits=lambda: [SimpleNamespace(bucket=0)])
with patch('pypaimon.execution.native.read.plan.native_plan', return_value=plan) as native_plan:
    assigner = HashBucketAssigner(table, 1, 1, 0, 1, -1, snapshot=snapshot)
    assert assigner.assign_batch(partitions, partition_hashes, key_hashes) == [(1, True), (0, False)]
    assert assigner.assign(partitions[1], partition_hashes[1], key_hashes[1]) == 0
native_plan.assert_called_once()
assert native_plan.call_args[0][0].options.scan_snapshot_id() == snapshot.id
assert native_plan.call_args[1]['all_files'] is True
assert not any(name.startswith('pypaimon.execution.python') for name in sys.modules)
'''
    result = subprocess.run(
        [sys.executable, '-c', source, warehouse],
        cwd=str(Path(__file__).resolve().parents[2]),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
