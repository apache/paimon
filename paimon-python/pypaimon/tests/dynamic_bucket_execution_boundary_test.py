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

"""Shared bucket configuration and Native routing stay outside Python execution."""

from pathlib import Path
import subprocess
import sys

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


def test_native_bucket_execution_does_not_load_python_routing_or_hash_restoration(tmp_path):
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
from unittest.mock import Mock, patch
import pyarrow as pa

class BlockPythonExecution(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith('pypaimon.execution.python'):
            raise AssertionError('Native assignment loaded Python execution: ' + fullname)

sys.meta_path.insert(0, BlockPythonExecution())
from pypaimon import CatalogFactory
from pypaimon.index import dynamic_bucket as shared
from pypaimon.write.backend_dispatch import (
    create_dynamic_bucket_assigner, create_dynamic_bucket_assigner_router,
)

# Only configuration, limits and validation remain in the shared module.
for name in ['DynamicBucketAssigner', '_PartitionIndex', '_iter_hashes', 'compute_assigner']:
    assert not hasattr(shared, name), name
shared.validate_max_buckets(shared.MAX_DYNAMIC_BUCKETS)
shared.validate_bucket_id(shared.SHORT_MAX_VALUE)
assert shared.to_signed_int32(0xffffffff) == -1

catalog = CatalogFactory.create({'warehouse': sys.argv[1]})
table = catalog.get_table('default.records')
snapshot = shared.resolve_bucket_snapshot(table)
assert snapshot is not None and snapshot.index_manifest is not None
assert shared.resolve_bucket_snapshot(table, 0) is None
read_builder = shared.bucket_read_builder(table, ('p',), snapshot)
assert read_builder.table.options.scan_snapshot_id() == snapshot.id

# Native adapters pass the captured baseline and Arrow inputs to the SDK;
# neither constructor nor execution may import the Python HASH implementation.
rows = pa.record_batch([pa.array([2, 1], type=pa.int64()), pa.array(['other', 'new']), pa.array(['p', 'p'])],
                       names=['id', 'value', 'p'])
hashes = pa.array([42, -3], type=pa.int32())
routing = pa.record_batch([pa.array([0, 0], type=pa.int32()), hashes], names=['assigner', 'key_hash'])
buckets = pa.array([1, 0], type=pa.int32())
sdk = SimpleNamespace(DynamicBucketAssignerRouter=Mock(), DynamicBucketAssigner=Mock())
sdk.DynamicBucketAssignerRouter.return_value.route.return_value = routing
sdk.DynamicBucketAssigner.return_value.restore_snapshot_id = snapshot.id
sdk.DynamicBucketAssigner.return_value.assign.return_value = buckets
native_table = Mock()
with patch.dict(sys.modules, {'pypaimon_rust.datafusion': sdk}), \
        patch('pypaimon.execution.native.write.input_routing.native_runtime_available', return_value=True), \
        patch('pypaimon.execution.native.write.input_routing.create_native_write_table', return_value=native_table):
    router = create_dynamic_bucket_assigner_router(table, 1, 1)
    assert router.route(rows).equals(routing)
    assigner = create_dynamic_bucket_assigner(table, 1, 1, 0, base_snapshot_id=snapshot.id)
    assert assigner.restore_snapshot_id == snapshot.id
    assert assigner.assign(rows, hashes).equals(buckets)
sdk.DynamicBucketAssigner.assert_called_once_with(
    native_table, 1, 1, 0, restore_snapshot_id=snapshot.id, overwrite=False)
sdk.DynamicBucketAssigner.return_value.assign.assert_called_once_with(rows, key_hashes=hashes)
native_table.new_batch_write_builder.assert_not_called()
assert not any(name.startswith('pypaimon.execution.python') for name in sys.modules)
'''
    result = subprocess.run(
        [sys.executable, '-c', source, warehouse],
        cwd=str(Path(__file__).resolve().parents[2]),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
