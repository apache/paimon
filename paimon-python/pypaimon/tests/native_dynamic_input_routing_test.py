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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Daft distributed input execution uses the real SDK for routing and restoration."""

from contextlib import ExitStack
import pickle
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.execution.native.write.input_routing import NativeDynamicBucketAssigner, NativeDynamicBucketAssignerRouter
from pypaimon.index.dynamic_bucket import DynamicBucketAssigner
from pypaimon.tests.rest.rest_server import RESTCatalogServer
from pypaimon.write.backend_dispatch import create_dynamic_bucket_assigner, create_dynamic_bucket_assigner_router
from pypaimon.write.input_routing import DynamicBucketInputRouter

pytestmark = pytest.mark.native_plan


def _table(catalog, bucket='-1', partitioned=True):
    fields = [('id', pa.int64()), ('value', pa.string())]
    if partitioned:
        fields.append(('p', pa.string()))
    catalog.create_table('default.dynamic_input', Schema.from_pyarrow_schema(
        pa.schema(fields), primary_keys=['id', 'p'] if partitioned else ['id'],
        partition_keys=['p'] if partitioned else [], options={
            'bucket': bucket, 'file.format': 'parquet', 'dynamic-bucket.target-row-num': '1',
            'write.native.enabled': 'true', 'commit.native.enabled': 'true',
            'read.native.enabled': 'true', 'scan.native-plan.enabled': 'true',
            'query-auth.enabled': 'true',
        }), False)
    return catalog.get_table('default.dynamic_input')


def _commit(table, rows):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table(rows))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _forbid_python_input_execution():
    stack = ExitStack()
    for target in [
        'pypaimon.write.input_routing.FixedBucketInputRouter.__init__',
        'pypaimon.write.input_routing.DynamicBucketInputRouter.__init__',
        'pypaimon.index.dynamic_bucket.DynamicBucketAssigner.__init__',
        'pypaimon.execution.python.write.table_write.PythonTableWrite.__init__',
    ]:
        stack.enter_context(patch(target, side_effect=AssertionError('Python input execution: ' + target)))
    return stack


def test_native_dynamic_arrow_route_and_assignment_keep_one_restore_snapshot(native_rest_catalog):
    table = _table(native_rest_catalog)
    _commit(table, {'id': [1], 'value': ['old'], 'p': ['west']})
    snapshot_id = table.snapshot_manager().get_latest_snapshot().id
    batch = pa.record_batch([['west', 'west', 'east'], [2, 1, 2]], names=['p', 'id'])
    reference = DynamicBucketInputRouter(table.table_schema)
    _, partition_hashes, hashes = reference.extract_hashes_batch(batch)
    reference_assigner = DynamicBucketAssigner(
        table, 1, 1, 0, 1, -1, snapshot=table.snapshot_manager().get_snapshot_by_id(snapshot_id))
    expected = [bucket for bucket, _ in reference_assigner.assign_batch(
        reference._extract_partitions_batch(batch), partition_hashes, hashes)]
    with _forbid_python_input_execution(), \
            patch.object(RESTCatalogServer, '_table_query_auth_handle',
                         side_effect=AssertionError('Bucket restoration must not request SELECT')) as query:
        router = create_dynamic_bucket_assigner_router(table, 1, 1)
        assert isinstance(router, NativeDynamicBucketAssignerRouter)
        routed = pickle.loads(pickle.dumps(router)).route(pa.Table.from_batches([batch.slice(0, 1), batch.slice(1)]))
        assert routed.schema.names == ['assigner', 'key_hash']
        assert routed.column(0).to_pylist() == [0] * batch.num_rows
        assert routed.column(1).to_pylist() == [value - (1 << 32) if value >= 1 << 31 else value for value in hashes]
        assigner = create_dynamic_bucket_assigner(table, 1, 1, 0, base_snapshot_id=snapshot_id)
        assert isinstance(assigner, NativeDynamicBucketAssigner)
        assert assigner.restore_snapshot_id == snapshot_id
        assert assigner.assign(batch, routed.column(1)).to_pylist() == expected
        assert assigner.assign(batch).to_pylist() == expected
        # Overwrite ignores existing mappings and exposes an empty restore point.
        overwritten = create_dynamic_bucket_assigner(table, 1, 1, 0, True, snapshot_id)
        assert overwritten.restore_snapshot_id == 0
        assert overwritten.assign(batch, routed.column(1)).to_pylist() == [0, 1, 0]
        query.assert_not_called()
    assert table.snapshot_manager().get_latest_snapshot().id == snapshot_id


@pytest.mark.parametrize('partitioned', [False, True])
def test_native_dynamic_precomputed_hashes_need_only_partition_input(native_rest_catalog, partitioned):
    table = _table(native_rest_catalog, partitioned=partitioned)
    source = {'id': [1, 2, 1]}
    if partitioned:
        source['p'] = ['west'] * 3
    source = pa.table(source)
    with _forbid_python_input_execution():
        routed = create_dynamic_bucket_assigner_router(table, 1, 1).route(source)
        assigner = create_dynamic_bucket_assigner(table, 1, 1, 0, base_snapshot_id=0)
        partitions = source.select(['p'] if partitioned else [])
        assert partitions.num_rows == 3
        hashes = pa.chunked_array([routed.column(1).slice(0, 1), routed.column(1).slice(1)])
        assert assigner.assign(partitions, hashes).to_pylist() == [0, 1, 0]
        assert assigner.restore_snapshot_id == 0
    assert table.snapshot_manager().get_latest_snapshot() is None


@pytest.mark.parametrize('bucket', ['4', '-1'])
@pytest.mark.parametrize('partitioned', [False, True])
def test_native_daft_fixed_and_dynamic_grouped_writes_skip_python_input_execution(
        native_rest_catalog, bucket, partitioned):
    daft = pytest.importorskip('daft')
    from pypaimon.daft.daft_paimon import _write_table
    from pypaimon.write.write_dispatch import NativeTableWrite

    table = _table(native_rest_catalog, bucket, partitioned)
    first = {'id': [1, 2], 'value': ['old', 'keep']}
    second = {'id': [1, 3], 'value': ['new', 'added']}
    overwritten = {'id': [1, 4], 'value': ['replaced', 'last']}
    if partitioned:
        first['p'], second['p'], overwritten['p'] = ['a', 'b'], ['a', 'c'], ['a', 'd']
    with _forbid_python_input_execution(), \
            patch.object(RESTCatalogServer, '_table_query_auth_handle',
                         side_effect=AssertionError('Distributed writes must not request SELECT')) as query, \
            patch.object(NativeTableWrite, '_switch_to_python', side_effect=AssertionError('Writer replay')):
        for rows, mode in [(first, 'append'), (second, 'append'), (overwritten, 'overwrite')]:
            assert _write_table(daft.from_pydict(rows), table, mode).to_pydict()['rows']
        query.assert_not_called()
    builder = table.new_read_builder()
    result = builder.new_read().to_arrow(builder.new_scan().plan().splits()).sort_by([('id', 'ascending')])
    expected = overwritten if not partitioned else {
        'id': [1, 2, 3, 4], 'value': ['replaced', 'keep', 'added', 'last'], 'p': ['a', 'b', 'c', 'd']}
    assert result.to_pydict() == expected
