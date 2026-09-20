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

import json
from collections import Counter
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.deletionvectors.bitmap_deletion_vector import BitmapDeletionVector
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.table_delete import TableDeleteByRowId


pytestmark = [pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='Rust planner required')]


@pytest.fixture(params=['append', 'append-dv', 'de', 'de-dv'])
def chunk_table(request, tmp_path):
    de = request.param.startswith('de')
    dv = request.param.endswith('dv')
    fields = [('id', pa.int64()), ('p', pa.string())]
    options = {'file.format': 'parquet', 'source.split.target-size': '1b',
               'source.split.open-file-cost': '1b'}
    if de:
        fields.append(('payload', pa.large_binary()))
        options.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
                        'blob.target-file-size': '1b'})
    if dv:
        options['deletion-vectors.enabled'] = 'true'
    schema = pa.schema(fields)
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, partition_keys=['p'], options=options), False)
    table = catalog.get_table('default.t')
    expected = []
    for start in (0, 6):
        rows = [{'id': i, 'p': (None, 'a', 'b')[i % 3]} for i in range(start, start + 6)]
        if de:
            for row in rows:
                row['payload'] = ('payload-%d' % row['id']).encode()
        expected.extend(rows)
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()
    for snapshot_id in (1, 2):
        path = table.snapshot_manager().get_snapshot_path(snapshot_id)
        snapshot = json.loads(table.file_io.read_file_utf8(path))
        snapshot['timeMillis'] = snapshot_id * 100
        table.file_io.write_file(path, json.dumps(snapshot), overwrite=True)
    before_deletes = list(expected)
    if dv:
        builder = table.copy({'scan.native-plan.enabled': 'false'}).new_read_builder()
        plan = builder.new_scan().plan()
        index_adds = []
        for split in plan.splits():
            # Delete the first physical row in every main file. Sidecar files
            # inherit the anchor's DV through their aligned row-id group.
            for file in split.files:
                if not file.file_name.endswith('.parquet'):
                    continue
                from pypaimon.read.split import DataSplit
                one = DataSplit(files=[file], partition=split.partition, bucket=split.bucket,
                                raw_convertible=not de)
                row = builder.with_projection(['id']).new_read().to_arrow([one]).to_pylist()[0]
                expected = [candidate for candidate in expected if candidate['id'] != row['id']]
                vector = BitmapDeletionVector()
                vector.delete(0)
                index_adds.append((tuple(split.partition.values), split.bucket,
                                   TableDeleteByRowId(table)._write_deletion_vector_index(
                                       split.partition, split.bucket, {file.file_name: vector})))
        commit = table.new_batch_write_builder().new_commit()
        try:
            commit.commit([CommitMessage(partition=partition, bucket=bucket, new_files=[],
                                         index_adds=[entry]) for partition, bucket, entry in index_adds])
        finally:
            commit.close()
    return table, expected, before_deletes


def _chunks(table, seed, chunk_size=3, shard=None, predicate=None, projection=None):
    results = []
    for native in (False, True):
        builder = table.copy({'scan.native-plan.enabled': str(native).lower()}).new_read_builder()
        if predicate is not None:
            builder.with_filter(predicate)
        if projection is not None:
            builder.with_projection(projection)
        scan = builder.new_scan().with_chunk_shuffle(seed, chunk_size)
        if shard is not None:
            scan.with_shard(*shard)
        if native:
            with patch.object(scan.file_scanner, 'scan', side_effect=AssertionError('native fallback')), \
                    patch.object(scan.file_scanner, 'plan_files', side_effect=AssertionError('Python manifest scan')):
                plan = scan.plan()
        else:
            plan = scan.plan()
        assert all(
            split.snapshot_id == plan.snapshot_id for split in plan.splits())
        chunks = []
        for split in plan.splits():
            if table.options.options.contains_key('incremental-between-timestamp'):
                assert split.is_streaming
            rows = builder.new_read().to_arrow([split]).to_pylist()
            assert 0 < len(rows) <= chunk_size
            assert split.merged_row_count() == len(rows)
            if rows and 'p' in rows[0]:
                assert len({row['p'] for row in rows}) == 1
            chunks.append(sorted(rows, key=lambda row: row['id']))
        results.append((plan.snapshot_id, chunks))
    assert results[0] == results[1]
    return results[1]


@pytest.mark.parametrize('seed', [-11, 42, 2 ** 70])
def test_native_chunks_preserve_order_live_counts_and_worker_assignment(chunk_table, seed):
    table, expected, _ = chunk_table
    snapshot_id, chunks = _chunks(table, seed)
    actual = [row for chunk in chunks for row in chunk]
    assert sorted(actual, key=lambda row: row['id']) == expected
    workers = []
    for worker in range(5):
        sid, assigned = _chunks(table, seed, shard=(worker, 5))
        assert sid == snapshot_id
        workers.extend(assigned)
    assert workers == chunks
    assert Counter(row['id'] for chunk in workers for row in chunk) == Counter(row['id'] for row in expected)


def test_native_chunks_keep_partition_filter_projection_and_time_travel(chunk_table):
    table, expected, before_deletes = chunk_table
    pb = table.new_read_builder().new_predicate_builder()
    for predicate, partition in ((pb.equal('p', 'a'), 'a'), (pb.is_null('p'), None)):
        _, chunks = _chunks(table, 7, predicate=predicate, projection=['id'])
        assert sorted(row['id'] for chunk in chunks for row in chunk) == [
            row['id'] for row in expected if row['p'] == partition]
    sid, chunks = _chunks(table.copy({'scan.snapshot-id': '2'}), 7)
    assert sid == 2
    assert sorted([row for chunk in chunks for row in chunk], key=lambda row: row['id']) == before_deletes
    sid, chunks = _chunks(table, 7, predicate=pb.equal('p', 'missing'))
    assert sid == table.snapshot_manager().get_latest_snapshot().id
    assert chunks == []


def test_native_incremental_chunks_keep_events_and_ignore_later_deletions(chunk_table):
    table, _, before_deletes = chunk_table
    incremental = table.copy({'incremental-between-timestamp': '0,200'})
    sid, chunks = _chunks(incremental, 42)
    assert sid == 2
    assert sorted([row for chunk in chunks for row in chunk], key=lambda row: row['id']) == before_deletes
