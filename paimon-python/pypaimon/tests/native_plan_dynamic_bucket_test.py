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

"""Real bucket growth and cross-partition updates through native planning."""

import json
from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.read.native_plan import native_method_available


pytestmark = [pytest.mark.native_plan, pytest.mark.skipif(
    not native_method_available('Table', 'new_write_builder'),
    reason='Rust main writer and planner required')]


@pytest.mark.parametrize('cross_partition', [False, True], ids=['dynamic', 'cross-partition'])
def test_native_pk_bucket_growth_and_partition_migration(tmp_path, cross_partition):
    from pypaimon_rust.datafusion import PaimonCatalog

    schema = pa.schema([('id', pa.int64()), ('p', pa.string()), ('v', pa.string())])
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'] if cross_partition else ['id', 'p'],
        partition_keys=['p'], options={
            'bucket': '-1', 'dynamic-bucket.target-row-num': '2',
            'source.split.target-size': '1 b', 'source.split.open-file-cost': '1 b',
        }), False)

    def read(table, native, predicate=None, shard=None, limit=None):
        builder = table.copy({
            'scan.native-plan.enabled': str(native).lower(),
            'read.native.enabled': str(native).lower(),
        }).new_read_builder()
        if predicate is not None:
            builder.with_filter(predicate)
        if limit is not None:
            builder.with_limit(limit)
        scan = builder.new_scan()
        if shard is not None:
            scan.with_shard(*shard)
        if native:
            with patch.object(scan.file_scanner, 'scan', side_effect=AssertionError('native fallback')):
                plan = scan.plan()
        else:
            plan = scan.plan()
        if native:
            assert all(getattr(split, '_native_split', None) is not None
                       for split in plan.splits())
            read_guard = patch(
                'pypaimon.read.table_read.TableRead._create_split_read',
                side_effect=AssertionError('dynamic-bucket native read fell back'))
        else:
            read_guard = ExitStack()
        with read_guard:
            rows = builder.new_read().to_arrow(plan.splits()).to_pylist()
        return plan, sorted(rows, key=lambda row: (row['id'], row['p']))

    initial = [{'id': 1, 'p': 'a', 'v': 'old'}, {'id': 2, 'p': 'a', 'v': 'two'},
               {'id': 3, 'p': 'b', 'v': 'three'}]
    updates = [{'id': 1, 'p': 'b', 'v': 'moved'}, {'id': 2, 'p': 'a', 'v': 'updated'},
               {'id': 4, 'p': 'a', 'v': 'four'}]
    for snapshot_id, rows in enumerate((initial, updates), 1):
        # The Rust writer maintains the dynamic/cross-partition hash index and
        # emits the old-partition DELETE when a key moves.
        table = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('default.t')
        builder = table.new_write_builder()
        writer = builder.new_write()
        writer.write_arrow(pa.RecordBatch.from_pylist(rows, schema=schema))
        builder.new_commit().commit(writer.prepare_commit())
        table = catalog.get_table('default.t')
        # Give incremental windows deterministic boundaries.
        path = table.snapshot_manager().get_snapshot_path(snapshot_id)
        snapshot = json.loads(table.file_io.read_file_utf8(path))
        snapshot['timeMillis'] = snapshot_id * 100
        table.file_io.write_file(path, json.dumps(snapshot), overwrite=True)
        pb = table.new_read_builder().new_predicate_builder()
        for predicate in (None, pb.equal('id', 1), pb.equal('p', 'a'), pb.equal('v', 'updated')):
            for shard in (None, (0, 2), (1, 2)):
                python_plan, python_rows = read(table, False, predicate, shard)
                native_plan, native_rows = read(table, True, predicate, shard)
                assert native_rows == python_rows
                assert native_plan.snapshot_id == python_plan.snapshot_id == snapshot_id
                for native in (False, True):
                    _, limited = read(table, native, predicate, shard, limit=1)
                    assert len(limited) <= 1
                    assert all(row in python_rows for row in limited)

    expected = [updates[0], updates[1], initial[2], updates[2]]
    if not cross_partition:
        expected.insert(0, initial[0])
    for native in (False, True):
        plan, actual = read(table, native)
        assert actual == expected
        if not cross_partition:
            assert 1 in {split.bucket for split in plan.splits()}
        # A time-travel scan must not use the latest migration/deletion state.
        old_plan, old_rows = read(table.copy({'scan.snapshot-id': '1'}), native)
        assert old_plan.snapshot_id == 1
        assert old_rows == initial

    # Java DeleteExistingProcessor emits DELETE in the old partition with
    # the incoming non-partition values. Batch merging hides this distinction;
    # incremental readers must preserve both the partition and the row kind.
    events = [(row['id'], row['p'], row['v'], 0) for row in initial + updates]
    if cross_partition:
        events.append((1, 'a', 'moved', 3))
    for native in (False, True):
        for partition in (None, 'a', 'b'):
            builder = table.copy({
                'scan.native-plan.enabled': str(native).lower(),
                'read.native.enabled': str(native).lower(),
                'incremental-between-timestamp': '0,200',
            }).new_read_builder()
            if partition is not None:
                builder.with_filter(pb.equal('p', partition))
            scan = builder.new_scan()
            if native:
                with patch.object(scan.file_scanner, 'scan', side_effect=AssertionError('native fallback')):
                    plan = scan.plan()
            else:
                plan = scan.plan()
            assert all(split.is_streaming for split in plan.splits())
            if native:
                from pypaimon.table.row.row_kind import RowKind
                assert all(getattr(split, '_native_split', None) is not None
                           for split in plan.splits())
                read = builder.new_read()
                read.include_row_kind = True
                with patch(
                        'pypaimon.read.table_read.TableRead._create_split_read',
                        side_effect=AssertionError(
                            'dynamic incremental native read fell back')):
                    rows = read.to_arrow(plan.splits()).to_pylist()
                actual = [
                    (row['id'], row['p'], row['v'],
                     RowKind.from_string(row['_row_kind']).value)
                    for row in rows
                ]
            else:
                actual = [
                    (row.get_field(0), row.get_field(1), row.get_field(2),
                     row.get_row_kind().value)
                    for row in builder.new_read().to_iterator(plan.splits())
                ]
            assert sorted(actual) == sorted(
                event for event in events if partition is None or event[1] == partition)
