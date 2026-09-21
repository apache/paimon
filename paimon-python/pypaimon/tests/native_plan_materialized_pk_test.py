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

from contextlib import ExitStack
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.deletionvectors.bitmap_deletion_vector import BitmapDeletionVector
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.table_delete import TableDeleteByRowId


pytestmark = [pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='Rust planner required')]


@pytest.mark.parametrize('engine', ['deduplicate', 'first-row'])
@pytest.mark.parametrize('merge_on_read', ['false', 'true'])
@pytest.mark.parametrize('target_size', ['1b', '1mb'])
def test_clustered_materialized_dv_files_use_native_raw_splits(tmp_path, engine, merge_on_read, target_size):
    schema = pa.schema([('id', pa.int64()), ('value', pa.string())])
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'], options={
            'bucket': '1', 'merge-engine': engine, 'file.format': 'parquet',
            'deletion-vectors.enabled': 'true', 'deletion-vectors.merge-on-read': merge_on_read,
            'pk-clustering-override': 'true', 'clustering.columns': 'value',
            'source.split.target-size': target_size, 'source.split.open-file-cost': '1b',
        }), False)
    table = catalog.get_table('default.t')
    expected = [{'id': i, 'value': 'v%d' % (10 - i)} for i in range(1, 5)]
    files = []
    for level, keys in ((1, (1, 3)), (2, (2, 4))):
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.Table.from_pylist([expected[key - 1] for key in keys], schema=schema))
            messages = writer.prepare_commit()
            for message in messages:
                rewritten = []
                for file in message.new_files:
                    path = table.path_factory().bucket_path((), 0) + '/' + file.file_name
                    # Simulate Java clustering compaction: physical order is by
                    # value, opposite to PK order, while min/max PK ranges overlap.
                    data = pq.read_table(path).sort_by([('value', 'ascending')])
                    pq.write_table(data, path)
                    rewritten.append(replace(file, level=level,
                                             file_size=Path(path).stat().st_size))
                message.new_files = rewritten
                files.extend(rewritten)
            commit.commit(messages)
        finally:
            writer.close()
            commit.close()

    vector = BitmapDeletionVector()
    vector.delete(0)  # clustered first file starts with id=3
    entry = TableDeleteByRowId(table)._write_deletion_vector_index(
        GenericRow([], []), 0, {files[0].file_name: vector})
    commit = table.new_batch_write_builder().new_commit()
    try:
        commit.commit([CommitMessage(partition=(), bucket=0, new_files=[], index_adds=[entry])])
    finally:
        commit.close()
    expected = [row for row in expected if row['id'] != 3]
    pb = table.new_read_builder().new_predicate_builder()
    for predicate in (None, pb.equal('id', 3), pb.equal('value', 'v8')):
        for native in (False, True):
            builder = table.copy({
                'scan.native-plan.enabled': str(native).lower(),
                'read.native.enabled': str(native).lower(),
            }).new_read_builder()
            if predicate is not None:
                builder.with_filter(predicate)
            scan = builder.new_scan()
            if native:
                with patch.object(scan.file_scanner, 'scan', side_effect=AssertionError('native fallback')):
                    plan = scan.plan()
            else:
                plan = scan.plan()
            assert all(split.raw_convertible for split in plan.splits())
            if native:
                assert all(getattr(split, '_native_split', None) is not None
                           for split in plan.splits())
                read_guard = patch(
                    'pypaimon.read.table_read.TableRead._create_split_read',
                    side_effect=AssertionError(
                        'materialized PK native read fell back'))
            else:
                read_guard = ExitStack()
            with read_guard:
                rows = builder.new_read().to_arrow(plan.splits()).to_pylist()
            wanted = expected if predicate is None else ([] if predicate.field == 'id' else [expected[1]])
            assert sorted(rows, key=lambda row: row['id']) == wanted
            assert plan.snapshot_id == 3
            if predicate is None:
                assert len(plan.splits()) == (2 if target_size == '1b' else 1)

    if engine == 'first-row' and merge_on_read == 'true':
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.Table.from_pylist([{'id': 5, 'value': 'pending'}], schema=schema))
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()
        scan = table.copy({'scan.native-plan.enabled': 'true'}).new_read_builder().new_scan()
        with patch.object(scan.file_scanner, 'scan', wraps=scan.file_scanner.scan) as fallback:
            plan = scan.plan()
        fallback.assert_called_once_with()
        assert any(file.level == 0 for split in plan.splits() for file in split.files)


@pytest.mark.parametrize('target_size', ['1b', '1mb'])
@pytest.mark.parametrize('compacted_partition', [False, True])
@pytest.mark.parametrize('batch_size', [1, 1024])
def test_first_row_level_zero_merges_before_filtering(tmp_path, target_size, compacted_partition, batch_size):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int64()), ('value', pa.string()), ('dt', pa.string())]),
        primary_keys=['id', 'dt'], partition_keys=['dt'], options={
            'bucket': '1', 'merge-engine': 'first-row', 'file.format': 'parquet',
            'deletion-vectors.enabled': 'true', 'deletion-vectors.merge-on-read': 'true',
            'pk-clustering-override': 'true', 'clustering.columns': 'value',
            'source.split.target-size': target_size, 'source.split.open-file-cost': '1b',
            'read.batch-size': str(batch_size),
        }), False)
    table = catalog.get_table('default.t')
    batches = [(0, [{'id': 1, 'value': 'first', 'dt': 'pending'},
                    {'id': 2, 'value': 'deleted', 'dt': 'pending'}]),
               (0, [{'id': 1, 'value': 'later', 'dt': 'pending'},
                    {'id': 3, 'value': 'third', 'dt': 'pending'}])]
    if compacted_partition:
        batches.append((1, [{'id': 4, 'value': 'compacted', 'dt': 'ready'}]))
    first_file = None
    for level, rows in batches:
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.Table.from_pylist(rows))
            messages = writer.prepare_commit()
            for message in messages:
                message.new_files = [replace(file, level=level) for file in message.new_files]
                if first_file is None:
                    first_file = message.new_files[0]
            commit.commit(messages)
        finally:
            writer.close()
            commit.close()
    vector = BitmapDeletionVector()
    vector.delete(1)  # Remove id=2 from the first sorted L0 run.
    entry = TableDeleteByRowId(table)._write_deletion_vector_index(
        GenericRow(['pending'], table.partition_keys_fields), 0, {first_file.file_name: vector})
    commit = table.new_batch_write_builder().new_commit()
    try:
        commit.commit([CommitMessage(partition=('pending',), bucket=0, new_files=[], index_adds=[entry])])
    finally:
        commit.close()
    pb = table.new_read_builder().new_predicate_builder()
    all_ids = [1, 3, 4] if compacted_partition else [1, 3]
    for predicate, expected in [(None, all_ids), (pb.equal('value', 'later'), []),
                                (pb.equal('value', 'first'), [1]), (pb.equal('id', 2), [])]:
        for native in (False, True):
            builder = table.copy({
                'scan.native-plan.enabled': str(native).lower(),
                'read.native.enabled': str(native).lower(),
            }).new_read_builder()
            builder.with_projection(['id'])
            if predicate is not None:
                builder.with_filter(predicate)
            scan = builder.new_scan()
            if native:
                with patch.object(scan.file_scanner, 'scan', side_effect=AssertionError('native fallback')):
                    plan = scan.plan()
            else:
                plan = scan.plan()
            assert plan.snapshot_id == len(batches) + 1
            if native:
                assert all(getattr(split, '_native_split', None) is not None
                           for split in plan.splits())
                read_guard = patch(
                    'pypaimon.read.table_read.TableRead._create_split_read',
                    side_effect=AssertionError(
                        'first-row native read fell back'))
            else:
                read_guard = ExitStack()
            with read_guard:
                actual = builder.new_read().to_arrow(
                    plan.splits()).column('id').to_pylist()
            assert sorted(actual) == expected


@pytest.mark.parametrize('engine,merged_value', [
    ('partial-update', 5), ('aggregation', 15),
])
def test_pk_merge_engines_read_deletion_vectors_natively(tmp_path, engine, merged_value):
    schema = pa.schema([('id', pa.int64()), ('value', pa.int64())])
    options = {
        'bucket': '1', 'merge-engine': engine, 'file.format': 'parquet',
        'deletion-vectors.enabled': 'true', 'deletion-vectors.merge-on-read': 'true',
    }
    if engine == 'aggregation':
        options['fields.value.aggregate-function'] = 'sum'
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'], options=options), False)
    table = catalog.get_table('default.t')
    # Each run has unique keys, so the Python writer's aggregation fallback
    # leaves its physical rows intact; aggregation happens across the runs.
    first_file = None
    for rows, level in (([{'id': 1, 'value': 10}, {'id': 2, 'value': 20}], 1),
                        ([{'id': 1, 'value': 5}, {'id': 3, 'value': 30}], 0)):
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
            messages = writer.prepare_commit()
            for message in messages:
                message.new_files = [replace(file, level=level)
                                     for file in message.new_files]
                if first_file is None:
                    first_file = message.new_files[0]
            commit.commit(messages)
        finally:
            writer.close()
            commit.close()

    first_path = table.path_factory().bucket_path((), 0) + '/' + first_file.file_name
    deleted_position = pq.read_table(first_path).column('id').to_pylist().index(2)
    vector = BitmapDeletionVector()
    vector.delete(deleted_position)
    entry = TableDeleteByRowId(table)._write_deletion_vector_index(
        GenericRow([], []), 0, {first_file.file_name: vector})
    commit = table.new_batch_write_builder().new_commit()
    try:
        commit.commit([CommitMessage(partition=(), bucket=0, new_files=[], index_adds=[entry])])
    finally:
        commit.close()

    expected = [{'id': 1, 'value': merged_value}, {'id': 3, 'value': 30}]
    for predicate_value, projection in ((None, None), (merged_value, ['id'])):
        for native in (False, True):
            candidate = table.copy({
                'scan.native-plan.enabled': str(native).lower(),
                'read.native.enabled': str(native).lower(),
            })
            predicates = candidate.new_read_builder().new_predicate_builder()
            builder = candidate.new_read_builder()
            if projection is not None:
                builder.with_projection(projection)
            if predicate_value is not None:
                builder.with_filter(predicates.equal('value', predicate_value))
            scan = builder.new_scan()
            if native:
                with patch.object(scan.file_scanner, 'scan',
                                  side_effect=AssertionError('native plan fell back')):
                    plan = scan.plan()
                assert all(getattr(split, '_native_split', None) is not None
                           for split in plan.splits())
                guard = patch('pypaimon.read.table_read.TableRead._create_split_read',
                              side_effect=AssertionError('native read fell back'))
            else:
                plan = scan.plan()
                guard = ExitStack()
            with guard:
                actual = builder.new_read().to_arrow(plan.splits()).to_pylist()
            wanted = (expected if predicate_value is None
                      else [{'id': 1}])
            assert sorted(actual, key=lambda row: row['id']) == wanted
            assert any(deletion is not None
                       for split in plan.splits()
                       for deletion in split.data_deletion_files or [])
