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

"""Fixed-bucket native writes use shared routing and Java commit semantics."""

import pickle
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.native_write import NativePostponeFixedBucketTableWrite
from pypaimon.write.postpone_bucket import PostponeBucketPlan, PostponeBucketPlanner

pytestmark = pytest.mark.native_plan
_SCHEMA = pa.schema([('id', pa.int32()), ('p', pa.string()), ('v', pa.int32())])


def _table(tmp_path, options=None, catalog=None, schema=_SCHEMA, partition_keys=None):
    catalog = catalog or CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    opts = {'bucket': '-2', 'file.format': 'parquet', 'write.native.enabled': 'true',
            'postpone.default-bucket-num': '3'}
    opts.update(options or {})
    keys = ['p'] if partition_keys is None else partition_keys
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'] + keys, partition_keys=keys, options=opts), False)
    return catalog.get_table('default.t')


def _batch(rows):
    return pa.RecordBatch.from_pylist([dict(zip(_SCHEMA.names, row)) for row in rows], schema=_SCHEMA)


def _rows(table):
    read = table.new_read_builder()
    return sorted(read.new_read().to_arrow(read.new_scan().plan().splits()).to_pylist(),
                  key=lambda row: (row['p'], row['id']))


def _write(table, rows, plan=None, overwrite=False):
    builder = table.new_postpone_fixed_bucket_write_builder()
    if plan is not None:
        builder.with_bucket_plan(plan)
    if overwrite:
        builder.overwrite()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow_batch(_batch(rows))
        messages = writer.prepare_commit()
        commit.commit(messages)
        return messages
    finally:
        writer.close()
        commit.close()


@pytest.mark.parametrize('native_commit', [False, True])
@pytest.mark.parametrize('explicit_plan', [False, True])
def test_fixed_buckets_are_visible_to_both_readers(
        tmp_path, native_rest_catalog, native_commit, explicit_plan):
    table = _table(tmp_path, {'commit.native.enabled': str(native_commit).lower()}, native_rest_catalog)
    plan = PostponeBucketPlan({('a/b',): 3, ('b',): 5}) if explicit_plan else None
    builder = table.new_postpone_fixed_bucket_write_builder()
    if plan is not None:
        builder.with_bucket_plan(plan)
    writer, commit = builder.new_write(), builder.new_commit()
    assert isinstance(writer, NativePostponeFixedBucketTableWrite)
    expected = [dict(id=i, p=p, v=i * 10) for p in ['a/b', 'b'] for i in range(12)]
    try:
        writer.write_arrow(pa.Table.from_pylist(expected, schema=_SCHEMA))
        writer.write_row(GenericRow([11, 'b', 999], table.fields))
        messages = writer.prepare_commit()
        assert writer._python_writer is None
        assert messages and all(m.bucket >= 0 for m in messages)
        for message in messages:
            count = 5 if explicit_plan and message.partition == ('b',) else 3
            assert message.total_buckets == count
            assert 0 <= message.bucket < count
            assert message.check_from_snapshot == 0
        writer.close()
        if native_commit:
            with patch.object(commit.file_store_commit, 'commit',
                              side_effect=AssertionError('Python commit fallback')):
                commit.commit(messages)
        else:
            commit.commit(messages)
    finally:
        writer.close()
        commit.close()
    expected[-1]['v'] = 999
    for native in (False, True):
        copied = table.copy({'read.native.enabled': str(native).lower(),
                             'scan.native-plan.enabled': str(native).lower()})
        assert _rows(copied) == expected


@pytest.mark.parametrize('native', [False, True])
def test_default_is_exact_and_overwrite_replaces_old_layout(tmp_path, native):
    table = _table(tmp_path, {'write.native.enabled': str(native).lower(),
                              'postpone.batch-write-fixed-bucket.max-parallelism': '1',
                              'postpone.target-row-num-per-bucket': '0',
                              'postpone.target-size-per-bucket': 'invalid'})
    first = _write(table, [(1, 'a', 10), (2, 'b', 20)])
    assert {message.total_buckets for message in first} == {3}
    changed = table.copy({'postpone.default-bucket-num': '5'})
    appended = _write(changed, [(1, 'a', 11), (3, 'c', 30)])
    assert {m.partition: m.total_buckets for m in appended} == {('a',): 3, ('c',): 5}
    replaced = _write(changed, [(4, 'a', 40)], overwrite=True)
    assert {message.total_buckets for message in replaced} == {5}
    assert _rows(changed) == [dict(id=4, p='a', v=40), dict(id=2, p='b', v=20), dict(id=3, p='c', v=30)]


@pytest.mark.parametrize('engine,expected', [('deduplicate', 30), ('partial-update', 30),
                                             ('aggregation', 60), ('first-row', 10)])
def test_fixed_writer_uses_core_merge_engine(tmp_path, engine, expected):
    options = {'merge-engine': engine}
    if engine == 'aggregation':
        options['fields.v.aggregate-function'] = 'sum'
    table = _table(tmp_path, options)
    builder = table.new_postpone_fixed_bucket_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    assert isinstance(writer, NativePostponeFixedBucketTableWrite)
    try:
        for value in [10, 20, 30]:
            writer.write_arrow_batch(_batch([(1, 'a', value)]))
        messages = writer.prepare_commit()
        assert sum(f.row_count for m in messages for f in m.new_files) == 1
        # Inspect output even for first-row, whose normal read hides level zero.
        import pyarrow.parquet as pq
        values = []
        for message in messages:
            for file in message.new_files:
                with table.file_io.new_input_stream(file.file_path) as source:
                    values.extend(pq.ParquetFile(source).read().column('v').to_pylist())
        assert values == [expected]
        commit.commit(messages)
    finally:
        writer.close()
        commit.close()
    if engine != 'first-row':
        assert _rows(table) == [dict(id=1, p='a', v=expected)]


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('count', ['0', '-1', '2147483648', 'x'])
def test_default_bucket_validation_is_not_silently_ignored(tmp_path, native, count):
    table = _table(tmp_path, {'write.native.enabled': str(native).lower(),
                              'postpone.default-bucket-num': count})
    with pytest.raises((ValueError, TypeError), match='(postpone.default-bucket-num|invalid literal)'):
        table.new_postpone_fixed_bucket_write_builder().new_write()


def test_provided_plan_does_not_fill_missing_partitions_from_default(tmp_path):
    table = _table(tmp_path)
    builder = table.new_postpone_fixed_bucket_write_builder().with_bucket_plan(
        PostponeBucketPlan({('a',): 2}))
    writer = builder.new_write()
    assert isinstance(writer, NativePostponeFixedBucketTableWrite)
    try:
        writer.write_arrow_batch(_batch([(1, 'a', 10)]))
        with pytest.raises(Exception, match='does not contain an input partition'):
            writer.write_arrow_batch(_batch([(2, 'missing', 20)]))
        with pytest.raises(Exception, match='closed or failed'):
            writer.prepare_commit()
        assert table.snapshot_manager().get_latest_snapshot() is None
        assert writer._python_writer is None
    finally:
        writer.abort()


@pytest.mark.parametrize('prepared', [False, True])
def test_abort_and_close_preserve_prepared_files(tmp_path, prepared):
    table = _table(tmp_path)
    builder = table.new_postpone_fixed_bucket_write_builder()
    writer = builder.new_write()
    writer.write_arrow_batch(_batch([(1, 'a', 10)]))
    paths = []
    if prepared:
        messages = writer.prepare_commit()
        paths = [f.file_path for m in messages for f in m.new_files]
        assert paths and all(table.file_io.exists(path) for path in paths)
    writer.abort()
    writer.close()
    if prepared:
        assert all(table.file_io.exists(path) for path in paths)
    else:
        assert not list(tmp_path.rglob('*.parquet'))
    assert table.snapshot_manager().get_latest_snapshot() is None


def test_close_preserves_prepared_and_committed_files(tmp_path):
    table = _table(tmp_path)
    builder = table.new_postpone_fixed_bucket_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow_batch(_batch([(1, 'a', 10)]))
        messages = writer.prepare_commit()
        writer.close()
        paths = [f.file_path for m in messages for f in m.new_files]
        assert paths and all(table.file_io.exists(path) for path in paths)
        commit.commit(messages)
        writer.abort()
        assert all(table.file_io.exists(path) for path in paths)
        assert _rows(table) == [dict(id=1, p='a', v=10)]
    finally:
        writer.close()
        commit.close()


def test_fixed_writer_is_one_shot(tmp_path):
    table = _table(tmp_path)
    writer = table.new_postpone_fixed_bucket_write_builder().new_write()
    try:
        assert writer.prepare_commit() == []
        with pytest.raises(Exception, match='one-time|one prepare_commit'):
            writer.prepare_commit()
        with pytest.raises(Exception, match='one prepare_commit'):
            writer.write_arrow_batch(_batch([(1, 'a', 10)]))
    finally:
        writer.close()


def test_python_fallback_retains_specialized_writer_and_plan(tmp_path):
    from pypaimon.write.postpone_batch_table_write import PostponeFixedBucketBatchTableWrite

    table = _table(tmp_path)
    plan = PostponeBucketPlan({('a',): 7})
    writer = table.new_postpone_fixed_bucket_write_builder().with_bucket_plan(plan).new_write()
    assert isinstance(writer, NativePostponeFixedBucketTableWrite)
    try:
        # Distributed coordinators and advanced methods may select Python before data.
        writer.file_store_write
        assert isinstance(writer._python_writer, PostponeFixedBucketBatchTableWrite)
        writer.write_row(GenericRow([1, 'a', 10], table.fields))
        messages = writer.prepare_commit()
        assert {message.total_buckets for message in messages} == {7}
    finally:
        writer.abort()


def test_cannot_fallback_after_native_data(tmp_path):
    table = _table(tmp_path)
    writer = table.new_postpone_fixed_bucket_write_builder().new_write()
    try:
        writer.write_arrow_batch(_batch([(1, 'a', 10)]))
        with pytest.raises(RuntimeError, match='Cannot switch'):
            writer.file_store_write
        assert writer._python_writer is None
    finally:
        writer.abort()


def test_distributed_worker_uses_pickled_plan_without_python_routing(tmp_path):
    from pypaimon.write.ray_datasink import PaimonDatasink
    from pypaimon.write.row_key_extractor import PostponeFixedBucketRowKeyExtractor

    table = _table(tmp_path)
    plan = pickle.loads(pickle.dumps(PostponeBucketPlan({('a',): 3})))
    sink = PaimonDatasink(table, postpone_bucket_plan=plan)
    with patch.object(PostponeBucketPlanner, '_load_bucket_metadata',
                      side_effect=AssertionError('worker planner scan')), \
            patch.object(PostponeFixedBucketRowKeyExtractor, 'extract',
                         side_effect=AssertionError('Python routing'), create=True):
        messages = sink.write([pa.Table.from_batches([_batch([(i, 'a', i) for i in range(20)])])], Mock())
    assert {m.total_buckets for m in messages} == {3}
    assert {m.bucket for m in messages} == {0, 1, 2}
    assert sum(f.row_count for m in messages for f in m.new_files) == 20


def test_plan_arrow_encoding_preserves_colliding_partition_name(tmp_path):
    schema = pa.schema([('id', pa.int32()), ('total_buckets', pa.string()), ('v', pa.int32())])
    table = _table(tmp_path, schema=schema, partition_keys=['total_buckets'])
    plan = PostponeBucketPlan({('a',): 3})
    encoded = plan.to_arrow(table)
    assert encoded.column(0).to_pylist() == ['a']
    assert encoded.column(1).to_pylist() == [3]
    builder = table.new_postpone_fixed_bucket_write_builder().with_bucket_plan(plan)
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        assert isinstance(writer, NativePostponeFixedBucketTableWrite)
        writer.write_arrow_batch(pa.RecordBatch.from_pylist(
            [dict(id=1, total_buckets='a', v=10)], schema=schema))
        messages = writer.prepare_commit()
        assert [m.partition for m in messages] == [('a',)]
        assert {m.total_buckets for m in messages} == {3}
        commit.commit(messages)
    finally:
        writer.close()
        commit.close()


@pytest.mark.parametrize('partition, count, message', [
    ((), 1, 'arity'), (('a',), 0, 'positive'), (('a',), -1, 'positive'),
    (('a',), True, 'positive'), (('a',), 1.5, 'positive'),
    (('a',), 2147483648, 'positive'),
])
def test_invalid_shared_plan_fails_before_writing(tmp_path, partition, count, message):
    table = _table(tmp_path)
    with pytest.raises(ValueError, match=message):
        PostponeBucketPlan({partition: count}).to_arrow(table)


def test_empty_plan_has_typed_schema(tmp_path):
    table = _table(tmp_path)
    plan = PostponeBucketPlan({}).to_arrow(table)
    assert plan.num_rows == 0
    assert plan.schema.names == ['p', 'total_buckets']
    assert plan.schema.field(0).type == pa.string()
    assert plan.schema.field(1).type == pa.int32()


def test_unpartitioned_default_writer(tmp_path):
    table = _table(tmp_path, partition_keys=[])
    messages = _write(table, [(i, 'a', i) for i in range(20)])
    assert {m.total_buckets for m in messages} == {3}
    assert {m.bucket for m in messages} == {0, 1, 2}
    assert all(m.partition == () for m in messages)
    assert len(_rows(table)) == 20


@pytest.mark.parametrize('native_commit', [False, True])
@pytest.mark.parametrize('combined', [False, True])
def test_concurrent_bucket_owners_cannot_silently_lose_update(
        tmp_path, native_rest_catalog, native_commit, combined):
    table = _table(tmp_path, {'commit.native.enabled': str(native_commit).lower(),
                              'postpone.default-bucket-num': '1'}, native_rest_catalog)
    builders = [table.new_postpone_fixed_bucket_write_builder() for _ in range(2)]
    writers = [builder.new_write() for builder in builders]
    commits = [builder.new_commit() for builder in builders]
    try:
        for i in range(10):
            writers[0].write_arrow_batch(_batch([(1, 'a', i)]))
        writers[1].write_arrow_batch(_batch([(1, 'a', 999)]))
        messages = [writer.prepare_commit() for writer in writers]
        assert {m.check_from_snapshot for group in messages for m in group} == {0}
        if combined:
            with pytest.raises(Exception, match='ownership conflict'):
                commits[0].commit(messages[0] + messages[1])
            assert table.snapshot_manager().get_latest_snapshot() is None
        else:
            commits[0].commit(messages[0])
            with pytest.raises(Exception, match='ownership conflict'):
                commits[1].commit(messages[1])
            assert _rows(table) == [dict(id=1, p='a', v=9)]
            assert table.snapshot_manager().get_latest_snapshot().id == 1
    finally:
        for writer in writers:
            writer.close()
        for commit in commits:
            commit.close()


@pytest.mark.parametrize('native_commit', [False, True])
def test_disjoint_bucket_owners_can_commit_concurrently(tmp_path, native_rest_catalog, native_commit):
    from pypaimon.write.row_key_extractor import PostponeFixedBucketRowKeyExtractor

    table = _table(tmp_path, {'commit.native.enabled': str(native_commit).lower()}, native_rest_catalog)
    extractor = PostponeFixedBucketRowKeyExtractor(table, PostponeBucketPlan({('a',): 3}))
    representatives = {}
    for i in range(50):
        _, bucket = extractor.extract_partition_bucket_row(dict(id=i, p='a', v=i))
        representatives.setdefault(bucket, i)
    assert set(representatives) == {0, 1, 2}
    builders = [table.new_postpone_fixed_bucket_write_builder() for _ in representatives]
    writers = [builder.new_write() for builder in builders]
    commits = [builder.new_commit() for builder in builders]
    try:
        for writer, i in zip(writers, representatives.values()):
            writer.write_arrow_batch(_batch([(i, 'a', i)]))
        messages = [writer.prepare_commit() for writer in writers]
        assert len({m.bucket for group in messages for m in group}) == 3
        for commit, group in zip(commits, messages):
            commit.commit(group)
        assert _rows(table) == [dict(id=i, p='a', v=i) for i in sorted(representatives.values())]
    finally:
        for writer in writers:
            writer.close()
        for commit in commits:
            commit.close()


@pytest.mark.parametrize('native_commit', [False, True])
@pytest.mark.parametrize('order', ['pending-first', 'fixed-first', 'concurrent'])
def test_pending_and_real_buckets_coexist_without_rewriting_history(
        tmp_path, native_rest_catalog, native_commit, order):
    table = _table(tmp_path, {'commit.native.enabled': str(native_commit).lower()}, native_rest_catalog)
    fixed = table.new_postpone_fixed_bucket_write_builder()
    pending = table.new_batch_write_builder()
    fixed_writer, pending_writer = fixed.new_write(), pending.new_write()
    fixed_commit, pending_commit = fixed.new_commit(), pending.new_commit()
    try:
        if order == 'pending-first':
            pending_writer.write_arrow_batch(_batch([(2, 'a', 20)]))
            pending_messages = pending_writer.prepare_commit()
            pending_commit.commit(pending_messages)
        fixed_writer.write_arrow_batch(_batch([(1, 'a', 10)]))
        fixed_messages = fixed_writer.prepare_commit()
        if order == 'fixed-first':
            fixed_commit.commit(fixed_messages)
        if order != 'pending-first':
            pending_writer.write_arrow_batch(_batch([(2, 'a', 20)]))
            pending_messages = pending_writer.prepare_commit()
            pending_commit.commit(pending_messages)
        if order != 'fixed-first':
            fixed_commit.commit(fixed_messages)
        snapshot = table.snapshot_manager().get_latest_snapshot()
        assert snapshot.total_record_count == 2
        for message in pending_messages + fixed_messages:
            for file in message.new_files:
                assert table.file_io.exists(file.file_path)
        # Ordinary reads expose real buckets; committed pending rows await assignment.
        assert _rows(table) == [dict(id=1, p='a', v=10)]
        scanner = table.new_read_builder().new_scan().file_scanner
        manifests, _ = scanner.manifest_scanner()
        entries = scanner.with_all_buckets().read_manifest_entries(manifests)
        assert {entry.bucket for entry in entries} == {-2, fixed_messages[0].bucket}
        assert sum(entry.file.row_count for entry in entries) == 2
    finally:
        fixed_writer.close()
        pending_writer.close()
        fixed_commit.close()
        pending_commit.close()


@pytest.mark.parametrize('native_commit', [False, True])
def test_fixed_overwrite_rejects_concurrent_pending_rows(tmp_path, native_rest_catalog, native_commit):
    table = _table(tmp_path, {'commit.native.enabled': str(native_commit).lower()}, native_rest_catalog)
    _write(table, [(1, 'a', 10)])
    builder = table.new_postpone_fixed_bucket_write_builder().overwrite()
    writer, commit = builder.new_write(), builder.new_commit()
    pending = table.new_batch_write_builder()
    pending_writer, pending_commit = pending.new_write(), pending.new_commit()
    try:
        writer.write_arrow_batch(_batch([(1, 'a', 30)]))
        messages = writer.prepare_commit()
        pending_writer.write_arrow_batch(_batch([(2, 'a', 20)]))
        pending_commit.commit(pending_writer.prepare_commit())
        with pytest.raises(Exception, match='overwrite conflict'):
            commit.commit(messages)
        assert table.snapshot_manager().get_latest_snapshot().total_record_count == 2
        assert _rows(table) == [dict(id=1, p='a', v=10)]
    finally:
        writer.close()
        commit.close()
        pending_writer.close()
        pending_commit.close()


@pytest.mark.parametrize('overwrite,static_partition,expected', [
    (False, None, 3), (True, None, 5), (False, {}, 5), (False, {'p': 'a'}, 5),
])
def test_ray_coordinator_uses_new_default_for_overwrite(
        tmp_path, overwrite, static_partition, expected):
    from pypaimon.write.ray_datasink import write_paimon_dataset

    table = _table(tmp_path)
    _write(table, [(1, 'a', 10)])
    changed = table.copy({'postpone.default-bucket-num': '5'})
    dataset = Mock()
    with patch('pypaimon.write.ray_datasink._collect_partition_stats',
               return_value=(dataset, {('a',): (1, 0)})), \
            patch('pypaimon.write.ray_datasink._write_primary_key_groups') as write:
        write_paimon_dataset(dataset, changed, overwrite=overwrite, static_partition=static_partition)
    assert write.call_args.kwargs['postpone_bucket_plan'].as_dict() == {('a',): expected}


@pytest.mark.parametrize('native', [False, True])
def test_fixed_writer_baseline_survives_message_serialization(tmp_path, native):
    from pypaimon.write.commit_message_serializer import serialize_commit_message, deserialize_commit_message

    table = _table(tmp_path, {'write.native.enabled': str(native).lower()})
    _write(table, [(1, 'a', 10)])
    writer = table.new_postpone_fixed_bucket_write_builder().new_write()
    try:
        writer.write_arrow_batch(_batch([(1, 'a', 20)]))
        messages = writer.prepare_commit()
        for message in messages:
            imported = deserialize_commit_message(
                serialize_commit_message(message, table.partition_keys_fields),
                table.partition_keys_fields, table.trimmed_primary_keys_fields)
            assert imported.check_from_snapshot == 1
            assert imported.total_buckets == 3
            assert imported.bucket == message.bucket
    finally:
        writer.abort()


@pytest.mark.parametrize('bucket,count', [(-1, 3), (3, 3), (0, 0)])
def test_python_commit_rejects_invalid_fixed_bucket_metadata(tmp_path, bucket, count):
    table = _table(tmp_path, {'commit.native.enabled': 'false'})
    builder = table.new_postpone_fixed_bucket_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow_batch(_batch([(1, 'a', 10)]))
        messages = writer.prepare_commit()
        messages[0].bucket = bucket
        messages[0].total_buckets = count
        with pytest.raises((ValueError, RuntimeError), match='Invalid fixed bucket'):
            commit.commit(messages)
        assert table.snapshot_manager().get_latest_snapshot() is None
    finally:
        writer.close()
        commit.close()


@pytest.mark.parametrize('kind', ['int', 'date', 'decimal', 'timestamp', 'unicode'])
def test_shared_plan_partition_encoding_matches_python(tmp_path, kind):
    from datetime import date, datetime
    from decimal import Decimal

    types = {'int': (pa.int32(), [-7, 9]),
             'date': (pa.date32(), [date(1969, 12, 31), date(2026, 10, 3)]),
             'decimal': (pa.decimal128(12, 2), [Decimal('-1.25'), Decimal('9.50')]),
             'timestamp': (pa.timestamp('us'), [datetime(1969, 12, 31, 23, 59, 59),
                                                datetime(2026, 10, 3, 12, 30)]),
             'unicode': (pa.string(), ['a/b=%', '中文'])}
    type_, values = types[kind]
    schema = pa.schema([('id', pa.int32()), ('p', type_), ('v', pa.int32())])
    table = _table(tmp_path, schema=schema)
    plan = PostponeBucketPlan({(value,): i + 2 for i, value in enumerate(values)})
    builder = table.new_postpone_fixed_bucket_write_builder().with_bucket_plan(plan)
    writer, commit = builder.new_write(), builder.new_commit()
    expected = [dict(id=i, p=value, v=i * 10) for i, value in enumerate(values)]
    try:
        assert isinstance(writer, NativePostponeFixedBucketTableWrite)
        writer.write_arrow(pa.Table.from_pylist(expected, schema=schema))
        messages = writer.prepare_commit()
        assert {m.partition: m.total_buckets for m in messages} == plan.as_dict()
        commit.commit(messages)
        for native in (False, True):
            copied = table.copy({'read.native.enabled': str(native).lower(),
                                 'scan.native-plan.enabled': str(native).lower()})
            assert _rows(copied) == expected
    finally:
        writer.close()
        commit.close()


@pytest.mark.parametrize('native', [False, True])
def test_reused_python_committer_drops_prior_fixed_bucket_baseline(tmp_path, native):
    table = _table(tmp_path, {'write.native.enabled': str(native).lower()})
    first = table.new_postpone_fixed_bucket_write_builder()
    writer = first.new_write()
    try:
        writer.write_arrow_batch(_batch([(1, 'a', 10)]))
        messages = writer.prepare_commit()
        # Exercise FileStoreCommit reuse directly: the public batch facade is one-shot.
        commit = first.new_commit().file_store_commit
        commit.commit(messages, 1)
        pending = table.new_batch_write_builder().new_write()
        try:
            pending.write_arrow_batch(_batch([(2, 'a', 20)]))
            commit.commit(pending.prepare_commit(), 2)
        finally:
            pending.close()
        assert commit.conflict_detection.fixed_bucket_commit_check is None
        assert table.snapshot_manager().get_latest_snapshot().total_record_count == 2
        assert _rows(table) == [dict(id=1, p='a', v=10)]
    finally:
        writer.close()
