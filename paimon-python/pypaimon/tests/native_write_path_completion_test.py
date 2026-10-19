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

"""Native write/commit routing and failure ownership contracts."""

import json
from types import ModuleType
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.write.native_commit import _rest_catalog_supported
from pypaimon.write.native_write import NativeTableWrite

pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
native = pytest.mark.skipif(not native_runtime_available(), reason='pypaimon-rust not installed')
_SCHEMA = pa.schema([('id', pa.int64()), ('pt', pa.string())])


def _table(tmp_path, catalog=None, options=None):
    catalog = catalog or CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        _SCHEMA, partition_keys=['pt'], options=dict({
            'commit.native.enabled': 'true', 'write.native.enabled': 'false'
        }, **(options or {}))), False)
    return catalog.get_table('default.t')


def _write(table, rows):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=_SCHEMA))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _rows(table):
    builder = table.new_read_builder()
    return sorted(builder.new_read().to_arrow(builder.new_scan().plan().splits()).to_pylist(),
                  key=lambda row: row['id'])


@pytest.mark.parametrize('operation,args', [
    ('truncate_table', ()), ('truncate_partitions', ([{'pt': 'a'}],)),
])
def test_native_truncate_publication_error_never_retries_or_aborts(tmp_path, operation, args):
    table = _table(tmp_path)
    commit = table.new_batch_write_builder().new_commit()
    native_commit = Mock()
    cause = RuntimeError('snapshot may have published before response failed')
    getattr(native_commit, operation).side_effect = cause
    try:
        with patch.object(commit, '_prepare_native_commit', return_value=(native_commit, [])), \
                patch.object(commit.file_store_commit, 'truncate_table') as truncate, \
                patch.object(commit.file_store_commit, 'drop_partitions') as drop, \
                patch.object(commit.file_store_commit, 'abort') as abort:
            with pytest.raises(RuntimeError) as error:
                getattr(commit, operation)(*args)
        assert error.value is cause
        truncate.assert_not_called()
        drop.assert_not_called()
        abort.assert_not_called()
        native_commit.abort.assert_not_called()
    finally:
        commit.close()


def test_abort_then_truncate_partitions_then_commit_keeps_batch_lifecycle(tmp_path):
    table = _table(tmp_path)
    commit = table.new_batch_write_builder().new_commit()
    stream, batch = Mock(), Mock()
    try:
        with patch('pypaimon.write.native_commit.create_native_commit', side_effect=[stream, batch]) as create, \
                patch('pypaimon.write.native_commit.to_native_commit_messages', return_value=[]), \
                patch('pypaimon.write.native_commit.native_messages_supported', return_value=True), \
                patch.object(commit.file_store_commit, 'commit') as fallback:
            commit.abort([])
            commit.truncate_partitions([{'pt': 'a'}])
            # Java truncatePartitions does not consume the one-shot batch commit.
            commit.table = table.copy({'snapshot.ignore-empty-commit': 'false'})
            commit.commit([])
        stream.abort.assert_called_once_with([])
        stream.close.assert_called_once_with()
        batch.truncate_partitions.assert_called_once_with([{'pt': 'a'}])
        batch.commit.assert_called_once_with([])
        assert create.call_args_list[1][1] == {'batch': True}
        fallback.assert_not_called()
        with pytest.raises(RuntimeError, match='one-time'):
            commit.commit([])
    finally:
        commit.close()


def _bind_fuse(table, root, cached=False):
    from pypaimon.catalog.rest.fuse_support import FusePathResolver
    from pypaimon.filesystem.caching_file_io import CachingFileIO
    from pypaimon.filesystem.local_file_io import FuseLocalFileIO, LocalFileIO

    options = table.catalog_environment.catalog_loader.context().options
    options.data.update({'fuse.enabled': 'true', 'fuse.root': root.as_uri(),
                         'fuse.mode': 'pvfs', 'fuse.validation-mode': 'none'})
    mount = LocalFileIO()._to_file(FusePathResolver(options, None).resolve_local_path(
        table.table_path, table.identifier)).absolute()
    mount.parent.mkdir(parents=True, exist_ok=True)
    mount.symlink_to(LocalFileIO()._to_file(table.table_path), target_is_directory=True)
    io = FuseLocalFileIO(table.table_path, str(mount), options)
    table.file_io = CachingFileIO(io, Mock()) if cached else io
    return mount


@pytest.mark.parametrize('cached', [False, True])
def test_builtin_fuse_is_accepted_without_relaxing_custom_file_io(tmp_path, native_rest_catalog, cached):
    from pypaimon.filesystem.local_file_io import FuseLocalFileIO

    table = _table(tmp_path, native_rest_catalog)
    mount = _bind_fuse(table, tmp_path / 'fuse', cached)
    assert _rest_catalog_supported(table)

    class CustomFuse(FuseLocalFileIO):
        pass

    table.file_io = CustomFuse(table.table_path, str(mount))
    assert not _rest_catalog_supported(table)


@pytest.mark.parametrize('cached', [False, True])
@pytest.mark.parametrize('invalid', [None, 'source-uuid', 'source-path', 'target-path', 'target-branch'])
def test_copied_fuse_write_uses_existing_copy_and_checks_identity(
        tmp_path, native_rest_catalog, cached, invalid):
    from pypaimon.write.native_commit import create_native_write_table

    original = _table(tmp_path, native_rest_catalog)
    _bind_fuse(original, tmp_path / 'fuse', cached)
    table = original.copy_without_time_travel({'branch': 'dev'})
    assert table.catalog_environment.uuid is None
    assert _rest_catalog_supported(table)

    module = ModuleType('pypaimon_rust.datafusion')
    module.Table = Mock()
    module.PaimonCatalog = Mock()
    source = Mock(spec=['location', 'rest_table_uuid', 'copy'])
    target = Mock(spec=['location', 'branch', 'copy_with_resolved_schema'])
    source.location.return_value = original.table_path if invalid != 'source-path' else '/different'
    source.rest_table_uuid.return_value = (
        original.catalog_environment.uuid if invalid != 'source-uuid' else 'replacement-uuid')
    source.copy.return_value = target
    target.location.return_value = table.table_path if invalid != 'target-path' else '/different'
    target.branch.return_value = 'dev' if invalid != 'target-branch' else 'main'
    target.copy_with_resolved_schema.return_value = target
    module.PaimonCatalog.return_value.get_table.return_value = source
    with patch.dict('sys.modules', {'pypaimon_rust.datafusion': module}):
        result = create_native_write_table(table)
    module.PaimonCatalog.return_value.get_table.assert_called_once_with(('default', 't'))
    if invalid is None:
        assert result is target
        source.copy.assert_called_once_with({'branch': 'dev'})
        target.copy_with_resolved_schema.assert_called_once()
        schema = json.loads(target.copy_with_resolved_schema.call_args[0][0])
        assert schema['options']['branch'] == 'dev'
    else:
        assert result is None
        target.copy_with_resolved_schema.assert_not_called()
        if invalid.startswith('source-'):
            source.copy.assert_not_called()


@pytest.mark.parametrize('identity', [None, '', 123])
def test_copied_fuse_write_requires_original_rest_uuid(tmp_path, native_rest_catalog, identity):
    table = _table(tmp_path, native_rest_catalog)
    _bind_fuse(table, tmp_path / 'fuse')
    table = table.copy_without_time_travel({'branch': 'dev'})
    response = json.loads(table.catalog_environment.rest_table_response)
    response['id'] = identity
    table.catalog_environment.rest_table_response = json.dumps(response)
    assert not _rest_catalog_supported(table)


@native
@pytest.mark.parametrize('copied_branch', [False, True])
def test_fuse_native_write_commit_and_truncate_preserve_bound_mount(
        tmp_path, native_rest_catalog, copied_branch):
    table = _table(tmp_path, native_rest_catalog, {'write.native.enabled': 'true'})
    _bind_fuse(table, tmp_path / 'fuse')
    if copied_branch:
        table.branch_manager().create_branch('dev')
        table = table.copy({'branch': 'dev'})
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    assert isinstance(writer, NativeTableWrite)
    try:
        writer.write_arrow(pa.Table.from_pylist([{'id': 1, 'pt': 'a'}, {'id': 2, 'pt': 'b'}], schema=_SCHEMA))
        with patch.object(commit.file_store_commit, 'commit', side_effect=AssertionError('Python commit')):
            commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()

    assert _rows(table) == [{'id': 1, 'pt': 'a'}, {'id': 2, 'pt': 'b'}]
    commit = table.new_batch_write_builder().new_commit()
    try:
        with patch.object(commit.file_store_commit, 'drop_partitions', side_effect=AssertionError('Python truncate')):
            commit.truncate_partitions([{'pt': 'a'}])
        assert _rows(table) == [{'id': 2, 'pt': 'b'}]
        with patch.object(commit.file_store_commit, 'truncate_table', side_effect=AssertionError('Python truncate')):
            commit.truncate_table()
        assert _rows(table) == []
    finally:
        commit.close()


def _merge_table(tmp_path, use_native):
    schema = pa.schema([('id', pa.int32()), ('value', pa.int32()), ('payload', pa.string())])
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'merge')})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'write.native.enabled': str(use_native).lower()}), False)
    table = catalog.get_table('default.t')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist([
            {'id': 1, 'value': 10, 'payload': 'old'}, {'id': 2, 'value': 20, 'payload': 'keep'}
        ], schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    return table


@pytest.mark.parametrize('use_native', [False, pytest.param(True, marks=native)])
@pytest.mark.parametrize('source_key', ['id', 'source_id'])
@pytest.mark.parametrize('dictionary_key', [False, True])
def test_merge_callable_reads_target_source_and_coalesced_keys(tmp_path, use_native, source_key, dictionary_key):
    import pyarrow.compute as pc
    import pypaimon.table.data_evolution_merge_into as merge_impl
    from pypaimon.table.data_evolution_merge_into import WhenMatched, WhenNotMatched
    table = _merge_table(tmp_path, use_native)
    keys = pa.array([1, 3], pa.int32())
    source = pa.table({source_key: keys.dictionary_encode() if dictionary_key else keys,
                       'delta': pa.array([4, 5], pa.int32()), 'label': ['unused', 'new']})
    calls = []

    def value(batch):
        calls.append(batch.column_names)
        assert batch['s.' + source_key].type == pa.int32()
        return pc.add(pc.add(batch['t.value'], batch['s.delta']), batch['s.' + source_key])

    update = table.new_batch_write_builder().new_update()
    with patch('pypaimon.table.data_evolution_merge_into._build_tables',
               wraps=merge_impl._build_tables) as python_merge:
        messages = update.merge_into(
            source, on={'id': source_key},
            when_matched=[WhenMatched.update({
                'value': value, 'payload': lambda batch: pc.utf8_upper(batch['t.payload'])})],
            when_not_matched=[WhenNotMatched({
                'value': lambda batch: pc.multiply(batch['s.delta'], 2),
                'payload': lambda batch: batch['s.label']})])
        if use_native:
            python_merge.assert_not_called()
    table.new_batch_write_builder().new_commit().commit(messages)
    assert _rows(table) == [{'id': 1, 'value': 15, 'payload': 'OLD'},
                            {'id': 2, 'value': 20, 'payload': 'keep'},
                            {'id': 3, 'value': 10, 'payload': 'new'}]
    assert len(calls) == 1
    assert calls[0] == sorted(['t._ROW_ID', 't.id', 't.value', 't.payload',
                               's.' + source_key, 's.delta', 's.label'])


@pytest.mark.parametrize('use_native', [False, pytest.param(True, marks=native)])
def test_merge_callable_is_lazy_and_failure_preserves_committed_rows(tmp_path, use_native):
    from pypaimon.table.data_evolution_merge_into import WhenMatched
    table = _merge_table(tmp_path, use_native)
    cause = RuntimeError('callable failure')
    calls = []

    def fail(batch):
        calls.append(batch.num_rows)
        raise cause

    update = table.new_batch_write_builder().new_update()
    clause = [WhenMatched.update({'value': fail})]
    assert update.merge_into(pa.table({'id': pa.array([99], pa.int32())}), on=['id'],
                             when_matched=clause, when_not_matched=[]) == []
    assert calls == []
    with pytest.raises(RuntimeError) as error:
        update.merge_into(pa.table({'id': pa.array([1], pa.int32())}), on=['id'],
                          when_matched=clause, when_not_matched=[])
    assert error.value is cause
    assert calls == [1]
    assert _rows(table) == [{'id': 1, 'value': 10, 'payload': 'old'},
                            {'id': 2, 'value': 20, 'payload': 'keep'}]


@pytest.mark.parametrize('use_native', [False, pytest.param(True, marks=native)])
@pytest.mark.parametrize('result_kind', ['chunked', 'wrong_length', 'not_arrow'])
def test_merge_callable_validates_complete_clause_result(tmp_path, use_native, result_kind):
    from pypaimon.table.data_evolution_merge_into import WhenMatched
    table = _merge_table(tmp_path, use_native)
    source = pa.table({'id': pa.array([1, 2], pa.int32())})
    calls = []

    def values(batch):
        calls.append(batch.num_rows)
        if result_kind == 'chunked':
            return pa.chunked_array([pa.array([101]), pa.array([202])])
        if result_kind == 'wrong_length':
            return pa.array([1])
        return [101, 202]

    update = table.new_batch_write_builder().new_update()
    if result_kind == 'chunked':
        messages = update.merge_into(source, on=['id'], when_matched=[WhenMatched.update({'value': values})])
        table.new_batch_write_builder().new_commit().commit(messages)
        assert [row['value'] for row in _rows(table)] == [101, 202]
    else:
        with pytest.raises((ValueError, TypeError)):
            update.merge_into(source, on=['id'], when_matched=[WhenMatched.update({'value': values})])
        assert [row['value'] for row in _rows(table)] == [10, 20]
    assert calls == [2]


def test_merge_callback_aliases_use_coalesced_key_type():
    from pypaimon.ray.data_evolution_merge_transform import _merge_callable_input
    dictionary = pa.array([1, 2], pa.int32()).dictionary_encode()
    batch = pa.table({'t.id': pa.array([1, 2], pa.int32()), 's.source_id': dictionary,
                      's.value': [10, 20], '_scratch': [0, 0]})
    actual = _merge_callable_input(batch, [('source_id', 'id')])
    assert actual.column_names == ['s.source_id', 's.value', 't.id']
    assert actual['s.source_id'].equals(actual['t.id'])
    # INSERT callbacks retain source layout because no target key exists.
    unmatched = _merge_callable_input(batch.drop(['t.id']), [('source_id', 'id')])
    assert unmatched['s.source_id'].type == dictionary.type


def test_explicit_callable_read_columns_keep_self_merge_projection_narrow():
    from pypaimon.ray.data_evolution_merge_into import _resolve_target_projection
    from pypaimon.ray.data_evolution_merge_transform import _NormalizedClause
    clauses = [_NormalizedClause({'value': lambda rows: rows['value']})]
    fields = ['id', 'value', 'large_blob']
    assert _resolve_target_projection(clauses, ['_ROW_ID'], ['value'], fields,
                                      callable_columns=['value']) == ['value']
    assert _resolve_target_projection(clauses, ['id'], ['value'], fields) == fields


def test_consumed_blob_rows_are_released_without_losing_unplanned_inputs():
    from pypaimon.schema.data_types import AtomicType
    from pypaimon.table.row.blob import Blob, BlobDescriptor
    from pypaimon.write.native_blob_rows import NativeBlobRows
    rows = NativeBlobRows()
    blob_type = AtomicType('BLOB')
    known = BlobDescriptor.deserialize(rows.encode(Blob.from_data(b'known'), blob_type)).uri
    unknown = BlobDescriptor.deserialize(rows.encode(Blob.from_data(b'unknown'), blob_type)).uri
    reader = rows.create(known)
    assert rows.create(known) is reader
    rows.release_consumed()
    assert not rows._supports_uri(known)
    assert rows._supports_uri(unknown)
    with rows.create(unknown).new_input_stream(unknown) as stream:
        assert stream.read() == b'unknown'
    rows.clear()
    assert not rows.readers
    assert not rows._consumed


@pytest.mark.parametrize('use_native', [False, pytest.param(True, marks=native)])
def test_automatic_postpone_plan_counts_all_batches_per_partition(tmp_path, use_native):
    from pypaimon.write.native_write import NativePostponeFixedBucketTableWrite
    schema = pa.schema([('id', pa.int64()), ('pt', pa.string())])
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'postpone')})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id', 'pt'], partition_keys=['pt'], options={
            'bucket': '-2', 'write.native.enabled': str(use_native).lower(),
            'postpone.target-row-num-per-bucket': '2',
            'postpone.batch-write-fixed-bucket.max-parallelism': '3'}), False)
    table = catalog.get_table('default.t')
    builder = table.new_postpone_fixed_bucket_write_builder()
    writer = builder.new_write()
    if use_native:
        assert isinstance(writer, NativePostponeFixedBucketTableWrite)
    try:
        writer.write_arrow(pa.Table.from_pylist([{'id': 1, 'pt': 'a'}, {'id': 2, 'pt': 'b'}], schema=schema))
        writer.write_arrow(pa.Table.from_pylist([{'id': n, 'pt': 'a'} for n in [3, 4, 5, 6]], schema=schema))
        messages = writer.prepare_commit()
        assert {m.partition: m.total_buckets for m in messages} == {('a',): 3, ('b',): 1}
        builder.new_commit().commit(messages)
    finally:
        writer.close()
    assert _rows(table) == [{'id': 1, 'pt': 'a'}, {'id': 2, 'pt': 'b'}] + [
        {'id': n, 'pt': 'a'} for n in [3, 4, 5, 6]]


@native
@pytest.mark.parametrize('kind', ['boolean', 'date', 'time', 'timestamp', 'decimal', 'float',
                                  'unicode_int', 'decimal_overflow'])
def test_native_truncate_partition_specs_use_java_typed_strings(tmp_path, native_rest_catalog, kind):
    from datetime import date, datetime, time
    from decimal import Decimal
    cases = {
        'boolean': (pa.bool_(), [True, False], 'TRUE'),
        'date': (pa.date32(), [date(2024, 1, 2), date(2024, 1, 3)], '2024-01-02'),
        'time': (pa.time32('ms'), [time(12, 34, 56, 789000), time(12, 35)], '12:34:56.789'),
        'timestamp': (pa.timestamp('us'), [datetime(2024, 1, 2, 12, 34), datetime(2024, 1, 3)],
                      '2024-01-02 12:34:00'),
        'decimal': (pa.decimal128(6, 2), [Decimal('12.35'), Decimal('13.00')], '12.345'),
        'float': (pa.float32(), [1.2, 2.0], '1.2'),
        'unicode_int': (pa.int32(), [12, 13], '１２'),
        'decimal_overflow': (pa.decimal128(3, 0), [None, Decimal('12')], '1000'),
    }
    data_type, values, text = cases[kind]
    schema = pa.schema([('id', pa.int64()), ('pt', data_type)])
    native_rest_catalog.create_table('default.typed', Schema.from_pyarrow_schema(
        schema, partition_keys=['pt'], options={
            'write.native.enabled': 'false', 'commit.native.enabled': 'true'}), False)
    table = native_rest_catalog.get_table('default.typed')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table({'id': [1, 2], 'pt': values}, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    commit = table.new_batch_write_builder().new_commit()
    try:
        with patch.object(commit.file_store_commit, 'drop_partitions', side_effect=AssertionError('Python truncate')):
            commit.truncate_partitions([{'pt': text}])
        assert [row['id'] for row in _rows(table)] == [2]
    finally:
        commit.close()


@native
def test_automatic_postpone_blob_rows_keep_unknown_sources_until_prepare(tmp_path):
    from pypaimon.schema.data_types import AtomicType, DataField
    from pypaimon.table.row.blob import Blob, BlobDescriptor
    from pypaimon.table.row.generic_row import GenericRow
    from pypaimon.write.native_write import NativePostponeFixedBucketTableWrite
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'blob-postpone')})
    catalog.create_database('default', True)
    fields = [DataField(0, 'id', AtomicType('INT')), DataField(1, 'pt', AtomicType('STRING')),
              DataField(2, 'payload', AtomicType('BLOB'))]
    catalog.create_table('default.t', Schema(
        fields, partition_keys=['pt'], primary_keys=['id', 'pt'], options={
            'bucket': '-2', 'write.native.enabled': 'true',
            'postpone.target-row-num-per-bucket': '1'}), False)
    table = catalog.get_table('default.t')

    def write(rows, buffered):
        builder = table.new_postpone_fixed_bucket_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            assert isinstance(writer, NativePostponeFixedBucketTableWrite)
            for identifier, partition in rows:
                writer.write_row(GenericRow(
                    [identifier, partition, Blob.from_data(str(identifier).encode())], table.fields))
            assert len(writer._blob_rows.readers) == buffered
            messages = writer.prepare_commit()
            assert not writer._blob_rows.readers
            commit.commit(messages)
        finally:
            writer.close()
            commit.close()

    write([(1, 'known')], 1)
    write([(2, 'known'), (3, 'new')], 1)
    rows = _rows(table)
    assert [row['id'] for row in rows] == [1, 2, 3]
    assert [Blob.from_descriptor_bytes(row['payload'], table.file_io).to_data() for row in rows] == [b'1', b'2', b'3']
    assert all(not BlobDescriptor.deserialize(row['payload']).uri.startswith('pypaimon-blob-row:') for row in rows)
