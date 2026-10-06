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

"""Branch writes follow Java's independent snapshot and shared-data layout."""

from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.common.identifier import Identifier
from pypaimon.common.options.core_options import MergeEngine
from pypaimon.write.native_write import NativeTableWrite

pytestmark = pytest.mark.native_plan
_SCHEMA = pa.schema([('id', pa.int32()), ('value', pa.int32()), ('p', pa.string())])


def _table(tmp_path, rest_catalog, rest, options=None, primary_keys=None):
    catalog = rest_catalog if rest else CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    settings = {'file.format': 'parquet', 'write.native.enabled': 'true',
                'commit.native.enabled': 'true', 'manifest.sidecar.enabled': 'true',
                'bucket': '1' if primary_keys else '-1'}
    settings.update(options or {})
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        _SCHEMA, primary_keys=primary_keys or [], partition_keys=['p'], options=settings), False)
    return catalog, catalog.get_table('default.t')


def _prepare(builder, rows, checkpoint=None):
    writer = builder.new_write()
    assert isinstance(writer, NativeTableWrite)
    try:
        writer.write_arrow(rows if isinstance(rows, pa.Table)
                           else pa.Table.from_pylist(rows, schema=_SCHEMA))
        assert writer._python_writer is None
        return writer.prepare_commit() if checkpoint is None else writer.prepare_commit(checkpoint)
    finally:
        writer.close()


def _commit(builder, messages, checkpoint=None, overwrite=False):
    commit = builder.new_commit()
    try:
        def publish():
            if checkpoint is None:
                commit.commit(messages)
            else:
                commit.commit(messages, checkpoint)
        if builder.table.catalog_environment.supports_version_management:
            with patch.object(commit.file_store_commit, 'overwrite' if overwrite else 'commit',
                              side_effect=AssertionError('Python commit fallback')):
                publish()
        else:
            publish()
    finally:
        commit.close()


def _write(table, rows):
    builder = (table.new_postpone_fixed_bucket_write_builder()
               if table.options.bucket() == -2 else table.new_batch_write_builder())
    _commit(builder, _prepare(builder, rows))


def _branch(catalog, table, from_tag=True, name='dev'):
    if from_tag:
        catalog.create_tag(table.identifier, 'base')
    catalog.create_branch(table.identifier, name, tag_name='base' if from_tag else None)
    return catalog.get_table(Identifier('default', 't', branch=name))


def _rows(table, native):
    builder = table.copy({'scan.native-plan.enabled': str(native).lower(),
                          'read.native.enabled': str(native).lower()}).new_read_builder()
    scan = builder.new_scan()
    splits = (scan.plan_for_write() if table.is_primary_key_table
              and table.options.merge_engine() == MergeEngine.FIRST_ROW else scan.plan()).splits()
    reader = builder.new_read()
    if native:
        with patch.object(reader, '_create_split_read', side_effect=AssertionError('Python read fallback')):
            rows = reader.to_arrow(splits).to_pylist()
    else:
        rows = reader.to_arrow(splits).to_pylist()
    return sorted(rows, key=lambda row: (row['p'], row['id'], row['value']))


def _assert_rows(table, expected):
    for native in (False, True):
        assert _rows(table, native) == expected


@pytest.mark.parametrize('rest', [False, True], ids=['filesystem', 'rest'])
@pytest.mark.parametrize('from_tag', [False, True], ids=['empty', 'tagged'])
def test_native_branch_append_isolates_main(tmp_path, native_rest_catalog, rest, from_tag):
    catalog, main = _table(tmp_path, native_rest_catalog, rest)
    seed = {'id': 1, 'value': 10, 'p': 'a'}
    _write(main, [seed])
    branch = _branch(catalog, main, from_tag)
    _write(main, [{'id': 2, 'value': 200, 'p': 'a'}])
    _write(branch, [{'id': 3, 'value': 30, 'p': 'b'}])
    _assert_rows(main, [seed, {'id': 2, 'value': 200, 'p': 'a'}])
    _assert_rows(branch, ([seed] if from_tag else []) + [{'id': 3, 'value': 30, 'p': 'b'}])
    assert main.snapshot_manager().get_latest_snapshot().id == 2
    assert branch.snapshot_manager().get_latest_snapshot().id == (2 if from_tag else 1)


@pytest.mark.parametrize('rest', [False, True], ids=['filesystem', 'rest'])
@pytest.mark.parametrize('engine,branch_value,main_value', [
    ('deduplicate', 20, 100), ('first-row', 10, 10),
    ('partial-update', 20, 100), ('aggregation', 30, 110),
])
def test_native_branch_primary_key_merge_engines(
        tmp_path, native_rest_catalog, rest, engine, branch_value, main_value):
    options = {'merge-engine': engine}
    if engine == 'aggregation':
        options['fields.value.aggregate-function'] = 'sum'
    catalog, main = _table(tmp_path, native_rest_catalog, rest, options, ['p', 'id'])
    _write(main, [{'id': 1, 'value': 10, 'p': 'a'}])
    branch = _branch(catalog, main)
    _write(main, [{'id': 1, 'value': 100, 'p': 'a'}])
    _write(branch, [{'id': 1, 'value': 20, 'p': 'a'}])
    _assert_rows(main, [{'id': 1, 'value': main_value, 'p': 'a'}])
    _assert_rows(branch, [{'id': 1, 'value': branch_value, 'p': 'a'}])


@pytest.mark.parametrize('rest', [False, True], ids=['filesystem', 'rest'])
@pytest.mark.parametrize('routing', ['dynamic', 'cross-partition', 'postpone'])
def test_native_branch_bucket_routing(tmp_path, native_rest_catalog, rest, routing):
    options = {'bucket': '-2' if routing == 'postpone' else '-1',
               'dynamic-bucket.target-row-num': '1', 'postpone.default-bucket-num': '2'}
    keys = ['id'] if routing == 'cross-partition' else ['p', 'id']
    catalog, main = _table(tmp_path, native_rest_catalog, rest, options, keys)
    original = {'id': 1, 'value': 10, 'p': 'a'}
    _write(main, [original])
    branch = _branch(catalog, main)
    _write(main, [{'id': 2, 'value': 200, 'p': 'b'}])
    _write(branch, [{'id': 1, 'value': 20, 'p': 'b'}, {'id': 3, 'value': 30, 'p': 'b'}])
    _assert_rows(main, [original, {'id': 2, 'value': 200, 'p': 'b'}])
    expected = [] if routing == 'cross-partition' else [original]
    _assert_rows(branch, expected + [
        {'id': 1, 'value': 20, 'p': 'b'}, {'id': 3, 'value': 30, 'p': 'b'}])


@pytest.mark.parametrize('rest', [False, True], ids=['filesystem', 'rest'])
@pytest.mark.parametrize('data_evolution', [False, True], ids=['append', 'data-evolution'])
def test_native_branch_overwrite(tmp_path, native_rest_catalog, rest, data_evolution):
    options = ({'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
               if data_evolution else {})
    catalog, main = _table(tmp_path, native_rest_catalog, rest, options)
    original = {'id': 1, 'value': 10, 'p': 'a'}
    _write(main, [original])
    branch = _branch(catalog, main)
    builder = branch.new_batch_write_builder().overwrite({'p': 'a'})
    changed = {'id': 2, 'value': 20, 'p': 'a'}
    _commit(builder, _prepare(builder, [changed]), overwrite=True)
    _assert_rows(main, [original])
    _assert_rows(branch, [changed])
    assert main.snapshot_manager().get_latest_snapshot().id == 1
    assert branch.snapshot_manager().get_latest_snapshot().id == 2
    if data_evolution:
        assert main.snapshot_manager().get_latest_snapshot().next_row_id == 1
        assert branch.snapshot_manager().get_latest_snapshot().next_row_id == 2


@pytest.mark.parametrize('rest', [False, True], ids=['filesystem', 'rest'])
def test_native_branch_stream_checkpoint_histories(tmp_path, native_rest_catalog, rest):
    catalog, main = _table(tmp_path, native_rest_catalog, rest, primary_keys=['p', 'id'])
    _write(main, [{'id': 1, 'value': 10, 'p': 'a'}])
    branch = _branch(catalog, main)
    for target, value in ((main, 100), (branch, 20)):
        builder = target.new_stream_write_builder()._with_commit_user('same-writer')
        messages = _prepare(builder, [{'id': 1, 'value': value, 'p': 'a'}], checkpoint=7)
        _commit(builder, messages, checkpoint=7)
        snapshot = target.snapshot_manager().get_latest_snapshot()
        assert snapshot.id == 2
        assert snapshot.commit_identifier == 7
    _assert_rows(main, [{'id': 1, 'value': 100, 'p': 'a'}])
    _assert_rows(branch, [{'id': 1, 'value': 20, 'p': 'a'}])


def _update_value(table, value):
    from pypaimon.write.table_update_by_row_id import TableUpdateByRowId

    builder = table.new_batch_write_builder()
    data = pa.table({'_ROW_ID': pa.array([0], type=pa.int64()),
                     'value': pa.array([value], type=pa.int32())})
    with patch.object(TableUpdateByRowId, 'update_columns',
                      side_effect=AssertionError('Python update fallback')):
        messages = builder.new_update().with_update_type(['value']).update_by_arrow_with_row_id(data)
    assert messages
    return builder, messages


@pytest.mark.parametrize('rest', [False, True], ids=['filesystem', 'rest'])
def test_native_branch_row_id_conflicts_are_scoped(tmp_path, native_rest_catalog, rest):
    catalog, main = _table(tmp_path, native_rest_catalog, rest, {
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'})
    _write(main, [{'id': 1, 'value': 10, 'p': 'a'}])
    branch = _branch(catalog, main)
    main_builder, main_messages = _update_value(main, 100)
    branch_builder, branch_messages = _update_value(branch, 20)
    _commit(branch_builder, branch_messages)
    _commit(main_builder, main_messages)
    _assert_rows(main, [{'id': 1, 'value': 100, 'p': 'a'}])
    _assert_rows(branch, [{'id': 1, 'value': 20, 'p': 'a'}])
    stale_builder, stale_messages = _update_value(branch, 21)
    fresh_builder, fresh_messages = _update_value(branch, 22)
    _commit(fresh_builder, fresh_messages)
    with pytest.raises(Exception, match='conflict|Conflict|overlap'):
        _commit(stale_builder, stale_messages)
    assert branch.snapshot_manager().get_latest_snapshot().id == 3
    _assert_rows(branch, [{'id': 1, 'value': 22, 'p': 'a'}])
    _assert_rows(main, [{'id': 1, 'value': 100, 'p': 'a'}])


@pytest.mark.parametrize('rest', [False, True], ids=['filesystem', 'rest'])
@pytest.mark.parametrize('from_tag', [False, True], ids=['empty', 'tagged'])
def test_native_branch_deletion_vectors_and_row_id_allocation(
        tmp_path, native_rest_catalog, rest, from_tag):
    from pypaimon.write.table_delete import TableDeleteByRowId

    catalog, main = _table(tmp_path, native_rest_catalog, rest, {
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'deletion-vectors.enabled': 'true'})
    original = [{'id': 1, 'value': 10, 'p': 'a'}, {'id': 2, 'value': 20, 'p': 'a'}]
    _write(main, original)
    branch = _branch(catalog, main, from_tag)
    if not from_tag:
        _write(branch, original)
    builder = branch.new_batch_write_builder()
    with patch.object(TableDeleteByRowId, 'delete',
                      side_effect=AssertionError('Python delete fallback')):
        messages = builder.new_update().delete_by_row_id([0])
    _commit(builder, messages)
    _assert_rows(main, original)
    _assert_rows(branch, [original[1]])
    assert branch.snapshot_manager().get_latest_snapshot().next_row_id == 2
    third = {'id': 3, 'value': 30, 'p': 'a'}
    _write(branch, [third])
    _assert_rows(branch, [original[1], third])
    assert branch.snapshot_manager().get_latest_snapshot().next_row_id == 3
    assert main.snapshot_manager().get_latest_snapshot().next_row_id == 2


@pytest.mark.parametrize('rest', [False, True], ids=['filesystem', 'rest'])
def test_native_branch_schema_evolution_is_independent(tmp_path, native_rest_catalog, rest):
    from pypaimon.schema.data_types import AtomicType
    from pypaimon.schema.schema_change import SchemaChange

    catalog, main = _table(tmp_path, native_rest_catalog, rest)
    seed = {'id': 1, 'value': 10, 'p': 'a'}
    _write(main, [seed])
    branch = _branch(catalog, main)
    catalog.alter_table(main.identifier, [SchemaChange.add_column('main_only', AtomicType('INT'))], False)
    main = catalog.get_table(main.identifier)
    main_schema = _SCHEMA.append(pa.field('main_only', pa.int32()))
    _write(main, pa.Table.from_pylist(
        [{'id': 2, 'value': 200, 'p': 'a', 'main_only': 7}], schema=main_schema))
    _write(branch, [{'id': 3, 'value': 30, 'p': 'b'}])
    assert main.snapshot_manager().get_latest_snapshot().schema_id == 1
    assert branch.snapshot_manager().get_latest_snapshot().schema_id == 0
    assert 'main_only' not in branch.field_names
    catalog.alter_table(branch.identifier, [
        SchemaChange.add_column('dev_only', AtomicType('STRING')),
        SchemaChange.set_option('target-file-row-num', '1')], False)
    branch = catalog.get_table(branch.identifier)
    branch_schema = _SCHEMA.append(pa.field('dev_only', pa.string()))
    _write(branch, pa.Table.from_pylist(
        [{'id': 4, 'value': 40, 'p': 'b', 'dev_only': 'dev'}], schema=branch_schema))
    _assert_rows(branch, [dict(seed, dev_only=None),
                          {'id': 3, 'value': 30, 'p': 'b', 'dev_only': None},
                          {'id': 4, 'value': 40, 'p': 'b', 'dev_only': 'dev'}])
    _assert_rows(main, [dict(seed, main_only=None),
                        {'id': 2, 'value': 200, 'p': 'a', 'main_only': 7}])
    assert 'dev_only' not in main.field_names
    assert branch.snapshot_manager().get_latest_snapshot().schema_id == branch.table_schema.id
    assert main.snapshot_manager().get_latest_snapshot().schema_id == 1


@pytest.mark.parametrize('rest', [False, True], ids=['filesystem', 'rest'])
def test_native_branch_predicate_upsert_and_batch_updates(tmp_path, native_rest_catalog, rest):
    from pypaimon.read.table_read import TableRead
    from pypaimon.write.table_update import BatchTableUpdate
    from pypaimon.write.table_upsert_by_key import TableUpsertByKey
    from pypaimon.write.table_delete import TableDeleteByRowId

    catalog, main = _table(tmp_path, native_rest_catalog, rest, {
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'deletion-vectors.enabled': 'true', 'target-file-row-num': '1'})
    original = [{'id': 1, 'value': 10, 'p': 'a'}, {'id': 2, 'value': 20, 'p': 'b'}]
    _write(main, original)
    branch = _branch(catalog, main)
    predicate = branch.new_read_builder().new_predicate_builder().equal('id', 1)
    builder = branch.new_batch_write_builder()
    with patch.object(TableRead, 'to_arrow', side_effect=AssertionError('Python predicate scan fallback')):
        messages = builder.new_update().update_by_predicate(
            predicate, {'value': lambda batch: pa.compute.add(batch['value'], 1)}, ['value'])
    _commit(builder, messages)
    builder = branch.new_batch_write_builder()
    with patch.object(TableUpsertByKey, '_upsert_partition',
                      side_effect=AssertionError('Python upsert fallback')):
        messages = builder.new_update().upsert_by_arrow_with_key(pa.Table.from_pylist([
            {'id': 1, 'value': 12, 'p': 'a'}, {'id': 3, 'value': 30, 'p': 'c'}], schema=_SCHEMA), ['id'])
    _commit(builder, messages)
    # Partition groups may be published in either order. Match logical rows
    # to their actual IDs rather than assuming a HashMap iteration order.
    reader = branch.copy({'scan.native-plan.enabled': 'true', 'read.native.enabled': 'true'})
    read = reader.new_read_builder().with_projection(['id', '_ROW_ID'])
    ids = {row['id']: row['_ROW_ID'] for row in read.new_read().to_arrow(
        read.new_scan().plan().splits()).to_pylist()}
    builder = branch.new_batch_write_builder()
    with patch.object(BatchTableUpdate, '_update_by_arrow_batches_with_row_id',
                      side_effect=AssertionError('Python grouped update fallback')):
        messages = builder.new_update().with_update_type(['value']).update_by_arrow_batches_with_row_id([
            pa.table({'_ROW_ID': [ids[1]], 'value': [13]}),
            pa.table({'_ROW_ID': [ids[2]], 'value': [21]})])
    _commit(builder, messages)
    builder = branch.new_batch_write_builder()
    with patch.object(TableDeleteByRowId, 'delete', side_effect=AssertionError('Python delete fallback')):
        messages = builder.new_update().delete_by_predicate(
            branch.new_read_builder().new_predicate_builder().equal('id', 2))
    _commit(builder, messages)
    _assert_rows(main, original)
    _assert_rows(branch, [{'id': 1, 'value': 13, 'p': 'a'}, {'id': 3, 'value': 30, 'p': 'c'}])
    assert branch.snapshot_manager().get_latest_snapshot().next_row_id == 3
    assert main.snapshot_manager().get_latest_snapshot().next_row_id == 2


@pytest.mark.parametrize('rest', [False, True], ids=['filesystem', 'rest'])
def test_native_branch_blob_append_and_overwrite_leave_shared_payloads_readable(tmp_path, native_rest_catalog, rest):
    from pypaimon.schema.data_types import AtomicType, DataField

    catalog = native_rest_catalog if rest else CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    options = {'file.format': 'parquet', 'write.native.enabled': 'true',
               'commit.native.enabled': 'true', 'manifest.sidecar.enabled': 'true',
               'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
               'blob.target-file-size': '1 B'}
    fields = [DataField(0, 'id', AtomicType('INT')), DataField(1, 'value', AtomicType('BLOB')),
              DataField(2, 'p', AtomicType('STRING'))]
    catalog.create_table('default.t', Schema(fields=fields, partition_keys=['p'], options=options), False)
    main = catalog.get_table('default.t')
    schema = pa.schema([('id', pa.int32()), ('value', pa.large_binary()), ('p', pa.string())])
    seed = {'id': 1, 'value': b'original', 'p': 'a'}
    _write(main, pa.Table.from_pylist([seed], schema=schema))
    branch = _branch(catalog, main)
    builder = branch.new_batch_write_builder()
    added = {'id': 2, 'value': b'branch', 'p': 'a'}
    messages = _prepare(builder, pa.Table.from_pylist([added], schema=schema))
    assert any(file.file_name.endswith('.blob') for message in messages for file in message.new_files)
    _commit(builder, messages)
    assert _rows(main, True) == [seed]
    assert _rows(branch, True) == [seed, added]
    builder = branch.new_batch_write_builder().overwrite({'p': 'a'})
    replacement = {'id': 2, 'value': b'replacement', 'p': 'a'}
    _commit(builder, _prepare(builder, pa.Table.from_pylist([replacement], schema=schema)), overwrite=True)
    assert _rows(branch, True) == [replacement]
    assert _rows(main, True) == [seed]


@pytest.mark.parametrize('rest', [False, True], ids=['filesystem', 'rest'])
def test_native_branch_abort_preserves_prepared_output(tmp_path, native_rest_catalog, rest):
    catalog, main = _table(tmp_path, native_rest_catalog, rest)
    seed = {'id': 1, 'value': 10, 'p': 'a'}
    _write(main, [seed])
    branch = _branch(catalog, main)
    builder = branch.new_batch_write_builder()
    messages = _prepare(builder, [{'id': 2, 'value': 20, 'p': 'a'}])
    paths = [file.file_path for message in messages for file in message.new_files]
    assert paths and all(branch.file_io.exists(path) for path in paths)
    commit = builder.new_commit()
    try:
        if rest:
            with patch.object(commit.file_store_commit, 'abort',
                              side_effect=AssertionError('Python abort fallback')):
                commit.abort(messages)
        else:
            commit.abort(messages)
    finally:
        commit.close()
    assert all(branch.file_io.exists(path) for path in paths)
    assert branch.snapshot_manager().get_latest_snapshot().id == 1
    _assert_rows(main, [seed])
    _assert_rows(branch, [seed])


@pytest.mark.parametrize('rest', [False, True], ids=['filesystem', 'rest'])
def test_native_branch_lifecycle_preserves_histories(tmp_path, native_rest_catalog, rest):
    from pypaimon.catalog.catalog_exception import TableNotExistException

    catalog, main = _table(tmp_path, native_rest_catalog, rest)
    seed = {'id': 1, 'value': 10, 'p': 'a'}
    added = {'id': 2, 'value': 20, 'p': 'a'}
    _write(main, [seed])
    branch = _branch(catalog, main)
    _write(branch, [added])
    catalog.rename_branch(main.identifier, 'dev', 'renamed')
    catalog.rename_table(main.identifier, Identifier('default', 'renamed'))
    main = catalog.get_table('default.renamed')
    branch = catalog.get_table(Identifier('default', 'renamed', branch='renamed'))
    _assert_rows(main, [seed])
    _assert_rows(branch, [seed, added])
    assert catalog.list_branches(main.identifier) == ['renamed']
    assert catalog.get_tag(main.identifier, 'base').snapshot.id == 1
    with pytest.raises(TableNotExistException):
        catalog.get_table('default.t')
    third = {'id': 3, 'value': 30, 'p': 'b'}
    _write(branch, [third])
    _assert_rows(branch, [seed, added, third])
    catalog.drop_branch(main.identifier, 'renamed')
    assert catalog.list_branches(main.identifier) == []
    _assert_rows(main, [seed])
    catalog.create_branch(main.identifier, 'renamed')
    empty = catalog.get_table(Identifier('default', 'renamed', branch='renamed'))
    _assert_rows(empty, [])
    _write(empty, [added])
    _assert_rows(empty, [added])
    assert empty.snapshot_manager().get_latest_snapshot().id == 1
    _assert_rows(main, [seed])


@pytest.mark.parametrize('name', ['main', '', ' ', '123'])
def test_native_rest_branch_invalid_names_preserve_main(tmp_path, native_rest_catalog, name):
    from pypaimon.api.rest_exception import BadRequestException
    from pypaimon.catalog.catalog_exception import IllegalArgumentError

    catalog, main = _table(tmp_path, native_rest_catalog, True)
    seed = {'id': 1, 'value': 10, 'p': 'a'}
    _write(main, [seed])
    for operation in (lambda: catalog.create_branch(main.identifier, name),
                      lambda: catalog.drop_branch(main.identifier, name)):
        with pytest.raises((BadRequestException, IllegalArgumentError, ValueError)):
            operation()
        _assert_rows(main, [seed])
    branch = _branch(catalog, main)
    with pytest.raises((BadRequestException, IllegalArgumentError, ValueError)):
        catalog.rename_branch(main.identifier, 'dev', name)
    _assert_rows(branch, [seed])
    _assert_rows(main, [seed])
    assert main.schema_manager.latest().id == 0
    assert catalog.list_branches(main.identifier) == ['dev']


@pytest.mark.parametrize('rest', [False, True], ids=['filesystem', 'rest'])
@pytest.mark.parametrize('name', [' dev ', ' main ', ' 123 ', 'dev+plus'])
def test_native_branch_names_preserve_whitespace(tmp_path, native_rest_catalog, rest, name):
    catalog, main = _table(tmp_path, native_rest_catalog, rest)
    seed = {'id': 1, 'value': 10, 'p': 'a'}
    _write(main, [seed])
    dev = _branch(catalog, main)
    normal = {'id': 2, 'value': 20, 'p': 'a'}
    _write(dev, [normal])
    catalog.create_branch(main.identifier, name, tag_name='base')
    branch = catalog.get_table(Identifier('default', 't', branch=name))
    assert branch.current_branch() == name
    changed = {'id': 3, 'value': 30, 'p': 'a'}
    _write(branch, [changed])
    _assert_rows(branch, [seed, changed])
    _assert_rows(dev, [seed, normal])
    _assert_rows(main, [seed])
    assert set(catalog.list_branches(main.identifier)) == {'dev', name}
    catalog.drop_branch(main.identifier, name)
    _assert_rows(dev, [seed, normal])
    _assert_rows(main, [seed])
    assert catalog.list_branches(main.identifier) == ['dev']
