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

"""Native updates of Parquet columns preserve dedicated Blob files."""

from contextlib import ExitStack
from unittest.mock import patch
from urllib.parse import urlparse

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.schema.data_types import AtomicType, DataField, PyarrowFieldParser
from pypaimon.table.row.blob import Blob, BlobDescriptor, BlobViewStruct
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.table_delete import TableDeleteByRowId
from pypaimon.write.native_update import NativeTableUpdateByRowId
from pypaimon.write.native_write import NativeTableWrite
from pypaimon.write.table_update import TableUpdate
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
from pypaimon.write.table_upsert_by_key import TableUpsertByKey

pytestmark = pytest.mark.native_plan


def _table(tmp_path, extra=None, catalog=None, payloads=None):
    catalog = catalog or CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('db', True)
    fields = [DataField(0, 'id', AtomicType('INT')),
              DataField(1, 'pt', AtomicType('STRING')),
              DataField(2, 'value', AtomicType('INT')),
              DataField(3, 'payload', AtomicType('BLOB'))]
    options = {'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
               'write.native.enabled': 'true', 'read.native.enabled': 'true',
               'scan.native-plan.enabled': 'true', 'deletion-vectors.enabled': 'true',
               'blob.target-file-size': '16 B', 'target-file-row-num': '2'}
    options.update(extra or {})
    catalog.create_table('db.t', Schema(fields, partition_keys=['pt'], options=options), False)
    table = catalog.get_table('db.t')
    schema = PyarrowFieldParser.from_paimon_schema(table.fields)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    assert isinstance(writer, NativeTableWrite)
    try:
        writer.write_arrow(pa.table({'id': [0, 1, 2, 3], 'pt': ['a', 'a', 'b', 'b'],
                                     'value': [10, 11, 12, 13],
                                     'payload': payloads if payloads is not None else
                                     [b'large' * 10, None, b'', b'last']}, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    return table, schema


def _read(table, descriptors=False, projection=None, resolve_views=True):
    table = table.copy({'blob-as-descriptor': str(descriptors).lower(),
                        'blob-view.resolve.enabled': str(resolve_views).lower()})
    builder = table.new_read_builder()
    if projection is not None:
        builder.with_projection(projection)
    splits = builder.new_scan().plan().splits()
    read = builder.new_read()
    with patch.object(read, '_create_split_read', side_effect=AssertionError('Python read fallback')):
        return read.to_arrow(splits).sort_by('id').to_pylist()


def _native_only():
    stack = ExitStack()
    for cls, method in [(TableUpdateByRowId, '_load_existing_files_info'),
                        (TableUpdate, '_build_predicate_update_table'),
                        (TableUpsertByKey, '_upsert_partition'),
                        (TableUpsertByKey, '_upsert_row_partition'),
                        (TableDeleteByRowId, 'delete')]:
        stack.enter_context(patch.object(cls, method, side_effect=AssertionError('Python update fallback')))
    return stack


def _commit(builder, messages, stream):
    commit = builder.new_commit()
    try:
        commit.commit(messages, 7) if stream else commit.commit(messages)
    finally:
        commit.close()


@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('operation', ['row_id', 'predicate'])
def test_native_updates_keep_blob_references(tmp_path, stream, operation):
    table, _ = _table(tmp_path)
    before = _read(table, descriptors=True)
    assert BlobDescriptor.is_blob_descriptor(before[0]['payload'])
    row_ids = {row['id']: row['_ROW_ID']
               for row in _read(table, projection=['id', '_ROW_ID'])}
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    update = builder.new_update().with_update_type(['value'])
    data = pa.table({'_ROW_ID': [row_ids[0], row_ids[2]],
                     'value': pa.array([100, 200], type=pa.int32())})
    with _native_only():
        if operation == 'row_id':
            messages = (update.update_by_arrow_with_row_id(data, 7) if stream
                        else update.update_by_arrow_with_row_id(data))
        else:
            predicate = PredicateBuilder(table.fields).is_in('id', [0, 2])

            def assignment(matched):
                return pa.array([100 if value == 0 else 200
                                 for value in matched['id'].to_pylist()], type=pa.int32())

            args = (predicate, {'value': assignment})
            messages = (update.update_by_predicate(*args, 7, read_columns=['id']) if stream
                        else update.update_by_predicate(*args, read_columns=['id']))
    _commit(builder, messages, stream)
    after = _read(table, descriptors=True)
    assert [row['payload'] for row in after] == [row['payload'] for row in before]
    assert [row['value'] for row in after] == [100, 11, 200, 13]
    assert [row['payload'] for row in _read(table)] == [b'large' * 10, None, b'', b'last']


@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('row_input', [False, True])
def test_native_upsert_updates_normal_columns_and_appends_blob_rows(tmp_path, stream, row_input):
    table, schema = _table(tmp_path)
    before = _read(table, descriptors=True)
    source = pa.table({'id': [0, 4], 'pt': ['a', 'b'], 'value': [100, 400],
                       'payload': [b'ignored matched payload', b'new payload']}, schema=schema)
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    update = builder.new_update().with_update_type(['value'])
    with _native_only():
        if row_input:
            rows = [GenericRow([row[field.name] for field in table.fields], table.fields)
                    for row in source.to_pylist()]
            messages = (update.upsert_by_key(rows, ['id'], 7) if stream
                        else update.upsert_by_key(rows, ['id']))
        else:
            messages = (update.upsert_by_arrow_with_key(source, ['id'], 7) if stream
                        else update.upsert_by_arrow_with_key(source, ['id']))
    _commit(builder, messages, stream)
    rows = _read(table)
    assert [row['value'] for row in rows] == [100, 11, 12, 13, 400]
    assert [row['payload'] for row in rows] == [b'large' * 10, None, b'', b'last', b'new payload']
    after = _read(table, descriptors=True)
    assert [row['payload'] for row in after[:4]] == [row['payload'] for row in before]


@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('predicate_delete', [False, True])
def test_native_blob_table_delete_preserves_remaining_rows(tmp_path, stream, predicate_delete):
    table, _ = _table(tmp_path)
    before = _read(table, descriptors=True)
    ids = {row['id']: row['_ROW_ID'] for row in _read(table, projection=['id', '_ROW_ID'])}
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    update = builder.new_update()
    with _native_only():
        if predicate_delete:
            predicate = PredicateBuilder(table.fields).is_in('id', [0, 2])
            messages = (update.delete_by_predicate(predicate, 7) if stream
                        else update.delete_by_predicate(predicate))
        else:
            row_ids = [ids[0], ids[2], ids[0]]
            messages = (update.delete_by_row_id(row_ids, 7) if stream
                        else update.delete_by_row_id(row_ids))
    assert messages and all(not message.new_files for message in messages)
    _commit(builder, messages, stream)
    assert _read(table, descriptors=True) == [before[1], before[3]]
    assert [row['payload'] for row in _read(table)] == [None, b'last']


def test_normal_column_update_does_not_open_dedicated_blobs(tmp_path):
    table, _ = _table(tmp_path)
    ids = _read(table, projection=['id', '_ROW_ID'])
    for path in (tmp_path / 'warehouse').rglob('*.blob'):
        path.unlink()
    builder = table.new_batch_write_builder()
    with _native_only():
        messages = builder.new_update().update_by_arrow_with_row_id(pa.table({
            '_ROW_ID': [row['_ROW_ID'] for row in ids],
            'value': pa.array([100, 101, 102, 103], type=pa.int32()),
        }))
    assert all(file.write_cols == ['value'] for message in messages for file in message.new_files)
    _commit(builder, messages, False)
    assert _read(table, projection=['id', 'value']) == [
        {'id': index, 'value': 100 + index} for index in range(4)]


def _inline_table(tmp_path, option, catalog=None):
    source = tmp_path / 'references'
    source.write_bytes(b'firstlast')
    references = [BlobDescriptor(source.as_uri(), 0, 5).serialize(), None,
                  BlobDescriptor(source.as_uri(), 5, 0).serialize(),
                  BlobDescriptor(source.as_uri(), 5, 4).serialize()]
    if option == 'blob-view-field':
        references = [BlobViewStruct('db.absent_upstream', 3, row_id).serialize()
                      if value is not None else None for row_id, value in enumerate(references)]
    table, schema = _table(tmp_path, {option: 'payload'}, catalog=catalog, payloads=references)
    return table, schema, references


@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('option', ['blob-descriptor-field', 'blob.stored-descriptor-fields', 'blob-view-field'])
@pytest.mark.parametrize('operation', ['row_id', 'incremental', 'row', 'rows', 'predicate', 'upsert'])
def test_native_inline_blob_updates_preserve_references(
        tmp_path, native_rest_catalog, stream, native, option, operation):
    # REST views reference an absent table. Updating the local reference must
    # neither resolve it nor read an old upstream payload for unmatched rows.
    table, schema, references = _inline_table(tmp_path, option, native_rest_catalog)
    table = table.copy({'write.native.enabled': str(native).lower()})
    ids = {row['id']: row['_ROW_ID'] for row in _read(table, projection=['id', '_ROW_ID'])}
    replacement = references[0]
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    update = builder.new_update().with_update_type(['payload'])
    data = pa.table({'_ROW_ID': [ids[1], ids[2]],
                     'payload': pa.array([replacement, None], type=pa.large_binary())})
    with _native_only() if native else ExitStack():
        if operation == 'row_id':
            messages = (update.update_by_arrow_with_row_id(data, 7) if stream
                        else update.update_by_arrow_with_row_id(data))
        elif operation == 'incremental':
            updater = update.new_update_by_row_id(7) if stream else update.new_update_by_row_id()
            assert isinstance(updater, NativeTableUpdateByRowId) == native
            updater.update_columns(data.slice(0, 1), ['payload'])
            updater.update_columns(data.slice(1), ['payload'])
            messages = updater.commit_messages
        elif operation in ('row', 'rows'):
            updater = update.new_update_by_row_id(7) if stream else update.new_update_by_row_id()
            assert isinstance(updater, NativeTableUpdateByRowId) == native
            value = (Blob.from_view(BlobViewStruct.deserialize(replacement)) if option == 'blob-view-field'
                     else Blob.from_descriptor_bytes(replacement, file_io=table.file_io))
            rows = [GenericRow([1, 'a', 999, value], table.fields),
                    GenericRow([2, 'b', 999, None], table.fields)]
            if operation == 'row':
                updater.update_row_columns(rows[0], [ids[1]], ['payload'])
                updater.update_row_columns(rows[1], [ids[2]], ['payload'])
            else:
                updater.update_rows_columns(rows, [[ids[1]], [ids[2]]], ['payload'])
            messages = updater.commit_messages
        elif operation == 'predicate':
            predicate = PredicateBuilder(table.fields).is_in('id', [1, 2])

            def assignment(matched):
                return pa.array([replacement if row_id == 1 else None
                                 for row_id in matched['id'].to_pylist()], type=pa.large_binary())

            args = (predicate, {'payload': assignment})
            messages = (update.update_by_predicate(*args, 7, read_columns=['id']) if stream
                        else update.update_by_predicate(*args, read_columns=['id']))
        else:
            source = pa.table({'id': [1, 2], 'pt': ['a', 'b'], 'value': [999, 999],
                               'payload': [replacement, None]}, schema=schema)
            messages = (update.upsert_by_arrow_with_key(source, ['id'], 7) if stream
                        else update.upsert_by_arrow_with_key(source, ['id']))
    assert all(file.file_name.endswith('.parquet') and file.write_cols == ['payload']
               for message in messages for file in message.new_files)
    _commit(builder, messages, stream)
    rows = _read(table, descriptors=True, resolve_views=False)
    assert [row['payload'] for row in rows] == [references[0], replacement, None, references[3]]
    assert [row['value'] for row in rows] == [10, 11, 12, 13]
    if option != 'blob-view-field':
        assert [row['payload'] for row in _read(table)] == [b'first', b'first', None, b'last']


def test_raw_blob_incremental_updater_keeps_one_implementation(tmp_path):
    # Later calls choose their columns independently. Keeping this operation
    # on Python permits a scalar update followed by a raw Blob update.
    table, _ = _table(tmp_path)
    from pypaimon.write.native_update import create_native_update_by_row_id
    assert create_native_update_by_row_id(table, 'test', 7) is None


def test_view_prescan_keeps_inline_descriptor_predicate(tmp_path, native_rest_catalog):
    source, _ = _table(tmp_path, catalog=native_rest_catalog)
    expected = _read(source, descriptors=True)[0]['payload']
    row_id = _read(source, projection=['id', '_ROW_ID'])[0]['_ROW_ID']
    file = tmp_path / 'descriptor'
    file.write_bytes(b'unopened')
    reference = BlobDescriptor(file.as_uri(), 0, 8).serialize()
    fields = [DataField(0, 'id', AtomicType('INT')), DataField(1, 'value', AtomicType('INT')),
              DataField(2, 'reference', AtomicType('BLOB')), DataField(3, 'payload', AtomicType('BLOB'))]
    options = {'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
               'blob-descriptor-field': 'reference', 'blob-view-field': 'payload',
               'write.native.enabled': 'true', 'read.native.enabled': 'true',
               'scan.native-plan.enabled': 'true', 'blob-as-descriptor': 'true'}
    native_rest_catalog.create_table('db.mixed', Schema(fields, options=options), False)
    table = native_rest_catalog.get_table('db.mixed')
    schema = PyarrowFieldParser.from_paimon_schema(table.fields)
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    try:
        writer.write_arrow(pa.table({'id': [0, 1], 'value': [10, 11],
                                     'reference': [reference, None],
                                     'payload': [BlobViewStruct('db.t', 3, row_id).serialize(), None]}, schema=schema))
        _commit(builder, writer.prepare_commit(), False)
    finally:
        writer.close()
    with _native_only():
        messages = builder.new_update().update_by_arrow_with_row_id(pa.table({
            '_ROW_ID': [0], 'value': pa.array([100], type=pa.int32())}))
    _commit(builder, messages, False)
    for native in (False, True):
        view = table.copy({'read.native.enabled': str(native).lower(),
                           'scan.native-plan.enabled': str(native).lower()})
        read_builder = view.new_read_builder().with_projection(['id', 'payload']).with_limit(1)
        read_builder.with_filter(PredicateBuilder(view.fields).equal('reference', reference))
        plan = read_builder.new_scan().plan()
        read = read_builder.new_read()
        with (patch.object(read, '_create_split_read', side_effect=AssertionError('Python read fallback'))
              if native else ExitStack()):
            rows = read.to_arrow(plan.splits()).to_pylist()
            assert len(rows) == 1 and rows[0]['id'] == 0
            actual = BlobDescriptor.deserialize(rows[0]['payload'])
            original = BlobDescriptor.deserialize(expected)
            assert (urlparse(actual.uri).path, actual.offset, actual.length) == (
                urlparse(original.uri).path, original.offset, original.length)
