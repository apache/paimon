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

"""Existing row Blob inputs must retain the core writer and source lifetime."""

import io
from decimal import Decimal
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.schema.data_types import AtomicType, ArrayType, DataField, MapType, PyarrowFieldParser
from pypaimon.table.row.blob import Blob, BlobDescriptor, BlobViewStruct
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.native_write import NativeTableWrite

pytestmark = pytest.mark.native_plan


class StreamBlob(Blob):
    def __init__(self, payload, error=None):
        self.payload = payload
        self.error = error
        self.streams = []
        self.opened = 0

    def to_data(self):
        raise AssertionError('Blob payload was materialized in Python')

    def to_descriptor(self):
        raise AssertionError('Custom Blob must use its input stream')

    def new_input_stream(self):
        self.opened += 1
        if self.error is not None:
            raise self.error

        class Stream(io.BytesIO):
            def __init__(self, payload):
                super().__init__(payload)
                self.read_sizes = []

            def read(self, size=-1):
                assert 0 < size <= 3, 'Core must use its configured copy buffer'
                self.read_sizes.append(size)
                return super().read(size)

        stream = Stream(self.payload)
        self.streams.append(stream)
        return stream


def _type(kind):
    blob = AtomicType('BLOB')
    if kind == 'array':
        return ArrayType(True, blob)
    if kind == 'map':
        return MapType(True, AtomicType('STRING'), blob)
    return blob


def _value(kind, payload):
    if kind == 'array':
        return [payload, None, b'']
    if kind == 'map':
        return [('object', payload), ('null', None), ('empty', b'')]
    return payload


def _table(tmp_path, kind, external=False, extra=None, payload_type=None):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('db', True)
    fields = [DataField(0, 'id', AtomicType('INT')),
              DataField(1, 'pt', AtomicType('STRING')), DataField(2, 'payload', payload_type or _type(kind))]
    options = {'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
               'write.native.enabled': 'true', 'read.native.enabled': 'true',
               'scan.native-plan.enabled': 'true', 'target-file-row-num': '2',
               'blob.target-file-size': '32 B', 'blob.copy-buffer-size': '3 B'}
    if external:
        options['data-file.external-paths'] = (tmp_path / 'external').as_uri()
        options['data-file.external-paths.strategy'] = 'round-robin'
    options.update(extra or {})
    catalog.create_table('db.t', Schema(fields, partition_keys=['pt'], options=options), False)
    return catalog.get_table('db.t')


def _read(table):
    builder = table.new_read_builder()
    read = builder.new_read()
    with patch.object(read, '_create_split_read', side_effect=AssertionError('Python read fallback')):
        return sorted(read.to_arrow(builder.new_scan().plan().splits()).to_pylist(), key=lambda row: row['id'])


def _commit(builder, messages, stream, identifier=7):
    commit = builder.new_commit()
    try:
        commit.commit(messages, identifier) if stream else commit.commit(messages)
    finally:
        commit.close()


@pytest.mark.parametrize('kind', ['scalar', 'array', 'map'])
@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('external', [False, True])
def test_blob_rows_mix_with_arrow_and_release_sources(tmp_path, kind, stream, external):
    table = _table(tmp_path, kind, external)
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer = builder.new_write()
    blob = StreamBlob(b'streamed payload')
    empty = b'' if kind == 'scalar' else []
    expected = [dict(id=0, pt='a', payload=_value(kind, b'arrow')),
                dict(id=1, pt='a', payload=_value(kind, blob.payload)),
                dict(id=2, pt='b', payload=empty), dict(id=3, pt='b', payload=None)]
    try:
        assert isinstance(writer, NativeTableWrite)
        with patch.object(writer, '_switch_to_python', side_effect=AssertionError('Python writer fallback')):
            writer.write_arrow(pa.Table.from_pylist(
                expected[:1], schema=PyarrowFieldParser.from_paimon_schema(table.fields)))
            writer.write_row(GenericRow([1, 'a', _value(kind, blob)], table.fields))
            writer.write_row(GenericRow([2, 'b', empty], table.fields))
            writer.write_row(GenericRow([3, 'b', None], table.fields))
        assert blob.opened == 1
        assert all(source.closed and source.read_sizes for source in blob.streams)
        assert writer._blob_rows.readers == {}
        assert writer._python_writer is None
        messages = writer.prepare_commit(7) if stream else writer.prepare_commit()
        _commit(builder, messages, stream)
        assert _read(table) == expected
    finally:
        writer.close()


@pytest.mark.parametrize('kind', ['scalar', 'array', 'map'])
def test_stream_blob_rows_keep_routing_after_multiple_prepares(tmp_path, kind):
    table = _table(tmp_path, kind)
    builder = table.new_stream_write_builder()
    writer = builder.new_write()
    expected = []
    try:
        for identifier in [1, 2]:
            blob = StreamBlob(('payload-%d' % identifier).encode())
            with patch.object(writer, '_switch_to_python', side_effect=AssertionError('Python writer fallback')):
                writer.write_row(GenericRow([identifier, 'a', _value(kind, blob)], table.fields))
            assert blob.opened == 1 and all(source.closed for source in blob.streams)
            assert writer._blob_rows.readers == {}
            _commit(builder, writer.prepare_commit(identifier), True, identifier)
            expected.append(dict(id=identifier, pt='a', payload=_value(kind, blob.payload)))
        assert _read(table) == expected
    finally:
        writer.close()


@pytest.mark.parametrize('kind', ['scalar', 'array', 'map'])
def test_blob_row_subset_uses_named_fields_without_opening_omitted_blobs(tmp_path, kind):
    table = _table(tmp_path, kind)
    builder = table.new_batch_write_builder()
    writer = builder.new_write().with_write_type(['pt', 'id'])
    omitted = StreamBlob(b'ignored', AssertionError('omitted Blob opened'))
    try:
        with patch.object(writer, '_switch_to_python', side_effect=AssertionError('Python writer fallback')):
            writer.write_row(GenericRow([_value(kind, omitted), 1, 'a'],
                                        [table.fields[2], table.fields[0], table.fields[1]]))
        assert omitted.opened == 0
        _commit(builder, writer.prepare_commit(), False)
        assert _read(table) == [dict(id=1, pt='a', payload=None)]
    finally:
        writer.close()


@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('blob_object', [False, True])
def test_blob_row_map_preserves_java_null_keys(tmp_path, stream, blob_object):
    from pypaimon.read.reader.format_blob_reader import FormatBlobReader
    table = _table(tmp_path, 'map', extra={'blob.target-file-size': '1 MB'})
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer = builder.new_write()
    value = StreamBlob(b'NULL key') if blob_object else b'NULL key'
    try:
        with patch.object(writer, '_switch_to_python', side_effect=AssertionError('Python writer fallback')):
            writer.write_row(GenericRow([1, 'a', [(None, value), ('normal', None)]], table.fields))
        messages = writer.prepare_commit(7) if stream else writer.prepare_commit()
        _commit(builder, messages, stream)
        files = [file for message in messages for file in message.new_files if file.file_name.endswith('.blob')]
        assert len(files) == 1
        reader = FormatBlobReader(table.file_io, files[0].file_path, ['payload'], [table.fields[2]], None, False)
        try:
            row = reader.read_values_at([0])[0]
            assert row[None].to_data() == b'NULL key' and row['normal'] is None
        finally:
            reader.close()
    finally:
        writer.close()


def test_duplicate_blob_map_keys_are_rejected_before_opening_payloads(tmp_path):
    table = _table(tmp_path, 'map')
    writer = table.new_batch_write_builder().new_write()
    first = StreamBlob(b'first', AssertionError('duplicate map opened'))
    second = StreamBlob(b'second', AssertionError('duplicate map opened'))
    try:
        with patch.object(writer, '_switch_to_python', side_effect=AssertionError('Python writer fallback')):
            with pytest.raises(ValueError, match='unique'):
                writer.write_row(GenericRow([1, 'a', [(None, first), (None, second)]], table.fields))
        assert first.opened == second.opened == 0
        assert writer._blob_rows.readers == {}
        assert not list(tmp_path.rglob('*.blob'))
    finally:
        writer.close()


@pytest.mark.parametrize('kind', ['scalar', 'array', 'map'])
def test_blob_row_stream_error_preserves_prepared_files_and_exception(tmp_path, kind):
    table = _table(tmp_path, kind)
    builder = table.new_stream_write_builder()
    writer = builder.new_write()
    error = OSError('original Python stream failure')
    bad = StreamBlob(b'bad', error)
    try:
        writer.write_row(GenericRow([0, 'a', _value(kind, b'prepared')], table.fields))
        messages = writer.prepare_commit(1)
        prepared = {path for path in tmp_path.rglob('*') if path.is_file() and path.suffix in ('.parquet', '.blob')}
        with patch.object(writer, '_switch_to_python', side_effect=AssertionError('Python writer fallback')):
            with pytest.raises(OSError) as caught:
                writer.write_row(GenericRow([1, 'a', _value(kind, bad)], table.fields))
        assert caught.value is error
        assert writer._blob_rows.readers == {}
        writer.abort()
        assert all(path.exists() for path in prepared)
        _commit(builder, messages, True, 1)
        assert _read(table) == [dict(id=0, pt='a', payload=_value(kind, b'prepared'))]
    finally:
        writer.close()


def test_blob_object_stream_and_native_file_descriptor_share_one_writer(tmp_path):
    table = _table(tmp_path, 'scalar')
    source = tmp_path / 'source'
    source.write_bytes(b'prefixREFERENCEDsuffix')
    reference = BlobDescriptor(source.as_uri(), 6, 10).serialize()
    blob = StreamBlob(b'OBJECT')
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    try:
        writer.write_row(GenericRow([0, 'a', blob], table.fields))
        writer.write_row(GenericRow([1, 'a', reference], table.fields))
        assert writer._blob_rows.readers == {}
        _commit(builder, writer.prepare_commit(), False)
        assert _read(table) == [dict(id=0, pt='a', payload=b'OBJECT'),
                                dict(id=1, pt='a', payload=b'REFERENCED')]
    finally:
        writer.close()


@pytest.mark.parametrize('kind', ['scalar', 'array', 'map'])
@pytest.mark.parametrize('stream', [False, True])
def test_row_upsert_matches_and_deduplicates_before_opening_blob_streams(tmp_path, kind, stream):
    from pypaimon.tests.native_blob_update_test import _native_only
    table = _table(tmp_path, kind)
    seed_builder = table.new_batch_write_builder()
    seed = seed_builder.new_write()
    try:
        seed.write_arrow(pa.Table.from_pylist([
            dict(id=0, pt='a', payload=_value(kind, b'old')),
            dict(id=1, pt='a', payload=_value(kind, b'keep')),
        ], schema=PyarrowFieldParser.from_paimon_schema(table.fields)))
        _commit(seed_builder, seed.prepare_commit(), False)
    finally:
        seed.close()
    matched = StreamBlob(b'MATCHED')
    appended = StreamBlob(b'APPENDED')
    shadowed = StreamBlob(b'bad', AssertionError('Shadowed upsert source opened'))
    source = [GenericRow([0, 'a', _value(kind, shadowed)], table.fields),
              GenericRow([2, 'b', _value(kind, shadowed)], table.fields),
              GenericRow([0, 'a', _value(kind, matched)], table.fields),
              GenericRow([2, 'b', _value(kind, appended)], table.fields)]
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    with _native_only():
        update = builder.new_update().with_update_type(['payload'])
        messages = update.upsert_by_key(source, ['id'], 7) if stream else update.upsert_by_key(source, ['id'])
    assert shadowed.opened == 0
    assert matched.opened == appended.opened == 1
    assert all(source.closed for blob in (matched, appended) for source in blob.streams)
    _commit(builder, messages, stream)
    assert _read(table) == [dict(id=0, pt='a', payload=_value(kind, b'MATCHED')),
                            dict(id=1, pt='a', payload=_value(kind, b'keep')),
                            dict(id=2, pt='b', payload=_value(kind, b'APPENDED'))]


@pytest.mark.parametrize('kind', ['scalar', 'array', 'map'])
def test_row_upsert_source_error_is_not_retried_through_python(tmp_path, kind):
    from pypaimon.tests.native_blob_update_test import _native_only
    table = _table(tmp_path, kind)
    bad = StreamBlob(b'bad', OSError('upsert Blob source failed'))
    builder = table.new_batch_write_builder()
    with _native_only(), pytest.raises(OSError) as caught:
        builder.new_update().upsert_by_key([GenericRow([0, 'a', _value(kind, bad)], table.fields)], ['id'])
    assert caught.value is bad.error
    assert bad.opened == 1
    assert table.snapshot_manager().get_latest_snapshot() is None
    assert not list(tmp_path.rglob('*.blob'))


@pytest.mark.parametrize('kind', ['scalar', 'array', 'map'])
def test_blob_row_missing_partition_is_rejected_without_opening_streams(tmp_path, kind):
    table = _table(tmp_path, kind)
    blob = StreamBlob(b'not opened')
    writer = table.new_batch_write_builder().new_write()
    try:
        with pytest.raises(ValueError, match='pt'):
            writer.write_row(GenericRow([0, _value(kind, blob)], [table.fields[0], table.fields[2]]))
        assert blob.opened == 0
        assert not list(tmp_path.rglob('*.blob'))
        writer.write_row(GenericRow([0, 'a', _value(kind, blob)], table.fields))
        assert blob.opened == 1
    finally:
        writer.close()


@pytest.mark.parametrize('operation', ['write', 'upsert'])
@pytest.mark.parametrize('entry', ['ab', b'ab', None, {'a': b'value'}])
def test_blob_map_rows_reject_non_pair_entries(tmp_path, operation, entry):
    table = _table(tmp_path, 'map')
    blob = StreamBlob(b'not opened')
    row = GenericRow([0, 'a', [('valid', blob), entry]], table.fields)
    builder = table.new_batch_write_builder()
    with pytest.raises(ValueError, match='key/value pairs'):
        if operation == 'upsert':
            builder.new_update().upsert_by_key([row], ['id'])
        else:
            writer = builder.new_write()
            try:
                writer.write_row(row)
            finally:
                writer.close()
    assert blob.opened == 0
    assert not list(tmp_path.rglob('*.blob'))


@pytest.mark.parametrize('operation', ['write', 'upsert'])
@pytest.mark.parametrize('type_name, key, expected', [
    ('DECIMAL(10,2)', '12.345', '12.35'),
    ('DECIMAL(10,2)', '-12.345', '-12.35'),
    ('DECIMAL(38,18)', '12345678901234567890.1234567890123456785',
     '12345678901234567890.123456789012345679'),
    ('DECIMAL(10,2)', '0.004999999999999999999999999999999999999999999', '0.00'),
])
def test_blob_map_row_decimal_keys_round_in_core(tmp_path, operation, type_name, key, expected):
    from pypaimon.tests.native_blob_update_test import _native_only
    from pypaimon.read.reader.format_blob_reader import FormatBlobReader
    table = _table(tmp_path, 'map', payload_type=MapType(True, AtomicType(type_name), AtomicType('BLOB')))
    builder = table.new_batch_write_builder()
    row = GenericRow([0, 'a', [(Decimal(key), StreamBlob(b'value'))]], table.fields)
    if operation == 'upsert':
        seed = builder.new_write()
        try:
            seed.write_row(GenericRow([0, 'a', [(Decimal('1'), b'old')]], table.fields))
            _commit(builder, seed.prepare_commit(), False)
        finally:
            seed.close()
    with _native_only():
        if operation == 'upsert':
            messages = builder.new_update().with_update_type(['payload']).upsert_by_key([row], ['id'])
        else:
            writer = builder.new_write()
            try:
                writer.write_row(row)
                messages = writer.prepare_commit()
            finally:
                writer.close()
    _commit(builder, messages, False)
    file = next(file for message in messages for file in message.new_files if file.file_name.endswith('.blob'))
    reader = FormatBlobReader(table.file_io, file.file_path, ['payload'], [table.fields[2]], None, False)
    try:
        result = reader.read_values_at([0])[0]
        assert list(result) == [Decimal(expected)]
        assert result[Decimal(expected)].to_data() == b'value'
    finally:
        reader.close()


@pytest.mark.parametrize('option', ['blob-descriptor-field', 'blob.stored-descriptor-fields', 'blob-view-field'])
@pytest.mark.parametrize('stream', [False, True])
def test_inline_blob_row_objects_keep_real_references(tmp_path, native_rest_catalog, option, stream):
    from pypaimon.tests.native_blob_update_test import _inline_table, _read as read_inline
    table, _, references = _inline_table(tmp_path, option, catalog=native_rest_catalog)
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer = builder.new_write()
    reference = references[0]
    value = (Blob.from_view(BlobViewStruct.deserialize(reference)) if option == 'blob-view-field'
             else Blob.from_descriptor_bytes(reference, table.file_io))
    try:
        with patch.object(writer, '_switch_to_python', side_effect=AssertionError('Python writer fallback')):
            with patch.object(value, 'new_input_stream', side_effect=AssertionError('Inline reference opened')):
                writer.write_row(GenericRow([4, 'a', 14, value], table.fields))
        assert writer._blob_rows.readers == {}
        messages = writer.prepare_commit(7) if stream else writer.prepare_commit()
        assert all(not file.file_name.endswith('.blob') for message in messages for file in message.new_files)
        _commit(builder, messages, stream)
        rows = read_inline(table, descriptors=True, resolve_views=False)
        assert rows[-1] == dict(id=4, pt='a', value=14, payload=reference)
    finally:
        writer.close()


@pytest.mark.parametrize('key', ['99999999.995', '-99999999.995'])
def test_decimal_blob_map_overflow_is_rejected_before_opening_payload(tmp_path, key):
    table = _table(tmp_path, 'map', payload_type=MapType(True, AtomicType('DECIMAL(10,2)'), AtomicType('BLOB')))
    blob = StreamBlob(b'not opened')
    writer = table.new_batch_write_builder().new_write()
    try:
        with pytest.raises(ValueError, match='precision'):
            writer.write_row(GenericRow([0, 'a', [(Decimal(key), blob)]], table.fields))
        assert blob.opened == 0
        assert writer._blob_rows.readers == {}
        assert not list(tmp_path.rglob('*.blob'))
    finally:
        writer.close()


def test_decimal_blob_map_duplicate_keys_are_checked_after_rounding(tmp_path):
    table = _table(tmp_path, 'map', payload_type=MapType(True, AtomicType('DECIMAL(10,2)'), AtomicType('BLOB')))
    blob = StreamBlob(b'not opened')
    writer = table.new_batch_write_builder().new_write()
    try:
        with pytest.raises(ValueError, match='unique'):
            writer.write_row(GenericRow([0, 'a', [(Decimal('12.345'), blob), (Decimal('12.35'), blob)]], table.fields))
        assert blob.opened == 0
        assert writer._blob_rows.readers == {}
        assert not list(tmp_path.rglob('*.blob'))
    finally:
        writer.close()
