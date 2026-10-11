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

"""Exercise managed primary-key Blobs exclusively through Rust reads and writes."""

from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon import CatalogFactory
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.table.row.blob import Blob, BlobDescriptor
from pypaimon.write.native_write import NativeTableWrite

pytestmark = pytest.mark.native_plan


def _table(tmp_path, mode='fixed', extra=None):
    # Table creation and schema validation belong to Rust. Python's catalog
    # only loads the resulting Java-compatible schema for the native bridge.
    from pypaimon_rust.datafusion import SQLContext
    options = {'bucket': '1' if mode == 'fixed' else '-2', 'file.format': 'parquet',
               'write.native.enabled': 'true', 'read.native.enabled': 'true',
               'scan.native-plan.enabled': 'true', 'rowkind.field': 'op',
               'blob.target-file-size': '1 B', 'postpone.default-bucket-num': '2'}
    options.update(extra or {})
    context = SQLContext()
    context.register_catalog('paimon', {'warehouse': str(tmp_path)})
    context.sql('CREATE SCHEMA paimon.managed')
    properties = ', '.join("'%s' = '%s'" % (key, value) for key, value in options.items())
    context.sql('CREATE TABLE paimon.managed.t ('
                'id INT, payload BLOB, items ARRAY<BLOB>, mapping MAP(STRING, BLOB), op STRING, '
                'PRIMARY KEY (id)) WITH (' + properties + ')')
    return CatalogFactory.create({'warehouse': str(tmp_path)}).get_table('managed.t')


def _builder(table, mode):
    return (table.new_postpone_fixed_bucket_write_builder() if mode == 'postpone-fixed'
            else table.new_batch_write_builder())


def _input(table, values):
    return pa.Table.from_pylist(values, schema=PyarrowFieldParser.from_paimon_schema(table.fields))


def _read(table, predicate=None, projection=None, limit=None, descriptors=False):
    table = table.copy({'blob-as-descriptor': str(descriptors).lower()})
    builder = table.new_read_builder()
    if predicate:
        builder.with_filter(predicate)
    if projection:
        builder.with_projection(projection)
    if limit is not None:
        builder.with_limit(limit)
    splits = builder.new_scan().plan().splits()
    read = builder.new_read()
    guard = patch.object(read, '_create_split_read', side_effect=AssertionError('Python read fallback'))
    with guard:
        return read.to_arrow(splits, blob_parallelism=2).to_pylist()


@pytest.mark.parametrize('mode', ['fixed', 'postpone-fixed'])
def test_managed_blob_roundtrip_nested_nulls_duplicates_and_payload_filters(tmp_path, mode):
    table = _table(tmp_path, mode)
    # Two commits force merging old descriptors with new descriptors.
    batches = [
        [dict(id=1, payload=b'old', items=[b'a', None], mapping=[('a', b'x'), ('a', b'y')], op='+I'),
         dict(id=2, payload=None, items=None, mapping=None, op='+I'),
         dict(id=3, payload=b'other', items=[], mapping=[], op='+I')],
        [dict(id=1, payload=b'new', items=[None, b'', b'new'], mapping=[('b', None), ('b', b'')], op='+U')],
    ]
    for rows in batches:
        builder = _builder(table, mode)
        writer, commit = builder.new_write(), builder.new_commit()
        assert isinstance(writer, NativeTableWrite)
        try:
            writer.write_arrow(_input(table, rows))
            messages = writer.prepare_commit()
            assert all(any(name.endswith('.blobref') for name in f.extra_files)
                       for m in messages for f in m.new_files)
            commit.commit(messages)
        finally:
            writer.close()
            commit.close()
    expected = [batches[1][0], *batches[0][1:]]
    assert sorted(_read(table), key=lambda row: row['id']) == expected
    predicate = table.new_read_builder().new_predicate_builder().equal('payload', b'new')
    assert _read(table, predicate, ['items', 'id'], limit=1) == [dict(items=[None, b'', b'new'], id=1)]
    assert _read(table, predicate, ['payload', 'id'], limit=1) == [dict(payload=b'new', id=1)]
    # Descriptors are exposed only when requested, including nested leaves.
    descriptors = sorted(_read(table, descriptors=True), key=lambda row: row['id'])
    assert BlobDescriptor.is_blob_descriptor(descriptors[0]['payload'])
    assert BlobDescriptor.is_blob_descriptor(descriptors[0]['items'][1])
    assert descriptors[0]['items'][0] is None
    assert descriptors[1]['payload'] is None


@pytest.mark.parametrize('mode', ['fixed', 'postpone-fixed', 'pending'])
def test_managed_blob_arrow_inputs_copy_referenced_payloads(tmp_path, mode):
    table = _table(tmp_path, mode)
    source = tmp_path / 'source.bin'
    source.write_bytes(b'reference')
    reference = Blob.from_file(table.file_io, str(source), 0, 9)
    builder = _builder(table, mode)
    writer = builder.new_write()
    assert isinstance(writer, NativeTableWrite)
    try:
        writer.write_arrow(_input(table, [dict(
            id=1, payload=b'row', items=[reference.to_descriptor().serialize(), None],
            mapping=[('a', b'')], op='+I')]))
        writer.write_arrow(_input(table, [dict(id=2, payload=b'arrow', items=[], mapping=[], op='+I')]))
        messages = writer.prepare_commit()
        assert writer._python_writer is None
        rows = []
        for message in messages:
            for file in message.new_files:
                with table.file_io.new_input_stream(file.file_path) as stream:
                    rows.extend(pq.ParquetFile(stream).read().to_pylist())
        row = next(row for row in rows if row['id'] == 1)
        assert Blob.from_descriptor_bytes(row['items'][0], table.file_io).to_data() == b'reference'
        assert BlobDescriptor.is_blob_descriptor(row['payload'])
        assert source.read_bytes() == b'reference'
    finally:
        writer.close()


def test_pending_blob_retracts_drop_payloads_but_preserve_events(tmp_path):
    table = _table(tmp_path, 'pending', {'target-file-row-num': '2'})
    builder = table.new_stream_write_builder()
    writer = builder.new_write()
    try:
        for checkpoint in (1, 2):
            rows = [dict(id=3, payload=b'a', items=[b'b'], mapping=[('c', b'd')], op='+I'),
                    dict(id=1, payload=b'ignored', items=[b'ignored'], mapping=[('c', b'ignored')], op='-D'),
                    dict(id=3, payload=b'e', items=[], mapping=[], op='+U')]
            writer.write_arrow(_input(table, rows))
            messages = writer.prepare_commit(checkpoint)
            files = [f for m in messages for f in m.new_files]
            assert [f.row_count for f in files] == [3]
            assert files[0].delete_row_count == 1
            with table.file_io.new_input_stream(files[0].file_path) as stream:
                physical = pq.ParquetFile(stream).read().to_pylist()
            assert [row['id'] for row in physical] == [3, 1, 3]
            assert physical[1]['payload'] is None
            assert physical[1]['items'] is None
            assert physical[1]['mapping'] is None
            assert writer.prepare_commit(checkpoint + 100) == []
    finally:
        writer.close()
