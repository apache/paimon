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

"""Append event validation follows Java before routing or opening BLOB inputs."""

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.native.runtime import (native_runtime_available)
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.row_kind import RowKind

pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
_NATIVE = [False, pytest.param(True, marks=[pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed')])]


def _table(tmp_path, native=False, evolution=False, generated=True, **options):
    schema = pa.schema([pa.field('id', pa.int32(), nullable=False), ('op', pa.string()),
                        ('pt', pa.string())])
    config = {'write.native.enabled': str(native).lower(), 'commit.native.enabled': 'false',
              'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
              'data-evolution.enabled': str(evolution).lower(),
              'row-tracking.enabled': str(evolution).lower()}
    if generated:
        config['rowkind.field'] = 'op'
    config.update(options)
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, partition_keys=['pt'], options=config), False)
    return catalog.get_table('default.t'), schema


def _write(writer, table, schema, values, input_, generated):
    if input_ == 'row':
        for value in values:
            kind = RowKind.INSERT if generated else RowKind.from_string(value['op'] or '+I')
            writer.write_row(GenericRow([value[field.name] for field in table.fields], table.fields, kind))
    elif input_ == 'pandas':
        import pandas as pd
        writer.write_pandas(pd.DataFrame.from_records(values, columns=schema.names))
    else:
        if input_ == 'large_string':
            schema = pa.schema([field.with_type(pa.large_string()) if pa.types.is_string(field.type)
                                else field for field in schema])
        data = pa.Table.from_pylist(values, schema=schema)
        writer.write_arrow(data.to_batches()[0] if input_ == 'batch' else data)


def _read(table):
    read = table.new_read_builder()
    return sorted(read.new_read().to_arrow(read.new_scan().plan().splits()).to_pylist(), key=lambda x: x['id'])


@pytest.mark.parametrize('native', _NATIVE)
@pytest.mark.parametrize('evolution', [False, True])
@pytest.mark.parametrize('input_', ['row', 'batch', 'table', 'large_string', 'pandas'])
@pytest.mark.parametrize('kind', ['-D', '-U'])
def test_append_retract_is_rejected(tmp_path, native, evolution, input_, kind):
    table, schema = _table(tmp_path, native, evolution)
    writer = table.new_batch_write_builder().new_write()
    try:
        with pytest.raises(Exception, match='(?i)append.*row.*kind|append.*retract'):
            _write(writer, table, schema, [dict(id=1, op=kind, pt='bad')], input_, True)
        assert writer.prepare_commit() == []
    finally:
        writer.close()


@pytest.mark.parametrize('native', _NATIVE)
@pytest.mark.parametrize('evolution', [False, True])
@pytest.mark.parametrize('input_', ['row', 'batch', 'table', 'large_string', 'pandas'])
def test_append_ignore_delete_filters_before_partitioning(tmp_path, native, evolution, input_):
    table, schema = _table(tmp_path, native, evolution, **{'ignore-delete': 'true'})
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    values = [dict(id=i, op=kind, pt=pt) for i, kind, pt in [
        (1, '-D', 'delete'), (2, '+U', 'keep'), (3, '-U', 'before'), (4, '+I', 'keep')]]
    try:
        _write(writer, table, schema, values, input_, True)
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    assert _read(table) == [values[1], values[3]]


@pytest.mark.parametrize('native', _NATIVE)
@pytest.mark.parametrize('generated', [False, True])
@pytest.mark.parametrize('ignored', [False, True])
def test_append_internal_row_header_respects_retract_policy(tmp_path, native, generated, ignored):
    table, schema = _table(tmp_path, native, generated=generated, **{'ignore-delete': str(ignored).lower()})
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    value = dict(id=1, op='-D', pt='p')
    try:
        if ignored:
            _write(writer, table, schema, [value], 'row', generated)
            assert writer.prepare_commit() == []
        else:
            with pytest.raises(Exception, match='(?i)append.*row.*kind|append.*retract'):
                _write(writer, table, schema, [value], 'row', generated)
    finally:
        writer.close()
        commit.close()


@pytest.mark.parametrize('native', _NATIVE)
@pytest.mark.parametrize('input_', ['row', 'batch', 'table', 'pandas'])
def test_append_checks_not_null_before_ignoring_delete(tmp_path, native, input_):
    table, schema = _table(tmp_path, native, **{'ignore-delete': 'true'})
    writer = table.new_batch_write_builder().new_write()
    try:
        with pytest.raises(Exception, match='(?i)non-null|non nullable|non-nullable'):
            _write(writer, table, schema, [dict(id=None, op='-D', pt='bad')], input_, True)
        assert writer.prepare_commit() == []
    finally:
        writer.close()


@pytest.mark.parametrize('native', _NATIVE)
def test_append_ignore_before_does_not_ignore_delete(tmp_path, native):
    table, schema = _table(tmp_path, native, **{'ignore-update-before': 'true'})
    writer = table.new_batch_write_builder().new_write()
    try:
        writer.write_arrow(pa.Table.from_pylist([dict(id=1, op='-U', pt='bad')], schema=schema))
        with pytest.raises(Exception, match='(?i)append.*row.*kind|append.*retract'):
            writer.write_arrow(pa.Table.from_pylist([dict(id=2, op='-D', pt='bad')], schema=schema))
        assert writer.prepare_commit() == []
    finally:
        writer.close()


@pytest.mark.parametrize('native', _NATIVE)
def test_append_configured_add_still_checks_internal_retract_header(tmp_path, native):
    table, _ = _table(tmp_path, native)
    writer = table.new_batch_write_builder().new_write()
    try:
        row = GenericRow([1, '+I', 'p'], table.fields, RowKind.DELETE)
        with pytest.raises(Exception, match='(?i)append.*row.*kind|append.*retract'):
            writer.write_row(row)
        assert writer.prepare_commit() == []
    finally:
        writer.close()


@pytest.mark.parametrize('native', _NATIVE)
@pytest.mark.parametrize('input_', ['row', 'batch'])
def test_append_write_type_checks_only_written_not_null_fields(tmp_path, native, input_):
    table, schema = _table(tmp_path, native, generated=False)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    writer.with_write_type(['pt'])
    value = dict(id=None, op=None, pt='p')
    try:
        _write(writer, table, schema, [value], input_, False)
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    assert _read(table) == [value]


@pytest.mark.parametrize('native', _NATIVE)
@pytest.mark.parametrize('ignored', [False, True])
def test_append_retract_never_opens_blob_input(tmp_path, native, ignored, monkeypatch):
    from pypaimon.table.row.blob import BlobData
    schema = pa.schema([('id', pa.int32()), ('op', pa.string()), ('payload', pa.large_binary())])
    options = {'write.native.enabled': str(native).lower(), 'rowkind.field': 'op',
               'ignore-delete': str(ignored).lower(), 'row-tracking.enabled': 'true',
               'data-evolution.enabled': 'true'}
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options=options), False)
    table = catalog.get_table('default.t')
    writer = table.new_batch_write_builder().new_write()
    blob = BlobData(b'unused')

    def unexpected(*args):
        raise AssertionError('Append retract opened its Blob payload')

    monkeypatch.setattr(blob, 'to_data', unexpected)
    monkeypatch.setattr(blob, 'new_input_stream', unexpected)
    try:
        row = GenericRow([1, '-D', blob], table.fields)
        if ignored:
            writer.write_row(row)
        else:
            with pytest.raises(Exception, match='(?i)append.*row.*kind|append.*retract'):
                writer.write_row(row)
        assert writer.prepare_commit() == []
    finally:
        writer.close()


@pytest.mark.parametrize('native', _NATIVE)
@pytest.mark.parametrize('stream', [False, True])
def test_generated_primary_key_kinds_preserve_input_changelog(tmp_path, native, stream):
    import pyarrow.parquet as pq
    schema = pa.schema([('id', pa.int32()), ('value', pa.int32()), ('op', pa.string())])
    options = {'write.native.enabled': str(native).lower(), 'commit.native.enabled': 'false',
               'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
               'bucket': '1', 'rowkind.field': 'op', 'changelog-producer': 'input'}
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, primary_keys=['id'], options=options), False)
    table = catalog.get_table('default.t')
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    values = [dict(id=key, value=value, op=kind) for key, value, kind in [
        (1, 10, '+I'), (1, 20, '-D'), (2, 25, '-U'), (2, 30, '+U')]]
    try:
        writer.write_arrow(pa.Table.from_pylist(values, schema=schema))
        messages = writer.prepare_commit(1) if stream else writer.prepare_commit()
        logs = [file for message in messages for file in message.changelog_files]
        assert len(logs) == 1
        assert logs[0].row_count == 4
        assert logs[0].delete_row_count == 2
        from pathlib import Path
        path = next(Path(tmp_path).rglob(logs[0].file_name))
        events = pq.read_table(path).to_pylist()
        assert [(row['value'], row['_VALUE_KIND']) for row in events] == [(10, 0), (20, 3), (25, 1), (30, 2)]
        if stream:
            commit.commit(messages, 1)
        else:
            commit.commit(messages)
    finally:
        writer.close()
        commit.close()
    assert _read(table) == [values[-1]]
    assert table.snapshot_manager().get_latest_snapshot().changelog_record_count == 4


@pytest.mark.parametrize('native', _NATIVE)
@pytest.mark.parametrize('input_', ['row', 'pandas'])
@pytest.mark.parametrize('write_cols', [['id'], ['id', 'pt']])
def test_append_write_type_retains_event_input_before_physical_projection(tmp_path, native, input_, write_cols):
    table, schema = _table(tmp_path, native, **{'ignore-delete': 'true'})
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    writer.with_write_type(write_cols)
    values = [dict(id=1, op='-D', pt='bad'), dict(id=2, op='+I', pt='keep')]
    try:
        _write(writer, table, schema, values, input_, True)
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    assert _read(table) == [dict(id=2, op=None, pt='keep')]


@pytest.mark.parametrize('native', _NATIVE)
def test_pandas_partial_event_transport_does_not_convert_unwritten_blob(tmp_path, native, monkeypatch):
    import pandas as pd
    from pypaimon.table.row.blob import BlobData
    schema = pa.schema([('id', pa.int32()), ('op', pa.string()), ('payload', pa.large_binary())])
    options = {'write.native.enabled': str(native).lower(), 'commit.native.enabled': 'false',
               'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
               'rowkind.field': 'op', 'ignore-delete': 'true',
               'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options=options), False)
    table = catalog.get_table('default.t')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    writer.with_write_type(['id'])
    blob = BlobData(b'unwritten')

    def unexpected(*args):
        raise AssertionError('Unwritten Blob payload was opened')

    monkeypatch.setattr(blob, 'to_data', unexpected)
    monkeypatch.setattr(blob, 'new_input_stream', unexpected)
    source = pd.DataFrame({'id': [1, 2], 'op': ['-D', '+I'], 'payload': [blob, blob]})
    try:
        writer.write_pandas(source)
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    assert source['payload'].iloc[0] is blob
    assert _read(table) == [dict(id=2, op=None, payload=None)]


def _pandas_subset_table(tmp_path, native):
    schema = pa.schema([('id', pa.int32()), ('payload', pa.int32()), ('op', pa.string()), ('pt', pa.string())])
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, partition_keys=['pt'], options={
        'write.native.enabled': str(native).lower(), 'commit.native.enabled': 'false',
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false', 'rowkind.field': 'op',
    }), False)
    return catalog.get_table('default.t')


@pytest.mark.parametrize('native', _NATIVE)
def test_pandas_partial_write_requires_every_selected_input(tmp_path, native):
    import pandas as pd
    table = _pandas_subset_table(tmp_path, native)
    writer = table.new_batch_write_builder().new_write()
    writer.with_write_type(['payload'])
    try:
        with pytest.raises(KeyError, match='payload'):
            writer.write_pandas(pd.DataFrame({'id': [1], 'op': ['+I'], 'pt': ['p']}))
        assert writer.prepare_commit() == []
    finally:
        writer.close()


@pytest.mark.parametrize('native', _NATIVE)
def test_pandas_partial_write_keeps_selected_named_index(tmp_path, native):
    import pandas as pd
    table = _pandas_subset_table(tmp_path, native)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    writer.with_write_type(['payload'])
    source = pd.DataFrame({'id': [1], 'payload': [42], 'op': ['+I'], 'pt': ['p']}).set_index('payload')
    try:
        writer.write_pandas(source)
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    assert _read(table) == [dict(id=None, payload=42, op=None, pt='p')]
    assert source.index.tolist() == [42]


@pytest.mark.parametrize('columns', [['payload', 'id'], []])
def test_partial_batch_projection_uses_columns_and_preserves_schema_metadata(columns):
    from unittest.mock import Mock
    from pypaimon.execution.python.write.file_store_write import FileStoreWrite
    schema = pa.schema([pa.field('id', pa.int32()),
                        pa.field('payload', pa.string(), metadata={'role': 'value'})],
                       metadata={'source': 'batch'})
    batch = pa.RecordBatch.from_pylist([dict(id=1, payload='kept')], schema=schema)

    class ColumnBatch:
        # Arrow 6 exposes column access, but has no RecordBatch.select API.
        schema = batch.schema
        num_rows = batch.num_rows

        def column(self, index):
            return batch.column(index)

    writer = object.__new__(FileStoreWrite)
    writer.table = Mock(field_names=schema.names)
    writer.write_cols = columns
    writer.data_writers = {}
    writer._write_columns_by_bucket = {}
    writer._runtime_total_buckets = {}
    writer.options = object()
    target = Mock()
    writer._create_data_writer = Mock(return_value=target)
    writer.write((), 0, ColumnBatch())
    actual = target.write.call_args[0][0]
    assert actual.num_rows == batch.num_rows
    assert actual.to_pydict() == {name: batch.column(name).to_pylist() for name in columns}
    assert actual.schema.metadata == schema.metadata
    if 'payload' in columns:
        assert actual.schema.field('payload').metadata == schema.field('payload').metadata
