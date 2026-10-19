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

"""Deduplicate tombstones persist nullable values without changing logical NOT NULL."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.row_kind import RowKind
from pypaimon.write.native_write import NativeTableWrite


pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
_NATIVE = [False, pytest.param(True, marks=[pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed')])]


def _table(tmp_path, native=False, **options):
    schema = pa.schema([pa.field('id', pa.int32(), nullable=False),
                        pa.field('seq', pa.int32(), nullable=False),
                        pa.field('value', pa.string(), nullable=False),
                        pa.field('op', pa.string())])
    config = {'bucket': '1', 'sequence.field': 'seq', 'ignore-delete': 'false',
              'rowkind.field': 'op',
              'write.native.enabled': str(native).lower(), 'commit.native.enabled': 'false',
              'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'}
    config.update(options)
    config = {key: value for key, value in config.items() if value is not None}
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'], options=config), False)
    return catalog.get_table('default.t'), schema


def _write(writer, table, schema, rows, input_, bucket=None):
    if input_ == 'row':
        for values in rows:
            row = GenericRow([values[field.name] for field in table.fields], table.fields,
                             RowKind.from_string(values['op']))
            writer.write_row(row)
    elif input_ == 'pandas':
        import pandas as pd
        writer.write_pandas(pd.DataFrame.from_records(rows, columns=schema.names))
    else:
        if input_ == 'large_string':
            schema = pa.schema([field.with_type(pa.large_string()) if pa.types.is_string(field.type)
                                else field for field in schema])
        data = pa.Table.from_pylist(rows, schema=schema)
        writer.write_arrow(data.to_batches()[0] if input_ == 'batch' else data, bucket)


def _read(table, predicate=None):
    reader = table.new_read_builder()
    if predicate is not None:
        reader.with_filter(predicate)
    return reader.new_read().to_arrow(reader.new_scan().plan().splits()).to_pylist()


@pytest.mark.parametrize('native', _NATIVE)
@pytest.mark.parametrize('input_', ['row', 'batch', 'table', 'large_string', 'pandas'])
@pytest.mark.parametrize('stream', [False, True])
def test_retained_delete_persists_only_required_values(tmp_path, native, input_, stream):
    table, schema = _table(tmp_path, native)
    for identifier, (seq, value, kind, expected) in enumerate([
            (10, 'before', '+I', [dict(id=1, seq=10, value='before', op='+I')]),
            (9, None, '-D', [dict(id=1, seq=10, value='before', op='+I')]),
            (11, None, '-D', [])], 1):
        builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            assert isinstance(writer, NativeTableWrite) == native
            _write(writer, table, schema, [dict(id=1, seq=seq, value=value, op=kind)], input_)
            messages = writer.prepare_commit(identifier) if stream else writer.prepare_commit()
            # Data and INPUT changelog writers share the relaxed value type.
            for path in Path(tmp_path).rglob('*.parquet'):
                physical = pq.read_schema(path)
                assert physical.field('value').nullable
                assert not physical.field('id').nullable
                assert not physical.field('seq').nullable
            commit.commit(messages, identifier) if stream else commit.commit(messages)
            assert _read(table) == expected
            predicate = table.new_read_builder().new_predicate_builder().equal('value', 'before')
            assert _read(table, predicate) == expected
            assert not table.table_schema.fields[2].type.nullable
            if native:
                assert writer._python_writer is None
        finally:
            writer.close()
            commit.close()


@pytest.mark.parametrize('native', _NATIVE)
@pytest.mark.parametrize('input_', ['row', 'batch', 'large_string', 'pandas'])
@pytest.mark.parametrize('kind,field', [
    ('-D', 'id'), ('-D', 'seq'), ('-U', 'value'), ('+U', 'value'), ('+I', 'value')])
def test_retained_events_keep_logical_not_null_checks(tmp_path, native, kind, field, input_):
    table, schema = _table(tmp_path, native)
    writer = table.new_batch_write_builder().new_write()
    row = dict(id=1, seq=1, value='ok', op=kind)
    row[field] = None
    try:
        with pytest.raises((ValueError, RuntimeError), match='non-null column|non-nullable'):
            _write(writer, table, schema, [row], input_)
        assert not list(Path(tmp_path).rglob('*.parquet'))
    finally:
        writer.abort()


@pytest.mark.parametrize('native', _NATIVE)
def test_internal_row_delete_preserves_input_changelog_nulls(tmp_path, native):
    table, schema = _table(tmp_path, native, **{'rowkind.field': None, 'changelog-producer': 'input'})
    for seq, value, kind in [(1, 'before', '+I'), (2, None, '-D')]:
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            _write(writer, table, schema, [dict(id=1, seq=seq, value=value, op=kind)], 'row')
            messages = writer.prepare_commit()
            files = [file for message in messages for file in message.changelog_files]
            assert len(files) == 1
            path = next(Path(tmp_path).rglob(files[0].file_name))
            assert pq.read_table(path, columns=['_VALUE_KIND', 'value']).to_pydict() == {
                '_VALUE_KIND': [RowKind.from_string(kind).value], 'value': [value]}
            commit.commit(messages)
        finally:
            writer.close()
            commit.close()
    assert _read(table) == []


@pytest.mark.parametrize('native', _NATIVE)
@pytest.mark.parametrize('engine', ['partial-update', 'aggregation', 'first-row'])
def test_delete_null_exception_does_not_relax_other_write_modes(tmp_path, native, engine):
    options = {'merge-engine': engine}
    if engine == 'first-row':
        options['sequence.field'] = None
    table, schema = _table(tmp_path, native, **options)
    writer = table.new_batch_write_builder().new_write()
    try:
        with pytest.raises((ValueError, RuntimeError), match=r'non-null column\(value\)'):
            _write(writer, table, schema, [dict(id=1, seq=1, value=None, op='-D')], 'table')
        assert not list(Path(tmp_path).rglob('*.parquet'))
    finally:
        writer.abort()
