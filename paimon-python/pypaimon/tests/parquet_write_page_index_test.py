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

from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.table.format.format_table import Format, FormatTable
from pypaimon.write.table_update_by_row_id import _RowIdUpdateFileWriter


OPTION = 'parquet.write-page-index.enabled'
MODES = ['buffered', 'changelog', 'shared_shredding', 'row_id', 'format_table']
HAS_PAGE_INDEX = int(pa.__version__.split('.')[0]) >= 13


def _data():
    return pa.Table.from_pydict({
        'id': [1, 2, 3],
        'items': [[1, None], [], None],
        'metrics': [[('a', 10), ('b', None)], [], None],
        'info': [{'score': 5}, None, {'score': None}],
    }, schema=pa.schema([
        pa.field('id', pa.int32(), nullable=False),
        ('items', pa.list_(pa.int32())),
        ('metrics', pa.map_(pa.string(), pa.int64())),
        ('info', pa.struct([('score', pa.int32())])),
    ]))


def _table(tmp_path, mode, setting, file_format='parquet'):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    options = {'file.format': file_format}
    if setting is not None:
        options[OPTION] = setting
    if mode == 'shared_shredding':
        options['fields.metrics.map.storage-layout'] = 'shared-shredding'
        options['fields.metrics.map.shared-shredding.max-columns'] = '2'
    if mode == 'changelog':
        options.update({'bucket': '1', 'changelog-producer': 'input'})
    catalog.create_table('default.data', Schema.from_pyarrow_schema(
        _data().schema, options=options,
        primary_keys=['id'] if mode == 'changelog' else []), False)
    table = catalog.get_table('default.data')
    if mode == 'format_table':
        table = FormatTable(table.file_io, table.identifier, table.table_schema,
                            str(tmp_path / 'format'), Format.parse(file_format))
    return table


def _write(table, mode, data):
    if mode == 'row_id':
        writer = _RowIdUpdateFileWriter(table, (), data.column_names)
        writer.write_batches(iter(data.to_batches()))
        return
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    commit = builder.new_commit()
    try:
        writer.write_arrow(data)
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


@pytest.mark.parametrize('mode', MODES)
@pytest.mark.parametrize('setting', [None, 'false', 'true'])
def test_written_indexes_and_round_trip(tmp_path, mode, setting):
    if setting == 'true' and not HAS_PAGE_INDEX:
        pytest.skip('Writing page indexes requires PyArrow >= 13')
    table = _table(tmp_path, mode, setting)
    data = _data()
    _write(table, mode, data)
    paths = list(tmp_path.rglob('*.parquet'))
    assert len(paths) == (2 if mode == 'changelog' else 1)
    for path in paths:
        metadata = pq.read_metadata(str(path))
        if HAS_PAGE_INDEX:
            for i in range(metadata.num_row_groups):
                for j in range(metadata.num_columns):
                    column = metadata.row_group(i).column(j)
                    assert column.has_column_index == (setting != 'false')
                    assert column.has_offset_index == (setting != 'false')
        if mode != 'shared_shredding':
            actual = pq.read_table(str(path)).select(data.column_names)
            assert actual.to_pydict() == data.to_pydict()
    if mode != 'row_id':
        reader = table.new_read_builder()
        actual = reader.new_read().to_arrow(reader.new_scan().plan().splits())
        assert actual.to_pydict() == data.to_pydict()
    if mode not in ('row_id', 'format_table'):
        reader = table.new_read_builder()
        reader.with_filter(reader.new_predicate_builder().greater_or_equal('id', 2))
        reader.with_projection(['id', 'items'])
        actual = reader.new_read().to_arrow(reader.new_scan().plan().splits())
        assert actual.to_pydict() == data.slice(1).select(['id', 'items']).to_pydict()


@pytest.mark.parametrize('mode', MODES)
@pytest.mark.python_write
def test_unsupported_arrow_rejected_before_output(tmp_path, mode):
    table = _table(tmp_path, mode, 'true')
    with patch.object(pa, '__version__', '12.0.1'), \
            patch.object(table.file_io, 'new_output_stream',
                         wraps=table.file_io.new_output_stream) as open_stream, \
            patch.object(table.file_io, 'write_parquet',
                         wraps=table.file_io.write_parquet) as write_parquet:
        with pytest.raises(ValueError, match=r'parquet.write-page-index.enabled.*PyArrow >= 13'):
            _write(table, mode, _data())
        open_stream.assert_not_called()
        write_parquet.assert_not_called()
    assert not list(tmp_path.rglob('*.parquet'))


@pytest.mark.parametrize('setting', [None, 'false'])
@pytest.mark.parametrize('arrow_version', ['6.0.1', '12.0.1'])
@pytest.mark.python_write
def test_legacy_arrow_omits_new_argument(tmp_path, setting, arrow_version):
    table = _table(tmp_path, 'buffered', setting)
    original = table.file_io.write_parquet
    with patch.object(pa, '__version__', arrow_version), \
            patch.object(table.file_io, 'write_parquet', wraps=original) as write:
        _write(table, 'buffered', _data())
    assert write.called
    assert 'write_page_index' not in write.call_args[1]


@pytest.mark.python_write
def test_explicit_false_passed_to_supported_arrow(tmp_path):
    if not HAS_PAGE_INDEX:
        pytest.skip('Writing page indexes requires PyArrow >= 13')
    table = _table(tmp_path, 'buffered', 'false')
    with patch.object(table.file_io, 'write_parquet', wraps=table.file_io.write_parquet) as write:
        _write(table, 'buffered', _data())
    assert write.call_args[1]['write_page_index'] is False


@pytest.mark.parametrize('mode', ['buffered', 'format_table'])
def test_other_formats_ignore_parquet_option(tmp_path, mode):
    pytest.importorskip('pyarrow.orc')
    table = _table(tmp_path, mode, 'true', file_format='orc')
    with patch.object(pa, '__version__', '12.0.1'):
        _write(table, mode, _data())
    assert list(tmp_path.rglob('*.orc'))


def test_parquet_changelog_with_orc_data(tmp_path):
    pytest.importorskip('pyarrow.orc')
    if not HAS_PAGE_INDEX:
        pytest.skip('Writing page indexes requires PyArrow >= 13')
    table = _table(tmp_path, 'changelog', 'true', file_format='orc')
    table = table.copy({'changelog-file.format': 'parquet'})
    _write(table, 'changelog', _data())
    assert len(list(tmp_path.rglob('*.orc'))) == 1
    paths = list(tmp_path.rglob('*.parquet'))
    assert len(paths) == 1
    column = pq.read_metadata(str(paths[0])).row_group(0).column(0)
    assert column.has_column_index
    assert column.has_offset_index


def test_format_overwrite_checks_support_before_deleting(tmp_path):
    table = _table(tmp_path, 'format_table', None)
    _write(table, 'format_table', _data())
    paths = {path: path.read_bytes() for path in tmp_path.rglob('*.parquet')}
    table.options()[OPTION] = 'true'
    from pypaimon.table.format.format_table_write import FormatTableWrite
    with patch.object(pa, '__version__', '12.0.1'):
        with pytest.raises(ValueError, match='PyArrow >= 13'):
            FormatTableWrite(table, overwrite=True).write_arrow(_data())
    assert {path: path.read_bytes() for path in tmp_path.rglob('*.parquet')} == paths


def _vector_table(tmp_path, file_format, setting):
    data = pa.Table.from_pydict({
        'id': [1, 2],
        'embedding': [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]],
    }, schema=pa.schema([
        ('id', pa.int64()),
        ('embedding', pa.list_(pa.float32(), 3)),
    ]))
    options = {
        'file.format': file_format,
        'vector.file.format': 'parquet',
        'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true',
    }
    if setting is not None:
        options[OPTION] = setting
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.vectors', Schema.from_pyarrow_schema(
        data.schema, options=options), False)
    return catalog.get_table('default.vectors'), data


@pytest.mark.parametrize('file_format', ['parquet', 'orc'])
@pytest.mark.parametrize('setting', [None, 'false', 'true'])
def test_vector_parquet_indexes_and_round_trip(tmp_path, file_format, setting):
    if setting == 'true' and not HAS_PAGE_INDEX:
        pytest.skip('Writing page indexes requires PyArrow >= 13')
    if file_format == 'orc':
        pytest.importorskip('pyarrow.orc')
    table, data = _vector_table(tmp_path, file_format, setting)
    _write(table, 'buffered', data)
    paths = list(tmp_path.rglob('*.parquet'))
    assert len(list(tmp_path.rglob('*.vector.parquet'))) == 1
    assert len(paths) == (2 if file_format == 'parquet' else 1)
    for path in paths:
        metadata = pq.read_metadata(str(path))
        if HAS_PAGE_INDEX:
            for i in range(metadata.num_row_groups):
                for j in range(metadata.num_columns):
                    column = metadata.row_group(i).column(j)
                    assert column.has_column_index == (setting != 'false')
                    assert column.has_offset_index == (setting != 'false')
    reader = table.new_read_builder()
    actual = reader.new_read().to_arrow(reader.new_scan().plan().splits())
    assert actual.to_pydict() == data.to_pydict()


def test_vector_parquet_rejects_unsupported_arrow_before_output(tmp_path):
    table, data = _vector_table(tmp_path, 'orc', 'true')
    with patch.object(pa, '__version__', '12.0.1'), \
            patch.object(table.file_io, 'write_parquet',
                         wraps=table.file_io.write_parquet) as write_parquet, \
            patch.object(table.file_io, 'write_orc',
                         wraps=table.file_io.write_orc) as write_orc:
        with pytest.raises(ValueError, match=r'parquet.write-page-index.enabled.*PyArrow >= 13'):
            _write(table, 'buffered', data)
        write_parquet.assert_not_called()
        write_orc.assert_not_called()
    assert not list(tmp_path.rglob('*.parquet'))
    assert not list(tmp_path.rglob('*.orc'))
