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

"""Public format writes preserve behavior while backend I/O stays in execution."""

import inspect
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
import subprocess
import sys
import time as system_time
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.table.format import Format, FormatTable, FormatTableCommit, FormatTableWrite


def _table(tmp_path, options=None, pa_schema=None, partition_keys=None):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', True)
    schema = Schema.from_pyarrow_schema(
        pa_schema or pa.schema([('id', pa.int32()), ('pt', pa.string())]),
        partition_keys=partition_keys or ['pt'], options=options or {})
    catalog.create_table('default.data', schema, False)
    table = catalog.get_table('default.data')
    return FormatTable(table.file_io, table.identifier, table.table_schema,
                       str(tmp_path / 'format'), Format.PARQUET)


def _rows(paths):
    return sorted(
        row['id']
        for path in paths
        for row in pq.ParquetFile(path).read().to_pylist())


@pytest.mark.parametrize('format_option', ['file.format', 'vector.file.format'])
def test_removed_lance_format_cannot_publish_data(tmp_path, format_option):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('embedding', pa.list_(pa.float32(), 2))])
    options = {
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        format_option: 'lance',
    }
    table = None
    writer = None
    try:
        with pytest.raises(ValueError, match='[Uu]nsupported.*lance'):
            catalog.create_table('default.removed', Schema.from_pyarrow_schema(schema, options=options), False)
            table = catalog.get_table('default.removed')
            writer = table.new_batch_write_builder().new_write()
            writer.write_arrow(pa.Table.from_pylist([{'id': 1, 'embedding': [1.0, 2.0]}], schema=schema))
            writer.prepare_commit()
    finally:
        if writer is not None:
            writer.abort()
            writer.close()
    if table is not None:
        assert table.snapshot_manager().get_latest_snapshot() is None


def test_public_format_import_does_not_import_python_execution():
    code = """
import sys
from pypaimon.table.format import FormatTableWrite, FormatTableCommit
assert FormatTableWrite.__module__ == 'pypaimon.table.format.format_table_write'
assert FormatTableCommit.__module__ == 'pypaimon.table.format.format_table_commit'
for name in ('format_table_write', 'format_table_commit'):
    assert 'pypaimon.execution.python.write.' + name not in sys.modules
"""
    subprocess.run([sys.executable, '-c', code], check=True)


def test_format_write_public_types_and_partitioned_rolling(tmp_path):
    table = _table(tmp_path, {'target-file-row-num': '2'})
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    commit = builder.new_commit()
    assert type(writer) is FormatTableWrite
    assert type(commit) is FormatTableCommit
    assert writer.table is table
    assert commit.table is table

    writer.write_arrow(pa.table({'id': [1, 2, 3, 4, 5], 'pt': ['a', 'a', 'b', 'b', 'b']}))
    messages = writer.prepare_commit()
    paths = messages[0].written_paths
    assert sorted(pq.ParquetFile(path).metadata.num_rows for path in paths) == [1, 2, 2]
    assert _rows(paths) == [1, 2, 3, 4, 5]
    assert writer.prepare_commit() == messages
    commit.commit(messages)
    with pytest.raises(RuntimeError, match='only one commit'):
        commit.commit(messages)
    writer.close()
    commit.close()


@pytest.mark.parametrize('only_value', [False, True])
@pytest.mark.parametrize('legacy', [False, True])
def test_format_partition_paths_round_trip(tmp_path, only_value, legacy):
    schema = pa.schema([('pt/key', pa.string()), ('id', pa.int32()), ('flag', pa.bool_()),
                        ('day', pa.date32()), ('amount', pa.decimal128(10, 9))])
    keys = ['pt/key', 'flag', 'day', 'amount']
    table = _table(tmp_path, {'partition.legacy-name': str(legacy).lower(),
                              'partition.default-name': '__NULL/VALUE__',
                              'format-table.partition-path-only-value': str(only_value).lower()}, schema, keys)
    data = pa.Table.from_pydict({'pt/key': ['a/b=% 中', None], 'id': [1, 2], 'flag': [True, False],
                                'day': [date(1970, 1, 2)] * 2, 'amount': [Decimal('0E-9')] * 2}, schema=schema)
    writer = table.new_batch_write_builder().new_write()
    try:
        writer.write_arrow(data)
        paths = writer.prepare_commit()[0].written_paths
    finally:
        writer.close()
    for path, string, flag in zip(paths, ['a%2Fb%3D%25 中', '__NULL%2FVALUE__'], ['true', 'false']):
        values = [string, flag, '1' if legacy else '1970-01-02', '0.000000000']
        parts = values if only_value else [k + '=' + v for k, v in zip(['pt%2Fkey'] + keys[1:], values)]
        assert Path(path).parent.relative_to(table.location()).as_posix() == '/'.join(parts)
        assert pq.ParquetFile(path).schema_arrow.names == ['id']
    reader = table.new_read_builder()
    result = reader.new_read().to_arrow(reader.new_scan().plan().splits())
    assert sorted(result.to_pylist(), key=lambda row: row['id']) == data.to_pylist()


@pytest.mark.parametrize('root_name', ['with space', 'with%20space'])
def test_format_file_uri_root_preserves_partition_escapes(tmp_path, root_name):
    table = _table(tmp_path)
    root = tmp_path / root_name / 'table'
    table._location = root.as_uri()
    directory = root / 'pt=a%2Fb'
    directory.mkdir(parents=True)
    pq.write_table(pa.table({'id': [1]}, schema=pa.schema([('id', pa.int32())])), str(directory / 'old.parquet'))
    # Directories of another key, or without '=', are not partitions of this table and are skipped.
    for row_id, name in [(98, 'other=c'), (99, 'nokey')]:
        (root / name).mkdir()
        pq.write_table(pa.table({'id': [row_id]}, schema=pa.schema([('id', pa.int32())])),
                       str(root / name / 'old.parquet'))
    reader = table.new_read_builder()
    assert reader.new_read().to_arrow(reader.new_scan().plan().splits()).to_pylist() == [{'id': 1, 'pt': 'a/b'}]
    writer = table.new_batch_write_builder().new_write()
    try:
        writer.write_pandas(pd.DataFrame({'id': [2], 'pt': ['a/b']}))
    finally:
        writer.close()
    assert len(list(directory.glob('*.parquet'))) == 2
    result = reader.new_read().to_arrow(reader.new_scan().plan().splits())
    assert sorted(result.to_pylist(), key=lambda row: row['id']) == [{'id': 1, 'pt': 'a/b'}, {'id': 2, 'pt': 'a/b'}]


@pytest.mark.parametrize('data_type,value,text', [
    (pa.bool_(), True, 'TRUE'), (pa.date32(), date(1970, 1, 2), '1970-1-2'),
    (pa.timestamp('us'), datetime(2024, 1, 2, 12, 34), '2024-01-02 12:34:00'),
    (pa.time32('ms'), time(12, 34), '12:34:00'), (pa.binary(), b'a/b', 'a/b'),
])
def test_format_partition_equality_filters(tmp_path, data_type, value, text):
    schema = pa.schema([('id', pa.int32()), ('pt', data_type)])
    table = _table(tmp_path, pa_schema=schema)
    writer = table.new_batch_write_builder().new_write()
    try:
        writer.write_arrow(pa.Table.from_pydict({'id': [1, 2], 'pt': [value, None]}, schema=schema))
    finally:
        writer.close()
    predicate = table.new_read_builder().new_predicate_builder().equal('pt', value)
    for reader in [table.new_read_builder().with_filter(predicate),
                   table.new_read_builder().with_partition_filter({'pt': text})]:
        result = reader.new_read().to_arrow(reader.new_scan().plan().splits())
        assert result['id'].to_pylist() == [1]


@pytest.mark.skipif(not hasattr(system_time, 'tzset'), reason='requires POSIX timezone support')
@pytest.mark.parametrize('legacy', [False, True])
@pytest.mark.parametrize('unit', ['us', 'ns'])
def test_format_ltz_partitions_in_local_timezone(tmp_path, legacy, unit):
    schema = pa.schema([('id', pa.int32()), ('pt', pa.timestamp(unit, tz='UTC'))])
    table = _table(tmp_path, {'partition.legacy-name': str(legacy).lower()}, schema)
    data = pa.Table.from_pydict({'id': [1, 2], 'pt': [
        pd.Timestamp('2024-01-02T12:00:00.123456789Z'), pd.Timestamp('2024-07-02T12:00:00.123456789Z')]}, schema)
    try:
        with patch.dict('os.environ', {'TZ': 'America/New_York'}):
            system_time.tzset()
            writer = table.new_batch_write_builder().new_write()
            try:
                writer.write_arrow(data)
            finally:
                writer.close()
            reader = table.new_read_builder()
            splits = reader.new_scan().plan().splits()
            assert sorted(reader.new_read().to_arrow(splits).to_pylist(), key=lambda row: row['id']) == data.to_pylist()
            assert sorted(pa.Table.from_batches(list(reader.new_read().to_iterator(splits))).to_pylist(),
                          key=lambda row: row['id']) == data.to_pylist()
            value = data['pt'][0].as_py()
            predicate = reader.new_predicate_builder().equal('pt', value)
            local = pd.Timestamp(value).tz_convert('America/New_York').tz_localize(None).isoformat(sep=' ')
            for filtered in [table.new_read_builder().with_filter(predicate),
                             table.new_read_builder().with_partition_filter({'pt': local})]:
                assert filtered.new_read().to_arrow(filtered.new_scan().plan().splits())['id'].to_pylist() == [1]
    finally:
        system_time.tzset()


@pytest.mark.parametrize('data_type,value,literal', [
    (pa.int32(), 1, 1.5), (pa.int32(), 1, None), (pa.float32(), 0.1, 0.1),
    (pa.decimal128(4, 2), Decimal('1.00'), '99999'),
    (pa.timestamp('us'), datetime(2024, 1, 2), pd.Timestamp('2024-01-02 00:00:00.000000001')),
])
def test_format_equality_does_not_narrow_literals(tmp_path, data_type, value, literal):
    schema = pa.schema([('id', pa.int32()), ('pt', data_type)])
    table = _table(tmp_path, pa_schema=schema)
    writer = table.new_batch_write_builder().new_write()
    try:
        writer.write_arrow(pa.Table.from_pydict({'id': [1, 2], 'pt': [value, None]}, schema))
    finally:
        writer.close()
    reader = table.new_read_builder()
    predicate = reader.new_predicate_builder().equal('pt', literal)
    assert reader.with_filter(predicate).new_scan().plan().splits() == []
    reader = table.new_read_builder().with_partition_filter({'pt': None})
    assert reader.new_read().to_arrow(reader.new_scan().plan().splits())['id'].to_pylist() == [2]


@pytest.mark.parametrize('data_type', [pa.float32(), pa.float64()])
def test_format_filter_skips_unparseable_partition_names(tmp_path, data_type):
    schema = pa.schema([('id', pa.int32()), ('pt', data_type)])
    table = _table(tmp_path, pa_schema=schema)
    for i, name in enumerate(['1.0', 'inf', '-inf', 'nan']):
        directory = Path(table.location()) / ('pt=' + name)
        directory.mkdir(parents=True)
        pq.write_table(pa.Table.from_pydict({'id': [i], 'pt': [float(name)]}, schema), str(directory / 'old.parquet'))
    reader = table.new_read_builder()
    reader.with_filter(reader.new_predicate_builder().equal('pt', 1.0))
    assert reader.new_read().to_arrow(reader.new_scan().plan().splits())['id'].to_pylist() == [0]
    with pytest.raises(ValueError, match='Invalid floating partition value'):
        table.new_read_builder().with_partition_filter({'pt': 'inf'})


@pytest.mark.parametrize('file_format', [Format.PARQUET, Format.CSV, Format.JSON])
@pytest.mark.parametrize('nullable', [False, True])
def test_format_reads_old_and_new_partitioned_files(tmp_path, file_format, nullable):
    schema = pa.schema([pa.field('pt', pa.int32(), nullable), ('id', pa.int64())])
    table = _table(tmp_path, pa_schema=schema)
    table._format = file_format
    directory = Path(table.location()) / 'pt=1'
    directory.mkdir(parents=True)
    # The old file's own value differs from its directory, so a reader that takes the directory value fails.
    old = pa.Table.from_pydict({'pt': [5], 'id': [1]}, schema=pa.schema([
        pa.field('pt', pa.int64(), not nullable), ('id', pa.int64())]))
    path = str(directory / ('old.' + file_format.value))
    if file_format == Format.PARQUET:
        pq.write_table(old, path)
    elif file_format == Format.CSV:
        import pyarrow.csv as csv
        csv.write_csv(old, path)
    else:
        Path(path).write_text('{"pt":5,"id":1}\n')
    writer = table.new_batch_write_builder().new_write()
    try:
        writer.write_arrow(pa.Table.from_pydict({'pt': [1], 'id': [2]}, schema=schema))
    finally:
        writer.close()
    reader = table.new_read_builder()
    result = reader.new_read().to_arrow(reader.new_scan().plan().splits())
    assert result.schema == schema
    assert sorted(result.to_pylist(), key=lambda row: row['id']) == [{'pt': 5, 'id': 1}, {'pt': 1, 'id': 2}]


@pytest.mark.parametrize('projection', [['id'], ['id', 'pt']])
def test_format_reads_retained_csv_time_partition(tmp_path, projection):
    schema = pa.schema([('id', pa.int64()), ('pt', pa.time32('ms'))])
    table = _table(tmp_path, pa_schema=schema)
    table._format = Format.CSV
    directory = Path(table.location()) / 'pt=45296789'
    directory.mkdir(parents=True)
    (directory / 'old.csv').write_text('id,pt\n1,12:34:56.789\n')
    reader = table.new_read_builder().with_projection(projection)
    result = reader.new_read().to_arrow(reader.new_scan().plan().splits())
    expected = {'id': [1], 'pt': [time(12, 34, 56, 789000)]}
    assert result.to_pydict() == {key: expected[key] for key in projection}
    assert result.schema == pa.schema([schema.field(key) for key in projection])


@pytest.mark.parametrize('data_type,value,spec', [
    (pa.bool_(), True, 'true'), (pa.bool_(), True, True), (pa.decimal128(4, 2), Decimal('1.00'), '1.0'),
    (pa.float64(), 1.0, '1'), (pa.date32(), date(2024, 1, 2), '2024-01-02'),
    (pa.float64(), float('nan'), 'NaN'),
    (pa.string(), None, '__DEFAULT_PARTITION__'), (pa.string(), '  ', '__DEFAULT_PARTITION__'),
    (pa.string(), '', ''),
])
def test_format_static_overwrite_matches_typed_partition_values(tmp_path, data_type, value, spec):
    schema = pa.schema([('id', pa.int32()), ('pt', data_type)])
    table = _table(tmp_path, pa_schema=schema)
    for row_id, static in [(1, None), (2, spec)]:
        builder = table.new_batch_write_builder()
        if static is not None:
            builder = builder.overwrite({'pt': static})
        writer = builder.new_write()
        commit = builder.new_commit()
        try:
            writer.write_arrow(pa.Table.from_pydict({'id': [row_id], 'pt': [value]}, schema=schema))
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()
    reader = table.new_read_builder()
    assert reader.new_read().to_arrow(reader.new_scan().plan().splits())['id'].to_pylist() == [2]


def _decimal_table_with_null_partition(tmp_path):
    schema = pa.schema([('id', pa.int32()), ('pt', pa.decimal128(4, 2))])
    table = _table(tmp_path, pa_schema=schema)
    writer = table.new_batch_write_builder().new_write()
    try:
        writer.write_arrow(pa.Table.from_pydict({'id': [1, 2], 'pt': [None, Decimal('1.00')]}, schema=schema))
    finally:
        writer.close()
    return table


def test_format_static_overwrite_rejects_unconvertible_partition_value(tmp_path):
    # 99999 overflows DECIMAL(4, 2); it must not select the null partition and delete its files.
    table = _decimal_table_with_null_partition(tmp_path)
    with pytest.raises(ValueError, match="'99999' cannot be represented"):
        table.new_batch_write_builder().overwrite({'pt': '99999'}).new_write()
    reader = table.new_read_builder()
    assert sorted(reader.new_read().to_arrow(reader.new_scan().plan().splits())['id'].to_pylist()) == [1, 2]


def test_format_partition_filter_rejects_unconvertible_value(tmp_path):
    table = _decimal_table_with_null_partition(tmp_path)
    with pytest.raises(ValueError, match="'99999' cannot be represented"):
        table.new_read_builder().with_partition_filter({'pt': '99999'})
    for spec in ({'pt': None}, {'pt': '__DEFAULT_PARTITION__'}):
        reader = table.new_read_builder().with_partition_filter(spec)
        assert reader.new_read().to_arrow(reader.new_scan().plan().splits())['id'].to_pylist() == [1]


def test_format_static_overwrite_keeps_other_signed_zero_directory(tmp_path):
    # 0.0 and -0.0 are equal in Python but name different directories, as in Java.
    schema = pa.schema([('id', pa.int32()), ('pt', pa.float64())])
    table = _table(tmp_path, pa_schema=schema)
    for row_id, static in [(1, None), (2, '0.0')]:
        builder = table.new_batch_write_builder()
        if static is not None:
            builder = builder.overwrite({'pt': static})
        writer = builder.new_write()
        commit = builder.new_commit()
        try:
            writer.write_arrow(pa.Table.from_pydict({'id': [row_id], 'pt': [-0.0]}, schema=schema))
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()
    reader = table.new_read_builder()
    assert sorted(reader.new_read().to_arrow(reader.new_scan().plan().splits())['id'].to_pylist()) == [1, 2]


@pytest.mark.parametrize('value,message', [
    ('_hidden', 'cannot be written in a value-only partition'),
    ('.hidden', 'cannot be written in a value-only partition'),
    ('.', 'cannot be used as a partition path component'),
    ('..', 'cannot be used as a partition path component'),
])
def test_format_value_only_layout_rejects_hidden_values(tmp_path, value, message):
    table = _table(tmp_path, options={'format-table.partition-path-only-value': 'true'})
    writer = table.new_batch_write_builder().new_write()
    try:
        with pytest.raises(ValueError, match=message):
            writer.write_arrow(pa.Table.from_pydict(
                {'id': [1], 'pt': [value]}, schema=pa.schema([('id', pa.int32()), ('pt', pa.string())])))
    finally:
        writer.close()


@pytest.mark.parametrize('file_format', [Format.PARQUET, Format.ORC, Format.TEXT])
def test_format_write_rejects_table_with_only_partition_columns(tmp_path, file_format):
    table = _table(tmp_path, pa_schema=pa.schema([('pt', pa.string())]))
    table._format = file_format
    with pytest.raises(ValueError, match='at least one non-partition column'):
        table.new_batch_write_builder().new_write()


@pytest.mark.parametrize('file_format', [Format.CSV, Format.JSON])
def test_format_writes_table_with_only_partition_columns(tmp_path, file_format):
    # CSV and JSON files keep the partition columns of such a table, so its rows are not lost.
    schema = pa.schema([('pt', pa.string())])
    table = _table(tmp_path, pa_schema=schema)
    table._format = file_format
    writer = table.new_batch_write_builder().new_write()
    try:
        writer.write_arrow(pa.Table.from_pydict({'pt': ['a', 'a', 'b']}, schema=schema))
    finally:
        writer.close()
    reader = table.new_read_builder()
    result = reader.new_read().to_arrow(reader.new_scan().plan().splits())
    assert sorted(result['pt'].to_pylist()) == ['a', 'a', 'b']


def test_format_overwrite_and_explicit_abort_keep_other_files(tmp_path):
    table = _table(tmp_path)
    writer = table.new_batch_write_builder().new_write()
    writer.write_pandas(pd.DataFrame({'id': [1, 2], 'pt': ['a', 'b']}))
    old_paths = writer.prepare_commit()[0].written_paths
    writer.close()
    partition_a = Path(old_paths[0]).parent
    marker = partition_a / '_retained_metadata'
    marker.write_text('keep')

    builder = table.new_batch_write_builder().overwrite({'pt': 'a'})
    overwrite = builder.new_write()
    overwrite.write_arrow_batch(pa.record_batch([[3], ['a']], names=['id', 'pt']))
    overwrite.write_arrow_batch(pa.record_batch([[4], ['a']], names=['id', 'pt']))
    messages = overwrite.prepare_commit()
    assert marker.read_text() == 'keep'
    assert not Path(old_paths[0]).exists()
    assert Path(old_paths[1]).exists()
    assert _rows(messages[0].written_paths) == [3, 4]
    overwrite.close()

    commit = builder.new_commit()
    commit.abort(messages)
    assert all(not Path(path).exists() for path in messages[0].written_paths)
    assert marker.read_text() == 'keep'
    assert Path(old_paths[1]).exists()
    commit.close()


@pytest.mark.parametrize('value', ['0', '-1', str(2 ** 63)])
def test_format_writer_retains_eager_row_limit_validation(tmp_path, value):
    table = _table(tmp_path)
    table.options()['target-file-row-num'] = value
    with pytest.raises(ValueError, match='target-file-row-num'):
        FormatTableWrite(table)
    assert not Path(table.location()).exists()


def test_format_writer_checks_parquet_options_at_construction(tmp_path):
    table = _table(tmp_path, {'parquet.write-page-index.enabled': 'true'})
    with patch.object(pa, '__version__', '12.0.1'):
        with pytest.raises(ValueError, match='PyArrow >= 13'):
            FormatTableWrite(table, overwrite=True)
    assert not Path(table.location()).exists()


def test_format_public_method_parameter_defaults_are_unchanged():
    parameters = inspect.signature(FormatTableWrite.__init__).parameters
    assert list(parameters) == ['self', 'table', 'overwrite', 'static_partitions']
    assert parameters['table'].default is inspect.Parameter.empty
    assert parameters['overwrite'].default is False
    assert parameters['static_partitions'].default is None
    assert list(inspect.signature(FormatTableCommit.commit).parameters) == ['self', 'commit_messages']
    assert list(inspect.signature(FormatTableCommit.abort).parameters) == ['self', 'commit_messages']


@pytest.mark.parametrize('input_method', ['write_arrow', 'write_pandas'])
@pytest.mark.parametrize('override', ['subclass', 'instance'])
def test_format_batch_write_public_overrides_are_honored(tmp_path, input_method, override):
    table = _table(tmp_path)
    batches = []

    class ObservedWriter(FormatTableWrite):
        def write_arrow_batch(self, data):
            batches.append(data)
            return super().write_arrow_batch(data)

    if override == 'subclass':
        writer = ObservedWriter(table)
    else:
        writer = FormatTableWrite(table)
        original = writer.write_arrow_batch

        def observed(data):
            batches.append(data)
            return original(data)

        writer.write_arrow_batch = observed
    data = pa.table({'id': [1, 2], 'pt': ['a', 'b']})
    value = data if input_method == 'write_arrow' else data.to_pandas()
    getattr(writer, input_method)(value)
    assert len(batches) == 1
    assert batches[0].to_pydict() == data.to_pydict()
    assert _rows(writer.prepare_commit()[0].written_paths) == [1, 2]
    writer.close()


@pytest.mark.parametrize('file_format, content', [
    (Format.PARQUET, b'not a data file'), (Format.ORC, b'not a data file'), (Format.CSV, b'a,b\n1,2,3\n')])
def test_format_read_raises_on_unreadable_file(tmp_path, file_format, content):
    table = _table(tmp_path)
    table._format = file_format
    directory = Path(table.location()) / 'pt=a'
    directory.mkdir(parents=True)
    (directory / ('bad.' + file_format.value)).write_bytes(content)
    reader = table.new_read_builder()
    with pytest.raises(RuntimeError, match='Failed to read .*bad'):
        reader.new_read().to_arrow(reader.new_scan().plan().splits())


@pytest.mark.parametrize('file_format, readable', [(Format.PARQUET, False), (Format.ORC, True), (Format.CSV, True)])
def test_format_reads_zero_byte_files_like_java(tmp_path, file_format, readable):
    table = _table(tmp_path)
    table._format = file_format
    directory = Path(table.location()) / 'pt=a'
    directory.mkdir(parents=True)
    (directory / ('empty.' + file_format.value)).write_bytes(b'')
    reader = table.new_read_builder()
    splits = reader.new_scan().plan().splits()
    if readable:
        assert reader.new_read().to_arrow(splits).num_rows == 0
    else:
        with pytest.raises(RuntimeError, match='Failed to read .*empty'):
            reader.new_read().to_arrow(splits)


def test_format_scan_raises_listing_errors(tmp_path):
    table = _table(tmp_path)
    with patch.object(type(table.file_io), 'list_status', side_effect=OSError('connection reset')):
        with pytest.raises(OSError, match='connection reset'):
            table.new_read_builder().new_scan().plan()


def test_format_scan_skips_directory_that_vanishes_after_listing(tmp_path):
    table = _table(tmp_path)
    for pt in ('a', 'b'):
        directory = Path(table.location()) / f'pt={pt}'
        directory.mkdir(parents=True)
        pq.write_table(pa.table({'id': [1]}), str(directory / 'data.parquet'))
    file_io_type = type(table.file_io)
    list_status = file_io_type.list_status

    def list_status_while_pt_b_vanishes(self, path):
        if path.rstrip('/').endswith('pt=b'):
            raise FileNotFoundError(path)
        return list_status(self, path)

    with patch.object(file_io_type, 'list_status', list_status_while_pt_b_vanishes):
        splits = table.new_read_builder().new_scan().plan().splits()
    assert [split.partition for split in splits] == [{'pt': 'a'}]
