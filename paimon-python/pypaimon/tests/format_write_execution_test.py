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
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path
import subprocess
import sys
import time as system_time
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.orc as pa_orc
import pyarrow.parquet as pq
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.python.write.format_table_write import PythonFormatTableWrite
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


@pytest.mark.skipif(not hasattr(system_time, 'tzset'), reason='requires POSIX timezone support')
@pytest.mark.parametrize('legacy', [False, True])
def test_format_static_overwrite_of_ltz_takes_local_time(tmp_path, legacy):
    # A static LTZ value is a local time, as a partition filter's is, also where legacy names keep UTC fields.
    schema = pa.schema([('id', pa.int32()), ('pt', pa.timestamp('us', tz='UTC'))])
    table = _table(tmp_path, {'partition.legacy-name': str(legacy).lower()}, schema)
    value = datetime(2024, 1, 2, 12, tzinfo=timezone.utc)
    try:
        with patch.dict('os.environ', {'TZ': 'America/New_York'}):
            system_time.tzset()
            _write_rows(table, [{'id': 1, 'pt': value}], schema)
            assert [row[0] for row in _read_rows(table, {'pt': '2024-01-02 07:00:00'})] == [1]
            _write_rows(table, [{'id': 2, 'pt': value}], schema, overwrite={'pt': '2024-01-02 07:00:00'})
            assert [row[0] for row in _read_rows(table)] == [2]
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


def test_format_reads_float_partition_names_of_either_jdk(tmp_path):
    # Float.toString names 268873285632f 2.68873286E11 before JDK 19 and 2.6887329E11 since.
    schema = pa.schema([('id', pa.int32()), ('pt', pa.float32())])
    table = _table(tmp_path, pa_schema=schema)
    for i, name in enumerate(['2.68873286E11', '2.6887329E11', '1.0']):
        directory = Path(table.location()) / ('pt=' + name)
        directory.mkdir(parents=True)
        pq.write_table(pa.Table.from_pydict({'id': [i]}, pa.schema([('id', pa.int32())])),
                       str(directory / 'data.parquet'))

    def ids(reader):
        return sorted(reader.new_read().to_arrow(reader.new_scan().plan().splits())['id'].to_pylist())

    reader = table.new_read_builder()
    assert ids(reader) == [0, 1, 2]
    reader.with_filter(reader.new_predicate_builder().equal('pt', 268873285632.0))
    assert ids(reader) == [0, 1]
    assert ids(table.new_read_builder().with_partition_filter({'pt': '2.6887329E11'})) == [0, 1]


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


@pytest.mark.parametrize('schema,keys', [
    (pa.schema([('v', pa.string()), ('pt', pa.string())]), ['pt']),
    (pa.schema([('v', pa.int32()), ('pt', pa.string())]), ['pt']),
    (pa.schema([('v', pa.string())]), ['v']),
])
def test_format_csv_keeps_rows_whose_file_columns_are_null(tmp_path, schema, keys):
    # Such a row is an empty line in the CSV file, which must not be skipped as blank.
    table = _table(tmp_path, pa_schema=schema, partition_keys=keys)
    table._format = Format.CSV
    values = ['', 'x', None] if pa.types.is_string(schema.field('v').type) else [1, None]
    data = pa.Table.from_pydict(
        {name: values if name == 'v' else ['a'] * len(values) for name in schema.names}, schema=schema)
    writer = table.new_batch_write_builder().new_write()
    try:
        writer.write_arrow(data)
    finally:
        writer.close()
    reader = table.new_read_builder()
    result = reader.new_read().to_arrow(reader.new_scan().plan().splits())
    expected = data.to_pylist()
    if keys == ['v']:
        # A blank partition value names the default partition, which reads as null, as in Java.
        expected = [dict(row, v=row['v'] or None) for row in expected]
    elif pa.types.is_string(schema.field('v').type):
        # CSV writes a null and an empty string alike, and reads a null STRING back as an empty string, as before.
        expected = [dict(row, v=row['v'] or '') for row in expected]
    assert sorted(result.to_pylist(), key=str) == sorted(expected, key=str)


@pytest.mark.parametrize('skip_empty_lines,expected', [(None, [1, None, 2]), ('false', [1, None, 2]),
                                                       ('true', [1, 2])])
def test_format_csv_reads_empty_lines_like_java(tmp_path, skip_empty_lines, expected):
    options = {} if skip_empty_lines is None else {'csv.skip-empty-lines': skip_empty_lines}
    table = _table(tmp_path, options)
    table._format = Format.CSV
    directory = Path(table.location()) / 'pt=a'
    directory.mkdir(parents=True)
    (directory / 'data-1.csv').write_text('id\n1\n\n2\n')
    reader = table.new_read_builder()
    for result in [reader.new_read().to_arrow(reader.new_scan().plan().splits()),
                   pa.Table.from_batches(list(reader.new_read().to_iterator(reader.new_scan().plan().splits())))]:
        assert result['id'].to_pylist() == expected


def test_format_csv_reads_unquoted_null_words_as_strings(tmp_path):
    schema = pa.schema([('id', pa.int32()), ('v', pa.string()), ('pt', pa.string())])
    table = _table(tmp_path, pa_schema=schema)
    table._format = Format.CSV
    directory = Path(table.location()) / 'pt=a'
    directory.mkdir(parents=True)
    # A writer that quotes only when needed, such as Java's, leaves these strings unquoted.
    (directory / 'data-1.csv').write_text('id,v\n1,NA\n2,NULL\n3,nan\n4,N/A\n5,x\n')
    reader = table.new_read_builder()
    result = reader.new_read().to_arrow(reader.new_scan().plan().splits())
    assert sorted(result.to_pylist(), key=lambda row: row['id']) == [
        {'id': i, 'v': v, 'pt': 'a'} for i, v in enumerate(['NA', 'NULL', 'nan', 'N/A', 'x'], 1)]


def test_format_csv_with_null_literal_keeps_values(tmp_path):
    # A non-null value that equals csv.null-literal, and an empty Java string, keep their values.
    schema = pa.schema([('id', pa.int32()), ('v', pa.string()), ('pt', pa.string())])
    table = _table(tmp_path, {'csv.null-literal': '1'}, schema)
    table._format = Format.CSV
    writer = table.new_batch_write_builder().new_write()
    try:
        writer.write_arrow(pa.Table.from_pydict({'id': [1, None], 'v': ['x', 'y'], 'pt': ['a', 'a']}, schema=schema))
    finally:
        writer.close()
    (Path(table.location()) / 'pt=a' / 'data-java.csv').write_text('id,v\n3,\n4,z\n')
    reader = table.new_read_builder()
    result = reader.new_read().to_arrow(reader.new_scan().plan().splits())
    assert sorted(result.to_pylist(), key=lambda row: row['v']) == [
        {'id': 3, 'v': '', 'pt': 'a'}, {'id': 1, 'v': 'x', 'pt': 'a'}, {'id': None, 'v': 'y', 'pt': 'a'},
        {'id': 4, 'v': 'z', 'pt': 'a'}]


@pytest.mark.parametrize('default_name', [None, 'missing'])
@pytest.mark.parametrize('file_format,schema', [
    (Format.PARQUET, pa.schema([('id', pa.int32()), ('pt', pa.string())])),
    # Files keep the partition columns of such a table, but the value would still share the null directory.
    (Format.CSV, pa.schema([('pt', pa.string())])),
    (Format.JSON, pa.schema([('pt', pa.string())])),
])
def test_format_write_rejects_partition_value_equal_to_default_name(tmp_path, default_name, file_format, schema):
    options = {} if default_name is None else {'partition.default-name': default_name}
    table = _table(tmp_path, options, schema)
    table._format = file_format
    value = default_name or '__DEFAULT_PARTITION__'
    writer = table.new_batch_write_builder().new_write()
    try:
        with pytest.raises(ValueError, match='equals the default partition name'):
            writer.write_arrow(pa.Table.from_pydict(
                {name: [None, value] if name == 'pt' else [1, 2] for name in schema.names}, schema=schema))
        assert writer.prepare_commit()[0].written_paths == []
    finally:
        writer.close()


def test_format_static_overwrite_rejects_value_that_names_another_partition(tmp_path):
    schema = pa.schema([('id', pa.int32()), ('pt', pa.binary())])
    table = _table(tmp_path, pa_schema=schema)
    writer = table.new_batch_write_builder().new_write()
    try:
        writer.write_arrow(pa.Table.from_pydict({'id': [1], 'pt': [b'\xef\xbf\xbd']}, schema=schema))
    finally:
        writer.close()
    # b'\xff' and b'\xef\xbf\xbd' both name the directory of U+FFFD, whose files the overwrite would delete.
    with pytest.raises(ValueError, match=r"Partition value b'\\xff' of 'pt' cannot be written"):
        table.new_batch_write_builder().overwrite({'pt': b'\xff'}).new_write()
    reader = table.new_read_builder()
    assert reader.new_read().to_arrow(reader.new_scan().plan().splits()).to_pylist() == [
        {'id': 1, 'pt': b'\xef\xbf\xbd'}]


def test_format_static_overwrite_takes_typed_value_in_partition_type(tmp_path):
    schema = pa.schema([('id', pa.int32()), ('pt', pa.decimal128(4, 2))])
    table = _table(tmp_path, pa_schema=schema)
    for overwrite, ids in [(None, [1]), ({'pt': Decimal('1.0')}, [2])]:
        builder = table.new_batch_write_builder()
        writer = (builder.overwrite(overwrite) if overwrite else builder).new_write()
        try:
            # Rows of DECIMAL(4, 2) name the directory 1.00, which Decimal('1.0') must select.
            writer.write_arrow(pa.Table.from_pydict({'id': ids, 'pt': [Decimal('1.00')]}, schema=schema))
        finally:
            writer.close()
    reader = table.new_read_builder()
    assert reader.new_read().to_arrow(reader.new_scan().plan().splits())['id'].to_pylist() == [2]
    with pytest.raises(ValueError, match='cannot be represented as DECIMAL'):
        table.new_batch_write_builder().overwrite({'pt': Decimal('1.005')}).new_write()


@pytest.mark.parametrize('file_format', [Format.CSV, Format.JSON])
def test_format_partition_columns_kept_in_files_read_like_their_directories(tmp_path, file_format):
    # Blank values name the default partition, which reads and filters as null, so their files store null.
    schema = pa.schema([('pt', pa.string())])
    table = _table(tmp_path, pa_schema=schema)
    table._format = file_format
    writer = table.new_batch_write_builder().new_write()
    try:
        writer.write_arrow(pa.Table.from_pydict({'pt': ['', ' ', None, 'a']}, schema=schema))
    finally:
        writer.close()

    def read(partition_filter=None):
        reader = table.new_read_builder()
        if partition_filter is not None:
            reader = reader.with_partition_filter(partition_filter)
        return sorted(reader.new_read().to_arrow(reader.new_scan().plan().splits())['pt'].to_pylist(),
                      key=lambda value: (value is not None, value))

    assert read() == [None, None, None, 'a']
    assert read({'pt': None}) == [None, None, None]
    assert read({'pt': 'a'}) == ['a']


def _write_earlier_version_file(table, directory, row, schema, name='data-old.parquet'):
    # Earlier versions named directories with str() of the values, without escaping, stopped the path at the
    # first null value, and kept the partition columns in the files.
    path = Path(table.location()) / directory
    path.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist([row], schema=schema), str(path / name))
    return path / name


def _write_rows(table, rows, schema, overwrite=None):
    builder = table.new_batch_write_builder()
    writer = (builder.overwrite(overwrite) if overwrite is not None else builder).new_write()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
    finally:
        writer.close()


def _read_rows(table, partition_filter=None, keys=('pt',)):
    reader = table.new_read_builder()
    if partition_filter is not None:
        reader = reader.with_partition_filter(partition_filter)
    result = reader.new_read().to_arrow(reader.new_scan().plan().splits()).to_pylist()
    return sorted((row['id'],) + tuple(row[key] for key in keys) for row in result)


_STRING_SCHEMA = pa.schema([('id', pa.int32()), ('pt', pa.string())])


@pytest.mark.parametrize('directory,value,written,overwrite,old_kept,expected', [
    # pt=a%3Ab holds the earlier files of 'a%3Ab' and the files of 'a:b': each overwrite keeps the other's.
    ('pt=a%3Ab', 'a%3Ab', 'a:b', 'a:b', True, [(1, 'a%3Ab'), (3, 'a:b')]),
    ('pt=a%3Ab', 'a%3Ab', 'a:b', 'a%3Ab', False, [(2, 'a:b'), (3, 'a%3Ab')]),
    # The earlier files of 'x/y' are below the directory of 'x', deeper than the partition, so reads skip them.
    ('pt=x/y', 'x/y', None, 'x', True, [(3, 'x')]),
])
def test_format_overwrite_deletes_only_its_partition_of_earlier_files(tmp_path, directory, value, written,
                                                                      overwrite, old_kept, expected):
    table = _table(tmp_path, pa_schema=_STRING_SCHEMA)
    old = _write_earlier_version_file(table, directory, {'id': 1, 'pt': value}, _STRING_SCHEMA)
    if written is not None:
        _write_rows(table, [{'id': 2, 'pt': written}], _STRING_SCHEMA)
    _write_rows(table, [{'id': 3, 'pt': overwrite}], _STRING_SCHEMA, overwrite={'pt': overwrite})
    assert old.exists() == old_kept
    assert _read_rows(table) == expected


@pytest.mark.parametrize('arrow_type,options,directory,value', [
    (pa.bool_(), {}, 'pt=True', True),
    (pa.date32(), {'partition.legacy-name': 'true'}, 'pt=2024-01-01', date(2024, 1, 1)),
    (pa.float64(), {}, 'pt=1e+23', 1e23),
])
def test_format_overwrite_deletes_partition_files_of_earlier_spelling(tmp_path, arrow_type, options, directory,
                                                                      value):
    schema = pa.schema([('id', pa.int32()), ('pt', arrow_type)])
    table = _table(tmp_path, options, schema)
    old = _write_earlier_version_file(table, directory, {'id': 1, 'pt': value}, schema)
    _write_rows(table, [{'id': 2, 'pt': value}], schema)
    assert _read_rows(table, {'pt': value}) == [(1, value), (2, value)]
    _write_rows(table, [{'id': 3, 'pt': value}], schema, overwrite={'pt': value})
    assert not old.exists()
    assert _read_rows(table) == [(3, value)]


@pytest.mark.parametrize('keys,directory,old_rows,overwrite,remaining', [
    # Earlier versions stopped the path at the first null value; the overwrite deletes the files whose
    # values are the partition's.
    (['pt'], '', [{'pt': None}], {'pt': None}, []),
    (['a', 'pt'], 'a=x', [{'a': 'x', 'pt': None}], {'a': 'x', 'pt': None}, []),
    (['a', 'pt'], 'a=x', [{'a': 'x', 'pt': None}], {'a': 'x'}, []),
    (['pt'], '', [{'pt': None}], {'pt': 'y'}, [0]),
    (['pt', 'a'], '', [{'pt': None, 'a': 'x'}, {'pt': None, 'a': 'y'}], {'pt': None, 'a': 'x'}, [1]),
])
def test_format_overwrite_deletes_null_partition_files_of_earlier_versions(tmp_path, keys, directory, old_rows,
                                                                           overwrite, remaining):
    schema = pa.schema([('id', pa.int32())] + [(key, pa.string()) for key in keys])
    table = _table(tmp_path, pa_schema=schema, partition_keys=keys)
    old = [_write_earlier_version_file(table, directory, dict(row, id=i + 1), schema, f'data-old-{i}.parquet')
           for i, row in enumerate(old_rows)]
    row = {'id': 10, **{key: overwrite.get(key, 'x' if key == 'a' else None) for key in keys}}
    _write_rows(table, [row], schema, overwrite=overwrite)
    assert [i for i, path in enumerate(old) if path.exists()] == remaining
    reader = table.new_read_builder()
    ids = sorted(reader.new_read().to_arrow(reader.new_scan().plan().splits())['id'].to_pylist())
    assert ids == sorted([i + 1 for i in remaining] + [10])


def test_format_reads_and_filters_earlier_version_directories(tmp_path):
    table = _table(tmp_path, pa_schema=_STRING_SCHEMA)
    # pt=a=b holds the value 'a=b', which this version writes to pt=a%3Db.
    _write_earlier_version_file(table, 'pt=a=b', {'id': 1, 'pt': 'a=b'}, _STRING_SCHEMA)
    # pt=a%3Ab holds the earlier value 'a%3Ab' and the value 'a:b' written now.
    _write_earlier_version_file(table, 'pt=a%3Ab', {'id': 2, 'pt': 'a%3Ab'}, _STRING_SCHEMA)
    _write_earlier_version_file(table, '', {'id': 3, 'pt': None}, _STRING_SCHEMA)
    _write_rows(table, [{'id': 4, 'pt': 'a:b'}, {'id': 5, 'pt': None}], _STRING_SCHEMA)
    assert _read_rows(table) == [(1, 'a=b'), (2, 'a%3Ab'), (3, None), (4, 'a:b'), (5, None)]
    assert _read_rows(table, {'pt': 'a=b'}) == [(1, 'a=b')]
    assert _read_rows(table, {'pt': 'a%3Ab'}) == [(2, 'a%3Ab')]
    assert _read_rows(table, {'pt': 'a:b'}) == [(4, 'a:b')]
    assert _read_rows(table, {'pt': None}) == [(3, None), (5, None)]


def test_format_reads_earlier_version_directory_of_empty_string(tmp_path):
    # Earlier versions wrote '' to pt=.
    table = _table(tmp_path, pa_schema=_STRING_SCHEMA)
    _write_earlier_version_file(table, 'pt=', {'id': 1, 'pt': ''}, _STRING_SCHEMA)
    _write_rows(table, [{'id': 2, 'pt': 'a'}], _STRING_SCHEMA)
    assert _read_rows(table) == [(1, ''), (2, 'a')]
    assert _read_rows(table, {'pt': ''}) == [(1, '')]


@pytest.mark.parametrize('arrow_type,value', [
    (pa.binary(), b'ab'), (pa.float64(), float('inf')), (pa.float32(), float('-inf')),
    (pa.timestamp('us', tz='UTC'), datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)),
])
def test_format_filters_earlier_version_directories_that_do_not_parse(tmp_path, arrow_type, value):
    # Earlier versions named these directories pt=b'ab', pt=inf and pt=2024-01-02 03:04:05+00:00.
    schema = pa.schema([('id', pa.int32()), ('pt', arrow_type)])
    table = _table(tmp_path, pa_schema=schema)
    _write_earlier_version_file(table, 'pt=' + str(value), {'id': 1, 'pt': value}, schema)
    _write_rows(table, [{'id': 2, 'pt': value}], schema)
    reader = table.new_read_builder()
    reader.with_filter(reader.new_predicate_builder().equal('pt', value))
    assert sorted(reader.new_read().to_arrow(reader.new_scan().plan().splits())['id'].to_pylist()) == [1, 2]


def test_format_names_float_partitions_for_every_java(tmp_path):
    # Float.toString differs between JDKs, and Java rejects the other JDK's name (#11164). Every Java reads the exact
    # value, also of powers of two, whose neighbors are not evenly spaced.
    schema = pa.schema([('id', pa.int32()), ('pt', pa.float32())])
    table = _table(tmp_path, pa_schema=schema)
    values = [0.1, 268873285632.0, 1e23, 2.0 ** 90, -2.0 ** 90, 2.0 ** 87, 2.0 ** -96, float('inf'), float('nan')]
    _write_rows(table, [{'id': i, 'pt': value} for i, value in enumerate(values)], schema)
    names = [path.name[len('pt='):] for path in Path(table.location()).iterdir() if path.name.startswith('pt=')]
    assert {'Infinity', 'NaN', '0.10000000149011612', '268873285632.0'} <= set(names)
    for name in names:
        if name != 'NaN':
            assert float(name) == pa.array([float(name)], pa.float32())[0].as_py()
    assert [row[0] for row in _read_rows(table)] == list(range(len(values)))


def test_format_overwrite_deletes_float_partition_of_other_jdk(tmp_path):
    schema = pa.schema([('id', pa.int32()), ('pt', pa.float32())])
    table = _table(tmp_path, pa_schema=schema)
    # Java 11 names 268873285632f 2.68873286E11, Java 19+ 2.6887329E11.
    java11 = Path(table.location()) / 'pt=2.68873286E11'
    java11.mkdir(parents=True)
    pq.write_table(pa.table({'id': pa.array([1], pa.int32())}), str(java11 / 'data-java11.parquet'))
    _write_rows(table, [{'id': 2, 'pt': 1.0}], schema)
    _write_rows(table, [{'id': 3, 'pt': 268873285632.0}], schema, overwrite={'pt': '2.6887329E11'})
    assert not (java11 / 'data-java11.parquet').exists()
    assert sorted(_read_rows(table)) == [(2, 1.0), (3, 268873285632.0)]


@pytest.mark.parametrize('arrow_type', [pa.float32(), pa.float64()])
@pytest.mark.parametrize('only_value', ['false', 'true'])
def test_format_overwrite_of_float_keeps_default_partition_named_as_number(tmp_path, arrow_type, only_value):
    # pt=0 names the null partition, not another name of 0.0.
    schema = pa.schema([('id', pa.int32()), ('pt', arrow_type)])
    options = {'partition.default-name': '0', 'format-table.partition-path-only-value': only_value}
    table = _table(tmp_path, options, schema)
    _write_rows(table, [{'id': 1, 'pt': None}], schema)
    _write_rows(table, [{'id': 2, 'pt': 0.0}], schema, overwrite={'pt': '0.0'})
    assert _read_rows(table) == [(1, None), (2, 0.0)]


def test_format_overwrite_of_blank_value_replaces_its_null_rows(tmp_path):
    # A table of only partition columns stores null for a blank value in its JSON files.
    schema = pa.schema([('pt', pa.string())])
    table = _table(tmp_path, options={'partition.default-name': '__NULL/VALUE__'}, pa_schema=schema)
    table._format = Format.JSON
    _write_rows(table, [{'pt': ''}], schema)
    _write_rows(table, [{'pt': ''}], schema, overwrite={'pt': ''})
    reader = table.new_read_builder()
    assert reader.new_read().to_arrow(reader.new_scan().plan().splits())['pt'].to_pylist() == [None]


def test_format_text_overwrite_replaces_partition(tmp_path):
    # A TEXT file holds only its text column, not the partition columns.
    schema = pa.schema([('v', pa.string()), ('pt', pa.string())])
    table = _table(tmp_path, pa_schema=schema)
    table._format = Format.TEXT
    _write_rows(table, [{'v': 'old', 'pt': 'x/y'}], schema)
    _write_rows(table, [{'v': 'new', 'pt': 'x/y'}], schema, overwrite={'pt': 'x/y'})
    reader = table.new_read_builder()
    assert reader.new_read().to_arrow(reader.new_scan().plan().splits())['v'].to_pylist() == ['new']


def test_format_csv_reads_partition_columns_as_text(tmp_path):
    # CSV would infer 001 as the number 1 in the partition columns that files keep.
    schema = pa.schema([('v', pa.string()), ('pt', pa.string())])
    table = _table(tmp_path, pa_schema=schema)
    table._format = Format.CSV
    _write_rows(table, [{'v': 'x', 'pt': 'a'}], schema)
    old = Path(table.location()) / 'pt=001'
    old.mkdir(parents=True)
    pa_csv.write_csv(pa.Table.from_pylist([{'v': 'y', 'pt': '001'}], schema=schema), str(old / 'data-old.csv'))
    reader = table.new_read_builder()
    result = reader.new_read().to_arrow(reader.new_scan().plan().splits())
    assert sorted(zip(result['v'].to_pylist(), result['pt'].to_pylist())) == [('x', 'a'), ('y', '001')]
    reader = table.new_read_builder().with_partition_filter({'pt': '001'})
    assert reader.new_read().to_arrow(reader.new_scan().plan().splits())['v'].to_pylist() == ['y']

    only_partitions = pa.schema([('pt', pa.string())])
    table = _table(tmp_path / 'only', pa_schema=only_partitions)
    table._format = Format.CSV
    _write_rows(table, [{'pt': '001'}, {'pt': ''}], only_partitions)
    reader = table.new_read_builder()
    result = reader.new_read().to_arrow(reader.new_scan().plan().splits())['pt'].to_pylist()
    assert sorted(result, key=lambda v: (v is None, v)) == ['001', None]
    reader = table.new_read_builder().with_partition_filter({'pt': '001'})
    assert reader.new_read().to_arrow(reader.new_scan().plan().splits())['pt'].to_pylist() == ['001']


def test_format_csv_keeps_null_string_of_earlier_file(tmp_path):
    # A STRING column of only nulls reads as null, as before.
    schema = pa.schema([('id', pa.int32()), ('v', pa.string()), ('pt', pa.string())])
    table = _table(tmp_path, pa_schema=schema)
    table._format = Format.CSV
    old = Path(table.location()) / 'pt=a'
    old.mkdir(parents=True)
    pa_csv.write_csv(pa.Table.from_pylist([{'id': 1, 'v': None, 'pt': 'a'}], schema=schema), str(old / 'data-old.csv'))
    reader = table.new_read_builder()
    assert reader.new_read().to_arrow(reader.new_scan().plan().splits())['v'].to_pylist() == [None]


@pytest.mark.parametrize('declared,given,value', [
    (pa.timestamp('ns'), pa.timestamp('us'), datetime(2024, 1, 2, 3, 4, 5, 678000)),
    (pa.timestamp('us'), pa.timestamp('ms'), datetime(2024, 1, 2, 3, 4, 5, 678000)),
    (pa.string(), pa.large_string(), 'a'), (pa.int32(), pa.int64(), 1), (pa.decimal128(6, 2), pa.decimal128(4, 2),
                                                                         Decimal('1.00')),
])
def test_format_write_takes_partition_values_of_a_narrower_type(tmp_path, declared, given, value):
    table = _table(tmp_path, pa_schema=pa.schema([('id', pa.int32()), ('pt', declared)]))
    _write_rows(table, [{'id': 1, 'pt': value}], pa.schema([('id', pa.int32()), ('pt', given)]))
    assert _read_rows(table)[0][1] == value


@pytest.mark.parametrize('declared,given,value', [
    (pa.timestamp('us', tz='UTC'), pa.timestamp('us'), datetime(2024, 1, 2)), (pa.int8(), pa.int64(), 300),
    (pa.timestamp('us'), pa.timestamp('us', tz='UTC'), datetime(2024, 1, 2, tzinfo=timezone.utc)),
])
def test_format_write_refuses_partition_values_of_another_type(tmp_path, declared, given, value):
    table = _table(tmp_path, pa_schema=pa.schema([('id', pa.int32()), ('pt', declared)]))
    with pytest.raises(ValueError):
        _write_rows(table, [{'id': 1, 'pt': value}], pa.schema([('id', pa.int32()), ('pt', given)]))
    assert not list(Path(table.location()).rglob('*.parquet'))


def test_format_write_formats_each_partition_value_once(tmp_path, monkeypatch):
    table = _table(tmp_path, pa_schema=pa.schema([('id', pa.int32()), ('pt', pa.float64())]))
    checked = []
    check = PythonFormatTableWrite._check_partition_value_round_trips
    monkeypatch.setattr(PythonFormatTableWrite, '_check_partition_value_round_trips',
                        lambda self, key, value, text, parsed: checked.append(value) or check(
                            self, key, value, text, parsed))
    values = [0.0, -0.0, 1.5, None] * 1000
    data = pa.table({'id': pa.array(range(4000), pa.int32()), 'pt': pa.array(values, pa.float64())})
    writer = table.new_batch_write_builder().new_write()
    writer.write_arrow(data)
    writer.close()
    assert len(checked) == 3
    assert sorted(path.name for path in Path(table.location()).iterdir()) == [
        'pt=-0.0', 'pt=0.0', 'pt=1.5', 'pt=__DEFAULT_PARTITION__']
    rows = _read_rows(table)
    assert [repr(row[1]) for row in rows] == [repr(value) for value in values]


def test_format_overwrite_of_blank_binary_replaces_its_null_rows(tmp_path):
    # b' ' names the default partition, so a CSV file of only partition columns stores null for it.
    schema = pa.schema([('pt', pa.binary())])
    table = _table(tmp_path, options={'partition.default-name': '__NULL/VALUE__'}, pa_schema=schema)
    table._format = Format.CSV
    _write_rows(table, [{'pt': b' '}], schema)
    _write_rows(table, [{'pt': b' '}], schema, overwrite={'pt': b' '})
    reader = table.new_read_builder()
    assert reader.new_read().to_arrow(reader.new_scan().plan().splits())['pt'].to_pylist() == [None]


@pytest.mark.parametrize('options', [{}, {'partition.default-name': 'NULLS'}])
def test_format_overwrite_keeps_earlier_value_named_as_default_partition(tmp_path, options):
    # Earlier versions wrote the string '__DEFAULT_PARTITION__' to the directory of the default partition.
    table = _table(tmp_path, options=options, pa_schema=_STRING_SCHEMA)
    name = options.get('partition.default-name', '__DEFAULT_PARTITION__')
    old = _write_earlier_version_file(table, 'pt=' + name, {'id': 1, 'pt': name}, _STRING_SCHEMA)
    _write_rows(table, [{'id': 2, 'pt': ''}], _STRING_SCHEMA, overwrite={'pt': ''})
    assert old.exists()


def test_format_overwrite_of_null_and_blank_values_clears_their_earlier_directories(tmp_path):
    # They share the default partition, but earlier versions wrote '' to pt= and ' ' to an unescaped 'pt= '.
    table = _table(tmp_path, pa_schema=_STRING_SCHEMA)
    old = [_write_earlier_version_file(table, 'pt=', {'id': 1, 'pt': ''}, _STRING_SCHEMA),
           _write_earlier_version_file(table, 'pt= ', {'id': 2, 'pt': ' '}, _STRING_SCHEMA)]
    _write_rows(table, [{'id': 3, 'pt': None}, {'id': 4, 'pt': ''}, {'id': 5, 'pt': ' '}], _STRING_SCHEMA,
                overwrite={})
    assert not any(path.exists() for path in old)
    assert [row[0] for row in _read_rows(table)] == [3, 4, 5]


@pytest.mark.parametrize('written', [None, '', ' '])
@pytest.mark.parametrize('old_directory,old_value', [
    # Earlier versions stopped the path at a null value, and wrote '' to pt= and ' ' to 'pt= '.
    ('', None), ('pt=', ''), ('pt= ', ' '),
    # This version writes them all to the default partition.
    (None, None),
])
def test_format_overwrite_of_default_partition_clears_all_its_spellings(tmp_path, written, old_directory, old_value):
    table = _table(tmp_path, pa_schema=_STRING_SCHEMA)
    if old_directory is None:
        _write_rows(table, [{'id': 1, 'pt': old_value}], _STRING_SCHEMA)
    else:
        _write_earlier_version_file(table, old_directory, {'id': 1, 'pt': old_value}, _STRING_SCHEMA)
    _write_rows(table, [{'id': 2, 'pt': written}], _STRING_SCHEMA, overwrite={'pt': None})
    assert [row[0] for row in _read_rows(table)] == [2]


def test_format_overwrite_of_default_partition_keeps_other_partitions_where_path_stopped(tmp_path):
    # Earlier versions wrote the files of (None, 'x') and (None, 'y') to the table root.
    schema = pa.schema([('id', pa.int32()), ('pt', pa.string()), ('a', pa.string())])
    table = _table(tmp_path, pa_schema=schema, partition_keys=['pt', 'a'])
    x = _write_earlier_version_file(table, '', {'id': 1, 'pt': None, 'a': 'x'}, schema, 'data-old-x.parquet')
    y = _write_earlier_version_file(table, '', {'id': 2, 'pt': None, 'a': 'y'}, schema, 'data-old-y.parquet')
    _write_rows(table, [{'id': 3, 'pt': '', 'a': 'x'}], schema, overwrite={'pt': None, 'a': 'x'})
    assert not x.exists() and y.exists()


@pytest.mark.parametrize('file_format', [Format.PARQUET, Format.ORC, Format.CSV])
@pytest.mark.parametrize('value', [b'', b' ', b'\t', '\u3000'.encode()])
@pytest.mark.parametrize('only_value', ['false', 'true'])
@pytest.mark.parametrize('static', [True, False])
def test_format_overwrite_of_blank_binary_clears_its_earlier_directory(
        tmp_path, file_format, value, only_value, static):
    # Earlier versions wrote b' ' to pt=b' ', which is a blank value of the default partition now.
    schema = pa.schema([('id', pa.int32()), ('pt', pa.binary())])
    table = _table(tmp_path, {'format-table.partition-path-only-value': only_value}, schema)
    table._format = file_format
    directory = Path(table.location()) / (str(value) if only_value == 'true' else 'pt=' + str(value))
    directory.mkdir(parents=True)
    old = directory / ('data-old.' + file_format.value)
    data = pa.Table.from_pylist([{'id': 1, 'pt': value}], schema=schema)
    {Format.PARQUET: pq.write_table, Format.ORC: pa_orc.write_table, Format.CSV: pa_csv.write_csv}[file_format](
        data, str(old))
    _write_rows(table, [{'id': 2, 'pt': value}], schema, overwrite={'pt': value} if static else {})
    assert not old.exists()
    assert [row[0] for row in _read_rows(table)] == [2]


def test_format_overwrite_fails_when_it_cannot_check_a_file(tmp_path):
    # pt=x%2Fy can be an earlier version's name of another value, so its files are read before they are deleted.
    table = _table(tmp_path, pa_schema=_STRING_SCHEMA)
    _write_rows(table, [{'id': 1, 'pt': 'x/y'}], _STRING_SCHEMA)

    def fail(*args, **kwargs):
        raise OSError('unavailable')

    writer = table.new_batch_write_builder().overwrite({'pt': 'x/y'}).new_write()
    data = pa.Table.from_pylist([{'id': 2, 'pt': 'x/y'}], schema=_STRING_SCHEMA)
    try:
        with patch('pypaimon.execution.python.read.format_table_read._read_file_to_arrow', fail):
            with pytest.raises(RuntimeError, match='check whether the overwrite replaces it'):
                writer.write_arrow(data)
        assert [row[0] for row in _read_rows(table)] == [1]
        # The same writer clears the partition when the write is retried.
        writer.write_arrow(data)
    finally:
        writer.close()
    assert [row[0] for row in _read_rows(table)] == [2]


def _text_table(tmp_path, partition_type):
    schema = pa.schema([('v', pa.string()), ('pt', partition_type)])
    table = _table(tmp_path, pa_schema=schema)
    table._format = Format.TEXT
    return table, schema


def _write_earlier_text_file(table, directory, line):
    # TEXT files keep no partition columns, also in earlier versions.
    path = Path(table.location()) / directory
    path.mkdir(parents=True, exist_ok=True)
    (path / 'data-old.txt').write_text(line + '\n')
    return path / 'data-old.txt'


@pytest.mark.parametrize('partition_type,directory,value', [
    (pa.float64(), 'pt=inf', float('inf')),
    (pa.timestamp('us', tz='UTC'), 'pt=2024-01-02 03:04:05+00:00',
     datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)),
])
def test_format_text_reads_earlier_version_directories(tmp_path, partition_type, directory, value):
    table, _ = _text_table(tmp_path, partition_type)
    _write_earlier_text_file(table, directory, 'old')
    reader = table.new_read_builder()
    assert reader.new_read().to_arrow(reader.new_scan().plan().splits()).to_pylist() == [{'v': 'old', 'pt': value}]
    reader = table.new_read_builder()
    reader.with_filter(reader.new_predicate_builder().equal('pt', value))
    assert reader.new_read().to_arrow(reader.new_scan().plan().splits())['v'].to_pylist() == ['old']


def test_format_text_reads_earlier_version_file_above_partition_depth(tmp_path):
    # Earlier versions wrote the files of a null value to the table root, where they have no partition values.
    table = _table(tmp_path, pa_schema=pa.schema([('pt', pa.string()), ('text', pa.string())]))
    table._format = Format.TEXT
    _write_earlier_text_file(table, '', 'hello')
    reader = table.new_read_builder()
    splits = reader.new_scan().plan().splits()
    assert reader.new_read().to_arrow(splits).to_pylist() == [{'text': 'hello'}]
    assert pa.Table.from_batches(list(reader.new_read().to_iterator(splits))).to_pylist() == [{'text': 'hello'}]


def test_format_text_overwrite_of_blank_keeps_string_spelled_as_bytes(tmp_path):
    # pt=b' ' names the bytes b' ' only in a BINARY column; here it holds the string "b' '".
    table, schema = _text_table(tmp_path, pa.string())
    old = _write_earlier_text_file(table, "pt=b' '", 'old')
    _write_rows(table, [{'v': 'new', 'pt': ''}], schema, overwrite={'pt': ''})
    assert old.exists()


def test_format_text_overwrite_deletes_files_of_earlier_directory(tmp_path):
    table, schema = _text_table(tmp_path, pa.string())
    old = _write_earlier_text_file(table, 'pt=a:b', 'old')
    _write_rows(table, [{'v': 'new', 'pt': 'a:b'}], schema, overwrite={'pt': 'a:b'})
    assert not old.exists()
    # pt=a%3Ab is the earlier directory of 'a%3Ab' and the directory of 'a:b': its TEXT files are kept.
    _write_rows(table, [{'v': 'other', 'pt': 'a%3Ab'}], schema, overwrite={'pt': 'a%3Ab'})
    reader = table.new_read_builder()
    result = reader.new_read().to_arrow(reader.new_scan().plan().splits())
    assert sorted(result['v'].to_pylist()) == ['new', 'other']


@pytest.mark.parametrize('file_format', [Format.CSV, Format.JSON])
def test_format_reads_file_with_only_nulls_in_declared_type(tmp_path, file_format):
    # Such a file infers the null type, which cannot be concatenated with the other files.
    schema = pa.schema([('v', pa.int32()), ('pt', pa.string())])
    table = _table(tmp_path, pa_schema=schema)
    table._format = file_format
    for values in ([None, None], [1]):
        writer = table.new_batch_write_builder().new_write()
        try:
            writer.write_arrow(pa.Table.from_pydict({'v': values, 'pt': ['a'] * len(values)}, schema=schema))
        finally:
            writer.close()
    reader = table.new_read_builder()
    splits = reader.new_scan().plan().splits()
    for result in [reader.new_read().to_arrow(splits),
                   pa.Table.from_batches(list(reader.new_read().to_iterator(splits)))]:
        assert result.schema == schema
        assert sorted(result['v'].to_pylist(), key=lambda v: (v is None, v)) == [1, None, None]


@pytest.mark.parametrize('batches', [[['a', '_hidden']], [['a'], ['_hidden']]])
def test_format_rejected_overwrite_keeps_existing_files(tmp_path, batches):
    table = _table(tmp_path, options={'format-table.partition-path-only-value': 'true'})
    writer = table.new_batch_write_builder().new_write()
    writer.write_arrow(pa.table({'id': pa.array([1], pa.int32()), 'pt': ['a']}))
    old_paths = writer.prepare_commit()[0].written_paths
    writer.close()

    schema = pa.schema([('id', pa.int32()), ('pt', pa.string())])
    data = pa.Table.from_batches(
        [pa.record_batch([pa.array([2] * len(pts), pa.int32()), pts], schema=schema) for pts in batches])
    overwrite = table.new_batch_write_builder().overwrite().new_write()
    try:
        with pytest.raises(ValueError, match='cannot be written in a value-only partition'):
            overwrite.write_arrow(data)
        assert overwrite.prepare_commit()[0].written_paths == []
    finally:
        overwrite.close()
    assert _rows(old_paths) == [1]


@pytest.mark.parametrize('legacy', [False, True])
def test_format_write_rejects_partition_values_that_do_not_round_trip(tmp_path, legacy):
    schema = pa.schema([('id', pa.int32()), ('pt', pa.binary())])
    table = _table(tmp_path, {'partition.legacy-name': str(legacy).lower()}, schema)
    writer = table.new_batch_write_builder().new_write()
    try:
        # BINARY partition directories are named by UTF-8 text. The batch is rejected before any of it is written.
        with pytest.raises(ValueError, match=r"Partition value b'\\xff' of 'pt' cannot be written"):
            writer.write_arrow(pa.Table.from_pydict({'id': [1, 2], 'pt': [b'ok', b'\xff']}, schema=schema))
        writer.write_arrow(pa.Table.from_pydict({'id': [3], 'pt': ['中'.encode()]}, schema=schema))
    finally:
        writer.close()
    reader = table.new_read_builder()
    result = reader.new_read().to_arrow(reader.new_scan().plan().splits())
    assert result.to_pylist() == [{'id': 3, 'pt': '中'.encode()}]


@pytest.mark.skipif(not hasattr(system_time, 'tzset'), reason='requires POSIX timezone support')
def test_format_write_rejects_ltz_partition_in_dst_overlap(tmp_path):
    schema = pa.schema([('id', pa.int32()), ('pt', pa.timestamp('us', tz='UTC'))])
    table = _table(tmp_path, {'partition.legacy-name': 'false'}, schema)
    try:
        with patch.dict('os.environ', {'TZ': 'America/New_York'}):
            system_time.tzset()
            writer = table.new_batch_write_builder().new_write()
            try:
                # 05:30Z and 06:30Z are both 01:30 in New York, which names a single partition directory.
                for hour in (5, 6):
                    value = pd.Timestamp(f'2024-11-03T0{hour}:30:00Z')
                    data = pa.Table.from_pydict({'id': [hour], 'pt': [value]}, schema)
                    if hour == 5:
                        writer.write_arrow(data)
                    else:
                        with pytest.raises(ValueError, match="'2024-11-03 01:30:00.000000' would read back as"):
                            writer.write_arrow(data)
            finally:
                writer.close()
    finally:
        system_time.tzset()


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
