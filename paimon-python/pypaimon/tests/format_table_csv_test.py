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

"""Format table CSV files follow Java CsvFormatWriter and CsvParser."""

from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path
import struct
import time as time_module

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.python import format_table_csv, format_table_text
from pypaimon.execution.python.format_table_csv import CsvReader, CsvWriter
from pypaimon.execution.python.write.format_table_write import PythonFormatTableWrite
from pypaimon.schema.data_types import AtomicType, DataField, PyarrowFieldParser
from pypaimon.table.format import Format, FormatTable

_SCHEMA = pa.schema([
    ('id', pa.int32()), ('s', pa.string()), ('b', pa.binary()), ('d', pa.decimal128(10, 2)),
    ('dt', pa.date32()), ('ts', pa.timestamp('ms')), ('f', pa.float32()), ('x', pa.float64()),
    ('flag', pa.bool_()),
])


def _fields(schema=_SCHEMA):
    return PyarrowFieldParser.to_paimon_schema(schema)


_TABLE_SCHEMA = pa.schema([('id', pa.int32()), ('s', pa.string()), ('pt', pa.string())])


def _table(tmp_path, options=None, schema=_TABLE_SCHEMA):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, ['pt'], options=options or {}), False)
    table = catalog.get_table('default.t')
    return FormatTable(table.file_io, table.identifier, table.table_schema, str(tmp_path / 'format'), Format.CSV)


def _read(table, projection=None):
    builder = table.new_read_builder()
    if projection is not None:
        builder = builder.with_projection(projection)
    return builder.new_read().to_arrow(builder.new_scan().plan().splits())


def test_writes_values_like_java():
    rows = [
        {'id': 1, 's': 'a,b', 'b': b'\x00\xff', 'd': Decimal('1.50'), 'dt': date(2026, 11, 5),
         'ts': datetime(2026, 11, 5, 1, 2, 3, 400000), 'f': 1.5, 'x': 1e10, 'flag': True},
        {'id': 2, 's': 'say "hi" \\ ok', 'b': b'', 'd': Decimal('-0.01'), 'dt': date(1, 1, 1),
         'ts': datetime(1970, 1, 1), 'f': 0.1, 'x': -0.0, 'flag': False},
        {key: None for key in _SCHEMA.names},
        {'id': None, 's': '', 'b': None, 'd': None, 'dt': None, 'ts': None, 'f': float('nan'),
         'x': float('-inf'), 'flag': None},
    ]
    data = CsvWriter(_fields(), {}).write(pa.Table.from_pylist(rows, schema=_SCHEMA))
    assert data.decode('utf-8').split('\n') == [
        '1,"a,b",AP8=,1.50,2026-11-05,2026-11-05 01:02:03.400,1.5,1.0E10,true',
        '2,"say \\"hi\\" \\\\ ok","",-0.01,0001-01-01,1970-01-01 00:00:00.000,0.1,-0.0,false',
        ',,,,,,,,',
        ',"",,,,,NaN,-Infinity,',
        '',
    ]


def test_writes_header_and_custom_options():
    schema = pa.schema([('id', pa.int32()), ('s', pa.string())])
    options = {'csv.include-header': 'true', 'csv.field-delimiter': ';', 'csv.null-literal': 'NULL',
               'csv.quote-character': "'"}
    data = CsvWriter(_fields(schema), options).write(pa.Table.from_pylist(
        [{'id': 1, 's': 'a;b'}, {'id': None, 's': 'NULL'}, {'id': 3, 's': "it's"}], schema=schema))
    assert data.decode('utf-8') == "id;s\n1;'a;b'\nNULL;'NULL'\n3;'it\\'s'\n"


def test_fallback_option_keys():
    schema = pa.schema([('id', pa.int32()), ('s', pa.string())])
    data = CsvWriter(_fields(schema), {'sep': '|', 'header': 'true'}).write(
        pa.Table.from_pylist([{'id': 1, 's': 'x'}], schema=schema))
    assert data == b'id|s\n1|x\n'


@pytest.mark.parametrize('value', ['a\nb', 'a\rb'])
def test_refuses_line_break_in_value(value):
    schema = pa.schema([('s', pa.string())])
    with pytest.raises(ValueError, match="Column 's' contains the row separator"):
        CsvWriter(_fields(schema), {}).write(pa.Table.from_pylist([{'s': value}], schema=schema))


def test_custom_line_delimiter_carries_line_breaks():
    schema = pa.schema([('id', pa.int32()), ('s', pa.string())])
    options = {'csv.line-delimiter': '\x01'}
    table = pa.Table.from_pylist([{'id': 1, 's': 'a\nb'}, {'id': 2, 's': 'c\r\nd'}], schema=schema)
    data = CsvWriter(_fields(schema), options).write(table)
    assert data == b'1,a\nb\x012,c\r\nd\x01'
    assert CsvReader(_fields(schema), None, options).read(data).to_pylist() == table.to_pylist()


def test_round_trip_all_types():
    rows = [
        {'id': -2147483648, 's': 'x,"y"\\z', 'b': bytes(range(256)), 'd': Decimal('99999999.99'),
         'dt': date(2026, 11, 5), 'ts': datetime(2026, 11, 5, 23, 59, 59, 999000), 'f': 3.4028234663852886e38,
         'x': 5e-324, 'flag': True},
        {'id': 0, 's': ' ', 'b': b'', 'd': Decimal('0.00'), 'dt': date(9999, 12, 31), 'ts': datetime(1, 1, 1),
         'f': float('inf'), 'x': 1.7976931348623157e308, 'flag': False},
        {key: None for key in _SCHEMA.names},
    ]
    table = pa.Table.from_pylist(rows, schema=_SCHEMA)
    data = CsvWriter(_fields(), {}).write(table)
    assert CsvReader(_fields(), None, {}).read(data) == table


def test_reads_java_quoting():
    schema = pa.schema([('a', pa.string()), ('b', pa.string()), ('c', pa.string())])
    lines = [
        '"a,1","say ""hi""","x\\"y\\\\z"',
        '"",,""',
        'C:\\dir,unquoted "quote",3',
        'only',
        '1,2,3,4',
        ' \t',
        'x,y," "a""',
    ]
    result = CsvReader(_fields(schema), None, {}).read('\r\n'.join(lines).encode('utf-8'))
    assert result.to_pylist() == [
        {'a': 'a,1', 'b': 'say "hi"', 'c': 'x"y\\z'},
        {'a': '', 'b': None, 'c': ''},
        {'a': 'C:\\dir', 'b': 'unquoted "quote"', 'c': '3'},
        {'a': 'only', 'b': None, 'c': None},
        {'a': '1', 'b': '2', 'c': '3'},
        {'a': None, 'b': None, 'c': None},
        {'a': 'x', 'b': 'y', 'c': 'a"'},
    ]


def test_reads_header_bom_and_empty_lines():
    schema = pa.schema([('id', pa.int32()), ('s', pa.string())])
    data = '\ufeffid,s\n1,a\n\n2,b\n'.encode('utf-8')
    assert CsvReader(_fields(schema), None, {'csv.include-header': 'true'}).read(data).to_pylist() == [
        {'id': 1, 's': 'a'}, {'id': None, 's': None}, {'id': 2, 's': 'b'}]
    assert CsvReader(_fields(schema), None, {
        'csv.include-header': 'true', 'csv.skip-empty-lines': 'true'}).read(data).to_pylist() == [
        {'id': 1, 's': 'a'}, {'id': 2, 's': 'b'}]
    assert CsvReader(_fields(schema), None, {}).read(data).to_pylist()[0] == {'id': None, 's': 's'}
    strings = pa.schema([('a', pa.string()), ('b', pa.string())])
    assert CsvReader(_fields(strings), None, {}).read('\ufeffa,b\n'.encode('utf-8')).to_pylist() == [
        {'a': 'a', 'b': 'b'}]


def test_reads_in_batches(monkeypatch):
    monkeypatch.setattr(format_table_text, '_BATCH_ROWS', 2)
    schema = pa.schema([('id', pa.int32())])
    result = CsvReader(_fields(schema), None, {}).read(b'1\n2\n3\n4\n5\n')
    assert [batch.num_rows for batch in result.to_batches()] == [2, 2, 1]
    assert result.column('id').to_pylist() == [1, 2, 3, 4, 5]
    assert CsvReader([], None, {}).read(b'\n\n\n').num_rows == 3


def test_parse_modes():
    schema = pa.schema([('id', pa.int8()), ('flag', pa.bool_()), ('x', pa.float64()), ('b', pa.binary())])
    data = b'1,true,1.5,AQ==\n128,yes,abc,!\n2,FALSE,-1e3d,AQ\n'
    permissive = CsvReader(_fields(schema), None, {}).read(data).to_pylist()
    assert permissive == [
        {'id': 1, 'flag': True, 'x': 1.5, 'b': b'\x01'},
        {'id': None, 'flag': None, 'x': None, 'b': None},
        {'id': 2, 'flag': False, 'x': -1000.0, 'b': b'\x01'},
    ]
    dropped = CsvReader(_fields(schema), None, {'csv.mode': 'DROPMALFORMED'}).read(data).to_pylist()
    assert [row['id'] for row in dropped] == [1, 2]
    with pytest.raises(ValueError, match="Malformed CSV value '128' for column 'id'"):
        CsvReader(_fields(schema), None, {'csv.mode': 'FAILFAST'}).read(data)
    # Only projected columns are parsed by Java, so other malformed values keep the row.
    projected = CsvReader(_fields(schema), ['b'], {'csv.mode': 'FAILFAST'}).read(b'x,y,z,AQ==\n')
    assert projected.column('b').to_pylist() == [b'\x01']


def test_converts_only_projected_columns():
    schema = pa.schema([('id', pa.int32()), ('dt', pa.date32())])
    # Day 3000000 is valid in Java but past Python's date range.
    data = b'1,3000000\n'
    assert CsvReader(_fields(schema), ['id'], {'csv.mode': 'FAILFAST'}).read(data).column('id').to_pylist() == [1]
    with pytest.raises(NotImplementedError, match='cannot be represented in Python'):
        CsvReader(_fields(schema), None, {}).read(data)
    assert CsvReader(_fields(schema), [], {}).read(b'1,2026-11-05\n').to_pylist() == [
        {'id': 1, 'dt': date(2026, 11, 5)}]


def test_table_write_and_read(tmp_path):
    table = _table(tmp_path, {'csv.field-delimiter': '\t'})
    writer = table.new_batch_write_builder().new_write()
    rows = [{'id': 1, 's': 'a\tb', 'pt': 'x'}, {'id': 2, 's': None, 'pt': 'y'}]
    writer.write_arrow(pa.Table.from_pylist(rows, schema=pa.schema(
        [('id', pa.int32()), ('s', pa.string()), ('pt', pa.string())])))
    paths = writer.prepare_commit()[0].written_paths
    writer.close()
    assert sorted(Path(path).read_text() for path in paths) == ['1\t"a\tb"\n', '2\t\n']
    assert sorted(_read(table).to_pylist(), key=lambda row: row['id']) == rows
    assert sorted(_read(table, ['pt']).column('pt').to_pylist()) == ['x', 'y']
    builder = table.new_read_builder()
    batches = builder.new_read().to_iterator(builder.new_scan().plan().splits())
    assert sorted((row for batch in batches for row in batch.to_pylist()), key=lambda row: row['id']) == rows


def test_writes_columns_in_schema_order(tmp_path):
    table = _table(tmp_path)
    writer = table.new_batch_write_builder().new_write()
    reversed_schema = pa.schema(list(reversed(_TABLE_SCHEMA)))
    writer.write_arrow(pa.Table.from_pylist([{'pt': 'x', 's': 'a', 'id': 1}], schema=reversed_schema))
    paths = writer.prepare_commit()[0].written_paths
    writer.close()
    assert [Path(path).read_text() for path in paths] == ['1,a\n']
    assert _read(table).to_pylist() == [{'id': 1, 's': 'a', 'pt': 'x'}]


def test_table_with_only_partition_columns(tmp_path):
    schema = pa.schema([('pt', pa.string())])
    table = _table(tmp_path, schema=schema)
    writer = table.new_batch_write_builder().new_write()
    writer.write_arrow(pa.Table.from_pylist([{'pt': 'x'}, {'pt': 'x'}], schema=schema))
    paths = writer.prepare_commit()[0].written_paths
    writer.close()
    # Java writes an empty line per row and reads each one back as a row.
    assert [Path(path).read_text() for path in paths] == ['\n\n']
    assert _read(table).to_pylist() == [{'pt': 'x'}, {'pt': 'x'}]


def test_reads_files_written_by_java(tmp_path):
    table = _table(tmp_path, {'csv.include-header': 'true'})
    directory = Path(table.location()) / 'pt=p'
    directory.mkdir(parents=True)
    (directory / 'part-0.csv').write_text('id,s\n1,"a,b"\n2,\n3,""\n')
    assert _read(table).to_pylist() == [
        {'id': 1, 's': 'a,b', 'pt': 'p'}, {'id': 2, 's': None, 'pt': 'p'}, {'id': 3, 's': '', 'pt': 'p'}]


@pytest.mark.parametrize('options, message', [
    ({'csv.field-delimiter': ',,'}, "'csv.field-delimiter' must be a single character"),
    ({'csv.quote-character': ','}, 'Separator and quote character cannot be the same'),
    ({'csv.mode': 'LENIENT'}, "Could not parse value 'LENIENT' for key 'csv.mode'"),
    ({'csv.field-delimiter': '\0'}, 'Separator cannot be the null character'),
    ({'csv.quote-character': '|', 'csv.escape-character': '|'}, 'Quote and escape character cannot be the same'),
])
def test_rejects_invalid_options(options, message):
    with pytest.raises(ValueError, match=message):
        CsvReader(_fields(), None, options)


@pytest.mark.parametrize('data_type', [
    PyarrowFieldParser.to_paimon_type(pa.list_(pa.int32()), True), AtomicType('BLOB'), AtomicType('VARIANT')])
def test_rejects_types_java_does_not_support(data_type):
    fields = [DataField(0, 'a', data_type)]
    with pytest.raises(NotImplementedError, match='Unsupported data type for CSV format'):
        CsvWriter(fields, {})
    with pytest.raises(NotImplementedError, match='Unsupported data type for CSV format'):
        CsvReader(fields, None, {})


def test_null_quote_and_escape_disable_quoting():
    schema = pa.schema([('a', pa.string()), ('b', pa.string())])
    options = {'csv.quote-character': '\0', 'csv.escape-character': '\0'}
    assert CsvReader(_fields(schema), None, options).read(b'"x,y",\\z\n').to_pylist() == [
        {'a': '"x', 'b': 'y"'}]


def test_reads_columns_in_table_order(tmp_path):
    # Files leave the partition columns out; reads put them back where the schema has them.
    schema = pa.schema([('pt', pa.string()), ('id', pa.int32()), ('s', pa.string())])
    table = _table(tmp_path, schema=schema)
    writer = table.new_batch_write_builder().new_write()
    writer.write_arrow(pa.Table.from_pylist([{'pt': 'x', 'id': 1, 's': 'a'}], schema=schema))
    writer.close()
    assert _read(table).column_names == ['pt', 'id', 's']
    builder = table.new_read_builder()
    batches = list(builder.new_read().to_iterator(builder.new_scan().plan().splits()))
    assert pa.Table.from_batches(batches).column_names == ['pt', 'id', 's']


def test_base64_padding_like_java():
    # Base64.getDecoder takes a last group without padding, but no padding elsewhere.
    schema = pa.schema([('b', pa.binary())])
    assert CsvReader(_fields(schema), None, {}).read(b'AQ\nAQ==\nAQI\nAQI=\n').column('b').to_pylist() == [
        b'\x01', b'\x01', b'\x01\x02', b'\x01\x02']


@pytest.mark.parametrize('text', [b'AQ=', b'AAAA=', b'AQI==', b'A', b'A===', b'AQ=Q'])
def test_base64_malformed_padding_follows_mode(text):
    schema = pa.schema([('b', pa.binary())])
    data = text + b'\n'
    assert CsvReader(_fields(schema), None, {}).read(data).to_pylist() == [{'b': None}]
    assert CsvReader(_fields(schema), None, {'csv.mode': 'DROPMALFORMED'}).read(data).num_rows == 0
    with pytest.raises(ValueError):
        CsvReader(_fields(schema), None, {'csv.mode': 'FAILFAST'}).read(data)


@pytest.mark.parametrize('text,expected', [
    # Float.parseFloat rounds once; through a double, the first would round to 1.0.
    ('1.0000000596046448', 1.0000001192092896), ('0x1.000001p0', 1.0), ('-0', -0.0), ('3.4028236e38', float('inf')),
])
def test_float_rounds_text_once(text, expected):
    schema = pa.schema([('f', pa.float32())])
    value = CsvReader(_fields(schema), None, {}).read((text + '\n').encode()).column('f').to_pylist()[0]
    assert struct.pack('>f', value) == struct.pack('>f', expected)


def test_partition_columns_read_with_table_fields(tmp_path):
    schema = pa.schema([('id', pa.int32()), ('s', pa.string()), pa.field('pt', pa.string(), nullable=False)])
    table = _table(tmp_path, schema=schema)
    writer = table.new_batch_write_builder().new_write()
    writer.write_arrow(pa.Table.from_pylist([{'id': 1, 's': 'a', 'pt': 'x'}], schema=schema))
    writer.close()
    assert _read(table).schema.field('pt').nullable is False


@pytest.mark.parametrize('text,expected', [
    # Character.digit reads one UTF-16 char at a time: BMP digits count, a supplementary digit is two surrogates.
    ('\uff11\uff12', 12), ('\u0661', 1), ('\U0001d7d9', None), ('0' * 5000 + '1', 1), ('1' + '0' * 5000, None),
])
def test_integers_parse_like_java(text, expected):
    schema = pa.schema([('i', pa.int32())])
    data = (text + '\n').encode('utf-8')
    assert CsvReader(_fields(schema), None, {}).read(data).column('i').to_pylist() == [expected]
    if expected is None:
        assert CsvReader(_fields(schema), None, {'csv.mode': 'DROPMALFORMED'}).read(data).num_rows == 0


@pytest.mark.parametrize('text,expected', [
    ('0' * 5000 + '1', 1.0), ('0.' + '0' * 5000 + '1', 0.0), ('1e-1000000', 0.0), ('-1e1000000', float('-inf')),
    ('1e' + '0' * 5000 + '1', 10.0),
])
def test_float_text_of_any_length(text, expected):
    schema = pa.schema([('f', pa.float32())])
    assert CsvReader(_fields(schema), None, {}).read((text + '\n').encode()).column('f').to_pylist() == [expected]


@pytest.mark.parametrize('arrow_type,value', [
    # pyarrow's time32[ms] declares TIME(0), so the value has whole seconds.
    (pa.time32('ms'), time(12, 34, 56)),
    (pa.timestamp('ms'), datetime(2024, 1, 2, 3, 4, 5, 678000)),
    (pa.timestamp('us', tz='UTC'), datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)),
])
def test_reads_time_partitions_with_and_without_projection(tmp_path, arrow_type, value):
    # Arrow cannot cast every partition type from its directory name, and a projection leaves it out.
    schema = pa.schema([('id', pa.int32()), ('pt', arrow_type)])
    table = _table(tmp_path, schema=schema)
    writer = table.new_batch_write_builder().new_write()
    writer.write_arrow(pa.Table.from_pylist([{'id': 1, 'pt': value}], schema=schema))
    writer.close()
    builder = table.new_read_builder()
    assert builder.new_read().to_arrow(builder.new_scan().plan().splits()).to_pylist() == [{'id': 1, 'pt': value}]
    builder = table.new_read_builder().with_projection(['id'])
    assert builder.new_read().to_arrow(builder.new_scan().plan().splits()).to_pylist() == [{'id': 1}]


@pytest.mark.parametrize('skip_empty_lines', ['false', 'true'])
def test_whitespace_only_values_round_trip(skip_empty_lines):
    # A line holding only a bare whitespace value would read back as null, or be skipped as an empty line.
    schema = pa.schema([('s', pa.string())])
    values = [' ', '\t', ' \t ', '\u3000', 'a', '', None]
    options = {'csv.skip-empty-lines': skip_empty_lines}
    data = CsvWriter(_fields(schema), options).write(pa.Table.from_pydict({'s': values}, schema=schema))
    assert data.decode('utf-8').split('\n')[:4] == ['" "', '"\t"', '" \t "', '"\u3000"']
    expected = values if skip_empty_lines == 'false' else values[:-1]
    assert CsvReader(_fields(schema), None, options).read(data).column('s').to_pylist() == expected


def test_writes_in_batches(monkeypatch):
    schema = pa.schema([('id', pa.int32()), ('s', pa.string())])
    data = pa.Table.from_pydict({'id': [1, 2, 3, 4, 5], 's': ['a', ' ', None, 'b,c', 'd']}, schema=schema)
    expected = CsvWriter(_fields(schema), {'csv.include-header': 'true'}).write(data)
    monkeypatch.setattr(format_table_csv, '_BATCH_ROWS', 2)
    converted = []
    to_string = CsvWriter._to_string
    monkeypatch.setattr(CsvWriter, '_to_string', lambda self, value, index: converted.append(value) or to_string(
        self, value, index))
    assert CsvWriter(_fields(schema), {'csv.include-header': 'true'}).write(data) == expected
    assert expected.decode('utf-8').split('\n')[0] == 'id,s'
    assert len(converted) == 10


@pytest.mark.parametrize('batches', [
    [[{'id': 2, 's': 'a\nb', 'pt': 'x'}]],
    # A later batch that the writer refuses must fail before the first deletes its partition's files.
    [[{'id': 2, 's': 'ok', 'pt': 'x'}], [{'id': 3, 's': 'a\nb', 'pt': 'y'}]],
    # A later partition of the same batch.
    [[{'id': 2, 's': 'ok', 'pt': 'x'}, {'id': 3, 's': 'a\nb', 'pt': 'y'}]],
])
def test_refused_overwrite_keeps_existing_rows(tmp_path, batches):
    table = _table(tmp_path)
    writer = table.new_batch_write_builder().new_write()
    writer.write_arrow(pa.Table.from_pylist([{'id': 1, 's': 'old', 'pt': 'x'}, {'id': 4, 's': 'old', 'pt': 'y'}],
                                            schema=_TABLE_SCHEMA))
    writer.close()
    data = pa.Table.from_batches([pa.RecordBatch.from_pylist(rows, schema=_TABLE_SCHEMA) for rows in batches])
    writer = table.new_batch_write_builder().overwrite().new_write()
    with pytest.raises(ValueError, match='row separator'):
        writer.write_arrow(data)
    writer.close()
    assert sorted(_read(table).to_pylist(), key=lambda row: row['id']) == [
        {'id': 1, 's': 'old', 'pt': 'x'}, {'id': 4, 's': 'old', 'pt': 'y'}]


def test_refused_overwrite_of_formatted_value_keeps_existing_rows(tmp_path):
    # The row separator '.' is in the formatted DECIMAL 1.00 of a later batch.
    schema = pa.schema([('d', pa.decimal128(4, 2)), ('pt', pa.string())])
    table = _table(tmp_path, {'csv.line-delimiter': '.'}, schema)
    writer = table.new_batch_write_builder().new_write()
    writer.write_arrow(pa.Table.from_pylist([{'d': None, 'pt': 'x'}, {'d': None, 'pt': 'y'}], schema=schema))
    writer.close()
    data = pa.Table.from_batches([pa.RecordBatch.from_pylist([{'d': None, 'pt': 'x'}], schema=schema),
                                  pa.RecordBatch.from_pylist([{'d': Decimal('1.00'), 'pt': 'y'}], schema=schema)])
    old = sorted(path for path in Path(table.location()).rglob('*') if path.is_file())
    writer = table.new_batch_write_builder().overwrite().new_write()
    with pytest.raises(ValueError, match='row separator'):
        writer.write_arrow(data)
    writer.close()
    assert sorted(path for path in Path(table.location()).rglob('*') if path.is_file()) == old


def test_failed_overwrite_removes_its_new_files(tmp_path, monkeypatch):
    # Files are written one at a time before the old ones are deleted; a failure deletes the new ones instead.
    table = _table(tmp_path, {'target-file-row-num': '1'})
    writer = table.new_batch_write_builder().new_write()
    writer.write_arrow(pa.Table.from_pylist([{'id': 1, 's': 'old', 'pt': 'x'}], schema=_TABLE_SCHEMA))
    writer.close()
    calls = []
    serialize = PythonFormatTableWrite._serialize

    def fail_second(self, data):
        calls.append(data.num_rows)
        if len(calls) == 2:
            raise OSError('disk full')
        return serialize(self, data)

    monkeypatch.setattr(PythonFormatTableWrite, '_serialize', fail_second)
    writer = table.new_batch_write_builder().overwrite().new_write()
    rows = [{'id': 2, 's': 'a', 'pt': 'x'}, {'id': 3, 's': 'b', 'pt': 'x'}]
    with pytest.raises(OSError, match='disk full'):
        writer.write_arrow(pa.Table.from_pylist(rows, schema=_TABLE_SCHEMA))
    assert writer.prepare_commit()[0].written_paths == []
    writer.close()
    monkeypatch.undo()
    assert _read(table).to_pylist() == [{'id': 1, 's': 'old', 'pt': 'x'}]


def test_time_with_long_whitespace_parses_in_linear_time():
    # A fraction followed by whitespace and a sign used to backtrack quadratically.
    schema = pa.schema([('t', pa.time32('ms'))])
    data = ('12:34:56.' + ' ' * 50000 + '+\n').encode()
    started = time_module.monotonic()
    assert CsvReader(_fields(schema), None, {}).read(data).column('t').to_pylist() == [None]
    assert time_module.monotonic() - started < 5


def test_decodes_malformed_utf8_like_java():
    # Java replaces the encoded surrogate ED A0 80 with one character, not one per byte.
    schema = pa.schema([('s', pa.string())])
    assert CsvReader(_fields(schema), None, {}).read(b'a\xed\xa0\x80b\n').column('s').to_pylist() == ['a\ufffdb']
