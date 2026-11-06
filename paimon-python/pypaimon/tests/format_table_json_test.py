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

"""Format table JSON files follow Java JsonFormatWriter and JsonFileReader."""

import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.python.format_table_json import JsonReader, JsonWriter
from pypaimon.schema.data_types import AtomicType, DataField, PyarrowFieldParser, VectorType
from pypaimon.table.format import Format, FormatTable

_SCHEMA = pa.schema([
    ('id', pa.int32()), ('s', pa.string()), ('b', pa.binary()), ('d', pa.decimal128(10, 2)),
    ('dt', pa.date32()), ('ts', pa.timestamp('ms')), ('f', pa.float32()), ('x', pa.float64()),
    ('flag', pa.bool_()), ('arr', pa.list_(pa.int64())), ('m', pa.map_(pa.string(), pa.date32())),
    ('r', pa.struct([('a', pa.int32()), ('b', pa.string())])),
])


def _fields(schema=_SCHEMA):
    return PyarrowFieldParser.to_paimon_schema(schema)


def _lines(data: bytes):
    return [json.loads(line) for line in data.decode('utf-8').splitlines()]


def test_writes_values_like_java():
    rows = [
        {'id': 1, 's': '中文 "q"', 'b': b'\x00\xff', 'd': Decimal('1.50'), 'dt': date(2026, 11, 5),
         'ts': datetime(2026, 11, 5, 1, 2, 3, 400000), 'f': 1.5, 'x': 1e10, 'flag': True, 'arr': [1, None],
         'm': [('k', date(2026, 1, 1))], 'r': {'a': 7, 'b': None}},
        {key: None for key in _SCHEMA.names},
    ]
    data = JsonWriter(_fields(), {}).write(pa.Table.from_pylist(rows, schema=_SCHEMA))
    assert data.decode('utf-8').split('\n')[0] == (
        '{"id":"1","s":"中文 \\"q\\"","b":"AP8=","d":"1.50","dt":"2026-11-05","ts":"2026-11-05 01:02:03.400",'
        '"f":"1.5","x":"1.0E10","flag":"true","arr":["1",null],"m":{"k":"2026-01-01"},"r":{"a":"7","b":null}}')
    assert _lines(data)[1] == {key: None for key in _SCHEMA.names}


def test_round_trip_all_types():
    rows = [
        {'id': -2147483648, 's': 'line\nbreak\ttab', 'b': bytes(range(256)), 'd': Decimal('99999999.99'),
         'dt': date(1, 1, 1), 'ts': datetime(2026, 11, 5, 23, 59, 59, 999000), 'f': float('nan'),
         'x': float('-inf'), 'flag': False, 'arr': [], 'm': [], 'r': {'a': None, 'b': ''}},
        {key: None for key in _SCHEMA.names},
    ]
    table = pa.Table.from_pylist(rows, schema=_SCHEMA)
    result = JsonReader(_fields(), None, {}).read(JsonWriter(_fields(), {}).write(table))
    assert result.schema == table.schema
    actual = result.to_pylist()
    assert str(actual[0].pop('f')) == 'nan'
    rows[0].pop('f')
    assert actual == [rows[0], dict(rows[1], f=None)]


def test_reads_native_json_values_by_schema():
    schema = pa.schema([('id', pa.int64()), ('s', pa.string()), ('d', pa.decimal128(5, 2)),
                        ('x', pa.float64()), ('flag', pa.bool_()), ('dt', pa.date32())])
    data = '\n'.join([
        '{"id":1,"s":2.5,"d":0.1,"x":1,"flag":true,"dt":"2026-11-05","extra":[1]}',
        '{"s":{"nested":1}}',
        'null',
        '  {"id":"3","flag":"FALSE"} trailing',
    ]).encode('utf-8')
    assert JsonReader(_fields(schema), None, {}).read(data).to_pylist() == [
        {'id': 1, 's': '2.5', 'd': Decimal('0.10'), 'x': 1.0, 'flag': True, 'dt': date(2026, 11, 5)},
        {'id': None, 's': '', 'd': None, 'x': None, 'flag': None, 'dt': None},
        {'id': 3, 's': None, 'd': None, 'x': None, 'flag': False, 'dt': None},
    ]


def test_parse_errors():
    schema = pa.schema([('id', pa.int8()), ('s', pa.string())])
    data = b'{"id":1,"s":"a"}\n{"id":128,"s":"b"}\nnot json\n[1]\n{"id":2,"s":"c"}\n'
    with pytest.raises(ValueError, match='Failed to convert JSON line: {"id":128'):
        JsonReader(_fields(schema), None, {}).read(data)
    with pytest.raises(ValueError, match='Failed to parse JSON line: not json'):
        JsonReader(_fields(schema), None, {}).read(b'not json\n')
    with pytest.raises(ValueError, match='Failed to convert JSON line: \\[1\\]'):
        JsonReader(_fields(schema), None, {}).read(b'[1]\n')
    # Jackson reads a blank line as a missing node, which Java fails to convert like any non-object.
    with pytest.raises(ValueError, match='Failed to parse JSON line'):
        JsonReader(_fields(schema), None, {}).read(b'{"id":1}\n \n')
    assert JsonReader(_fields(schema), None, {'json.ignore-parse-errors': 'true'}).read(data).to_pylist() == [
        {'id': 1, 's': 'a'}, {'id': None, 's': 'b'}, {'id': 2, 's': 'c'}]


def _format_table(tmp_path, schema, partition_keys):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, partition_keys), False)
    loaded = catalog.get_table('default.t')
    return FormatTable(loaded.file_io, loaded.identifier, loaded.table_schema, str(tmp_path / 'format'), Format.JSON)


def _write(table, schema, rows):
    writer = table.new_batch_write_builder().new_write()
    writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
    paths = writer.prepare_commit()[0].written_paths
    writer.close()
    return paths


def _read(table, projection=None):
    builder = table.new_read_builder()
    if projection is not None:
        builder = builder.with_projection(projection)
    result = builder.new_read().to_arrow(builder.new_scan().plan().splits())
    return sorted(result.to_pylist(), key=str)


def test_table_write_and_read(tmp_path):
    schema = pa.schema([('id', pa.int32()), ('d', pa.decimal128(4, 1)), ('pt', pa.string())])
    table = _format_table(tmp_path, schema, ['pt'])
    rows = [{'id': 1, 'd': Decimal('1.5'), 'pt': 'x'}, {'id': 2, 'd': None, 'pt': 'y'}]
    paths = _write(table, schema, rows)
    assert sorted(Path(path).read_text() for path in paths) == [
        '{"id":"1","d":"1.5","pt":"x"}\n', '{"id":"2","d":null,"pt":"y"}\n']
    assert _read(table) == rows
    assert _read(table, []) == rows


def test_partition_values_the_path_cannot_hold(tmp_path):
    schema = pa.schema([('id', pa.int32()), ('pt', pa.binary())])
    table = _format_table(tmp_path, schema, ['pt'])
    rows = [{'id': 1, 'pt': None}, {'id': 2, 'pt': b'ab'}]
    _write(table, schema, rows)
    assert _read(table) == rows


def test_reads_partition_columns_from_the_path_when_files_lack_them(tmp_path):
    schema = pa.schema([('id', pa.int32()), ('pt', pa.string())])
    table = _format_table(tmp_path, schema, ['pt'])
    (tmp_path / 'format' / 'pt=x').mkdir(parents=True)
    (tmp_path / 'format' / 'pt=x' / 'data-1.json').write_text('{"id":"1"}\n')
    assert _read(table) == [{'id': 1, 'pt': 'x'}]


def test_table_with_only_partition_columns(tmp_path):
    schema = pa.schema([('pt', pa.string())])
    table = _format_table(tmp_path, schema, ['pt'])
    _write(table, schema, [{'pt': 'x'}, {'pt': 'x'}])
    (tmp_path / 'format' / 'pt=y').mkdir()
    (tmp_path / 'format' / 'pt=y' / 'data-1.json').write_text('{}\n{}\n')
    assert _read(table) == [{'pt': 'x'}, {'pt': 'x'}, {'pt': 'y'}, {'pt': 'y'}]


@pytest.mark.parametrize('key', ['json.line-delimiter', 'lineSep'])
def test_line_delimiter_option(key):
    schema = pa.schema([('s', pa.string())])
    table = pa.Table.from_pylist([{'s': 'a\nb'}, {'s': 'c'}], schema=schema)
    data = JsonWriter(_fields(schema), {key: '|'}).write(table)
    assert data == b'{"s":"a\\nb"}|{"s":"c"}|'
    assert JsonReader(_fields(schema), None, {key: '|'}).read(data) == table


def test_parses_only_projected_columns():
    schema = pa.schema([('id', pa.int32()), ('n', pa.int8())])
    data = b'{"id":"1","n":"bad"}\n'
    assert JsonReader(_fields(schema), ['id'], {}).read(data).column('id').to_pylist() == [1]
    with pytest.raises(ValueError, match='Failed to convert JSON line'):
        JsonReader(_fields(schema), None, {}).read(data)


@pytest.mark.parametrize('type_name', ['BLOB', 'VARIANT'])
def test_rejects_types_java_does_not_support(type_name):
    fields = [DataField(0, 'v', AtomicType(type_name))]
    with pytest.raises(NotImplementedError, match='Unsupported data type for JSON format'):
        JsonWriter(fields, {})
    with pytest.raises(NotImplementedError, match='Unsupported data type for JSON format'):
        JsonReader(fields, None, {})


def test_map_keys_like_java():
    schema = pa.schema([('d', pa.map_(pa.date32(), pa.int32())), ('t', pa.map_(pa.timestamp('ms'), pa.int32())),
                        ('n', pa.map_(pa.decimal128(5, 2), pa.int32())), ('f', pa.map_(pa.float64(), pa.int32()))])
    table = pa.Table.from_pylist([{'d': [(date(2026, 11, 5), 1)], 't': [(datetime(2026, 11, 5, 1, 2, 3, 400000), 2)],
                                   'n': [(Decimal('1.50'), 3)], 'f': [(1e10, 4)]}], schema=schema)
    data = JsonWriter(_fields(schema), {}).write(table)
    assert _lines(data) == [{'d': {'20762': '1'}, 't': {'2026-11-05T01:02:03.400': '2'}, 'n': {'1.50': '3'},
                             'f': {'1.0E10': '4'}}]
    assert JsonReader(_fields(schema), None, {}).read(data) == table


def test_map_keys_converting_to_the_same_value_keep_the_last():
    schema = pa.schema([('m', pa.map_(pa.int32(), pa.int32())), ('f', pa.map_(pa.float64(), pa.int32()))])
    result = JsonReader(_fields(schema), None, {}).read(
        b'{"m":{"1":"1","01":"2","2":"3"},"f":{"0.0":"1","-0.0":"2","NaN":"3"," NaN":"4"}}\n')
    assert result.column('m').to_pylist() == [[(1, 2), (2, 3)]]
    # Java compares Double keys by their bits.
    assert str(result.column('f').to_pylist()) == '[[(0.0, 1), (-0.0, 2), (nan, 4)]]'


def test_vector_of_wrong_length():
    fields = [DataField(0, 'v', VectorType(True, AtomicType('FLOAT'), 2))]
    data = b'{"v":["1","2","3"]}\n{"v":["1","2"]}\n'
    with pytest.raises(ValueError, match='Failed to convert JSON line'):
        JsonReader(fields, None, {}).read(data)
    result = JsonReader(fields, None, {'json.ignore-parse-errors': 'true'}).read(data)
    assert result.column('v').to_pylist() == [None, [1.0, 2.0]]
