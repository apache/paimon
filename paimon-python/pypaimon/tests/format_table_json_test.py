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
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.python import format_table_text
from pypaimon.execution.python.format_table_json import JsonReader, JsonWriter
from pypaimon.schema.data_types import ArrayType, AtomicType, DataField, MapType, PyarrowFieldParser, VectorType
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


def test_reads_columns_in_table_order(tmp_path):
    # Files leave the partition columns out; reads put them back where the schema has them.
    schema = pa.schema([('pt', pa.string()), ('id', pa.int32()), ('s', pa.string())])
    table = _format_table(tmp_path, schema, ['pt'])
    _write(table, schema, [{'pt': 'x', 'id': 1, 's': 'a'}])
    builder = table.new_read_builder()
    splits = builder.new_scan().plan().splits()
    assert builder.new_read().to_arrow(splits).column_names == ['pt', 'id', 's']
    assert pa.Table.from_batches(list(builder.new_read().to_iterator(splits))).column_names == ['pt', 'id', 's']


def test_base64_padding_like_java():
    # Base64.getDecoder takes a last group without padding, but no padding elsewhere.
    schema = pa.schema([('b', pa.binary())])
    data = b'{"b":"AQ"}\n{"b":"AQ=="}\n{"b":"AQI"}\n{"b":"AQI="}\n'
    assert JsonReader(_fields(schema), None, {}).read(data).column('b').to_pylist() == [
        b'\x01', b'\x01', b'\x01\x02', b'\x01\x02']


@pytest.mark.parametrize('text', ['AQ=', 'AAAA=', 'AQI==', 'A', 'A===', 'AQ=Q'])
def test_base64_malformed_padding_is_a_parse_error(text):
    schema = pa.schema([('b', pa.binary())])
    data = ('{"b":"%s"}\n' % text).encode()
    with pytest.raises(ValueError):
        JsonReader(_fields(schema), None, {}).read(data)
    ignored = JsonReader(_fields(schema), None, {'json.ignore-parse-errors': 'true'}).read(data)
    assert ignored.column('b').to_pylist() == [None]


@pytest.mark.parametrize('line', [b'{"f":1.0000000596046448}\n', b'{"f":"1.0000000596046448"}\n'])
def test_float_rounds_text_once(line):
    # Java reads a number as Double.toString text and rounds it once with Float.parseFloat.
    schema = pa.schema([('f', pa.float32())])
    assert JsonReader(_fields(schema), None, {}).read(line).column('f').to_pylist() == [1.0000001192092896]


def test_float_map_keys_round_text_once():
    # Float.parseFloat reads 1.0000000596046448 as 1.0000001, so it and 1.0 are two keys.
    schema = pa.schema([('m', pa.map_(pa.float32(), pa.string()))])
    result = JsonReader(_fields(schema), None, {}).read(b'{"m":{"1.0000000596046448":"a","1.0":"b"}}\n')
    assert sorted(result.column('m').to_pylist()[0]) == [(1.0, 'b'), (1.0000001192092896, 'a')]


def test_not_null_partition_reads_with_files_of_java(tmp_path):
    # PyPaimon's files keep the partition column, Java's take it from the path: both have the table's field.
    schema = pa.schema([pa.field('id', pa.int32()), pa.field('pt', pa.string(), nullable=False)])
    table = _format_table(tmp_path, schema, ['pt'])
    _write(table, schema, [{'id': 1, 'pt': 'x'}])
    (Path(table.location()) / 'pt=x' / 'data-java.json').write_text('{"id":2}\n')
    builder = table.new_read_builder()
    result = builder.new_read().to_arrow(builder.new_scan().plan().splits())
    assert sorted(result.column('id').to_pylist()) == [1, 2]
    assert result.schema.field('pt').nullable is False


def test_vector_with_malformed_element_is_a_parse_error():
    # A VECTOR cannot hold a null element, so an element that does not parse makes the vector the error.
    fields = [DataField(0, 'v', VectorType(True, AtomicType('FLOAT'), 2))]
    data = b'{"v":["bad","2"]}\n{"v":[1,2]}\n'
    with pytest.raises(ValueError):
        JsonReader(fields, None, {}).read(data)
    result = JsonReader(fields, None, {'json.ignore-parse-errors': 'true'}).read(data)
    assert result.column('v').to_pylist() == [None, [1.0, 2.0]]


@pytest.mark.parametrize('text,expected', [
    # Integer.parseInt reads one UTF-16 char at a time: BMP digits count, a supplementary digit is two surrogates.
    ('\uff11\uff12', 12), ('\u0661', 1), ('0' * 5000 + '1', 1),
])
def test_integer_text_parses_like_java(text, expected):
    fields = _fields(pa.schema([('i', pa.int32())]))
    line = json.dumps({'i': text}).encode() + b'\n'
    assert JsonReader(fields, None, {}).read(line).column('i').to_pylist() == [expected]


def test_integer_text_with_supplementary_digit_is_a_parse_error():
    fields = _fields(pa.schema([('i', pa.int32())]))
    line = json.dumps({'i': '\U0001d7d9'}).encode() + b'\n'
    with pytest.raises(ValueError):
        JsonReader(fields, None, {}).read(line)


@pytest.mark.parametrize('text,expected', [('0' * 5000 + '1', 1.0), ('1e-1000000', 0.0), ('-1e1000000', float('-inf'))])
def test_float_text_of_any_length(text, expected):
    fields = _fields(pa.schema([('f', pa.float32())]))
    line = json.dumps({'f': text}).encode() + b'\n'
    assert JsonReader(fields, None, {}).read(line).column('f').to_pylist() == [expected]


@pytest.mark.parametrize('arrow_type,value', [
    # pyarrow's time32[ms] declares TIME(0), so the value has whole seconds.
    (pa.time32('ms'), time(12, 34, 56)),
    (pa.timestamp('ms'), datetime(2024, 1, 2, 3, 4, 5, 678000)),
    (pa.timestamp('us', tz='UTC'), datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)),
])
def test_reads_time_partitions_with_and_without_projection(tmp_path, arrow_type, value):
    # Arrow cannot cast every partition type from its directory name, and a projection leaves it out.
    schema = pa.schema([('id', pa.int32()), ('pt', arrow_type)])
    table = _format_table(tmp_path, schema, ['pt'])
    writer = table.new_batch_write_builder().new_write()
    writer.write_arrow(pa.Table.from_pylist([{'id': 1, 'pt': value}], schema=schema))
    writer.close()
    builder = table.new_read_builder()
    assert builder.new_read().to_arrow(builder.new_scan().plan().splits()).to_pylist() == [{'id': 1, 'pt': value}]
    builder = table.new_read_builder().with_projection(['id'])
    assert builder.new_read().to_arrow(builder.new_scan().plan().splits()).to_pylist() == [{'id': 1}]


@pytest.mark.parametrize('key', ['json.line-delimiter', 'lineSep'])
def test_rejects_empty_line_delimiter(key):
    # Lines split by an empty delimiter would never advance.
    options = {key: '', 'json.ignore-parse-errors': 'true'}
    with pytest.raises(ValueError, match="'json.line-delimiter' must not be empty"):
        JsonReader(_fields(pa.schema([('id', pa.int32())])), None, options)
    with pytest.raises(ValueError, match="'json.line-delimiter' must not be empty"):
        JsonWriter(_fields(pa.schema([('id', pa.int32())])), options)


def test_numbers_read_into_strings_follow_java_19():
    # Double.toString of Java 19+, as java_floating_string; Java 8 to 17 spell 1e23 as 9.999999999999999E22 (#11164).
    fields = _fields(pa.schema([('s', pa.string())]))
    data = b'{"s":1e23}\n{"s":0.1}\n{"s":1}\n'
    assert JsonReader(fields, None, {}).read(data).column('s').to_pylist() == ['1.0E23', '0.1', '1']


def test_reads_in_batches(monkeypatch):
    monkeypatch.setattr(format_table_text, '_BATCH_ROWS', 2)
    result = JsonReader(_fields(pa.schema([('id', pa.int32())])), None, {}).read(
        b''.join(b'{"id":%d}\n' % i for i in range(1, 6)))
    assert [batch.num_rows for batch in result.to_batches()] == [2, 2, 1]
    assert result.column('id').to_pylist() == [1, 2, 3, 4, 5]


def test_failed_overwrite_keeps_existing_rows(tmp_path, monkeypatch):
    # The files are written only after the new rows convert, so a failure leaves the old ones.
    schema = pa.schema([('id', pa.int32()), ('pt', pa.string())])
    table = _format_table(tmp_path, schema, ['pt'])
    _write(table, schema, [{'id': 1, 'pt': 'x'}])

    def fail(self, data):
        raise ValueError('cannot convert')

    monkeypatch.setattr(JsonWriter, 'write', fail)
    writer = table.new_batch_write_builder().overwrite().new_write()
    with pytest.raises(ValueError, match='cannot convert'):
        writer.write_arrow(pa.Table.from_pylist([{'id': 2, 'pt': 'x'}], schema=schema))
    writer.close()
    monkeypatch.undo()
    assert _read(table) == [{'id': 1, 'pt': 'x'}]


def test_ignored_errors_of_nested_values_follow_java():
    # As JsonFileReader: a VECTOR that cannot be built drops its map entry and nulls its element or field, while a
    # value that does not convert keeps its map entry as null.
    vector = VectorType(True, AtomicType('FLOAT'), 2)
    options = {'json.ignore-parse-errors': 'true'}
    map_of_vectors = [DataField(0, 'm', MapType(True, AtomicType('STRING'), vector))]
    data = b'{"m":{"bad":["bad","1"],"x":"str","ok":["2","3"]}}\n'
    assert JsonReader(map_of_vectors, None, options).read(data).column('m').to_pylist() == [[('ok', [2.0, 3.0])]]
    map_of_ints = [DataField(0, 'm', MapType(True, AtomicType('STRING'), AtomicType('INT')))]
    data = b'{"m":{"a":"x","b":"1"}}\n'
    assert JsonReader(map_of_ints, None, options).read(data).column('m').to_pylist() == [[('a', None), ('b', 1)]]
    array_of_vectors = [DataField(0, 'a', ArrayType(True, vector))]
    data = b'{"a":[["bad","1"],["2","3"]]}\n'
    assert JsonReader(array_of_vectors, None, options).read(data).column('a').to_pylist() == [[None, [2.0, 3.0]]]
    with pytest.raises(ValueError):
        JsonReader(map_of_vectors, None, {}).read(b'{"m":{"bad":["bad","1"]}}\n')


def test_writer_refuses_vector_with_null_element():
    # BinaryVector cannot hold a null element, so neither Java nor the reader could read it back.
    fields = [DataField(0, 'v', VectorType(True, AtomicType('FLOAT'), 2))]
    data = pa.table({'v': pa.array([[1.0, None]], pa.list_(pa.float32(), 2))})
    with pytest.raises(ValueError, match='cannot hold a null element'):
        JsonWriter(fields, {}).write(data)


_OVERWRITE_SCHEMA = pa.schema([('id', pa.int32()), ('v', pa.list_(pa.float32(), 2)), ('pt', pa.string())])


def _overwrite_row(row_id, pt, vector=(1.0, 2.0)):
    return {'id': row_id, 'v': list(vector), 'pt': pt}


@pytest.mark.parametrize('batches', [
    # A later batch, or a later partition of a batch, holds a value that the writer refuses.
    [[_overwrite_row(3, 'x')], [_overwrite_row(4, 'y', vector=(1.0, None))]],
    [[_overwrite_row(3, 'x'), _overwrite_row(4, 'y', vector=(1.0, None))]],
])
def test_refused_overwrite_keeps_existing_rows(tmp_path, batches):
    table = _format_table(tmp_path, _OVERWRITE_SCHEMA, ['pt'])
    _write(table, _OVERWRITE_SCHEMA, [_overwrite_row(1, 'x'), _overwrite_row(2, 'y')])
    data = pa.Table.from_batches([pa.RecordBatch.from_pylist(rows, schema=_OVERWRITE_SCHEMA) for rows in batches])
    writer = table.new_batch_write_builder().overwrite().new_write()
    with pytest.raises(ValueError, match='cannot hold a null element'):
        writer.write_arrow(data)
    writer.close()
    assert sorted(row['id'] for row in _read(table)) == [1, 2]


@pytest.mark.parametrize('key', [pa.binary(), pa.list_(pa.int32())])
def test_writer_refuses_map_keys_it_cannot_write(tmp_path, key):
    # Java writes the identity string of such a key, so the writer refuses the table before it deletes anything.
    schema = pa.schema([('m', pa.map_(key, pa.int32())), ('pt', pa.string())])
    table = _format_table(tmp_path, schema, ['pt'])
    with pytest.raises(NotImplementedError, match='Unsupported map key type'):
        table.new_batch_write_builder().overwrite().new_write()


def test_lone_surrogates_read_as_question_marks():
    # Java encodes a lone surrogate that a \\ud800 escape leaves in a string as '?'.
    fields = _fields(pa.schema([('s', pa.string()), ('m', pa.map_(pa.string(), pa.string()))]))
    data = b'{"s":"a\\ud800b","m":{"k\\udc00":"v\\ud800"}}\n{"s":"ok"}\n{"s":"\\ud83d\\ude00"}\n'
    assert JsonReader(fields, None, {}).read(data).to_pylist() == [
        {'s': 'a?b', 'm': [('k?', 'v?')]}, {'s': 'ok', 'm': None}, {'s': '\U0001f600', 'm': None}]


def test_decodes_malformed_utf8_like_java():
    # Java replaces the encoded surrogate ED A0 80 with one character, not one per byte.
    fields = _fields(pa.schema([('s', pa.string())]))
    assert JsonReader(fields, None, {}).read(b'{"s":"a\xed\xa0\x80b"}\n').column('s').to_pylist() == ['a\ufffdb']


@pytest.mark.parametrize('line', [b'{"d":1e9999999999999999999}', b'{"d":' + b'[' * 100000 + b']' * 100000 + b'}'])
def test_line_that_does_not_parse_is_a_parse_error(line):
    # A DECIMAL column parses numbers as Decimal, which refuses this exponent; the other line nests too deep.
    fields = _fields(pa.schema([('d', pa.decimal128(10, 2))]))
    data = line + b'\n{"d":1.5}\n'
    with pytest.raises(ValueError, match='Failed to parse JSON line'):
        JsonReader(fields, None, {}).read(data)
    result = JsonReader(fields, None, {'json.ignore-parse-errors': 'true'}).read(data)
    assert result.column('d').to_pylist() == [Decimal('1.50')]
