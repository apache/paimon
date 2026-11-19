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

"""Text helpers shared by the CSV and JSON files of format tables, following Java's parsers."""

import re
import struct
from typing import Iterable, Iterator, List

import pyarrow

from pypaimon.schema.data_types import DataField, PyarrowFieldParser

_JAVA_FLOATING = re.compile(
    r'[+-]?(?:NaN|Infinity|(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?[fFdD]?'
    r'|0[xX](?:[0-9a-fA-F]+(?:\.[0-9a-fA-F]*)?|\.[0-9a-fA-F]+)[pP][+-]?[0-9]+[fFdD]?)')
_INTEGER = re.compile(r'[+-]?\d+')
_TRIMMED = ''.join(chr(code) for code in range(33))
_SUPPORTED_ATOMIC_TYPES = ('CHAR', 'VARCHAR', 'STRING', 'BOOLEAN', 'BINARY', 'VARBINARY', 'BYTES', 'DECIMAL',
                           'TINYINT', 'SMALLINT', 'INT', 'INTEGER', 'BIGINT', 'FLOAT', 'DOUBLE', 'DATE', 'TIME',
                           'TIMESTAMP', 'TIMESTAMP_LTZ')
_LINE_BREAK = re.compile(r'\r\n|\r|\n')
_BATCH_ROWS = 8192


def _type_name(data_type) -> str:
    return str(data_type).upper().split('(', 1)[0].split()[0]


def _is_default_line_delimiter(delimiter: str) -> bool:
    return delimiter in ('\n', '\r\n', '\r')


def _iter_lines(text: str, delimiter: str) -> Iterator[str]:
    start = 0
    if _is_default_line_delimiter(delimiter):
        if text.startswith('\ufeff'):
            start = 1
        for match in _LINE_BREAK.finditer(text, start):
            yield text[start:match.start()]
            start = match.end()
    else:
        end = text.find(delimiter)
        while end >= 0:
            yield text[start:end]
            start = end + len(delimiter)
            end = text.find(delimiter, start)
    if start < len(text):
        yield text[start:]


def _rows_to_table(rows: Iterable[list], fields: List[DataField]) -> pyarrow.Table:
    """Converts the rows in bounded batches. A table without columns keeps its row count."""
    schema = PyarrowFieldParser.from_paimon_schema(fields)
    batches = []
    columns = [[] for _ in fields]
    count = 0
    for row in rows:
        for column, value in zip(columns, row):
            column.append(value)
        count += 1
        if count % _BATCH_ROWS == 0:
            batches.append(pyarrow.RecordBatch.from_arrays(
                [pyarrow.array(column, type=field.type) for column, field in zip(columns, schema)], schema=schema))
            columns = [[] for _ in fields]
    if not fields:
        return pyarrow.table({'': pyarrow.nulls(count)}).select([])
    batches.append(pyarrow.RecordBatch.from_arrays(
        [pyarrow.array(column, type=field.type) for column, field in zip(columns, schema)], schema=schema))
    return pyarrow.Table.from_batches(batches, schema=schema)


def _parse_java_floating(text: str, single: bool) -> float:
    text = text.strip(_TRIMMED)
    if _JAVA_FLOATING.fullmatch(text) is None:
        raise ValueError(f'For input string: "{text}"')
    text = text[:-1] if text[-1:] in ('f', 'F', 'd', 'D') else text
    try:
        value = float.fromhex(text) if 'p' in text.lower() else float(text)
    except OverflowError:
        value = float('-inf') if text.startswith('-') else float('inf')
    if single:
        try:
            value = struct.unpack('>f', struct.pack('>f', value))[0]
        except OverflowError:
            value = float('-inf') if value < 0 else float('inf')
    return value
