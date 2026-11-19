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

"""JSON files of format tables, read and written like Java JsonFileReader and JsonFormatWriter."""

import base64
import json
import math
import struct
from decimal import Decimal
from typing import Dict, Iterable, List, Optional

import pyarrow

from pypaimon.execution.python.format_table_text import (_INTEGER, _SUPPORTED_ATOMIC_TYPES, _iter_lines,
                                                         _parse_java_floating, _rows_to_table, _type_name)
from pypaimon.schema.data_types import ArrayType, AtomicType, DataField, MapType, RowType, VectorType
from pypaimon.utils.file_store_path_factory import _format_partition_value
from pypaimon.utils.numeric_utils import java_floating_string

_INTEGER_BITS = {'TINYINT': 8, 'SMALLINT': 16, 'INT': 32, 'INTEGER': 32, 'BIGINT': 64}


def _is_binary(data_type) -> bool:
    return _type_name(data_type) in ('BINARY', 'VARBINARY', 'BYTES')


def _validate(data_type):
    """JsonFileFormat.validateDataFields."""
    if isinstance(data_type, (ArrayType, VectorType)):
        _validate(data_type.element)
    elif isinstance(data_type, MapType):
        _validate(data_type.key)
        _validate(data_type.value)
    elif isinstance(data_type, RowType):
        for field in data_type.fields:
            _validate(field.type)
    elif not isinstance(data_type, AtomicType) or _type_name(data_type) not in _SUPPORTED_ATOMIC_TYPES:
        raise NotImplementedError(f'Unsupported data type for JSON format: {data_type}')


def _contains_decimal(data_type) -> bool:
    if isinstance(data_type, (ArrayType, VectorType)):
        return _contains_decimal(data_type.element)
    if isinstance(data_type, MapType):
        return _contains_decimal(data_type.key) or _contains_decimal(data_type.value)
    if isinstance(data_type, RowType):
        return any(_contains_decimal(field.type) for field in data_type.fields)
    return _type_name(data_type) == 'DECIMAL'


class JsonOptions:

    def __init__(self, options: Dict[str, str]):
        self.ignore_parse_errors = options.get('json.ignore-parse-errors', 'false').lower() == 'true'
        self.line_delimiter = options.get('json.line-delimiter', options.get('lineSep', '\n'))


def _from_string(text: str, data_type):
    """JsonMapKeyConverter.convert: Java's parse methods, then the string cast."""
    name = _type_name(data_type)
    if name in _INTEGER_BITS:
        bits = _INTEGER_BITS[name]
        value = int(text) if _INTEGER.fullmatch(text) else None
        if value is None or not -(1 << (bits - 1)) <= value < (1 << (bits - 1)):
            raise ValueError(f'For input string: "{text}"')
        return value
    if name in ('FLOAT', 'DOUBLE'):
        return _parse_java_floating(text, name == 'FLOAT')
    if name == 'BOOLEAN':
        if text.lower() not in ('true', 'false'):
            raise ValueError(f'Invalid boolean value: {text}')
        return text.lower() == 'true'
    if name in ('CHAR', 'VARCHAR', 'STRING'):
        return text
    if not isinstance(data_type, AtomicType):
        raise NotImplementedError(f'Unsupported data type for JSON format: {data_type}')
    from pypaimon.execution.python.write.partition_spec import (
        _partition_spec_value, _UNREPRESENTABLE_PARTITION_VALUE)
    value = _partition_spec_value(text, data_type)
    if value is _UNREPRESENTABLE_PARTITION_VALUE:
        raise NotImplementedError(f"JSON value '{text}' of type {data_type} cannot be represented in Python")
    return value


class JsonWriter:

    def __init__(self, fields: List[DataField], options: Dict[str, str]):
        for field in fields:
            _validate(field.type)
        self._fields = fields
        self._options = JsonOptions(options)

    def write(self, data: pyarrow.Table) -> bytes:
        columns = [data.column(field.name).to_pylist() for field in self._fields]
        lines = []
        for row in range(data.num_rows):
            record = {field.name: self._convert(columns[i][row], field.type) for i, field in enumerate(self._fields)}
            lines.append(json.dumps(record, ensure_ascii=False, separators=(',', ':')))
        return ''.join(line + self._options.line_delimiter for line in lines).encode('utf-8')

    def _convert(self, value, data_type):
        if value is None:
            return None
        if isinstance(data_type, (ArrayType, VectorType)):
            if isinstance(data_type, VectorType) and len(value) != data_type.length:
                raise ValueError(f'Size {len(value)} != {data_type.length} in JsonWriter')
            return [self._convert(element, data_type.element) for element in value]
        if isinstance(data_type, MapType):
            return self._convert_map(value, data_type)
        if isinstance(data_type, RowType):
            return {field.name: self._convert(value.get(field.name), field.type) for field in data_type.fields}
        name = _type_name(data_type)
        if name == 'BOOLEAN':
            return 'true' if value else 'false'
        if name in ('FLOAT', 'DOUBLE'):
            return java_floating_string(value, name == 'FLOAT')
        if name in _INTEGER_BITS or name in ('CHAR', 'VARCHAR', 'STRING'):
            return str(value)
        if _is_binary(data_type):
            return base64.b64encode(value).decode('ascii')
        return _format_partition_value(value, data_type, False, None)

    def _convert_map(self, entries, data_type: MapType) -> dict:
        # Arrow map keys are never null, so json.map-null-key-mode does not apply.
        result = {}
        key_type = data_type.key
        for key, value in entries:
            if _type_name(key_type) in ('CHAR', 'VARCHAR', 'STRING'):
                text = key
            elif _is_binary(key_type) or not isinstance(key_type, AtomicType):
                # Java writes the identity string of the byte array or the nested value.
                raise NotImplementedError(f'Unsupported map key type for JSON format: {key_type}')
            else:
                # Java writes the internal value's toString, as legacy partition names do.
                text = _format_partition_value(key, key_type, True, None)
            result[text] = self._convert(value, data_type.value)
        return result


class JsonReader:
    """Fields named in optional_names are returned only when the file has them."""

    def __init__(self, fields: List[DataField], read_names: Optional[List[str]], options: Dict[str, str],
                 optional_names: Iterable[str] = ()):
        for field in fields:
            _validate(field.type)
        self._fields = fields
        self._options = JsonOptions(options)
        self._optional = set(optional_names)
        names = set(read_names) if read_names else None
        # As Java, which reads with the projected row type, other columns are not parsed.
        self._projected = [names is None or field.name in names for field in fields]
        self._decimal = any(_contains_decimal(field.type)
                            for field, projected in zip(fields, self._projected) if projected)

    def read(self, data: bytes) -> pyarrow.Table:
        present = set()
        table = _rows_to_table(self._rows(data, present), self._fields)
        # select keeps the row count when no column is left.
        return table.select([field.name for field in self._fields
                             if field.name not in self._optional or field.name in present])

    def _rows(self, data: bytes, present: set):
        for line in _iter_lines(data.decode('utf-8', errors='replace'), self._options.line_delimiter):
            row = self._parse_line(line, present)
            if row is not None:
                yield row

    def _parse_line(self, line: str, present: set):
        try:
            start = len(line) - len(line.lstrip(' \t\r\n'))
            node, _ = json.JSONDecoder(
                parse_float=Decimal if self._decimal else float,
                parse_constant=_reject_constant).raw_decode(line, start)
        except ValueError as e:
            if self._options.ignore_parse_errors:
                return None
            raise ValueError(f'Failed to parse JSON line: {line}') from e
        try:
            if node is None:
                return None
            if not isinstance(node, dict):
                return self._error(ValueError(f'Expected object node but got: {type(node).__name__}'))
            present.update(field.name for field, projected in zip(self._fields, self._projected)
                           if projected and field.name in node)
            return [self._field(node.get(field.name), field.type) if projected else None
                    for field, projected in zip(self._fields, self._projected)]
        except Exception as e:
            raise ValueError(f'Failed to convert JSON line: {line}') from e

    def _error(self, error: Exception):
        if self._options.ignore_parse_errors:
            return None
        raise error

    def _field(self, node, data_type):
        try:
            return self._convert(node, data_type)
        except Exception as e:
            return self._error(e)

    def _convert(self, node, data_type):
        if node is None:
            return None
        if isinstance(data_type, (ArrayType, VectorType)):
            if not isinstance(node, list):
                return self._error(ValueError(f'Expected array node but got: {type(node).__name__}'))
            values = [self._field(element, data_type.element) for element in node]
            if isinstance(data_type, VectorType) and len(values) != data_type.length:
                return self._error(ValueError(f'Size {len(values)} != {data_type.length} for {data_type}'))
            return values
        if isinstance(data_type, MapType):
            if not isinstance(node, dict):
                return self._error(ValueError(f'Expected object node but got: {type(node).__name__}'))
            entries = {}
            for key, value in node.items():
                try:
                    converted = _from_string(key, data_type.key)
                except Exception as e:
                    converted = self._error(e)
                if converted is not None:
                    # Keys that convert to the same value keep the last one, as in Java's map.
                    entries[_map_key_identity(converted)] = (converted, self._field(value, data_type.value))
            return list(entries.values())
        if isinstance(data_type, RowType):
            if not isinstance(node, dict):
                return self._error(ValueError(f'Expected object node but got: {type(node).__name__}'))
            return {field.name: self._field(node.get(field.name), field.type) for field in data_type.fields}
        text = _as_text(node, _type_name(data_type) == 'DECIMAL')
        if _is_binary(data_type):
            return base64.b64decode(text + '=' * (-len(text) % 4), validate=True)
        return _from_string(text, data_type)


def _map_key_identity(key):
    """Java's Float and Double keys are equal by bits: 0.0 and -0.0 differ, and NaNs are equal."""
    if isinstance(key, float):
        return 'NaN' if math.isnan(key) else struct.pack('>d', key)
    return key


def _as_text(node, decimal_type: bool) -> str:
    """JsonNode.asText, with non-DECIMAL types reading big decimals as doubles like Java."""
    if isinstance(node, str):
        return node
    if isinstance(node, bool):
        return 'true' if node else 'false'
    if isinstance(node, int):
        return str(node)
    if isinstance(node, Decimal):
        return str(node) if decimal_type else java_floating_string(float(node))
    if isinstance(node, float):
        return java_floating_string(node)
    return ''


def _reject_constant(name):
    raise ValueError(f'Non-standard token {name}')
