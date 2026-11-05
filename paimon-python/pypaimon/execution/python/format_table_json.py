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

from pypaimon.execution.python.format_table_text import (_decode_java_base64, _iter_lines, _parse_java_floating,
                                                         _parse_java_integer, _rows_to_table, _SUPPORTED_ATOMIC_TYPES,
                                                         _type_name)
from pypaimon.schema.data_types import ArrayType, AtomicType, DataField, MapType, RowType, VectorType
from pypaimon.utils.file_store_path_factory import _decode_java_utf8, _format_partition_value
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


def _java_string(text: str) -> str:
    """A JSON string as Java keeps it: \\ud800 escapes a lone surrogate, which UTF-8 encodes as '?'."""
    try:
        text.encode('utf-8')
        return text
    except UnicodeEncodeError:
        return text.encode('utf-8', 'replace').decode('utf-8')


def _check_writable_map_keys(data_type):
    """Java writes the identity string of a byte array or nested map key, which nothing reads back."""
    if isinstance(data_type, (ArrayType, VectorType)):
        _check_writable_map_keys(data_type.element)
    elif isinstance(data_type, MapType):
        if _is_binary(data_type.key) or not isinstance(data_type.key, AtomicType):
            raise NotImplementedError(f'Unsupported map key type for JSON format: {data_type.key}')
        _check_writable_map_keys(data_type.value)
    elif isinstance(data_type, RowType):
        for field in data_type.fields:
            _check_writable_map_keys(field.type)


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
        if not self.line_delimiter:
            # As 'csv.line-delimiter': lines split by an empty delimiter would never advance.
            raise ValueError("'json.line-delimiter' must not be empty.")


def _from_string(text: str, data_type):
    """JsonMapKeyConverter.convert: Java's parse methods, then the string cast."""
    name = _type_name(data_type)
    if name in _INTEGER_BITS:
        value = _parse_java_integer(text, _INTEGER_BITS[name])
        if value is None:
            raise ValueError(f'For input string: "{text}"')
        return value
    if name in ('FLOAT', 'DOUBLE'):
        return _parse_java_floating(text, name == 'FLOAT')
    if name == 'BOOLEAN':
        if text.lower() not in ('true', 'false'):
            raise ValueError(f'Invalid boolean value: {text}')
        return text.lower() == 'true'
    if name in ('CHAR', 'VARCHAR', 'STRING'):
        return _java_string(text)
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
            _check_writable_map_keys(field.type)
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
            if isinstance(data_type, VectorType) and any(element is None for element in value):
                # BinaryVector cannot hold a null element, so neither Java nor the reader could read it back.
                raise ValueError(f'{data_type} cannot hold a null element')
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
        for line in _iter_lines(_decode_java_utf8(data), self._options.line_delimiter):
            row = self._parse_line(line, present)
            if row is not None:
                yield row

    def _parse_line(self, line: str, present: set):
        try:
            start = len(line) - len(line.lstrip(' \t\r\n'))
            node, _ = json.JSONDecoder(
                parse_float=Decimal if self._decimal else float,
                parse_constant=_reject_constant).raw_decode(line, start)
        except (ValueError, ArithmeticError, RecursionError) as e:
            # Such as a DECIMAL exponent past Decimal's range, or nesting past Python's depth, which Jackson also
            # refuses as a malformed line.
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
        """A row field or an array element, which JsonFileReader converts in its own try: an error makes it null."""
        try:
            return self._convert(node, data_type)
        except Exception as e:
            return self._error(e)

    def _convert(self, node, data_type):
        """JsonFileReader.convertJsonValue. Errors of values are handled here, as there; a vector that cannot be
        built raises to the enclosing field, element or map entry, as BinaryVector.fromInternalArray throws."""
        if node is None:
            return None
        if isinstance(data_type, (ArrayType, VectorType)):
            values = (self._error(ValueError(f'Expected array node but got: {type(node).__name__}'))
                      if not isinstance(node, list) else [self._field(element, data_type.element) for element in node])
            if isinstance(data_type, VectorType):
                if values is None or any(value is None for value in values):
                    raise ValueError(f'{data_type} cannot hold a null element')
                if len(values) != data_type.length:
                    raise ValueError(f'Size {len(values)} != {data_type.length} for {data_type}')
            return values
        if isinstance(data_type, MapType):
            if not isinstance(node, dict):
                return self._error(ValueError(f'Expected object node but got: {type(node).__name__}'))
            entries = {}
            for key, value in node.items():
                # Java converts an entry in one try: an error that the value conversion raises drops the entry.
                try:
                    converted = self._primitive(key, data_type.key)
                    if converted is not None:
                        # Keys that convert to the same value keep the last one, as in Java's map.
                        entries[_map_key_identity(converted)] = (converted, self._convert(value, data_type.value))
                except Exception as e:
                    self._error(e)
            return list(entries.values())
        if isinstance(data_type, RowType):
            if not isinstance(node, dict):
                return self._error(ValueError(f'Expected object node but got: {type(node).__name__}'))
            return {field.name: self._field(node.get(field.name), field.type) for field in data_type.fields}
        text = _as_text(node, _type_name(data_type) == 'DECIMAL')
        if _is_binary(data_type):
            try:
                return _decode_java_base64(text)
            except Exception as e:
                return self._error(e)
        return self._primitive(text, data_type)

    def _primitive(self, text, data_type):
        """JsonFileReader.convertPrimitiveStringToType: a value that does not convert is a parse error."""
        try:
            return _from_string(text, data_type)
        except Exception as e:
            return self._error(e)


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
