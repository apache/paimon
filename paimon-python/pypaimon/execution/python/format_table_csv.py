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

"""CSV files of format tables, read and written like Java CsvParser and CsvFormatWriter."""

import base64
from typing import Dict, List, Optional

import pyarrow

from pypaimon.execution.python.format_table_text import (_INTEGER, _SUPPORTED_ATOMIC_TYPES, _TRIMMED,
                                                         _is_default_line_delimiter, _iter_lines,
                                                         _parse_java_floating, _rows_to_table, _type_name)
from pypaimon.schema.data_types import AtomicType, DataField, PyarrowFieldParser
from pypaimon.utils.file_store_path_factory import _format_partition_value, _is_null_or_whitespace_only
from pypaimon.utils.numeric_utils import java_floating_string

_MODES = ('PERMISSIVE', 'DROPMALFORMED', 'FAILFAST')


def _option(options: Dict[str, str], key: str, fallbacks, default: str) -> str:
    for name in (key,) + fallbacks:
        if name in options:
            return options[name]
    return default


def _single_character(options, key, fallbacks, default) -> str:
    value = _option(options, key, fallbacks, default)
    if len(value) != 1:
        raise ValueError(f"'{key}' must be a single character, but was '{value}'.")
    return value


class CsvOptions:

    def __init__(self, options: Dict[str, str]):
        self.field_delimiter = _single_character(
            options, 'csv.field-delimiter', ('field-delimiter', 'seq', 'delimiter', 'sep'), ',')
        self.line_delimiter = _option(options, 'csv.line-delimiter', ('lineSep',), '\n')
        if not self.line_delimiter:
            raise ValueError("'csv.line-delimiter' must not be empty.")
        self.quote = _single_character(options, 'csv.quote-character', ('quote',), '"')
        self.escape = _single_character(options, 'csv.escape-character', ('escape',), '\\')
        self.include_header = _option(options, 'csv.include-header', ('header',), 'false').lower() == 'true'
        self.null_literal = _option(options, 'csv.null-literal', ('nullvalue',), '')
        self.skip_empty_lines = options.get('csv.skip-empty-lines', 'false').lower() == 'true'
        self.mode = _option(options, 'csv.mode', ('mode',), 'PERMISSIVE').upper()
        if self.mode not in _MODES:
            raise ValueError(f"Could not parse value '{self.mode}' for key 'csv.mode'.")


def _check_type(field: DataField) -> pyarrow.DataType:
    """CsvFileFormat.validateDataFields."""
    if not isinstance(field.type, AtomicType) or _type_name(field.type) not in _SUPPORTED_ATOMIC_TYPES:
        raise NotImplementedError(f'Unsupported data type for CSV format: {field.type}')
    return PyarrowFieldParser.from_paimon_type(field.type)


class CsvWriter:

    def __init__(self, fields: List[DataField], options: Dict[str, str]):
        self._fields = fields
        self._types = [_check_type(field) for field in fields]
        self._options = CsvOptions(options)
        self._line_break_splits = _is_default_line_delimiter(self._options.line_delimiter)

    def write(self, data: pyarrow.Table) -> bytes:
        options = self._options
        columns = [data.column(field.name).to_pylist() for field in self._fields]
        lines = []
        if options.include_header:
            lines.append(options.field_delimiter.join(
                self._escape(field.name, field.name) for field in self._fields))
        for row in range(data.num_rows):
            lines.append(options.field_delimiter.join(
                self._escape(self._to_string(columns[i][row], i), field.name)
                for i, field in enumerate(self._fields)))
        return ''.join(line + options.line_delimiter for line in lines).encode('utf-8')

    def _to_string(self, value, index: int) -> Optional[str]:
        if value is None:
            return None
        arrow_type = self._types[index]
        if pyarrow.types.is_boolean(arrow_type):
            return 'true' if value else 'false'
        if pyarrow.types.is_floating(arrow_type):
            return java_floating_string(value, pyarrow.types.is_float32(arrow_type))
        if pyarrow.types.is_binary(arrow_type) or pyarrow.types.is_large_binary(arrow_type) \
                or pyarrow.types.is_fixed_size_binary(arrow_type):
            return base64.b64encode(value).decode('ascii')
        if pyarrow.types.is_integer(arrow_type) or pyarrow.types.is_string(arrow_type) \
                or pyarrow.types.is_large_string(arrow_type):
            return str(value)
        return _format_partition_value(value, self._fields[index].type, False, None)

    def _escape(self, field: Optional[str], name: str) -> str:
        options = self._options
        if field is None:
            return options.null_literal
        delimiter = options.line_delimiter
        if delimiter in field or (self._line_break_splits and ('\r' in field or '\n' in field)):
            shown = field if len(field) <= 64 else field[:64] + '...'
            raise ValueError(
                f"Column '{name}' contains the row separator, which the CSV format cannot represent: '{shown}'")
        quote, escape = options.quote, options.escape
        if not (field == options.null_literal or options.field_delimiter in field
                or delimiter[0] in field or quote in field or escape in field):
            return field
        escaped = field.replace(escape, escape + escape)
        return quote + escaped.replace(quote, escape + quote) + quote


class CsvReader:

    def __init__(self, fields: List[DataField], read_names: Optional[List[str]], options: Dict[str, str]):
        self._fields = fields
        self._types = [_check_type(field) for field in fields]
        self._options = CsvOptions(options)
        names = set(read_names) if read_names else None
        self._projected = [names is None or field.name in names for field in fields]
        options = self._options
        if options.field_delimiter == '\0':
            raise ValueError('Separator cannot be the null character (ASCII 0)')
        if options.field_delimiter == options.quote:
            raise ValueError('Separator and quote character cannot be the same')
        if options.field_delimiter == options.escape:
            raise ValueError('Separator and escape character cannot be the same')
        # Setting both to the null character disables quoting.
        self._quoting = options.quote != '\0' or options.escape != '\0'
        if self._quoting and options.quote == options.escape:
            raise ValueError('Quote and escape character cannot be the same')

    def read(self, data: bytes) -> pyarrow.Table:
        return _rows_to_table(self._rows(data), self._fields)

    def _rows(self, data: bytes):
        options = self._options
        lines = _iter_lines(data.decode('utf-8', errors='replace'), options.line_delimiter)
        if options.include_header:
            next(lines, None)
        for line in lines:
            if options.skip_empty_lines and not line.strip(_TRIMMED):
                continue
            row = self._parse(line)
            if row is not None:
                yield row

    def _parse(self, line: str) -> Optional[list]:
        count = len(self._fields)
        if _is_null_or_whitespace_only(line):
            return [None] * count
        values, quoted = self._split(line, count)
        row = []
        for i, field in enumerate(self._fields):
            # As Java, which only converts the projected columns.
            if not self._projected[i]:
                row.append(None)
                continue
            try:
                ok, value = self._parse_field(values[i], i, quoted[i])
                error = None
            except (ValueError, TypeError, ArithmeticError) as e:
                ok, value, error = False, None, e
            if ok:
                row.append(value)
            elif self._options.mode == 'PERMISSIVE':
                row.append(None)
            elif self._options.mode == 'DROPMALFORMED':
                return None
            else:
                raise ValueError(
                    f"Malformed CSV value '{values[i]}' for column '{field.name}' of type {field.type}") from error
        return row

    def _split(self, line: str, count: int):
        options = self._options
        separator, quote, escape = options.field_delimiter, options.quote, options.escape
        values = [None] * count
        quoted = [False] * count
        if not self._quoting or quote not in line:
            parts = line.split(separator, count)[:count]
            values[:len(parts)] = parts
            return values, quoted
        buffer = []
        # How much of the buffer is known to be whitespace, so that the check below stays linear.
        checked, has_text = 0, False
        column = 0
        in_quotes = in_field = False
        position = 0
        length = len(line)
        while position < length and column < count:
            char = line[position]
            following = line[position + 1] if position + 1 < length else None
            if not in_field and not buffer and char == quote:
                quoted[column] = True
            if quoted[column] and char == escape:
                if (in_quotes or in_field) and following in (quote, escape):
                    buffer.append(following)
                    position += 1
                else:
                    buffer.append(char)
            elif quoted[column] and char == quote:
                if (in_quotes or in_field) and following == quote:
                    buffer.append(following)
                    position += 1
                else:
                    if position > 2 and line[position - 1] != separator and following not in (None, separator):
                        if not has_text:
                            has_text = not _is_null_or_whitespace_only(''.join(buffer[checked:]))
                            checked = len(buffer)
                        if buffer and not has_text:
                            buffer = []
                            checked = 0
                        else:
                            buffer.append(char)
                    in_quotes = not in_quotes
                in_field = not in_field
            elif char == separator and not in_quotes:
                values[column] = ''.join(buffer)
                column += 1
                buffer = []
                checked, has_text = 0, False
                in_field = False
            else:
                buffer.append(char)
                in_field = True
            position += 1
        if column < count and not in_quotes:
            values[column] = ''.join(buffer)
        return values, quoted

    def _parse_field(self, text: Optional[str], index: int, quoted: bool):
        if text is None or (not quoted and text == self._options.null_literal):
            return True, None
        arrow_type = self._types[index]
        if pyarrow.types.is_integer(arrow_type):
            if _INTEGER.fullmatch(text) is None:
                return False, None
            value = int(text)
            bits = arrow_type.bit_width
            return -(1 << (bits - 1)) <= value < (1 << (bits - 1)), value
        if pyarrow.types.is_floating(arrow_type):
            return True, _parse_java_floating(text, pyarrow.types.is_float32(arrow_type))
        if pyarrow.types.is_boolean(arrow_type):
            lowered = text.lower()
            return lowered in ('true', 'false'), lowered == 'true'
        if pyarrow.types.is_string(arrow_type) or pyarrow.types.is_large_string(arrow_type):
            return True, text
        if pyarrow.types.is_binary(arrow_type) or pyarrow.types.is_large_binary(arrow_type) \
                or pyarrow.types.is_fixed_size_binary(arrow_type):
            return True, base64.b64decode(text + '=' * (-len(text) % 4), validate=True)
        from pypaimon.execution.python.write.partition_spec import (
            _partition_spec_value, _UNREPRESENTABLE_PARTITION_VALUE)
        value = _partition_spec_value(text, self._fields[index].type)
        if value is _UNREPRESENTABLE_PARTITION_VALUE:
            raise NotImplementedError(
                f"CSV value '{text}' of column '{self._fields[index].name}' cannot be represented in Python")
        return True, value
