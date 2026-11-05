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

import base64
import math
import re
import struct
import unicodedata
from fractions import Fraction
from typing import Iterable, Iterator, List, Optional

import pyarrow

from pypaimon.schema.data_types import DataField, PyarrowFieldParser

_JAVA_FLOATING = re.compile(
    r'[+-]?(?:NaN|Infinity|(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?[fFdD]?'
    r'|0[xX](?:[0-9a-fA-F]+(?:\.[0-9a-fA-F]*)?|\.[0-9a-fA-F]+)[pP][+-]?[0-9]+[fFdD]?)')
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
    if not delimiter:
        raise ValueError('The line delimiter must not be empty.')
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
    if not single or 'n' in text.lower():
        return value
    # Float.parseFloat rounds the text once. Rounding it to a double first gives the same float unless the double is
    # exactly halfway between two floats: halfway points are doubles, so a value on either side of one rounds to it.
    try:
        packed = _FLOAT.pack(value)
    except OverflowError:
        return _parse_float32(text)
    single_value = _FLOAT.unpack(packed)[0]
    if single_value == value or math.isinf(single_value):
        return single_value if single_value == value else _parse_float32(text)
    bits = _BITS.unpack(packed)[0]
    neighbor = _FLOAT.unpack(_BITS.pack(bits + 1 if abs(value) > abs(single_value) else bits - 1))[0]
    return _parse_float32(text) if value == (single_value + neighbor) / 2 else single_value


_FLOAT = struct.Struct('>f')
_BITS = struct.Struct('>I')


_FLOAT32_MAX = Fraction(struct.unpack('>f', struct.pack('>I', 0x7f7fffff))[0])
_FLOAT32_OVERFLOW = _FLOAT32_MAX + Fraction(2) ** 103
_HEX_FLOATING = re.compile(r'([+-]?)0[xX]([0-9a-fA-F]*)(?:\.([0-9a-fA-F]*))?[pP]([+-][0-9]+|[0-9]+)')
_DECIMAL_FLOATING = re.compile(r'([+-]?)([0-9]*)(?:\.([0-9]*))?(?:[eE]([+-]?[0-9]+))?')
# Digits past these cannot change how a value rounds to a float: its midpoints have fewer significant digits.
_SIGNIFICANT_DIGITS = 200


def _exponent(text: str) -> int:
    """A decimal exponent, saturated far past the float range rather than converted digit by digit."""
    digits = text.lstrip('+-').lstrip('0')
    value = int(digits) if 0 < len(digits) <= 9 else (10 ** 9 if digits else 0)
    return -value if text.startswith('-') else value


def _parse_float32(text: str) -> float:
    """Float.parseFloat of finite text, which may have any number of digits."""
    hexadecimal = _HEX_FLOATING.fullmatch(text)
    sign, integer, fraction, exponent = (hexadecimal or _DECIMAL_FLOATING.fullmatch(text)).groups()
    fraction = fraction or ''
    digits = (integer + fraction).lstrip('0')
    # value = digits * base ** -len(fraction) * (2 or 10) ** exponent
    scale = _exponent(exponent or '0')
    if not digits:
        exact = Fraction(0)
    elif hexadecimal:
        # value is in [2 ** (point - 1), 2 ** point): past 2 ** 128 it overflows, below 2 ** -150 it is zero.
        point = 4 * (len(digits) - 1 - len(fraction)) + int(digits[0], 16).bit_length() + scale
        if point > 128 or point <= -150:
            exact = _FLOAT32_OVERFLOW if point > 0 else Fraction(0)
        else:
            exact = Fraction(int(digits, 16), 16 ** len(fraction)) * Fraction(2) ** scale
    else:
        scale -= len(fraction)
        point = len(digits) + scale
        if point > 40 or point < -46:
            exact = _FLOAT32_OVERFLOW if point > 0 else Fraction(0)
        else:
            if len(digits) > _SIGNIFICANT_DIGITS:
                kept = digits[:_SIGNIFICANT_DIGITS]
                # A sticky digit keeps a value that is past a midpoint from reading as the midpoint.
                kept += '1' if digits[_SIGNIFICANT_DIGITS:].strip('0') else ''
                scale += len(digits) - len(kept)
                digits = kept
            exact = Fraction(int(digits)) * Fraction(10) ** scale
    return _round_to_float32(exact, sign == '-')


def _parse_java_integer(text: str, bits: int) -> Optional[int]:
    """Integer.parseInt and its kin: a sign, then decimal digits read one UTF-16 char at a time.

    Character.digit takes any decimal digit of the BMP, but a digit past it is two surrogate chars. Leading zeros
    may be any number. None for text that does not parse or does not fit in ``bits``.
    """
    digits = text[1:] if text[:1] in ('+', '-') else text
    if not digits:
        return None
    value = 0
    limit = 1 << (bits - 1)
    for char in digits:
        digit = unicodedata.decimal(char, None) if ord(char) <= 0xffff else None
        if digit is None:
            return None
        value = value * 10 + digit
        if value > limit:
            return None
    value = -value if text.startswith('-') else value
    return value if -limit <= value < limit else None


def _round_to_float32(exact: Fraction, negative: bool) -> float:
    """The float nearest to exact, ties to even, as Float.parseFloat rounds; -0 keeps its sign."""
    sign = -1.0 if negative else 1.0
    magnitude = abs(exact)
    if magnitude >= _FLOAT32_OVERFLOW:
        return sign * float('inf')
    approximate = struct.unpack('>I', struct.pack('>f', min(float(magnitude), float(_FLOAT32_MAX))))[0]
    candidates = [bits for bits in (approximate - 1, approximate, approximate + 1) if 0 <= bits <= 0x7f7fffff]

    def distance(bits):
        return abs(Fraction(struct.unpack('>f', struct.pack('>I', bits))[0]) - magnitude), bits % 2

    value = struct.unpack('>f', struct.pack('>I', min(candidates, key=distance)))[0]
    return sign * value


# Base64.getDecoder: whole groups of four, then a last group of two or three that may be padded to four. Padding
# anywhere else, such as AAAA= or AQ=, is malformed.
_JAVA_BASE64 = re.compile(r'(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}(?:==)?|[A-Za-z0-9+/]{3}=?)?')


def _decode_java_base64(text: str) -> bytes:
    if _JAVA_BASE64.fullmatch(text) is None:
        raise ValueError(f'Illegal base64 value: {text}')
    return base64.b64decode(text.rstrip('=') + '=' * (-len(text.rstrip('=')) % 4))
