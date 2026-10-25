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

"""Java RowCompactedSerializer keys used by composite BTree indexes."""

import math
import struct
from dataclasses import dataclass

from pypaimon.execution.python.globalindex.key_serializer import (
    KeySerializer, create_serializer, _read_var_len_int, _write_var_len_int,
    _parse_precision,
)


@dataclass(frozen=True)
class _TimestampKey:
    milliseconds: int
    nanos_of_millisecond: int

    def get_millisecond(self):
        return self.milliseconds

    def get_nano_of_millisecond(self):
        return self.nanos_of_millisecond


class CompositeKeySerializer(KeySerializer):
    def __init__(self, fields):
        self._types = [field.type.type.upper() for field in fields]
        self._serializers = [create_serializer(field.type) for field in fields]
        self._comparators = [serializer.create_comparator()
                             for serializer in self._serializers]
        self._header_size = (len(fields) + 15) // 8

    def serialize(self, key):
        if len(key) != len(self._serializers):
            raise ValueError("Wrong composite key arity")
        header = bytearray(self._header_size)
        values = bytearray()
        for index, (value, serializer, type_name) in enumerate(
                zip(key, self._serializers, self._types)):
            if value is None:
                header[(index + 8) // 8] |= 1 << ((index + 8) % 8)
                continue
            if type_name == 'FLOAT' and math.isnan(float(value)):
                encoded = struct.pack('<I', 0x7fc00000)
            elif type_name == 'DOUBLE' and math.isnan(float(value)):
                encoded = struct.pack('<Q', 0x7ff8000000000000)
            else:
                encoded = serializer.serialize(value)
            if _variable_length(type_name):
                values.extend(_write_var_len_int(len(encoded)))
            values.extend(encoded)
        return bytes(header + values)

    def deserialize(self, data):
        if len(data) < self._header_size:
            raise ValueError("Truncated composite key")
        position = self._header_size
        result = []
        for index, (serializer, type_name) in enumerate(
                zip(self._serializers, self._types)):
            if data[(index + 8) // 8] & (1 << ((index + 8) % 8)):
                result.append(None)
                continue
            if _variable_length(type_name):
                length, position = _read_var_len_int(data, position)
            else:
                length = _fixed_length(type_name)
                if type_name.startswith('TIMESTAMP') and _parse_precision(
                        type_name, 6) > 3:
                    _, end = _read_var_len_int(data, position + 8)
                    length = end - position
            end = position + length
            if end > len(data):
                raise ValueError("Truncated composite key value")
            encoded = data[position:end]
            if type_name.startswith('TIMESTAMP'):
                milliseconds = struct.unpack('<q', encoded[:8])[0]
                nanos = (_read_var_len_int(encoded, 8)[0]
                         if len(encoded) > 8 else 0)
                result.append(_TimestampKey(milliseconds, nanos))
            elif type_name in ('DATE', 'TIME'):
                result.append(struct.unpack('<i', encoded)[0])
            else:
                result.append(serializer.deserialize(encoded))
            position = end
        if position != len(data):
            raise ValueError("Trailing composite key bytes")
        return tuple(result)

    def create_comparator(self):
        def compare(left, right):
            for a, b, comparator in zip(left, right, self._comparators):
                difference = compare_nullable(a, b, comparator)
                if difference:
                    return difference
            return 0
        return compare


def _variable_length(type_name):
    return (type_name.startswith(('CHAR', 'VARCHAR', 'STRING'))
            or type_name.startswith(('DECIMAL', 'NUMERIC'))
            and _decimal_precision(type_name) > 18)


def _decimal_precision(type_name):
    from pypaimon.execution.python.globalindex.key_serializer import _parse_decimal_params
    return _parse_decimal_params(type_name)[0]


def _fixed_length(type_name):
    if type_name in ('BOOLEAN', 'TINYINT'):
        return 1
    if type_name == 'SMALLINT':
        return 2
    if type_name in ('INT', 'INTEGER', 'DATE', 'TIME', 'FLOAT'):
        return 4
    if type_name in ('BIGINT', 'DOUBLE') or type_name.startswith(('DECIMAL', 'NUMERIC')):
        return 8
    if type_name.startswith('TIMESTAMP'):
        # The caller handles the optional trailing varint separately.
        return 8
    raise ValueError("Unsupported composite key type: %s" % type_name)


def compare_nullable(left, right, comparator):
    if left is None:
        return 0 if right is None else -1
    if right is None:
        return 1
    return comparator(left, right)
