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

"""Key serializer for global indexes."""

from abc import ABC, abstractmethod
from typing import Callable
import struct

from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.data_types import DataType


class KeySerializer(ABC):
    """Interface for serializing, deserializing, and comparing global index keys."""

    @abstractmethod
    def serialize(self, key: object) -> bytes:
        """Serialize a key to bytes."""
        pass

    @abstractmethod
    def deserialize(self, data: bytes) -> object:
        """Deserialize bytes to a key."""
        pass

    @abstractmethod
    def create_comparator(self) -> Callable[[object, object], int]:
        """Create a comparator function for keys."""
        pass


class StringSerializer(KeySerializer):
    """Serializer for STRING type."""

    def serialize(self, key: object) -> bytes:
        if isinstance(key, str):
            return key.encode('utf-8')
        return str(key).encode('utf-8')

    def deserialize(self, data: bytes) -> object:
        return data.decode('utf-8')

    def create_comparator(self) -> Callable[[object, object], int]:
        def compare(a: object, b: object) -> int:
            str_a = a if isinstance(a, str) else str(a)
            str_b = b if isinstance(b, str) else str(b)
            if str_a < str_b:
                return -1
            if str_a > str_b:
                return 1
            return 0
        return compare


class LongSerializer(KeySerializer):
    """Serializer for BIGINT type."""

    def serialize(self, key: object) -> bytes:
        return struct.pack('<q', int(key))

    def deserialize(self, data: bytes) -> object:
        return struct.unpack('<q', data)[0]

    def create_comparator(self) -> Callable[[object, object], int]:
        def compare(a: object, b: object) -> int:
            long_a = int(a)
            long_b = int(b)
            if long_a < long_b:
                return -1
            if long_a > long_b:
                return 1
            return 0
        return compare


class IntSerializer(KeySerializer):
    """Serializer for INT type."""

    def serialize(self, key: object) -> bytes:
        return struct.pack('<i', int(key))

    def deserialize(self, data: bytes) -> object:
        return struct.unpack('<i', data)[0]

    def create_comparator(self) -> Callable[[object, object], int]:
        def compare(a: object, b: object) -> int:
            int_a = int(a)
            int_b = int(b)
            if int_a < int_b:
                return -1
            if int_a > int_b:
                return 1
            return 0
        return compare


def create_serializer(data_type: DataType) -> KeySerializer:
    if not isinstance(data_type, AtomicType):
        raise ValueError(
            f"Key serializer only support AtomicType yet, meet {data_type.__class__}")
    type_name = data_type.type.upper()
    if type_name in ('CHAR', 'VARCHAR', 'STRING'):
        return StringSerializer()
    if type_name == 'BIGINT':
        return LongSerializer()
    if type_name == 'INT':
        return IntSerializer()
    raise ValueError(f"DataType: {data_type} is not supported by global index now.")
