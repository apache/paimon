################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
################################################################################

"""Key serializer for B-tree index."""

from abc import ABC, abstractmethod
from typing import Callable
import struct


class KeySerializer(ABC):
    """
    Interface for serializing and deserializing B-tree index keys.
    
    This interface provides core methods to ser/de and compare btree index keys.
    """

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
        """
        Create a comparator function for keys.
        
        Returns:
            A function that takes two keys and returns:
            - negative if first < second
            - 0 if first == second
            - positive if first > second
        """
        pass


class StringSerializer(KeySerializer):
    """Serializer for STRING type."""

    def serialize(self, key: object) -> bytes:
        """Serialize a string key to bytes."""
        if isinstance(key, str):
            return key.encode('utf-8')
        return str(key).encode('utf-8')

    def deserialize(self, data: bytes) -> object:
        """Deserialize bytes to a string key."""
        return data.decode('utf-8')

    def create_comparator(self) -> Callable[[object, object], int]:
        """Create a comparator for string keys."""
        def compare(a: object, b: object) -> int:
            str_a = a if isinstance(a, str) else str(a)
            str_b = b if isinstance(b, str) else str(b)
            if str_a < str_b:
                return -1
            elif str_a > str_b:
                return 1
            return 0
        return compare


class LongSerializer(KeySerializer):
    """Serializer for BIGINT type."""

    def serialize(self, key: object) -> bytes:
        """Serialize a long key to bytes."""
        return struct.pack('>q', int(key))

    def deserialize(self, data: bytes) -> object:
        """Deserialize bytes to a long key."""
        return struct.unpack('>q', data)[0]

    def create_comparator(self) -> Callable[[object, object], int]:
        """Create a comparator for long keys."""
        def compare(a: object, b: object) -> int:
            long_a = int(a)
            long_b = int(b)
            if long_a < long_b:
                return -1
            elif long_a > long_b:
                return 1
            return 0
        return compare


class IntSerializer(KeySerializer):
    """Serializer for INT type."""

    def serialize(self, key: object) -> bytes:
        """Serialize an int key to bytes."""
        return struct.pack('>i', int(key))

    def deserialize(self, data: bytes) -> object:
        """Deserialize bytes to an int key."""
        return struct.unpack('>i', data)[0]

    def create_comparator(self) -> Callable[[object, object], int]:
        """Create a comparator for int keys."""
        def compare(a: object, b: object) -> int:
            int_a = int(a)
            int_b = int(b)
            if int_a < int_b:
                return -1
            elif int_a > int_b:
                return 1
            return 0
        return compare


class FloatSerializer(KeySerializer):
    """Serializer for FLOAT type."""

    def serialize(self, key: object) -> bytes:
        """Serialize a float key to bytes."""
        return struct.pack('>f', float(key))

    def deserialize(self, data: bytes) -> object:
        """Deserialize bytes to a float key."""
        return struct.unpack('>f', data)[0]

    def create_comparator(self) -> Callable[[object, object], int]:
        """Create a comparator for float keys."""
        def compare(a: object, b: object) -> int:
            float_a = float(a)
            float_b = float(b)
            if float_a < float_b:
                return -1
            elif float_a > float_b:
                return 1
            return 0
        return compare


class DoubleSerializer(KeySerializer):
    """Serializer for DOUBLE type."""

    def serialize(self, key: object) -> bytes:
        """Serialize a double key to bytes."""
        return struct.pack('>d', float(key))

    def deserialize(self, data: bytes) -> object:
        """Deserialize bytes to a double key."""
        return struct.unpack('>d', data)[0]

    def create_comparator(self) -> Callable[[object, object], int]:
        """Create a comparator for double keys."""
        def compare(a: object, b: object) -> int:
            double_a = float(a)
            double_b = float(b)
            if double_a < double_b:
                return -1
            elif double_a > double_b:
                return 1
            return 0
        return compare


class BooleanSerializer(KeySerializer):
    """Serializer for BOOLEAN type."""

    def serialize(self, key: object) -> bytes:
        """Serialize a boolean key to bytes."""
        return struct.pack('>B', 1 if key else 0)

    def deserialize(self, data: bytes) -> object:
        """Deserialize bytes to a boolean key."""
        return struct.unpack('>B', data)[0] == 1

    def create_comparator(self) -> Callable[[object, object], int]:
        """Create a comparator for boolean keys."""
        def compare(a: object, b: object) -> int:
            bool_a = bool(a)
            bool_b = bool(b)
            if bool_a < bool_b:
                return -1
            elif bool_a > bool_b:
                return 1
            return 0
        return compare


def create_serializer(data_type: str) -> KeySerializer:
    """
    Factory method to create a KeySerializer based on data type.
    
    Args:
        data_type: String representation of the data type
        
    Returns:
        Appropriate KeySerializer instance
        
    Raises:
        ValueError: If the data type is not supported
    """
    data_type_lower = data_type.lower()
    
    if data_type_lower in ('string', 'varchar', 'char'):
        return StringSerializer()
    elif data_type_lower in ('bigint', 'long'):
        return LongSerializer()
    elif data_type_lower in ('int', 'integer'):
        return IntSerializer()
    elif data_type_lower in ('float'):
        return FloatSerializer()
    elif data_type_lower in ('double'):
        return DoubleSerializer()
    elif data_type_lower in ('boolean', 'bool'):
        return BooleanSerializer()
    else:
        raise ValueError(f"DataType: {data_type} is not supported by btree index now.")
