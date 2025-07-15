#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
import json
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, TypeVar, Dict, Any, List, Union

T = TypeVar('T')


@dataclass
class Identifier:
    """Table/View/Function identifier"""
    database_name: str
    object_name: str
    branch_name: Optional[str] = None

    @classmethod
    def create(cls, database_name: str, object_name: str) -> 'Identifier':
        return cls(database_name, object_name)

    @classmethod
    def from_string(cls, full_name: str) -> 'Identifier':
        parts = full_name.split('.')
        if len(parts) == 2:
            return cls(parts[0], parts[1])
        elif len(parts) == 3:
            return cls(parts[0], parts[1], parts[2])
        else:
            raise ValueError(f"Invalid identifier format: {full_name}")

    def get_full_name(self) -> str:
        if self.branch_name:
            return f"{self.database_name}.{self.object_name}.{self.branch_name}"
        return f"{self.database_name}.{self.object_name}"

    def get_database_name(self) -> str:
        return self.database_name

    def get_table_name(self) -> str:
        return self.object_name

    def get_object_name(self) -> str:
        return self.object_name

    def get_branch_name(self) -> Optional[str]:
        return self.branch_name

    def is_system_table(self) -> bool:
        return self.object_name.startswith('$')


class AtomicInteger:

    def __init__(self, initial_value: int = 0):
        self._value = initial_value
        self._lock = threading.Lock()

    def get(self) -> int:
        with self._lock:
            return self._value

    def increment_and_get(self) -> int:
        with self._lock:
            self._value += 1
            return self._value

    def get_and_increment(self) -> int:
        with self._lock:
            old_value = self._value
            self._value += 1
            return old_value

    def set(self, value: int):
        with self._lock:
            self._value = value


class DataType(ABC):
    def __init__(self, nullable: bool = True):
        self.nullable = nullable

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def __str__(self) -> str:
        pass


@dataclass
class AtomicType(DataType):
    type: str

    def __init__(self, type: str, nullable: bool = True):
        super().__init__(nullable)
        self.type = type

    def to_dict(self) -> Dict[str, Any]:
        return {
            'type': self.type,
            'nullable': self.nullable
        }

    def __str__(self) -> str:
        null_suffix = '' if self.nullable else ' NOT NULL'
        return f"{self.type}{null_suffix}"


@dataclass
class ArrayType(DataType):
    element: DataType

    def __init__(self, nullable: bool, element_type: DataType):
        super().__init__(nullable)
        self.element = element_type

    def to_dict(self) -> Dict[str, Any]:
        return {
            'type': f"ARRAY{'<' + str(self.element) + '>' if self.element else ''}",
            'element': self.element.to_dict() if self.element else None,
            'nullable': self.nullable
        }

    def __str__(self) -> str:
        null_suffix = '' if self.nullable else ' NOT NULL'
        return f"ARRAY<{self.element}>{null_suffix}"


@dataclass
@dataclass
class MultisetType(DataType):
    element: DataType

    def __init__(self, nullable: bool, element_type: DataType):
        super().__init__(nullable)
        self.element = element_type

    def to_dict(self) -> Dict[str, Any]:
        return {
            'type': f"MULTISET{'<' + str(self.element) + '>' if self.element else ''}",
            'element': self.element.to_dict() if self.element else None,
            'nullable': self.nullable
        }

    def __str__(self) -> str:
        null_suffix = '' if self.nullable else ' NOT NULL'
        return f"MULTISET<{self.element}>{null_suffix}"


@dataclass
class MapType(DataType):
    key: DataType
    value: DataType

    def __init__(self, nullable: bool, key_type: DataType, value_type: DataType):
        super().__init__(nullable)
        self.key = key_type
        self.value = value_type

    def to_dict(self) -> Dict[str, Any]:
        return {
            'type': f"MAP<{self.key}, {self.value}>",
            'key': self.key.to_dict() if self.key else None,
            'value': self.value.to_dict() if self.value else None,
            'nullable': self.nullable
        }

    def __str__(self) -> str:
        null_suffix = '' if self.nullable else ' NOT NULL'
        return f"MAP<{self.key}, {self.value}>{null_suffix}"


@dataclass
class DataField:
    FIELD_ID = "id"
    FIELD_NAME = "name"
    FIELD_TYPE = "type"
    FIELD_DESCRIPTION = "description"
    FIELD_DEFAULT_VALUE = "defaultValue"

    id: int
    name: str
    type: DataType
    description: Optional[str] = None
    default_value: Optional[str] = None

    def __init__(self, id: int, name: str, type: DataType, description: Optional[str] = None,
                 default_value: Optional[str] = None):
        self.id = id
        self.name = name
        self.type = type
        self.description = description
        self.default_value = default_value

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DataField':
        return DataTypeParser.parse_data_field(data)

    def to_dict(self) -> Dict[str, Any]:
        result = {
            self.FIELD_ID: self.id,
            self.FIELD_NAME: self.name,
            self.FIELD_TYPE: self.type.to_dict() if self.type else None
        }

        if self.description is not None:
            result[self.FIELD_DESCRIPTION] = self.description

        if self.default_value is not None:
            result[self.FIELD_DEFAULT_VALUE] = self.default_value

        return result


@dataclass
class RowType(DataType):
    fields: List[DataField]

    def __init__(self, nullable: bool, fields: List[DataField]):
        super().__init__(nullable)
        self.fields = fields or []

    def to_dict(self) -> Dict[str, Any]:
        return {
            'type': 'ROW' + ('' if self.nullable else ' NOT NULL'),
            'fields': [field.to_dict() for field in self.fields],
            'nullable': self.nullable
        }

    def __str__(self) -> str:
        field_strs = [f"{field.name}: {field.type}" for field in self.fields]
        null_suffix = '' if self.nullable else ' NOT NULL'
        return f"ROW<{', '.join(field_strs)}>{null_suffix}"


class DataTypeParser:
    ATOMIC_TYPES = {
        'BOOLEAN', 'TINYINT', 'SMALLINT', 'INT', 'INTEGER', 'BIGINT',
        'FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC',
        'CHAR', 'VARCHAR', 'STRING', 'TEXT',
        'BINARY', 'VARBINARY', 'BYTES',
        'DATE', 'TIME', 'TIMESTAMP', 'TIMESTAMP_LTZ',
        'INTERVAL', 'NULL'
    }

    @staticmethod
    def parse_atomic_type_sql_string(type_string: str) -> DataType:
        type_upper = type_string.upper().strip()

        if '(' in type_upper:
            base_type = type_upper.split('(')[0]
        else:
            base_type = type_upper

        if base_type in DataTypeParser.ATOMIC_TYPES:
            return AtomicType(type_string)

        return AtomicType(type_string)

    @staticmethod
    def parse_data_type(json_data: Union[Dict[str, Any], str], field_id: Optional[AtomicInteger] = None) -> DataType:

        if isinstance(json_data, str):
            return DataTypeParser.parse_atomic_type_sql_string(json_data)

        if isinstance(json_data, dict):
            if 'type' not in json_data:
                raise ValueError(f"Missing 'type' field in JSON: {json_data}")

            type_string = json_data['type']

            if type_string.startswith("ARRAY"):
                element = DataTypeParser.parse_data_type(json_data.get('element'), field_id)
                nullable = 'NOT NULL' not in type_string
                return ArrayType(nullable, element)

            elif type_string.startswith("MULTISET"):
                element = DataTypeParser.parse_data_type(json_data.get('element'), field_id)
                nullable = 'NOT NULL' not in type_string
                return MultisetType(nullable, element)

            elif type_string.startswith("MAP"):
                key = DataTypeParser.parse_data_type(json_data.get('key'), field_id)
                value = DataTypeParser.parse_data_type(json_data.get('value'), field_id)
                nullable = 'NOT NULL' not in type_string
                return MapType(nullable, key, value)

            elif type_string.startswith("ROW"):
                field_array = json_data.get('fields', [])
                fields = []
                for field_json in field_array:
                    fields.append(DataTypeParser.parse_data_field(field_json, field_id))
                nullable = 'NOT NULL' not in type_string
                return RowType(nullable, fields)

            else:
                return DataTypeParser.parse_atomic_type_sql_string(type_string)

        raise ValueError(f"Cannot parse data type: {json_data}")

    @staticmethod
    def parse_data_field(json_data: Dict[str, Any], field_id: Optional[AtomicInteger] = None) -> DataField:

        if DataField.FIELD_ID in json_data and json_data[DataField.FIELD_ID] is not None:
            if field_id is not None and field_id.get() != -1:
                raise ValueError("Partial field id is not allowed.")
            field_id_value = int(json_data['id'])
        else:
            if field_id is None:
                raise ValueError("Field ID is required when not provided in JSON")
            field_id_value = field_id.increment_and_get()

        if DataField.FIELD_NAME not in json_data:
            raise ValueError("Missing 'name' field in JSON")
        name = json_data[DataField.FIELD_NAME]

        if DataField.FIELD_TYPE not in json_data:
            raise ValueError("Missing 'type' field in JSON")
        data_type = DataTypeParser.parse_data_type(json_data[DataField.FIELD_TYPE], field_id)

        description = json_data.get(DataField.FIELD_DESCRIPTION)

        default_value = json_data.get(DataField.FIELD_DEFAULT_VALUE)

        return DataField(
            id=field_id_value,
            name=name,
            type=data_type,
            description=description,
            default_value=default_value
        )


def parse_data_type_from_json(json_str: str, field_id: Optional[AtomicInteger] = None) -> DataType:
    json_data = json.loads(json_str)
    return DataTypeParser.parse_data_type(json_data, field_id)


def parse_data_field_from_json(json_str: str, field_id: Optional[AtomicInteger] = None) -> DataField:
    json_data = json.loads(json_str)
    return DataTypeParser.parse_data_field(json_data, field_id)
