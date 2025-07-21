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
from enum import Enum
from typing import Dict, Any, Optional, List, Union


class AtomicInteger:

    def __init__(self, initial_value: int = 0):
        self._value = initial_value
        self._lock = threading.RLock()

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
        return {"type": self.type, "nullable": self.nullable}

    def __str__(self) -> str:
        null_suffix = "" if self.nullable else " NOT NULL"
        return f"{self.type}{null_suffix}"


@dataclass
class ArrayType(DataType):
    element: DataType

    def __init__(self, nullable: bool, element_type: DataType):
        super().__init__(nullable)
        self.element = element_type

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": f"ARRAY{'<' + str(self.element) + '>' if self.element else ''}",
            "element": self.element.to_dict() if self.element else None,
            "nullable": self.nullable,
        }

    def __str__(self) -> str:
        null_suffix = "" if self.nullable else " NOT NULL"
        return f"ARRAY<{self.element}>{null_suffix}"


@dataclass
class MultisetType(DataType):
    element: DataType

    def __init__(self, nullable: bool, element_type: DataType):
        super().__init__(nullable)
        self.element = element_type

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": f"MULTISET{'<' + str(self.element) + '>' if self.element else ''}",
            "element": self.element.to_dict() if self.element else None,
            "nullable": self.nullable,
        }

    def __str__(self) -> str:
        null_suffix = "" if self.nullable else " NOT NULL"
        return f"MULTISET<{self.element}>{null_suffix}"


@dataclass
class MapType(DataType):
    key: DataType
    value: DataType

    def __init__(
            self,
            nullable: bool,
            key_type: DataType,
            value_type: DataType):
        super().__init__(nullable)
        self.key = key_type
        self.value = value_type

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": f"MAP<{self.key}, {self.value}>",
            "key": self.key.to_dict() if self.key else None,
            "value": self.value.to_dict() if self.value else None,
            "nullable": self.nullable,
        }

    def __str__(self) -> str:
        null_suffix = "" if self.nullable else " NOT NULL"
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

    def __init__(
        self,
        id: int,
        name: str,
        type: DataType,
        description: Optional[str] = None,
        default_value: Optional[str] = None,
    ):
        self.id = id
        self.name = name
        self.type = type
        self.description = description
        self.default_value = default_value

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DataField":
        return DataTypeParser.parse_data_field(data)

    def to_dict(self) -> Dict[str, Any]:
        result = {
            self.FIELD_ID: self.id,
            self.FIELD_NAME: self.name,
            self.FIELD_TYPE: self.type.to_dict() if self.type else None,
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
            "type": "ROW" + ("" if self.nullable else " NOT NULL"),
            "fields": [field.to_dict() for field in self.fields],
            "nullable": self.nullable,
        }

    def __str__(self) -> str:
        field_strs = [f"{field.name}: {field.type}" for field in self.fields]
        null_suffix = "" if self.nullable else " NOT NULL"
        return f"ROW<{', '.join(field_strs)}>{null_suffix}"


class Keyword(Enum):
    CHAR = "CHAR"
    VARCHAR = "VARCHAR"
    STRING = "STRING"
    BOOLEAN = "BOOLEAN"
    BINARY = "BINARY"
    VARBINARY = "VARBINARY"
    BYTES = "BYTES"
    DECIMAL = "DECIMAL"
    NUMERIC = "NUMERIC"
    DEC = "DEC"
    TINYINT = "TINYINT"
    SMALLINT = "SMALLINT"
    INT = "INT"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    DATE = "DATE"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMP_LTZ = "TIMESTAMP_LTZ"
    VARIANT = "VARIANT"


class DataTypeParser:

    @staticmethod
    def parse_nullability(type_string: str) -> bool:
        if "NOT NULL" in type_string:
            return False
        elif "NULL" in type_string:
            return True
        return True

    @staticmethod
    def parse_atomic_type_sql_string(type_string: str) -> DataType:
        type_upper = type_string.upper().strip()

        if "(" in type_upper:
            base_type = type_upper.split("(")[0]
        else:
            base_type = type_upper

        try:
            Keyword(base_type)
            return AtomicType(
                type_string, DataTypeParser.parse_nullability(type_string)
            )
        except ValueError:
            raise Exception(f"Unknown type: {base_type}")

    @staticmethod
    def parse_data_type(
        json_data: Union[Dict[str, Any], str], field_id: Optional[AtomicInteger] = None
    ) -> DataType:

        if isinstance(json_data, str):
            return DataTypeParser.parse_atomic_type_sql_string(json_data)

        if isinstance(json_data, dict):
            if "type" not in json_data:
                raise ValueError(f"Missing 'type' field in JSON: {json_data}")

            type_string = json_data["type"]

            if type_string.startswith("ARRAY"):
                element = DataTypeParser.parse_data_type(
                    json_data.get("element"), field_id
                )
                nullable = "NOT NULL" not in type_string
                return ArrayType(nullable, element)

            elif type_string.startswith("MULTISET"):
                element = DataTypeParser.parse_data_type(
                    json_data.get("element"), field_id
                )
                nullable = "NOT NULL" not in type_string
                return MultisetType(nullable, element)

            elif type_string.startswith("MAP"):
                key = DataTypeParser.parse_data_type(
                    json_data.get("key"), field_id)
                value = DataTypeParser.parse_data_type(
                    json_data.get("value"), field_id)
                nullable = "NOT NULL" not in type_string
                return MapType(nullable, key, value)

            elif type_string.startswith("ROW"):
                field_array = json_data.get("fields", [])
                fields = []
                for field_json in field_array:
                    fields.append(
                        DataTypeParser.parse_data_field(
                            field_json, field_id))
                nullable = "NOT NULL" not in type_string
                return RowType(nullable, fields)

            else:
                return DataTypeParser.parse_atomic_type_sql_string(type_string)

        raise ValueError(f"Cannot parse data type: {json_data}")

    @staticmethod
    def parse_data_field(
        json_data: Dict[str, Any], field_id: Optional[AtomicInteger] = None
    ) -> DataField:

        if (
            DataField.FIELD_ID in json_data
            and json_data[DataField.FIELD_ID] is not None
        ):
            if field_id is not None and field_id.get() != -1:
                raise ValueError("Partial field id is not allowed.")
            field_id_value = int(json_data["id"])
        else:
            if field_id is None:
                raise ValueError(
                    "Field ID is required when not provided in JSON")
            field_id_value = field_id.increment_and_get()

        if DataField.FIELD_NAME not in json_data:
            raise ValueError("Missing 'name' field in JSON")
        name = json_data[DataField.FIELD_NAME]

        if DataField.FIELD_TYPE not in json_data:
            raise ValueError("Missing 'type' field in JSON")
        data_type = DataTypeParser.parse_data_type(
            json_data[DataField.FIELD_TYPE], field_id
        )

        description = json_data.get(DataField.FIELD_DESCRIPTION)

        default_value = json_data.get(DataField.FIELD_DEFAULT_VALUE)

        return DataField(
            id=field_id_value,
            name=name,
            type=data_type,
            description=description,
            default_value=default_value,
        )


def parse_data_type_from_json(
    json_str: str, field_id: Optional[AtomicInteger] = None
) -> DataType:
    json_data = json.loads(json_str)
    return DataTypeParser.parse_data_type(json_data, field_id)


def parse_data_field_from_json(
    json_str: str, field_id: Optional[AtomicInteger] = None
) -> DataField:
    json_data = json.loads(json_str)
    return DataTypeParser.parse_data_field(json_data, field_id)
