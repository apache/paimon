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

import re
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Union

import pyarrow
from pyarrow import types


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

    def to_dict(self) -> str:
        if not self.nullable:
            return self.type + " NOT NULL"
        return self.type

    @classmethod
    def from_dict(cls, data: str) -> "AtomicType":
        return DataTypeParser.parse_data_type(data)

    def __str__(self) -> str:
        null_suffix = "" if self.nullable else " NOT NULL"
        return "{}{}".format(self.type, null_suffix)


@dataclass
class ArrayType(DataType):
    element: DataType

    def __init__(self, nullable: bool, element_type: DataType):
        super().__init__(nullable)
        self.element = element_type

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "ARRAY" + (" NOT NULL" if not self.nullable else ""),
            "element": self.element.to_dict() if self.element else None,
            "nullable": self.nullable
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ArrayType":
        return DataTypeParser.parse_data_type(data)

    def __str__(self) -> str:
        null_suffix = "" if self.nullable else " NOT NULL"
        return "ARRAY<{}>{}".format(self.element, null_suffix)


@dataclass
class MultisetType(DataType):
    element: DataType

    def __init__(self, nullable: bool, element_type: DataType):
        super().__init__(nullable)
        self.element = element_type

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "MULTISET{}{}".format('<' + str(self.element) + '>' if self.element else '',
                                          " NOT NULL" if not self.nullable else ""),
            "element": self.element.to_dict() if self.element else None,
            "nullable": self.nullable,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MultisetType":
        return DataTypeParser.parse_data_type(data)

    def __str__(self) -> str:
        null_suffix = "" if self.nullable else " NOT NULL"
        return "MULTISET<{}>{}".format(self.element, null_suffix)


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
            "type": "MAP<{}, {}>".format(self.key, self.value),
            "key": self.key.to_dict() if self.key else None,
            "value": self.value.to_dict() if self.value else None,
            "nullable": self.nullable,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MapType":
        return DataTypeParser.parse_data_type(data)

    def __str__(self) -> str:
        null_suffix = "" if self.nullable else " NOT NULL"
        return "MAP<{}, {}>{}".format(self.key, self.value, null_suffix)


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

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RowType":
        return DataTypeParser.parse_data_type(data)

    def __str__(self) -> str:
        field_strs = []
        for field in self.fields:
            description = " COMMENT {}".format(field.description) if field.description else ""
            field_strs.append("{}: {}{}".format(field.name, field.type, description))
        null_suffix = "" if self.nullable else " NOT NULL"
        return "ROW<{}>{}".format(', '.join(field_strs), null_suffix)


class Keyword(Enum):
    CHAR = "CHAR"
    VARCHAR = "VARCHAR"
    STRING = "STRING"
    BOOLEAN = "BOOLEAN"
    BINARY = "BINARY"
    VARBINARY = "VARBINARY"
    BYTES = "BYTES"
    BLOB = "BLOB"
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
        elif " " in type_upper:
            base_type = type_upper.split(" ")[0]
            type_upper = base_type
        else:
            base_type = type_upper

        try:
            Keyword(base_type)
            return AtomicType(
                type_upper, DataTypeParser.parse_nullability(type_string)
            )
        except ValueError:
            raise Exception("Unknown type: {}".format(base_type))

    @staticmethod
    def parse_data_type(
            json_data: Union[Dict[str, Any], str], field_id: Optional[AtomicInteger] = None
    ) -> DataType:

        if isinstance(json_data, str):
            return DataTypeParser.parse_atomic_type_sql_string(json_data)

        if isinstance(json_data, dict):
            if "type" not in json_data:
                raise ValueError("Missing 'type' field in JSON: {}".format(json_data))

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

        raise ValueError("Cannot parse data type: {}".format(json_data))

    @staticmethod
    def parse_data_field(
            json_data: Dict[str, Any], field_id: Optional[AtomicInteger] = None
    ) -> DataField:
        if DataField.FIELD_ID in json_data and json_data[DataField.FIELD_ID] is not None:
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


class PyarrowFieldParser:

    @staticmethod
    def from_paimon_type(data_type: DataType) -> pyarrow.DataType:
        # Based on Paimon DataTypes Doc: https://paimon.apache.org/docs/master/concepts/data-types/
        if isinstance(data_type, AtomicType):
            type_name = data_type.type.upper()
            if type_name == 'TINYINT':
                return pyarrow.int8()
            elif type_name == 'SMALLINT':
                return pyarrow.int16()
            elif type_name == 'INT':
                return pyarrow.int32()
            elif type_name == 'BIGINT':
                return pyarrow.int64()
            elif type_name == 'FLOAT':
                return pyarrow.float32()
            elif type_name == 'DOUBLE':
                return pyarrow.float64()
            elif type_name == 'BOOLEAN':
                return pyarrow.bool_()
            elif type_name == 'STRING' or type_name.startswith('CHAR') or type_name.startswith('VARCHAR'):
                return pyarrow.string()
            elif type_name == 'BYTES' or type_name.startswith('VARBINARY'):
                return pyarrow.binary()
            elif type_name == 'BLOB':
                return pyarrow.large_binary()
            elif type_name.startswith('BINARY'):
                if type_name == 'BINARY':
                    return pyarrow.binary(1)
                match = re.fullmatch(r'BINARY\((\d+)\)', type_name)
                if match:
                    length = int(match.group(1))
                    if length > 0:
                        return pyarrow.binary(length)
            elif type_name.startswith('DECIMAL'):
                if type_name == 'DECIMAL':
                    return pyarrow.decimal128(10, 0)  # default to 10, 0
                match_ps = re.fullmatch(r'DECIMAL\((\d+),\s*(\d+)\)', type_name)
                if match_ps:
                    precision, scale = map(int, match_ps.groups())
                    return pyarrow.decimal128(precision, scale)
                match_p = re.fullmatch(r'DECIMAL\((\d+)\)', type_name)
                if match_p:
                    precision = int(match_p.group(1))
                    return pyarrow.decimal128(precision, 0)
            if type_name.startswith('TIMESTAMP'):
                # WITH_LOCAL_TIME_ZONE is ambiguous and not supported
                if type_name == 'TIMESTAMP':
                    return pyarrow.timestamp('us', tz=None)  # default to 6
                match = re.fullmatch(r'TIMESTAMP\((\d+)\)', type_name)
                if match:
                    precision = int(match.group(1))
                    if precision == 0:
                        return pyarrow.timestamp('s', tz=None)
                    elif 1 <= precision <= 3:
                        return pyarrow.timestamp('ms', tz=None)
                    elif 4 <= precision <= 6:
                        return pyarrow.timestamp('us', tz=None)
                    elif 7 <= precision <= 9:
                        return pyarrow.timestamp('ns', tz=None)
            elif type_name == 'DATE':
                return pyarrow.date32()
            if type_name.startswith('TIME'):
                if type_name == 'TIME':
                    return pyarrow.time64('us')  # default to 6
                match = re.fullmatch(r'TIME\((\d+)\)', type_name)
                if match:
                    precision = int(match.group(1))
                    if precision == 0:
                        return pyarrow.time32('s')
                    if 1 <= precision <= 3:
                        return pyarrow.time32('ms')
                    if 4 <= precision <= 6:
                        return pyarrow.time64('us')
                    if 7 <= precision <= 9:
                        return pyarrow.time64('ns')
        elif isinstance(data_type, ArrayType):
            return pyarrow.list_(PyarrowFieldParser.from_paimon_type(data_type.element))
        elif isinstance(data_type, MapType):
            key_type = PyarrowFieldParser.from_paimon_type(data_type.key)
            value_type = PyarrowFieldParser.from_paimon_type(data_type.value)
            return pyarrow.map_(key_type, value_type)
        raise ValueError("Unsupported data type: {}".format(data_type))

    @staticmethod
    def from_paimon_field(data_field: DataField) -> pyarrow.Field:
        pa_field_type = PyarrowFieldParser.from_paimon_type(data_field.type)
        metadata = {}
        if data_field.description:
            metadata[b'description'] = data_field.description.encode('utf-8')
        return pyarrow.field(data_field.name, pa_field_type, nullable=data_field.type.nullable, metadata=metadata)

    @staticmethod
    def from_paimon_schema(data_fields: List[DataField]):
        pa_fields = []
        for field in data_fields:
            pa_fields.append(PyarrowFieldParser.from_paimon_field(field))
        return pyarrow.schema(pa_fields)

    @staticmethod
    def to_paimon_type(pa_type: pyarrow.DataType, nullable: bool) -> DataType:
        # Based on Arrow DataTypes Doc: https://arrow.apache.org/docs/python/api/datatypes.html
        # All safe mappings are already implemented, adding new mappings requires rigorous evaluation
        # to avoid potential data loss
        type_name = None
        if types.is_int8(pa_type):
            type_name = 'TINYINT'
        elif types.is_int16(pa_type):
            type_name = 'SMALLINT'
        elif types.is_int32(pa_type):
            type_name = 'INT'
        elif types.is_int64(pa_type):
            type_name = 'BIGINT'
        elif types.is_float32(pa_type):
            type_name = 'FLOAT'
        elif types.is_float64(pa_type):
            type_name = 'DOUBLE'
        elif types.is_boolean(pa_type):
            type_name = 'BOOLEAN'
        elif types.is_string(pa_type):
            type_name = 'STRING'
        elif types.is_fixed_size_binary(pa_type):
            type_name = f'BINARY({pa_type.byte_width})'
        elif types.is_binary(pa_type):
            type_name = 'BYTES'
        elif types.is_large_binary(pa_type):
            type_name = 'BLOB'
        elif types.is_decimal(pa_type):
            type_name = f'DECIMAL({pa_type.precision}, {pa_type.scale})'
        elif types.is_timestamp(pa_type) and pa_type.tz is None:
            precision_mapping = {'s': 0, 'ms': 3, 'us': 6, 'ns': 9}
            type_name = f'TIMESTAMP({precision_mapping[pa_type.unit]})'
        elif types.is_date32(pa_type):
            type_name = 'DATE'
        elif types.is_time(pa_type):
            precision_mapping = {'s': 0, 'ms': 3, 'us': 6, 'ns': 9}
            type_name = f'TIME({precision_mapping[pa_type.unit]})'
        elif types.is_list(pa_type) or types.is_large_list(pa_type):
            pa_type: pyarrow.ListType
            element_type = PyarrowFieldParser.to_paimon_type(pa_type.value_type, nullable)
            return ArrayType(nullable, element_type)
        elif types.is_map(pa_type):
            pa_type: pyarrow.MapType
            key_type = PyarrowFieldParser.to_paimon_type(pa_type.key_type, nullable)
            value_type = PyarrowFieldParser.to_paimon_type(pa_type.item_type, nullable)
            return MapType(nullable, key_type, value_type)
        if type_name is not None:
            return AtomicType(type_name, nullable)
        raise ValueError("Unsupported pyarrow type: {}".format(pa_type))

    @staticmethod
    def to_paimon_field(field_idx: int, pa_field: pyarrow.Field) -> DataField:
        data_type = PyarrowFieldParser.to_paimon_type(pa_field.type, pa_field.nullable)
        description = pa_field.metadata.get(b'description', b'').decode('utf-8') \
            if pa_field.metadata and b'description' in pa_field.metadata else None
        return DataField(
            id=field_idx,
            name=pa_field.name,
            type=data_type,
            description=description
        )

    @staticmethod
    def to_paimon_schema(pa_schema: pyarrow.Schema) -> List[DataField]:
        # Convert PyArrow schema to Paimon fields
        fields = []
        for i, pa_field in enumerate(pa_schema):
            pa_field: pyarrow.Field
            data_field = PyarrowFieldParser.to_paimon_field(i, pa_field)
            fields.append(data_field)
        return fields

    @staticmethod
    def to_avro_type(field_type: pyarrow.DataType, field_name: str) -> Union[str, Dict[str, Any]]:
        if pyarrow.types.is_integer(field_type):
            if (pyarrow.types.is_signed_integer(field_type) and field_type.bit_width <= 32) or \
               (pyarrow.types.is_unsigned_integer(field_type) and field_type.bit_width < 32):
                return "int"
            else:
                return "long"
        elif pyarrow.types.is_float32(field_type):
            return "float"
        elif pyarrow.types.is_float64(field_type):
            return "double"
        elif pyarrow.types.is_boolean(field_type):
            return "boolean"
        elif pyarrow.types.is_string(field_type) or pyarrow.types.is_large_string(field_type):
            return "string"
        elif pyarrow.types.is_binary(field_type) or pyarrow.types.is_large_binary(field_type):
            return "bytes"
        elif pyarrow.types.is_decimal(field_type):
            return {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": field_type.precision,
                "scale": field_type.scale,
            }
        elif pyarrow.types.is_date(field_type):
            return {"type": "int", "logicalType": "date"}
        elif pyarrow.types.is_timestamp(field_type):
            unit = field_type.unit
            if unit == 'us':
                return {"type": "long", "logicalType": "timestamp-micros"}
            elif unit == 'ms':
                return {"type": "long", "logicalType": "timestamp-millis"}
            else:
                return {"type": "long", "logicalType": "timestamp-micros"}
        elif pyarrow.types.is_list(field_type) or pyarrow.types.is_large_list(field_type):
            value_field = field_type.value_field
            return {
                "type": "array",
                "items": PyarrowFieldParser.to_avro_type(value_field.type, value_field.name)
            }
        elif pyarrow.types.is_struct(field_type):
            return PyarrowFieldParser.to_avro_schema(field_type, name="{}_record".format(field_name))

        raise ValueError("Unsupported pyarrow type for Avro conversion: {}".format(field_type))

    @staticmethod
    def to_avro_schema(pyarrow_schema: Union[pyarrow.Schema, pyarrow.StructType],
                       name: str = "Root",
                       namespace: str = "pyarrow.avro"
                       ) -> Dict[str, Any]:
        fields = []
        for field in pyarrow_schema:
            avro_type = PyarrowFieldParser.to_avro_type(field.type, field.name)
            if field.nullable:
                avro_type = ["null", avro_type]
            fields.append({"name": field.name, "type": avro_type})
        return {
            "type": "record",
            "name": name,
            "namespace": namespace,
            "fields": fields,
        }
