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
from dataclasses import field, fields, is_dataclass
from typing import Any, Dict, Type, TypeVar, Union, List

T = TypeVar("T")


def json_field(json_name: str, **kwargs):
    """Create a field with custom JSON name"""
    return field(metadata={"json_name": json_name}, **kwargs)


def optional_json_field(json_name: str, json_include: str):
    """Create a field with custom JSON name"""
    return field(metadata={"json_name": json_name, "json_include": json_include}, default=None)


class JSON:

    @staticmethod
    def to_json(obj: Any, **kwargs) -> str:
        """Convert to JSON string"""
        return json.dumps(JSON.__to_dict(obj), ensure_ascii=False, **kwargs)

    @staticmethod
    def from_json(json_str: str, target_class: Type[T]) -> T:
        """Create instance from JSON string"""
        data = json.loads(json_str)
        return JSON.__from_dict(data, target_class)

    @staticmethod
    def __to_dict(obj: Any) -> Dict[str, Any]:
        """Convert to dictionary with custom field names"""
        # If object has custom to_dict method, use it
        if hasattr(obj, "to_dict") and callable(getattr(obj, "to_dict")):
            return obj.to_dict()

        # Otherwise, use dataclass field-by-field serialization
        result = {}
        for field_info in fields(obj):
            field_value = getattr(obj, field_info.name)

            # Get custom JSON name from metadata
            json_name = field_info.metadata.get("json_name", field_info.name)

            # Json include
            if field_value is None:
                if field_info.metadata.get("json_include", None) == "non_null":
                    continue

            # Handle nested objects
            if hasattr(field_value, "to_dict"):
                result[json_name] = field_value.to_dict()
            elif is_dataclass(field_value):
                result[json_name] = JSON.__to_dict(field_value)
            elif isinstance(field_value, list):
                result[json_name] = [
                    item.to_dict() if hasattr(item, "to_dict")
                    else JSON.__to_dict(item) if is_dataclass(item)
                    else item
                    for item in field_value
                ]
            else:
                result[json_name] = field_value

        return result

    @staticmethod
    def __from_dict(data: Dict[str, Any], target_class: Type[T]) -> T:
        """Create instance from dictionary"""
        # If target class has custom from_dict method, use it
        if hasattr(target_class, "from_dict") and callable(getattr(target_class, "from_dict")):
            return target_class.from_dict(data)

        # Otherwise, use dataclass field-by-field deserialization
        # Create field name mapping (json_name -> field_name)
        field_mapping = {}
        type_mapping = {}
        for field_info in fields(target_class):
            json_name = field_info.metadata.get("json_name", field_info.name)
            field_mapping[json_name] = field_info.name
            origin_type = getattr(field_info.type, '__origin__', None)
            args = getattr(field_info.type, '__args__', None)
            field_type = field_info.type
            if origin_type is Union and len(args) == 2:
                field_type = args[0]
            if is_dataclass(field_type):
                type_mapping[json_name] = field_type
            elif origin_type in (list, List) and is_dataclass(args[0]):
                type_mapping[json_name] = field_info.type

        # Map JSON data to field names
        kwargs = {}
        for json_name, value in data.items():
            if json_name in field_mapping:
                field_name = field_mapping[json_name]
                if json_name in type_mapping:
                    tp = getattr(type_mapping[json_name], '__origin__', None)
                    if tp in (list, List):
                        item_type = getattr(type_mapping[json_name], '__args__', None)[0]
                        if is_dataclass(item_type):
                            kwargs[field_name] = [
                                item_type.from_dict(item)
                                if hasattr(item_type, "to_dict")
                                else JSON.__from_dict(item, item_type)
                                for item in value]
                        else:
                            kwargs[field_name] = value
                    else:
                        kwargs[field_name] = JSON.__from_dict(value, type_mapping[json_name])
                else:
                    kwargs[field_name] = value

        return target_class(**kwargs)
