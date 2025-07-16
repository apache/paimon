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
from typing import Any, Type, Dict

from .typedef import T


def json_field(json_name: str, **kwargs):
    """Create a field with custom JSON name"""
    return field(metadata={"json_name": json_name}, **kwargs)


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
        result = {}
        for field_info in fields(obj):
            field_value = getattr(obj, field_info.name)

            # Get custom JSON name from metadata
            json_name = field_info.metadata.get("json_name", field_info.name)

            # Handle nested objects
            if is_dataclass(field_value):
                result[json_name] = JSON.__to_dict(field_value)
            elif hasattr(field_value, "to_dict"):
                result[json_name] = field_value.to_dict()
            elif isinstance(field_value, list):
                result[json_name] = [
                    item.to_dict() if hasattr(item, "to_dict") else item
                    for item in field_value
                ]
            else:
                result[json_name] = field_value

        return result

    @staticmethod
    def __from_dict(data: Dict[str, Any], target_class: Type[T]) -> T:
        """Create instance from dictionary"""
        # Create field name mapping (json_name -> field_name)
        field_mapping = {}
        for field_info in fields(target_class):
            json_name = field_info.metadata.get("json_name", field_info.name)
            field_mapping[json_name] = field_info.name

        # Map JSON data to field names
        kwargs = {}
        for json_name, value in data.items():
            if json_name in field_mapping:
                field_name = field_mapping[json_name]
                kwargs[field_name] = value

        return target_class(**kwargs)
