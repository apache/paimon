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
from dataclasses import asdict
from typing import TypeVar, Any, Type

T = TypeVar('T')


class JSON:
    """Universal JSON serializer"""

    @staticmethod
    def from_json(json_str: str, target_class: Type[T]) -> T:
        data = json.loads(json_str)
        if hasattr(target_class, 'from_dict'):
            return target_class.from_dict(data)
        return data

    @staticmethod
    def to_json(obj: Any) -> str:
        """Serialize any object to JSON"""
        return json.dumps(obj, default=JSON._default_serializer)

    @staticmethod
    def _default_serializer(obj):
        """Default serialization handler"""

        # Handle objects with to_dict method
        if hasattr(obj, 'to_dict') and callable(obj.to_dict):
            return obj.to_dict()

        # Handle dataclass objects
        if hasattr(obj, '__dataclass_fields__'):
            return asdict(obj)

        raise TypeError(f"Object of type {type(obj).__name__} is not JSON")