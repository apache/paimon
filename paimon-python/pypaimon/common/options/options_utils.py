"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from enum import Enum
from typing import Any, Type

from pypaimon.common.memory_size import MemorySize


class OptionsUtils:
    """Utility methods for options conversion and validation."""

    @staticmethod
    def convert_value(value: Any, target_type: Type) -> Any:
        """
        Convert a value to the target type.

        Args:
            value: The value to convert
            target_type: The target type to convert to

        Returns:
            The converted value

        Raises:
            ValueError: If the conversion is not possible
        """
        if value is None:
            return None

        if isinstance(value, target_type):
            return value

        try:
            if issubclass(target_type, Enum):
                return OptionsUtils.convert_to_enum(value, target_type)
        except TypeError:
            pass

        # Handle string conversions
        if target_type == str:
            return OptionsUtils.convert_to_string(value)
        elif target_type == bool:
            return OptionsUtils.convert_to_boolean(value)
        elif target_type == int:
            return OptionsUtils.convert_to_int(value)
        elif target_type == float:
            return OptionsUtils.convert_to_double(value)
        elif target_type == MemorySize:
            return OptionsUtils.convert_to_memory_size(value)
        else:
            raise ValueError(f"Unsupported type: {target_type}")

    @staticmethod
    def convert_to_string(value: Any) -> str:
        """Convert value to string."""
        return str(value)

    @staticmethod
    def convert_to_boolean(value: Any) -> bool:
        """Convert value to boolean."""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lower_value = value.lower().strip()
            if lower_value in ('true', '1', 'yes', 'on'):
                return True
            elif lower_value in ('false', '0', 'no', 'off'):
                return False
            else:
                raise ValueError(f"Cannot convert '{value}' to boolean")
        elif isinstance(value, (int, float)):
            return bool(value)
        else:
            raise ValueError(f"Cannot convert {type(value)} to boolean")

    @staticmethod
    def convert_to_int(value: Any) -> int:
        """Convert value to integer."""
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            return int(value.strip())
        if isinstance(value, float):
            return int(value)
        raise ValueError(f"Cannot convert {type(value)} to int")

    @staticmethod
    def convert_to_long(value: Any) -> int:
        """Convert value to long (same as int in Python)."""
        return OptionsUtils.convert_to_int(value)

    @staticmethod
    def convert_to_double(value: Any) -> float:
        """Convert value to double (float in Python)."""
        if isinstance(value, float):
            return value
        if isinstance(value, str):
            return float(value.strip())
        if isinstance(value, int):
            return float(value)
        raise ValueError(f"Cannot convert {type(value)} to float")

    @staticmethod
    def convert_to_memory_size(value: Any) -> MemorySize:
        """Convert value to MemorySize."""
        if isinstance(value, MemorySize):
            return value
        if isinstance(value, str):
            return MemorySize.parse(value)
        raise ValueError(f"Cannot convert {type(value)} to MemorySize")

    @staticmethod
    def convert_to_enum(value: Any, enum_class: Type[Enum]) -> Enum:

        if isinstance(value, enum_class):
            return value

        if isinstance(value, str):
            value_lower = value.lower().strip()
            for enum_member in enum_class:
                if enum_member.value.lower() == value_lower:
                    return enum_member
            try:
                return enum_class[value.upper()]
            except KeyError:
                raise ValueError(
                    f"Cannot convert '{value}' to {enum_class.__name__}. "
                    f"Valid values: {[e.value for e in enum_class]}"
                )
        elif isinstance(value, Enum):
            for enum_member in enum_class:
                if enum_member.value == value.value:
                    return enum_member
            raise ValueError(
                f"Cannot convert {value} (from {type(value).__name__}) to {enum_class.__name__}"
            )
        else:
            raise ValueError(f"Cannot convert {type(value)} to {enum_class.__name__}")
