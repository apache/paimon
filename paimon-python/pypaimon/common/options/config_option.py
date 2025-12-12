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

from typing import Optional, Any, Type, Union, Generic, TypeVar
from dataclasses import dataclass


@dataclass(frozen=True)
class Description:
    """Configuration option description."""

    text: str

    @staticmethod
    def builder() -> 'DescriptionBuilder':
        """Create a description builder."""
        return DescriptionBuilder()


class DescriptionBuilder:
    """Builder for Description objects."""

    def __init__(self):
        self._text = ""

    def text(self, text: str) -> 'DescriptionBuilder':
        """Set the description text."""
        self._text = text
        return self

    def build(self) -> Description:
        """Build the Description object."""
        return Description(text=self._text)


T = TypeVar('T')


class ConfigOption(Generic[T]):
    """
    A ConfigOption describes a configuration parameter. It encapsulates the configuration
    key, deprecated older versions of the key, and an optional default value for the configuration
    parameter.

    ConfigOptions are built via the ConfigOptions class. Once created, a config
    option is immutable.
    """

    EMPTY_DESCRIPTION = Description(text="")

    def __init__(self,
                 key: str,
                 clazz: Type,
                 description: Optional[Description] = None,
                 default_value: Any = None):
        """
        Creates a new config option with fallback keys.

        Args:
            key: The current key for that config option
            clazz: Type of the ConfigOption value
            description: Description for that option
            default_value: The default value for this option
        """
        if not key:
            raise ValueError("Key must not be null.")

        self._key = key
        self._clazz = clazz
        self._description = description or self.EMPTY_DESCRIPTION
        self._default_value = default_value

    def key(self) -> str:
        """Gets the configuration key."""
        return self._key

    def get_clazz(self) -> Type:
        """Gets the type class of this config option."""
        return self._clazz

    def description(self) -> Description:
        """Returns the description of this option."""
        return self._description

    def has_default_value(self) -> bool:
        """Checks if this option has a default value."""
        return self._default_value is not None

    def default_value(self) -> Any:
        """Returns the default value, or None if there is no default value."""
        return self._default_value

    def with_description(self, description: Union[str, Description]) -> 'ConfigOption':
        """
        Creates a new config option, using this option's key and default value, and adding the given
        description. The given description is used when generation the configuration documentation.
        Args:
            description: The description for this option.
        Returns:
            A new config option, with given description.
        """
        if isinstance(description, str):
            desc = Description.builder().text(description).build()
        else:
            desc = description

        return ConfigOption(
            key=self._key,
            clazz=self._clazz,
            description=desc,
            default_value=self._default_value,
        )

    def __eq__(self, other) -> bool:
        """Check equality with another ConfigOption."""
        if not isinstance(other, ConfigOption):
            return False

        return (self._key == other._key and
                self._default_value == other._default_value)

    def __hash__(self) -> int:
        """Calculate hash code."""
        return hash((self._key, self._default_value))

    def __str__(self) -> str:
        """String representation of the config option."""
        return f"key: '{self._key}'; default_value: {self._default_value}"
