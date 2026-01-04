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

from typing import Dict, Type, TypeVar, Generic
from datetime import timedelta
from enum import Enum

from .config_option import ConfigOption, Description
from ..memory_size import MemorySize

T = TypeVar('T')


class ConfigOptions:
    """
    ConfigOptions are used to build a ConfigOption. The option is typically built in
    one of the following pattern:

    Examples:
        # simple string-valued option with a default value
        temp_dirs = ConfigOptions.key("tmp.dir").string_type().default_value("/tmp")

        # simple integer-valued option with a default value
        parallelism = ConfigOptions.key("application.parallelism").int_type().default_value(100)

        # option with no default value
        user_name = ConfigOptions.key("user.name").string_type().no_default_value()
    """

    @staticmethod
    def key(key: str) -> 'OptionBuilder':
        """
        Starts building a new ConfigOption.

        Args:
            key: The key for the config option.

        Returns:
            The builder for the config option with the given key.
        """
        if not key:
            raise ValueError("Key must not be None or empty.")
        return ConfigOptions.OptionBuilder(key)

    class OptionBuilder:
        """
        The option builder is used to create a ConfigOption. It is instantiated via
        ConfigOptions.key(String).
        """

        def __init__(self, key: str):
            """
            Creates a new OptionBuilder.

            Args:
                key: The key for the config option
            """
            self.key = key

        def boolean_type(self) -> 'TypedConfigOptionBuilder[bool]':
            """Defines that the value of the option should be of bool type."""
            return ConfigOptions.TypedConfigOptionBuilder(self.key, bool)

        def int_type(self) -> 'TypedConfigOptionBuilder[int]':
            """Defines that the value of the option should be of int type."""
            return ConfigOptions.TypedConfigOptionBuilder(self.key, int)

        def long_type(self) -> 'TypedConfigOptionBuilder[int]':
            """Defines that the value of the option should be of long (int) type."""
            return ConfigOptions.TypedConfigOptionBuilder(self.key, int)

        def float_type(self) -> 'TypedConfigOptionBuilder[float]':
            """Defines that the value of the option should be of float type."""
            return ConfigOptions.TypedConfigOptionBuilder(self.key, float)

        def double_type(self) -> 'TypedConfigOptionBuilder[float]':
            """Defines that the value of the option should be of double (float) type."""
            return ConfigOptions.TypedConfigOptionBuilder(self.key, float)

        def string_type(self) -> 'TypedConfigOptionBuilder[str]':
            """Defines that the value of the option should be of str type."""
            return ConfigOptions.TypedConfigOptionBuilder(self.key, str)

        def duration_type(self) -> 'TypedConfigOptionBuilder[timedelta]':
            """Defines that the value of the option should be of timedelta type."""
            return ConfigOptions.TypedConfigOptionBuilder(self.key, timedelta)

        def memory_type(self) -> 'TypedConfigOptionBuilder[MemorySize]':
            """Defines that the value of the option should be of MemorySize type."""
            return ConfigOptions.TypedConfigOptionBuilder(self.key, MemorySize)

        def enum_type(self, enum_class: Type[T]) -> 'TypedConfigOptionBuilder[T]':
            """
            Defines that the value of the option should be of Enum type.

            Args:
                enum_class: Concrete type of the expected enum.
            """
            if not issubclass(enum_class, Enum):
                raise ValueError("enum_class must be a subclass of Enum")
            return ConfigOptions.TypedConfigOptionBuilder(self.key, enum_class)

        def map_type(self) -> 'TypedConfigOptionBuilder[Dict[str, str]]':
            """
            Defines that the value of the option should be a set of properties,
            which can be represented as Dict[str, str].
            """
            return ConfigOptions.TypedConfigOptionBuilder(self.key, Dict[str, str])

        def default_value(self, value: T) -> 'ConfigOption[T]':
            """
            Creates a ConfigOption with the given default value.

            This method does not accept None. For options with no default value, choose
            one of the no_default_value methods.

            Args:
                value: The default value for the config option

            Returns:
                The config option with the default value.

            Note:
                This method is deprecated. Define the type explicitly first with one of the
                *_type() methods.
            """
            if value is None:
                raise ValueError("Value must not be None.")
            return ConfigOption(
                key=self.key,
                clazz=type(value),
                description=ConfigOption.EMPTY_DESCRIPTION,
                default_value=value
            )

        def no_default_value(self) -> ConfigOption[str]:
            """
            Creates a string-valued option with no default value. String-valued options are
            the only ones that can have no default value.

            Returns:
                The created ConfigOption.

            Note:
                This method is deprecated. Define the type explicitly first with one of the
                *_type() methods.
            """
            return ConfigOption(
                key=self.key,
                clazz=str,
                description=ConfigOption.EMPTY_DESCRIPTION,
                default_value=None
            )

    class TypedConfigOptionBuilder(Generic[T]):
        """
        Builder for ConfigOption with a defined atomic type.
        """

        def __init__(self, key: str, clazz: Type[T]):
            """
            Create a new TypedConfigOptionBuilder.

            Args:
                key: The configuration key
                clazz: The type class for this option
            """
            self.key = key
            self.clazz = clazz

        def default_value(self, value: T) -> ConfigOption[T]:
            """
            Creates a ConfigOption with the given default value.

            Args:
                value: The default value for the config option

            Returns:
                The config option with the default value.
            """
            return ConfigOption(
                key=self.key,
                clazz=self.clazz,
                description=ConfigOption.EMPTY_DESCRIPTION,
                default_value=value
            )

        def no_default_value(self) -> ConfigOption[T]:
            """
            Creates a ConfigOption without a default value.

            Returns:
                The config option without a default value.
            """
            return ConfigOption(
                key=self.key,
                clazz=self.clazz,
                description=Description.builder().text("").build(),
                default_value=None
            )
