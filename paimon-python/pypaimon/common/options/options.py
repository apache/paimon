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

from pypaimon.common.options import ConfigOption
from pypaimon.common.options.options_utils import OptionsUtils


class Options:
    def __init__(self, data: dict):
        self.data = data

    @classmethod
    def from_none(cls):
        return cls({})

    def to_map(self) -> dict:
        return self.data

    def get(self, key: ConfigOption, default=None):
        """
        Get the value for the given ConfigOption, with type conversion.
        Args:
            key: The ConfigOption to get the value for
            default: The default value to return if the ConfigOption is not found
        Returns:
            The converted value according to the ConfigOption's type, or default if not found
        """
        main_key = key.key()
        if main_key in self.data:
            raw_value = self.data[main_key]
            if raw_value is not None:
                return OptionsUtils.convert_value(raw_value, key.get_clazz())

        return default if default is not None else key.default_value()

    def set(self, key: ConfigOption, value):
        self.data[key.key()] = OptionsUtils.convert_to_string(value)

    def contains(self, key: ConfigOption):
        return key.key() in self.data

    def copy(self) -> 'Options':
        return Options(dict(self.data))
