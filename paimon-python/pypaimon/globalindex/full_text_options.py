# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Backend-neutral full-text index options and wire encoding."""

import json
from typing import Mapping


FULL_TEXT_IDENTIFIER = "full-text"


_FULL_TEXT_OPTION_PREFIX = "full-text."


class NativeFullTextIndexOptions:
    """Native full-text options with the public ``full-text.`` prefix removed."""

    def __init__(self, options=None):
        self._options = dict(options or {})

    @staticmethod
    def deserialize(data):
        if not data:
            return NativeFullTextIndexOptions()
        return NativeFullTextIndexOptions(json.loads(data.decode("utf-8")))

    @staticmethod
    def from_options(options: Mapping[str, object]):
        config = {}
        for key, value in options.items():
            key = str(key)
            if key.startswith(_FULL_TEXT_OPTION_PREFIX):
                key = key[len(_FULL_TEXT_OPTION_PREFIX):]
                config[key] = _option_value(value)
        return NativeFullTextIndexOptions._from_config(config)

    @staticmethod
    def _from_config(config):
        return NativeFullTextIndexOptions(
            {str(key): _option_value(value) for key, value in config.items()})

    def serialize(self) -> bytes:
        return json.dumps(self._options, separators=(",", ":")).encode("utf-8")

    def to_native_options(self):
        return dict(self._options)


def _option_value(value) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, list):
        return ";".join(str(item) for item in value)
    return str(value)
