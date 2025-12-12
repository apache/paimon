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

from typing import Dict
from urllib.parse import unquote

from pypaimon.common.options import Options


class RESTUtil:
    @staticmethod
    def encode_string(value: str) -> str:
        import urllib.parse

        return urllib.parse.quote(value)

    @staticmethod
    def decode_string(encoded: str) -> str:
        """Decode URL-encoded string"""
        return unquote(encoded)

    @staticmethod
    def extract_prefix_map(
            options: Options, prefix: str) -> Dict[str, str]:
        result = {}
        config = options.to_map()
        for key, value in config.items():
            if key.startswith(prefix):
                new_key = key[len(prefix):]
                result[new_key] = str(value)
        return result
