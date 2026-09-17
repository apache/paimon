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

import copy
from dataclasses import dataclass, field
from typing import Dict, Optional, TypeVar
from urllib.parse import quote

T = TypeVar("T")


def _encode_string(value: str) -> str:
    if value is None:
        return value
    return quote(str(value), safe='')


@dataclass
class RESTAuthParameter:
    method: str
    path: str
    data: str
    parameters: Dict[str, str] = field(default_factory=dict)
    api_name: Optional[str] = None

    def __post_init__(self):
        if self.parameters:
            self.parameters = {
                k: _encode_string(v) for k, v in self.parameters.items()
            }

    def with_api_name(self, api_name: Optional[str]) -> "RESTAuthParameter":
        """Returns a copy naming the API this request calls; parameters are not encoded again."""
        named = copy.copy(self)
        named.api_name = api_name
        return named
