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

from abc import ABC, abstractmethod
from typing import Dict, Optional

from pypaimon.api.typedef import RESTAuthParameter


class AuthProvider(ABC):

    @abstractmethod
    def merge_auth_header(
            self, base_header: Dict[str, str], parameter: RESTAuthParameter
    ) -> Dict[str, str]:
        """Merge authorization header into header."""


class RESTAuthFunction:

    def __init__(self,
                 init_header: Dict[str, str],
                 auth_provider: AuthProvider,
                 api_name: Optional[str] = None):
        self.init_header = init_header.copy() if init_header else {}
        self.auth_provider = auth_provider
        self.api_name = api_name

    def with_api_name(self, api_name: str) -> "RESTAuthFunction":
        """Returns a function that tags every request it signs with the given API name."""
        return RESTAuthFunction(self.init_header, self.auth_provider, api_name)

    def __call__(
            self, rest_auth_parameter: RESTAuthParameter) -> Dict[str, str]:
        if self.api_name is not None:
            rest_auth_parameter = rest_auth_parameter.with_api_name(self.api_name)
        return self.auth_provider.merge_auth_header(
            self.init_header, rest_auth_parameter
        )

    def apply(self, rest_auth_parameter: RESTAuthParameter) -> Dict[str, str]:
        return self.__call__(rest_auth_parameter)
