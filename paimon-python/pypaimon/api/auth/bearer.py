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

from pypaimon.api.auth.base import AuthProvider
from pypaimon.api.typedef import RESTAuthParameter


class BearTokenAuthProvider(AuthProvider):

    def __init__(self, token: str):
        self.token = token

    def merge_auth_header(
            self, base_header: Dict[str, str], rest_auth_parameter: RESTAuthParameter
    ) -> Dict[str, str]:
        headers_with_auth = base_header.copy()
        headers_with_auth['Authorization'] = f'Bearer {self.token}'
        return headers_with_auth
