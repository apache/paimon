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
from typing import Dict, Optional


class RESTToken:

    def __init__(self, token: Dict[str, str], expire_at_millis: int):
        self.token = token
        self.expire_at_millis = expire_at_millis
        self.hash: Optional[int] = None

    def __eq__(self, other: object) -> bool:
        if other is None or not isinstance(other, RESTToken):
            return False

        return (self.expire_at_millis == other.expire_at_millis and
                self.token == other.token)

    def __hash__(self) -> int:
        if self.hash is None:
            self.hash = hash((frozenset(self.token.items()), self.expire_at_millis))
        return self.hash
