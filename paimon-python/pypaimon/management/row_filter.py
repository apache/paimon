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
from typing import Any, Dict

from pypaimon.management.java_string import is_blank, utf8_length


class RowFilter:
    MAX_PREDICATE_BYTES = 60 * 1024

    FIELD_PREDICATE = "predicate"

    def __init__(self, predicate: str):
        if is_blank(predicate):
            raise ValueError("predicate cannot be empty.")
        if utf8_length(predicate) > self.MAX_PREDICATE_BYTES:
            raise ValueError("predicate must not exceed {} UTF-8 bytes.".format(self.MAX_PREDICATE_BYTES))
        self._predicate = predicate

    def get_predicate(self) -> str:
        return self._predicate

    def to_dict(self) -> Dict[str, Any]:
        return {self.FIELD_PREDICATE: self._predicate}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RowFilter":
        return cls(data.get(cls.FIELD_PREDICATE))
