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


class ColumnMask:
    MAX_TRANSFORM_BYTES = 60 * 1024

    FIELD_ON_COLUMN = "onColumn"
    FIELD_TRANSFORM = "transform"

    def __init__(self, on_column: str, transform: str):
        if is_blank(on_column):
            raise ValueError("onColumn cannot be empty.")
        if is_blank(transform):
            raise ValueError("transform cannot be empty.")
        if utf8_length(transform) > self.MAX_TRANSFORM_BYTES:
            raise ValueError("transform must not exceed {} UTF-8 bytes.".format(self.MAX_TRANSFORM_BYTES))
        self._on_column = on_column
        self._transform = transform

    def get_on_column(self) -> str:
        return self._on_column

    def get_transform(self) -> str:
        return self._transform

    def to_dict(self) -> Dict[str, Any]:
        return {self.FIELD_ON_COLUMN: self._on_column, self.FIELD_TRANSFORM: self._transform}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ColumnMask":
        return cls(data.get(cls.FIELD_ON_COLUMN), data.get(cls.FIELD_TRANSFORM))
