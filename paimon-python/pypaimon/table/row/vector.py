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

import array
from typing import List, Union


class Vector:
    """A fixed-length dense vector of numeric values backed by array.array."""

    def __init__(self, values, typecode: str = 'f'):
        if isinstance(values, array.array):
            self._values = values
        else:
            self._values = array.array(typecode, values)

    def to_list(self) -> List[Union[int, float]]:
        return self._values.tolist()

    @staticmethod
    def from_list(values: List[Union[int, float]], typecode: str = 'f') -> 'Vector':
        return Vector(values, typecode)

    def __len__(self) -> int:
        return len(self._values)

    def __getitem__(self, index):
        return self._values[index]

    def __eq__(self, other) -> bool:
        if not isinstance(other, Vector):
            return False
        return self._values == other._values

    def __hash__(self) -> int:
        return hash(self._values.tobytes())

    def __str__(self) -> str:
        return f"Vector({self._values.tolist()})"

    def __repr__(self) -> str:
        return self.__str__()
