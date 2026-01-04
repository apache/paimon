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

from dataclasses import dataclass
from typing import Optional


@dataclass
class DeletionVectorMeta:
    """Metadata of deletion vector."""

    data_file_name: str
    offset: int
    length: int
    cardinality: Optional[int] = None

    def __eq__(self, other):
        if not isinstance(other, DeletionVectorMeta):
            return False
        return (self.data_file_name == other.data_file_name and
                self.offset == other.offset and
                self.length == other.length and
                self.cardinality == other.cardinality)

    def __hash__(self):
        return hash((self.data_file_name, self.offset, self.length, self.cardinality))
