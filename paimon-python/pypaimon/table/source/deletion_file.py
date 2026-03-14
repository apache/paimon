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
class DeletionFile:
    """
    Deletion file for data file, the first 4 bytes are length, the following is the bitmap content.

    - The first 4 bytes are length, should equal to length().
    - Next 4 bytes are the magic number, should be equal to 1581511376.
    - The remaining content should be a RoaringBitmap.
    """
    dv_index_path: str  # The file where the vector for data file is located
    offset: int  # The offset where the vector for data file is located in the dv index file
    length: int
    cardinality: Optional[int] = None

    def __eq__(self, other):
        if not isinstance(other, DeletionFile):
            return False
        return (self.dv_index_path == other.dv_index_path and
                self.offset == other.offset and
                self.length == other.length and
                self.cardinality == other.cardinality)

    def __hash__(self):
        return hash((self.dv_index_path, self.offset, self.length, self.cardinality))

    def __str__(self):
        return (f"DeletionFile(path='{self.dv_index_path}', offset={self.offset}, "
                f"length={self.length}, cardinality={self.cardinality})")
