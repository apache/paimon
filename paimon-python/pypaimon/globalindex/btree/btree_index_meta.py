################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
################################################################################

"""B-tree index metadata."""

from dataclasses import dataclass
from typing import Optional
import struct


@dataclass
class BTreeIndexMeta:
    """
    Index Meta of each BTree index file.
    
    The first key and last key of this meta could be null if the
    entire btree index file only contains nulls.
    """

    first_key: Optional[bytes]
    last_key: Optional[bytes]
    has_nulls: bool

    def only_nulls(self) -> bool:
        """Check if this index only contains nulls."""
        return self.first_key is None and self.last_key is None

    @classmethod
    def deserialize(cls, data: bytes) -> 'BTreeIndexMeta':
        """Deserialize metadata from byte array."""
        offset = 0
        
        # Read first key
        first_key_length = struct.unpack('<I', data[offset:offset + 4])[0]
        offset += 4
        first_key = None if first_key_length == 0 else data[offset:offset + first_key_length]
        offset += first_key_length
        
        # Read last key
        last_key_length = struct.unpack('<I', data[offset:offset + 4])[0]
        offset += 4
        last_key = None if last_key_length == 0 else data[offset:offset + last_key_length]
        offset += last_key_length
        
        # Read has_nulls flag
        has_nulls = struct.unpack('<B', data[offset:offset + 1])[0] == 1
        
        return cls(first_key, last_key, has_nulls)
