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

"""Entry represents a key value."""


class BlockEntry:
    """Entry represents a key value."""
    
    def __init__(self, key: bytes, value: bytes):
        """Initialize BlockEntry.
        
        Args:
            key: The key bytes
            value: The value bytes
        """
        if key is None:
            raise ValueError("key is null")
        if value is None:
            raise ValueError("value is null")
        self.key = key
        self.value = value
    
    def __eq__(self, other) -> bool:
        """Check equality with another BlockEntry."""
        if self is other:
            return True
        if other is None or not isinstance(other, BlockEntry):
            return False
        return self.key == other.key and self.value == other.value
    
    def __hash__(self) -> int:
        """Return hash of this BlockEntry."""
        result = hash(self.key)
        result = 31 * result + hash(self.value)
        return result
    
    def __repr__(self) -> str:
        """Return string representation."""
        return f"BlockEntry(key={self.key!r}, value={self.value!r})"
