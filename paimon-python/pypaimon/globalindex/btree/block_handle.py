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

from dataclasses import dataclass


@dataclass
class BlockHandle:
    """Handle for a data block."""

    def __init__(self, offset: int, size: int):
        """
        Initialize the block handle.

        Args:
            offset: Offset of the block in the file
            size: Size of the block in bytes
        """
        self.offset = offset
        self.size = size

    def is_null(self) -> bool:
        """Check if this handle represents a null block."""
        return self.offset == 0 and self.size == 0
