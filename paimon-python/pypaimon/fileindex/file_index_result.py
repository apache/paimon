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

"""File index evaluation results."""

from enum import Enum


class FileIndexResult(Enum):
    """Result of file index predicate evaluation."""

    REMAIN = "REMAIN"  # File must be read
    SKIP = "SKIP"      # File can be skipped

    def remain(self) -> bool:
        """Check if file should remain (be read)."""
        return self == FileIndexResult.REMAIN

    def and_(self, other: 'FileIndexResult') -> 'FileIndexResult':
        """Combine with AND logic."""
        if self == FileIndexResult.SKIP or other == FileIndexResult.SKIP:
            return FileIndexResult.SKIP
        return FileIndexResult.REMAIN

    def or_(self, other: 'FileIndexResult') -> 'FileIndexResult':
        """Combine with OR logic."""
        if self == FileIndexResult.REMAIN or other == FileIndexResult.REMAIN:
            return FileIndexResult.REMAIN
        return FileIndexResult.SKIP


# Constants for convenience
REMAIN = FileIndexResult.REMAIN
SKIP = FileIndexResult.SKIP
