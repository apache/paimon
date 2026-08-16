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

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List

from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.snapshot.snapshot import Snapshot


@dataclass
class CommitCallbackContext:
    """Context passed to CommitCallback after a successful commit.

    Implementations must be idempotent because the callback might be called
    multiple times if a failure occurs right after the commit.
    """

    snapshot: Snapshot
    commit_entries: List[ManifestEntry]
    identifier: int


class CommitCallback(ABC):
    """Callback invoked after a list of commit entries is successfully committed.

    Implementations must be idempotent because the callback might be called
    multiple times if a failure occurs right after the commit.
    """

    @abstractmethod
    def call(self, context: CommitCallbackContext) -> None:
        """Invoked after a snapshot is successfully committed."""

    def close(self) -> None:
        """Release any resources held by this callback."""
