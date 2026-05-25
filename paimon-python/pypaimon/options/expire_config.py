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

import sys
from dataclasses import dataclass
from typing import Optional


@dataclass
class ExpireConfig:
    snapshot_retain_max: int = sys.maxsize
    snapshot_retain_min: int = 1
    snapshot_time_retain_millis: int = sys.maxsize
    snapshot_max_deletes: int = sys.maxsize
    changelog_retain_max: Optional[int] = None
    changelog_retain_min: Optional[int] = None
    changelog_time_retain_millis: Optional[int] = None
    changelog_max_deletes: Optional[int] = None
    changelog_decoupled: Optional[bool] = None

    def effective_changelog_retain_max(self) -> int:
        return self.changelog_retain_max if self.changelog_retain_max is not None else self.snapshot_retain_max

    def effective_changelog_retain_min(self) -> int:
        return self.changelog_retain_min if self.changelog_retain_min is not None else self.snapshot_retain_min

    def effective_changelog_time_retain_millis(self) -> int:
        if self.changelog_time_retain_millis is not None:
            return self.changelog_time_retain_millis
        return self.snapshot_time_retain_millis

    def effective_changelog_max_deletes(self) -> int:
        return self.changelog_max_deletes if self.changelog_max_deletes is not None else self.snapshot_max_deletes

    def is_changelog_decoupled(self) -> bool:
        if self.changelog_decoupled is not None:
            return self.changelog_decoupled
        return (
            self.effective_changelog_retain_max() > self.snapshot_retain_max
            or self.effective_changelog_retain_min() > self.snapshot_retain_min
            or self.effective_changelog_time_retain_millis() > self.snapshot_time_retain_millis
        )

    def validate(self):
        if self.snapshot_retain_max < self.snapshot_retain_min:
            raise ValueError(
                f"snapshot_retain_max ({self.snapshot_retain_max}) must be >= "
                f"snapshot_retain_min ({self.snapshot_retain_min})"
            )
