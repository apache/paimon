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

from dataclasses import dataclass, field
from typing import Any, List, Optional


from pypaimon.read.split import Split
from pypaimon.read.scan_stats import ScanStats


@dataclass
class Plan:
    """Implementation of Plan for native Python reading."""
    _splits: List[Split]
    snapshot_id: Optional[int] = field(default=None)
    _scan_stats: Optional[ScanStats] = field(default=None, repr=False, compare=False)
    _native_stats: Any = field(default=None, repr=False, compare=False)
    # A LIMIT hint deferred until after Native row-position selection.
    _stats_limit: Optional[int] = field(default=None, repr=False, compare=False)

    def splits(self) -> List[Split]:
        return self._splits

    def stats(self) -> Optional[ScanStats]:
        """Planning counters, converted from Native only when requested."""
        native_stats = self._native_stats
        if native_stats is not None:
            self._scan_stats = ScanStats.from_native(native_stats)
            # Python can filter or limit splits after Native planning. Keep the
            # actual planning work, but summarize the final returned selection.
            files, data_bytes = 0, 0
            for split in self._splits:
                for file in split.files:
                    files += 1
                    data_bytes += max(0, file.file_size)
            counters = self._scan_stats.planning_counters
            counters.update(final_splits=len(self._splits), splits_after_limit=len(self._splits),
                            final_files=files, planned_data_file_bytes=min(data_bytes, 2 ** 64 - 1))
            if self._stats_limit is not None:
                counters['limit'] = self._stats_limit
            self._native_stats = None
        return self._scan_stats

    def __getstate__(self):
        # Native handles are process-local. Serialize counters as Python scalars.
        self.stats()
        return self.__dict__.copy()
