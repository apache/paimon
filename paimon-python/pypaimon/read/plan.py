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
# limitations under the License.
################################################################################

from dataclasses import dataclass
from typing import List

from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.read.split import Split


@dataclass
class Plan:
    """Implementation of Plan for native Python reading."""
    _splits: List[Split]
    _plan_start_row: int = None
    _plan_end_row: int = None

    def splits(self) -> List[Split]:
        return self._splits

    def plan_start_row(self) -> int:
        return self._plan_start_row

    def plan_end_row(self) -> int:
        return self._plan_end_row

    def extract_entries(self) -> List[ManifestEntry]:
        """Extract ManifestEntry list from _splits variable."""
        manifest_entries = []

        for split in self._splits:
            # For each file in the split, create a ManifestEntry
            for data_file in split.files:
                manifest_entry = ManifestEntry(
                    kind=0,  # 0 indicates ADD operation
                    partition=split.partition,
                    bucket=split.bucket,
                    total_buckets=-1,  # Default value, may need to be set based on table configuration
                    file=data_file
                )
                manifest_entries.append(manifest_entry)

        return manifest_entries
