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

from abc import ABC, abstractmethod
from typing import List, Optional

from pypaimon.compact.levels import LevelSortedRun
from pypaimon.compact.strategy.compact_unit import CompactUnit


class CompactStrategy(ABC):
    """Picks which sorted runs to compact next.

    Implementations are stateful (they may track `last_full_compaction` time
    or similar) but each pick() call inspects only the level snapshot it is
    handed; the coordinator owns the Levels object.
    """

    @abstractmethod
    def pick(
        self,
        num_levels: int,
        runs: List[LevelSortedRun],
    ) -> Optional[CompactUnit]:
        """Return the next compaction unit, or None if nothing should run now."""


def pick_full_compaction(
    num_levels: int,
    runs: List[LevelSortedRun],
) -> Optional[CompactUnit]:
    """Force a single unit covering every run, output to the max level.

    Returns None when there are no runs to compact, or when the runs already
    consist of a single file already at the max level (idempotent no-op).
    Mirrors CompactStrategy.pickFullCompaction(int, List<LevelSortedRun>).
    """
    if not runs:
        return None
    max_level = num_levels - 1
    if (
        len(runs) == 1
        and runs[0].level == max_level
        and len(runs[0].run.files) == 1
    ):
        return None
    return CompactUnit.from_level_runs(max_level, runs)
