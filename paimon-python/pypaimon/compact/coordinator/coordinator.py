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
from typing import List

from pypaimon.compact.task.compact_task import CompactTask


class CompactCoordinator(ABC):
    """Driver-side planner that turns a snapshot into a list of CompactTask.

    The coordinator runs once per CompactJob, in the driver, and **does not**
    rewrite data itself. Splitting plan() from task.run() lets us pin the
    snapshot scan to a single process (no concurrent manifest re-reads) while
    still letting the executor distribute the resulting tasks however it likes.
    """

    @abstractmethod
    def plan(self) -> List[CompactTask]:
        """Return a possibly-empty list of compact tasks for the current snapshot.

        An empty list means there is nothing worth compacting at this moment;
        the job should commit nothing rather than produce an empty snapshot.
        """
