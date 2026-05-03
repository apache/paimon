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

import logging
import uuid
from typing import Any, Dict, List, Optional

from pypaimon.common.predicate import Predicate
from pypaimon.compact.coordinator.append_compact_coordinator import \
    AppendCompactCoordinator
from pypaimon.compact.coordinator.coordinator import CompactCoordinator
from pypaimon.compact.coordinator.merge_tree_compact_coordinator import \
    MergeTreeCompactCoordinator
from pypaimon.compact.executor.executor import CompactExecutor
from pypaimon.compact.executor.local_executor import LocalExecutor
from pypaimon.compact.options import CompactOptions
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.file_store_commit import FileStoreCommit

logger = logging.getLogger(__name__)


class CompactJob:
    """Driver-side orchestrator: plan → distribute → commit, in three steps.

    The flow purposely mirrors what Spark's CompactProcedure does:
    1. A Coordinator runs once on the driver and produces independent tasks.
    2. An Executor (Local in Phase 2, Ray in Phase 4) runs the tasks and
       returns CommitMessage(compact_before, compact_after) per task.
    3. The driver collects all messages and commits them atomically with
       commit_kind="COMPACT".
    """

    def __init__(
        self,
        table,
        compact_options: Optional[CompactOptions] = None,
        executor: Optional[CompactExecutor] = None,
        partition_predicate: Optional[Predicate] = None,
        commit_user: Optional[str] = None,
        catalog_options: Optional[Dict[str, Any]] = None,
        table_identifier: Optional[str] = None,
    ):
        """Construct a CompactJob.

        catalog_options + table_identifier are required when using a
        distributed executor (RayExecutor) — workers need them to rebuild
        the table on the worker process. LocalExecutor never reads them.
        """
        self.table = table
        self.compact_options = compact_options or CompactOptions()
        self.executor = executor or LocalExecutor()
        self.partition_predicate = partition_predicate
        self.commit_user = commit_user or str(uuid.uuid4())
        self.catalog_options = dict(catalog_options) if catalog_options else None
        # Identifier is a dataclass with no custom __str__; str(...) would
        # return its repr ("Identifier(database=...)") and Identifier.from_string
        # would refuse to parse that. Use get_full_name() so the worker can
        # round-trip the identifier through CatalogFactory.
        self.table_identifier = table_identifier or table.identifier.get_full_name()

    def execute(self) -> List[CommitMessage]:
        """Run the compaction job and return the messages that were committed.

        Returns an empty list when there is nothing to compact.
        """
        coordinator = self._build_coordinator()
        tasks = coordinator.plan()
        if not tasks:
            logger.info(
                "No compaction work for table %s at the current snapshot",
                self.table.identifier,
            )
            return []

        # Distributed executors can't share the in-process FileStoreTable, so
        # attach the loader spec when caller provided one. LocalExecutor
        # ignores it and uses the in-process table the coordinator already
        # baked into each task.
        if self.catalog_options is not None:
            for task in tasks:
                task.with_table_loader(self.catalog_options, self.table_identifier)

        logger.info(
            "Compacting table %s: %d task(s) via %s",
            self.table.identifier,
            len(tasks),
            type(self.executor).__name__,
        )
        messages = self.executor.execute(tasks)
        self._commit(messages)
        return messages

    def _build_coordinator(self) -> CompactCoordinator:
        if self.table.is_primary_key_table:
            return MergeTreeCompactCoordinator(
                table=self.table,
                compact_options=self.compact_options,
                partition_predicate=self.partition_predicate,
            )
        return AppendCompactCoordinator(
            table=self.table,
            compact_options=self.compact_options,
            partition_predicate=self.partition_predicate,
        )

    def _commit(self, messages: List[CommitMessage]) -> None:
        non_empty = [m for m in messages if not m.is_empty()]
        if not non_empty:
            return
        snapshot_commit = self.table.new_snapshot_commit()
        if snapshot_commit is None:
            raise RuntimeError("Table does not provide a SnapshotCommit instance")
        file_store_commit = FileStoreCommit(snapshot_commit, self.table, self.commit_user)
        try:
            file_store_commit.commit_compact(non_empty, BATCH_COMMIT_IDENTIFIER)
        finally:
            file_store_commit.close()
