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

"""
Commit rollback to rollback 'COMPACT' commits for resolving conflicts.
"""

from pypaimon.table.instant import Instant


class CommitRollback:
    """Rollback COMPACT commits to resolve conflicts.

    When a conflict is detected during commit, if the latest snapshot is a
    COMPACT commit, it can be rolled back via TableRollback.
    """

    def __init__(self, table_rollback):
        """Initialize CommitRollback.

        Args:
            table_rollback: A TableRollback instance used to perform the rollback.
        """
        self._table_rollback = table_rollback

    def try_to_rollback(self, latest_snapshot):
        """Try to rollback a COMPACT commit to resolve conflicts.

        Only rolls back COMPACT type commits. Delegates to TableRollback
        to rollback to the previous snapshot (latest - 1), passing the
        latest snapshot ID as from_snapshot.

        Args:
            latest_snapshot: The latest snapshot that may need to be rolled back.

        Returns:
            True if rollback succeeded, False otherwise.
        """
        if latest_snapshot.commit_kind == "COMPACT":
            latest_id = latest_snapshot.id
            try:
                self._table_rollback.rollback_to(
                    Instant.snapshot(latest_id - 1), latest_id)
                return True
            except Exception:
                pass
        return False
