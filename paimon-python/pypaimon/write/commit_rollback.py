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

"""Commit rollback to rollback 'COMPACT' commits for resolving conflicts.

Follows the design of Java's org.apache.paimon.operation.commit.CommitRollback
and org.apache.paimon.table.RollbackHelper.
"""

import logging

logger = logging.getLogger(__name__)


class CommitRollback:
    """Rollback COMPACT commits to resolve conflicts.

    When a conflict is detected during commit, if the latest snapshot is a
    COMPACT commit, it can be rolled back by cleaning all snapshots and tags
    with IDs larger than the retained snapshot, following the logic of Java's
    RollbackHelper.cleanLargerThan.
    """

    def __init__(self, snapshot_manager, tag_manager, file_io):
        """Initialize CommitRollback.

        Args:
            snapshot_manager: Manager for reading snapshot metadata.
            tag_manager: Manager for tag operations.
            file_io: FileIO instance for file operations.
        """
        self.snapshot_manager = snapshot_manager
        self.tag_manager = tag_manager
        self.file_io = file_io

    def try_to_rollback(self, latest_snapshot):
        """Try to rollback a COMPACT commit to resolve conflicts.

        Only rolls back COMPACT type commits. Follows Java
        CommitRollback.tryToRollback and RollbackHelper.cleanLargerThan:
        cleans all snapshots and tags with IDs larger than the retained
        snapshot, then updates the LATEST hint.

        Args:
            latest_snapshot: The latest snapshot that may need to be rolled back.

        Returns:
            True if rollback succeeded, False otherwise.
        """
        if latest_snapshot.commit_kind == "COMPACT":
            latest_id = latest_snapshot.id
            previous_id = latest_id - 1
            try:
                previous_snapshot = self.snapshot_manager.get_snapshot_by_id(
                    previous_id)
                if previous_snapshot is None:
                    logger.warning(
                        "Cannot rollback: previous snapshot %d does not exist.",
                        previous_id,
                    )
                    return False

                self._clean_larger_than(previous_snapshot)
                logger.info(
                    "Rolled back COMPACT snapshot %d to snapshot %d "
                    "to resolve conflict.",
                    latest_id, previous_id,
                )
                return True
            except Exception:
                logger.warning(
                    "Failed to rollback COMPACT snapshot %d.",
                    latest_id,
                    exc_info=True,
                )
        return False

    def _clean_larger_than(self, retained_snapshot):
        """Clean snapshots and tags whose ID is larger than the retained snapshot.

        Follows Java RollbackHelper.cleanLargerThan logic:
        1. Clean snapshots with ID > retained and update LATEST hint
        2. Clean tags with ID > retained

        Args:
            retained_snapshot: The snapshot to retain; all later ones are removed.
        """
        self._clean_snapshots(retained_snapshot)
        self._clean_tags(retained_snapshot)

    def _clean_snapshots(self, retained_snapshot):
        """Clean snapshots with ID larger than the retained snapshot.

        Follows Java RollbackHelper.cleanSnapshots logic:
        updates the LATEST hint first, then deletes snapshot files
        from latest down to retained + 1.

        Args:
            retained_snapshot: The snapshot to retain.
        """
        earliest_snapshot = self.snapshot_manager.try_get_earliest_snapshot()
        earliest_id = earliest_snapshot.id if earliest_snapshot is not None else 1

        latest_content = self.snapshot_manager.read_latest_file()
        latest_id = int(latest_content)

        # Update the LATEST hint to point to the retained snapshot
        self.file_io.overwrite_file_utf8(
            self.snapshot_manager.latest_file, str(retained_snapshot.id))

        # Delete snapshot files from latest down to retained + 1
        lower_bound = max(earliest_id, retained_snapshot.id + 1)
        for snapshot_id in range(latest_id, lower_bound - 1, -1):
            snapshot_path = self.snapshot_manager.get_snapshot_path(snapshot_id)
            if self.file_io.exists(snapshot_path):
                self.file_io.delete_quietly(snapshot_path)

    def _clean_tags(self, retained_snapshot):
        """Clean tags whose snapshot ID is larger than the retained snapshot.

        Follows Java RollbackHelper.cleanTags logic:
        iterates all tags and deletes those pointing to snapshots
        with IDs larger than the retained snapshot.

        Args:
            retained_snapshot: The snapshot to retain.
        """
        try:
            tag_names = self.tag_manager.list_tags()
        except Exception:
            return

        if not tag_names:
            return

        for tag_name in tag_names:
            tag = self.tag_manager.get(tag_name)
            if tag is None:
                continue
            if tag.id > retained_snapshot.id:
                tag_path = self.tag_manager.tag_path(tag_name)
                self.file_io.delete_quietly(tag_path)
