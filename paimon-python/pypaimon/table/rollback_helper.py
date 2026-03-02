"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from pypaimon.common.json_util import JSON


class RollbackHelper:
    """Helper class for table rollback including utils to clean snapshots.
    """

    def __init__(self, snapshot_manager, tag_manager, file_io):
        """Initialize RollbackHelper.

        Args:
            snapshot_manager: The SnapshotManager instance.
            tag_manager: The TagManager instance.
            file_io: The FileIO instance for file operations.
        """
        self._snapshot_manager = snapshot_manager
        self._tag_manager = tag_manager
        self._file_io = file_io

    def clean_larger_than(self, retained_snapshot):
        """Clean snapshots and tags whose id is larger than the retained snapshot.

        Updates the LATEST hint and removes snapshot files and tag files
        for snapshots newer than the retained one.

        Args:
            retained_snapshot: The snapshot to retain; everything newer is removed.
        """
        latest = self._snapshot_manager.get_latest_snapshot()
        if latest is None:
            return

        latest_id = latest.id
        retained_id = retained_snapshot.id

        # Update LATEST hint
        self._file_io.write_file(
            self._snapshot_manager.latest_file, str(retained_id), overwrite=True)

        # Delete snapshot files larger than retained
        for snapshot_id in range(retained_id + 1, latest_id + 1):
            snapshot_path = self._snapshot_manager.get_snapshot_path(snapshot_id)
            if self._file_io.exists(snapshot_path):
                self._file_io.delete(snapshot_path)

        # Clean tags whose snapshot id is larger than retained
        for tag_name in self._tag_manager.list_tags():
            tag = self._tag_manager.get(tag_name)
            if tag is not None:
                tag_snapshot = tag.trim_to_snapshot()
                if tag_snapshot.id > retained_id:
                    self._tag_manager.delete_tag(tag_name)

    def create_snapshot_file_if_needed(self, tagged_snapshot):
        """Create a snapshot file from a tag if the snapshot file doesn't exist.

        When rolling back to a tag, the snapshot file may have been expired.
        This method recreates it from the tag data and updates the earliest hint.

        Args:
            tagged_snapshot: The snapshot from the tag to potentially write.
        """
        snapshot_path = self._snapshot_manager.get_snapshot_path(tagged_snapshot.id)
        if not self._file_io.exists(snapshot_path):
            self._file_io.write_file(
                snapshot_path, JSON.to_json(tagged_snapshot), overwrite=False)
