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

from bisect import bisect_right

from pypaimon.common.json_util import JSON


class RollbackHelper:
    """Clean filesystem metadata during table rollback."""

    def __init__(self, snapshot_manager, changelog_manager, tag_manager, file_io):
        self._snapshot_manager = snapshot_manager
        self._changelog_manager = changelog_manager
        self._tag_manager = tag_manager
        self._file_io = file_io

    def clean_larger_than(self, retained_snapshot):
        """Remove snapshot, long-lived changelog, and tag files above the target ID."""
        retained_id = retained_snapshot.id
        self._clean_snapshots(retained_id)
        self._clean_long_lived_changelogs(retained_id)
        self._clean_tags(retained_id)

    def _clean_snapshots(self, retained_id):
        manager = self._snapshot_manager
        # A previous attempt can leave LATEST at the target while newer files
        # still exist. List actual IDs so a gap cannot hide those files.
        snapshot_ids = manager.list_snapshot_ids()
        self._file_io.overwrite_file_utf8(manager.latest_file, str(retained_id))
        for snapshot_id in reversed(snapshot_ids):
            if snapshot_id <= retained_id:
                break
            path = manager.get_snapshot_path(snapshot_id)
            if not self._file_io.delete(path) and self._file_io.exists(path):
                raise OSError(f"Failed to delete snapshot file {path}")

    def _clean_long_lived_changelogs(self, retained_id):
        manager = self._changelog_manager
        changelog_ids = manager.list_changelog_ids()
        first_removed = bisect_right(changelog_ids, retained_id)
        if first_removed == len(changelog_ids):
            return
        latest = changelog_ids[first_removed - 1] if first_removed else -1
        manager.commit_long_lived_changelog_latest_hint(latest)
        for changelog_id in reversed(changelog_ids[first_removed:]):
            self._file_io.delete_quietly(manager.long_lived_changelog_path(changelog_id))

    def _clean_tags(self, retained_id):
        for tag_name in self._tag_manager.list_tags():
            tag = self._tag_manager.get(tag_name)
            if tag is not None and tag.id > retained_id:
                self._file_io.delete_quietly(self._tag_manager.tag_path(tag_name))

    def create_snapshot_file_if_needed(self, tagged_snapshot):
        """Create a snapshot file from a tag if the snapshot file doesn't exist.

        When rolling back to a tag, the snapshot file may have been expired.
        This method recreates it from the tag data and updates the earliest hint.

        Args:
            tagged_snapshot: The snapshot from the tag to potentially write.
        """
        snapshot_path = self._snapshot_manager.get_snapshot_path(tagged_snapshot.id)
        if not self._file_io.exists(snapshot_path):
            # Update EARLIEST before creating the file so a failed hint write
            # leaves the missing snapshot available to restore on retry.
            earliest_file = f"{self._snapshot_manager.snapshot_dir}/EARLIEST"
            self._file_io.overwrite_file_utf8(earliest_file, str(tagged_snapshot.id))
            self._file_io.write_file(
                snapshot_path, JSON.to_json(tagged_snapshot), overwrite=False)
