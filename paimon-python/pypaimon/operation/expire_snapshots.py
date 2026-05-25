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

import logging
import time
from typing import List, Optional

from pypaimon.options.expire_config import ExpireConfig
from pypaimon.snapshot.snapshot import Snapshot

logger = logging.getLogger(__name__)


class ExpireSnapshots:

    def __init__(self, snapshot_manager, changelog_manager, consumer_manager,
                 snapshot_deletion, tag_manager):
        self.snapshot_manager = snapshot_manager
        self.changelog_manager = changelog_manager
        self.consumer_manager = consumer_manager
        self.snapshot_deletion = snapshot_deletion
        self.tag_manager = tag_manager
        self._config: Optional[ExpireConfig] = None

    def config(self, expire_config: ExpireConfig) -> 'ExpireSnapshots':
        self._config = expire_config
        return self

    def expire(self) -> int:
        if self._config is None:
            self._config = ExpireConfig()
        self._config.validate()

        changelog_decoupled = self._config.is_changelog_decoupled()
        retain_max = self._config.snapshot_retain_max
        retain_min = self._config.snapshot_retain_min
        max_deletes = self._config.snapshot_max_deletes
        older_than_millis = int(time.time() * 1000) - self._config.snapshot_time_retain_millis

        latest_id = self.snapshot_manager.latest_snapshot_id()
        earliest_id = self.snapshot_manager.earliest_snapshot_id()
        if latest_id is None or earliest_id is None:
            return 0

        begin = max(latest_id - retain_max + 1, earliest_id)
        max_exclusive = latest_id - retain_min + 1

        consumer_min = self.consumer_manager.min_next_snapshot()
        if consumer_min is not None:
            max_exclusive = min(max_exclusive, consumer_min)
        max_exclusive = min(max_exclusive, earliest_id + max_deletes)

        for sid in range(begin, max_exclusive):
            try:
                next_snapshot = self.snapshot_manager.get_snapshot_by_id(sid + 1)
                if next_snapshot is not None and next_snapshot.time_millis >= older_than_millis:
                    return self._expire_until(earliest_id, sid, changelog_decoupled)
            except FileNotFoundError:
                pass

        return self._expire_until(earliest_id, max_exclusive, changelog_decoupled)

    def _expire_until(self, earliest_id: int, end_exclusive_id: int,
                      changelog_decoupled: bool) -> int:
        if end_exclusive_id <= earliest_id:
            self.snapshot_manager.commit_earliest_hint(earliest_id)
            return 0

        snapshots_batch = self.snapshot_manager.get_snapshots_batch(
            list(range(earliest_id, end_exclusive_id + 1))
        )
        all_snapshots = [snapshots_batch[sid] for sid in range(earliest_id, end_exclusive_id + 1)
                         if snapshots_batch.get(sid) is not None]
        expiring_snapshots = [s for s in all_snapshots if s.id < end_exclusive_id]
        if not expiring_snapshots:
            self.snapshot_manager.commit_earliest_hint(end_exclusive_id)
            return 0

        end_snapshot = snapshots_batch.get(end_exclusive_id)
        if end_snapshot is None:
            self.snapshot_manager.commit_earliest_hint(earliest_id)
            return 0

        tagged_snapshots = self.tag_manager.tagged_snapshots()

        self._clean_data_files(all_snapshots, earliest_id, tagged_snapshots,
                               changelog_decoupled)

        if not changelog_decoupled:
            self._clean_changelog_data(expiring_snapshots)

        self.snapshot_deletion.clean_empty_directories()

        self._clean_metadata(expiring_snapshots, tagged_snapshots, earliest_id,
                             end_exclusive_id, end_snapshot, changelog_decoupled)

        return self._delete_snapshots(expiring_snapshots, end_exclusive_id,
                                      changelog_decoupled)

    def _clean_data_files(self, all_snapshots: List[Snapshot], earliest_id: int,
                          tagged_snapshots: list, changelog_decoupled: bool) -> None:
        for snapshot in all_snapshots:
            if snapshot.id == earliest_id:
                continue
            try:
                skipper = self.snapshot_deletion.create_data_file_skipper_for_tags(
                    tagged_snapshots, snapshot.id)
                self.snapshot_deletion.clean_unused_data_files(
                    snapshot, skipper, changelog_decoupled)
            except IOError:
                logger.warning("Failed to clean data files for snapshot %d",
                               snapshot.id, exc_info=True)

    def _clean_changelog_data(self, expiring_snapshots: List[Snapshot]) -> None:
        for snapshot in expiring_snapshots:
            if snapshot.changelog_manifest_list:
                self.snapshot_deletion.delete_added_data_files(
                    snapshot.changelog_manifest_list)

    def _clean_metadata(self, expiring_snapshots: List[Snapshot], tagged_snapshots: list,
                        earliest_id: int, end_exclusive_id: int,
                        end_snapshot: Snapshot, changelog_decoupled: bool) -> None:
        skipping_tags = [t for t in tagged_snapshots if earliest_id <= t.id < end_exclusive_id]
        skipping_snapshots = list(skipping_tags) + [end_snapshot]
        skipping_set = self.snapshot_deletion.manifest_skipping_set(skipping_snapshots)
        for snapshot in expiring_snapshots:
            self.snapshot_deletion.clean_unused_manifests(
                snapshot, skipping_set, changelog_decoupled)

    def _delete_snapshots(self, expiring_snapshots: List[Snapshot],
                          end_exclusive_id: int, changelog_decoupled: bool) -> int:
        new_earliest_id = end_exclusive_id
        for snapshot in expiring_snapshots:
            if changelog_decoupled:
                if not self._commit_changelog(snapshot):
                    logger.warning(
                        "Stopping expiration at snapshot %d: "
                        "failed to commit changelog in decoupled mode", snapshot.id)
                    new_earliest_id = snapshot.id
                    break
            self.snapshot_manager.delete_snapshot(snapshot.id)

        self.snapshot_manager.commit_earliest_hint(new_earliest_id)

        if new_earliest_id == end_exclusive_id:
            return len(expiring_snapshots)
        return sum(1 for s in expiring_snapshots if s.id < new_earliest_id)

    def _commit_changelog(self, snapshot: Snapshot) -> bool:
        """Convert an expiring snapshot to a long-lived changelog before deletion."""
        try:
            from pypaimon.changelog.changelog import Changelog
            changelog = Changelog.from_snapshot(snapshot)
            self.changelog_manager.commit_changelog(changelog, snapshot.id)
            self.changelog_manager.commit_long_lived_changelog_latest_hint(snapshot.id)
            return True
        except Exception:
            logger.error("Failed to commit changelog for snapshot %d",
                         snapshot.id, exc_info=True)
            return False
