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
ChangelogFollowUpScanner for primary key tables with changelog producer.

This scanner is used for tables with changelog-producer=input/full-compaction/lookup.
It reads from the changelog_manifest_list which contains INSERT/UPDATE_BEFORE/
UPDATE_AFTER/DELETE records for downstream changelog consumers.

See Java's ChangelogFollowUpScanner for reference:
paimon-core/src/main/java/org/apache/paimon/table/source/snapshot/ChangelogFollowUpScanner.java
"""

from pypaimon.read.scanner.follow_up_scanner import FollowUpScanner
from pypaimon.snapshot.snapshot import Snapshot


class ChangelogFollowUpScanner(FollowUpScanner):
    """
    Scanner for primary key tables with changelog-producer settings.

    This scanner checks if a snapshot has a changelog_manifest_list and should
    be scanned. Unlike DeltaFollowUpScanner which only scans APPEND commits,
    this scanner scans any commit that produces changelog data.

    Changelog producers:
    - INPUT: Changelog is written during input processing
    - FULL_COMPACTION: Changelog is generated during full compaction
    - LOOKUP: Changelog is generated through lookup compaction
    """

    def should_scan(self, snapshot: Snapshot) -> bool:
        """
        Determine if this snapshot should be scanned.

        For changelog mode, we scan snapshots that have a changelog_manifest_list.
        This differs from DeltaFollowUpScanner which only scans APPEND commits.

        Args:
            snapshot: The snapshot to check

        Returns:
            True if the snapshot has changelog data to read
        """
        # Check if snapshot has changelog manifest
        changelog_list = snapshot.changelog_manifest_list
        return changelog_list is not None and changelog_list != ""
