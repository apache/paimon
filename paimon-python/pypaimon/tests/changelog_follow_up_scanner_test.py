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
Tests for ChangelogFollowUpScanner.

ChangelogFollowUpScanner is used for primary key tables with
changelog-producer=input/full-compaction/lookup. It reads from
changelog_manifest_list instead of delta_manifest_list.
"""

import unittest
from unittest.mock import Mock

from pypaimon.read.scanner.changelog_follow_up_scanner import \
    ChangelogFollowUpScanner


class ChangelogFollowUpScannerTest(unittest.TestCase):
    """Tests for ChangelogFollowUpScanner."""

    def test_should_scan_returns_true_when_changelog_exists(self):
        """Scanner should scan when snapshot has changelog_manifest_list."""
        scanner = ChangelogFollowUpScanner()
        snapshot = Mock()
        snapshot.changelog_manifest_list = "changelog-manifest-abc123"

        self.assertTrue(scanner.should_scan(snapshot))

    def test_should_scan_returns_false_when_no_changelog(self):
        """Scanner should skip when snapshot has no changelog_manifest_list."""
        scanner = ChangelogFollowUpScanner()
        snapshot = Mock()
        snapshot.changelog_manifest_list = None

        self.assertFalse(scanner.should_scan(snapshot))

    def test_should_scan_returns_false_for_empty_string(self):
        """Scanner should handle empty string as no changelog."""
        scanner = ChangelogFollowUpScanner()
        snapshot = Mock()
        snapshot.changelog_manifest_list = ""

        # Empty string should be treated as no changelog
        self.assertFalse(scanner.should_scan(snapshot))

    def test_should_scan_append_commit_with_changelog(self):
        """Scanner should scan APPEND commits that have changelog."""
        scanner = ChangelogFollowUpScanner()
        snapshot = Mock()
        snapshot.commit_kind = "APPEND"
        snapshot.changelog_manifest_list = "changelog-manifest-xyz"

        self.assertTrue(scanner.should_scan(snapshot))

    def test_should_scan_compact_commit_with_changelog(self):
        """Scanner should scan COMPACT commits if they have changelog (full-compaction mode)."""
        scanner = ChangelogFollowUpScanner()
        snapshot = Mock()
        snapshot.commit_kind = "COMPACT"
        snapshot.changelog_manifest_list = "changelog-manifest-compact"

        # In full-compaction mode, COMPACT commits produce changelog
        self.assertTrue(scanner.should_scan(snapshot))

    def test_should_skip_compact_commit_without_changelog(self):
        """Scanner should skip COMPACT commits without changelog."""
        scanner = ChangelogFollowUpScanner()
        snapshot = Mock()
        snapshot.commit_kind = "COMPACT"
        snapshot.changelog_manifest_list = None

        self.assertFalse(scanner.should_scan(snapshot))


if __name__ == '__main__':
    unittest.main()
