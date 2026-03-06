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
"""Tests for ChangelogFollowUpScanner."""

import unittest
from unittest.mock import Mock

from pypaimon.read.scanner.changelog_follow_up_scanner import \
    ChangelogFollowUpScanner


class ChangelogFollowUpScannerTest(unittest.TestCase):
    """Tests for ChangelogFollowUpScanner."""

    def test_should_scan_any_commit_with_changelog(self):
        """Scanner scans based on changelog_manifest_list, regardless of commit kind."""
        scanner = ChangelogFollowUpScanner()
        for kind in ("APPEND", "COMPACT"):
            snapshot = Mock(changelog_manifest_list=f"changelog-manifest-{kind}")
            self.assertTrue(scanner.should_scan(snapshot), kind)

    def test_should_skip_when_no_changelog(self):
        """Scanner should skip when changelog_manifest_list is None."""
        scanner = ChangelogFollowUpScanner()
        snapshot = Mock(changelog_manifest_list=None)
        self.assertFalse(scanner.should_scan(snapshot))

    def test_should_skip_for_empty_string(self):
        """Empty string should be treated as no changelog."""
        scanner = ChangelogFollowUpScanner()
        snapshot = Mock(changelog_manifest_list="")
        self.assertFalse(scanner.should_scan(snapshot))


if __name__ == '__main__':
    unittest.main()
