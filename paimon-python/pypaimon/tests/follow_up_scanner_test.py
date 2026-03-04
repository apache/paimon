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
Tests for FollowUpScanner implementations.
TDD: These tests are written first, before the implementation.
"""

import unittest
from unittest.mock import Mock

from pypaimon.read.scanner.delta_follow_up_scanner import DeltaFollowUpScanner
from pypaimon.read.scanner.follow_up_scanner import FollowUpScanner


class FollowUpScannerInterfaceTest(unittest.TestCase):
    """Test that FollowUpScanner interface is properly defined."""

    def test_follow_up_scanner_is_abstract(self):
        """FollowUpScanner should be an abstract base class."""
        with self.assertRaises(TypeError):
            FollowUpScanner()

    def test_follow_up_scanner_requires_should_scan_method(self):
        """FollowUpScanner subclasses must implement should_scan."""
        self.assertTrue(hasattr(FollowUpScanner, 'should_scan'))


class DeltaFollowUpScannerTest(unittest.TestCase):
    """Tests for DeltaFollowUpScanner which handles APPEND commits only."""

    def setUp(self):
        self.scanner = DeltaFollowUpScanner()

    def test_should_scan_returns_true_for_append_commit(self):
        """DeltaFollowUpScanner should scan APPEND commits."""
        snapshot = Mock()
        snapshot.commit_kind = "APPEND"

        result = self.scanner.should_scan(snapshot)

        self.assertTrue(result)

    def test_should_scan_returns_false_for_compact_commit(self):
        """DeltaFollowUpScanner should skip COMPACT commits."""
        snapshot = Mock()
        snapshot.commit_kind = "COMPACT"

        result = self.scanner.should_scan(snapshot)

        self.assertFalse(result)

    def test_should_scan_returns_false_for_overwrite_commit(self):
        """DeltaFollowUpScanner should skip OVERWRITE commits."""
        snapshot = Mock()
        snapshot.commit_kind = "OVERWRITE"

        result = self.scanner.should_scan(snapshot)

        self.assertFalse(result)

    def test_should_scan_returns_false_for_expire_commit(self):
        """DeltaFollowUpScanner should skip EXPIRE commits."""
        snapshot = Mock()
        snapshot.commit_kind = "EXPIRE"

        result = self.scanner.should_scan(snapshot)

        self.assertFalse(result)

    def test_should_scan_returns_false_for_analyze_commit(self):
        """DeltaFollowUpScanner should skip ANALYZE commits."""
        snapshot = Mock()
        snapshot.commit_kind = "ANALYZE"

        result = self.scanner.should_scan(snapshot)

        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
