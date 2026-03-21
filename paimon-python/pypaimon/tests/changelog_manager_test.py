################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and
#  limitations under the License.
################################################################################

import os
import shutil
import tempfile
import unittest

from pypaimon import CatalogFactory, Schema
from pypaimon.changelog import Changelog, ChangelogManager

import pyarrow as pa


class TestChangelogManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', True)

        cls.pa_schema = pa.schema([
            ('user_id', pa.int32()),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            ('dt', pa.string())
        ])
        schema = Schema.from_pyarrow_schema(cls.pa_schema, partition_keys=['dt'],
                                            options={"bucket": "2"})
        cls.catalog.create_table('default.test_changelog_table', schema, False)
        cls.table = cls.catalog.get_table('default.test_changelog_table')

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_changelog_manager_initialization(self):
        """Test that ChangelogManager can be initialized."""
        changelog_manager = ChangelogManager(
            self.table.file_io,
            self.table.table_path
        )
        self.assertIsNotNone(changelog_manager)
        self.assertEqual(changelog_manager.branch, 'main')

    def test_changelog_manager_with_branch(self):
        """Test that ChangelogManager can be initialized with a custom branch."""
        changelog_manager = ChangelogManager(
            self.table.file_io,
            self.table.table_path,
            branch='feature'
        )
        self.assertIsNotNone(changelog_manager)
        self.assertEqual(changelog_manager.branch, 'feature')

    def test_changelog_directory_path(self):
        """Test that changelog directory path is correct."""
        changelog_manager = ChangelogManager(
            self.table.file_io,
            self.table.table_path
        )
        expected = f"{self.table.table_path}/changelog"
        self.assertEqual(changelog_manager.changelog_directory(), expected)

    def test_changelog_directory_path_with_branch(self):
        """Test that changelog directory path includes branch."""
        changelog_manager = ChangelogManager(
            self.table.file_io,
            self.table.table_path,
            branch='feature'
        )
        expected = f"{self.table.table_path}/branch/branch-feature/changelog"
        self.assertEqual(changelog_manager.changelog_directory(), expected)

    def test_long_lived_changelog_path(self):
        """Test that long-lived changelog path is correct."""
        changelog_manager = ChangelogManager(
            self.table.file_io,
            self.table.table_path
        )
        expected = f"{self.table.table_path}/changelog/changelog-123"
        self.assertEqual(changelog_manager.long_lived_changelog_path(123), expected)

    def test_latest_long_lived_changelog_id_none(self):
        """Test that latest changelog ID is None when no changelog exists."""
        changelog_manager = ChangelogManager(
            self.table.file_io,
            self.table.table_path
        )
        # No changelog files should exist yet
        self.assertIsNone(changelog_manager.latest_long_lived_changelog_id())

    def test_earliest_long_lived_changelog_id_none(self):
        """Test that earliest changelog ID is None when no changelog exists."""
        changelog_manager = ChangelogManager(
            self.table.file_io,
            self.table.table_path
        )
        # No changelog files should exist yet
        self.assertIsNone(changelog_manager.earliest_long_lived_changelog_id())

    def test_changelog_from_snapshot(self):
        """Test that Changelog can be created from a Snapshot."""
        from pypaimon.snapshot.snapshot_manager import SnapshotManager

        snapshot_manager = SnapshotManager(self.table)
        snapshot = snapshot_manager.get_latest_snapshot()

        if snapshot:
            changelog = Changelog.from_snapshot(snapshot)
            self.assertEqual(changelog.id, snapshot.id)
            self.assertEqual(changelog.schema_id, snapshot.schema_id)
            self.assertEqual(changelog.time_millis, snapshot.time_millis)


if __name__ == '__main__':
    unittest.main()
