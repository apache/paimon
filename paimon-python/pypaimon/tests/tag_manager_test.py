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
#  limitations under the License.
################################################################################

import shutil
import tempfile
import unittest

from pypaimon.common.options import Options
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.tag.tag_manager import TagManager


class TagManagerTest(unittest.TestCase):
    """Test cases for TagManager."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp(prefix="tag_manager_test_")
        self.warehouse_path = f"file://{self.temp_dir}"
        self.file_io = LocalFileIO(self.warehouse_path, Options({}))
        self.tag_manager = TagManager(self.file_io, self.temp_dir)

    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_test_snapshot(self, snapshot_id=1, schema_id=1) -> Snapshot:
        """Create a test snapshot instance."""
        return Snapshot(
            version=1,
            id=snapshot_id,
            schema_id=schema_id,
            base_manifest_list="manifest-list-1",
            delta_manifest_list="delta-manifest-list-1",
            total_record_count=100,
            delta_record_count=50,
            commit_user="test_user",
            commit_identifier=123456789,
            commit_kind="APPEND",
            time_millis=1609459200000,
            changelog_manifest_list="changelog-manifest-list-1",
            index_manifest="index-manifest-1",
            changelog_record_count=10,
            watermark=1234567890,
            statistics="statistics-1",
            next_row_id=100
        )

    def test_tag_directory(self):
        """Test tag directory path."""
        expected_dir = f"{self.temp_dir}/tag"
        self.assertEqual(self.tag_manager.tag_directory(), expected_dir)

    def test_tag_directory_with_branch(self):
        """Test tag directory path with branch."""
        tag_manager = TagManager(self.file_io, self.temp_dir, "test_branch")
        expected_dir = f"{self.temp_dir}/branch/branch-test_branch/tag"
        self.assertEqual(tag_manager.tag_directory(), expected_dir)

    def test_tag_path(self):
        """Test tag file path."""
        expected_path = f"{self.temp_dir}/tag/tag-test_tag"
        self.assertEqual(self.tag_manager.tag_path("test_tag"), expected_path)

    def test_tag_exists(self):
        """Test tag existence check."""
        snapshot = self._create_test_snapshot()
        self.assertFalse(self.tag_manager.tag_exists("test_tag"))

        self.tag_manager.create_tag(snapshot, "test_tag")
        self.assertTrue(self.tag_manager.tag_exists("test_tag"))

    def test_create_and_get_tag(self):
        """Test creating and getting a tag."""
        snapshot = self._create_test_snapshot()
        self.tag_manager.create_tag(snapshot, "test_tag")

        tag = self.tag_manager.get("test_tag")
        self.assertIsNotNone(tag)
        self.assertEqual(tag.id, snapshot.id)
        self.assertEqual(tag.schema_id, snapshot.schema_id)
        self.assertEqual(tag.total_record_count, snapshot.total_record_count)

    def test_get_nonexistent_tag(self):
        """Test getting a nonexistent tag."""
        tag = self.tag_manager.get("nonexistent")
        self.assertIsNone(tag)

    def test_get_tag_with_blank_name(self):
        """Test getting a tag with blank name."""
        with self.assertRaises(ValueError) as context:
            self.tag_manager.get("")
        self.assertIn("blank", str(context.exception))

        with self.assertRaises(ValueError) as context:
            self.tag_manager.get("   ")
        self.assertIn("blank", str(context.exception))

    def test_get_or_throw_existing_tag(self):
        """Test get_or_throw with existing tag."""
        snapshot = self._create_test_snapshot()
        self.tag_manager.create_tag(snapshot, "test_tag")

        tag = self.tag_manager.get_or_throw("test_tag")
        self.assertIsNotNone(tag)
        self.assertEqual(tag.id, snapshot.id)

    def test_get_or_throw_nonexistent_tag(self):
        """Test get_or_throw with nonexistent tag."""
        with self.assertRaises(ValueError) as context:
            self.tag_manager.get_or_throw("nonexistent")
        self.assertIn("doesn't exist", str(context.exception))

    def test_create_tag_with_blank_name(self):
        """Test creating tag with blank name."""
        snapshot = self._create_test_snapshot()
        with self.assertRaises(ValueError) as context:
            self.tag_manager.create_tag(snapshot, "")
        self.assertIn("blank", str(context.exception))

        with self.assertRaises(ValueError) as context:
            self.tag_manager.create_tag(snapshot, "   ")
        self.assertIn("blank", str(context.exception))

    def test_create_duplicate_tag(self):
        """Test creating duplicate tag."""
        snapshot = self._create_test_snapshot()
        self.tag_manager.create_tag(snapshot, "test_tag")

        with self.assertRaises(ValueError) as context:
            self.tag_manager.create_tag(snapshot, "test_tag")
        self.assertIn("already exists", str(context.exception))

    def test_create_tag_ignore_if_exists(self):
        """Test creating tag with ignore_if_exists flag."""
        snapshot = self._create_test_snapshot()
        self.tag_manager.create_tag(snapshot, "test_tag")

        # Should not raise exception
        self.tag_manager.create_tag(snapshot, "test_tag", ignore_if_exists=True)

    def test_list_tags(self):
        """Test listing all tags."""
        snapshot = self._create_test_snapshot()

        tags = self.tag_manager.list_tags()
        self.assertEqual(len(tags), 0)

        self.tag_manager.create_tag(snapshot, "tag1")
        tags = self.tag_manager.list_tags()
        self.assertEqual(len(tags), 1)
        self.assertIn("tag1", tags)

        self.tag_manager.create_tag(snapshot, "tag2")
        self.tag_manager.create_tag(snapshot, "tag3")
        tags = self.tag_manager.list_tags()
        self.assertEqual(len(tags), 3)
        self.assertIn("tag1", tags)
        self.assertIn("tag2", tags)
        self.assertIn("tag3", tags)

    def test_delete_tag(self):
        """Test deleting a tag."""
        snapshot = self._create_test_snapshot()
        self.tag_manager.create_tag(snapshot, "test_tag")

        result = self.tag_manager.delete_tag("test_tag")
        self.assertTrue(result)
        self.assertFalse(self.tag_manager.tag_exists("test_tag"))

    def test_delete_nonexistent_tag(self):
        """Test deleting nonexistent tag."""
        result = self.tag_manager.delete_tag("nonexistent")
        self.assertFalse(result)

    def test_delete_tag_with_blank_name(self):
        """Test deleting tag with blank name."""
        with self.assertRaises(ValueError) as context:
            self.tag_manager.delete_tag("")
        self.assertIn("blank", str(context.exception))

        with self.assertRaises(ValueError) as context:
            self.tag_manager.delete_tag("   ")
        self.assertIn("blank", str(context.exception))

    def test_rename_tag(self):
        """Test renaming a tag."""
        snapshot = self._create_test_snapshot()
        self.tag_manager.create_tag(snapshot, "old_tag")

        self.tag_manager.rename_tag("old_tag", "new_tag")

        self.assertFalse(self.tag_manager.tag_exists("old_tag"))
        self.assertTrue(self.tag_manager.tag_exists("new_tag"))

        tag = self.tag_manager.get("new_tag")
        self.assertIsNotNone(tag)
        self.assertEqual(tag.id, snapshot.id)

    def test_rename_tag_with_blank_old_name(self):
        """Test renaming tag with blank old name."""
        snapshot = self._create_test_snapshot()
        self.tag_manager.create_tag(snapshot, "test_tag")

        with self.assertRaises(ValueError) as context:
            self.tag_manager.rename_tag("", "new_tag")
        self.assertIn("Old tag name shouldn't be blank", str(context.exception))

        with self.assertRaises(ValueError) as context:
            self.tag_manager.rename_tag("   ", "new_tag")
        self.assertIn("Old tag name shouldn't be blank", str(context.exception))

    def test_rename_tag_with_blank_new_name(self):
        """Test renaming tag with blank new name."""
        snapshot = self._create_test_snapshot()
        self.tag_manager.create_tag(snapshot, "old_tag")

        with self.assertRaises(ValueError) as context:
            self.tag_manager.rename_tag("old_tag", "")
        self.assertIn("New tag name shouldn't be blank", str(context.exception))

        with self.assertRaises(ValueError) as context:
            self.tag_manager.rename_tag("old_tag", "   ")
        self.assertIn("New tag name shouldn't be blank", str(context.exception))

    def test_rename_tag_nonexistent_old_tag(self):
        """Test renaming nonexistent old tag."""
        with self.assertRaises(ValueError) as context:
            self.tag_manager.rename_tag("nonexistent", "new_tag")
        self.assertIn("doesn't exist", str(context.exception))

    def test_rename_tag_new_name_already_exists(self):
        """Test renaming tag when new name already exists."""
        snapshot = self._create_test_snapshot()
        self.tag_manager.create_tag(snapshot, "old_tag")
        self.tag_manager.create_tag(snapshot, "existing_tag")

        with self.assertRaises(ValueError) as context:
            self.tag_manager.rename_tag("old_tag", "existing_tag")
        self.assertIn("already exists", str(context.exception))

    def test_rename_tag_same_name(self):
        """Test renaming tag to the same name."""
        snapshot = self._create_test_snapshot()
        self.tag_manager.create_tag(snapshot, "test_tag")

        with self.assertRaises(ValueError) as context:
            self.tag_manager.rename_tag("test_tag", "test_tag")
        self.assertIn("already exists", str(context.exception))

    def test_rename_tag_preserves_content(self):
        """Test that renaming tag preserves content correctly."""
        snapshot1 = self._create_test_snapshot(snapshot_id=1, schema_id=1)
        snapshot2 = self._create_test_snapshot(snapshot_id=2, schema_id=2)

        self.tag_manager.create_tag(snapshot1, "old_tag")
        self.tag_manager.create_tag(snapshot2, "other_tag")

        self.tag_manager.rename_tag("old_tag", "new_tag")

        # Check that renamed tag has correct content
        new_tag = self.tag_manager.get("new_tag")
        self.assertEqual(new_tag.id, 1)
        self.assertEqual(new_tag.schema_id, 1)

        # Check that other tag is unchanged
        other_tag = self.tag_manager.get("other_tag")
        self.assertEqual(other_tag.id, 2)
        self.assertEqual(other_tag.schema_id, 2)

    def test_rename_tag_with_branch(self):
        """Test renaming tag in a branch."""
        tag_manager = TagManager(self.file_io, self.temp_dir, "test_branch")
        snapshot = self._create_test_snapshot()
        tag_manager.create_tag(snapshot, "old_tag")

        tag_manager.rename_tag("old_tag", "new_tag")

        self.assertFalse(tag_manager.tag_exists("old_tag"))
        self.assertTrue(tag_manager.tag_exists("new_tag"))

        tag = tag_manager.get("new_tag")
        self.assertIsNotNone(tag)
        self.assertEqual(tag.id, snapshot.id)


if __name__ == '__main__':
    unittest.main()
