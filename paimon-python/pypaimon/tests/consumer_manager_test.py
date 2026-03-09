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
#  Unless required by applicable law or agreed to in writing,
#  distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
################################################################################
import os
import tempfile
import time
import unittest
from datetime import datetime

from pypaimon.consumer.consumer import Consumer
from pypaimon.consumer.consumer_manager import ConsumerManager
from pypaimon.filesystem.local_file_io import LocalFileIO


class ConsumerManagerTest(unittest.TestCase):
    """Test cases for ConsumerManager."""

    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.file_io = LocalFileIO(self.temp_dir)
        self.manager = ConsumerManager(self.file_io, self.temp_dir)
        self.consumer_manager_branch = ConsumerManager(self.file_io, self.temp_dir, "branch1")

    def tearDown(self):
        """Clean up test environment."""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_retry(self):
        """Test retry mechanism for corrupted consumer file."""
        # Create corrupted consumer file
        consumer_dir = os.path.join(self.temp_dir, "consumer")
        os.makedirs(consumer_dir, exist_ok=True)
        consumer_file = os.path.join(consumer_dir, "consumer-id1")
        with open(consumer_file, 'w') as f:
            f.write("invalid json content")

        # Should raise RuntimeError after retries
        with self.assertRaises(RuntimeError) as context:
            self.manager.consumer("id1")
        self.assertIn("Retry fail after 10 times", str(context.exception))

    def test_basic_operations(self):
        """Test basic consumer operations."""
        # Test non-existent consumer
        consumer = self.manager.consumer("id1")
        self.assertIsNone(consumer)

        # Test min next snapshot when no consumers
        min_snapshot = self.manager.min_next_snapshot()
        self.assertIsNone(min_snapshot)

        # Reset consumer
        self.manager.reset_consumer("id1", Consumer(5))
        consumer = self.manager.consumer("id1")
        self.assertIsNotNone(consumer)
        self.assertEqual(consumer.next_snapshot, 5)

        # Reset another consumer
        self.manager.reset_consumer("id2", Consumer(8))
        consumer = self.manager.consumer("id2")
        self.assertIsNotNone(consumer)
        self.assertEqual(consumer.next_snapshot, 8)

        # Test min next snapshot
        min_snapshot = self.manager.min_next_snapshot()
        self.assertEqual(min_snapshot, 5)

    def test_branch_operations(self):
        """Test consumer operations on different branches."""
        # Test non-existent consumer on branch
        consumer = self.consumer_manager_branch.consumer("id1")
        self.assertIsNone(consumer)

        # Test min next snapshot when no consumers on branch
        min_snapshot = self.consumer_manager_branch.min_next_snapshot()
        self.assertIsNone(min_snapshot)

        # Reset consumer on branch
        self.consumer_manager_branch.reset_consumer("id1", Consumer(5))
        consumer = self.consumer_manager_branch.consumer("id1")
        self.assertIsNotNone(consumer)
        self.assertEqual(consumer.next_snapshot, 5)

        # Reset another consumer on branch
        self.consumer_manager_branch.reset_consumer("id2", Consumer(8))
        consumer = self.consumer_manager_branch.consumer("id2")
        self.assertIsNotNone(consumer)
        self.assertEqual(consumer.next_snapshot, 8)

        # Test min next snapshot on branch
        min_snapshot = self.consumer_manager_branch.min_next_snapshot()
        self.assertEqual(min_snapshot, 5)

    def test_expire(self):
        """Test consumer expiration."""
        # Create consumers with different timestamps
        self.manager.reset_consumer("id1", Consumer(1))
        time.sleep(1)
        expire_datetime = datetime.now()
        time.sleep(1)
        self.manager.reset_consumer("id2", Consumer(2))

        # Check expire
        self.manager.expire(expire_datetime)
        consumer1 = self.manager.consumer("id1")
        self.assertIsNone(consumer1)
        consumer2 = self.manager.consumer("id2")
        self.assertIsNotNone(consumer2)
        self.assertEqual(consumer2.next_snapshot, 2)

        # Check last modification
        expire_datetime = datetime.now()
        time.sleep(1)
        self.manager.reset_consumer("id2", Consumer(3))
        self.manager.expire(expire_datetime)
        consumer2 = self.manager.consumer("id2")
        self.assertIsNotNone(consumer2)
        self.assertEqual(consumer2.next_snapshot, 3)

    def test_expire_branch(self):
        """Test consumer expiration on branch."""
        # Create consumers on branch with different timestamps
        self.consumer_manager_branch.reset_consumer("id3", Consumer(1))
        time.sleep(1)
        expire_datetime = datetime.now()
        time.sleep(1)
        self.consumer_manager_branch.reset_consumer("id4", Consumer(2))

        # Check expire on branch
        self.consumer_manager_branch.expire(expire_datetime)
        consumer3 = self.consumer_manager_branch.consumer("id3")
        self.assertIsNone(consumer3)
        consumer4 = self.consumer_manager_branch.consumer("id4")
        self.assertIsNotNone(consumer4)
        self.assertEqual(consumer4.next_snapshot, 2)

        # Check last modification on branch
        expire_datetime = datetime.now()
        time.sleep(1)
        self.consumer_manager_branch.reset_consumer("id4", Consumer(3))
        self.consumer_manager_branch.expire(expire_datetime)
        consumer4 = self.consumer_manager_branch.consumer("id4")
        self.assertIsNotNone(consumer4)
        self.assertEqual(consumer4.next_snapshot, 3)

    def test_read_consumer(self):
        """Test reading consumer from different branches."""
        # Create consumer on main branch
        self.manager.reset_consumer("id1", Consumer(5))
        consumer = self.manager.consumer("id1")
        self.assertIsNotNone(consumer)
        self.assertEqual(consumer.next_snapshot, 5)

        # Create consumer on branch
        self.consumer_manager_branch.reset_consumer("id2", Consumer(5))
        consumer = self.consumer_manager_branch.consumer("id2")
        self.assertIsNotNone(consumer)
        self.assertEqual(consumer.next_snapshot, 5)

        # Verify id2 doesn't exist on main branch
        consumer = self.manager.consumer("id2")
        self.assertIsNone(consumer)

    def test_list_all_ids(self):
        """Test listing all consumer IDs."""
        # Initially empty
        ids = self.manager.list_all_ids()
        self.assertEqual(len(ids), 0)

        # Add consumers
        self.manager.reset_consumer("id1", Consumer(1))
        self.manager.reset_consumer("id2", Consumer(2))
        self.manager.reset_consumer("id3", Consumer(3))

        # List IDs
        ids = self.manager.list_all_ids()
        self.assertEqual(len(ids), 3)
        self.assertIn("id1", ids)
        self.assertIn("id2", ids)
        self.assertIn("id3", ids)

    def test_consumers(self):
        """Test getting all consumers."""
        # Initially empty
        consumers = self.manager.consumers()
        self.assertEqual(len(consumers), 0)

        # Add consumers
        self.manager.reset_consumer("id1", Consumer(1))
        self.manager.reset_consumer("id2", Consumer(2))
        self.manager.reset_consumer("id3", Consumer(3))

        # Get all consumers
        consumers = self.manager.consumers()
        self.assertEqual(len(consumers), 3)
        self.assertEqual(consumers["id1"], 1)
        self.assertEqual(consumers["id2"], 2)
        self.assertEqual(consumers["id3"], 3)

    def test_delete_consumer(self):
        """Test deleting consumer."""
        # Create consumer
        self.manager.reset_consumer("id1", Consumer(5))
        consumer = self.manager.consumer("id1")
        self.assertIsNotNone(consumer)

        # Delete consumer
        self.manager.delete_consumer("id1")
        consumer = self.manager.consumer("id1")
        self.assertIsNone(consumer)

    def test_clear_consumers(self):
        """Test clearing consumers with patterns."""
        # Add multiple consumers
        self.manager.reset_consumer("test-id1", Consumer(1))
        self.manager.reset_consumer("test-id2", Consumer(2))
        self.manager.reset_consumer("prod-id1", Consumer(3))
        self.manager.reset_consumer("prod-id2", Consumer(4))

        # Clear test consumers
        self.manager.clear_consumers("test-.*")

        # Verify test consumers are deleted
        self.assertIsNone(self.manager.consumer("test-id1"))
        self.assertIsNone(self.manager.consumer("test-id2"))

        # Verify prod consumers remain
        self.assertIsNotNone(self.manager.consumer("prod-id1"))
        self.assertIsNotNone(self.manager.consumer("prod-id2"))

    def test_clear_consumers_with_exclusion(self):
        """Test clearing consumers with inclusion and exclusion patterns."""
        # Add multiple consumers
        self.manager.reset_consumer("test-id1", Consumer(1))
        self.manager.reset_consumer("test-id2", Consumer(2))
        self.manager.reset_consumer("test-backup", Consumer(3))

        # Clear test consumers but exclude backup
        self.manager.clear_consumers("test-.*", "test-backup")

        # Verify test-id1 and test-id2 are deleted
        self.assertIsNone(self.manager.consumer("test-id1"))
        self.assertIsNone(self.manager.consumer("test-id2"))

        # Verify test-backup remains
        self.assertIsNotNone(self.manager.consumer("test-backup"))


if __name__ == '__main__':
    unittest.main()
