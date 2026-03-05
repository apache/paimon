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
Tests for Consumer and ConsumerManager.
TDD: These tests are written first, before the implementation.
"""

import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import Mock

from pypaimon.consumer.consumer import Consumer
from pypaimon.consumer.consumer_manager import ConsumerManager


class ConsumerTest(unittest.TestCase):
    """Tests for Consumer data class."""

    def test_consumer_creation(self):
        """Consumer should store next_snapshot value."""
        consumer = Consumer(next_snapshot=42)
        self.assertEqual(consumer.next_snapshot, 42)

    def test_consumer_to_json(self):
        """Consumer should serialize to JSON with nextSnapshot field."""
        consumer = Consumer(next_snapshot=42)
        json_str = consumer.to_json()

        # Parse and verify
        data = json.loads(json_str)
        self.assertEqual(data["nextSnapshot"], 42)

    def test_consumer_from_json(self):
        """Consumer should deserialize from JSON."""
        json_str = '{"nextSnapshot": 42}'
        consumer = Consumer.from_json(json_str)

        self.assertEqual(consumer.next_snapshot, 42)

    def test_consumer_from_json_ignores_unknown_fields(self):
        """Consumer should ignore unknown fields in JSON."""
        json_str = '{"nextSnapshot": 42, "unknownField": "value"}'
        consumer = Consumer.from_json(json_str)

        self.assertEqual(consumer.next_snapshot, 42)

    def test_consumer_roundtrip(self):
        """Consumer should survive JSON roundtrip."""
        original = Consumer(next_snapshot=12345)
        json_str = original.to_json()
        restored = Consumer.from_json(json_str)

        self.assertEqual(restored.next_snapshot, original.next_snapshot)


class ConsumerManagerTest(unittest.TestCase):
    """Tests for ConsumerManager."""

    def setUp(self):
        """Create a temporary directory for testing."""
        self.tempdir = tempfile.mkdtemp()
        self.table_path = os.path.join(self.tempdir, "test_table")
        os.makedirs(self.table_path)

        # Create mock file_io
        self.file_io = Mock()
        self._setup_file_io_mock()

    def tearDown(self):
        """Clean up temporary directory."""
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def _setup_file_io_mock(self):
        """Setup file_io mock to use real filesystem."""
        def read_file_utf8(path):
            with open(path, 'r') as f:
                return f.read()

        def overwrite_file_utf8(path, content):
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'w') as f:
                f.write(content)

        def exists(path):
            return os.path.exists(path)

        def delete_quietly(path):
            if os.path.exists(path):
                os.remove(path)

        self.file_io.read_file_utf8 = read_file_utf8
        self.file_io.overwrite_file_utf8 = overwrite_file_utf8
        self.file_io.exists = exists
        self.file_io.delete_quietly = delete_quietly

    def test_consumer_manager_reset_consumer(self):
        """reset_consumer should write consumer state to file."""
        manager = ConsumerManager(self.file_io, self.table_path)
        consumer = Consumer(next_snapshot=42)

        manager.reset_consumer("my-consumer", consumer)

        # Verify file exists
        consumer_file = os.path.join(self.table_path, "consumer", "consumer-my-consumer")
        self.assertTrue(os.path.exists(consumer_file))

        # Verify content
        with open(consumer_file, 'r') as f:
            content = f.read()
        data = json.loads(content)
        self.assertEqual(data["nextSnapshot"], 42)

    def test_consumer_manager_get_consumer(self):
        """consumer() should read consumer state from file."""
        manager = ConsumerManager(self.file_io, self.table_path)

        # Write consumer file directly
        consumer_dir = os.path.join(self.table_path, "consumer")
        os.makedirs(consumer_dir, exist_ok=True)
        consumer_file = os.path.join(consumer_dir, "consumer-my-consumer")
        with open(consumer_file, 'w') as f:
            f.write('{"nextSnapshot": 42}')

        # Read via manager
        consumer = manager.consumer("my-consumer")

        self.assertIsNotNone(consumer)
        self.assertEqual(consumer.next_snapshot, 42)

    def test_consumer_manager_get_nonexistent_consumer(self):
        """consumer() should return None for non-existent consumer."""
        manager = ConsumerManager(self.file_io, self.table_path)

        consumer = manager.consumer("nonexistent")

        self.assertIsNone(consumer)

    def test_consumer_manager_delete_consumer(self):
        """delete_consumer should remove consumer file."""
        manager = ConsumerManager(self.file_io, self.table_path)

        # Create consumer first
        manager.reset_consumer("my-consumer", Consumer(next_snapshot=42))
        consumer_file = os.path.join(self.table_path, "consumer", "consumer-my-consumer")
        self.assertTrue(os.path.exists(consumer_file))

        # Delete
        manager.delete_consumer("my-consumer")

        self.assertFalse(os.path.exists(consumer_file))

    def test_consumer_manager_update_consumer(self):
        """reset_consumer should update existing consumer."""
        manager = ConsumerManager(self.file_io, self.table_path)

        # Create initial consumer
        manager.reset_consumer("my-consumer", Consumer(next_snapshot=42))

        # Update
        manager.reset_consumer("my-consumer", Consumer(next_snapshot=100))

        # Verify updated
        consumer = manager.consumer("my-consumer")
        self.assertEqual(consumer.next_snapshot, 100)

    def test_consumer_path(self):
        """Consumer files should be in {table_path}/consumer/consumer-{id}."""
        manager = ConsumerManager(self.file_io, self.table_path)

        path = manager._consumer_path("test-id")

        expected = os.path.join(self.table_path, "consumer", "consumer-test-id")
        self.assertEqual(path, expected)


if __name__ == '__main__':
    unittest.main()
