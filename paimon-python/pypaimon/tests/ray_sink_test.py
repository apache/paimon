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

import os
import tempfile
import unittest
from unittest.mock import Mock, patch

import pyarrow as pa
from ray.data._internal.execution.interfaces import TaskContext

from pypaimon import CatalogFactory, Schema
from pypaimon.write.ray_datasink import PaimonDatasink
from pypaimon.write.commit_message import CommitMessage


class RaySinkTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.warehouse_path = os.path.join(self.temp_dir, "warehouse")
        os.makedirs(self.warehouse_path, exist_ok=True)

        catalog_options = {
            "warehouse": self.warehouse_path
        }
        self.catalog = CatalogFactory.create(catalog_options)
        self.catalog.create_database("test_db", ignore_if_exists=True)

        pa_schema = pa.schema([
            ('id', pa.int64()),
            ('name', pa.string()),
            ('value', pa.float64())
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema=pa_schema,
            partition_keys=None,
            primary_keys=['id'],
            options={'bucket': '2'},  # Use fixed bucket mode for testing
            comment='test table'
        )

        self.table_identifier = "test_db.test_table"
        self.catalog.create_table(self.table_identifier, schema, ignore_if_exists=False)
        self.table = self.catalog.get_table(self.table_identifier)

    def tearDown(self):
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_init_and_serialization(self):
        """Test initialization, serialization, and table name."""
        datasink = PaimonDatasink(self.table, overwrite=False)
        self.assertEqual(datasink.table, self.table)
        self.assertFalse(datasink.overwrite)
        self.assertIsNone(datasink._writer_builder)
        self.assertEqual(datasink._table_name, "test_db.test_table")

        datasink_overwrite = PaimonDatasink(self.table, overwrite=True)
        self.assertTrue(datasink_overwrite.overwrite)

        # Test serialization
        datasink._writer_builder = Mock()
        state = datasink.__getstate__()
        self.assertIn('table', state)
        self.assertIn('overwrite', state)
        self.assertIn('_writer_builder', state)

        new_datasink = PaimonDatasink.__new__(PaimonDatasink)
        new_datasink.__setstate__(state)
        self.assertEqual(new_datasink.table, self.table)
        self.assertFalse(new_datasink.overwrite)

    def test_table_and_writer_builder_serializable(self):
        import pickle
        try:
            pickled_table = pickle.dumps(self.table)
            unpickled_table = pickle.loads(pickled_table)
            self.assertIsNotNone(unpickled_table)
            builder = unpickled_table.new_batch_write_builder()
            self.assertIsNotNone(builder)
        except Exception as e:
            self.fail(f"Table object is not serializable: {e}")
        
        writer_builder = self.table.new_batch_write_builder()
        try:
            pickled_builder = pickle.dumps(writer_builder)
            unpickled_builder = pickle.loads(pickled_builder)
            self.assertIsNotNone(unpickled_builder)
            table_write = unpickled_builder.new_write()
            self.assertIsNotNone(table_write)
            table_write.close()
        except Exception as e:
            self.fail(f"WriterBuilder is not serializable: {e}")
        
        overwrite_builder = self.table.new_batch_write_builder().overwrite()
        try:
            pickled_overwrite = pickle.dumps(overwrite_builder)
            unpickled_overwrite = pickle.loads(pickled_overwrite)
            self.assertIsNotNone(unpickled_overwrite)
            # static_partition is a dict, empty dict {} means overwrite all partitions
            self.assertIsNotNone(unpickled_overwrite.static_partition)
            self.assertIsInstance(unpickled_overwrite.static_partition, dict)
        except Exception as e:
            self.fail(f"Overwrite WriterBuilder is not serializable: {e}")

    def test_on_write_start(self):
        """Test on_write_start with normal and overwrite modes."""
        datasink = PaimonDatasink(self.table, overwrite=False)
        datasink.on_write_start()
        self.assertIsNotNone(datasink._writer_builder)
        self.assertFalse(datasink._writer_builder.static_partition)

        datasink_overwrite = PaimonDatasink(self.table, overwrite=True)
        datasink_overwrite.on_write_start()
        self.assertIsNotNone(datasink_overwrite._writer_builder.static_partition)

    def test_write(self):
        """Test write method: empty blocks, multiple blocks, error handling, and resource cleanup."""
        datasink = PaimonDatasink(self.table, overwrite=False)
        datasink.on_write_start()
        ctx = Mock(spec=TaskContext)

        # Test empty block
        empty_table = pa.table({
            'id': pa.array([], type=pa.int64()),
            'name': pa.array([], type=pa.string()),
            'value': pa.array([], type=pa.float64())
        })
        result = datasink.write([empty_table], ctx)
        self.assertEqual(result, [])

        # Test single and multiple blocks
        single_block = pa.table({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [1.1, 2.2, 3.3]
        })
        result = datasink.write([single_block], ctx)
        self.assertIsInstance(result, list)
        if result:
            self.assertTrue(all(isinstance(msg, CommitMessage) for msg in result))

        block1 = pa.table({
            'id': [4, 5],
            'name': ['David', 'Eve'],
            'value': [4.4, 5.5]
        })
        block2 = pa.table({
            'id': [6, 7],
            'name': ['Frank', 'Grace'],
            'value': [6.6, 7.7]
        })
        result = datasink.write([block1, block2], ctx)
        self.assertIsInstance(result, list)
        if result:
            self.assertTrue(all(isinstance(msg, CommitMessage) for msg in result))

        # Test that write creates WriteBuilder on worker (not using driver's builder)
        with patch.object(self.table, 'new_batch_write_builder') as mock_builder:
            mock_write_builder = Mock()
            mock_write_builder.overwrite.return_value = mock_write_builder
            mock_write = Mock()
            mock_write.prepare_commit.return_value = []
            mock_write_builder.new_write.return_value = mock_write
            mock_builder.return_value = mock_write_builder

            data_table = pa.table({
                'id': [1],
                'name': ['Alice'],
                'value': [1.1]
            })
            datasink.write([data_table], ctx)
            mock_builder.assert_called_once()

        invalid_table = pa.table({
            'wrong_column': [1, 2, 3]
        })
        with self.assertRaises(Exception):
            datasink.write([invalid_table], ctx)

        with patch.object(self.table, 'new_batch_write_builder') as mock_builder:
            mock_write_builder = Mock()
            mock_write_builder.overwrite.return_value = mock_write_builder
            mock_write = Mock()
            mock_write.write_arrow.side_effect = Exception("Write error")
            mock_write_builder.new_write.return_value = mock_write
            mock_builder.return_value = mock_write_builder

            data_table = pa.table({
                'id': [1],
                'name': ['Alice'],
                'value': [1.1]
            })
            with self.assertRaises(Exception):
                datasink.write([data_table], ctx)
            mock_write.close.assert_called_once()

    def test_on_write_complete(self):
        from ray.data.datasource.datasink import WriteResult

        # Test empty messages
        datasink = PaimonDatasink(self.table, overwrite=False)
        datasink.on_write_start()
        write_result = WriteResult(
            num_rows=0,
            size_bytes=0,
            write_returns=[[], []]
        )
        datasink.on_write_complete(write_result)

        # Test with messages and filtering empty messages
        datasink = PaimonDatasink(self.table, overwrite=False)
        datasink.on_write_start()
        commit_msg1 = Mock(spec=CommitMessage)
        commit_msg1.is_empty.return_value = False
        commit_msg2 = Mock(spec=CommitMessage)
        commit_msg2.is_empty.return_value = False
        empty_msg = Mock(spec=CommitMessage)
        empty_msg.is_empty.return_value = True

        write_result = WriteResult(
            num_rows=0,
            size_bytes=0,
            write_returns=[[commit_msg1], [commit_msg2], [empty_msg]]
        )

        mock_commit = Mock()
        datasink._writer_builder.new_commit = Mock(return_value=mock_commit)
        datasink.on_write_complete(write_result)

        mock_commit.commit.assert_called_once()
        commit_args = mock_commit.commit.call_args[0][0]
        self.assertEqual(len(commit_args), 2)  # Empty message filtered out
        mock_commit.close.assert_called_once()

        # Test commit failure: abort should be called
        datasink = PaimonDatasink(self.table, overwrite=False)
        datasink.on_write_start()
        commit_msg1 = Mock(spec=CommitMessage)
        commit_msg1.is_empty.return_value = False
        commit_msg2 = Mock(spec=CommitMessage)
        commit_msg2.is_empty.return_value = False

        write_result = WriteResult(
            num_rows=0,
            size_bytes=0,
            write_returns=[[commit_msg1], [commit_msg2]]
        )

        mock_commit = Mock()
        mock_commit.commit.side_effect = Exception("Commit failed")
        datasink._writer_builder.new_commit = Mock(return_value=mock_commit)

        with self.assertRaises(Exception):
            datasink.on_write_complete(write_result)

        mock_commit.abort.assert_called_once()
        abort_args = mock_commit.abort.call_args[0][0]
        self.assertEqual(len(abort_args), 2)
        mock_commit.close.assert_called_once()

        # Test table_commit creation failure
        datasink = PaimonDatasink(self.table, overwrite=False)
        datasink.on_write_start()
        commit_msg1 = Mock(spec=CommitMessage)
        commit_msg1.is_empty.return_value = False

        write_result = WriteResult(
            num_rows=0,
            size_bytes=0,
            write_returns=[[commit_msg1]]
        )

        mock_new_commit = Mock(side_effect=Exception("Failed to create table_commit"))
        datasink._writer_builder.new_commit = mock_new_commit
        with self.assertRaises(Exception):
            datasink.on_write_complete(write_result)
        self.assertEqual(len(datasink._pending_commit_messages), 1)

    def test_on_write_failed(self):
        # Test without pending messages (on_write_complete() never called)
        datasink = PaimonDatasink(self.table, overwrite=False)
        datasink.on_write_start()
        self.assertEqual(datasink._pending_commit_messages, [])
        error = Exception("Write job failed")
        datasink.on_write_failed(error)  # Should not raise exception

        # Test with pending messages (on_write_complete() was called but failed)
        datasink = PaimonDatasink(self.table, overwrite=False)
        datasink.on_write_start()
        commit_msg1 = Mock(spec=CommitMessage)
        commit_msg2 = Mock(spec=CommitMessage)
        datasink._pending_commit_messages = [commit_msg1, commit_msg2]

        mock_commit = Mock()
        datasink._writer_builder.new_commit = Mock(return_value=mock_commit)
        error = Exception("Write job failed")
        datasink.on_write_failed(error)

        mock_commit.abort.assert_called_once()
        abort_args = mock_commit.abort.call_args[0][0]
        self.assertEqual(len(abort_args), 2)
        self.assertEqual(abort_args[0], commit_msg1)
        self.assertEqual(abort_args[1], commit_msg2)
        mock_commit.close.assert_called_once()
        self.assertEqual(datasink._pending_commit_messages, [])

        # Test abort failure handling (should not raise exception)
        datasink = PaimonDatasink(self.table, overwrite=False)
        datasink.on_write_start()
        commit_msg1 = Mock(spec=CommitMessage)
        datasink._pending_commit_messages = [commit_msg1]

        mock_commit = Mock()
        mock_commit.abort.side_effect = Exception("Abort failed")
        datasink._writer_builder.new_commit = Mock(return_value=mock_commit)
        error = Exception("Write job failed")
        datasink.on_write_failed(error)

        mock_commit.abort.assert_called_once()
        mock_commit.close.assert_called_once()
        self.assertEqual(datasink._pending_commit_messages, [])


if __name__ == '__main__':
    unittest.main()
