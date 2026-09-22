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

import unittest
from tempfile import TemporaryDirectory
from unittest.mock import Mock

from parameterized import parameterized

from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.table_commit import BatchTableCommit, StreamTableCommit


class TestTableCommit(unittest.TestCase):

    def test_empty_append_snapshot_is_opt_in_and_can_be_tagged(self):
        import pyarrow as pa
        import pypaimon.multimodal as pmm

        with TemporaryDirectory(prefix="paimon-empty-commit-") as warehouse:
            connection = pmm.connect(options={"warehouse": warehouse})
            schema = pa.schema([pa.field("feature", pa.string(), False)])
            table = connection.create_table("stat", schema=schema)
            empty = pa.Table.from_pylist([], schema=schema)
            table.add(empty)
            snapshots = table.raw_table.snapshot_manager()
            self.assertIsNone(snapshots.get_latest_snapshot())

            def commit_empty():
                writable = table.raw_table.copy({
                    "snapshot.ignore-empty-commit": "false",
                })
                commit = writable.new_batch_write_builder().new_commit()
                try:
                    commit.commit([], snapshot_properties={"source": "empty-stat"})
                finally:
                    commit.close()

            commit_empty()
            snapshot = snapshots.get_latest_snapshot()
            self.assertIsNotNone(snapshot)
            self.assertEqual((1, 0, 0), (
                snapshot.id, snapshot.total_record_count,
                snapshot.delta_record_count))
            self.assertEqual({"source": "empty-stat"}, snapshot.properties)
            table.raw_table.create_tag("empty")
            tagged = table.scan(tag_name="empty").to_arrow()
            self.assertEqual(0, tagged.num_rows)
            self.assertEqual(schema, tagged.schema)

            table.add([{"feature": "state_imu_body"}])
            table.add(empty)
            self.assertEqual(2, snapshots.get_latest_snapshot().id)
            commit_empty()
            snapshot = snapshots.get_latest_snapshot()
            self.assertEqual((3, 1, 0), (
                snapshot.id, snapshot.total_record_count,
                snapshot.delta_record_count))
            self.assertEqual([{"feature": "state_imu_body"}], table.scan().to_list())
            self.assertEqual([], table.scan(tag_name="empty").to_list())

    def _create_commit(self, cls, overwrite_partition=None):
        commit = cls.__new__(cls)
        commit.table = Mock()
        commit.table.identifier = 'default.test_table'
        commit.table.options.native_commit_enabled.return_value = False
        commit.commit_user = 'test_user'
        commit.overwrite_partition = overwrite_partition
        commit.file_store_commit = Mock()
        commit.batch_committed = False
        commit._commit_callbacks = []
        commit._native_commit = None
        return commit, commit.file_store_commit

    # -- Overwrite mode: should always call overwrite(), even with empty messages --

    @parameterized.expand([
        ("no_messages", []),
        ("all_empty", [False]),
        ("non_empty", [True]),
        ("mixed", [False, True]),
    ])
    def test_overwrite_forwards_filtered_messages(self, name, msg_flags):
        """Overwrite mode should always call overwrite(), filtering out empty messages."""
        commit, mock_fsc = self._create_commit(BatchTableCommit, overwrite_partition={'f0': 1})

        messages = [
            CommitMessage(partition=(1,), bucket=0, new_files=[Mock()] if has_files else [])
            for has_files in msg_flags
        ]
        commit.commit(messages)

        mock_fsc.overwrite.assert_called_once_with(
            overwrite_partition={'f0': 1},
            commit_messages=[m for m in messages if not m.is_empty()],
            commit_identifier=BATCH_COMMIT_IDENTIFIER,
        )

    # -- Append mode: should only call commit() when there are non-empty messages --

    @parameterized.expand([
        ("no_messages", []),
        ("all_empty", [False]),
        ("non_empty", [True]),
    ])
    def test_append_forwards_non_empty_messages(self, name, msg_flags):
        """Append mode should only call commit() when there are non-empty messages."""
        commit, mock_fsc = self._create_commit(BatchTableCommit, overwrite_partition=None)

        messages = [
            CommitMessage(partition=(), bucket=0, new_files=[Mock()] if has_files else [])
            for has_files in msg_flags
        ]
        commit.commit(messages)

        if any(msg_flags):
            mock_fsc.commit.assert_called_once_with(
                commit_messages=[m for m in messages if not m.is_empty()],
                commit_identifier=BATCH_COMMIT_IDENTIFIER,
            )
        else:
            mock_fsc.commit.assert_not_called()
            mock_fsc.overwrite.assert_not_called()

    def test_batch_commit_forwards_snapshot_properties(self):
        commit, mock_fsc = self._create_commit(
            BatchTableCommit, overwrite_partition=None)
        message = CommitMessage(
            partition=(), bucket=0, new_files=[Mock()])

        commit.commit([message], snapshot_properties={"source": "capture"})

        mock_fsc.commit.assert_called_once_with(
            commit_messages=[message],
            commit_identifier=BATCH_COMMIT_IDENTIFIER,
            snapshot_properties={"source": "capture"},
        )

    def test_overwrite_forwards_snapshot_properties(self):
        commit, mock_fsc = self._create_commit(
            BatchTableCommit, overwrite_partition={"dt": "2024-01-15"})
        message = CommitMessage(
            partition=("2024-01-15",), bucket=0, new_files=[Mock()])

        commit.commit([message], snapshot_properties={"source": "capture"})

        mock_fsc.overwrite.assert_called_once_with(
            overwrite_partition={"dt": "2024-01-15"},
            commit_messages=[message],
            commit_identifier=BATCH_COMMIT_IDENTIFIER,
            snapshot_properties={"source": "capture"},
        )

    # -- StreamTableCommit overwrite should also reach overwrite() with empty messages --

    def test_stream_commit_overwrite_empty_messages(self):
        commit, mock_fsc = self._create_commit(StreamTableCommit, overwrite_partition={'dt': '2024-01-15'})

        commit.commit([], commit_identifier=42)

        mock_fsc.overwrite.assert_called_once_with(
            overwrite_partition={'dt': '2024-01-15'},
            commit_messages=[],
            commit_identifier=42,
        )

    def test_stream_commit_forwards_snapshot_properties(self):
        commit, mock_fsc = self._create_commit(
            StreamTableCommit, overwrite_partition=None)
        message = CommitMessage(
            partition=(), bucket=0, new_files=[Mock()])

        commit.commit(
            [message],
            commit_identifier=42,
            snapshot_properties={"checkpoint": "42"},
        )

        mock_fsc.commit.assert_called_once_with(
            commit_messages=[message],
            commit_identifier=42,
            snapshot_properties={"checkpoint": "42"},
        )
