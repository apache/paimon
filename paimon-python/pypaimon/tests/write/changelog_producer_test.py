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

import glob
import os
import shutil
import tempfile
import unittest

import pyarrow as pa

import json

from pypaimon import CatalogFactory, Schema
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.write.commit_message import CommitMessage


class ChangelogProducerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)
        cls.pk_schema = pa.schema([
            pa.field('user_id', pa.int32(), nullable=False),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            pa.field('dt', pa.string(), nullable=False)
        ])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create_table(self, table_name, options=None):
        schema = Schema.from_pyarrow_schema(
            self.pk_schema,
            partition_keys=['dt'],
            primary_keys=['user_id', 'dt'],
            options=options or {}
        )
        self.catalog.create_table(f'default.{table_name}', schema, False)
        return self.catalog.get_table(f'default.{table_name}')

    def _sample_data(self):
        return pa.Table.from_pydict({
            'user_id': [1, 2, 3],
            'item_id': [101, 102, 103],
            'behavior': ['click', 'buy', 'view'],
            'dt': ['p1', 'p1', 'p1']
        }, schema=self.pk_schema)

    def test_commit_message_with_changelog(self):
        msg = CommitMessage(partition=('p1',), bucket=0, new_files=[], changelog_files=[])
        self.assertTrue(msg.is_empty())

        msg2 = CommitMessage(partition=('p1',), bucket=0, new_files=['fake'])
        self.assertFalse(msg2.is_empty())
        self.assertEqual(msg2.changelog_files, [])

    def test_full_compaction_and_lookup_no_changelog_from_writer(self):
        """FULL_COMPACTION and LOOKUP rely on dedicated compaction for changelog,
        so the Python writer should not produce changelog files for these modes."""
        for mode in ['full-compaction', 'lookup']:
            table = self._create_table(
                f'test_no_changelog_{mode.replace("-", "_")}',
                options={'changelog-producer': mode, 'bucket': '1'}
            )
            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()

            table_write.write_arrow(self._sample_data())
            table_commit.commit(table_write.prepare_commit())

            bucket_dir = os.path.join(
                self.warehouse, 'default.db',
                f'test_no_changelog_{mode.replace("-", "_")}', 'dt=p1', 'bucket-0')
            changelog_files = glob.glob(os.path.join(bucket_dir, 'changelog-*'))
            self.assertEqual(len(changelog_files), 0,
                             f"Writer should not produce changelog files for {mode}")

            table_write.close()
            table_commit.close()

    def test_none_mode_no_changelog(self):
        table = self._create_table(
            'test_none_mode',
            options={'changelog-producer': 'none', 'bucket': '1'}
        )
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        table_write.write_arrow(self._sample_data())
        table_commit.commit(table_write.prepare_commit())

        snapshot_path = glob.glob(
            os.path.join(self.warehouse, 'default.db', 'test_none_mode', 'snapshot', 'snapshot-*'))
        self.assertTrue(len(snapshot_path) > 0)

        snapshot_json = open(snapshot_path[0]).read()
        snapshot = json.loads(snapshot_json)
        self.assertNotIn('changelogManifestList', snapshot)

        table_write.close()
        table_commit.close()

    def test_input_mode_produces_changelog_files(self):
        table = self._create_table(
            'test_input_files',
            options={'changelog-producer': 'input', 'bucket': '1'}
        )
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        table_write.write_arrow(self._sample_data())
        table_commit.commit(table_write.prepare_commit())

        bucket_dir = os.path.join(
            self.warehouse, 'default.db', 'test_input_files', 'dt=p1', 'bucket-0')
        data_files = glob.glob(os.path.join(bucket_dir, 'data-*'))
        changelog_files = glob.glob(os.path.join(bucket_dir, 'changelog-*'))
        self.assertTrue(len(data_files) > 0, "Should have data files")
        self.assertTrue(len(changelog_files) > 0, "Should have changelog files")

        table_write.close()
        table_commit.close()

    def test_input_mode_snapshot_has_changelog_manifest(self):
        table = self._create_table(
            'test_input_snapshot',
            options={'changelog-producer': 'input', 'bucket': '1'}
        )
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        table_write.write_arrow(self._sample_data())
        table_commit.commit(table_write.prepare_commit())

        snapshot_path = glob.glob(
            os.path.join(self.warehouse, 'default.db', 'test_input_snapshot',
                         'snapshot', 'snapshot-*'))
        self.assertTrue(len(snapshot_path) > 0)

        snapshot_json = open(snapshot_path[0]).read()
        snapshot = json.loads(snapshot_json)
        self.assertIn('changelogManifestList', snapshot)
        self.assertIsNotNone(snapshot['changelogManifestList'])
        self.assertIn('changelogRecordCount', snapshot)
        self.assertEqual(snapshot['changelogRecordCount'], 3)
        self.assertIn('changelogManifestListSize', snapshot)

        table_write.close()
        table_commit.close()

    def test_input_mode_changelog_manifest_readable(self):
        table = self._create_table(
            'test_input_readable',
            options={'changelog-producer': 'input', 'bucket': '1'}
        )
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        table_write.write_arrow(self._sample_data())
        table_commit.commit(table_write.prepare_commit())

        from pypaimon.snapshot.snapshot_manager import SnapshotManager
        snapshot_manager = SnapshotManager(table)
        snapshot = snapshot_manager.get_latest_snapshot()

        self.assertIsNotNone(snapshot.changelog_manifest_list)

        manifest_list_manager = ManifestListManager(table)
        changelog_manifests = manifest_list_manager.read_changelog(snapshot)
        self.assertTrue(len(changelog_manifests) > 0)

        total_changelog_added = sum(m.num_added_files for m in changelog_manifests)
        self.assertTrue(total_changelog_added > 0)

        table_write.close()
        table_commit.close()

    def test_input_mode_multiple_commits(self):
        table = self._create_table(
            'test_input_multi',
            options={'changelog-producer': 'input', 'bucket': '1'}
        )

        # First commit
        write_builder1 = table.new_batch_write_builder()
        table_write1 = write_builder1.new_write()
        table_commit1 = write_builder1.new_commit()
        table_write1.write_arrow(self._sample_data())
        table_commit1.commit(table_write1.prepare_commit())
        table_write1.close()
        table_commit1.close()

        # Second commit with different data
        write_builder2 = table.new_batch_write_builder()
        table_write2 = write_builder2.new_write()
        table_commit2 = write_builder2.new_commit()
        data2 = pa.Table.from_pydict({
            'user_id': [4, 5],
            'item_id': [104, 105],
            'behavior': ['click', 'buy'],
            'dt': ['p1', 'p1']
        }, schema=self.pk_schema)
        table_write2.write_arrow(data2)
        table_commit2.commit(table_write2.prepare_commit())
        table_write2.close()
        table_commit2.close()

        from pypaimon.snapshot.snapshot_manager import SnapshotManager
        snapshot_manager = SnapshotManager(table)
        snapshot = snapshot_manager.get_latest_snapshot()
        self.assertEqual(snapshot.id, 2)
        self.assertIsNotNone(snapshot.changelog_manifest_list)
        self.assertEqual(snapshot.changelog_record_count, 2)

    def test_abort_cleans_up_changelog_files(self):
        table = self._create_table(
            'test_input_abort',
            options={'changelog-producer': 'input', 'bucket': '1'}
        )
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        table_write.write_arrow(self._sample_data())
        commit_messages = table_write.prepare_commit()

        bucket_dir = os.path.join(
            self.warehouse, 'default.db', 'test_input_abort', 'dt=p1', 'bucket-0')
        changelog_files_before = glob.glob(os.path.join(bucket_dir, 'changelog-*'))
        self.assertTrue(len(changelog_files_before) > 0)

        table_commit.abort(commit_messages)

        data_files_after = glob.glob(os.path.join(bucket_dir, 'data-*'))
        changelog_files_after = glob.glob(os.path.join(bucket_dir, 'changelog-*'))
        self.assertEqual(len(data_files_after), 0, "Data files should be cleaned up after abort")
        self.assertEqual(len(changelog_files_after), 0, "Changelog files should be cleaned up after abort")

        table_write.close()
        table_commit.close()

    def test_reject_changelog_producer_on_append_only_table(self):
        append_schema = pa.schema([
            ('user_id', pa.int32()),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            ('dt', pa.string())
        ])
        for mode in ['input', 'full-compaction', 'lookup']:
            with self.assertRaises(ValueError, msg=f"Should reject changelog-producer={mode} without PKs"):
                Schema.from_pyarrow_schema(
                    append_schema,
                    partition_keys=['dt'],
                    options={'changelog-producer': mode, 'bucket': '1'}
                )

    def test_changelog_producer_none_allowed_on_append_only_table(self):
        append_schema = pa.schema([
            ('user_id', pa.int32()),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            ('dt', pa.string())
        ])
        schema = Schema.from_pyarrow_schema(
            append_schema,
            partition_keys=['dt'],
            options={'changelog-producer': 'none', 'bucket': '1'}
        )
        self.assertIsNotNone(schema)

    def test_input_mode_changelog_inherits_data_file_format(self):
        table = self._create_table(
            'test_input_changelog_format',
            options={'changelog-producer': 'input', 'bucket': '1', 'file.format': 'orc'}
        )
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        table_write.write_arrow(self._sample_data())
        table_commit.commit(table_write.prepare_commit())

        bucket_dir = os.path.join(
            self.warehouse, 'default.db', 'test_input_changelog_format', 'dt=p1', 'bucket-0')
        changelog_files = glob.glob(os.path.join(bucket_dir, 'changelog-*'))
        self.assertTrue(len(changelog_files) > 0, "Should have changelog files")
        for f in changelog_files:
            self.assertTrue(f.endswith('.orc'),
                            f"Changelog file should inherit data file format (orc), got {f}")

        data_files = glob.glob(os.path.join(bucket_dir, 'data-*'))
        self.assertTrue(len(data_files) > 0, "Should have data files")
        for f in data_files:
            self.assertTrue(f.endswith('.orc'),
                            f"Data file should use orc format, got {f}")

        table_write.close()
        table_commit.close()

    def test_input_mode_changelog_respects_changelog_file_format(self):
        table = self._create_table(
            'test_input_cl_file_fmt',
            options={'changelog-producer': 'input', 'bucket': '1',
                     'file.format': 'parquet', 'changelog-file.format': 'orc'}
        )
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        table_write.write_arrow(self._sample_data())
        table_commit.commit(table_write.prepare_commit())

        bucket_dir = os.path.join(
            self.warehouse, 'default.db', 'test_input_cl_file_fmt', 'dt=p1', 'bucket-0')
        changelog_files = glob.glob(os.path.join(bucket_dir, 'changelog-*'))
        self.assertTrue(len(changelog_files) > 0, "Should have changelog files")
        for f in changelog_files:
            self.assertTrue(f.endswith('.orc'),
                            f"Changelog file should use orc format, got {f}")

        data_files = glob.glob(os.path.join(bucket_dir, 'data-*'))
        self.assertTrue(len(data_files) > 0, "Should have data files")
        for f in data_files:
            self.assertTrue(f.endswith('.parquet'),
                            f"Data file should use parquet format, got {f}")

        table_write.close()
        table_commit.close()


if __name__ == '__main__':
    unittest.main()
