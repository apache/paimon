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

import datetime
import tempfile
import unittest
from decimal import Decimal
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.index.dynamic_bucket import (
    HashBucketAssigner,
    _PartitionIndex,
    _iter_hashes,
    compute_assigner,
    to_signed_int32,
    validate_bucket_id,
)
from pypaimon.index.index_file_handler import IndexFileHandler
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.row_key_extractor import DynamicBucketRowKeyExtractor


class DynamicBucketTest(unittest.TestCase):

    @staticmethod
    def _create_table(
        root, name, target_row_num=100, max_buckets=None
    ):
        catalog = CatalogFactory.create({'warehouse': root})
        catalog.create_database('default', True)
        options = {
            'bucket': '-1',
            'dynamic-bucket.target-row-num': str(target_row_num),
            'file.format': 'parquet',
        }
        if max_buckets is not None:
            options['dynamic-bucket.max-buckets'] = str(max_buckets)
        schema = Schema.from_pyarrow_schema(
            pa.schema([
                pa.field('id', pa.int64()),
                pa.field('value', pa.string()),
            ]),
            primary_keys=['id'],
            options=options,
        )
        catalog.create_table(f'default.{name}', schema, False)
        return catalog.get_table(f'default.{name}')

    @staticmethod
    def _prepare_indexed_write(table, ids, bucket=None):
        builder = table.new_batch_write_builder()
        writer = builder.new_write().with_dynamic_bucket_index()
        batch = pa.RecordBatch.from_pydict({
            'id': ids,
            'value': [f'v-{value}' for value in ids],
        })
        if bucket is None:
            writer.write_arrow_batch(batch)
        else:
            writer.write_arrow_batch_to_bucket(batch, bucket)
        return writer, builder.new_commit(), writer.prepare_commit()

    @staticmethod
    def _hash_indexes(table):
        snapshot = table.snapshot_manager().get_latest_snapshot()
        return [
            entry for entry in IndexFileHandler(table).scan(snapshot)
            if entry.index_file.index_type == 'HASH'
        ]

    @staticmethod
    def _commit_arrow(table, ids, values):
        builder = table.new_batch_write_builder()
        writer = builder.new_write()
        writer.write_arrow(pa.table({'id': ids, 'value': values}))
        messages = writer.prepare_commit()
        commit = builder.new_commit()
        commit.commit(messages)
        writer.close()
        commit.close()
        return messages

    @staticmethod
    def _read_arrow(table):
        builder = table.new_read_builder()
        return builder.new_read().to_arrow(builder.new_scan().plan().splits())

    def test_compute_assigner_matches_java(self):
        max_int = 2 ** 31 - 1
        self.assertEqual(compute_assigner(max_int, 0, 5, 5), 2)
        self.assertEqual(compute_assigner(max_int, 1, 5, 5), 3)
        self.assertEqual(compute_assigner(max_int, 2, 5, 5), 4)
        self.assertEqual(compute_assigner(max_int, 3, 5, 5), 0)
        self.assertEqual(compute_assigner(2, 0, 5, 3), 2)
        self.assertEqual(compute_assigner(2, 1, 5, 3), 3)
        self.assertEqual(compute_assigner(2, 2, 5, 3), 4)
        self.assertEqual(compute_assigner(2, 3, 5, 3), 2)
        self.assertEqual(compute_assigner(3, 1, 5, 1), 3)
        self.assertEqual(compute_assigner(3, 2, 5, 1), 3)
        min_int = -(2 ** 31)
        self.assertEqual(compute_assigner(min_int, 0, 5, 5), 3)
        self.assertEqual(compute_assigner(2, min_int, 5, 5), 0)

    def test_binary_row_hash_matches_java_for_bucket_key_types(self):
        # Generated with Java InternalRowSerializer.toBinaryRow(...).hashCode().
        cases = [
            (
                'inline string',
                ('hello',),
                [DataField(0, 'key', AtomicType('STRING'))],
                243722546,
            ),
            (
                'variable string',
                ('hello-java',),
                [DataField(0, 'key', AtomicType('STRING'))],
                -201703277,
            ),
            (
                'composite bucket key',
                ('hello-java', 42),
                [
                    DataField(0, 'key1', AtomicType('STRING')),
                    DataField(1, 'key2', AtomicType('BIGINT')),
                ],
                -2066620165,
            ),
            (
                'compact decimal',
                (Decimal('12345.67'),),
                [DataField(0, 'key', AtomicType('DECIMAL(10, 2)'))],
                754928256,
            ),
            (
                'variable decimal',
                (Decimal('12345678901234567890123.45'),),
                [DataField(0, 'key', AtomicType('DECIMAL(25, 2)'))],
                1388205002,
            ),
            (
                'compact timestamp',
                (datetime.datetime(2026, 1, 2, 3, 4, 5, 123000),),
                [DataField(0, 'key', AtomicType('TIMESTAMP(3)'))],
                -1766746798,
            ),
            (
                'variable timestamp',
                (datetime.datetime(2026, 1, 2, 3, 4, 5, 123456),),
                [DataField(0, 'key', AtomicType('TIMESTAMP(6)'))],
                1245041971,
            ),
            (
                'inline binary',
                (bytes([0, 1, 255, 127]),),
                [DataField(0, 'key', AtomicType('BYTES'))],
                586821318,
            ),
            (
                'variable binary',
                (bytes(range(10)),),
                [DataField(0, 'key', AtomicType('BYTES'))],
                1822312655,
            ),
        ]

        for name, values, fields, java_hash in cases:
            with self.subTest(name=name):
                actual = DynamicBucketRowKeyExtractor._binary_row_hash_code(
                    values, fields
                )
                self.assertEqual(java_hash, to_signed_int32(actual))

    def test_unbounded_bucket_id_matches_java_short_limit(self):
        index = _PartitionIndex({}, {}, 1)

        bucket = index.assign(
            key_hash=1,
            bucket_filter=lambda candidate: candidate == 32767,
            max_buckets_num=-1,
            max_bucket_id=32766,
        )

        self.assertEqual(bucket, 32767)

    def test_rejects_bucket_count_above_java_short_range(self):
        index = _PartitionIndex({}, {}, 1)

        with self.assertRaisesRegex(
            ValueError,
            "'dynamic-bucket.max-buckets' must be -1 or between 1 and 32768",
        ):
            index.assign(
                key_hash=1,
                bucket_filter=lambda candidate: candidate >= 32768,
                max_buckets_num=40000,
                max_bucket_id=32767,
            )

    def test_rejects_bucket_id_above_java_short_range(self):
        with self.assertRaisesRegex(
            ValueError,
            "Dynamic bucket id must be between 0 and 32767, but was 32768",
        ):
            validate_bucket_id(32768)

    def test_restore_rejects_bucket_id_above_java_short_range(self):
        assigner = HashBucketAssigner(
            table=Mock(),
            num_channels=1,
            num_assigners=1,
            assign_id=0,
            target_bucket_row_number=1,
            max_buckets_num=-1,
            snapshot=Mock(),
        )
        entry = Mock()
        entry.bucket = 32768

        with patch(
            'pypaimon.index.dynamic_bucket.IndexFileHandler'
        ) as handler:
            handler.return_value.scan.return_value = [entry]
            with patch.object(
                assigner, '_active_data_buckets', return_value=set()
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "Dynamic bucket id must be between 0 and 32767",
                ):
                    assigner._load_partition((), {1})

            with self.assertRaisesRegex(
                ValueError,
                "Dynamic bucket id must be between 0 and 32767",
            ):
                assigner._restore_requested_hashes((), {1}, {})

    def test_assigner_rejects_record_owned_by_another_writer(self):
        with tempfile.TemporaryDirectory() as root:
            table = self._create_table(root, 'wrong_assigner')
            assigner = HashBucketAssigner(
                table=table,
                num_channels=2,
                num_assigners=2,
                assign_id=0,
                target_bucket_row_number=100,
                max_buckets_num=-1,
            )

            with self.assertRaisesRegex(
                ValueError, 'Record assigner 1 does not match writer assigner 0'
            ):
                assigner.assign((), partition_hash=0, key_hash=1)

    def test_max_buckets_rejects_assigner_without_bucket(self):
        index = _PartitionIndex({}, {}, 1)

        with self.assertRaisesRegex(
            RuntimeError, 'No dynamic bucket is available for this assigner'
        ):
            index.assign(
                key_hash=1,
                bucket_filter=lambda _: False,
                max_buckets_num=1,
                max_bucket_id=0,
            )

    def test_unbounded_buckets_rejects_java_short_id_exhaustion(self):
        index = _PartitionIndex({}, {}, 1)

        with self.assertRaisesRegex(
            RuntimeError, 'No dynamic bucket id remains below Java Short.MAX_VALUE'
        ):
            index.assign(
                key_hash=1,
                bucket_filter=lambda _: False,
                max_buckets_num=-1,
                max_bucket_id=32767,
            )

    def test_corrupt_hash_index_rejects_trailing_bytes(self):
        with tempfile.TemporaryDirectory() as root:
            table = self._create_table(root, 'corrupt_index')
            path = f'{root}/corrupt-hash-index'
            with table.file_io.new_output_stream(path) as stream:
                stream.write(b'\x00\x00\x00')
            entry = IndexManifestEntry(
                kind=0,
                partition=GenericRow([], []),
                bucket=0,
                index_file=IndexFileMeta(
                    index_type='HASH',
                    file_name='corrupt-hash-index',
                    file_size=3,
                    row_count=1,
                    external_path=path,
                ),
            )

            with self.assertRaisesRegex(
                RuntimeError, 'expected a multiple of 4 bytes'
            ):
                list(_iter_hashes(table, entry))

    def test_regular_dynamic_writer_uses_persistent_index(self):
        with tempfile.TemporaryDirectory() as root:
            table = self._create_table(root, 'regular_dynamic')
            writer = table.new_batch_write_builder().new_write()

            self.assertIs(table, writer.row_key_extractor._table)
            writer.write_arrow_batch(pa.RecordBatch.from_pydict({
                'id': [1],
                'value': ['v-1'],
            }))
            messages = writer.prepare_commit()

            self.assertTrue(any(message.index_adds for message in messages))
            self.assertFalse(any(message.index_deletes for message in messages))

    def test_regular_dynamic_writer_restores_mapping_across_commits(self):
        with tempfile.TemporaryDirectory() as root:
            table = self._create_table(root, 'regular_upsert', target_row_num=1)
            self._commit_arrow(table, [1], ['old'])
            self._commit_arrow(table, [2, 1], ['other', 'new'])

            result = self._read_arrow(table).sort_by('id').to_pydict()
            self.assertEqual({'id': [1, 2], 'value': ['new', 'other']}, result)

    @pytest.mark.python_write
    def test_regular_dynamic_writer_retains_only_requested_index_hashes(self):
        with tempfile.TemporaryDirectory() as root:
            table = self._create_table(root, 'bounded_restore')
            self._commit_arrow(
                table, list(range(10)), [f'old-{value}' for value in range(10)]
            )

            writer = table.new_batch_write_builder().new_write()
            writer.write_arrow(pa.table({'id': [3], 'value': ['new-3']}))

            partition_index = writer.row_key_extractor._assigner._partition_indexes[()]
            self.assertEqual(1, len(partition_index.hash_to_bucket))
            self.assertEqual({}, writer.row_key_extractor._index_maintainer._states)

    @pytest.mark.python_write
    def test_legacy_dynamic_data_without_hash_index_fails_fast(self):
        with tempfile.TemporaryDirectory() as root:
            table = self._create_table(root, 'legacy_no_index')
            builder = table.new_batch_write_builder()
            writer = builder.new_write()
            writer.row_key_extractor = DynamicBucketRowKeyExtractor(
                table.table_schema
            )
            writer.write_arrow(pa.table({'id': [1], 'value': ['old']}))
            messages = writer.prepare_commit()
            builder.new_commit().commit(messages)
            writer.close()

            new_writer = table.new_batch_write_builder().new_write()
            with self.assertRaisesRegex(
                RuntimeError, 'has data files but no complete HASH index'
            ):
                new_writer.write_arrow(pa.table({'id': [1], 'value': ['new']}))

    @pytest.mark.python_write
    def test_cross_partition_write_requires_global_index(self):
        with tempfile.TemporaryDirectory() as root:
            catalog = CatalogFactory.create({'warehouse': root})
            catalog.create_database('default', True)
            schema = Schema.from_pyarrow_schema(
                pa.schema([
                    pa.field('id', pa.int64()),
                    pa.field('value', pa.string()),
                    pa.field('dt', pa.string()),
                ]),
                partition_keys=['dt'],
                primary_keys=['id'],
                options={'bucket': '-1'},
            )
            catalog.create_table('default.cross_partition', schema, False)
            table = catalog.get_table('default.cross_partition')

            with self.assertRaisesRegex(
                ValueError, 'CROSS_PARTITION.*global primary-key index'
            ):
                table.new_batch_write_builder().new_write()

    def test_batch_writer_abort_after_prepare_preserves_hash_index(self):
        with tempfile.TemporaryDirectory() as root:
            table = self._create_table(root, 'abort_prepared')
            writer = table.new_batch_write_builder().new_write()
            writer.write_arrow(pa.table({'id': [1], 'value': ['v-1']}))
            messages = writer.prepare_commit()
            index_path = messages[0].index_adds[0].index_file.external_path
            if index_path is None:
                index_path = (
                    table.path_factory().global_index_path_factory()
                    .to_path(messages[0].index_adds[0].index_file.file_name)
                )
            self.assertTrue(table.file_io.exists(index_path))

            writer.abort()

            self.assertTrue(table.file_io.exists(index_path))

    @pytest.mark.python_write
    def test_failed_hash_index_prepare_retries_the_complete_increment(self):
        for failed_call in (1, 2):
            with self.subTest(failed_call=failed_call), tempfile.TemporaryDirectory() as root:
                table = self._create_table(root, 'retry_prepare', target_row_num=1).copy({
                    'write.native.enabled': 'false', 'commit.native.enabled': 'false',
                    'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
                    'changelog-producer': 'input'})
                builder = table.new_stream_write_builder()
                writer, commit = builder.new_write(), builder.new_commit()
                data = pa.table({'id': [1, 2, 3], 'value': ['v-1', 'v-2', 'v-3']})
                try:
                    writer.write_arrow(data)
                    maintainer = writer.row_key_extractor._index_maintainer
                    bucket_count = len(maintainer._states)
                    self.assertGreaterEqual(bucket_count, 2)
                    write_index = maintainer._write_index
                    calls = 0

                    def fail_index(*args):
                        nonlocal calls
                        calls += 1
                        if calls == failed_call:
                            raise OSError('HASH index prepare failed')
                        return write_index(*args)

                    with patch.object(maintainer, '_write_index', side_effect=fail_index):
                        with self.assertRaisesRegex(OSError, 'HASH index prepare failed'):
                            writer.prepare_commit(1)
                    self.assertIsNone(table.snapshot_manager().get_latest_snapshot())
                    messages = writer.prepare_commit(1)
                    self.assertEqual(3, sum(file.row_count for message in messages
                                            for file in message.new_files))
                    self.assertEqual(3, sum(file.row_count for message in messages
                                            for file in message.changelog_files))
                    self.assertEqual(bucket_count, sum(len(message.index_adds) for message in messages))
                    self.assertEqual([], writer.prepare_commit(2))
                    commit.commit(messages, 1)
                    writer.abort()
                    self.assertEqual(data.to_pydict(), self._read_arrow(table).sort_by('id').to_pydict())
                    self.assertEqual(bucket_count, len(self._hash_indexes(table)))
                finally:
                    writer.close()
                    commit.close()

    @pytest.mark.python_write
    def test_stream_writer_releases_prepared_hash_index_ownership(self):
        with tempfile.TemporaryDirectory() as root:
            table = self._create_table(root, 'stream_prepared')
            builder = table.new_stream_write_builder()
            writer = builder.new_write()
            commit = builder.new_commit()
            writer.write_arrow(pa.table({'id': [1], 'value': ['v-1']}))
            messages = writer.prepare_commit(1)
            index_path = messages[0].index_adds[0].index_file.external_path
            if index_path is None:
                index_path = (
                    table.path_factory().global_index_path_factory()
                    .to_path(messages[0].index_adds[0].index_file.file_name)
                )
            self.assertEqual(
                [], writer.row_key_extractor._index_maintainer._new_paths
            )

            commit.commit(messages, 1)
            writer.close()

            self.assertTrue(table.file_io.exists(index_path))
            self.assertEqual(
                {'id': [1], 'value': ['v-1']},
                self._read_arrow(table).to_pydict(),
            )
            commit.close()

    def test_regular_dynamic_extractor_skips_partition_hash(self):
        with tempfile.TemporaryDirectory() as root:
            catalog = CatalogFactory.create({'warehouse': root})
            catalog.create_database('default', True)
            schema = Schema.from_pyarrow_schema(
                pa.schema([
                    pa.field('id', pa.int64()),
                    pa.field('value', pa.string()),
                    pa.field('dt', pa.string()),
                ]),
                partition_keys=['dt'],
                primary_keys=['id', 'dt'],
                options={
                    'bucket': '-1',
                    'dynamic-bucket.target-row-num': '100',
                },
            )
            catalog.create_table('default.partitioned', schema, False)
            table = catalog.get_table('default.partitioned')
            extractor = DynamicBucketRowKeyExtractor(table.table_schema)
            hash_code = Mock(wraps=extractor._binary_row_hash_code)
            extractor._binary_row_hash_code = hash_code

            extractor.extract_partition_bucket_batch(
                pa.RecordBatch.from_pydict({
                    'id': [1, 2],
                    'value': ['a', 'b'],
                    'dt': ['p', 'p'],
                })
            )

            self.assertEqual(2, hash_code.call_count)
            self.assertNotIn(
                ('p',),
                [call.args[0] for call in hash_code.call_args_list],
            )

    def test_hash_add_replaces_previous_index_without_explicit_delete(self):
        # Java DynamicBucketIndexMaintainer sends only the complete new HASH
        # file. Its bucket-owner protocol does not require an old-file DELETE.
        with tempfile.TemporaryDirectory() as root:
            table = self._create_table(root, 'implicit_hash_replace')
            self._commit_arrow(table, [1], ['one'])
            writer, commit, messages = self._prepare_indexed_write(table, [2])
            self.assertEqual([], messages[0].index_deletes)
            commit.commit(messages)
            indexes = self._hash_indexes(table)
            self.assertEqual(1, len(indexes))
            self.assertEqual(2, indexes[0].index_file.row_count)
            writer.close()
            commit.close()

    def test_sequential_hash_index_replacements_keep_one_complete_index(self):
        with tempfile.TemporaryDirectory() as root:
            table = self._create_table(root, 'concurrent_replace')
            seed_writer, seed_commit, seed_messages = (
                self._prepare_indexed_write(table, [1])
            )
            seed_commit.commit(seed_messages)
            seed_writer.close()
            seed_commit.close()

            writer1, commit1, messages1 = self._prepare_indexed_write(
                table, [2]
            )
            commit1.commit(messages1)
            writer2, commit2, messages2 = self._prepare_indexed_write(
                table, [3]
            )
            self.assertEqual([], messages1[0].index_deletes)
            self.assertEqual([], messages2[0].index_deletes)
            commit2.commit(messages2)

            indexes = self._hash_indexes(table)
            self.assertEqual(1, len(indexes))
            self.assertEqual(3, indexes[0].index_file.row_count)
            writer1.close()
            writer2.close()
            commit1.close()
            commit2.close()

    def test_concurrent_disjoint_bucket_replacements_succeed(self):
        with tempfile.TemporaryDirectory() as root:
            table = self._create_table(
                root,
                'concurrent_disjoint',
                target_row_num=1,
                max_buckets=2,
            )
            seed_writer, seed_commit, seed_messages = (
                self._prepare_indexed_write(table, [1, 2])
            )
            seed_commit.commit(seed_messages)
            seed_writer.close()
            seed_commit.close()

            writer1, commit1, messages1 = self._prepare_indexed_write(
                table, [3], bucket=0
            )
            writer2, commit2, messages2 = self._prepare_indexed_write(
                table, [4], bucket=1
            )

            self.assertNotEqual(messages1[0].bucket, messages2[0].bucket)
            commit1.commit(messages1)
            commit2.commit(messages2)

            self.assertEqual(2, len(self._hash_indexes(table)))
            self.assertEqual(
                {'id': [1, 2, 3, 4],
                 'value': ['v-1', 'v-2', 'v-3', 'v-4']},
                self._read_arrow(table).sort_by('id').to_pydict(),
            )
            writer1.close()
            writer2.close()
            commit1.close()
            commit2.close()

    def test_data_only_upsert_succeeds_after_concurrent_index_change(self):
        with tempfile.TemporaryDirectory() as root:
            table = self._create_table(
                root, 'data_only_concurrent_append', target_row_num=1
            )
            self._commit_arrow(table, [1, 2], ['one', 'two'])

            stale_builder = table.new_batch_write_builder()
            stale_writer = stale_builder.new_write()
            stale_writer.write_arrow(
                pa.table({'id': [2], 'value': ['stale-upsert']})
            )
            stale_messages = stale_writer.prepare_commit()
            stale_commit = stale_builder.new_commit()
            self.assertFalse(any(
                message.index_adds or message.index_deletes
                for message in stale_messages
            ))

            concurrent_messages = self._commit_arrow(table, [3], ['three'])
            self.assertTrue(any(
                message.index_adds or message.index_deletes
                for message in concurrent_messages
            ))
            stale_commit.commit(stale_messages)

            self.assertEqual(
                {'id': [1, 2, 3],
                 'value': ['one', 'stale-upsert', 'three']},
                self._read_arrow(table).sort_by('id').to_pydict(),
            )
            stale_writer.close()
            stale_commit.close()

    def test_retry_after_disjoint_hash_index_commit_preserves_prepared_files(self):
        with tempfile.TemporaryDirectory() as root:
            table = self._create_table(root, 'retry_hash_commit', target_row_num=1)
            self._commit_arrow(table, [0], ['seed'])
            # The pending upsert owns bucket 0. A new key is assigned bucket 1,
            # so the competing commit obeys Java's single owner per bucket.
            writer, commit, messages = self._prepare_indexed_write(table, [0])
            prepared_paths = [
                file.file_path
                for message in messages
                for file in message.new_files
            ] + [
                entry.index_file.external_path
                or table.path_factory().global_index_path_factory().to_path(
                    entry.index_file.file_name
                )
                for message in messages
                for entry in message.index_adds
            ]
            original_snapshot_commit = commit.file_store_commit.snapshot_commit
            snapshot_commit = original_snapshot_commit.commit
            calls = 0

            def lose_first_compare_and_set(
                    base_snapshot_uuid, snapshot, statistics):
                nonlocal calls
                calls += 1
                if calls == 1:
                    concurrent_writer, concurrent_commit, concurrent_messages = (
                        self._prepare_indexed_write(table, [2])
                    )
                    concurrent_commit.commit(concurrent_messages)
                    concurrent_writer.close()
                    concurrent_commit.close()
                    return False
                return snapshot_commit(base_snapshot_uuid, snapshot, statistics)

            with patch.object(
                original_snapshot_commit,
                'commit',
                side_effect=lose_first_compare_and_set,
            ), patch.object(
                commit.file_store_commit,
                '_commit_retry_wait',
            ):
                commit.commit(messages)

            self.assertEqual(2, calls)
            self.assertTrue(all(
                table.file_io.exists(path) for path in prepared_paths
            ))
            self.assertEqual({'id': [0, 2], 'value': ['v-0', 'v-2']},
                             self._read_arrow(table).sort_by('id').to_pydict())
            writer.close()
            commit.close()

    def test_invalid_dynamic_bucket_key_reports_schema_error(self):
        with tempfile.TemporaryDirectory() as root:
            table = self._create_table(root, 'invalid_bucket_key')
            options = dict(table.table_schema.options)
            options['bucket-key'] = 'missing_column'
            invalid_schema = table.table_schema.copy(options)

            with self.assertRaisesRegex(
                ValueError, "Cannot define 'bucket-key' in dynamic bucket mode"
            ):
                DynamicBucketRowKeyExtractor(invalid_schema)


if __name__ == '__main__':
    unittest.main()
