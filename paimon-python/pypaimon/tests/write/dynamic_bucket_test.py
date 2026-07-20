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

from pypaimon import CatalogFactory, Schema
from pypaimon.index.dynamic_bucket import (
    HashBucketAssigner,
    _PartitionIndex,
    _iter_hashes,
    compute_assigner,
    to_signed_int32,
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
    def _prepare_indexed_write(table, ids):
        builder = table.new_batch_write_builder()
        writer = builder.new_write().with_dynamic_bucket_index()
        writer.write_arrow_batch(pa.RecordBatch.from_pydict({
            'id': ids,
            'value': [f'v-{value}' for value in ids],
        }))
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
            bucket_filter=lambda candidate: candidate == 32766,
            max_buckets_num=-1,
            max_bucket_id=32765,
        )

        self.assertEqual(bucket, 32766)

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
                max_bucket_id=32766,
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

    def test_batch_writer_abort_after_prepare_deletes_hash_index(self):
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

            self.assertFalse(table.file_io.exists(index_path))

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

    def test_concurrent_initial_hash_index_add_conflicts(self):
        with tempfile.TemporaryDirectory() as root:
            table = self._create_table(root, 'concurrent_initial')
            writer1, commit1, messages1 = self._prepare_indexed_write(
                table, [1]
            )
            writer2, commit2, messages2 = self._prepare_indexed_write(
                table, [2]
            )

            commit1.commit(messages1)
            stale_data_paths = [
                file.file_path for message in messages2 for file in message.new_files
            ]
            stale_index_paths = [
                entry.index_file.external_path
                or table.path_factory().global_index_path_factory().to_path(
                    entry.index_file.file_name
                )
                for message in messages2 for entry in message.index_adds
            ]
            with self.assertRaisesRegex(
                RuntimeError, 'HASH index assignment conflict'
            ):
                commit2.commit(messages2)

            self.assertTrue(all(
                not table.file_io.exists(path)
                for path in stale_data_paths + stale_index_paths
            ))

            indexes = self._hash_indexes(table)
            self.assertEqual(1, len(indexes))
            self.assertEqual(1, indexes[0].index_file.row_count)
            writer1.close()
            writer2.close()
            commit1.close()
            commit2.close()

    def test_concurrent_hash_index_replacement_conflicts(self):
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
            writer2, commit2, messages2 = self._prepare_indexed_write(
                table, [3]
            )
            old_index = messages1[0].index_deletes[0].index_file.file_name
            self.assertEqual(
                old_index,
                messages2[0].index_deletes[0].index_file.file_name,
            )

            commit1.commit(messages1)
            with self.assertRaisesRegex(
                RuntimeError, 'HASH index assignment conflict'
            ):
                commit2.commit(messages2)

            indexes = self._hash_indexes(table)
            self.assertEqual(1, len(indexes))
            self.assertEqual(2, indexes[0].index_file.row_count)
            writer1.close()
            writer2.close()
            commit1.close()
            commit2.close()

    def test_concurrent_disjoint_bucket_replacements_conflict(self):
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

            with patch(
                'pypaimon.index.dynamic_bucket.random.choice',
                return_value=0,
            ):
                writer1, commit1, messages1 = self._prepare_indexed_write(
                    table, [3]
                )
            with patch(
                'pypaimon.index.dynamic_bucket.random.choice',
                return_value=1,
            ):
                writer2, commit2, messages2 = self._prepare_indexed_write(
                    table, [3]
                )

            self.assertNotEqual(messages1[0].bucket, messages2[0].bucket)
            commit1.commit(messages1)
            stale_paths = [
                file.file_path
                for message in messages2
                for file in message.new_files
            ] + [
                entry.index_file.external_path
                or table.path_factory().global_index_path_factory().to_path(
                    entry.index_file.file_name
                )
                for message in messages2
                for entry in message.index_adds
            ]

            with self.assertRaisesRegex(
                RuntimeError, 'assigned from snapshot.*latest snapshot'
            ):
                commit2.commit(messages2)

            self.assertTrue(all(
                not table.file_io.exists(path) for path in stale_paths
            ))
            writer1.close()
            writer2.close()
            commit1.close()
            commit2.close()

    def test_data_only_upsert_conflicts_after_overwrite_remaps_key(self):
        with tempfile.TemporaryDirectory() as root:
            table = self._create_table(
                root, 'data_only_overwrite_conflict', target_row_num=1
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

            overwrite_builder = table.new_batch_write_builder().overwrite({})
            overwrite_writer = overwrite_builder.new_write()
            overwrite_writer.write_arrow(
                pa.table({'id': [2], 'value': ['overwrite']})
            )
            overwrite_messages = overwrite_writer.prepare_commit()
            overwrite_commit = overwrite_builder.new_commit()
            overwrite_commit.commit(overwrite_messages)
            overwrite_writer.close()
            overwrite_commit.close()

            with self.assertRaisesRegex(
                RuntimeError, 'HASH index assignment conflict'
            ):
                stale_commit.commit(stale_messages)

            self.assertEqual(
                {'id': [2], 'value': ['overwrite']},
                self._read_arrow(table).to_pydict(),
            )
            stale_writer.close()
            stale_commit.close()

    def test_retry_then_hash_index_conflict_preserves_prepared_files(self):
        with tempfile.TemporaryDirectory() as root:
            table = self._create_table(root, 'retry_hash_conflict')
            writer, commit, messages = self._prepare_indexed_write(table, [1])
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
            calls = 0

            def lose_first_compare_and_set(snapshot, statistics):
                nonlocal calls
                calls += 1
                concurrent_writer, concurrent_commit, concurrent_messages = (
                    self._prepare_indexed_write(table, [2])
                )
                concurrent_commit.commit(concurrent_messages)
                concurrent_writer.close()
                concurrent_commit.close()
                return False

            with patch.object(
                original_snapshot_commit,
                'commit',
                side_effect=lose_first_compare_and_set,
            ), patch.object(
                commit.file_store_commit,
                '_commit_retry_wait',
            ):
                with self.assertRaisesRegex(
                    RuntimeError, 'HASH index assignment conflict'
                ):
                    commit.commit(messages)

            self.assertEqual(1, calls)
            self.assertTrue(all(
                table.file_io.exists(path) for path in prepared_paths
            ))
            writer.close()
            commit.close()

    def test_invalid_dynamic_bucket_key_reports_schema_error(self):
        with tempfile.TemporaryDirectory() as root:
            table = self._create_table(root, 'invalid_bucket_key')
            options = dict(table.table_schema.options)
            options['bucket-key'] = 'missing_column'
            invalid_schema = table.table_schema.copy(options)

            with self.assertRaisesRegex(
                ValueError, 'bucket-key references unknown columns'
            ):
                DynamicBucketRowKeyExtractor(invalid_schema)


if __name__ == '__main__':
    unittest.main()
