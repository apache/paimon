# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import itertools
import os
import shutil
import tempfile
import unittest
import uuid
from unittest import mock

import pyarrow as pa
import pytest

ray = pytest.importorskip("ray")

from pypaimon import CatalogFactory, Schema
from pypaimon.ray import write_paimon
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import SchemaChange


class RayIncrementalWriteTest(unittest.TestCase):

    target_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        ("payload", pa.string()),
        ("feature", pa.int32()),
    ])
    source_schema = pa.schema([
        ("id", pa.int64()),
        ("feature", pa.int32()),
    ])

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.catalog_options = {
            "warehouse": os.path.join(cls.tempdir, "warehouse")}
        cls.catalog = CatalogFactory.create(cls.catalog_options)
        cls.catalog.create_database("default", True)
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, num_cpus=2)

    @classmethod
    def tearDownClass(cls):
        if ray.is_initialized():
            ray.shutdown()
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create_table(self, prefix, schema, **schema_options):
        identifier = "default.{}_{}".format(prefix, uuid.uuid4().hex[:8])
        self.catalog.create_table(
            identifier,
            Schema.from_pyarrow_schema(schema, **schema_options),
            False,
        )
        return identifier

    def _create_target(self):
        target = self._create_table(
            "pk_target",
            self.target_schema,
            primary_keys=["id"],
            options={
                "bucket": "2",
                "merge-engine": "partial-update",
            },
        )
        self._write(target, pa.table({
            "id": [1, 2, 3],
            "payload": ["a", "b", "c"],
            "feature": [10, 20, 30],
        }, schema=self.target_schema))
        return target

    def _write(self, identifier, data):
        table = self.catalog.get_table(identifier)
        builder = table.new_batch_write_builder()
        writer = builder.new_write()
        commit = builder.new_commit()
        try:
            writer.write_arrow(data)
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()

    def _compact(self, identifier):
        data = self._read(identifier)
        builder = self.catalog.get_table(
            identifier).new_batch_write_builder().overwrite({})
        writer = builder.new_write()
        commit = builder.new_commit()
        try:
            writer.write_arrow(data)
            real_commit = commit.file_store_commit._try_commit

            def compact_commit(*args, **kwargs):
                kwargs["commit_kind"] = "COMPACT"
                return real_commit(*args, **kwargs)

            commit.file_store_commit._try_commit = compact_commit
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()

    def _read(self, identifier, sort_by="id"):
        table = self.catalog.get_table(identifier)
        builder = table.new_read_builder()
        return builder.new_read().to_arrow(
            builder.new_scan().plan().splits()).sort_by(sort_by)

    def _write_incrementally(self, target, source, interval=10):
        if isinstance(source, pa.Table):
            source = ray.data.from_arrow(source)
        return write_paimon(
            source,
            target,
            self.catalog_options,
            commit_mode="incremental",
            commit_interval_seconds=interval,
            update_cols=["feature"],
        )

    @staticmethod
    def _clock(*values):
        return mock.patch(
            "pypaimon.ray.incremental_write.time.monotonic",
            side_effect=values or itertools.count(step=10))

    def _snapshot_id(self, target):
        return self.catalog.get_table(
            target).snapshot_manager().get_latest_snapshot().id

    def _assert_features(self, target, expected):
        self.assertEqual(expected, self._read(target)["feature"].to_pylist())

    def _prepare_update(self, target, ids, features):
        table = self.catalog.get_table(target)
        writer = table.new_batch_write_builder().new_write()
        try:
            writer.write_arrow(pa.table({
                "id": ids,
                "payload": [None] * len(ids),
                "feature": features,
            }, schema=self.target_schema))
            return writer.prepare_commit()
        finally:
            writer.close()

    def test_commits_periodically(self):
        target = self._create_target()
        updates = pa.table({
            "id": list(range(1, 21)),
            "feature": list(range(101, 121)),
        }, schema=self.source_schema)
        updates = ray.data.from_arrow([
            updates.slice(0, 10), updates.slice(10, 10)])
        before = self._snapshot_id(target)

        with self._clock():
            self._write_incrementally(target, updates, interval=1)

        self.assertEqual(2, self._snapshot_id(target) - before)
        self._assert_features(target, list(range(101, 121)))
        self.assertEqual(["a", "b", "c"] + [None] * 17,
                         self._read(target)["payload"].to_pylist())

    def test_failure_keeps_completed_groups(self):
        target = self._create_target()
        updates = pa.table({
            "id": [1, 2],
            "feature": [101, 102],
        }, schema=self.source_schema)
        first_group = self._prepare_update(target, [1], [101])

        def fail_after_first_group(*_args, **kwargs):
            kwargs["on_group_result"](first_group)
            raise RuntimeError("injected worker failure")

        with self._clock(), mock.patch(
                "pypaimon.write.ray_datasink._write_primary_key_groups",
                side_effect=fail_after_first_group):
            with self.assertRaisesRegex(
                    RuntimeError, "injected worker failure"):
                self._write_incrementally(target, updates, interval=1)

        self._assert_features(target, [101, 20, 30])

    def test_allows_compaction_between_commits(self):
        target = self._create_target()
        updates = pa.table({
            "id": [1, 3],
            "feature": [101, 103],
        }, schema=self.source_schema)
        updates = ray.data.from_arrow([
            updates.slice(0, 1), updates.slice(1, 1)])
        groups = [
            self._prepare_update(target, [1], [101]),
            self._prepare_update(target, [3], [103]),
        ]
        calls = 0

        def compact_between_groups(*_args, **kwargs):
            nonlocal calls
            if calls == 1:
                self._compact(target)
            kwargs["on_group_result"](groups[calls])
            calls += 1

        with self._clock(), mock.patch(
                "pypaimon.write.ray_datasink._write_primary_key_groups",
                side_effect=compact_between_groups):
            self._write_incrementally(target, updates, interval=1)

        self._assert_features(target, [101, 20, 103])

    def test_partitioned_composite_key(self):
        schema = pa.schema([
            pa.field("key", pa.string(), nullable=False),
            pa.field("partition", pa.string(), nullable=False),
            ("payload", pa.string()),
            ("feature", pa.int32()),
        ])
        target = self._create_table(
            "partitioned",
            schema,
            partition_keys=["partition"],
            primary_keys=["key", "partition"],
            options={
                "bucket": "4096",
                "merge-engine": "partial-update",
            },
        )
        self._write(target, pa.table({
            "key": ["a", "b"],
            "partition": ["p1", "p1"],
            "payload": ["keep-a", "keep-b"],
            "feature": [1, 2],
        }, schema=schema))

        write_paimon(
            ray.data.from_arrow(pa.table({
                "key": ["a", "b"],
                "partition": ["p1", "p1"],
                "feature": [101, 102],
            })),
            target,
            self.catalog_options,
            commit_mode="incremental",
            commit_interval_seconds=10,
            update_cols=["feature"],
        )

        self.assertEqual(["keep-a", "keep-b"],
                         self._read(target, "key")["payload"].to_pylist())
        self.assertEqual([101, 102],
                         self._read(target, "key")["feature"].to_pylist())

    def test_validates_incremental_mode(self):
        target = self._create_target()
        updates = ray.data.from_arrow(pa.table({
            "id": [1], "feature": [101],
        }, schema=self.source_schema))

        cases = [
            ({"commit_mode": "unknown"}, "commit_mode"),
            ({"commit_mode": "incremental"}, "commit_interval_seconds"),
            ({"commit_mode": "incremental",
              "commit_interval_seconds": 0,
              "update_cols": ["feature"]}, "must be positive"),
            ({"commit_mode": "incremental",
              "commit_interval_seconds": 10,
              "update_cols": ["feature"],
              "overwrite": True}, "cannot overwrite"),
        ]
        for options, error in cases:
            with self.subTest(options=options):
                with self.assertRaisesRegex(ValueError, error):
                    write_paimon(
                        updates, target, self.catalog_options, **options)

    def test_requires_partial_update_target(self):
        target = self._create_table(
            "deduplicate_target",
            self.target_schema,
            primary_keys=["id"],
            options={"bucket": "2"},
        )
        with self.assertRaisesRegex(ValueError, "partial-update"):
            self._write_incrementally(target, pa.table({
                "id": [1], "feature": [101],
            }, schema=self.source_schema))

    def test_merges_concurrent_partial_updates(self):
        from pypaimon.write import ray_datasink

        schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            ("payload", pa.string()),
            ("feature_a", pa.int32()),
            ("feature_b", pa.int32()),
        ])
        target = self._create_table(
            "concurrent_target",
            schema,
            primary_keys=["id"],
            options={"bucket": "2", "merge-engine": "partial-update"},
        )
        self._write(target, pa.table({
            "id": [1],
            "payload": ["keep"],
            "feature_a": [1],
            "feature_b": [2],
        }, schema=schema))
        real_write = ray_datasink._write_primary_key_groups

        def concurrent_update(*args, **kwargs):
            self._write(target, pa.table({
                "id": [1],
                "payload": [None],
                "feature_a": [None],
                "feature_b": [202],
            }, schema=schema))
            return real_write(*args, **kwargs)

        with mock.patch.object(
                ray_datasink,
                "_write_primary_key_groups",
                side_effect=concurrent_update):
            write_paimon(
                ray.data.from_arrow(pa.table({
                    "id": [1], "feature_a": [101],
                })),
                target,
                self.catalog_options,
                commit_mode="incremental",
                commit_interval_seconds=10,
                update_cols=["feature_a"],
            )

        self.assertEqual({
            "id": [1],
            "payload": ["keep"],
            "feature_a": [101],
            "feature_b": [202],
        }, self._read(target).to_pydict())

    def test_rejects_schema_change(self):
        from pypaimon.write import ray_datasink

        target = self._create_target()
        real_write = ray_datasink._write_primary_key_groups

        def alter_schema(*args, **kwargs):
            self.catalog.alter_table(target, [
                SchemaChange.add_column(
                    "extra", AtomicType("STRING"))], False)
            return real_write(*args, **kwargs)

        with mock.patch.object(
                ray_datasink,
                "_write_primary_key_groups",
                side_effect=alter_schema):
            with self.assertRaisesRegex(RuntimeError, "schema changed"):
                self._write_incrementally(target, pa.table({
                    "id": [1], "feature": [101],
                }, schema=self.source_schema))

        self._assert_features(target, [10, 20, 30])


if __name__ == "__main__":
    unittest.main()
