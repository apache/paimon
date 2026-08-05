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
import json
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
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.ray import (
    PaimonOffsetSource,
    delete_write_paimon_checkpoint,
    write_paimon,
)
from pypaimon.ray.offset_source import _stable_units


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
        identifier = "default.{}_{}".format(
            prefix, uuid.uuid4().hex[:8])
        self.catalog.create_table(
            identifier,
            Schema.from_pyarrow_schema(schema, **schema_options),
            False,
        )
        return identifier

    def _create_tables(self, source_rows=2):
        target = self._create_table(
            "pk_target",
            self.target_schema,
            primary_keys=["id"],
            options={
                "bucket": "2",
                "merge-engine": "partial-update",
            },
        )
        source = self._create_table(
            "pk_source", self.source_schema, partition_keys=["id"])
        self._write(target, pa.table({
            "id": [1, 2, 3],
            "payload": ["a", "b", "c"],
            "feature": [10, 20, 30],
        }, schema=self.target_schema))
        ids = list(range(1, source_rows + 1))
        self._write(source, pa.table({
            "id": ids,
            "feature": [100 + value for value in ids],
        }, schema=self.source_schema))
        return target, source

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
            file_commit = commit.file_store_commit
            real_commit = file_commit._try_commit

            def compact_commit(*args, **kwargs):
                kwargs["commit_kind"] = "COMPACT"
                return real_commit(*args, **kwargs)

            file_commit._try_commit = compact_commit
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()

    def _read(self, identifier, sort_by="id"):
        table = self.catalog.get_table(identifier)
        builder = table.new_read_builder()
        return builder.new_read().to_arrow(
            builder.new_scan().plan().splits()).sort_by(sort_by)

    def _write_incrementally(
            self, target, source, operation_id=None, interval=10):
        if isinstance(source, str):
            source = PaimonOffsetSource(source)
        elif isinstance(source, pa.Table):
            source = ray.data.from_arrow(source)
        return write_paimon(
            source,
            target,
            self.catalog_options,
            commit_mode="incremental",
            operation_id=operation_id,
            commit_interval_seconds=interval,
            update_cols=["feature"],
        )

    @staticmethod
    def _clock(*values):
        side_effect = values or itertools.count(step=10)
        return mock.patch(
            "pypaimon.ray.incremental_write.time.monotonic",
            side_effect=side_effect)

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

    def test_ray_dataset_commits_periodically(self):
        target, _ = self._create_tables()
        updates = pa.table({
            "id": list(range(1, 21)),
            "feature": list(range(101, 121)),
        }, schema=self.source_schema)
        updates = ray.data.from_arrow([
            updates.slice(0, 10), updates.slice(10, 10)])
        before = self._snapshot_id(target)

        with self._clock():
            self._write_incrementally(target, updates, interval=1)

        after = self._snapshot_id(target)
        self.assertEqual(2, after - before)
        self._assert_features(target, list(range(101, 121)))

    def test_ray_dataset_retry_recomputes_source(self):
        target, _ = self._create_tables()
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

        self._write_incrementally(target, updates)
        self._assert_features(target, [101, 102, 30])

    def test_ray_dataset_allows_compaction_between_commits(self):
        target, _ = self._create_tables()
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

    def test_ray_dataset_rejects_operation_id(self):
        target, _ = self._create_tables()
        updates = pa.table({
            "id": [1], "feature": [101],
        }, schema=self.source_schema)

        with self.assertRaisesRegex(
                ValueError, "does not expose resumable source offsets"):
            self._write_incrementally(
                target, updates, operation_id="not-resumable")

    def test_partitioned_composite_key_dataset(self):
        schema = pa.schema([
            pa.field("key", pa.string(), nullable=False),
            pa.field("partition", pa.string(), nullable=False),
            ("payload", pa.string()),
            ("feature", pa.int32()),
        ])
        identifier = self._create_table(
            "partitioned",
            schema,
            partition_keys=["partition"],
            primary_keys=["key", "partition"],
            options={
                "bucket": "4096",
                "merge-engine": "partial-update",
            },
        )
        self._write(identifier, pa.table({
            "key": ["a", "b"],
            "partition": ["p1", "p1"],
            "payload": ["keep-a", "keep-b"],
            "feature": [1, 2],
        }, schema=schema))
        updates = pa.table({
            "key": ["a", "b"],
            "partition": ["p1", "p1"],
            "feature": [101, 102],
        })

        write_paimon(
            ray.data.from_arrow(updates),
            identifier,
            self.catalog_options,
            commit_mode="incremental",
            commit_interval_seconds=10,
            update_cols=["feature"],
        )

        self.assertEqual({
            "key": ["a", "b"],
            "partition": ["p1", "p1"],
            "payload": ["keep-a", "keep-b"],
            "feature": [101, 102],
        }, self._read(identifier, "key").to_pydict())

    def test_transform_identity_ignores_runtime_state(self):
        class RuntimeState:

            def __init__(self, token):
                self.token = token

        def source(token):
            state = RuntimeState(token)

            def transform(dataset):
                if state.token:
                    return dataset
                return dataset

            return PaimonOffsetSource(
                "default.source", transform=transform)

        self.assertEqual(
            source("expired")._transform_identity(),
            source("refreshed")._transform_identity(),
        )

        def changed_transform(dataset):
            return dataset.limit(1)

        self.assertNotEqual(
            source("expired")._transform_identity(),
            PaimonOffsetSource(
                "default.source",
                transform=changed_transform)._transform_identity(),
        )

    def test_offset_units_have_stable_order(self):
        first, first_ids = _stable_units([("p2", 2), ("p1", 1)])
        second, second_ids = _stable_units([("p1", 1), ("p2", 2)])
        self.assertEqual(first, second)
        self.assertEqual(first_ids, second_ids)

    def test_resume_allows_compaction(self):
        target, source = self._create_tables()
        operation_id = "resume-{}".format(uuid.uuid4().hex)
        from pypaimon.write import ray_datasink

        real_prepare = ray_datasink._write_primary_key_groups
        calls = 0

        def fail_second_window(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("injected driver failure")
            return real_prepare(*args, **kwargs)

        with mock.patch(
                "pypaimon.ray.incremental_write._SPLITS_PER_WINDOW", 1), \
                self._clock(0, 11, 11), mock.patch.object(
                ray_datasink, "_write_primary_key_groups",
                side_effect=fail_second_window):
            with self.assertRaisesRegex(
                    RuntimeError, "injected driver failure"):
                self._write_incrementally(target, source, operation_id)

        partial = self._read(target).to_pydict()
        self.assertIn(partial["feature"], ([101, 20, 30], [10, 102, 30]))

        self._compact(target)
        self._write_incrementally(target, source, operation_id)
        self._assert_features(target, [101, 102, 30])

    def test_time_trigger_batches_completed_windows(self):
        target, source = self._create_tables(source_rows=4)
        operation_id = "time-{}".format(uuid.uuid4().hex)
        from pypaimon.write import ray_datasink

        before = self._snapshot_id(target)
        real_prepare = ray_datasink._write_primary_key_groups
        calls = 0

        def count_windows(*args, **kwargs):
            nonlocal calls
            calls += 1
            return real_prepare(*args, **kwargs)

        with mock.patch(
                "pypaimon.ray.incremental_write._SPLITS_PER_WINDOW", 2), \
                self._clock(0, 11, 11), mock.patch.object(
                ray_datasink, "_write_primary_key_groups",
                side_effect=count_windows):
            self._write_incrementally(target, source, operation_id)

        after = self._snapshot_id(target)
        self.assertEqual(2, calls)
        self.assertEqual(3, after - before)
        self.assertEqual({
            "id": [1, 2, 3, 4],
            "payload": ["a", "b", "c", None],
            "feature": [101, 102, 103, 104],
        }, self._read(target).to_pydict())
        self.assertTrue(delete_write_paimon_checkpoint(
            target, self.catalog_options, operation_id))
        self.assertFalse(delete_write_paimon_checkpoint(
            target, self.catalog_options, operation_id))

    def test_metadata_checkpoint_for_empty_transform(self):
        target, source = self._create_tables()
        self.catalog.alter_table(target, [
            SchemaChange.add_column(
                "extra", AtomicType("STRING"))], False)
        operation_id = "empty-transform-{}".format(uuid.uuid4().hex)
        source = PaimonOffsetSource(source, transform=lambda data: data.limit(0))

        self._write_incrementally(target, source, operation_id)
        snapshot_id = self._snapshot_id(target)

        self._assert_features(target, [10, 20, 30])
        self._write_incrementally(target, source, operation_id)
        self.assertEqual(snapshot_id, self._snapshot_id(target))
        self.catalog.alter_table(target, [
            SchemaChange.add_column(
                "later", AtomicType("STRING"))], False)
        self.assertTrue(delete_write_paimon_checkpoint(
            target, self.catalog_options, operation_id))

    def test_rejects_late_schema_change_and_mismatched_checkpoint(self):
        from pypaimon.ray import incremental_write

        target, source = self._create_tables(source_rows=1)
        table = self.catalog.get_table(target)
        base = table.snapshot_manager().get_latest_snapshot()
        operation_id = "fence-{}".format(uuid.uuid4().hex)
        plan = {
            "table": source,
            "snapshot_id": 1,
            "fingerprint": "expected",
            "num_units": 1,
            "splits_per_window": 1,
        }
        state = incremental_write._checkpoint_state(
            operation_id, table.table_schema.id, ["feature"], plan, 0)

        def alter_schema(*_args, **_kwargs):
            self.catalog.alter_table(target, [
                SchemaChange.drop_column("feature")], False)

        with mock.patch.object(
                incremental_write, "_store_checkpoint_tag",
                side_effect=alter_schema):
            with self.assertRaisesRegex(RuntimeError, "schema change"):
                incremental_write._commit_checkpoint(
                    self.catalog, target, table, base,
                    table.table_schema.id,
                    incremental_write._commit_user(operation_id),
                    incremental_write._checkpoint_tags(operation_id),
                    1, [], state)

        other_state = dict(state)
        other_state["source"] = dict(plan, fingerprint="other")
        committed = mock.Mock(
            properties={incremental_write._CHECKPOINT_PROPERTY: json.dumps(
                other_state)},
            id=base.id + 2,
        )
        commit = mock.Mock()
        with mock.patch(
                "pypaimon.write.table_commit.StreamTableCommit",
                return_value=commit), mock.patch.object(
                incremental_write, "_find_checkpoint_snapshot",
                return_value=committed), mock.patch.object(
                incremental_write, "_store_checkpoint_tag"):
            with self.assertRaisesRegex(RuntimeError, "different state"):
                incremental_write._commit_checkpoint(
                    self.catalog, target, table, base,
                    table.table_schema.id,
                    incremental_write._commit_user(operation_id),
                    incremental_write._checkpoint_tags(operation_id),
                    2, [], state)

    def test_rejects_concurrent_target_changes(self):
        from pypaimon.write import ray_datasink

        real_prepare = ray_datasink._write_primary_key_groups
        for change, error in (("data", "Concurrent target commit"),
                              ("schema", "schema change")):
            with self.subTest(change=change):
                target, source = self._create_tables()
                changed = False

                def mutate_target(*args, **kwargs):
                    nonlocal changed
                    if changed:
                        return real_prepare(*args, **kwargs)
                    changed = True
                    if change == "data":
                        self._write(target, pa.table({
                            "id": [4], "payload": ["external"],
                            "feature": [40]}, schema=self.target_schema))
                    else:
                        self.catalog.alter_table(target, [
                            SchemaChange.add_column(
                                "extra", AtomicType("STRING"))], False)
                    return real_prepare(*args, **kwargs)

                with mock.patch.object(
                        ray_datasink, "_write_primary_key_groups",
                        side_effect=mutate_target):
                    with self.assertRaisesRegex(RuntimeError, error):
                        self._write_incrementally(
                            target, source,
                            "concurrent-{}".format(uuid.uuid4().hex))

                expected = ([10, 20, 30, 40] if change == "data"
                            else [10, 20, 30])
                self._assert_features(target, expected)

if __name__ == "__main__":
    unittest.main()
