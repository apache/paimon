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

    def _create_tables(self, source_rows=2):
        suffix = uuid.uuid4().hex[:8]
        target = "default.pk_target_{}".format(suffix)
        source = "default.pk_source_{}".format(suffix)
        self.catalog.create_table(
            target,
            Schema.from_pyarrow_schema(
                self.target_schema,
                primary_keys=["id"],
                options={
                    "bucket": "2",
                    "merge-engine": "partial-update",
                },
            ),
            False,
        )
        self.catalog.create_table(
            source, Schema.from_pyarrow_schema(self.source_schema), False)
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

    def _read(self, identifier):
        table = self.catalog.get_table(identifier)
        builder = table.new_read_builder()
        return builder.new_read().to_arrow(
            builder.new_scan().plan().splits()).sort_by("id")

    def _incremental_write(self, target, source, operation_id, interval=10):
        return write_paimon(
            PaimonOffsetSource(source),
            target,
            self.catalog_options,
            commit_mode="incremental",
            operation_id=operation_id,
            commit_interval_seconds=interval,
            update_cols=["feature"],
        )

    def test_resumes_after_timed_commit(self):
        target, source = self._create_tables()
        operation_id = "resume-{}".format(uuid.uuid4().hex)
        from pypaimon.write import ray_datasink

        real_prepare = ray_datasink._prepare_primary_key_groups
        calls = 0

        def fail_second_window(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("injected driver failure")
            return real_prepare(*args, **kwargs)

        with mock.patch(
                "pypaimon.ray.offset_source._ROWS_PER_UNIT", 1), mock.patch(
                "pypaimon.ray.incremental_write.time.monotonic",
                side_effect=[0, 11, 11]), mock.patch.object(
                ray_datasink, "_prepare_primary_key_groups",
                side_effect=fail_second_window):
            with self.assertRaisesRegex(
                    RuntimeError, "injected driver failure"):
                self._incremental_write(target, source, operation_id)

        partial = self._read(target).to_pydict()
        self.assertEqual([101, 20, 30], partial["feature"])

        with mock.patch(
                "pypaimon.ray.offset_source._ROWS_PER_UNIT", 1):
            result = self._incremental_write(target, source, operation_id)

        self.assertEqual({"num_written": 2}, result)
        self.assertEqual([101, 102, 30], self._read(target)["feature"].to_pylist())

    def test_time_trigger_batches_completed_windows(self):
        target, source = self._create_tables(source_rows=3)
        operation_id = "time-{}".format(uuid.uuid4().hex)
        before = self.catalog.get_table(
            target).snapshot_manager().get_latest_snapshot().id

        with mock.patch(
                "pypaimon.ray.offset_source._ROWS_PER_UNIT", 1), mock.patch(
                "pypaimon.ray.incremental_write.time.monotonic",
                side_effect=[0, 1, 11, 11]):
            self._incremental_write(target, source, operation_id)

        after = self.catalog.get_table(
            target).snapshot_manager().get_latest_snapshot().id
        self.assertEqual(3, after - before)

    def test_inserts_into_empty_target_and_cleans_checkpoint(self):
        target, source = self._create_tables()
        empty_target = "default.pk_empty_{}".format(uuid.uuid4().hex[:8])
        self.catalog.create_table(
            empty_target,
            Schema.from_pyarrow_schema(
                self.target_schema,
                primary_keys=["id"],
                options={
                    "bucket": "2",
                    "merge-engine": "partial-update",
                },
            ),
            False,
        )
        operation_id = "empty-{}".format(uuid.uuid4().hex)
        result = self._incremental_write(
            empty_target, source, operation_id)

        self.assertEqual({"num_written": 2}, result)
        self.assertEqual({
            "id": [1, 2],
            "payload": [None, None],
            "feature": [101, 102],
        }, self._read(empty_target).to_pydict())
        self.assertTrue(delete_write_paimon_checkpoint(
            empty_target, self.catalog_options, operation_id))
        self.assertFalse(delete_write_paimon_checkpoint(
            empty_target, self.catalog_options, operation_id))

    def test_metadata_checkpoint_for_empty_transform(self):
        target, source = self._create_tables()
        operation_id = "empty-transform-{}".format(uuid.uuid4().hex)
        source = PaimonOffsetSource(source, transform=lambda data: data.limit(0))

        result = write_paimon(
            source,
            target,
            self.catalog_options,
            commit_mode="incremental",
            operation_id=operation_id,
            commit_interval_seconds=10,
            update_cols=["feature"],
        )
        snapshot_id = self.catalog.get_table(
            target).snapshot_manager().get_latest_snapshot().id

        self.assertEqual({"num_written": 0}, result)
        self.assertEqual([10, 20, 30],
                         self._read(target)["feature"].to_pylist())
        self.assertEqual(
            {"num_written": 0},
            write_paimon(
                source,
                target,
                self.catalog_options,
                commit_mode="incremental",
                operation_id=operation_id,
                commit_interval_seconds=10,
                update_cols=["feature"],
            ),
        )
        self.assertEqual(
            snapshot_id,
            self.catalog.get_table(
                target).snapshot_manager().get_latest_snapshot().id,
        )

    def test_rejects_concurrent_target_write(self):
        target, source = self._create_tables()
        from pypaimon.write import ray_datasink

        real_prepare = ray_datasink._prepare_primary_key_groups

        def write_concurrently(*args, **kwargs):
            self._write(target, pa.table({
                "id": [4],
                "payload": ["external"],
                "feature": [40],
            }, schema=self.target_schema))
            return real_prepare(*args, **kwargs)

        with mock.patch.object(
                ray_datasink, "_prepare_primary_key_groups",
                side_effect=write_concurrently):
            with self.assertRaisesRegex(
                    RuntimeError, "Concurrent target commit"):
                self._incremental_write(
                    target, source,
                    "concurrent-{}".format(uuid.uuid4().hex))

        self.assertEqual([10, 20, 30, 40],
                         self._read(target)["feature"].to_pylist())

    def test_uses_schema_planned_after_existing_ddl(self):
        target, source = self._create_tables()
        self.catalog.alter_table(
            target,
            [SchemaChange.add_column("extra", AtomicType("STRING"))],
            False,
        )

        self._incremental_write(
            target, source, "existing-ddl-{}".format(uuid.uuid4().hex))

        self.assertEqual([101, 102, 30],
                         self._read(target)["feature"].to_pylist())

    def test_rejects_ddl_during_write(self):
        target, source = self._create_tables()
        from pypaimon.write import ray_datasink

        real_prepare = ray_datasink._prepare_primary_key_groups

        def alter_concurrently(*args, **kwargs):
            self.catalog.alter_table(
                target,
                [SchemaChange.add_column("extra", AtomicType("STRING"))],
                False,
            )
            return real_prepare(*args, **kwargs)

        with mock.patch.object(
                ray_datasink, "_prepare_primary_key_groups",
                side_effect=alter_concurrently):
            with self.assertRaisesRegex(RuntimeError, "schema change"):
                self._incremental_write(
                    target, source,
                    "concurrent-ddl-{}".format(uuid.uuid4().hex))

        self.assertEqual([10, 20, 30],
                         self._read(target)["feature"].to_pylist())

    def test_rejects_non_partial_update_target(self):
        suffix = uuid.uuid4().hex[:8]
        target = "default.pk_dedupe_{}".format(suffix)
        source = "default.pk_source_{}".format(suffix)
        self.catalog.create_table(
            target,
            Schema.from_pyarrow_schema(
                self.target_schema, primary_keys=["id"],
                options={"bucket": "1"}),
            False,
        )
        self.catalog.create_table(
            source, Schema.from_pyarrow_schema(self.source_schema), False)
        with self.assertRaisesRegex(ValueError, "partial-update"):
            self._incremental_write(
                target, source, "reject-{}".format(suffix))

    def test_rejects_unprovided_non_nullable_column(self):
        suffix = uuid.uuid4().hex[:8]
        target = "default.pk_not_null_{}".format(suffix)
        source = "default.pk_source_{}".format(suffix)
        schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("payload", pa.string(), nullable=False),
            ("feature", pa.int32()),
        ])
        self.catalog.create_table(
            target,
            Schema.from_pyarrow_schema(
                schema, primary_keys=["id"], options={
                    "bucket": "1",
                    "merge-engine": "partial-update",
                }),
            False,
        )
        self.catalog.create_table(
            source, Schema.from_pyarrow_schema(self.source_schema), False)
        with self.assertRaisesRegex(ValueError, "payload.*nullable"):
            self._incremental_write(
                target, source, "non-null-{}".format(suffix))


if __name__ == "__main__":
    unittest.main()
