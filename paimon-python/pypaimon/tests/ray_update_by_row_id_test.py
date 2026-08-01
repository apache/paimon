#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import os
import shutil
import tempfile
import types
import unittest
import uuid
from unittest import mock

import pyarrow as pa
import pytest

pypaimon = pytest.importorskip("pypaimon")
ray = pytest.importorskip("ray")

from pypaimon import CatalogFactory, Schema
from pypaimon.ray import (
    PaimonCoBucketedJoinOffsetSource,
    PaimonOffsetSource,
    delete_update_by_row_id_checkpoint,
    map_with_blobs,
    read_by_row_id,
    update_by_row_id,
)


class RayUpdateByRowIdTest(unittest.TestCase):
    """Distributed row-id update: rewrite only the files owning the given row ids,
    without reading or joining the whole target (unlike merge_into(on=_ROW_ID))."""

    pa_schema = pa.schema([
        ("id", pa.int32()),
        ("name", pa.string()),
        ("age", pa.int32()),
    ])
    de_options = {"row-tracking.enabled": "true", "data-evolution.enabled": "true"}

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.catalog_options = {"warehouse": os.path.join(cls.tempdir, "wh")}
        cls.catalog = CatalogFactory.create(cls.catalog_options)
        cls.catalog.create_database("default", True)
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, num_cpus=2)

    @classmethod
    def tearDownClass(cls):
        try:
            if ray.is_initialized():
                ray.shutdown()
        except Exception:
            pass
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create(self, options=None, partition_keys=None, schema=None):
        name = f"default.u_{uuid.uuid4().hex[:8]}"
        opts = self.de_options if options is None else options
        self.catalog.create_table(
            name,
            Schema.from_pyarrow_schema(
                schema or self.pa_schema,
                partition_keys=partition_keys,
                options=opts,
            ),
            False,
        )
        return name

    def _write(self, target, data):
        t = self.catalog.get_table(target)
        wb = t.new_batch_write_builder()
        w = wb.new_write()
        w.write_arrow(data)
        wb.new_commit().commit(w.prepare_commit())
        w.close()

    def _create_bucketed_table(
            self, schema, data, bucket_count=4, bucket_key="lookup_key"):
        name = f"default.b_{uuid.uuid4().hex[:8]}"
        self.catalog.create_table(
            name,
            Schema.from_pyarrow_schema(
                schema,
                options={
                    "bucket": str(bucket_count),
                    "bucket-key": bucket_key,
                },
            ),
            False,
        )
        self._write(name, data)
        return name

    def _overwrite(self, target, partition, data):
        t = self.catalog.get_table(target)
        wb = t.new_batch_write_builder().overwrite(partition)
        w = wb.new_write()
        w.write_arrow(data)
        wb.new_commit().commit(w.prepare_commit())
        w.close()

    def _read(self, target, projection=None):
        t = self.catalog.get_table(target)
        rb = t.new_read_builder()
        if projection is not None:
            rb = rb.with_projection(projection)
        return rb.new_read().to_arrow(rb.new_scan().plan().splits())

    def _rowid_by_id(self, target):
        tab = self._read(target, ["_ROW_ID", "id"])
        return dict(zip(tab.column("id").to_pylist(), tab.column("_ROW_ID").to_pylist()))

    @staticmethod
    def _offset_age_source(target):
        def transform(dataset):
            def to_updates(batch):
                return pa.table(
                    {
                        "_ROW_ID": batch["_ROW_ID"],
                        "age": pa.compute.add(batch["id"], 100),
                    },
                    schema=pa.schema([
                        ("_ROW_ID", pa.int64()),
                        ("age", pa.int32()),
                    ]),
                )

            return dataset.map_batches(
                to_updates, batch_format="pyarrow")

        return PaimonOffsetSource(
            target,
            projection=["_ROW_ID", "id"],
            transform=transform,
            rows_per_unit=1,
            units_per_checkpoint=1,
        )

    def test_offset_source_fingerprints_unserializable_closure(self):
        import threading

        lock = threading.Lock()

        def transform(dataset):
            lock.locked()
            return dataset

        source = PaimonOffsetSource(
            "default.source", transform=transform)
        self.assertEqual(
            source._transform_identity(),
            source._transform_identity(),
        )

    @staticmethod
    def _data_files_under(table):
        table_path = table.file_io.to_filesystem_path(table.table_path)
        return {
            os.path.join(root, file_name)
            for root, _, files in os.walk(table_path)
            for file_name in files
            if file_name.endswith(".parquet")
        }

    def test_update_by_row_id_basic(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": list(range(1, 7)), "name": [f"n{i}" for i in range(1, 7)],
             "age": [i * 10 for i in range(1, 7)]}, schema=self.pa_schema))
        rid = self._rowid_by_id(target)

        # update age for ids 2 and 5 only, addressed by their _ROW_ID
        src = pa.table({"_ROW_ID": [rid[2], rid[5]], "age": [999, 888]},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]))

        # Proof of no full-target read: read_paimon is never called (source is a
        # Dataset, and the update routes by manifest metadata, not a scan).
        import pypaimon.ray.ray_paimon as rp
        with mock.patch.object(rp, "read_paimon",
                               side_effect=AssertionError("target was read!")):
            stats = update_by_row_id(target, ray.data.from_arrow(src),
                                     self.catalog_options, update_cols=["age"])
        self.assertEqual(stats, {"num_updated": 2})

        back = self._read(target).sort_by("id").to_pydict()
        self.assertEqual(back["age"], [10, 999, 30, 40, 888, 60])
        self.assertEqual(back["name"], [f"n{i}" for i in range(1, 7)])  # untouched

    def test_offset_source_resumes_from_next_unit(self):
        import importlib

        module = importlib.import_module(
            "pypaimon.ray.update_by_row_id")
        offset_module = importlib.import_module(
            "pypaimon.ray.offset_source")

        target = self._create()
        for value in range(4):
            self._write(target, pa.Table.from_pydict(
                {
                    "id": [value],
                    "name": ["x"],
                    "age": [0],
                },
                schema=self.pa_schema,
            ))

        operation_id = "offset-resume-" + uuid.uuid4().hex
        original_read = offset_module._BoundPaimonOffsetSource.read_window
        first_reads = []

        def fail_second_window(bound, start, end):
            first_reads.append(start)
            if start == 1:
                raise RuntimeError("stop after first source window")
            return original_read(bound, start, end)

        with mock.patch.object(
                offset_module._BoundPaimonOffsetSource,
                "read_window",
                fail_second_window):
            with self.assertRaisesRegex(
                    RuntimeError, "stop after first source window"):
                update_by_row_id(
                    target,
                    self._offset_age_source(target),
                    self.catalog_options,
                    update_cols=["age"],
                    num_partitions=1,
                    commit_mode="incremental",
                    operation_id=operation_id,
                )

        self.assertEqual([0, 1], first_reads)
        checkpoint_tags = module._operation_checkpoint_tags(operation_id)
        tagged = [
            module._get_checkpoint_tag(
                self.catalog, target, checkpoint_tag)
            for checkpoint_tag in checkpoint_tags
        ]
        checkpoint = max(
            (tag.snapshot for tag in tagged if tag is not None),
            key=lambda snapshot: snapshot.id,
        )
        self.assertEqual(
            1, module._offset_checkpoint_state(checkpoint)["next_offset"])

        resumed_reads = []

        def record_window(bound, start, end):
            resumed_reads.append(start)
            return original_read(bound, start, end)

        with mock.patch.object(
                offset_module._BoundPaimonOffsetSource,
                "read_window",
                record_window):
            stats = update_by_row_id(
                target,
                self._offset_age_source(target),
                self.catalog_options,
                update_cols=["age"],
                num_partitions=1,
                commit_mode="incremental",
                operation_id=operation_id,
            )

        self.assertEqual({"num_updated": 4}, stats)
        self.assertEqual([1, 2, 3], resumed_reads)
        self.assertEqual(
            [100, 101, 102, 103],
            self._read(target).sort_by("id")["age"].to_pylist(),
        )

        table = self.catalog.get_table(target)
        commit_user = module._operation_commit_user(operation_id)
        latest = table.snapshot_manager().get_latest_snapshot()
        checkpoint_properties = []
        for snapshot_id in range(1, latest.id + 1):
            snapshot = table.snapshot_manager().get_snapshot_by_id(
                snapshot_id)
            if (snapshot is not None
                    and snapshot.commit_user == commit_user):
                checkpoint_properties.append(
                    snapshot.properties[module._CHECKPOINT_PROPERTY])
        self.assertEqual(6, len(checkpoint_properties))
        self.assertTrue(all(
            len(encoded) < 2048
            and "completed_ranges" not in encoded
            for encoded in checkpoint_properties
        ))
        final_state = module._offset_checkpoint_state(latest)
        self.assertEqual(4, final_state["next_offset"])
        self.assertTrue(final_state["complete"])

        completed_snapshot_id = latest.id
        self.assertEqual(
            {"num_updated": 4},
            update_by_row_id(
                target,
                self._offset_age_source(target),
                self.catalog_options,
                update_cols=["age"],
                num_partitions=1,
                commit_mode="incremental",
                operation_id=operation_id,
            ),
        )
        self.assertEqual(
            completed_snapshot_id,
            table.snapshot_manager().get_latest_snapshot().id,
        )

        source_tag = module._operation_source_tag(operation_id, target)
        self.assertIsNotNone(
            module._get_checkpoint_tag(
                self.catalog, target, source_tag))
        self.assertTrue(delete_update_by_row_id_checkpoint(
            target, self.catalog_options, operation_id))
        self.assertIsNone(
            module._get_checkpoint_tag(
                self.catalog, target, source_tag))

    def test_offset_source_reads_separate_table(self):
        import importlib

        module = importlib.import_module(
            "pypaimon.ray.update_by_row_id")

        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {
                "id": [0, 1, 2],
                "name": ["x", "x", "x"],
                "age": [0, 0, 0],
            },
            schema=self.pa_schema,
        ))
        row_ids = self._rowid_by_id(target)

        source_schema = pa.schema([
            ("target_row_id", pa.int64()),
            ("new_age", pa.int32()),
        ])
        source_table = "default.s_" + uuid.uuid4().hex[:8]
        self.catalog.create_table(
            source_table,
            Schema.from_pyarrow_schema(source_schema),
            False,
        )
        self._write(source_table, pa.Table.from_pydict(
            {
                "target_row_id": [
                    row_ids[value] for value in range(3)],
                "new_age": [
                    value + 100 for value in range(3)],
            },
            schema=source_schema,
        ))

        def transform(dataset):
            def to_updates(batch):
                return pa.table({
                    "_ROW_ID": batch["target_row_id"],
                    "age": batch["new_age"],
                })

            return dataset.map_batches(
                to_updates, batch_format="pyarrow")

        source = PaimonOffsetSource(
            source_table,
            projection=["target_row_id", "new_age"],
            transform=transform,
            rows_per_unit=1,
            units_per_checkpoint=1,
        )
        operation_id = "offset-upstream-" + uuid.uuid4().hex
        self.assertEqual(
            {"num_updated": 3},
            update_by_row_id(
                target,
                source,
                self.catalog_options,
                update_cols=["age"],
                num_partitions=1,
                commit_mode="incremental",
                operation_id=operation_id,
            ),
        )
        self.assertEqual(
            [100, 101, 102],
            self._read(target).sort_by("id")["age"].to_pylist(),
        )
        target_table = self.catalog.get_table(target)
        commit_user = module._operation_commit_user(operation_id)
        snapshot_manager = target_table.snapshot_manager()
        operation_snapshots = []
        for snapshot_id in range(
                1, snapshot_manager.get_latest_snapshot().id + 1):
            snapshot = snapshot_manager.get_snapshot_by_id(snapshot_id)
            if snapshot is not None and snapshot.commit_user == commit_user:
                operation_snapshots.append(snapshot)
        self.assertEqual(5, len(operation_snapshots))

        source_tag = module._operation_source_tag(operation_id, target)
        self.assertIsNotNone(module._get_checkpoint_tag(
            self.catalog, source_table, source_tag))
        self.assertTrue(delete_update_by_row_id_checkpoint(
            target, self.catalog_options, operation_id))
        self.assertIsNone(module._get_checkpoint_tag(
            self.catalog, source_table, source_tag))

    def test_offset_window_failure_commits_no_partial_group(self):
        import importlib

        from pypaimon.ray.data_evolution_merge_join import GroupApplyError

        module = importlib.import_module(
            "pypaimon.ray.update_by_row_id")

        target = self._create(partition_keys=["id"])
        for value in range(2):
            self._write(target, pa.Table.from_pydict(
                {
                    "id": [value],
                    "name": ["x"],
                    "age": [0],
                },
                schema=self.pa_schema,
            ))
        table = self.catalog.get_table(target)
        base_snapshot_id = table.snapshot_manager().get_latest_snapshot().id
        files_before = self._data_files_under(table)
        operation_id = "offset-atomic-" + uuid.uuid4().hex
        failure_marker = tempfile.NamedTemporaryFile(delete=False)
        failure_marker_path = failure_marker.name
        failure_marker.close()
        self.addCleanup(
            lambda: os.path.exists(failure_marker_path)
            and os.unlink(failure_marker_path))

        def transform(dataset):
            def to_updates(batch):
                row_ids = batch["_ROW_ID"].to_pylist()
                ages = [value + 100 for value in batch["id"].to_pylist()]
                if os.path.exists(failure_marker_path) and 101 in ages:
                    row_ids.append(row_ids[-1])
                    ages.append(ages[-1])
                return pa.table({
                    "_ROW_ID": row_ids,
                    "age": ages,
                }, schema=pa.schema([
                    ("_ROW_ID", pa.int64()),
                    ("age", pa.int32()),
                ]))

            return dataset.map_batches(
                to_updates, batch_format="pyarrow")

        source = PaimonOffsetSource(
            target,
            projection=["_ROW_ID", "id"],
            transform=transform,
            rows_per_unit=1,
            units_per_checkpoint=2,
        )

        with self.assertRaisesRegex(GroupApplyError, "Deduplicate"):
            update_by_row_id(
                target,
                source,
                self.catalog_options,
                update_cols=["age"],
                num_partitions=1,
                commit_mode="incremental",
                operation_id=operation_id,
            )

        self.assertEqual(files_before, self._data_files_under(table))
        self.assertEqual(
            base_snapshot_id + 1,
            table.snapshot_manager().get_latest_snapshot().id,
        )
        checkpoint_tag = next(
            tag for tag in (
                module._get_checkpoint_tag(
                    self.catalog, target, checkpoint_tag)
                for checkpoint_tag in
                module._operation_checkpoint_tags(operation_id)
            )
            if tag is not None
        )
        self.assertEqual(
            0,
            module._offset_checkpoint_state(
                checkpoint_tag.snapshot)["next_offset"],
        )
        self.assertEqual(
            [0, 0],
            self._read(target).sort_by("id")["age"].to_pylist(),
        )

        os.unlink(failure_marker_path)
        self.assertEqual(
            {"num_updated": 2},
            update_by_row_id(
                target,
                source,
                self.catalog_options,
                update_cols=["age"],
                num_partitions=1,
                commit_mode="incremental",
                operation_id=operation_id,
            ),
        )
        self.assertEqual(
            [100, 101],
            self._read(target).sort_by("id")["age"].to_pylist(),
        )

    def test_offset_resume_rejects_external_overwrite(self):
        import importlib

        from pypaimon.write.commit.conflict_detection import (
            CommitConflictError,
        )

        module = importlib.import_module(
            "pypaimon.ray.update_by_row_id")
        offset_module = importlib.import_module(
            "pypaimon.ray.offset_source")

        target = self._create(partition_keys=["id"])
        for value in range(3):
            self._write(target, pa.Table.from_pydict(
                {
                    "id": [value],
                    "name": ["x"],
                    "age": [0],
                },
                schema=self.pa_schema,
            ))
        operation_id = "offset-overwrite-" + uuid.uuid4().hex
        original_read = offset_module._BoundPaimonOffsetSource.read_window

        def fail_second_window(bound, start, end):
            if start == 1:
                raise RuntimeError("pause before overwrite")
            return original_read(bound, start, end)

        with mock.patch.object(
                offset_module._BoundPaimonOffsetSource,
                "read_window",
                fail_second_window):
            with self.assertRaisesRegex(
                    RuntimeError, "pause before overwrite"):
                update_by_row_id(
                    target,
                    self._offset_age_source(target),
                    self.catalog_options,
                    update_cols=["age"],
                    num_partitions=1,
                    commit_mode="incremental",
                    operation_id=operation_id,
                )

        self._overwrite(
            target,
            {"id": 0},
            pa.Table.from_pydict(
                {
                    "id": [0],
                    "name": ["external"],
                    "age": [999],
                },
                schema=self.pa_schema,
            ),
        )
        table = self.catalog.get_table(target)
        files_before_retry = self._data_files_under(table)

        with self.assertRaisesRegex(
                CommitConflictError, "Concurrent rewrite"):
            update_by_row_id(
                target,
                self._offset_age_source(target),
                self.catalog_options,
                update_cols=["age"],
                num_partitions=1,
                commit_mode="incremental",
                operation_id=operation_id,
            )

        self.assertEqual(
            files_before_retry, self._data_files_under(table))
        state = max(
            (
                module._get_checkpoint_tag(
                    self.catalog, target, checkpoint_tag).snapshot
                for checkpoint_tag in
                module._operation_checkpoint_tags(operation_id)
                if module._get_checkpoint_tag(
                    self.catalog, target, checkpoint_tag) is not None
            ),
            key=lambda snapshot: snapshot.id,
        )
        self.assertEqual(
            1, module._offset_checkpoint_state(state)["next_offset"])
        back = self._read(target).sort_by("id").to_pydict()
        self.assertEqual(["external", "x", "x"], back["name"])
        self.assertEqual([999, 0, 0], back["age"])

    def test_offset_final_marker_rejects_late_overwrite(self):
        import importlib

        from pypaimon.write.commit.conflict_detection import (
            CommitConflictError,
        )

        module = importlib.import_module(
            "pypaimon.ray.update_by_row_id")
        target = self._create(partition_keys=["id"])
        self._write(target, pa.Table.from_pydict(
            {
                "id": [0],
                "name": ["x"],
                "age": [0],
            },
            schema=self.pa_schema,
        ))
        operation_id = "offset-final-overwrite-" + uuid.uuid4().hex
        original_finish = module._OffsetUpdateCommitter.finish
        overwrite_done = False

        def overwrite_before_finish(committer):
            nonlocal overwrite_done
            if not overwrite_done:
                overwrite_done = True
                self._overwrite(
                    target,
                    {"id": 0},
                    pa.Table.from_pydict(
                        {
                            "id": [0],
                            "name": ["external"],
                            "age": [999],
                        },
                        schema=self.pa_schema,
                    ),
                )
            return original_finish(committer)

        with mock.patch.object(
                module._OffsetUpdateCommitter,
                "finish",
                overwrite_before_finish):
            with self.assertRaisesRegex(
                    CommitConflictError, "Concurrent rewrite"):
                update_by_row_id(
                    target,
                    self._offset_age_source(target),
                    self.catalog_options,
                    update_cols=["age"],
                    num_partitions=1,
                    commit_mode="incremental",
                    operation_id=operation_id,
                )

        checkpoint = max(
            (
                module._get_checkpoint_tag(
                    self.catalog, target, checkpoint_tag).snapshot
                for checkpoint_tag in
                module._operation_checkpoint_tags(operation_id)
                if module._get_checkpoint_tag(
                    self.catalog, target, checkpoint_tag) is not None
            ),
            key=lambda snapshot: snapshot.id,
        )
        state = module._offset_checkpoint_state(checkpoint)
        self.assertEqual(1, state["next_offset"])
        self.assertFalse(state["complete"])
        back = self._read(target).to_pydict()
        self.assertEqual(["external"], back["name"])
        self.assertEqual([999], back["age"])

    def test_offset_checkpoint_recovers_without_updated_tag(self):
        import importlib

        module = importlib.import_module(
            "pypaimon.ray.update_by_row_id")

        target = self._create(partition_keys=["id"])
        for value in range(2):
            self._write(target, pa.Table.from_pydict(
                {
                    "id": [value],
                    "name": ["x"],
                    "age": [0],
                },
                schema=self.pa_schema,
            ))
        operation_id = "offset-tag-failure-" + uuid.uuid4().hex
        original_store = module._store_operation_checkpoint
        store_calls = 0

        def fail_initial_tag(*args, **kwargs):
            nonlocal store_calls
            store_calls += 1
            if store_calls == 1:
                raise RuntimeError("checkpoint tag unavailable")
            return original_store(*args, **kwargs)

        with mock.patch.object(
                module,
                "_store_operation_checkpoint",
                fail_initial_tag):
            with self.assertRaisesRegex(
                    RuntimeError, "checkpoint tag unavailable"):
                update_by_row_id(
                    target,
                    self._offset_age_source(target),
                    self.catalog_options,
                    update_cols=["age"],
                    num_partitions=1,
                    commit_mode="incremental",
                    operation_id=operation_id,
                )

        table = self.catalog.get_table(target)
        checkpoint_snapshot_id = (
            table.snapshot_manager().get_latest_snapshot().id)
        self.assertEqual(
            0,
            module._offset_checkpoint_state(
                table.snapshot_manager().get_latest_snapshot()
            )["next_offset"],
        )
        self.assertEqual(
            {"num_updated": 2},
            update_by_row_id(
                target,
                self._offset_age_source(target),
                self.catalog_options,
                update_cols=["age"],
                num_partitions=1,
                commit_mode="incremental",
                operation_id=operation_id,
            ),
        )
        self.assertEqual(
            checkpoint_snapshot_id + 3,
            table.snapshot_manager().get_latest_snapshot().id,
        )
        self.assertEqual(
            [100, 101],
            self._read(target).sort_by("id")["age"].to_pylist(),
        )

    def test_bucket_join_offset_resumes_entire_pipeline(self):
        import importlib

        offset_module = importlib.import_module(
            "pypaimon.ray.offset_source")
        update_module = importlib.import_module(
            "pypaimon.ray.update_by_row_id")
        from pypaimon.write.table_update_by_row_id import (
            TableUpdateByRowId,
        )

        target = self._create()
        row_count = 128
        for start in range(0, row_count, 16):
            values = list(range(start, start + 16))
            self._write(target, pa.Table.from_pydict(
                {
                    "id": values,
                    "name": [f"key-{value}" for value in values],
                    "age": [0] * len(values),
                },
                schema=self.pa_schema,
            ))
        row_ids = self._rowid_by_id(target)
        input_schema = pa.schema([("lookup_key", pa.string())])
        locator_schema = pa.schema([
            ("lookup_key", pa.string()),
            ("row_id", pa.int64()),
        ])
        input_table = self._create_bucketed_table(
            input_schema,
            pa.Table.from_pydict(
                {"lookup_key": [
                    f"key-{value}" for value in range(row_count)]},
                schema=input_schema,
            ),
        )
        locator_table = self._create_bucketed_table(
            locator_schema,
            pa.Table.from_pydict(
                {
                    "lookup_key": [
                        f"key-{value}" for value in range(row_count)],
                    "row_id": [
                        row_ids[value] for value in range(row_count)],
                },
                schema=locator_schema,
            ),
        )
        catalog_options = dict(self.catalog_options)

        def source():
            def infer(joined):
                self.assertEqual(["row_id"], joined.schema().names)
                rows = read_by_row_id(
                    target,
                    joined,
                    catalog_options,
                    projection=["id"],
                    row_id_col="row_id",
                    num_partitions=1,
                )

                def to_updates(batch):
                    return pa.table({
                        "_ROW_ID": batch["_ROW_ID"],
                        "age": pa.compute.add(
                            batch["id"], pa.scalar(100)),
                    })

                return rows.map_batches(
                    to_updates, batch_format="pyarrow")

            return PaimonCoBucketedJoinOffsetSource(
                input_table,
                locator_table,
                on="lookup_key",
                left_projection=["lookup_key"],
                right_projection=["lookup_key", "row_id"],
                transform=infer,
                units_per_checkpoint=1,
                routing_buckets=4,
                route_units_per_commit=2,
            )

        operation_id = "bucket-offset-" + uuid.uuid4().hex
        bound_type = offset_module._BoundPaimonCoBucketedJoinOffsetSource
        original_read = bound_type.read_window
        original_route = bound_type.read_join_units
        original_plan = TableUpdateByRowId._load_existing_files_info
        plan_calls = []

        def record_plan(planner):
            plan_calls.append(True)
            return original_plan(planner)

        first_route_reads = []

        def fail_second_route(bound, unit_indexes):
            first_route_reads.append(list(unit_indexes))
            if unit_indexes[0] == 2:
                raise RuntimeError("stop during routing")
            return original_route(bound, unit_indexes)

        with mock.patch.object(
                TableUpdateByRowId,
                "_load_existing_files_info",
                record_plan), mock.patch.object(
                    bound_type, "read_join_units", fail_second_route):
            with self.assertRaisesRegex(
                    RuntimeError, "stop during routing"):
                update_by_row_id(
                    target,
                    source(),
                    self.catalog_options,
                    update_cols=["age"],
                    num_partitions=1,
                    commit_mode="incremental",
                    operation_id=operation_id,
                )
        self.assertEqual([[0, 1], [2, 3]], first_route_reads)
        self.assertEqual(1, len(plan_calls))
        self.assertEqual(
            [0] * row_count,
            self._read(target).sort_by("id")["age"].to_pylist(),
        )

        plan_calls.clear()
        first_reads = []
        resumed_partial_route_reads = []

        def fail_second_window(bound, start, end):
            first_reads.append(start)
            if len(first_reads) == 2:
                raise RuntimeError("stop after first bucket")
            return original_read(bound, start, end)

        def record_partial_route(bound, unit_indexes):
            resumed_partial_route_reads.append(list(unit_indexes))
            return original_route(bound, unit_indexes)

        with mock.patch.object(
                TableUpdateByRowId,
                "_load_existing_files_info",
                record_plan), mock.patch.object(
                    bound_type, "read_window", fail_second_window), mock.patch.object(
                    bound_type, "read_join_units", record_partial_route):
            with self.assertRaisesRegex(
                    RuntimeError, "stop after first bucket"):
                update_by_row_id(
                    target,
                    source(),
                    self.catalog_options,
                    update_cols=["age"],
                    num_partitions=1,
                    commit_mode="incremental",
                    operation_id=operation_id,
                )
        self.assertEqual(2, len(first_reads))
        self.assertEqual([[2, 3]], resumed_partial_route_reads)
        self.assertEqual(1, len(plan_calls))

        resumed_reads = []
        plan_calls.clear()
        resumed_route_reads = []

        def record_window(bound, start, end):
            resumed_reads.append(start)
            return original_read(bound, start, end)

        def record_route(bound, unit_indexes):
            resumed_route_reads.append(list(unit_indexes))
            return original_route(bound, unit_indexes)

        with mock.patch.object(
                TableUpdateByRowId,
                "_load_existing_files_info",
                record_plan), mock.patch.object(
                    bound_type, "read_window", record_window), mock.patch.object(
                    bound_type, "read_join_units", record_route):
            self.assertEqual(
                {"num_updated": row_count},
                update_by_row_id(
                    target,
                    source(),
                    self.catalog_options,
                    update_cols=["age"],
                    num_partitions=1,
                    commit_mode="incremental",
                    operation_id=operation_id,
                ),
            )
        self.assertNotIn(first_reads[0], resumed_reads)
        self.assertEqual([], resumed_route_reads)
        self.assertEqual(1, len(plan_calls))
        self.assertEqual(
            [value + 100 for value in range(row_count)],
            self._read(target).sort_by("id")["age"].to_pylist(),
        )
        latest = self.catalog.get_table(
            target).snapshot_manager().get_latest_snapshot()
        files_info = update_module._load_row_id_files_info(
            self.catalog.get_table(target), latest.id)
        self.assertTrue(files_info.first_row_id_index)
        self.assertTrue(all(
            len(group[1]) == 2
            for group in files_info.first_row_id_index.values()
        ))

        checkpoint = update_module._get_checkpoint_tag(
            self.catalog,
            target,
            update_module._operation_checkpoint_tags(operation_id)[0],
        ) or update_module._get_checkpoint_tag(
            self.catalog,
            target,
            update_module._operation_checkpoint_tags(operation_id)[1],
        )
        source_plan = update_module._offset_checkpoint_state(
            checkpoint.snapshot)["source"]
        route_table = source_plan["route_table"]
        planner_tag = update_module._operation_planner_tag(operation_id)
        retained_target = update_module._get_checkpoint_tag(
            self.catalog, target, planner_tag)
        self.assertEqual(
            source_plan["target_snapshot_id"],
            retained_target.snapshot.id,
        )
        self.assertIn(
            route_table.rsplit(".", 1)[1],
            self.catalog.list_tables("default"),
        )

        for role, table_identifier in (
                ("source", input_table),
                ("join-right", locator_table)):
            self.assertIsNotNone(update_module._get_checkpoint_tag(
                self.catalog,
                table_identifier,
                update_module._operation_source_tag(
                    operation_id, target, role),
            ))
        self.assertTrue(delete_update_by_row_id_checkpoint(
            target, self.catalog_options, operation_id))
        self.assertNotIn(
            route_table.rsplit(".", 1)[1],
            self.catalog.list_tables("default"),
        )
        self.assertIsNone(update_module._get_checkpoint_tag(
            self.catalog, target, planner_tag))
        for role, table_identifier in (
                ("source", input_table),
                ("join-right", locator_table)):
            self.assertIsNone(update_module._get_checkpoint_tag(
                self.catalog,
                table_identifier,
                update_module._operation_source_tag(
                    operation_id, target, role),
            ))
        self.assertFalse(delete_update_by_row_id_checkpoint(
            target, self.catalog_options, operation_id))

    def test_bucket_join_blob_transform_resumes_without_reprocessing(self):
        import importlib

        offset_module = importlib.import_module(
            "pypaimon.ray.offset_source")

        target_schema = pa.schema([
            ("id", pa.int32()),
            ("payload", pa.large_binary()),
            ("derived_value", pa.int32()),
        ])
        target_options = dict(self.de_options)
        target_options["blob-field"] = "payload"
        target = self._create(
            options=target_options, schema=target_schema)

        row_count = 64
        payloads = [
            bytes([(value % 251) + 1]) * ((value % 5) + 1)
            for value in range(row_count)
        ]
        for start in range(0, row_count, 8):
            values = list(range(start, start + 8))
            self._write(target, pa.Table.from_pydict(
                {
                    "id": values,
                    "payload": payloads[start:start + 8],
                    "derived_value": [0] * len(values),
                },
                schema=target_schema,
            ))

        row_ids = self._rowid_by_id(target)
        key_schema = pa.schema([("lookup_key", pa.string())])
        locator_schema = pa.schema([
            ("lookup_key", pa.string()),
            ("row_id", pa.int64()),
        ])
        keys = ["key-{}".format(value) for value in range(row_count)]
        input_table = self._create_bucketed_table(
            key_schema,
            pa.Table.from_pydict(
                {"lookup_key": keys}, schema=key_schema),
            bucket_key="lookup_key",
        )
        locator_table = self._create_bucketed_table(
            locator_schema,
            pa.Table.from_pydict(
                {
                    "lookup_key": keys,
                    "row_id": [row_ids[value] for value in range(row_count)],
                },
                schema=locator_schema,
            ),
            bucket_key="lookup_key",
        )
        catalog_options = dict(self.catalog_options)
        target_file_io = self.catalog.get_table(target).file_io

        def source():
            def transform(joined):
                descriptors = read_by_row_id(
                    target,
                    joined,
                    catalog_options,
                    projection=["payload"],
                    row_id_col="row_id",
                    dynamic_options={"blob-as-descriptor": "true"},
                    num_partitions=1,
                )

                def derive(batch, blobs):
                    return pa.table(
                        {
                            "_ROW_ID": batch["_ROW_ID"],
                            "derived_value": [
                                sum(value) for value in blobs["payload"]
                            ],
                        },
                        schema=pa.schema([
                            ("_ROW_ID", pa.int64()),
                            ("derived_value", pa.int32()),
                        ]),
                    )

                return map_with_blobs(
                    descriptors,
                    ["payload"],
                    derive,
                    file_io=target_file_io,
                    all_blob_columns=["payload"],
                    parallelism=2,
                    batch_size=8,
                )

            return PaimonCoBucketedJoinOffsetSource(
                input_table,
                locator_table,
                on="lookup_key",
                left_projection=["lookup_key"],
                right_projection=["lookup_key", "row_id"],
                row_id_col="row_id",
                transform=transform,
                units_per_checkpoint=1,
                routing_buckets=8,
                route_units_per_commit=2,
            )

        operation_id = "blob-offset-" + uuid.uuid4().hex
        bound_type = offset_module._BoundPaimonCoBucketedJoinOffsetSource
        original_read = bound_type.read_window
        first_reads = []

        def fail_second_window(bound, start, end):
            first_reads.append(start)
            if len(first_reads) == 2:
                raise RuntimeError("stop after first transform window")
            return original_read(bound, start, end)

        with mock.patch.object(
                bound_type, "read_window", fail_second_window):
            with self.assertRaisesRegex(
                    RuntimeError, "stop after first transform window"):
                update_by_row_id(
                    target,
                    source(),
                    self.catalog_options,
                    update_cols=["derived_value"],
                    num_partitions=1,
                    commit_mode="incremental",
                    operation_id=operation_id,
                )

        partial = self._read(target).sort_by("id")[
            "derived_value"].to_pylist()
        self.assertGreater(sum(value != 0 for value in partial), 0)
        self.assertLess(sum(value != 0 for value in partial), row_count)

        resumed_reads = []

        def record_window(bound, start, end):
            resumed_reads.append(start)
            return original_read(bound, start, end)

        with mock.patch.object(bound_type, "read_window", record_window):
            self.assertEqual(
                {"num_updated": row_count},
                update_by_row_id(
                    target,
                    source(),
                    self.catalog_options,
                    update_cols=["derived_value"],
                    num_partitions=1,
                    commit_mode="incremental",
                    operation_id=operation_id,
                ),
            )

        self.assertNotIn(first_reads[0], resumed_reads)
        result = self._read(target).sort_by("id").to_pydict()
        self.assertEqual(payloads, result["payload"])
        self.assertEqual(
            [sum(payload) for payload in payloads],
            result["derived_value"],
        )
        self.assertTrue(delete_update_by_row_id_checkpoint(
            target, self.catalog_options, operation_id))

    def test_bucket_join_offset_handles_no_matches(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [0]},
            schema=self.pa_schema,
        ))
        input_table = self._create_bucketed_table(
            pa.schema([("lookup_key", pa.string())]),
            pa.table({"lookup_key": ["missing"]}),
            bucket_count=2,
        )
        locator_schema = pa.schema([
            ("lookup_key", pa.string()),
            ("row_id", pa.int64()),
        ])
        locator_table = self._create_bucketed_table(
            locator_schema,
            pa.Table.from_pydict(
                {"lookup_key": ["present"], "row_id": [0]},
                schema=locator_schema,
            ),
            bucket_count=2,
        )
        source = PaimonCoBucketedJoinOffsetSource(
            input_table,
            locator_table,
            on="lookup_key",
            left_projection=["lookup_key"],
            right_projection=["lookup_key", "row_id"],
            transform=lambda rows: rows,
            routing_buckets=2,
            route_units_per_commit=1,
        )
        operation_id = "bucket-empty-" + uuid.uuid4().hex
        self.assertEqual(
            {"num_updated": 0},
            update_by_row_id(
                target,
                source,
                self.catalog_options,
                update_cols=["age"],
                commit_mode="incremental",
                operation_id=operation_id,
            ),
        )
        self.assertEqual([0], self._read(target)["age"].to_pylist())
        self.assertTrue(delete_update_by_row_id_checkpoint(
            target, self.catalog_options, operation_id))

    def test_bucket_join_offset_rejects_row_id_outside_window(self):
        import importlib

        module = importlib.import_module(
            "pypaimon.ray.update_by_row_id")

        class Extractor:
            def extract_partition_bucket_row(self, values):
                return (), values["_TARGET_GROUP"] % 4

        route_table = types.SimpleNamespace(
            create_row_key_extractor=lambda: Extractor())
        source = types.SimpleNamespace(_needs_target_read_plan=True)
        bound = types.SimpleNamespace(
            source=source, route_table=route_table)

        signature = ((), 0, 0, "file", (), None, None, 6, 1, 0, None)
        with self.assertRaisesRegex(
                ValueError, "outside its checkpoint window"):
            module._validate_routed_target_groups(
                bound, {signature}, 0, 2)

    def test_updates_correct_row_across_files(self):
        # A _ROW_ID owned by a middle data file must update only that row.
        target = self._create()
        for chunk in ([10, 11, 12], [20, 21], [30, 31, 32, 33]):
            self._write(target, pa.Table.from_pydict(
                {"id": chunk, "name": ["x"] * len(chunk), "age": [0] * len(chunk)},
                schema=self.pa_schema))
        rid = self._rowid_by_id(target)
        src = pa.table({"_ROW_ID": [rid[21]], "age": [999]},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]))
        stats = update_by_row_id(target, ray.data.from_arrow(src),
                                 self.catalog_options, update_cols=["age"])
        self.assertEqual(stats, {"num_updated": 1})
        back = self._read(target).sort_by("id").to_pydict()
        got = dict(zip(back["id"], back["age"]))
        self.assertEqual(got[21], 999)
        self.assertTrue(all(v == 0 for k, v in got.items() if k != 21))

    def test_incrementally_commits_file_group_windows(self):
        from pypaimon.write.table_commit import StreamTableCommit

        target = self._create()
        chunks = [[start, start + 1] for start in range(10, 60, 10)]
        for chunk in chunks:
            self._write(target, pa.Table.from_pydict(
                {"id": chunk, "name": ["x", "x"], "age": [0, 0]},
                schema=self.pa_schema,
            ))

        table = self.catalog.get_table(target)
        base_snapshot_id = table.snapshot_manager().get_latest_snapshot().id
        row_ids = self._rowid_by_id(target)
        updated_ids = [chunk[0] for chunk in chunks]
        source = pa.table(
            {
                "_ROW_ID": [row_ids[row_id] for row_id in updated_ids],
                "age": updated_ids,
            },
            schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]),
        )
        commits = []
        original_commit = StreamTableCommit.commit

        def record_commit(stream_commit, messages, commit_identifier):
            commits.append((len(messages), commit_identifier))
            return original_commit(
                stream_commit, messages, commit_identifier)

        with mock.patch.object(StreamTableCommit, "commit", record_commit):
            stats = update_by_row_id(
                target,
                ray.data.from_arrow(source).repartition(4),
                self.catalog_options,
                update_cols=["age"],
                num_partitions=4,
                commit_mode="incremental",
                max_groups_per_commit=2,
            )

        self.assertEqual({"num_updated": 5}, stats)
        self.assertEqual([(2, 1), (2, 2), (1, 3)], commits)
        self.assertEqual(
            base_snapshot_id + 3,
            table.snapshot_manager().get_latest_snapshot().id,
        )
        result = self._read(target).sort_by("id").to_pydict()
        ages = dict(zip(result["id"], result["age"]))
        self.assertEqual(
            {row_id: row_id for row_id in updated_ids},
            {row_id: ages[row_id] for row_id in updated_ids},
        )

    def test_group_failure_preserves_completed_groups(self):
        for max_groups, expected_snapshots in [(1, 2), (5, 1)]:
            with self.subTest(max_groups_per_commit=max_groups):
                target = self._create()
                for row_id in range(1, 4):
                    self._write(target, pa.Table.from_pydict(
                        {"id": [row_id], "name": ["a"], "age": [0]},
                        schema=self.pa_schema,
                    ))

                table = self.catalog.get_table(target)
                base_snapshot_id = (
                    table.snapshot_manager().get_latest_snapshot().id
                )
                row_ids = self._rowid_by_id(target)
                source = pa.table(
                    {
                        "_ROW_ID": [
                            row_ids[1], row_ids[2], row_ids[3], row_ids[3],
                        ],
                        "age": [100, 200, 300, 301],
                    },
                    schema=pa.schema([
                        ("_ROW_ID", pa.int64()),
                        ("age", pa.int32()),
                    ]),
                )

                with self.assertRaisesRegex(RuntimeError, "Deduplicate"):
                    update_by_row_id(
                        target,
                        ray.data.from_arrow(source),
                        self.catalog_options,
                        update_cols=["age"],
                        num_partitions=1,
                        commit_mode="incremental",
                        max_groups_per_commit=max_groups,
                    )

                self.assertEqual(
                    base_snapshot_id + expected_snapshots,
                    table.snapshot_manager().get_latest_snapshot().id,
                )
                self.assertEqual(
                    [100, 200, 0],
                    self._read(target).sort_by("id")["age"].to_pylist(),
                )

    def test_atomic_group_failure_commits_nothing(self):
        target = self._create()
        for row_id in range(1, 4):
            self._write(target, pa.Table.from_pydict(
                {"id": [row_id], "name": ["a"], "age": [0]},
                schema=self.pa_schema,
            ))

        table = self.catalog.get_table(target)
        base_snapshot_id = table.snapshot_manager().get_latest_snapshot().id
        row_ids = self._rowid_by_id(target)
        source = pa.table(
            {
                "_ROW_ID": [
                    row_ids[1], row_ids[2], row_ids[3], row_ids[3],
                ],
                "age": [100, 200, 300, 301],
            },
            schema=pa.schema([
                ("_ROW_ID", pa.int64()),
                ("age", pa.int32()),
            ]),
        )

        with self.assertRaisesRegex(ValueError, "Deduplicate"):
            update_by_row_id(
                target,
                ray.data.from_arrow(source),
                self.catalog_options,
                update_cols=["age"],
                num_partitions=1,
            )

        self.assertEqual(
            base_snapshot_id,
            table.snapshot_manager().get_latest_snapshot().id,
        )
        self.assertEqual(
            [0, 0, 0],
            self._read(target).sort_by("id")["age"].to_pylist(),
        )

    def test_incremental_committer_batches_and_aborts_pending_groups(self):
        import importlib

        module = importlib.import_module("pypaimon.ray.update_by_row_id")
        commits = []
        aborted = []
        close_calls = []

        class FakeCommit:
            def commit(self, messages, commit_identifier):
                commits.append((list(messages), commit_identifier))

            def close(self):
                close_calls.append(True)

        class FakeBuilder:
            def new_commit(self):
                return FakeCommit()

        class FakeTable:
            def new_stream_write_builder(self):
                return FakeBuilder()

        committer = module._IncrementalUpdateCommitter(FakeTable(), 2)
        with mock.patch.object(
                module,
                "_abort_pending_update_messages",
                side_effect=lambda table, messages: aborted.append(list(messages))):
            committer.add_group(["group-1"], 1, [])
            committer.add_group(["group-2"], 1, [])
            committer.add_group(["group-3"], 1, [])
            committer.abort_pending()
            committer.close()

        self.assertEqual(
            [(["group-1", "group-2"], 1)],
            commits,
        )
        self.assertEqual([["group-3"]], aborted)
        self.assertEqual([True], close_calls)

    def test_incremental_commit_failure_aborts_later_groups(self):
        import importlib

        module = importlib.import_module("pypaimon.ray.update_by_row_id")
        aborted = []
        commit_calls = []

        class FakeCommit:
            def commit(self, messages, commit_identifier):
                commit_calls.append((list(messages), commit_identifier))
                raise RuntimeError("commit failed")

            def close(self):
                pass

        class FakeBuilder:
            def new_commit(self):
                return FakeCommit()

        class FakeTable:
            def new_stream_write_builder(self):
                return FakeBuilder()

        committer = module._IncrementalUpdateCommitter(FakeTable(), 1)
        with mock.patch.object(
                module,
                "_abort_pending_update_messages",
                side_effect=lambda table, messages: aborted.append(list(messages))):
            committer.add_group(["group-1"], 1, [])
            committer.add_group(["group-2"], 1, [])
            with self.assertRaisesRegex(RuntimeError, "commit failed"):
                committer.finish()
            committer.abort_pending()
            committer.close()

        self.assertEqual([(["group-1"], 1)], commit_calls)
        self.assertEqual([["group-2"]], aborted)

    def test_incremental_commit_conflict_aborts_buffered_group_files(self):
        from pypaimon.write.commit.conflict_detection import CommitConflictError
        from pypaimon.write.file_store_commit import FileStoreCommit

        target = self._create()
        for row_id in range(1, 4):
            self._write(target, pa.Table.from_pydict(
                {"id": [row_id], "name": ["a"], "age": [0]},
                schema=self.pa_schema,
            ))

        table = self.catalog.get_table(target)
        base_snapshot_id = table.snapshot_manager().get_latest_snapshot().id
        files_before = self._data_files_under(table)
        row_ids = self._rowid_by_id(target)
        source = pa.table(
            {
                "_ROW_ID": [row_ids[1], row_ids[2], row_ids[3]],
                "age": [100, 200, 300],
            },
            schema=pa.schema([
                ("_ROW_ID", pa.int64()),
                ("age", pa.int32()),
            ]),
        )

        with mock.patch.object(
                FileStoreCommit,
                "commit",
                side_effect=CommitConflictError("forced conflict")):
            with self.assertRaisesRegex(CommitConflictError, "forced conflict"):
                update_by_row_id(
                    target,
                    ray.data.from_arrow(source),
                    self.catalog_options,
                    update_cols=["age"],
                    num_partitions=1,
                    commit_mode="incremental",
                    max_groups_per_commit=1,
                )

        self.assertEqual(files_before, self._data_files_under(table))
        self.assertEqual(
            base_snapshot_id,
            table.snapshot_manager().get_latest_snapshot().id,
        )
        self.assertEqual(
            [0, 0, 0],
            self._read(target).sort_by("id")["age"].to_pylist(),
        )

    def test_pins_base_snapshot_for_conflict_detection(self):
        # The update pins its base snapshot and threads it to distributed_update_apply,
        # which uses it for commit-time conflict detection against concurrent writers.
        import importlib
        m = importlib.import_module("pypaimon.ray.update_by_row_id")  # module, not the fn
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1, 2], "name": ["a", "b"], "age": [1, 2]}, schema=self.pa_schema))
        expected_sid = self.catalog.get_table(
            target).snapshot_manager().get_latest_snapshot().id
        rid = self._rowid_by_id(target)
        src = pa.table({"_ROW_ID": [rid[1]], "age": [9]},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]))

        captured = {}

        def fake_apply(update_ds, table, cols, *, num_partitions,
                       ray_remote_args=None, base_snapshot_id=None):
            captured["base_snapshot_id"] = base_snapshot_id
            return [], 0, []

        with mock.patch.object(m, "distributed_update_apply", fake_apply):
            update_by_row_id(target, src, self.catalog_options, update_cols=["age"])
        self.assertEqual(captured["base_snapshot_id"], expected_sid)

    def test_new_commit_failure_aborts_pending_messages(self):
        err = RuntimeError("new_commit failed")
        recorder = {}

        with self.assertRaisesRegex(RuntimeError, "new_commit failed"):
            self._run_with_fake_commit(
                recorder=recorder,
                new_commit_errors=[err],
            )

        self.assertEqual(recorder["commit_calls"], 0)
        self.assertEqual(recorder["abort_calls"], 1)
        self.assertEqual(recorder["abort_msgs"], recorder["msgs"])

    def test_commit_failure_does_not_abort_after_commit_started(self):
        err = RuntimeError("commit failed")
        recorder = {}

        with self.assertRaisesRegex(RuntimeError, "commit failed"):
            self._run_with_fake_commit(
                recorder=recorder,
                commit_error=err,
            )

        self.assertEqual(recorder["commit_calls"], 1)
        self.assertEqual(recorder["abort_calls"], 0)
        self.assertEqual(recorder["close_calls"], 1)

    def test_incremental_driver_commit_failure_is_not_unwrapped(self):
        retry_error = ValueError("retry failed")
        commit_error = RuntimeError("commit failed")
        commit_error.__cause__ = retry_error

        with self.assertRaises(RuntimeError) as raised:
            self._run_with_fake_commit(
                commit_error=commit_error,
                incremental=True,
            )

        self.assertIs(commit_error, raised.exception)
        self.assertIs(retry_error, raised.exception.__cause__)

    def test_close_failure_after_success_warns_and_returns_stats(self):
        close_error = RuntimeError("close failed")

        with self.assertLogs("pypaimon.ray.update_by_row_id", level="WARNING") as logs:
            recorder = self._run_with_fake_commit(close_error=close_error)

        self.assertEqual(recorder["result"], {"num_updated": 3})
        self.assertEqual(recorder["commit_calls"], 1)
        self.assertEqual(recorder["abort_calls"], 0)
        self.assertIn(
            "Failed to close update_by_row_id commit",
            "\n".join(logs.output),
        )

    def test_accepts_pyarrow_and_pandas_source(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1, 2], "name": ["a", "b"], "age": [1, 2]}, schema=self.pa_schema))
        rid = self._rowid_by_id(target)
        # pyarrow.Table source
        update_by_row_id(
            target,
            pa.table({"_ROW_ID": [rid[1]], "age": [77]},
                     schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())])),
            self.catalog_options, update_cols=["age"])
        self.assertEqual(self._read(target).sort_by("id").to_pydict()["age"], [77, 2])

        # pandas.DataFrame source, updating multiple columns at once
        import pandas as pd
        update_by_row_id(
            target,
            pd.DataFrame({"_ROW_ID": pd.array([rid[2]], dtype="int64"),
                          "name": ["z"], "age": pd.array([88], dtype="int32")}),
            self.catalog_options, update_cols=["name", "age"])
        back = self._read(target).sort_by("id").to_pydict()
        self.assertEqual(back["age"], [77, 88])
        self.assertEqual(back["name"], ["a", "z"])

    def test_rejects_table_name_source(self):
        # A source table's system _ROW_ID is its own, not the target's row ids, so a
        # table-name source is rejected rather than silently updating wrong rows.
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        with self.assertRaises(ValueError):
            update_by_row_id(target, "default.some_source", self.catalog_options,
                             update_cols=["age"])

    def test_rejects_non_data_evolution_table(self):
        target = self._create(options={})  # plain append table
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        src = pa.table({"_ROW_ID": [0], "age": [9]},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]))
        with self.assertRaises(ValueError):
            update_by_row_id(target, src, self.catalog_options, update_cols=["age"])

    def test_rejects_missing_row_id_column(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        src = pa.table({"age": [9]}, schema=pa.schema([("age", pa.int32())]))
        with self.assertRaises(ValueError):
            update_by_row_id(target, src, self.catalog_options, update_cols=["age"])

    def test_rejects_partition_column_update(self):
        name = f"default.u_{uuid.uuid4().hex[:8]}"
        s = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=["name"],
                                       options=self.de_options)
        self.catalog.create_table(name, s, False)
        self._write(name, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        src = pa.table({"_ROW_ID": [0], "name": ["b"]},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("name", pa.string())]))
        with self.assertRaises(ValueError):
            update_by_row_id(name, src, self.catalog_options, update_cols=["name"])

    def test_rejects_deletion_vectors_table(self):
        # A DV-deleted row still lives in its file, so update_by_row_id can't tell it is
        # gone without reading the target; DV tables are refused for now.
        opts = dict(self.de_options, **{"deletion-vectors.enabled": "true"})
        target = self._create(options=opts)
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        src = pa.table({"_ROW_ID": [0], "age": [9]},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]))
        with self.assertRaises(ValueError):
            update_by_row_id(target, src, self.catalog_options, update_cols=["age"])

    def test_rejects_blob_column_update(self):
        blob_schema = pa.schema([("id", pa.int32()), ("payload", pa.large_binary())])
        name = f"default.u_{uuid.uuid4().hex[:8]}"
        self.catalog.create_table(
            name, Schema.from_pyarrow_schema(blob_schema, options=self.de_options), False)
        self._write(name, pa.Table.from_pydict(
            {"id": [1], "payload": pa.array([b"x"], pa.large_binary())}, schema=blob_schema))
        src = pa.table({"_ROW_ID": [0], "payload": pa.array([b"y"], pa.large_binary())},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("payload", pa.large_binary())]))
        with self.assertRaises(ValueError):
            update_by_row_id(name, src, self.catalog_options, update_cols=["payload"])

    def test_empty_target_foreign_row_id_raises(self):
        src = pa.table({"_ROW_ID": [0], "age": [9]},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]))
        empty_src = pa.table({"_ROW_ID": pa.array([], pa.int64()),
                              "age": pa.array([], pa.int32())})

        # (a) never written -> no snapshot
        target = self._create()
        with self.assertRaises(ValueError):
            update_by_row_id(target, src, self.catalog_options, update_cols=["age"])
        # empty source against an empty target is a no-op, not an error
        self.assertEqual(
            update_by_row_id(target, empty_src, self.catalog_options, update_cols=["age"]),
            {"num_updated": 0})

        # (b) written then emptied by overwrite -> snapshot exists but 0 live rows
        target2 = self._create()
        self._write(target2, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        wb = self.catalog.get_table(target2).new_batch_write_builder().overwrite()
        w = wb.new_write()
        w.write_arrow(pa.Table.from_pydict(
            {"id": pa.array([], pa.int32()), "name": pa.array([], pa.string()),
             "age": pa.array([], pa.int32())}, schema=self.pa_schema))
        wb.new_commit().commit(w.prepare_commit())
        w.close()
        with self.assertRaises(ValueError):
            update_by_row_id(target2, src, self.catalog_options, update_cols=["age"])

    def test_rejects_unknown_and_empty_update_cols(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        src = pa.table({"_ROW_ID": [0], "age": [9]},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]))
        with self.assertRaises(ValueError):
            update_by_row_id(target, src, self.catalog_options, update_cols=["nope"])
        with self.assertRaises(ValueError):
            update_by_row_id(target, src, self.catalog_options, update_cols=[])

    def test_rejects_invalid_max_groups_per_commit(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        source = pa.table(
            {"_ROW_ID": [0], "age": [9]},
            schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]),
        )

        for value in [0, -1, True, 1.5, "2"]:
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                        ValueError, "must be a positive integer"):
                    update_by_row_id(
                        target,
                        source,
                        self.catalog_options,
                        update_cols=["age"],
                        commit_mode="incremental",
                        max_groups_per_commit=value,
                    )

    def test_requires_explicit_incremental_commit_mode(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        source = pa.table(
            {"_ROW_ID": [0], "age": [9]},
            schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]),
        )

        with self.assertRaisesRegex(
                ValueError, "requires commit_mode='incremental'"):
            update_by_row_id(
                target,
                source,
                self.catalog_options,
                update_cols=["age"],
                max_groups_per_commit=1,
            )
        with self.assertRaisesRegex(
                ValueError, "requires max_groups_per_commit"):
            update_by_row_id(
                target,
                source,
                self.catalog_options,
                update_cols=["age"],
                commit_mode="incremental",
            )
        with self.assertRaisesRegex(
                ValueError, "must be 'atomic' or 'incremental'"):
            update_by_row_id(
                target,
                source,
                self.catalog_options,
                update_cols=["age"],
                commit_mode="unknown",
            )

    def test_operation_id_requires_offset_source(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]},
            schema=self.pa_schema))
        source = pa.table(
            {"_ROW_ID": [0], "age": [9]},
            schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]),
        )

        with self.assertRaisesRegex(ValueError, "requires a PaimonOffsetSource"):
            update_by_row_id(
                target,
                source,
                self.catalog_options,
                update_cols=["age"],
                commit_mode="incremental",
                max_groups_per_commit=1,
                operation_id="raw-source",
            )
        with self.assertRaisesRegex(ValueError, "requires operation_id"):
            update_by_row_id(
                target,
                PaimonOffsetSource(target),
                self.catalog_options,
                update_cols=["age"],
                commit_mode="incremental",
            )

    def _run_with_fake_commit(self, *, recorder=None, new_commit_errors=None,
                              commit_error=None, close_error=None,
                              incremental=False):
        import importlib

        m = importlib.import_module("pypaimon.ray.update_by_row_id")

        if recorder is None:
            recorder = {}
        recorder.update({
            "msgs": [object()],
            "new_commit_errors": list(new_commit_errors or []),
            "commit_error": commit_error,
            "close_error": close_error,
            "new_commit_calls": 0,
            "commit_calls": 0,
            "abort_calls": 0,
            "close_calls": 0,
        })

        class FakeOptions:
            def data_evolution_enabled(self):
                return True

            def row_tracking_enabled(self):
                return True

            def deletion_vectors_enabled(self):
                return False

        class FakeCommit:
            def commit(self, msgs, *args):
                recorder["commit_calls"] += 1
                recorder["commit_msgs"] = list(msgs)
                if recorder["commit_error"] is not None:
                    raise recorder["commit_error"]

            def abort(self, msgs):
                recorder["abort_calls"] += 1
                recorder["abort_msgs"] = list(msgs)

            def close(self):
                recorder["close_calls"] += 1
                if recorder["close_error"] is not None:
                    raise recorder["close_error"]

        class FakeWriteBuilder:
            def new_commit(self):
                recorder["new_commit_calls"] += 1
                if recorder["new_commit_errors"]:
                    raise recorder["new_commit_errors"].pop(0)
                return FakeCommit()

        class FakeTable:
            field_names = ["age"]
            partition_keys = []
            options = FakeOptions()
            table_schema = types.SimpleNamespace(fields=[
                types.SimpleNamespace(
                    name="age",
                    type=types.SimpleNamespace(type="INT"),
                )
            ])

            def snapshot_manager(self):
                return types.SimpleNamespace(get_latest_snapshot=lambda: types.SimpleNamespace(
                    id=1,
                    total_record_count=1,
                ))

            def new_batch_write_builder(self):
                return FakeWriteBuilder()

            def new_stream_write_builder(self):
                return FakeWriteBuilder()

        class FakeCatalog:
            def get_table(self, target):
                return FakeTable()

        class FakeSource:
            def schema(self):
                return types.SimpleNamespace(names=["_ROW_ID", "age"])

            def map_batches(self, fn, batch_format=None):
                return self

        parser_path = (
            "pypaimon.schema.data_types.PyarrowFieldParser.from_paimon_schema"
        )

        def fake_apply(*args, **kwargs):
            if incremental:
                kwargs["on_group_result"](recorder["msgs"], 3, [])
                return [], 3, []
            return recorder["msgs"], 3, []

        with mock.patch(
                "pypaimon.catalog.catalog_factory.CatalogFactory.create",
                return_value=FakeCatalog()), \
                mock.patch.object(m, "_normalize_source",
                                  side_effect=lambda source, catalog_options: source), \
                mock.patch.object(m, "build_update_schema",
                                  return_value=pa.schema([
                                      ("_ROW_ID", pa.int64()),
                                      ("age", pa.int32()),
                                  ])), \
                mock.patch(parser_path,
                           return_value=pa.schema([
                               ("age", pa.int32()),
                           ])), \
                mock.patch.object(m, "distributed_update_apply",
                                  side_effect=fake_apply):
            recorder["result"] = m.update_by_row_id(
                "default.fake",
                FakeSource(),
                self.catalog_options,
                update_cols=["age"],
                commit_mode="incremental" if incremental else "atomic",
                max_groups_per_commit=1 if incremental else None,
            )
        return recorder


if __name__ == "__main__":
    unittest.main()
