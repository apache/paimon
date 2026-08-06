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

import functools
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
from pypaimon.ray import update_by_row_id, update_by_transform
from pypaimon.ray.data_evolution_merge_join import GroupApplyError


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

    def _create(self, options=None, schema=None):
        name = f"default.u_{uuid.uuid4().hex[:8]}"
        opts = self.de_options if options is None else options
        self.catalog.create_table(
            name,
            Schema.from_pyarrow_schema(schema or self.pa_schema, options=opts),
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

    def _read(self, target, projection=None):
        t = self.catalog.get_table(target)
        rb = t.new_read_builder()
        if projection is not None:
            rb = rb.with_projection(projection)
        return rb.new_read().to_arrow(rb.new_scan().plan().splits())

    def _rowid_by_id(self, target):
        tab = self._read(target, ["_ROW_ID", "id"])
        return dict(zip(tab.column("id").to_pylist(), tab.column("_ROW_ID").to_pylist()))

    def _three_file_target(self):
        target = self._create()
        for row_id in range(1, 4):
            self._write(target, pa.Table.from_pydict(
                {"id": [row_id], "name": ["a"], "age": [0]},
                schema=self.pa_schema,
            ))
        return target, self.catalog.get_table(target), self._rowid_by_id(target)

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
        chunks = [
            [10, 11, 12],
            [20],
            [30, 31, 32],
            [40, 41],
            [50],
        ]
        for chunk in chunks:
            self._write(target, pa.Table.from_pydict(
                {
                    "id": chunk,
                    "name": ["x"] * len(chunk),
                    "age": [0] * len(chunk),
                },
                schema=self.pa_schema,
            ))

        table = self.catalog.get_table(target)
        base_snapshot_id = table.snapshot_manager().get_latest_snapshot().id
        row_ids = self._rowid_by_id(target)
        first_ids = [chunk[0] for chunk in chunks]

        class Infer:

            def __init__(self, catalog_options, table_name):
                tags = (
                    CatalogFactory.create(catalog_options)
                    .get_table(table_name)
                    .list_tags()
                )
                if not any(
                        tag.startswith("pypaimon-transform-update-")
                        for tag in tags):
                    raise RuntimeError("planned snapshot is not retained")

            def __call__(self, batch):
                if batch.column_names != ["id"]:
                    raise AssertionError("transform must not receive _ROW_ID")
                return pa.table({
                    "age": batch.column("id"),
                })

        commits = []
        original_commit = StreamTableCommit.commit

        def record_commit(stream_commit, messages, commit_identifier):
            commits.append(tuple(sorted({
                data_file.first_row_id
                for message in messages
                for data_file in message.new_files
            })))
            return original_commit(
                stream_commit, messages, commit_identifier)

        with mock.patch.object(StreamTableCommit, "commit", record_commit):
            stats = update_by_transform(
                target,
                self.catalog_options,
                update_cols=["age"],
                num_partitions=4,
                rows_per_commit=4,
                read_projection=["id"],
                transform=functools.partial(
                    Infer, self.catalog_options, target),
                transform_batch_size=1,
            )

        self.assertEqual({"num_updated": 10}, stats)
        expected_ranges = [
            tuple(row_ids[row_id] for row_id in first_ids[start:start + 2])
            for start in range(0, len(first_ids), 2)
        ]
        self.assertEqual(sorted(expected_ranges), sorted(commits))
        self.assertEqual(
            base_snapshot_id + 3,
            table.snapshot_manager().get_latest_snapshot().id,
        )
        result = self._read(target).sort_by("id").to_pydict()
        self.assertEqual(result["id"], result["age"])
        self.assertFalse(table.list_tags())

        snapshot_id = table.snapshot_manager().get_latest_snapshot().id
        empty = pa.table({
            "_ROW_ID": pa.array([], pa.int64()),
            "age": pa.array([], pa.int32()),
        })
        self.assertEqual(
            {"num_updated": 0},
            update_by_row_id(
                target, empty, self.catalog_options,
                update_cols=["age"]),
        )
        self.assertEqual(
            snapshot_id, table.snapshot_manager().get_latest_snapshot().id)
        self.assertFalse(table.list_tags())

    def test_transform_filter_updates_only_matching_rows(self):
        target = self._create()
        self._write(target, pa.table({
            "id": [1, 2, 3, 4, 5],
            "name": ["x"] * 5,
            "age": pa.array([0] * 5, type=pa.int32()),
        }, schema=self.pa_schema))

        def transform(batch):
            return pa.table({"age": batch.column("id")})

        stats = update_by_transform(
            target,
            self.catalog_options,
            filter="id BETWEEN 2 AND 4",
            read_projection=["id"],
            transform=transform,
            update_cols=["age"],
            rows_per_commit=100,
        )

        self.assertEqual({"num_updated": 3}, stats)
        self.assertEqual(
            [0, 2, 3, 4, 0],
            self._read(target).sort_by("id")["age"].to_pylist(),
        )

        def keep_edges(batch):
            return pa.array([
                value.as_py() in (1, 5) for value in batch.column("id")
            ])

        self.assertEqual(
            {"num_updated": 2},
            update_by_transform(
                target,
                self.catalog_options,
                filter=keep_edges,
                read_projection=["id"],
                transform=transform,
                update_cols=["age"],
                rows_per_commit=100,
            ),
        )
        self.assertEqual(
            [1, 2, 3, 4, 5],
            self._read(target).sort_by("id")["age"].to_pylist(),
        )
        table = self.catalog.get_table(target)
        snapshot_id = table.snapshot_manager().get_latest_snapshot().id
        self.assertEqual(
            {"num_updated": 0},
            update_by_transform(
                target,
                self.catalog_options,
                filter="id > 100",
                read_projection=["id"],
                transform=transform,
                update_cols=["age"],
                rows_per_commit=100,
            ),
        )
        self.assertEqual(
            snapshot_id, table.snapshot_manager().get_latest_snapshot().id)

    def test_transform_backfills_new_column(self):
        from pypaimon.schema.data_types import AtomicType
        from pypaimon.schema.schema_change import SchemaChange

        source_schema = pa.schema([
            ("id", pa.int32()),
            ("name", pa.string()),
        ])
        target = self._create(schema=source_schema)
        self._write(target, pa.table({
            "id": [1, 2],
            "name": ["a", "bb"],
        }, schema=source_schema))
        self.catalog.alter_table(
            target,
            [SchemaChange.add_column("age", AtomicType("INT"))],
            False,
        )

        result = update_by_transform(
            target,
            self.catalog_options,
            read_projection=["name"],
            transform=lambda batch: pa.table({
                "age": [len(value.as_py())
                        for value in batch.column("name")],
            }),
            update_cols=["age"],
            rows_per_commit=100,
        )

        self.assertEqual({"num_updated": 2}, result)
        self.assertEqual(
            [1, 2],
            self._read(target).sort_by("id")["age"].to_pylist(),
        )

    def test_transform_failure_preserves_completed_range(self):
        target = self._create()
        for row_id in range(1, 9):
            self._write(target, pa.table({
                "id": [row_id],
                "name": ["x"],
                "age": pa.array([0], type=pa.int32()),
            }, schema=self.pa_schema))

        table = self.catalog.get_table(target)
        base_snapshot_id = table.snapshot_manager().get_latest_snapshot().id
        files_before = self._data_files_under(table)

        def transform(batch):
            if batch.column("id")[0].as_py() == 4:
                raise RuntimeError("forced transform failure")
            return pa.table({
                "age": batch.column("id"),
            })

        with self.assertRaisesRegex(
                GroupApplyError, "forced transform failure"):
            update_by_transform(
                target,
                self.catalog_options,
                update_cols=["age"],
                num_partitions=8,
                rows_per_commit=1,
                read_projection=["id"],
                transform=transform,
            )

        self.assertEqual(
            base_snapshot_id + 7,
            table.snapshot_manager().get_latest_snapshot().id,
        )
        result = self._read(target).sort_by("id")
        self.assertEqual(
            [1, 2, 3, 0, 5, 6, 7, 8], result["age"].to_pylist())
        live_files = {
            os.path.basename(data_file.file_name)
            for split in table.new_read_builder().new_scan().plan().splits()
            for data_file in split.files
        }
        staged_files = {
            os.path.basename(path)
            for path in self._data_files_under(table) - files_before
        }
        self.assertTrue(staged_files.issubset(live_files))
        self.assertFalse(table.list_tags())

    def test_transform_retries_application_exception(self):
        target = self._create()
        self._write(target, pa.table({
            "id": [1, 2],
            "name": ["x", "x"],
            "age": pa.array([0, 0], type=pa.int32()),
        }, schema=self.pa_schema))

        class FailOnce:

            def __init__(self):
                self.calls = 0

            def __call__(self, batch):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("transient transform failure")
                return pa.table({"age": batch.column("id")})

        result = update_by_transform(
            target,
            self.catalog_options,
            read_projection=["id"],
            transform=FailOnce,
            update_cols=["age"],
            rows_per_commit=100,
            ray_remote_args={
                "retry_exceptions": True,
                "max_retries": 3,
            },
        )

        self.assertEqual({"num_updated": 2}, result)
        self.assertEqual(
            [1, 2], self._read(target).sort_by("id")["age"].to_pylist())

    def test_transform_must_preserve_row_count(self):
        target = self._create()
        self._write(target, pa.table({
            "id": [1, 2],
            "name": ["x", "x"],
            "age": pa.array([0, 0], type=pa.int32()),
        }, schema=self.pa_schema))

        def transform(batch):
            return pa.table({
                "age": batch.column("id").slice(0, 0),
            })

        with self.assertRaisesRegex(GroupApplyError, "preserve input row count"):
            update_by_transform(
                target,
                self.catalog_options,
                update_cols=["age"],
                rows_per_commit=2,
                read_projection=["id"],
                transform=transform,
                transform_batch_size=2,
            )

        self.assertEqual([0, 0], self._read(target).sort_by("id")["age"].to_pylist())

    def test_callable_filter_validates_mask(self):
        target = self._create()
        self._write(target, pa.table({
            "id": [1, 2],
            "name": ["x", "x"],
            "age": pa.array([0, 0], type=pa.int32()),
        }, schema=self.pa_schema))

        def transform(batch):
            return pa.table({"age": batch.column("id")})

        for mask in ([True], [1, 0]):
            with self.subTest(mask=mask):
                with self.assertRaisesRegex(
                        GroupApplyError, "one boolean per input row"):
                    update_by_transform(
                        target,
                        self.catalog_options,
                        filter=lambda _batch, value=mask: value,
                        read_projection=["id"],
                        transform=transform,
                        update_cols=["age"],
                        rows_per_commit=100,
                    )

        self.assertEqual(
            [0, 0], self._read(target).sort_by("id")["age"].to_pylist())

    def test_transform_writes_one_file_group_at_a_time(self):
        import importlib
        import pickle

        module = importlib.import_module(
            "pypaimon.ray.data_evolution_merge_join")
        worker = object.__new__(module._RangeTransformUpdateWorker)
        worker._table = object()
        worker._init_error = None
        worker._files_info = object()
        worker._update_cols = ["age"]
        worker._file_groups = lambda group: [(10, None), (20, None)]
        worker._build_updates = lambda first_row_id, _: pa.table({
            "_ROW_ID": [first_row_id, first_row_id + 1],
            "age": pa.array([1, 2], type=pa.int32()),
        })
        write_sizes = []

        class FakeWriter:

            def __init__(self, *args, **kwargs):
                self.commit_messages = []

            def update_columns(self, updates, update_cols):
                write_sizes.append(updates.num_rows)
                self.commit_messages.append(updates.num_rows)

        with mock.patch(
                "pypaimon.write.table_update_by_row_id.TableUpdateByRowId",
                FakeWriter):
            result = worker(pa.table({"unused": []}))

        self.assertEqual([2, 2], write_sizes)
        self.assertEqual([2, 2], pickle.loads(result["msgs_blob"][0].as_py()))
        self.assertEqual(4, result["n_updated"][0].as_py())

    def test_incremental_update_rejects_external_overwrite(self):
        from pypaimon.write.commit.conflict_detection import (
            CommitConflictError,
        )
        from pypaimon.write.table_commit import StreamTableCommit

        target, _, _ = self._three_file_target()
        external = pa.Table.from_pydict(
            {
                "id": [1, 2, 3],
                "name": ["external"] * 3,
                "age": [999] * 3,
            },
            schema=self.pa_schema,
        )
        original_commit = StreamTableCommit.commit
        overwrite_landed = False

        def transform(batch):
            return pa.table({
                "age": [value.as_py() * 100
                        for value in batch.column("id")],
            })

        def commit_then_overwrite(
                stream_commit, messages, commit_identifier):
            nonlocal overwrite_landed
            result = original_commit(
                stream_commit, messages, commit_identifier)
            if not overwrite_landed:
                overwrite_landed = True
                builder = (
                    self.catalog.get_table(target)
                    .new_batch_write_builder()
                    .overwrite()
                )
                writer = builder.new_write()
                writer.write_arrow(external)
                builder.new_commit().commit(writer.prepare_commit())
                writer.close()
            return result

        with mock.patch.object(
                StreamTableCommit, "commit", commit_then_overwrite):
            with self.assertRaisesRegex(
                    CommitConflictError, "Concurrent rewrite"):
                update_by_transform(
                    target,
                    self.catalog_options,
                    update_cols=["age"],
                    num_partitions=1,
                    rows_per_commit=1,
                    read_projection=["id"],
                    transform=transform,
                )

        result = self._read(target).sort_by("id").to_pydict()
        self.assertEqual(["external"] * 3, result["name"])
        self.assertEqual([999] * 3, result["age"])

    def test_incremental_update_rejects_concurrent_schema_changes(self):
        from pypaimon.schema.data_types import AtomicType
        from pypaimon.schema.schema_change import SchemaChange
        from pypaimon.write.commit.conflict_detection import (
            CommitConflictError,
        )
        from pypaimon.write.table_commit import StreamTableCommit

        cases = [
            (SchemaChange.add_column("extra", AtomicType("INT")), False),
            (SchemaChange.drop_column("age"), False),
            (SchemaChange.rename_column("age", "renamed_age"), False),
            (SchemaChange.rename_column("age", "renamed_age"), True),
        ]
        for change, alter_after_commit in cases:
            with self.subTest(
                    change=type(change).__name__,
                    alter_after_commit=alter_after_commit):
                target, table, _ = self._three_file_target()
                base_snapshot_id = (
                    table.snapshot_manager().get_latest_snapshot().id
                )
                original_commit = StreamTableCommit.commit
                altered = False

                def transform(batch):
                    return pa.table({"age": batch.column("id")})

                def alter_then_commit(
                        stream_commit, messages, commit_identifier):
                    nonlocal altered
                    if not altered and not alter_after_commit:
                        altered = True
                        self.catalog.alter_table(target, [change], False)
                    result = original_commit(
                        stream_commit, messages, commit_identifier)
                    if not altered:
                        altered = True
                        self.catalog.alter_table(target, [change], False)
                    return result

                with mock.patch.object(
                        StreamTableCommit, "commit", alter_then_commit):
                    with self.assertRaisesRegex(
                            CommitConflictError, "Target schema changed"):
                        update_by_transform(
                            target,
                            self.catalog_options,
                            update_cols=["age"],
                            num_partitions=1,
                            rows_per_commit=1,
                            read_projection=["id"],
                            transform=transform,
                        )

                self.assertEqual(
                    base_snapshot_id + int(alter_after_commit),
                    table.snapshot_manager().get_latest_snapshot().id,
                )
                self.assertFalse(table.list_tags())

    def test_transform_rejects_concurrent_input_changes(self):
        from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
        from pypaimon.write.commit.conflict_detection import (
            CommitConflictError,
        )
        from pypaimon.write.table_commit import StreamTableCommit
        from pypaimon.write.table_update_by_row_id import TableUpdateByRowId

        cases = [
            ("read", ["name"], None),
            ("filter", ["id"], "name = 'a'"),
        ]
        for _, read_projection, transform_filter in cases:
            with self.subTest(read_projection=read_projection,
                              filter=transform_filter):
                target, _, row_ids = self._three_file_target()
                original_commit = StreamTableCommit.commit
                external_landed = False

                def transform(batch):
                    if "name" in batch.column_names:
                        values = [
                            len(value.as_py())
                            for value in batch.column("name")
                        ]
                    else:
                        values = [
                            value.as_py() * 10
                            for value in batch.column("id")
                        ]
                    return pa.table({"age": values})

                def commit_after_input_change(
                        stream_commit, messages, commit_identifier):
                    nonlocal external_landed
                    if not external_landed:
                        external_landed = True
                        external_table = self.catalog.get_table(target)
                        updater = TableUpdateByRowId(
                            external_table,
                            "_external_input_update_",
                            BATCH_COMMIT_IDENTIFIER,
                        )
                        external_messages = updater.update_columns(
                            pa.table({
                                "_ROW_ID": pa.array(
                                    [row_ids[1]], type=pa.int64()),
                                "name": ["long"],
                            }),
                            ["name"],
                        )
                        external_commit = (
                            external_table.new_stream_write_builder()
                            .new_commit()
                        )
                        try:
                            original_commit(
                                external_commit,
                                external_messages,
                                BATCH_COMMIT_IDENTIFIER,
                            )
                        finally:
                            external_commit.close()
                    return original_commit(
                        stream_commit, messages, commit_identifier)

                with mock.patch.object(
                        StreamTableCommit,
                        "commit",
                        commit_after_input_change):
                    with self.assertRaisesRegex(
                            CommitConflictError, "multiple 'MERGE INTO'"):
                        update_by_transform(
                            target,
                            self.catalog_options,
                            read_projection=read_projection,
                            transform=transform,
                            filter=transform_filter,
                            update_cols=["age"],
                            rows_per_commit=100,
                            num_partitions=1,
                        )

                result = self._read(target).sort_by("id").to_pydict()
                self.assertEqual(["long", "a", "a"], result["name"])
                self.assertEqual([0, 0, 0], result["age"])

    def test_range_failure_aborts_files_from_earlier_group(self):
        target = self._create()
        for row_id in [1, 2]:
            self._write(target, pa.Table.from_pydict(
                {"id": [row_id], "name": ["a"], "age": [0]},
                schema=self.pa_schema,
            ))
        table = self.catalog.get_table(target)
        files_before = self._data_files_under(table)

        def transform(batch):
            if batch.column("id")[0].as_py() == 2:
                raise RuntimeError("forced range failure")
            return pa.table({
                "age": [value.as_py() * 100
                        for value in batch.column("id")],
            })

        with self.assertRaisesRegex(GroupApplyError, "forced range failure"):
            update_by_transform(
                target,
                self.catalog_options,
                update_cols=["age"],
                num_partitions=1,
                rows_per_commit=100,
                read_projection=["id"],
                transform=transform,
            )

        self.assertEqual(files_before, self._data_files_under(table))
        self.assertEqual(
            [0, 0],
            self._read(target).sort_by("id")["age"].to_pylist(),
        )

    def test_incremental_commit_failure_aborts_later_groups(self):
        import importlib

        module = importlib.import_module("pypaimon.ray.update_by_transform")
        aborted = []
        commit_calls = []
        retry_error = ValueError("retry failed")
        commit_error = RuntimeError("commit failed")
        commit_error.__cause__ = retry_error

        class FakeCommit:
            def add_commit_callback(self, callback):
                pass

            def commit(self, messages, commit_identifier):
                commit_calls.append((list(messages), commit_identifier))
                raise commit_error

            def close(self):
                pass

        class FakeBuilder:
            def new_commit(self):
                return FakeCommit()

        class FakeTable:
            def new_stream_write_builder(self):
                return FakeBuilder()

        committer = module._IncrementalUpdateCommitter(FakeTable())
        with mock.patch.object(
                module,
                "_abort_pending_update_messages",
                side_effect=lambda table, messages: aborted.append(list(messages))):
            committer.add_range(["group-1"], 1, [])
            committer.add_range(["group-2"], 1, [])
            with self.assertRaisesRegex(RuntimeError, "commit failed") as raised:
                committer.finish()
            committer.close()

        self.assertIs(commit_error, raised.exception)
        self.assertIs(retry_error, raised.exception.__cause__)
        self.assertEqual([(["group-1"], 1)], commit_calls)
        self.assertEqual([["group-2"]], aborted)

    def test_incremental_commit_conflict_aborts_buffered_group_files(self):
        from pypaimon.write.commit.conflict_detection import CommitConflictError
        from pypaimon.write.file_store_commit import FileStoreCommit

        target, table, _ = self._three_file_target()
        base_snapshot_id = table.snapshot_manager().get_latest_snapshot().id
        files_before = self._data_files_under(table)

        def transform(batch):
            return pa.table({"age": batch.column("id")})

        with mock.patch.object(
                FileStoreCommit,
                "commit",
                side_effect=CommitConflictError("forced conflict")):
            with self.assertRaisesRegex(CommitConflictError, "forced conflict"):
                update_by_transform(
                    target,
                    self.catalog_options,
                    update_cols=["age"],
                    num_partitions=1,
                    rows_per_commit=1,
                    read_projection=["id"],
                    transform=transform,
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

    def test_transform_rejects_primary_key_table(self):
        target = "default.u_{}".format(uuid.uuid4().hex[:8])
        self.catalog.create_table(
            target,
            Schema.from_pyarrow_schema(
                self.pa_schema,
                primary_keys=["id"],
                options=dict(self.de_options, bucket="1"),
            ),
            False,
        )
        with self.assertRaisesRegex(ValueError, "non-primary-key"):
            update_by_transform(
                target,
                self.catalog_options,
                read_projection=["id"],
                transform=lambda batch: pa.table({
                    "age": batch.column("id"),
                }),
                update_cols=["age"],
                rows_per_commit=100,
            )

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

    def test_rejects_invalid_options(self):
        target = self._create()
        self._write(target, pa.Table.from_pydict(
            {"id": [1], "name": ["a"], "age": [1]}, schema=self.pa_schema))
        src = pa.table({"_ROW_ID": [0], "age": [9]},
                       schema=pa.schema([("_ROW_ID", pa.int64()), ("age", pa.int32())]))
        with self.assertRaises(ValueError):
            update_by_row_id(target, src, self.catalog_options, update_cols=["nope"])
        with self.assertRaises(ValueError):
            update_by_row_id(target, src, self.catalog_options, update_cols=[])

        for value in [0, -1, True, 1.5, "2"]:
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                        ValueError, "must be a positive integer"):
                    update_by_transform(
                        target,
                        self.catalog_options,
                        update_cols=["age"],
                        rows_per_commit=value,
                        read_projection=["id"],
                        transform=lambda batch: batch,
                    )

        with self.assertRaisesRegex(ValueError, "read_projection"):
            update_by_transform(
                target,
                self.catalog_options,
                update_cols=["age"],
                rows_per_commit=1,
                read_projection=[],
                transform=lambda batch: batch,
            )
        with self.assertRaisesRegex(ValueError, "keeps _ROW_ID internal"):
            update_by_transform(
                target,
                self.catalog_options,
                update_cols=["age"],
                rows_per_commit=1,
                read_projection=["_ROW_ID", "id"],
                transform=lambda batch: batch,
            )
        with self.assertRaisesRegex(ValueError, "filter must be"):
            update_by_transform(
                target,
                self.catalog_options,
                update_cols=["age"],
                rows_per_commit=1,
                filter=object(),
                read_projection=["id"],
                transform=lambda batch: batch,
            )

    def _run_with_fake_commit(self, *, recorder=None, new_commit_errors=None,
                              commit_error=None, close_error=None):
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
            def commit(self, msgs):
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
                                  return_value=(recorder["msgs"], 3, [])):
            recorder["result"] = m.update_by_row_id(
                "default.fake",
                FakeSource(),
                self.catalog_options,
                update_cols=["age"],
            )
        return recorder


if __name__ == "__main__":
    unittest.main()
