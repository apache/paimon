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
import unittest
import uuid

import pyarrow as pa
import pytest

pytest.importorskip("pypaimon")
ray = pytest.importorskip("ray")

from pypaimon import CatalogFactory, Schema
from pypaimon.ray import (
    process_row_id_ranges,
    update_by_row_id,
)
from pypaimon.ray.row_id_ranges import (
    _plan_row_id_ranges,
    _read_row_id_range,
    _update_by_row_id_from_plan,
)


class RayRowIdRangesTest(unittest.TestCase):

    schema = pa.schema([
        ("id", pa.int32()),
        ("text", pa.string()),
        ("embedding", pa.int32()),
    ])
    options = {
        "row-tracking.enabled": "true",
        "data-evolution.enabled": "true",
    }

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.catalog_options = {
            "warehouse": os.path.join(cls.tempdir, "warehouse")}
        cls.catalog = CatalogFactory.create(cls.catalog_options)
        cls.catalog.create_database("default", True)
        if not ray.is_initialized():
            ray.init(num_cpus=2)

    @classmethod
    def tearDownClass(cls):
        if ray.is_initialized():
            ray.shutdown()
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create(self, schema=None, options=None):
        target = "default.r_{}".format(uuid.uuid4().hex[:8])
        self.catalog.create_table(
            target,
            Schema.from_pyarrow_schema(
                schema or self.schema,
                options=self.options if options is None else options,
            ),
            False,
        )
        return target

    def _write(self, target, data):
        table = self.catalog.get_table(target)
        builder = table.new_batch_write_builder()
        writer = builder.new_write()
        writer.write_arrow(data)
        builder.new_commit().commit(writer.prepare_commit())
        writer.close()

    def _write_chunks(self, target, chunks):
        for chunk in chunks:
            self._write(target, pa.table({
                "id": chunk,
                "text": ["x"] * len(chunk),
                "embedding": pa.array([0] * len(chunk), pa.int32()),
            }, schema=self.schema))

    def _read(self, target):
        table = self.catalog.get_table(target)
        read = table.new_read_builder()
        return read.new_read().to_arrow(
            read.new_scan().plan().splits()).sort_by("id")

    def _row_ids(self, target):
        table = self.catalog.get_table(target)
        read = table.new_read_builder().with_projection(["id", "_ROW_ID"])
        return read.new_read().to_arrow(
            read.new_scan().plan().splits()).sort_by("id")["_ROW_ID"].to_pylist()

    @staticmethod
    def _embedding_updates(row_range):
        source = _read_row_id_range(row_range, ["id"], num_partitions=2)
        return source.map_batches(
            lambda batch: pa.table({
                "_ROW_ID": batch["_ROW_ID"],
                "embedding": batch["id"],
            }),
            batch_format="pyarrow",
        )

    def test_processes_file_group_aligned_ranges_with_actors(self):
        target = self._create()
        self._write_chunks(target, ([1, 2, 3], [4], [5, 6, 7]))

        with _plan_row_id_ranges(
            target,
            self.catalog_options,
            target_rows_per_range=4,
        ) as plan:
            self.assertEqual(
                [(0, 0, 3, 4), (1, 4, 6, 3)],
                [(range_.sequence_number,
                  range_.range_start,
                  range_.range_end,
                  range_.estimated_rows) for range_ in plan],
            )

        class Infer:
            def __call__(self, batch):
                return pa.table({
                    "_ROW_ID": batch["_ROW_ID"],
                    "embedding": batch["id"],
                })

        def process(source):
            return source.map_batches(
                Infer,
                concurrency=1,
                num_cpus=0.5,
                batch_format="pyarrow",
            )

        result = process_row_id_ranges(
            target,
            self.catalog_options,
            target_rows_per_range=4,
            read_projection=["id"],
            update_cols=["embedding"],
            processor=process,
            num_partitions=2,
        )

        self.assertEqual({"num_ranges": 2, "num_updated": 7}, result)
        self.assertEqual(
            [1, 2, 3, 4, 5, 6, 7],
            self._read(target)["embedding"].to_pylist(),
        )
        self.assertFalse(self.catalog.get_table(target).list_tags())

    def test_rolling_vector_files_stay_in_one_range(self):
        schema = pa.schema([
            ("id", pa.int32()),
            ("text", pa.string()),
            ("embedding", pa.list_(pa.float32(), 16)),
        ])
        target = self._create(schema, {
            **self.options,
            "vector.file.format": "parquet",
            "vector.target-file-size": "1KB",
        })
        rows = 128
        self._write(target, pa.Table.from_arrays([
            pa.array(range(rows), pa.int32()),
            pa.array(["old"] * rows),
            pa.FixedSizeListArray.from_arrays(
                pa.array([0.1] * rows * 16, pa.float32()), 16),
        ], schema=schema))

        table = self.catalog.get_table(target)
        files = [
            file
            for split in table.new_read_builder().new_scan(
            ).plan_for_write().splits()
            for file in split.files
        ]
        from pypaimon.manifest.schema.data_file_meta import DataFileMeta
        self.assertGreater(sum(
            DataFileMeta.is_vector_file(file.file_name) for file in files), 1)

        with _plan_row_id_ranges(
            target,
            self.catalog_options,
            target_rows_per_range=64,
        ) as plan:
            ranges = list(plan)
            self.assertEqual(1, len(ranges))
            source = _read_row_id_range(ranges[0], ["id"])
            updates = source.map_batches(
                lambda batch: pa.table({
                    "_ROW_ID": batch["_ROW_ID"],
                    "text": ["updated"] * batch.num_rows,
                }),
                batch_format="pyarrow",
            )
            result = _update_by_row_id_from_plan(
                ranges[0], updates, ["text"])

        self.assertEqual({"num_updated": rows}, result)
        read = table.new_read_builder().with_projection(["id", "text"])
        actual = read.new_read().to_arrow(
            read.new_scan().plan().splits()).sort_by("id")
        self.assertEqual(["updated"] * rows, actual["text"].to_pylist())

    def test_reads_existing_parquet_delta(self):
        target = self._create()
        self._write_chunks(target, (list(range(10)),))
        row_ids = self._row_ids(target)
        update_by_row_id(
            target,
            pa.table({
                "_ROW_ID": row_ids[3:6],
                "text": ["new"] * 3,
            }, schema=pa.schema([
                ("_ROW_ID", pa.int64()),
                ("text", pa.string()),
            ])),
            self.catalog_options,
            update_cols=["text"],
        )

        with _plan_row_id_ranges(
            target,
            self.catalog_options,
            target_rows_per_range=100,
        ) as plan:
            rows = sorted(
                _read_row_id_range(next(iter(plan)), ["id", "text"]).take_all(),
                key=lambda row: row["id"],
            )

        self.assertEqual(
            ["x"] * 3 + ["new"] * 3 + ["x"] * 4,
            [row["text"] for row in rows],
        )

    def test_reads_current_schema_from_pinned_snapshot(self):
        from pypaimon.schema.data_types import AtomicType
        from pypaimon.schema.schema_change import SchemaChange

        source_schema = pa.schema([
            ("id", pa.int32()),
            ("text", pa.string()),
        ])
        target = self._create(source_schema)
        self._write(target, pa.table({
            "id": [1, 2, 3],
            "text": ["a", "bb", "ccc"],
        }, schema=source_schema))
        self.catalog.alter_table(
            target,
            [SchemaChange.add_column("embedding", AtomicType("INT"))],
            False,
        )

        def process(source):
            def transform(batch):
                return pa.table({
                    "_ROW_ID": batch["_ROW_ID"],
                    "embedding": [
                        len(value.as_py()) for value in batch["text"]
                    ],
                })

            return source.map_batches(transform, batch_format="pyarrow")

        process_row_id_ranges(
            target,
            self.catalog_options,
            target_rows_per_range=100,
            read_projection=["text"],
            update_cols=["embedding"],
            filter="id >= 2",
            processor=process,
        )
        self.assertEqual(
            [None, 2, 3], self._read(target)["embedding"].to_pylist())

    def test_failure_keeps_only_earlier_ranges(self):
        target = self._create()
        self._write_chunks(target, ([1], [2], [3]))
        table = self.catalog.get_table(target)
        base = table.snapshot_manager().get_latest_snapshot().id

        calls = 0

        def process(source):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("stop after first range")
            return source.map_batches(
                lambda batch: pa.table({
                    "_ROW_ID": batch["_ROW_ID"],
                    "embedding": batch["id"],
                }),
                batch_format="pyarrow",
            )

        with self.assertRaisesRegex(
                RuntimeError, "stop after first range"):
            process_row_id_ranges(
                target,
                self.catalog_options,
                target_rows_per_range=1,
                read_projection=["id"],
                update_cols=["embedding"],
                processor=process,
            )

        self.assertEqual(
            base + 1, table.snapshot_manager().get_latest_snapshot().id)
        self.assertEqual(
            [1, 0, 0], self._read(target)["embedding"].to_pylist())
        self.assertFalse(table.list_tags())

    def test_processor_can_ignore_a_range(self):
        target = self._create()
        self._write_chunks(target, ([1], [2]))

        calls = 0

        def process(source):
            nonlocal calls
            calls += 1
            if calls == 1:
                return None
            return source.map_batches(
                lambda batch: pa.table({
                    "_ROW_ID": batch["_ROW_ID"],
                    "embedding": batch["id"],
                }),
                batch_format="pyarrow",
            )

        result = process_row_id_ranges(
            target,
            self.catalog_options,
            target_rows_per_range=1,
            read_projection=["id"],
            update_cols=["embedding"],
            processor=process,
        )

        self.assertEqual({"num_ranges": 2, "num_updated": 1}, result)
        self.assertEqual(
            [0, 2], self._read(target)["embedding"].to_pylist())

    def test_uses_normal_write_column_conflicts(self):
        target = self._create()
        self._write_chunks(target, ([1],))

        with _plan_row_id_ranges(
            target,
            self.catalog_options,
            target_rows_per_range=1,
        ) as plan:
            row_range = next(iter(plan))
            stale_updates = self._embedding_updates(row_range)
            update_by_row_id(
                target,
                pa.table({
                    "_ROW_ID": self._row_ids(target),
                    "embedding": pa.array([10], pa.int32()),
                }),
                self.catalog_options,
                update_cols=["embedding"],
            )
            with self.assertRaisesRegex(Exception, "conflict"):
                _update_by_row_id_from_plan(
                    row_range, stale_updates, ["embedding"])

        self.assertEqual([10], self._read(target)["embedding"].to_pylist())

    def test_rejects_row_ids_from_another_range(self):
        target = self._create()
        self._write_chunks(target, ([1], [2]))
        row_ids = self._row_ids(target)

        with _plan_row_id_ranges(
            target,
            self.catalog_options,
            target_rows_per_range=1,
        ) as plan:
            first = next(iter(plan))
            with self.assertRaises(Exception):
                _update_by_row_id_from_plan(
                    first,
                    pa.table({
                        "_ROW_ID": [row_ids[1]],
                        "embedding": pa.array([2], pa.int32()),
                    }),
                    ["embedding"],
                )

    def test_rejects_schema_change_after_planning(self):
        from pypaimon.schema.schema_change import SchemaChange
        from pypaimon.write.commit.conflict_detection import (
            CommitConflictError,
        )

        target = self._create()
        self._write_chunks(target, ([1, 2],))
        table = self.catalog.get_table(target)
        base = table.snapshot_manager().get_latest_snapshot().id
        plan = _plan_row_id_ranges(
            target,
            self.catalog_options,
            target_rows_per_range=10,
        )
        row_range = next(iter(plan))
        row_ids = self._row_ids(target)
        self.catalog.alter_table(
            target, [SchemaChange.drop_column("embedding")], False)

        with self.assertRaisesRegex(CommitConflictError, "schema changed"):
            _update_by_row_id_from_plan(
                row_range,
                pa.table({
                    "_ROW_ID": row_ids,
                    "embedding": pa.array([1, 2], pa.int32()),
                }),
                ["embedding"],
            )
        plan.close()
        self.assertEqual(
            base, table.snapshot_manager().get_latest_snapshot().id)

    def test_closed_plan_rejects_ranges_and_lazy_reads(self):
        target = self._create()
        self._write_chunks(target, ([1, 2],))
        plan = _plan_row_id_ranges(
            target,
            self.catalog_options,
            target_rows_per_range=10,
        )
        row_range = next(iter(plan))
        lazy_read = _read_row_id_range(row_range, ["id"])
        plan.close()

        with self.assertRaisesRegex(RuntimeError, "plan is closed"):
            _read_row_id_range(row_range, ["id"])
        with self.assertRaisesRegex(RuntimeError, "plan is closed"):
            _update_by_row_id_from_plan(
                row_range,
                pa.table({
                    "_ROW_ID": [0],
                    "embedding": pa.array([1], pa.int32()),
                }),
                ["embedding"],
            )
        with self.assertRaisesRegex(Exception, "plan is closed"):
            lazy_read.take_all()


if __name__ == "__main__":
    unittest.main()
