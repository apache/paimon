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
from pypaimon.ray import plan_row_id_ranges, process_row_id_ranges


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

    def _read(self, target):
        table = self.catalog.get_table(target)
        read = table.new_read_builder()
        return read.new_read().to_arrow(
            read.new_scan().plan().splits()).sort_by("id")

    def _row_ids(self, target):
        table = self.catalog.get_table(target)
        read = table.new_read_builder().with_projection(["id", "_ROW_ID"])
        rows = read.new_read().to_arrow(
            read.new_scan().plan().splits()).sort_by("id")
        return rows["_ROW_ID"].to_pylist()

    def _write_chunks(self, target, chunks):
        for chunk in chunks:
            self._write(target, pa.table({
                "id": chunk,
                "text": ["x"] * len(chunk),
                "embedding": pa.array([0] * len(chunk), pa.int32()),
            }, schema=self.schema))

    def test_processes_file_group_aligned_ranges_with_actors(self):
        target = self._create()
        self._write_chunks(target, ([1, 2, 3], [4], [5, 6, 7]))
        seen = []

        class Infer:
            def __call__(self, batch):
                return pa.table({
                    "_ROW_ID": batch["_ROW_ID"],
                    "embedding": batch["id"],
                })

        def process(context):
            seen.append((
                context.sequence_number,
                context.range_start,
                context.range_end,
                context.estimated_rows,
            ))
            source = context.read(["id"], num_partitions=2)
            updates = source.map_batches(
                Infer,
                concurrency=2,
                batch_format="pyarrow",
            )
            context.update_by_row_id(
                updates, ["embedding"], num_partitions=2)

        result = process_row_id_ranges(
            target,
            self.catalog_options,
            target_rows_per_range=4,
            processor=process,
        )

        self.assertEqual(
            [(0, 0, 3, 4), (1, 4, 6, 3)], seen)
        self.assertEqual(
            {"num_ranges": 2, "num_updated": 7}, result)
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

        def process(context):
            source = context.read(["id"])
            updates = source.map_batches(
                lambda batch: pa.table({
                    "_ROW_ID": batch["_ROW_ID"],
                    "text": ["updated"] * batch.num_rows,
                }),
                batch_format="pyarrow",
            )
            context.update_by_row_id(updates, ["text"])

        result = process_row_id_ranges(
            target,
            self.catalog_options,
            target_rows_per_range=64,
            processor=process,
        )
        self.assertEqual({"num_ranges": 1, "num_updated": rows}, result)
        read = table.new_read_builder().with_projection(["id", "text"])
        actual = read.new_read().to_arrow(
            read.new_scan().plan().splits()).sort_by("id")
        self.assertEqual(["updated"] * rows,
                         actual["text"].to_pylist())

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

        def process(context):
            source = context.read(
                ["text"], filter="id >= 2")

            def transform(batch):
                return pa.table({
                    "_ROW_ID": batch["_ROW_ID"],
                    "embedding": [
                        len(value.as_py()) for value in batch["text"]
                    ],
                })

            context.update_by_row_id(
                source.map_batches(transform, batch_format="pyarrow"),
                ["embedding"],
            )

        process_row_id_ranges(
            target,
            self.catalog_options,
            target_rows_per_range=100,
            processor=process,
        )
        self.assertEqual(
            [None, 2, 3],
            self._read(target)["embedding"].to_pylist(),
        )

    def test_failure_keeps_only_earlier_ranges(self):
        target = self._create()
        self._write_chunks(target, ([1], [2], [3]))
        table = self.catalog.get_table(target)
        base = table.snapshot_manager().get_latest_snapshot().id

        def process(context):
            if context.sequence_number == 1:
                raise RuntimeError("stop after first range")
            source = context.read(["id"])
            updates = source.map_batches(
                lambda batch: pa.table({
                    "_ROW_ID": batch["_ROW_ID"],
                    "embedding": batch["id"],
                }),
                batch_format="pyarrow",
            )
            context.update_by_row_id(updates, ["embedding"])

        with self.assertRaisesRegex(
                RuntimeError, "stop after first range"):
            process_row_id_ranges(
                target,
                self.catalog_options,
                target_rows_per_range=1,
                processor=process,
            )

        self.assertEqual(
            base + 1, table.snapshot_manager().get_latest_snapshot().id)
        self.assertEqual(
            [1, 0, 0], self._read(target)["embedding"].to_pylist())
        self.assertFalse(table.list_tags())

    def test_rejects_concurrent_changes_to_read_columns(self):
        from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
        from pypaimon.write.commit.conflict_detection import (
            CommitConflictError,
        )
        from pypaimon.write.table_update_by_row_id import TableUpdateByRowId

        target = self._create()
        self._write_chunks(target, ([1], [2]))
        table = self.catalog.get_table(target)

        with plan_row_id_ranges(
            target,
            self.catalog_options,
            target_rows_per_range=1,
        ) as ranges:
            context = next(iter(ranges))
            source = context.read(["text"])
            row_id = source.take(1)[0]["_ROW_ID"]

            external = TableUpdateByRowId(
                table, "external", BATCH_COMMIT_IDENTIFIER)
            messages = external.update_columns(pa.table({
                "_ROW_ID": [row_id],
                "text": ["new"],
            }), ["text"])
            table.new_batch_write_builder().new_commit().commit(messages)

            updates = source.map_batches(
                lambda batch: pa.table({
                    "_ROW_ID": batch["_ROW_ID"],
                    "embedding": [len(value.as_py()) for value in batch["text"]],
                }),
                batch_format="pyarrow",
            )
            with self.assertRaisesRegex(
                    CommitConflictError, "conflict"):
                context.update_by_row_id(updates, ["embedding"])

    def test_rejects_concurrent_schema_change(self):
        from pypaimon.schema.data_types import AtomicType
        from pypaimon.schema.schema_change import SchemaChange
        from pypaimon.write.commit.conflict_detection import (
            CommitConflictError,
        )

        target = self._create()
        self._write_chunks(target, ([1],))
        with plan_row_id_ranges(
            target,
            self.catalog_options,
            target_rows_per_range=1,
        ) as ranges:
            context = next(iter(ranges))
            source = context.read(["id"])
            self.catalog.alter_table(
                target,
                [SchemaChange.add_column("extra", AtomicType("INT"))],
                False,
            )
            with self.assertRaisesRegex(
                    CommitConflictError, "schema changed"):
                context.update_by_row_id(
                    source.map_batches(
                        lambda batch: pa.table({
                            "_ROW_ID": batch["_ROW_ID"],
                            "embedding": batch["id"],
                        }),
                        batch_format="pyarrow",
                    ),
                    ["embedding"],
                )

    def test_rejects_row_ids_from_another_range(self):
        target = self._create()
        self._write_chunks(target, ([1], [2]))
        row_ids = self._row_ids(target)

        with plan_row_id_ranges(
            target,
            self.catalog_options,
            target_rows_per_range=1,
        ) as ranges:
            first = next(iter(ranges))
            updates = pa.table({
                "_ROW_ID": [row_ids[1]],
                "embedding": pa.array([2], pa.int32()),
            })
            with self.assertRaises(Exception):
                first.update_by_row_id(updates, ["embedding"])


if __name__ == "__main__":
    unittest.main()
