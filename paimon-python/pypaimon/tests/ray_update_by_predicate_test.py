# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import shutil
import tempfile
import unittest
import uuid
from unittest import mock

import pyarrow as pa
import pyarrow.compute as pc
import pytest

ray = pytest.importorskip("ray")

from pypaimon import CatalogFactory, Schema
from pypaimon.data.generic_variant import GenericVariant
from pypaimon.data.variant_path import variant_get, variant_replace
from pypaimon.ray import update_by_predicate, update_by_row_id
from pypaimon.ray.update_by_predicate import _apply_transform, _validate


class RayUpdateByPredicateTest(unittest.TestCase):

    options = {
        "data-evolution.enabled": "true",
        "row-tracking.enabled": "true",
        "target-file-row-num": "2",
    }

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.catalog_options = {
            "warehouse": os.path.join(cls.tempdir, "warehouse")
        }
        cls.catalog = CatalogFactory.create(cls.catalog_options)
        cls.catalog.create_database("default", True)
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, num_cpus=2)

    @classmethod
    def tearDownClass(cls):
        if ray.is_initialized():
            ray.shutdown()
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create(self, schema):
        target = "default.predicate_{}".format(uuid.uuid4().hex[:8])
        self.catalog.create_table(
            target,
            Schema.from_pyarrow_schema(schema, options=self.options),
            False,
        )
        return target

    def _write(self, target, data):
        builder = self.catalog.get_table(target).new_batch_write_builder()
        writer = builder.new_write()
        writer.write_arrow(data)
        builder.new_commit().commit(writer.prepare_commit())
        writer.close()

    def _read(self, target):
        table = self.catalog.get_table(target)
        builder = table.new_read_builder()
        return builder.new_read().to_arrow(builder.new_scan().plan().splits())

    def test_transforms_matching_rows_in_sequential_commits(self):
        schema = pa.schema([
            ("id", pa.int32()),
            ("age", pa.int32()),
        ])
        target = self._create(schema)
        self._write(target, pa.table({
            "id": list(range(6)),
            "age": [10, 20, 30, 40, 50, 60],
        }, schema=schema))
        table = self.catalog.get_table(target)
        before = table.snapshot_manager().get_latest_snapshot().id
        predicate = (
            table.new_read_builder().new_predicate_builder()
            .greater_or_equal("id", 2)
        )

        def double_age(batch):
            return pa.table({"age": pc.multiply(batch["age"], 2)})

        result = update_by_predicate(
            target,
            predicate,
            double_age,
            self.catalog_options,
            read_columns=["age"],
            update_cols=["age"],
            rows_per_commit=3,
            batch_size=1,
            num_partitions=1,
        )

        self.assertEqual({"num_updated": 4}, result)
        self.assertEqual(
            [10, 20, 60, 80, 100, 120],
            self._read(target).sort_by("id")["age"].to_pylist(),
        )
        after = self.catalog.get_table(
            target
        ).snapshot_manager().get_latest_snapshot().id
        self.assertEqual(before + 2, after)

    def test_updates_variant_from_its_existing_value(self):
        variant_type = pa.struct([
            pa.field("value", pa.binary(), nullable=False),
            pa.field("metadata", pa.binary(), nullable=False),
        ])
        schema = pa.schema([
            ("id", pa.int32()),
            ("payload", variant_type),
        ])
        target = self._create(schema)
        payload = GenericVariant.to_arrow_array([
            GenericVariant.from_python({"x": value})
            for value in [1.0, 2.0, 3.0]
        ])
        self._write(target, pa.table({
            "id": [1, 2, 3],
            "payload": payload,
        }, schema=schema))
        table = self.catalog.get_table(target)
        predicate = (
            table.new_read_builder().new_predicate_builder()
            .greater_than("id", 1)
        )

        def negate_x(batch):
            payload = batch["payload"]
            values = variant_get(payload, "$.x", pa.float64())
            return pa.table({
                "payload": variant_replace(
                    payload, "$.x", pc.negate(values)
                )
            })

        result = update_by_predicate(
            target,
            predicate,
            negate_x,
            self.catalog_options,
            read_columns=["payload"],
            update_cols=["payload"],
            rows_per_commit=2,
            batch_size=2,
            num_partitions=1,
        )

        self.assertEqual({"num_updated": 2}, result)
        rows = self._read(target).sort_by("id")["payload"].to_pylist()
        self.assertEqual(
            [{"x": 1.0}, {"x": -2.0}, {"x": -3.0}],
            [
                GenericVariant.from_arrow_struct(value).to_python()
                for value in rows
            ],
        )

    def test_no_match_is_noop(self):
        schema = pa.schema([("id", pa.int32()), ("age", pa.int32())])
        target = self._create(schema)
        self._write(target, pa.table({
            "id": [1, 2],
            "age": [10, 20],
        }, schema=schema))
        table = self.catalog.get_table(target)
        before = table.snapshot_manager().get_latest_snapshot().id
        predicate = (
            table.new_read_builder().new_predicate_builder()
            .greater_than("id", 100)
        )

        def must_not_run(batch):
            raise AssertionError("empty input must not call transform")

        result = update_by_predicate(
            target,
            predicate,
            must_not_run,
            self.catalog_options,
            read_columns=["age"],
            update_cols=["age"],
            rows_per_commit=1,
            num_partitions=1,
        )

        self.assertEqual({"num_updated": 0}, result)
        self.assertEqual(
            before,
            self.catalog.get_table(
                target
            ).snapshot_manager().get_latest_snapshot().id,
        )

    def test_later_failure_keeps_completed_commit(self):
        schema = pa.schema([("id", pa.int32()), ("age", pa.int32())])
        target = self._create(schema)
        self._write(target, pa.table({
            "id": [0, 1, 2, 3],
            "age": [10, 20, 30, 40],
        }, schema=schema))

        def fail_second_range(batch):
            if batch["age"][0].as_py() >= 30:
                raise RuntimeError("transform failed")
            return pa.table({"age": pc.add(batch["age"], 1)})

        with self.assertRaises(Exception):
            update_by_predicate(
                target,
                None,
                fail_second_range,
                self.catalog_options,
                read_columns=["age"],
                update_cols=["age"],
                rows_per_commit=2,
                num_partitions=1,
            )

        self.assertEqual(
            [11, 21, 30, 40],
            self._read(target).sort_by("id")["age"].to_pylist(),
        )

    def test_transform_contract_is_checked(self):
        batch = pa.table({"_ROW_ID": [0], "age": [1]})
        schema = pa.schema([
            ("_ROW_ID", pa.int64()),
            ("age", pa.int64()),
        ])
        kwargs = {
            "read_columns": ["age"],
            "update_cols": ["age"],
            "update_schema": schema,
        }

        with self.assertRaisesRegex(TypeError, "pyarrow.Table"):
            _apply_transform(
                batch, transform=lambda value: value["age"], **kwargs
            )
        with self.assertRaisesRegex(ValueError, "missing update columns"):
            _apply_transform(
                batch, transform=lambda value: pa.table({"other": [1]}),
                **kwargs
            )
        with self.assertRaisesRegex(ValueError, "preserve.*row count"):
            _apply_transform(
                batch, transform=lambda value: pa.table({"age": [1, 2]}),
                **kwargs
            )

        table = type("Table", (), {"field_names": ["text", "embedding"]})()
        with self.assertRaisesRegex(ValueError, "conflict detection"):
            _validate(
                table,
                lambda value: value,
                ["text"],
                ["embedding"],
                None,
            )

    def test_uses_read_snapshot_as_conflict_baseline(self):
        import importlib

        update_module = importlib.import_module(
            "pypaimon.ray.update_by_predicate"
        )

        schema = pa.schema([("id", pa.int32()), ("age", pa.int32())])
        target = self._create(schema)
        self._write(target, pa.table({
            "id": [1, 2, 3, 4],
            "age": [10, 20, 30, 40],
        }, schema=schema))
        snapshot_id = self.catalog.get_table(
            target
        ).snapshot_manager().get_latest_snapshot().id
        baselines = []

        def capture(*args, **kwargs):
            baselines.append(kwargs["base_snapshot_id"])
            return {"num_updated": 0}

        with mock.patch.object(update_module, "_update_by_row_id", capture):
            update_by_predicate(
                target,
                None,
                lambda batch: batch,
                self.catalog_options,
                read_columns=["age"],
                update_cols=["age"],
                rows_per_commit=2,
            )

        self.assertEqual([snapshot_id, snapshot_id], baselines)

    def test_concurrent_update_after_read_plan_conflicts(self):
        import importlib

        update_module = importlib.import_module(
            "pypaimon.ray.update_by_predicate"
        )
        schema = pa.schema([("id", pa.int32()), ("age", pa.int32())])
        target = self._create(schema)
        self._write(target, pa.table({
            "id": [1, 2],
            "age": [10, 20],
        }, schema=schema))
        table = self.catalog.get_table(target)
        read_builder = table.new_read_builder().with_projection(
            ["id", "_ROW_ID"]
        )
        row_ids = read_builder.new_read().to_arrow(
            read_builder.new_scan().plan().splits()
        )
        first_row_id = dict(zip(
            row_ids["id"].to_pylist(), row_ids["_ROW_ID"].to_pylist()
        ))[1]
        original_update = update_module._update_by_row_id
        injected = []

        def inject_update(*args, **kwargs):
            if not injected:
                injected.append(True)
                update_by_row_id(
                    target,
                    pa.table({
                        "_ROW_ID": pa.array([first_row_id], pa.int64()),
                        "age": pa.array([99], pa.int32()),
                    }),
                    self.catalog_options,
                    update_cols=["age"],
                    num_partitions=1,
                )
            return original_update(*args, **kwargs)

        with mock.patch.object(
            update_module, "_update_by_row_id", inject_update
        ), self.assertRaises(Exception):
            update_by_predicate(
                target,
                None,
                lambda batch: pa.table({
                    "age": pc.multiply(batch["age"], 2)
                }),
                self.catalog_options,
                read_columns=["age"],
                update_cols=["age"],
                rows_per_commit=2,
                num_partitions=1,
            )

        self.assertEqual(
            [99, 20],
            self._read(target).sort_by("id")["age"].to_pylist(),
        )


if __name__ == "__main__":
    unittest.main()
