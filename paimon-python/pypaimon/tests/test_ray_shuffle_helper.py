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

"""Unit tests for the Ray pre-shuffle helper in pypaimon/ray/shuffle.py.

These tests exercise the helper in isolation: the bucket-key UDF (with a
stub extractor), the collision-safe column name picker, the
large-type coercion, and the bucket-mode dispatch in
``maybe_apply_repartition``. Ray-based end-to-end behaviour is covered
in ``pypaimon/tests/ray_repartition_test.py``.
"""

import unittest
from unittest.mock import MagicMock

import pyarrow as pa

from pypaimon.ray.shuffle import (BUCKET_KEY_COL, _coerce_large_string_types,
                                  _make_bucket_udf, _pick_bucket_col_name,
                                  maybe_apply_repartition)
from pypaimon.table.bucket_mode import BucketMode


class BucketUdfTest(unittest.TestCase):
    """The bucket-key UDF appends a deterministic int32 column."""

    def _make_extractor(self, buckets_per_row):
        extractor = MagicMock()
        extractor.extract_partition_bucket_batch.return_value = (
            [() for _ in buckets_per_row],
            list(buckets_per_row),
        )
        return extractor

    def test_appends_int32_bucket_column(self):
        extractor = self._make_extractor([0, 1, 0])
        udf = _make_bucket_udf(extractor, BUCKET_KEY_COL)
        batch = pa.table({"id": [10, 11, 12]})

        out = udf(batch)

        self.assertEqual(out.column_names, ["id", BUCKET_KEY_COL])
        self.assertEqual(out.schema.field(BUCKET_KEY_COL).type, pa.int32())
        self.assertEqual(out.column(BUCKET_KEY_COL).to_pylist(), [0, 1, 0])

    def test_empty_batch_appends_empty_column(self):
        extractor = self._make_extractor([])
        udf = _make_bucket_udf(extractor, BUCKET_KEY_COL)
        batch = pa.table({"id": pa.array([], type=pa.int32())})

        out = udf(batch)

        self.assertEqual(out.num_rows, 0)
        self.assertEqual(out.column_names, ["id", BUCKET_KEY_COL])
        # The extractor is short-circuited on empty input — we don't pay
        # the cost of combining empty chunks just to call into it.
        extractor.extract_partition_bucket_batch.assert_not_called()

    def test_multichunk_batch_combines_before_extracting(self):
        # Two record batches in the same table — the UDF must combine
        # before calling the extractor, otherwise the extractor sees
        # half the rows.
        extractor = self._make_extractor([0, 1, 2, 3])
        udf = _make_bucket_udf(extractor, BUCKET_KEY_COL)
        rb1 = pa.record_batch({"id": [1, 2]})
        rb2 = pa.record_batch({"id": [3, 4]})
        batch = pa.Table.from_batches([rb1, rb2])

        out = udf(batch)

        self.assertEqual(out.num_rows, 4)
        self.assertEqual(out.column(BUCKET_KEY_COL).to_pylist(), [0, 1, 2, 3])
        # Extractor is called exactly once with all four rows.
        call = extractor.extract_partition_bucket_batch.call_args
        passed_batch = call.args[0]
        self.assertEqual(passed_batch.num_rows, 4)


class PickBucketColNameTest(unittest.TestCase):
    """``_pick_bucket_col_name`` avoids collision with user columns."""

    def test_default_name_when_no_collision(self):
        self.assertEqual(
            _pick_bucket_col_name({"id", "name"}), BUCKET_KEY_COL)

    def test_fallback_when_default_collides(self):
        name = _pick_bucket_col_name({"id", BUCKET_KEY_COL})
        self.assertNotEqual(name, BUCKET_KEY_COL)
        self.assertTrue(name.startswith("__paimon_bucket_"))
        self.assertNotIn(name, {"id", BUCKET_KEY_COL})


class CoerceLargeStringTypesTest(unittest.TestCase):
    """``_identity_batch`` casts back the large_string / large_binary
    types that some Ray versions introduce when materialising blocks
    during ``groupby().map_groups``. The Paimon writer's strict schema
    check would otherwise reject those rows."""

    def test_pass_through_when_no_large_variants(self):
        batch = pa.table({"id": pa.array([1, 2], type=pa.int32()),
                          "name": pa.array(["a", "b"], type=pa.string())})
        out = _coerce_large_string_types(batch)
        self.assertEqual(out.schema, batch.schema)

    def test_casts_large_string_back_to_string(self):
        batch = pa.table({
            "id": pa.array([1, 2], type=pa.int32()),
            "name": pa.array(["x", "y"], type=pa.large_string()),
        })
        out = _coerce_large_string_types(batch)
        self.assertEqual(out.schema.field("name").type, pa.string())
        self.assertEqual(out.column("name").to_pylist(), ["x", "y"])

    def test_casts_large_binary_back_to_binary(self):
        batch = pa.table({
            "blob": pa.array([b"x", b"y"], type=pa.large_binary()),
        })
        out = _coerce_large_string_types(batch)
        self.assertEqual(out.schema.field("blob").type, pa.binary())


class BucketModeDispatchTest(unittest.TestCase):
    """``maybe_apply_repartition`` clusters HASH_FIXED tables and
    returns other bucket modes unchanged."""

    def _make_table(self, bucket_mode):
        table = MagicMock()
        table.bucket_mode.return_value = bucket_mode
        return table

    def test_bucket_unaware_returns_dataset_unchanged(self):
        dataset = object()  # sentinel; must not be wrapped or mutated
        table = self._make_table(BucketMode.BUCKET_UNAWARE)

        self.assertIs(maybe_apply_repartition(dataset, table), dataset)

    def test_hash_dynamic_returns_dataset_unchanged(self):
        dataset = object()
        table = self._make_table(BucketMode.HASH_DYNAMIC)

        self.assertIs(maybe_apply_repartition(dataset, table), dataset)

    def test_cross_partition_returns_dataset_unchanged(self):
        dataset = object()
        table = self._make_table(BucketMode.CROSS_PARTITION)

        self.assertIs(maybe_apply_repartition(dataset, table), dataset)

    def test_hash_fixed_runs_map_batches_groupby_chain(self):
        dataset = MagicMock(name="dataset")
        dataset.map_batches.return_value.groupby.return_value \
            .map_groups.return_value.drop_columns.return_value = "clustered"
        table = MagicMock()
        table.bucket_mode.return_value = BucketMode.HASH_FIXED
        table.table_schema.partition_keys = []
        table.table_schema.fields = [
            type("F", (), {"name": "id"})(),
            type("F", (), {"name": "value"})(),
        ]

        out = maybe_apply_repartition(dataset, table)

        self.assertEqual(out, "clustered")
        # The helper appends a transient bucket column, groups by it,
        # runs the identity batch over each group, then drops the
        # transient column. We assert the call chain, not its kwargs,
        # since defaults are an implementation detail.
        dataset.map_batches.assert_called_once()
        dataset.map_batches.return_value.groupby.assert_called_once()
        dataset.map_batches.return_value.groupby.return_value \
            .map_groups.assert_called_once()
        dataset.map_batches.return_value.groupby.return_value \
            .map_groups.return_value.drop_columns.assert_called_once_with(
                [BUCKET_KEY_COL]
            )

    def test_hash_fixed_groups_include_partition_keys(self):
        dataset = MagicMock(name="dataset")
        table = MagicMock()
        table.bucket_mode.return_value = BucketMode.HASH_FIXED
        table.table_schema.partition_keys = ["dt"]
        table.table_schema.fields = [
            type("F", (), {"name": "id"})(),
            type("F", (), {"name": "dt"})(),
        ]

        maybe_apply_repartition(dataset, table)

        group_call = dataset.map_batches.return_value.groupby.call_args
        passed_keys = group_call.args[0]
        self.assertEqual(passed_keys, ["dt", BUCKET_KEY_COL])


if __name__ == "__main__":
    unittest.main()
