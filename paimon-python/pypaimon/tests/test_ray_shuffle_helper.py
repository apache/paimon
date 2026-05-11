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
stub extractor) and the no-op / soft-fallback branches of
``maybe_apply_repartition``. Ray-based end-to-end behaviour is covered
in ``pypaimon/tests/ray_repartition_test.py``.
"""

import logging
import unittest
from unittest.mock import MagicMock

import pyarrow as pa

from pypaimon.ray.shuffle import (BUCKET_KEY_COL, _coerce_large_string_types,
                                  _make_bucket_udf, maybe_apply_repartition)
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
        udf = _make_bucket_udf(extractor)
        batch = pa.table({"id": [10, 11, 12]})

        out = udf(batch)

        self.assertEqual(out.column_names, ["id", BUCKET_KEY_COL])
        self.assertEqual(out.schema.field(BUCKET_KEY_COL).type, pa.int32())
        self.assertEqual(out.column(BUCKET_KEY_COL).to_pylist(), [0, 1, 0])

    def test_empty_batch_appends_empty_column(self):
        extractor = self._make_extractor([])
        udf = _make_bucket_udf(extractor)
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
        udf = _make_bucket_udf(extractor)
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


class NoOpBranchTest(unittest.TestCase):
    """The off / fallback branches don't touch the dataset object."""

    def _make_table(self, bucket_mode):
        table = MagicMock()
        table.bucket_mode.return_value = bucket_mode
        return table

    def test_shuffle_off_and_no_num_blocks_is_noop(self):
        dataset = object()  # sentinel; not touched
        table = self._make_table(BucketMode.HASH_FIXED)

        out_ds, applied = maybe_apply_repartition(
            dataset, table, shuffle=False, num_blocks=None,
        )

        self.assertIs(out_ds, dataset)
        self.assertFalse(applied)
        table.bucket_mode.assert_not_called()  # short-circuit early

    def test_shuffle_on_non_fixed_bucket_falls_through_with_warning(self):
        dataset = MagicMock()
        # repartition is what shuffle=False + num_blocks=None falls into
        # for the "after-soft-fallback" branch — but we passed num_blocks
        # = None, so that branch returns the dataset unchanged.
        table = self._make_table(BucketMode.BUCKET_UNAWARE)

        with self.assertLogs("pypaimon.ray.shuffle", level=logging.WARNING) as cm:
            out_ds, applied = maybe_apply_repartition(
                dataset, table, shuffle=True, num_blocks=None,
            )

        self.assertIs(out_ds, dataset)
        self.assertFalse(applied)
        self.assertTrue(
            any("HASH_FIXED" in msg for msg in cm.output),
            "expected the soft-fallback warning to mention HASH_FIXED",
        )

    def test_shuffle_on_dynamic_bucket_falls_through(self):
        dataset = MagicMock()
        table = self._make_table(BucketMode.HASH_DYNAMIC)

        with self.assertLogs("pypaimon.ray.shuffle", level=logging.WARNING):
            out_ds, applied = maybe_apply_repartition(
                dataset, table, shuffle=True, num_blocks=None,
            )

        self.assertIs(out_ds, dataset)
        self.assertFalse(applied)

    def test_shuffle_off_with_num_blocks_calls_plain_repartition(self):
        dataset = MagicMock()
        dataset.repartition.return_value = "rebalanced"
        table = self._make_table(BucketMode.HASH_FIXED)

        out_ds, applied = maybe_apply_repartition(
            dataset, table, shuffle=False, num_blocks=8,
        )

        self.assertEqual(out_ds, "rebalanced")
        self.assertFalse(applied)
        dataset.repartition.assert_called_once_with(8, shuffle=False)

    def test_soft_fallback_with_num_blocks_runs_plain_repartition(self):
        # shuffle=True on a non-fixed bucket table flips to shuffle=False,
        # so a num_blocks=N caller still gets a block rebalance.
        dataset = MagicMock()
        dataset.repartition.return_value = "rebalanced"
        table = self._make_table(BucketMode.BUCKET_UNAWARE)

        with self.assertLogs("pypaimon.ray.shuffle", level=logging.WARNING):
            out_ds, applied = maybe_apply_repartition(
                dataset, table, shuffle=True, num_blocks=4,
            )

        self.assertEqual(out_ds, "rebalanced")
        self.assertFalse(applied)
        dataset.repartition.assert_called_once_with(4, shuffle=False)


if __name__ == "__main__":
    unittest.main()
