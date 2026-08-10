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

import unittest
from unittest import mock

import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.table.special_fields import SpecialFields
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId


class TableUpdateByRowIdChunkedTest(unittest.TestCase):

    @staticmethod
    def _updater():
        updater = TableUpdateByRowId.__new__(TableUpdateByRowId)
        updater._is_blob_column = lambda _: False
        return updater

    def test_variant_update_preserves_chunks_and_non_nullable_fields(self):
        variant_type = pa.struct([
            pa.field("value", pa.binary(), nullable=False),
            pa.field("metadata", pa.binary(), nullable=False),
        ])
        original_payload = pa.chunked_array([
            pa.array([
                {"value": b"v0", "metadata": b"m0"},
                {"value": b"v1", "metadata": b"m1"},
            ], type=variant_type),
            pa.array([
                {"value": b"v2", "metadata": b"m2"},
                {"value": b"v3", "metadata": b"m3"},
            ], type=variant_type),
        ])
        update_payload = pa.chunked_array([
            pa.array([
                {"value": b"updated-1", "metadata": b"new-1"},
            ], type=variant_type),
            pa.array([
                {"value": b"updated-3", "metadata": b"new-3"},
            ], type=variant_type),
        ])
        original = pa.table({"payload": original_payload})
        updates = pa.table({
            SpecialFields.ROW_ID.name: pa.array([101, 103], type=pa.int64()),
            "payload": update_payload,
        })

        merged, _ = self._updater()._merge_update_with_original(
            original, updates, ["payload"], first_row_id=100)

        self.assertEqual(merged.schema.field("payload").type, variant_type)
        self.assertEqual(merged["payload"].num_chunks, 2)
        self.assertEqual(merged["payload"].to_pylist(), [
            {"value": b"v0", "metadata": b"m0"},
            {"value": b"updated-1", "metadata": b"new-1"},
            {"value": b"v2", "metadata": b"m2"},
            {"value": b"updated-3", "metadata": b"new-3"},
        ])

    def test_update_positions_are_sorted_once_for_all_columns(self):
        original = pa.table({
            "left": pa.chunked_array([[1], [2]]),
            "right": pa.chunked_array([[10], [20]]),
        })
        updates = pa.table({
            SpecialFields.ROW_ID.name: pa.array([1], type=pa.int64()),
            "left": pa.array([3]),
            "right": pa.array([30]),
        })

        with mock.patch("builtins.sorted", wraps=sorted) as sorted_mock:
            merged, _ = self._updater()._merge_update_with_original(
                original, updates, ["left", "right"], first_row_id=0)

        self.assertEqual(sorted_mock.call_count, 1)
        self.assertEqual(merged["left"].to_pylist(), [1, 3])
        self.assertEqual(merged["right"].to_pylist(), [10, 30])

    def test_update_position_outside_column_range_raises(self):
        original = pa.table({"payload": pa.array([1, 2])})
        updates = pa.table({
            SpecialFields.ROW_ID.name: pa.array([2], type=pa.int64()),
            "payload": pa.array([3]),
        })

        with self.assertRaisesRegex(IndexError, "outside column range"):
            self._updater()._merge_update_with_original(
                original, updates, ["payload"], first_row_id=0)

    def test_total_offsets_over_int32_remain_in_separate_chunks(self):
        child_length = 1_100_000_000
        large_chunk = self._list_chunk([0, child_length])
        original = pa.table({
            "payload": pa.chunked_array([large_chunk, large_chunk]),
        })
        updates = pa.table({
            SpecialFields.ROW_ID.name: pa.array([1], type=pa.int64()),
            "payload": pa.chunked_array([large_chunk]),
        })

        merged, _ = self._updater()._merge_update_with_original(
            original, updates, ["payload"], first_row_id=0)

        payload = merged["payload"]
        self.assertEqual(payload.num_chunks, 2)
        self.assertEqual(
            sum(len(chunk.values) for chunk in payload.chunks),
            2 * child_length,
        )

    def test_fallback_splits_before_temporary_concat_overflows(self):
        original_value_length = 600_000_000
        replacement_value_length = 1_100_000_000
        original_chunk = self._list_chunk([
            0,
            original_value_length,
            2 * original_value_length,
        ])
        replacement_chunk = self._list_chunk([0, replacement_value_length])
        original = pa.table({
            "payload": pa.chunked_array([original_chunk]),
        })
        updates = pa.table({
            SpecialFields.ROW_ID.name: pa.array([1], type=pa.int64()),
            "payload": pa.chunked_array([replacement_chunk]),
        })

        with mock.patch.object(
                TableUpdateByRowId,
                "_chunk_offsets",
                wraps=TableUpdateByRowId._chunk_offsets,
        ) as chunk_offsets:
            merged, _ = self._updater()._merge_update_with_original(
                original, updates, ["payload"], first_row_id=0)

        payload = merged["payload"]
        self.assertEqual(chunk_offsets.call_count, 1)
        self.assertEqual(payload.num_chunks, 2)
        self.assertEqual(
            [len(chunk[0].values) for chunk in payload.chunks],
            [original_value_length, replacement_value_length],
        )

    def test_replace_with_mask_capacity_error_splits_chunk(self):
        original = pa.table({
            "payload": pa.array(
                [b"a", b"b", b"c", b"d"], type=pa.binary()),
        })
        updates = pa.table({
            SpecialFields.ROW_ID.name:
                pa.array([1, 3], type=pa.int64()),
            "payload": pa.array([b"B", b"D"], type=pa.binary()),
        })
        real_replace_with_mask = pc.replace_with_mask
        attempted_lengths = []

        def replace_with_capacity_limit(values, mask, replacements):
            attempted_lengths.append(len(values))
            if len(values) > 2:
                raise pa.lib.ArrowCapacityError(
                    "array cannot contain more than 2147483647 bytes")
            return real_replace_with_mask(values, mask, replacements)

        with mock.patch.object(
                pc,
                "replace_with_mask",
                side_effect=replace_with_capacity_limit,
        ):
            merged, _ = self._updater()._merge_update_with_original(
                original, updates, ["payload"], first_row_id=0)

        self.assertEqual(attempted_lengths, [4, 2, 2])
        self.assertEqual(merged["payload"].num_chunks, 2)
        self.assertEqual(
            merged["payload"].to_pylist(),
            [b"a", b"B", b"c", b"D"],
        )

    @staticmethod
    def _list_chunk(offsets):
        return pa.ListArray.from_arrays(
            pa.array(offsets, type=pa.int32()),
            pa.nulls(offsets[-1]),
        )


if __name__ == '__main__':
    unittest.main()
