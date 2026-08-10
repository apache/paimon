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

import pyarrow as pa

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

        merged, _ = self._updater()._merge_update_with_original(
            original, updates, ["payload"], first_row_id=0)

        payload = merged["payload"]
        self.assertEqual(payload.num_chunks, 2)
        self.assertEqual(
            [len(chunk[0].values) for chunk in payload.chunks],
            [original_value_length, replacement_value_length],
        )

    def test_large_list_coercion_splits_before_casting_to_list(self):
        child_length = 1_100_000_000
        original = pa.table({
            "payload": pa.array([[], []], type=pa.list_(pa.null())),
        })
        large_replacement = self._large_list_chunk([0, child_length])
        updates = pa.table({
            SpecialFields.ROW_ID.name: pa.array([0, 1], type=pa.int64()),
            "payload": pa.chunked_array([
                large_replacement,
                large_replacement,
            ]),
        })

        merged, _ = self._updater()._merge_update_with_original(
            original, updates, ["payload"], first_row_id=0)

        payload = merged["payload"]
        self.assertEqual(payload.type, pa.list_(pa.null()))
        self.assertEqual(payload.num_chunks, 2)
        self.assertEqual(
            [len(chunk[0].values) for chunk in payload.chunks],
            [child_length, child_length],
        )

    @staticmethod
    def _list_chunk(offsets):
        return pa.ListArray.from_arrays(
            pa.array(offsets, type=pa.int32()),
            pa.nulls(offsets[-1]),
        )

    @staticmethod
    def _large_list_chunk(offsets):
        return pa.LargeListArray.from_arrays(
            pa.array(offsets, type=pa.int64()),
            pa.nulls(offsets[-1]),
        )


if __name__ == '__main__':
    unittest.main()
