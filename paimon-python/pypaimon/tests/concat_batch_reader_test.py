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

import unittest
from unittest.mock import patch

import pyarrow as pa

from pypaimon.read.reader.concat_batch_reader import (
    DataEvolutionMergeReader,
    MergeAllBatchReader,
)
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader


class _BatchReader(RecordBatchReader):

    def __init__(self, batches):
        self._batches = iter(batches)

    def read_arrow_batch(self):
        return next(self._batches, None)

    def close(self):
        pass


class ConcatBatchReaderTest(unittest.TestCase):

    def test_merge_all_coalesces_small_batches(self):
        batches = [
            pa.record_batch([pa.array(["a", "b"])], names=["value"]),
            pa.record_batch([pa.array(["c", "d", "e"])], names=["value"]),
        ]
        reader = MergeAllBatchReader(
            [lambda batch=batch: _BatchReader([batch]) for batch in batches],
            batch_size=2,
        )

        actual = []
        while True:
            batch = reader.read_arrow_batch()
            if batch is None:
                break
            actual.append(batch.column(0).to_pylist())

        self.assertEqual(actual, [["a", "b"], ["c", "d"], ["e"]])

    def test_merge_all_flushes_before_arrow_offset_limit(self):
        batches = [
            pa.record_batch([pa.array(["aaaa"])], names=["value"]),
            pa.record_batch([pa.array(["bbbb"])], names=["value"]),
        ]
        reader = MergeAllBatchReader(
            [lambda batch=batch: _BatchReader([batch]) for batch in batches],
            batch_size=2,
        )

        with patch(
                "pypaimon.read.reader.concat_batch_reader._MAX_ARROW_OFFSET",
                4):
            actual = []
            while True:
                batch = reader.read_arrow_batch()
                if batch is None:
                    break
                actual.append(batch.column(0).to_pylist())

        self.assertEqual(actual, [["aaaa"], ["bbbb"]])

    def test_merge_all_flushes_before_list_offset_limit(self):
        child_count = 1100000000

        def batch():
            values = pa.nulls(child_count)
            offsets = pa.array([0, child_count], type=pa.int32())
            return pa.record_batch(
                [pa.ListArray.from_arrays(offsets, values)], names=["value"])

        batches = [batch(), batch()]
        self.assertTrue(all(item.nbytes < child_count for item in batches))
        reader = MergeAllBatchReader(
            [lambda item=item: _BatchReader([item]) for item in batches],
            batch_size=2,
        )

        actual = []
        while True:
            item = reader.read_arrow_batch()
            if item is None:
                break
            actual.append(item.column(0).value_lengths()[0].as_py())

        self.assertEqual(actual, [child_count, child_count])

    def test_merge_all_checks_nested_and_map_offsets(self):
        arrays = [
            pa.array([["aaa"]], type=pa.list_(pa.string())),
            pa.array(
                [[("a", 1), ("b", 2)]],
                type=pa.map_(pa.string(), pa.int32()),
            ),
        ]
        for array in arrays:
            with self.subTest(data_type=array.type):
                batches = [
                    pa.record_batch([array], names=["value"]),
                    pa.record_batch([array], names=["value"]),
                ]
                reader = MergeAllBatchReader(
                    [
                        lambda item=item: _BatchReader([item])
                        for item in batches
                    ],
                    batch_size=2,
                )
                with patch(
                        "pypaimon.read.reader.concat_batch_reader."
                        "_MAX_ARROW_OFFSET",
                        3):
                    sizes = []
                    while True:
                        item = reader.read_arrow_batch()
                        if item is None:
                            break
                        sizes.append(item.num_rows)

                self.assertEqual(sizes, [1, 1])

    def test_misaligned_small_files_keep_bounded_batch_count(self):
        row_count = 10000
        left = MergeAllBatchReader([
            lambda value=value: _BatchReader([
                pa.record_batch([pa.array([value])], names=["left"])
            ])
            for value in range(row_count)
        ])
        right = _BatchReader([
            pa.record_batch(
                [pa.array(range(start, min(start + 1024, row_count)))],
                names=["right"],
            )
            for start in range(0, row_count, 1024)
        ])
        reader = DataEvolutionMergeReader(
            row_offsets=[0, 1],
            field_offsets=[0, 0],
            readers=[left, right],
            schema=pa.schema([("left", pa.int64()), ("right", pa.int64())]),
        )

        batch_sizes = []
        values = []
        while True:
            batch = reader.read_arrow_batch()
            if batch is None:
                break
            batch_sizes.append(batch.num_rows)
            values.extend(batch.column(0).to_pylist())

        self.assertEqual(batch_sizes, [1024] * 9 + [784])
        self.assertEqual(values, list(range(row_count)))

    def test_merge_reader_does_not_join_buffered_remainders(self):
        left = _BatchReader([
            pa.record_batch([pa.array([0])], names=["left"]),
            pa.record_batch([pa.array([1, 2])], names=["left"]),
        ])
        right = _BatchReader([
            pa.record_batch([pa.array([10, 11])], names=["right"]),
            pa.record_batch([pa.array([12])], names=["right"]),
        ])
        reader = DataEvolutionMergeReader(
            row_offsets=[0, 1],
            field_offsets=[0, 0],
            readers=[left, right],
            schema=pa.schema([("left", pa.int64()), ("right", pa.int64())]),
        )

        with patch.object(
                pa, "concat_arrays",
                side_effect=AssertionError("must preserve Arrow chunks")):
            actual = []
            while True:
                batch = reader.read_arrow_batch()
                if batch is None:
                    break
                actual.extend(
                    {"left": left, "right": right}
                    for left, right in zip(
                        batch.column(0).to_pylist(),
                        batch.column(1).to_pylist(),
                    )
                )

        self.assertEqual(actual, [
            {"left": 0, "right": 10},
            {"left": 1, "right": 11},
            {"left": 2, "right": 12},
        ])


if __name__ == "__main__":
    unittest.main()
