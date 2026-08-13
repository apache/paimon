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

    def test_merge_all_streams_without_concatenating_arrays(self):
        batches = [
            pa.record_batch([pa.array(["a", "b"])], names=["value"]),
            pa.record_batch([pa.array(["c", "d", "e"])], names=["value"]),
        ]
        reader = MergeAllBatchReader(
            [lambda batch=batch: _BatchReader([batch]) for batch in batches],
            batch_size=2,
        )

        with patch.object(
                pa, "concat_arrays",
                side_effect=AssertionError("must preserve Arrow chunks")):
            actual = []
            while True:
                batch = reader.read_arrow_batch()
                if batch is None:
                    break
                actual.append(batch.column(0).to_pylist())

        self.assertEqual(actual, [["a", "b"], ["c", "d"], ["e"]])

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
