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

import pyarrow as pa

from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.reader.shard_batch_reader import ShardBatchReader


class _BatchReader(RecordBatchReader):
    """A non-blob reader that replays an explicit list of arrow batches."""

    format_reader = None  # not a FormatBlobReader -> ShardBatchReader takes the row-range path

    def __init__(self, batches):
        self._batches = iter(batches)

    def read_arrow_batch(self):
        return next(self._batches, None)

    def close(self):
        pass


def _single_row_batches(count):
    return [pa.record_batch([pa.array([i])], names=["id"]) for i in range(count)]


def _read_all(reader):
    got = []
    while True:
        batch = reader.read_arrow_batch()
        if batch is None:
            break
        got.extend(batch.column("id").to_pylist())
    return got


class ShardBatchReaderTest(unittest.TestCase):

    def test_slice_deep_into_file_does_not_recurse(self):
        # A slice/shard whose range sits many batches into a file must not recurse
        # once per skipped batch. With the default parquet batch_size of 1024 rows a
        # slice starting ~1M rows in skips >1000 batches; recursing there overflowed
        # the stack with RecursionError. 2000 single-row batches reproduce that.
        batch_count = 2000
        reader = ShardBatchReader(
            _BatchReader(_single_row_batches(batch_count)), batch_count - 1, batch_count)

        batch = reader.read_arrow_batch()

        self.assertIsNotNone(batch)
        self.assertEqual(batch.column("id").to_pylist(), [batch_count - 1])
        self.assertIsNone(reader.read_arrow_batch())

    def test_slice_returns_only_rows_in_range(self):
        # Semantics guard: with single-row batches, slice [2, 5) yields rows 2, 3, 4
        # and nothing else, so the loop refactor preserves the range filtering.
        reader = ShardBatchReader(_BatchReader(_single_row_batches(8)), 2, 5)

        self.assertEqual(_read_all(reader), [2, 3, 4])

    def test_slice_straddling_batch_boundaries(self):
        # Multi-row batches so the two slice() branches are exercised: the first
        # batch straddles start_pos (2 in [0,4)) and the last straddles end_pos
        # (9 in [8,12)); slice [2, 9) must yield exactly rows 2..8.
        batches = [
            pa.record_batch([pa.array([0, 1, 2, 3])], names=["id"]),
            pa.record_batch([pa.array([4, 5, 6, 7])], names=["id"]),
            pa.record_batch([pa.array([8, 9, 10, 11])], names=["id"]),
        ]
        reader = ShardBatchReader(_BatchReader(batches), 2, 9)

        self.assertEqual(_read_all(reader), [2, 3, 4, 5, 6, 7, 8])


if __name__ == "__main__":
    unittest.main()
