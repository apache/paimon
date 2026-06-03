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
from typing import List, Optional

import pyarrow as pa

from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.reader.iface.record_iterator import RecordIterator
from pypaimon.read.reader.iface.record_reader import RecordReader
from pypaimon.read.reader.limited_record_reader import LimitedRecordReader


class _ListIterator(RecordIterator):
    def __init__(self, items: List):
        self._items = items
        self._idx = 0

    def next(self):
        if self._idx >= len(self._items):
            return None
        v = self._items[self._idx]
        self._idx += 1
        return v


class _StaticReader(RecordReader):
    """Hands back batches one at a time, tracks close calls and the
    number of times ``read_batch`` was invoked (so tests can prove the
    limiter actually short-circuited instead of draining the inner)."""

    def __init__(self, batches: List[List]):
        self._batches = batches
        self._idx = 0
        self.closed = False
        self.read_batch_calls = 0

    def read_batch(self) -> Optional[RecordIterator]:
        self.read_batch_calls += 1
        if self._idx >= len(self._batches):
            return None
        batch = self._batches[self._idx]
        self._idx += 1
        return _ListIterator(batch)

    def close(self):
        self.closed = True


def _drain(reader: RecordReader) -> List:
    out = []
    while True:
        batch = reader.read_batch()
        if batch is None:
            break
        while True:
            v = batch.next()
            if v is None:
                break
            out.append(v)
    return out


class LimitedRecordReaderTest(unittest.TestCase):

    def test_limit_within_first_batch(self):
        reader = LimitedRecordReader(
            _StaticReader([[1, 2, 3, 4, 5]]), limit=3)
        self.assertEqual(_drain(reader), [1, 2, 3])

    def test_limit_spans_multiple_batches(self):
        reader = LimitedRecordReader(
            _StaticReader([[1, 2], [3, 4], [5, 6]]), limit=5)
        self.assertEqual(_drain(reader), [1, 2, 3, 4, 5])

    def test_limit_larger_than_total_returns_everything(self):
        reader = LimitedRecordReader(
            _StaticReader([[1, 2, 3]]), limit=999)
        self.assertEqual(_drain(reader), [1, 2, 3])

    def test_limit_zero_returns_nothing(self):
        reader = LimitedRecordReader(
            _StaticReader([[1, 2, 3]]), limit=0)
        self.assertEqual(_drain(reader), [])
        # read_batch should short-circuit immediately rather than peek.
        self.assertIsNone(reader.read_batch())

    def test_negative_limit_rejected(self):
        with self.assertRaises(ValueError):
            LimitedRecordReader(_StaticReader([]), limit=-1)

    def test_close_propagates(self):
        inner = _StaticReader([[1, 2]])
        reader = LimitedRecordReader(inner, limit=10)
        reader.close()
        self.assertTrue(inner.closed)

    def test_iterator_stops_mid_batch(self):
        # Limit cuts halfway through a batch; the next() call past the limit
        # must return None even though the inner batch still has items.
        reader = LimitedRecordReader(
            _StaticReader([[1, 2, 3, 4, 5]]), limit=2)
        batch = reader.read_batch()
        self.assertEqual(batch.next(), 1)
        self.assertEqual(batch.next(), 2)
        self.assertIsNone(batch.next())
        # Subsequent read_batch is also None.
        self.assertIsNone(reader.read_batch())

    def test_count_visible_for_observability(self):
        reader = LimitedRecordReader(
            _StaticReader([[1, 2, 3, 4]]), limit=10)
        _drain(reader)
        self.assertEqual(reader.count, 4)

    def test_does_not_drain_inner_when_limit_met_within_first_batch(self):
        """Direct proof of the short-circuit: once the limiter has handed
        out ``limit`` rows the next ``read_batch`` short-circuits at the
        entry guard and never pulls a second batch from the inner."""
        inner = _StaticReader([[1, 2, 3, 4, 5], [6, 7, 8, 9, 10]])
        reader = LimitedRecordReader(inner, limit=3)
        self.assertEqual(_drain(reader), [1, 2, 3])
        # Only the first batch was fetched; the second is never asked for.
        self.assertEqual(inner.read_batch_calls, 1)


class _StaticBatchReader(RecordBatchReader):
    """Hands back arrow batches one at a time for testing
    ``read_arrow_batch`` on ``LimitedRecordReader``."""

    def __init__(self, batches: List[pa.RecordBatch]):
        self._batches = batches
        self._idx = 0
        self.closed = False
        self.read_arrow_batch_calls = 0

    def read_arrow_batch(self) -> Optional[pa.RecordBatch]:
        self.read_arrow_batch_calls += 1
        if self._idx >= len(self._batches):
            return None
        batch = self._batches[self._idx]
        self._idx += 1
        return batch

    def close(self):
        self.closed = True


def _make_batch(values: List[int]) -> pa.RecordBatch:
    return pa.RecordBatch.from_arrays(
        [pa.array(values, type=pa.int64())], names=["v"])


def _drain_arrow(reader) -> List[int]:
    out = []
    while True:
        batch = reader.read_arrow_batch()
        if batch is None:
            break
        out.extend(batch.column("v").to_pylist())
    return out


class LimitedRecordReaderArrowBatchTest(unittest.TestCase):

    def test_arrow_batch_limit_within_single_batch(self):
        inner = _StaticBatchReader([_make_batch([1, 2, 3, 4, 5])])
        reader = LimitedRecordReader(inner, limit=3)
        self.assertEqual(_drain_arrow(reader), [1, 2, 3])

    def test_arrow_batch_limit_spans_multiple_batches(self):
        inner = _StaticBatchReader([
            _make_batch([1, 2]),
            _make_batch([3, 4]),
            _make_batch([5, 6]),
        ])
        reader = LimitedRecordReader(inner, limit=5)
        self.assertEqual(_drain_arrow(reader), [1, 2, 3, 4, 5])

    def test_arrow_batch_limit_larger_than_total(self):
        inner = _StaticBatchReader([_make_batch([1, 2, 3])])
        reader = LimitedRecordReader(inner, limit=999)
        self.assertEqual(_drain_arrow(reader), [1, 2, 3])

    def test_arrow_batch_limit_zero(self):
        inner = _StaticBatchReader([_make_batch([1, 2, 3])])
        reader = LimitedRecordReader(inner, limit=0)
        self.assertEqual(_drain_arrow(reader), [])
        self.assertIsNone(reader.read_arrow_batch())

    def test_arrow_batch_and_read_batch_share_count(self):
        """Verify that consuming rows via ``read_arrow_batch`` advances
        the shared ``count`` so ``read_batch`` respects the limit too."""
        batches = [_make_batch([10, 20, 30])]
        row_batches = [[100, 200]]
        inner = _StaticBatchReader(batches)
        # Patch read_batch onto the batch reader so we can test the
        # shared counter across both paths.
        inner_row_batches = list(row_batches)
        original_read_batch = inner.read_batch

        def patched_read_batch():
            if not inner_row_batches:
                return None
            items = inner_row_batches.pop(0)
            return _ListIterator(items)

        inner.read_batch = patched_read_batch

        reader = LimitedRecordReader(inner, limit=4)
        # Consume 3 rows via arrow batch
        batch = reader.read_arrow_batch()
        self.assertEqual(batch.column("v").to_pylist(), [10, 20, 30])
        self.assertEqual(reader.count, 3)
        # Only 1 more row allowed via read_batch
        it = reader.read_batch()
        self.assertIsNotNone(it)
        self.assertEqual(it.next(), 100)
        self.assertIsNone(it.next())  # limit reached


if __name__ == '__main__':
    unittest.main()
