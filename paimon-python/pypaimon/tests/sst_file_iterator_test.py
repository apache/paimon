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
#  limitations under the License.
################################################################################

"""Tests for SstFileIterator BlockHandle varlen decoding."""

import unittest
from unittest.mock import MagicMock

from pypaimon.globalindex.btree.block_handle import BlockHandle
from pypaimon.globalindex.btree.block_entry import BlockEntry
from pypaimon.globalindex.btree.memory_slice_input import MemorySliceInput
from pypaimon.globalindex.btree.sst_file_reader import SstFileIterator


def _encode_var_len(value):
    result = bytearray()
    while value > 0x7F:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value & 0x7F)
    return bytes(result)


def _encode_block_handle(offset, size):
    return _encode_var_len(offset) + _encode_var_len(size)


def _mock_block_iterator(entries):
    """Mock a BlockIterator with has_next/next/seek_to over a list of BlockEntry."""
    state = {'pos': 0}
    entry_list = list(entries)

    mock = MagicMock()
    mock.has_next = lambda: state['pos'] < len(entry_list)

    def next_entry(_self=None):
        if state['pos'] >= len(entry_list):
            raise StopIteration
        entry = entry_list[state['pos']]
        state['pos'] += 1
        return entry
    mock.__next__ = next_entry
    mock.__iter__ = lambda _self=None: mock

    def seek_to(target_key):
        for i, entry in enumerate(entry_list):
            if entry.key >= target_key:
                state['pos'] = i
                return entry.key == target_key
        state['pos'] = len(entry_list)
        return False
    mock.seek_to = seek_to

    return mock


class SstFileIteratorTest(unittest.TestCase):

    def _make_iterator(self, index_entries, data_blocks):
        mock_index_entries = []
        for key, handle in index_entries:
            value = _encode_block_handle(handle.offset, handle.size)
            mock_index_entries.append(BlockEntry(key, value))

        index_iter = _mock_block_iterator(mock_index_entries)

        def read_block(block_handle):
            entries = data_blocks.get((block_handle.offset, block_handle.size))
            if entries is None:
                raise ValueError(
                    "Unexpected BlockHandle(offset={}, size={})".format(
                        block_handle.offset, block_handle.size))
            reader = MagicMock()
            reader.iterator = lambda e=entries: _mock_block_iterator(e)
            return reader

        return SstFileIterator(read_block, index_iter)

    def test_read_batch_varlen_small_values(self):
        handle = BlockHandle(100, 50)
        data = [BlockEntry(b"k1", b"v1"), BlockEntry(b"k2", b"v2")]

        it = self._make_iterator(
            [(b"k2", handle)],
            {(100, 50): data}
        )

        batch = it.read_batch()
        self.assertIsNotNone(batch)
        entries = [batch.__next__() for _ in range(2)]
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0].key, b"k1")
        self.assertEqual(entries[1].key, b"k2")
        self.assertIsNone(it.read_batch())

    def test_read_batch_varlen_large_offset(self):
        handle = BlockHandle(300, 200)
        data = [BlockEntry(b"a", b"1")]

        it = self._make_iterator(
            [(b"a", handle)],
            {(300, 200): data}
        )

        batch = it.read_batch()
        self.assertIsNotNone(batch)
        entry = batch.__next__()
        self.assertEqual(entry.key, b"a")

    def test_read_batch_varlen_very_large_offset(self):
        handle = BlockHandle(1000000, 65535)
        data = [BlockEntry(b"big", b"val")]

        it = self._make_iterator(
            [(b"big", handle)],
            {(1000000, 65535): data}
        )

        batch = it.read_batch()
        self.assertIsNotNone(batch)
        entry = batch.__next__()
        self.assertEqual(entry.key, b"big")

    def test_read_batch_multiple_blocks(self):
        h1 = BlockHandle(0, 100)
        h2 = BlockHandle(200, 150)
        h3 = BlockHandle(500, 80)

        it = self._make_iterator(
            [(b"b", h1), (b"d", h2), (b"f", h3)],
            {
                (0, 100): [BlockEntry(b"a", b"1"), BlockEntry(b"b", b"2")],
                (200, 150): [BlockEntry(b"c", b"3"), BlockEntry(b"d", b"4")],
                (500, 80): [BlockEntry(b"e", b"5"), BlockEntry(b"f", b"6")],
            }
        )

        all_entries = []
        while True:
            batch = it.read_batch()
            if batch is None:
                break
            while batch.has_next():
                all_entries.append(batch.__next__())

        self.assertEqual(len(all_entries), 6)
        keys = [e.key for e in all_entries]
        self.assertEqual(keys, [b"a", b"b", b"c", b"d", b"e", b"f"])

    def test_seek_then_read_batch_crosses_blocks(self):
        h1 = BlockHandle(0, 100)
        h2 = BlockHandle(256, 128)

        it = self._make_iterator(
            [(b"b", h1), (b"d", h2)],
            {
                (0, 100): [BlockEntry(b"a", b"1"), BlockEntry(b"b", b"2")],
                (256, 128): [BlockEntry(b"c", b"3"), BlockEntry(b"d", b"4")],
            }
        )

        it.seek_to(b"a")
        self.assertIsNotNone(it.sought_data_block)

        batch1 = it.read_batch()
        self.assertIsNotNone(batch1)
        self.assertEqual(batch1.__next__().key, b"a")

        batch2 = it.read_batch()
        self.assertIsNotNone(batch2)
        entries2 = []
        while batch2.has_next():
            entries2.append(batch2.__next__())
        self.assertEqual(len(entries2), 2)
        self.assertEqual(entries2[0].key, b"c")
        self.assertEqual(entries2[1].key, b"d")

        self.assertIsNone(it.read_batch())

    def test_read_batch_empty_index(self):
        it = self._make_iterator([], {})
        self.assertIsNone(it.read_batch())

    def test_varlen_encoding_roundtrip(self):
        test_cases = [
            (0, 0),
            (127, 127),
            (128, 128),
            (300, 200),
            (16384, 255),
            (1000000, 65535),
            (2**31 - 1, 2**31 - 1),
        ]
        for offset, size in test_cases:
            encoded = _encode_block_handle(offset, size)
            inp = MemorySliceInput(encoded)
            decoded_offset = inp.read_var_len_long()
            decoded_size = inp.read_var_len_int()
            self.assertEqual(decoded_offset, offset,
                             "offset mismatch for ({}, {})".format(offset, size))
            self.assertEqual(decoded_size, size,
                             "size mismatch for ({}, {})".format(offset, size))


if __name__ == '__main__':
    unittest.main()
