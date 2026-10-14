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

"""BTree posting format and Java interoperability tests."""

import json
from pathlib import Path
import unittest
from unittest.mock import patch

from pypaimon.globalindex.btree.btree_posting_list import add_row_ids
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


def _varint(value):
    data = bytearray()
    while value > 127:
        data.append((value & 127) | 128)
        value >>= 7
    data.append(value)
    return bytes(data)


class BTreePostingListTest(unittest.TestCase):

    def test_java_postings_accumulate_without_expanding_roaring(self):
        directory = Path(__file__).parent / 'resources' / 'btree'
        expected = json.loads((directory / 'expected.json').read_text())
        result = RoaringBitmap64()
        result.add(7)
        all_rows = [7]
        for key, type_ in ((1, 0), (2, 1), (3, 2), (4, 2), (5, 2)):
            data = (directory / ('posting-%d.bin' % key)).read_bytes()
            self.assertEqual(type_, data[0])
            if type_ == 2:
                with patch.object(RoaringBitmap64, '__iter__', side_effect=AssertionError('Expanded bitmap')), \
                        patch.object(RoaringBitmap64, 'to_list', side_effect=AssertionError('Expanded bitmap')):
                    add_row_ids(data, 2, result)
            else:
                add_row_ids(data, 2, result)
            all_rows.extend(expected[str(key)])
            self.assertEqual(sorted(all_rows), result.to_list())

    def test_java_roaring_crosses_high_word_buckets(self):
        directory = Path(__file__).parent / 'resources' / 'btree'
        data = (directory / 'posting-high-buckets.bin').read_bytes()
        self.assertEqual(2, data[0])
        result = RoaringBitmap64()
        add_row_ids(data, 2, result)
        self.assertEqual(list(range((1 << 32) - 64, (1 << 32) + 64)), result.to_list())

    def test_single_and_delta_numeric_boundaries(self):
        values = [0, 127, 128, 16383, 16384, (1 << 32) - 1, 1 << 32, (1 << 63) - 1]
        for value in values:
            with self.subTest(value=value):
                result = RoaringBitmap64()
                add_row_ids(b'\x00' + _varint(value), 2, result)
                self.assertEqual([value], result.to_list())
        data = b'\x01' + _varint(len(values)) + _varint(values[0])
        data += b''.join(_varint(b - a) for a, b in zip(values, values[1:]))
        result = RoaringBitmap64()
        add_row_ids(data, 2, result)
        self.assertEqual(values, result.to_list())

    def test_v1_uses_absolute_ids_and_footer_version(self):
        for values in ([10], [10, 20], [0, 128, 1 << 32, (1 << 63) - 1]):
            with self.subTest(values=values):
                result = RoaringBitmap64()
                data = _varint(len(values)) + b''.join(_varint(v) for v in values)
                add_row_ids(data, 1, result)
                self.assertEqual(values, result.to_list())

    def test_rejects_invalid_postings(self):
        oversized = _varint(1 << 63)
        invalid = [
            b'', b'\x63', b'\x00', b'\x00\x80',
            b'\x01', b'\x01\x80', b'\x01\x00', b'\x01\x01\x0a',
            b'\x01' + _varint(1 << 31), b'\x01\x02',
            b'\x01\x02\x80', b'\x01\x02\x0a', b'\x01\x02\x0a\x80',
            b'\x01\x02\x0a\x00', b'\x00' + oversized,
            b'\x01\x02' + oversized, b'\x01\x02\x00' + oversized,
            b'\x01\x02' + _varint((1 << 63) - 1) + b'\x01',
            b'\x02', b'\x02invalid', b'\x02' + RoaringBitmap64().serialize(),
        ]
        bitmap = RoaringBitmap64()
        bitmap.add(1 << 63)
        invalid.append(b'\x02' + bitmap.serialize())
        directory = Path(__file__).parent / 'resources' / 'btree'
        invalid.append((directory / 'posting-3.bin').read_bytes()[:-1])
        for data in invalid:
            with self.subTest(data=data):
                with self.assertRaises((ValueError, IndexError)):
                    add_row_ids(data, 2, RoaringBitmap64())

    def test_rejects_unknown_version(self):
        with self.assertRaisesRegex(ValueError, 'Unsupported BTree index file version: 3'):
            add_row_ids(b'\x00\x0a', 3, RoaringBitmap64())
