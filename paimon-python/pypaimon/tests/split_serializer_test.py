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

import base64
import unittest

from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.read.split_serializer import (
    _decode_modified_utf8, _decode_str_array, deserialize_split_v1)
from pypaimon.schema.data_types import AtomicType, DataField

# Hand-built BinaryArray<string> == ["id", "longcolumn12"]: hits both the inline
# (<=7 bytes) and var-pointer (>7) element encodings.
_BINARY_ARRAY_STR = (
    bytes([0x02, 0x00, 0x00, 0x00,                          # n = 2
           0x00, 0x00, 0x00, 0x00,                          # null bitset
           0x69, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x82,  # "id" inline (len 2)
           0x0C, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00])  # ptr len=12 off=24
    + b"longcolumn12" + bytes(4))

# Golden SplitSerializer v1: DataSplit, partition [2026, 7], bucket 3,
# bucket_path "dt=20260706/bucket-3", files file-a/file-b, dv on file-b.
# Captured from the Java SplitSerializer (the reference wire format); live
# pypaimon_rust byte-compat is covered by native_plan_integration_test.
_GOLDEN_DATA_SPLIT_V1 = base64.b64decode(
    "U1BMSVRfVjEAAAABAAAAAd7D0jAsGexmAAAACAAAAAAAAAAqAAAAHAAAAAIAAAAAAAAAAOoHAAAA"
    "AAAABwAAAAAAAAAAAAADABRkdD0yMDI2MDcwNi9idWNrZXQtMwEAAAAIAAAAAAAAAAACAAABcAAA"
    "QA8AAAAAZmlsZS1hAIYKAAAAAAAAAAoAAAAAAAAAFAAAAKgAAAAUAAAAwAAAAEgAAADYAAAASAAA"
    "ACABAAAAAAAAAAAAAGQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAABoAQAAZAAAAAAAAAAAAAAA"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEA"
    "AAAAAAAAAAEAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAoAAAAAAAAAAAAAAAAAAAAAAAAADAAAACAA"
    "AAAMAAAAMAAAAAgAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    "AAAAAAAAAAAADAAAACAAAAAMAAAAMAAAAAgAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABcAAAQA8AAAAAZmlsZS1iAIYKAAAAAAAAAAoAAAAA"
    "AAAAFAAAAKgAAAAUAAAAwAAAAEgAAADYAAAASAAAACABAAAAAAAAAAAAAMgAAAAAAAAAAAAAAAAA"
    "AAABAAAAAAAAAAgAAABoAQAAZAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAsAAAAAAAAAAAAAAAAAAAEAAAAA"
    "AAAAABQAAAAAAAAAAAAAAAAAAAAAAAAADAAAACAAAAAMAAAAMAAAAAgAAABAAAAAAAAAAAAAAAAA"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADAAAACAAAAAMAAAAMAAAAAgA"
    "AABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAA"
    "AAIAAQAJZHYvZmlsZS1iAAAAAAAAAAIAAAAAAAAACgAAAAAAAAADAAE="
)

# Same split serialized as an IndexedSplit (type id 3): identical DataSplit body
# with a trailing row-ranges/scores section that the reader skips.
_GOLDEN_INDEXED_SPLIT_V1 = base64.b64decode(
    "U1BMSVRfVjEAAAABAAAAA/L54FRCJC4xAAAAAd7D0jAsGexmAAAACAAAAAAAAAAqAAAAHAAAAAIA"
    "AAAAAAAAAOoHAAAAAAAABwAAAAAAAAAAAAADABRkdD0yMDI2MDcwNi9idWNrZXQtMwEAAAAIAAAA"
    "AAAAAAACAAABcAAAQA8AAAAAZmlsZS1hAIYKAAAAAAAAAAoAAAAAAAAAFAAAAKgAAAAUAAAAwAAA"
    "AEgAAADYAAAASAAAACABAAAAAAAAAAAAAGQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAABoAQAA"
    "ZAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    "AAAAAAAAAAAAAAEAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAoAAAAAAAAAAAAAAAAA"
    "AAAAAAAADAAAACAAAAAMAAAAMAAAAAgAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAADAAAACAAAAAMAAAAMAAAAAgAAABAAAAAAAAAAAAAAAAAAAAA"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABcAAAQA8AAAAAZmlsZS1iAIYK"
    "AAAAAAAAAAoAAAAAAAAAFAAAAKgAAAAUAAAAwAAAAEgAAADYAAAASAAAACABAAAAAAAAAAAAAMgA"
    "AAAAAAAAAAAAAAAAAAABAAAAAAAAAAgAAABoAQAAZAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAsAAAAAAAAA"
    "AAAAAAAAAAEAAAAAAAAAABQAAAAAAAAAAAAAAAAAAAAAAAAADAAAACAAAAAMAAAAMAAAAAgAAABA"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADAAAACAA"
    "AAAMAAAAMAAAAAgAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    "AAAAAAAAAAAAAQAAAAIAAQAJZHYvZmlsZS1iAAAAAAAAAAIAAAAAAAAACgAAAAAAAAADAAEAAAAC"
    "AAAAAAAAAAEAAAAAAAAABAAAAAAAAAALAAAAAAAAAA0BAAAAAz8AAAA+gAAAPgAAAA=="
)


class SplitSerializerTest(unittest.TestCase):

    def _partition_fields(self):
        return [DataField(0, 'y', AtomicType('INT')),
                DataField(1, 'm', AtomicType('INT'))]

    def test_deserialize_data_split_v1_golden(self):
        split = deserialize_split_v1(_GOLDEN_DATA_SPLIT_V1, self._partition_fields())

        self.assertIsNotNone(split.snapshot_id)
        self.assertEqual(split.bucket, 3)
        self.assertEqual(list(split.partition.values), [2026, 7])

        self.assertEqual([f.file_name for f in split.files], ['file-a', 'file-b'])
        self.assertEqual(
            [f.file_path for f in split.files],
            ['dt=20260706/bucket-3/file-a', 'dt=20260706/bucket-3/file-b'])
        self.assertEqual([f.level for f in split.files], [0, 1])
        self.assertEqual(split.files[0].max_sequence_number, 100)
        self.assertEqual(split.files[1].max_sequence_number, 200)

        self.assertEqual(len(split.data_deletion_files), 2)
        self.assertIsNone(split.data_deletion_files[0])
        dv = split.data_deletion_files[1]
        self.assertEqual(
            (dv.dv_index_path, dv.offset, dv.length, dv.cardinality),
            ('dv/file-b', 2, 10, 3))

    def test_decodes_min_max_keys_with_key_fields(self):
        # Trimmed primary keys -> per-file min/max keys are decoded for PK
        # merge-on-read. The golden files carry keys [1..10] and [11..20].
        key_fields = [DataField(0, 'k', AtomicType('BIGINT'))]
        split = deserialize_split_v1(
            _GOLDEN_DATA_SPLIT_V1, self._partition_fields(), key_fields)
        self.assertEqual([list(f.min_key.values) for f in split.files], [[1], [11]])
        self.assertEqual([list(f.max_key.values) for f in split.files], [[10], [20]])

    def test_keys_stay_empty_without_key_fields(self):
        # Append tables pass no key fields; keys stay empty (unchanged behavior).
        split = deserialize_split_v1(_GOLDEN_DATA_SPLIT_V1, self._partition_fields())
        self.assertEqual([list(f.min_key.values) for f in split.files], [[], []])
        self.assertEqual([list(f.max_key.values) for f in split.files], [[], []])

    def test_deserialize_indexed_split_v1_golden(self):
        # type id 3 -> IndexedSplit; row_ranges/scores must be preserved (dropping
        # them would make the reader scan the whole file, not the ANN/row-id result).
        split = deserialize_split_v1(_GOLDEN_INDEXED_SPLIT_V1, self._partition_fields())
        self.assertIsInstance(split, IndexedSplit)
        self.assertIsNotNone(split.snapshot_id)   # delegates to the inner DataSplit
        self.assertEqual(split.bucket, 3)
        self.assertEqual([f.file_name for f in split.files], ['file-a', 'file-b'])
        self.assertEqual(
            [(r.from_, r.to) for r in split.row_ranges()], [(1, 4), (11, 13)])
        self.assertEqual(split.scores(), [0.5, 0.25, 0.125])

    def test_decode_modified_utf8_supplementary_char(self):
        # Java writeUTF encodes U+1F600 as a CESU-8 surrogate pair; must recombine.
        self.assertEqual(
            _decode_modified_utf8(bytes([0xED, 0xA0, 0xBD, 0xED, 0xB8, 0x80])),
            '\U0001F600')

    def test_decode_str_array_inline_and_pointer(self):
        # Covers both element encodings: inline (<=7 bytes) and var pointer (>7).
        self.assertEqual(_decode_str_array(_BINARY_ARRAY_STR), ['id', 'longcolumn12'])
        self.assertIsNone(_decode_str_array(None))
        # Empty array: count 0 + empty null bitset.
        self.assertEqual(_decode_str_array(bytes([0, 0, 0, 0])), [])

    def test_bad_magic_and_version_raise(self):
        good = _GOLDEN_DATA_SPLIT_V1
        pf = self._partition_fields()
        with self.assertRaisesRegex(ValueError, "magic"):
            deserialize_split_v1(b'\x00' * 8 + good[8:], pf)
        bad_version = good[:8] + (99).to_bytes(4, 'big') + good[12:]
        with self.assertRaisesRegex(ValueError, "version"):
            deserialize_split_v1(bad_version, pf)

    def test_bad_indexed_magic_and_version_raise(self):
        # IndexedSplit magic/version follow the 16-byte SplitSerializer header.
        good = _GOLDEN_INDEXED_SPLIT_V1
        pf = self._partition_fields()
        with self.assertRaisesRegex(ValueError, "IndexedSplit magic"):
            deserialize_split_v1(good[:16] + b'\x00' * 8 + good[24:], pf)
        bad_version = good[:24] + (99).to_bytes(4, 'big') + good[28:]
        with self.assertRaisesRegex(ValueError, "IndexedSplit version"):
            deserialize_split_v1(bad_version, pf)


if __name__ == '__main__':
    unittest.main()
