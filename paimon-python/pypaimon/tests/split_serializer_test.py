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
import struct
import unittest

from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.read.split_serializer import (
    _decode_modified_utf8, _decode_str_array, _encode_modified_utf8,
    deserialize_split_v1, serialize_split_v1)
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
# with a trailing row-ranges/scores section that must survive decoding.
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


# Java DataSplitCompatibleTest's v9 golden, also used by paimon-rust's
# serialize_matches_datasplit_v9 test. Includes the new non-null per-column
# sequence numbers [15, 100, 150, 200] after write_cols.
_GOLDEN_DATA_SPLIT_V9 = base64.b64decode(
    "3sPSMCwZ7GYAAAAJAAAAAAAAABIAAAAUAAAAAQAAAAAAAAAAYWFhYWEAAIUAAAAUAAdteSBwYXRo"
    "AQAAACAAAAAAAAAAAAEAAAJoAAAAAAAAAABteV9maWxlhwAAEAAAAAAAAAQAAAAAAAAUAAAAsAAA"
    "ABQAAADIAAAAYAAAAOAAAACAAAAAQAEAAA8AAAAAAAAAyAAAAAAAAAAFAAAAAAAAAAMAAAAAAAAA"
    "GAAAAMABAABgTEpMfwEAAAsAAAAAAAAAAQIEAAAAAIMBAAAAAAAAACAAAADYAQAAGQAAAPgBAAAM"
    "AAAAAAAAACgAAAAYAgAAKAAAAEACAAAAAAABAAAAAAAAAABtaW5fa2V5hwAAAAAAAAABAAAAAAAA"
    "AABtYXhfa2V5hwAAAAAAAAAAAAAAABQAAAAgAAAAFAAAADgAAAAQAAAAUAAAAAAAAAEAAAAAAAAA"
    "AG1pbl9rZXmHAAAAAAAAAAEAAAAAAAAAAG1heF9rZXmHAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAA"
    "AAAAACQAAAAgAAAAJAAAAEgAAAAQAAAAcAAAAAAAAAEAAAAAAAAAAAkAAAAQAAAAbWluX3ZhbHVl"
    "AAAAAAAAAAAAAAAAAAABAAAAAAAAAAAJAAAAEAAAAG1heF92YWx1ZQAAAAAAAAAAAAAAAQAAAAAA"
    "AAAAAAAAAAAAAAIAAAAAAAAAZXh0cmExAIZleHRyYTIAhgMAAAAAAAAAZmllbGQxAIZmaWVsZDIA"
    "hmZpZWxkMwCGaGRmczovLy9wYXRoL3RvL3dhcmVob3VzZQAAAAAAAAAEAAAAAAAAAGEAAAAAAACB"
    "YgAAAAAAAIFjAAAAAAAAgWYAAAAAAACBBAAAAAAAAAAPAAAAAAAAAGQAAAAAAAAAlgAAAAAAAADI"
    "AAAAAAAAAAEAAAABAQANZGVsZXRpb25fZmlsZQAAAAAAAABkAAAAAAAAABYAAAAAAAAAIQAA"
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

    def test_streaming_java_flag_survives_decoding_and_selection(self):
        data = bytearray(_GOLDEN_DATA_SPLIT_V1)
        data[-2] = 1
        split = deserialize_split_v1(bytes(data), self._partition_fields())
        self.assertTrue(split.is_streaming)
        selected = split.filter_file(lambda file: file.file_name == 'file-b')
        self.assertTrue(selected.is_streaming)
        self.assertEqual(selected.snapshot_id, split.snapshot_id)
        indexed = IndexedSplit(selected, [])
        self.assertTrue(indexed.is_streaming)
        self.assertFalse(deserialize_split_v1(
            _GOLDEN_DATA_SPLIT_V1, self._partition_fields()).is_streaming)

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

    def test_deserialize_v9_data_and_indexed_splits(self):
        fields = [DataField(0, 's', AtomicType('STRING'))]
        for indexed in (False, True):
            with self.subTest(indexed=indexed):
                if indexed:
                    data = (_GOLDEN_INDEXED_SPLIT_V1[:28] + _GOLDEN_DATA_SPLIT_V9
                            + struct.pack('>iqqBif', 1, 13, 14, 1, 1, 0.75))
                else:
                    data = _GOLDEN_DATA_SPLIT_V1[:16] + _GOLDEN_DATA_SPLIT_V9
                split = deserialize_split_v1(data, fields, fields)
                self.assertEqual(split.snapshot_id, 18)
                self.assertEqual(list(split.partition.values), ['aaaaa'])
                self.assertEqual(split.bucket, 20)
                self.assertFalse(split.raw_convertible)
                self.assertEqual(len(split.files), 1)
                file = split.files[0]
                self.assertEqual(file.file_name, 'my_file')
                self.assertEqual(file.file_path, 'hdfs:///path/to/warehouse')
                self.assertEqual((file.file_size, file.row_count), (1024 * 1024, 1024))
                self.assertEqual(list(file.min_key.values), ['min_key'])
                self.assertEqual(list(file.max_key.values), ['max_key'])
                self.assertEqual((file.min_sequence_number, file.max_sequence_number), (15, 200))
                self.assertEqual((file.schema_id, file.level), (5, 3))
                self.assertEqual(file.extra_files, ['extra1', 'extra2'])
                self.assertEqual(file.embedded_index, bytes([1, 2, 4]))
                self.assertEqual(file.value_stats_cols, ['field1', 'field2', 'field3'])
                self.assertEqual(file.first_row_id, 12)
                self.assertEqual(file.write_cols, ['a', 'b', 'c', 'f'])
                self.assertEqual(
                    file.write_cols_sequences, [15, 100, 150, 200])
                self.assertEqual(len(split.data_deletion_files), 1)
                dv = split.data_deletion_files[0]
                self.assertEqual(
                    (dv.dv_index_path, dv.offset, dv.length, dv.cardinality),
                    ('deletion_file', 100, 22, 33))
                if indexed:
                    self.assertEqual([(r.from_, r.to) for r in split.row_ranges()], [(13, 14)])
                    self.assertEqual(split.scores(), [0.75])

    def test_v9_roundtrip_is_byte_compatible_with_java(self):
        fields = [DataField(0, 's', AtomicType('STRING'))]
        frame = _GOLDEN_DATA_SPLIT_V1[:16] + _GOLDEN_DATA_SPLIT_V9

        split = deserialize_split_v1(frame, fields, fields)

        self.assertEqual(serialize_split_v1(split), frame)

    def test_v8_is_upgraded_to_v9_without_losing_reader_metadata(self):
        fields = self._partition_fields()
        split = deserialize_split_v1(_GOLDEN_DATA_SPLIT_V1, fields)

        upgraded = deserialize_split_v1(serialize_split_v1(split), fields)

        self.assertEqual(upgraded.snapshot_id, split.snapshot_id)
        self.assertEqual(upgraded.bucket_path, split.bucket_path)
        self.assertEqual(upgraded.total_buckets, split.total_buckets)
        self.assertEqual(
            [file.file_path for file in upgraded.files],
            [file.file_path for file in split.files])
        self.assertEqual(
            [file.max_sequence_number for file in upgraded.files], [100, 200])
        self.assertEqual(
            upgraded.files[0].key_stats.null_counts,
            split.files[0].key_stats.null_counts)

    def test_indexed_roundtrip_preserves_or_strips_scores_explicitly(self):
        fields = self._partition_fields()
        split = deserialize_split_v1(_GOLDEN_INDEXED_SPLIT_V1, fields)

        with_scores = deserialize_split_v1(
            serialize_split_v1(split), fields)
        without_scores = deserialize_split_v1(
            serialize_split_v1(split, include_scores=False), fields)

        self.assertEqual(with_scores.scores(), [0.5, 0.25, 0.125])
        self.assertIsNone(without_scores.scores())
        self.assertEqual(
            [(r.from_, r.to) for r in without_scores.row_ranges()],
            [(1, 4), (11, 13)])

    def test_disjoint_file_paths_use_per_file_external_paths(self):
        fields = self._partition_fields()
        split = deserialize_split_v1(_GOLDEN_DATA_SPLIT_V1, fields)
        split.bucket_path = None
        split.files[0].file_path = 's3://bucket-a/data/file-a'
        split.files[1].file_path = 's3://bucket-b/data/file-b'

        restored = deserialize_split_v1(serialize_split_v1(split), fields)

        self.assertEqual(restored.bucket_path, '')
        self.assertEqual(
            [file.external_path for file in restored.files],
            ['s3://bucket-a/data/file-a', 's3://bucket-b/data/file-b'])
        self.assertEqual(
            [file.file_path for file in restored.files],
            ['s3://bucket-a/data/file-a', 's3://bucket-b/data/file-b'])

    def test_modified_utf8_roundtrip_nul_bmp_and_supplementary(self):
        value = 'a\x00\u07ff\u0800\U0001f600'
        self.assertEqual(
            _decode_modified_utf8(_encode_modified_utf8(value)), value)

    def test_modified_utf8_roundtrip_unpaired_surrogates(self):
        value = '\ud800x\udc00'
        self.assertEqual(
            _decode_modified_utf8(_encode_modified_utf8(value)), value)

    def test_modified_utf8_rejects_oversized_value(self):
        with self.assertRaisesRegex(ValueError, 'too long'):
            _encode_modified_utf8('\u0800' * 21846)

    def test_serializer_rejects_mismatched_deletion_files(self):
        split = deserialize_split_v1(
            _GOLDEN_DATA_SPLIT_V1, self._partition_fields())
        split.data_deletion_files = [None]
        with self.assertRaisesRegex(ValueError, 'does not match'):
            serialize_split_v1(split)

    def test_deserializer_rejects_trailing_bytes(self):
        with self.assertRaisesRegex(ValueError, 'trailing bytes'):
            deserialize_split_v1(
                _GOLDEN_DATA_SPLIT_V1 + b'junk', self._partition_fields())

    def test_unsupported_data_split_version_raises(self):
        for version in (7, 10):
            with self.subTest(version=version):
                data = (_GOLDEN_DATA_SPLIT_V1[:24] + struct.pack('>i', version)
                        + _GOLDEN_DATA_SPLIT_V1[28:])
                with self.assertRaisesRegex(ValueError, 'unsupported DataSplit version'):
                    deserialize_split_v1(data, self._partition_fields())

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
