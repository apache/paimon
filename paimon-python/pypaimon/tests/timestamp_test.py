"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import struct
import unittest
from datetime import datetime
from decimal import Decimal

from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import (GenericRow, GenericRowDeserializer,
                                            GenericRowSerializer)
from pypaimon.table.row.row_kind import RowKind

# Java-golden epoch millis, computed from Timestamp.fromLocalDateTime:
#   millisecond = epochDay(date) * 86_400_000 + nanoOfDay(time) / 1_000_000
#   nanoOfMillisecond = nanoOfDay(time) % 1_000_000
# These are deterministic, independent of host timezone, and match Java's
# BinaryRowWriter wire format exactly. Do NOT compute them via any pypaimon
# helper — the whole point is to have an external oracle.
TS_2025_04_08_10_30_00_123456 = datetime(2025, 4, 8, 10, 30, 0, 123456)
TS_2025_04_08_10_30_00_123456_MILLIS = 1_744_108_200_123
TS_2025_04_08_10_30_00_123456_NANO_OF_MS = 456_000

TS_2025_01_01_MILLIS = 1_735_689_600_000  # datetime(2025, 1, 1, 0, 0, 0)
TS_1969_07_20_20_17_00_MILLIS = -14_182_980_000  # datetime(1969, 7, 20, 20, 17, 0)


class TimestampTest(unittest.TestCase):
    """Tests for timestamp serialization/deserialization in GenericRow.

    Semantic contract: naive datetime fields are interpreted as if in UTC,
    matching Java's Timestamp.fromLocalDateTime (Timestamp.java:195). All
    oracles below are hand-computed from Java's algorithm, not from any
    pypaimon helper.
    """

    def test_timestamp_compact_binary_format(self):
        """Compact layout: raw epoch millis in fixed slot, no variable area."""
        fields = [DataField(0, "ts", AtomicType("TIMESTAMP(3)"))]
        row = GenericRow([TS_2025_04_08_10_30_00_123456], fields, RowKind.INSERT)
        serialized = GenericRowSerializer.to_bytes(row)

        data = serialized[4:]
        null_bits_size = 8
        fixed_part_size = null_bits_size + 1 * 8
        self.assertEqual(len(data), fixed_part_size)

        millis = struct.unpack('<q', data[null_bits_size:null_bits_size + 8])[0]
        self.assertEqual(millis, TS_2025_04_08_10_30_00_123456_MILLIS)

    def test_timestamp_compact_round_trip(self):
        """Compact round-trip preserves the datetime down to milliseconds."""
        fields = [DataField(0, "ts", AtomicType("TIMESTAMP(3)"))]
        ts = datetime(2025, 4, 8, 10, 30, 0, 123000)  # only millis meaningful
        row = GenericRow([ts], fields, RowKind.INSERT)
        serialized = GenericRowSerializer.to_bytes(row)
        result = GenericRowDeserializer.from_bytes(serialized, fields)
        self.assertEqual(result.values[0], ts)

    def test_timestamp_non_compact_binary_format(self):
        """Non-compact layout: (cursor<<32)|nanoOfMilli in fixed slot,
        epoch millis long in variable area. Both oracles come from Java."""
        fields = [DataField(0, "ts", AtomicType("TIMESTAMP(6)"))]
        row = GenericRow([TS_2025_04_08_10_30_00_123456], fields, RowKind.INSERT)
        serialized = GenericRowSerializer.to_bytes(row)

        data = serialized[4:]
        null_bits_size = 8
        fixed_part_size = null_bits_size + 1 * 8
        self.assertEqual(len(data), fixed_part_size + 8)

        offset_and_nano = struct.unpack('<q', data[null_bits_size:null_bits_size + 8])[0]
        cursor = (offset_and_nano >> 32) & 0xFFFFFFFF
        nano_of_millisecond = offset_and_nano & 0xFFFFFFFF
        self.assertEqual(cursor, fixed_part_size)
        self.assertEqual(nano_of_millisecond, TS_2025_04_08_10_30_00_123456_NANO_OF_MS)

        var_millis = struct.unpack('<q', data[cursor:cursor + 8])[0]
        self.assertEqual(var_millis, TS_2025_04_08_10_30_00_123456_MILLIS)

    def test_timestamp_non_compact_round_trip(self):
        """Non-compact round-trip preserves microseconds."""
        for type_str in ["TIMESTAMP(6)", "TIMESTAMP(9)"]:
            with self.subTest(type=type_str):
                fields = [DataField(0, "ts", AtomicType(type_str))]
                row = GenericRow([TS_2025_04_08_10_30_00_123456], fields, RowKind.INSERT)
                serialized = GenericRowSerializer.to_bytes(row)
                result = GenericRowDeserializer.from_bytes(serialized, fields)
                self.assertEqual(result.values[0], TS_2025_04_08_10_30_00_123456)

    def test_timestamp_non_compact_null(self):
        fields = [DataField(0, "ts", AtomicType("TIMESTAMP(6)"))]
        row = GenericRow([None], fields, RowKind.INSERT)
        serialized = GenericRowSerializer.to_bytes(row)
        result = GenericRowDeserializer.from_bytes(serialized, fields)
        self.assertIsNone(result.values[0])

    def test_timestamp_boundary_precision(self):
        """Precision 3 is last compact, precision 4 is first non-compact."""
        fixed_part_size = 8 + 1 * 8

        fields_3 = [DataField(0, "ts", AtomicType("TIMESTAMP(3)"))]
        s_3 = GenericRowSerializer.to_bytes(
            GenericRow([TS_2025_04_08_10_30_00_123456], fields_3, RowKind.INSERT))
        self.assertEqual(len(s_3[4:]), fixed_part_size)

        fields_4 = [DataField(0, "ts", AtomicType("TIMESTAMP(4)"))]
        s_4 = GenericRowSerializer.to_bytes(
            GenericRow([TS_2025_04_08_10_30_00_123456], fields_4, RowKind.INSERT))
        self.assertEqual(len(s_4[4:]), fixed_part_size + 8)

    def test_timestamp_mixed_with_other_types(self):
        fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "name", AtomicType("STRING")),
            DataField(2, "ts_compact", AtomicType("TIMESTAMP(3)")),
            DataField(3, "ts_non_compact", AtomicType("TIMESTAMP(6)")),
            DataField(4, "dec", AtomicType("DECIMAL(38, 10)")),
        ]

        ts_compact = datetime(2025, 1, 1, 0, 0, 0)
        dec_val = Decimal("12345678901234567890.1234567890")

        row = GenericRow(
            [42, "hello", ts_compact, TS_2025_04_08_10_30_00_123456, dec_val],
            fields, RowKind.INSERT
        )
        serialized = GenericRowSerializer.to_bytes(row)
        result = GenericRowDeserializer.from_bytes(serialized, fields)

        self.assertEqual(result.values[0], 42)
        self.assertEqual(result.values[1], "hello")
        self.assertEqual(result.values[2], ts_compact)
        self.assertEqual(result.values[3], TS_2025_04_08_10_30_00_123456)
        self.assertEqual(result.values[4], dec_val)

    def test_timestamp_default_precision_is_six(self):
        """Bare TIMESTAMP must match Java's TimestampType.DEFAULT_PRECISION = 6,
        i.e. use the non-compact layout and preserve microseconds."""
        fields = [DataField(0, "ts", AtomicType("TIMESTAMP"))]
        row = GenericRow([TS_2025_04_08_10_30_00_123456], fields, RowKind.INSERT)
        serialized = GenericRowSerializer.to_bytes(row)

        data = serialized[4:]
        null_bits_size = 8
        fixed_part_size = null_bits_size + 1 * 8
        # non-compact layout must emit an 8-byte variable area
        self.assertEqual(len(data), fixed_part_size + 8)

        offset_and_nano = struct.unpack('<q', data[null_bits_size:null_bits_size + 8])[0]
        nano_of_millisecond = offset_and_nano & 0xFFFFFFFF
        self.assertEqual(nano_of_millisecond, TS_2025_04_08_10_30_00_123456_NANO_OF_MS)

        result = GenericRowDeserializer.from_bytes(serialized, fields)
        self.assertEqual(result.values[0], TS_2025_04_08_10_30_00_123456)

    def test_timestamp_ltz_default_precision_is_six(self):
        """Bare TIMESTAMP_LTZ must also default to precision 6 (non-compact)."""
        fields = [DataField(0, "ts", AtomicType("TIMESTAMP_LTZ"))]
        row = GenericRow([TS_2025_04_08_10_30_00_123456], fields, RowKind.INSERT)
        serialized = GenericRowSerializer.to_bytes(row)
        fixed_part_size = 8 + 1 * 8
        self.assertEqual(len(serialized[4:]), fixed_part_size + 8)

        result = GenericRowDeserializer.from_bytes(serialized, fields)
        self.assertEqual(result.values[0], TS_2025_04_08_10_30_00_123456)

    def test_timestamp_golden_2025_01_01(self):
        """Hard-coded Java golden: 2025-01-01 00:00:00 UTC == 1_735_689_600_000 ms.
        This is what catches a regression into local-time semantics, since the
        value is picked so every tz offset produces a different number."""
        fields = [DataField(0, "ts", AtomicType("TIMESTAMP(3)"))]
        ts = datetime(2025, 1, 1, 0, 0, 0)
        serialized = GenericRowSerializer.to_bytes(GenericRow([ts], fields, RowKind.INSERT))
        data = serialized[4:]
        millis = struct.unpack('<q', data[8:16])[0]
        self.assertEqual(millis, TS_2025_01_01_MILLIS)

    def test_timestamp_pre_epoch(self):
        """Negative epoch millis round-trip correctly.

        Golden: 1969-07-20 20:17:00 UTC == -14_182_980_000 ms (Java-computed)."""
        fields3 = [DataField(0, "ts", AtomicType("TIMESTAMP(3)"))]
        ts_pre = datetime(1969, 7, 20, 20, 17, 0)
        serialized = GenericRowSerializer.to_bytes(GenericRow([ts_pre], fields3, RowKind.INSERT))
        data = serialized[4:]
        millis = struct.unpack('<q', data[8:16])[0]
        self.assertEqual(millis, TS_1969_07_20_20_17_00_MILLIS)

        result = GenericRowDeserializer.from_bytes(serialized, fields3)
        self.assertEqual(result.values[0], ts_pre)

        # Non-compact
        fields6 = [DataField(0, "ts", AtomicType("TIMESTAMP(6)"))]
        ts_pre_us = datetime(1969, 7, 20, 20, 17, 0, 123456)
        row6 = GenericRow([ts_pre_us], fields6, RowKind.INSERT)
        serialized6 = GenericRowSerializer.to_bytes(row6)
        result6 = GenericRowDeserializer.from_bytes(serialized6, fields6)
        self.assertEqual(result6.values[0], ts_pre_us)

    def test_timestamp_ltz_non_compact(self):
        fields = [DataField(0, "ts", AtomicType("TIMESTAMP_LTZ(6)"))]
        ts = datetime(2025, 4, 8, 10, 30, 0, 654321)
        row = GenericRow([ts], fields, RowKind.INSERT)
        serialized = GenericRowSerializer.to_bytes(row)
        result = GenericRowDeserializer.from_bytes(serialized, fields)
        self.assertEqual(result.values[0], ts)


if __name__ == '__main__':
    unittest.main()
