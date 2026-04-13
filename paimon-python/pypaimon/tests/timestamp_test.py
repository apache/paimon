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
                                            GenericRowSerializer,
                                            _datetime_to_millis_and_nanos)
from pypaimon.table.row.row_kind import RowKind


def _to_millis(dt: datetime) -> int:
    """Helper: datetime to epoch millis using the same logic as serialization."""
    millis, _ = _datetime_to_millis_and_nanos(dt)
    return millis


class TimestampTest(unittest.TestCase):
    """Tests for timestamp serialization/deserialization in GenericRow."""

    def test_timestamp_compact(self):
        """Compact timestamp (precision <= 3): round-trip preserves millis."""
        for type_str in ["TIMESTAMP(0)", "TIMESTAMP(3)"]:
            with self.subTest(type=type_str):
                fields = [DataField(0, "ts", AtomicType(type_str))]
                ts = datetime(2025, 4, 8, 10, 30, 0, 123000)
                row = GenericRow([ts], fields, RowKind.INSERT)
                serialized = GenericRowSerializer.to_bytes(row)
                result = GenericRowDeserializer.from_bytes(serialized, fields)
                self.assertEqual(_to_millis(result.values[0]), _to_millis(ts))

    def test_timestamp_compact_binary_format(self):
        """Verify compact binary layout: epoch millis in fixed slot, no variable area."""
        fields = [DataField(0, "ts", AtomicType("TIMESTAMP(3)"))]
        ts = datetime(2025, 4, 8, 10, 30, 0)
        row = GenericRow([ts], fields, RowKind.INSERT)
        serialized = GenericRowSerializer.to_bytes(row)

        data = serialized[4:]
        null_bits_size = 8
        fixed_part_size = null_bits_size + 1 * 8
        self.assertEqual(len(data), fixed_part_size)
        field_offset = null_bits_size
        millis = struct.unpack('<q', data[field_offset:field_offset + 8])[0]
        self.assertEqual(millis, _to_millis(ts))

    def test_timestamp_non_compact(self):
        """Non-compact timestamp (precision 4-9): round-trip preserves microseconds."""
        for type_str in ["TIMESTAMP(6)", "TIMESTAMP(9)"]:
            with self.subTest(type=type_str):
                fields = [DataField(0, "ts", AtomicType(type_str))]
                ts = datetime(2025, 4, 8, 10, 30, 0, 123456)
                row = GenericRow([ts], fields, RowKind.INSERT)
                serialized = GenericRowSerializer.to_bytes(row)
                result = GenericRowDeserializer.from_bytes(serialized, fields)
                self.assertEqual(result.values[0], ts)

    def test_timestamp_non_compact_binary_format(self):
        """Verify non-compact binary layout: (offset << 32 | nanoOfMilli) in fixed slot,
        epoch millis in variable area."""
        fields = [DataField(0, "ts", AtomicType("TIMESTAMP(6)"))]
        ts = datetime(2025, 4, 8, 10, 30, 0, 123456)
        row = GenericRow([ts], fields, RowKind.INSERT)
        serialized = GenericRowSerializer.to_bytes(row)

        data = serialized[4:]
        null_bits_size = 8
        fixed_part_size = null_bits_size + 1 * 8

        # Has 8-byte variable area
        self.assertEqual(len(data), fixed_part_size + 8)

        # Parse fixed slot
        field_offset = null_bits_size
        offset_and_nano = struct.unpack('<q', data[field_offset:field_offset + 8])[0]
        cursor = (offset_and_nano >> 32) & 0xFFFFFFFF
        nano_of_millisecond = offset_and_nano & 0xFFFFFFFF

        self.assertEqual(cursor, fixed_part_size)
        # 123456 us = 123 ms + 456 us = 123 ms + 456000 ns
        self.assertEqual(nano_of_millisecond, 456000)

        # Variable area contains epoch millis
        var_millis = struct.unpack('<q', data[cursor:cursor + 8])[0]
        self.assertEqual(var_millis, _to_millis(ts))

    def test_timestamp_non_compact_null(self):
        """Non-compact timestamp null value."""
        fields = [DataField(0, "ts", AtomicType("TIMESTAMP(6)"))]
        row = GenericRow([None], fields, RowKind.INSERT)
        serialized = GenericRowSerializer.to_bytes(row)
        result = GenericRowDeserializer.from_bytes(serialized, fields)
        self.assertIsNone(result.values[0])

    def test_timestamp_boundary_precision(self):
        """Precision 3 is last compact, precision 4 is first non-compact."""
        ts = datetime(2025, 4, 8, 10, 30, 0, 123456)
        fixed_part_size = 8 + 1 * 8

        # precision=3: compact, no variable area
        fields_3 = [DataField(0, "ts", AtomicType("TIMESTAMP(3)"))]
        s_3 = GenericRowSerializer.to_bytes(GenericRow([ts], fields_3, RowKind.INSERT))
        self.assertEqual(len(s_3[4:]), fixed_part_size)

        # precision=4: non-compact, has 8-byte variable area
        fields_4 = [DataField(0, "ts", AtomicType("TIMESTAMP(4)"))]
        s_4 = GenericRowSerializer.to_bytes(GenericRow([ts], fields_4, RowKind.INSERT))
        self.assertEqual(len(s_4[4:]), fixed_part_size + 8)

    def test_timestamp_mixed_with_other_types(self):
        """Non-compact timestamp mixed with other types in a single row."""
        fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "name", AtomicType("STRING")),
            DataField(2, "ts_compact", AtomicType("TIMESTAMP(3)")),
            DataField(3, "ts_non_compact", AtomicType("TIMESTAMP(6)")),
            DataField(4, "dec", AtomicType("DECIMAL(38, 10)")),
        ]

        ts_compact = datetime(2025, 1, 1, 0, 0, 0)
        ts_non_compact = datetime(2025, 4, 8, 10, 30, 0, 123456)
        dec_val = Decimal("12345678901234567890.1234567890")

        row = GenericRow(
            [42, "hello", ts_compact, ts_non_compact, dec_val],
            fields, RowKind.INSERT
        )
        serialized = GenericRowSerializer.to_bytes(row)
        result = GenericRowDeserializer.from_bytes(serialized, fields)

        self.assertEqual(result.values[0], 42)
        self.assertEqual(result.values[1], "hello")
        self.assertEqual(_to_millis(result.values[2]), _to_millis(ts_compact))
        self.assertEqual(result.values[3], ts_non_compact)
        self.assertEqual(result.values[4], dec_val)

    def test_timestamp_default_precision(self):
        """TIMESTAMP without explicit precision defaults to 0 (compact)."""
        fields = [DataField(0, "ts", AtomicType("TIMESTAMP"))]
        ts = datetime(2025, 4, 8, 10, 30, 0)
        row = GenericRow([ts], fields, RowKind.INSERT)
        serialized = GenericRowSerializer.to_bytes(row)
        result = GenericRowDeserializer.from_bytes(serialized, fields)
        self.assertEqual(_to_millis(result.values[0]), _to_millis(ts))

    def test_timestamp_pre_epoch(self):
        """Dates before 1970-01-01 (negative epoch millis) should round-trip correctly."""
        # Compact
        fields3 = [DataField(0, "ts", AtomicType("TIMESTAMP(3)"))]
        ts_pre = datetime(1969, 7, 20, 20, 17, 0)
        row = GenericRow([ts_pre], fields3, RowKind.INSERT)
        serialized = GenericRowSerializer.to_bytes(row)
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
        """TIMESTAMP_LTZ(6) should also use non-compact format."""
        fields = [DataField(0, "ts", AtomicType("TIMESTAMP_LTZ(6)"))]
        ts = datetime(2025, 4, 8, 10, 30, 0, 654321)
        row = GenericRow([ts], fields, RowKind.INSERT)
        serialized = GenericRowSerializer.to_bytes(row)
        result = GenericRowDeserializer.from_bytes(serialized, fields)
        self.assertEqual(result.values[0], ts)


if __name__ == '__main__':
    unittest.main()
