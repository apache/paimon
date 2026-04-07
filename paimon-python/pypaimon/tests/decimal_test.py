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
from decimal import Decimal

from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow, GenericRowSerializer, GenericRowDeserializer
from pypaimon.table.row.row_kind import RowKind


class DecimalTest(unittest.TestCase):
    """Tests for decimal serialization/deserialization in GenericRow."""

    def test_decimal_compact(self):
        """Test compact decimal (precision <= 18) round-trip."""
        # precision=4, scale=2, unscaled=5 => 0.05
        fields = [
            DataField(0, "d", AtomicType("DECIMAL(4, 2)")),
            DataField(1, "d2", AtomicType("DECIMAL(4, 2)")),
        ]
        row = GenericRow([Decimal("0.05"), None], fields, RowKind.INSERT)
        serialized = GenericRowSerializer.to_bytes(row)
        result = GenericRowDeserializer.from_bytes(serialized, fields)

        self.assertEqual(str(result.values[0]), "0.05")
        self.assertIsNone(result.values[1])

        # Another compact value: 0.06
        row2 = GenericRow([Decimal("0.06"), None], fields, RowKind.INSERT)
        serialized2 = GenericRowSerializer.to_bytes(row2)
        result2 = GenericRowDeserializer.from_bytes(serialized2, fields)
        self.assertEqual(str(result2.values[0]), "0.06")

    def test_decimal_not_compact(self):
        """Test non-compact decimal (precision > 18) round-trip."""
        # precision=25, scale=5
        fields = [
            DataField(0, "d", AtomicType("DECIMAL(25, 5)")),
            DataField(1, "d2", AtomicType("DECIMAL(25, 5)")),
        ]
        row = GenericRow([Decimal("5.55000"), None], fields, RowKind.INSERT)
        serialized = GenericRowSerializer.to_bytes(row)
        result = GenericRowDeserializer.from_bytes(serialized, fields)

        self.assertEqual(str(result.values[0]), "5.55000")
        self.assertIsNone(result.values[1])

        # Another value: 6.55
        row2 = GenericRow([Decimal("6.55000"), None], fields, RowKind.INSERT)
        serialized2 = GenericRowSerializer.to_bytes(row2)
        result2 = GenericRowDeserializer.from_bytes(serialized2, fields)
        self.assertEqual(str(result2.values[0]), "6.55000")

        # Negative value
        row3 = GenericRow([Decimal("-123.45000"), None], fields, RowKind.INSERT)
        serialized3 = GenericRowSerializer.to_bytes(row3)
        result3 = GenericRowDeserializer.from_bytes(serialized3, fields)
        self.assertEqual(str(result3.values[0]), "-123.45000")

    def test_decimal_high_precision_large_value(self):
        """Test high-precision decimal with large values that exceed long range."""
        fields = [DataField(0, "d", AtomicType("DECIMAL(38, 10)"))]

        test_values = [
            Decimal("12345678901234567890.1234567890"),
            Decimal("-99999999999999999999.9999999999"),
            Decimal("0E-10"),
        ]

        for val in test_values:
            with self.subTest(value=val):
                row = GenericRow([val], fields, RowKind.INSERT)
                serialized = GenericRowSerializer.to_bytes(row)
                result = GenericRowDeserializer.from_bytes(serialized, fields)
                self.assertEqual(result.values[0], val)

    def test_decimal_mixed_with_other_types(self):
        """Test decimal fields mixed with other types in a single row."""
        fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "name", AtomicType("STRING")),
            DataField(2, "compact_dec", AtomicType("DECIMAL(10, 2)")),
            DataField(3, "high_dec", AtomicType("DECIMAL(38, 2)")),
            DataField(4, "score", AtomicType("DOUBLE")),
        ]

        row = GenericRow(
            [42, "test_row", Decimal("12345.67"), Decimal("12312455.22"), 3.14],
            fields, RowKind.INSERT
        )
        serialized = GenericRowSerializer.to_bytes(row)
        result = GenericRowDeserializer.from_bytes(serialized, fields)

        self.assertEqual(result.values[0], 42)
        self.assertEqual(result.values[1], "test_row")
        self.assertEqual(result.values[2], Decimal("12345.67"))
        self.assertEqual(result.values[3], Decimal("12312455.22"))
        self.assertAlmostEqual(result.values[4], 3.14)


    def test_decimal_compact_binary_format(self):
        """Verify compact decimal binary layout: unscaled long in fixed part."""
        fields = [DataField(0, "d", AtomicType("DECIMAL(4, 2)"))]
        row = GenericRow([Decimal("0.05")], fields, RowKind.INSERT)
        serialized = GenericRowSerializer.to_bytes(row)

        # Skip 4-byte arity prefix
        data = serialized[4:]
        null_bits_size = 8  # ((1 + 63 + 8) // 64) * 8
        field_offset = null_bits_size
        unscaled_long = struct.unpack('<q', data[field_offset:field_offset + 8])[0]
        # Decimal("0.05") with scale=2 => unscaled = 5
        self.assertEqual(unscaled_long, 5)

    def test_decimal_not_compact_binary_format(self):
        """Verify non-compact decimal binary layout: (offset << 32 | length) in fixed part,
        16-byte big-endian unscaled bytes in variable part.
        """
        fields = [DataField(0, "d", AtomicType("DECIMAL(25, 5)"))]
        row = GenericRow([Decimal("5.55000")], fields, RowKind.INSERT)
        serialized = GenericRowSerializer.to_bytes(row)

        # Skip 4-byte arity prefix
        data = serialized[4:]
        null_bits_size = 8
        field_offset = null_bits_size
        fixed_part_size = null_bits_size + 1 * 8

        offset_and_len = struct.unpack('<q', data[field_offset:field_offset + 8])[0]
        cursor = (offset_and_len >> 32) & 0xFFFFFFFF
        byte_length = offset_and_len & 0xFFFFFFFF

        # cursor should point to the variable area (== fixed_part_size)
        self.assertEqual(cursor, fixed_part_size)
        # variable area should be exactly 16 bytes (matching Java's cursor += 16)
        var_area = data[cursor:]
        self.assertEqual(len(var_area), 16)
        # unscaled bytes are big-endian signed
        unscaled_bytes = data[cursor:cursor + byte_length]
        unscaled_value = int.from_bytes(unscaled_bytes, byteorder='big', signed=True)
        # Decimal("5.55000") with scale=5 => unscaled = 555000
        self.assertEqual(unscaled_value, 555000)


    def test_decimal_boundary_precision(self):
        """Test boundary: DECIMAL(18, ...) is compact, DECIMAL(19, ...) is non-compact."""
        # precision=18: last compact
        fields_18 = [DataField(0, "d", AtomicType("DECIMAL(18, 4)"))]
        row_18 = GenericRow([Decimal("12345678901234.5678")], fields_18, RowKind.INSERT)
        s_18 = GenericRowSerializer.to_bytes(row_18)
        r_18 = GenericRowDeserializer.from_bytes(s_18, fields_18)
        self.assertEqual(r_18.values[0], Decimal("12345678901234.5678"))
        # verify compact: no variable area beyond fixed part
        data_18 = s_18[4:]
        null_bits_size = 8
        fixed_part_size = null_bits_size + 1 * 8
        self.assertEqual(len(data_18), fixed_part_size)

        # precision=19: first non-compact
        fields_19 = [DataField(0, "d", AtomicType("DECIMAL(19, 4)"))]
        row_19 = GenericRow([Decimal("12345678901234.5678")], fields_19, RowKind.INSERT)
        s_19 = GenericRowSerializer.to_bytes(row_19)
        r_19 = GenericRowDeserializer.from_bytes(s_19, fields_19)
        self.assertEqual(r_19.values[0], Decimal("12345678901234.5678"))
        # verify non-compact: has 16-byte variable area
        data_19 = s_19[4:]
        self.assertEqual(len(data_19), fixed_part_size + 16)

    def test_decimal_zero_different_scales(self):
        """Test zero value with different precisions and scales."""
        test_cases = [
            ("DECIMAL(38, 0)", Decimal("0")),
            ("DECIMAL(38, 10)", Decimal("0E-10")),
            ("DECIMAL(10, 2)", Decimal("0.00")),
        ]
        for type_str, val in test_cases:
            with self.subTest(type=type_str):
                fields = [DataField(0, "d", AtomicType(type_str))]
                row = GenericRow([val], fields, RowKind.INSERT)
                serialized = GenericRowSerializer.to_bytes(row)
                result = GenericRowDeserializer.from_bytes(serialized, fields)
                self.assertEqual(result.values[0], val)

    def test_decimal_truncation_raises(self):
        """Serializing a value with more fractional digits than scale should raise."""
        fields = [DataField(0, "d", AtomicType("DECIMAL(10, 2)"))]
        row = GenericRow([Decimal("1.999")], fields, RowKind.INSERT)
        with self.assertRaises(ArithmeticError):
            GenericRowSerializer.to_bytes(row)


if __name__ == '__main__':
    unittest.main()
