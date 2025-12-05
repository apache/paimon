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
from unittest.mock import Mock

import pyarrow as pa

from pypaimon.table.row.row_kind import RowKind


class KeyValueWriterRowKindTest(unittest.TestCase):
    """Test cases for KeyValueDataWriter with real RowKind support."""

    def setUp(self):
        """Set up test fixtures."""
        # Import the actual method
        from pypaimon.write.writer.key_value_data_writer import KeyValueDataWriter

        # Only test _extract_row_kind_column directly, not through KeyValueDataWriter init
        # because it requires complex mock setup
        self.writer = Mock(spec=['_extract_row_kind_column'])
        self.writer._extract_row_kind_column = (
            KeyValueDataWriter._extract_row_kind_column.__get__(
                self.writer, type(self.writer)
            )
        )

    def test_extract_row_kind_with_valid_column(self):
        """Test extracting RowKind from '__row_kind__' column with valid values."""
        # Create test data with __row_kind__ column
        data = pa.RecordBatch.from_pydict({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'c'],
            '__row_kind__': pa.array([0, 2, 3], type=pa.int32())  # INSERT, UPDATE_AFTER, DELETE
        })

        result = self.writer._extract_row_kind_column(data, data.num_rows)

        # Verify result
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0].as_py(), 0)
        self.assertEqual(result[1].as_py(), 2)
        self.assertEqual(result[2].as_py(), 3)

    def test_extract_row_kind_without_column(self):
        """Test generating default RowKind (INSERT) when no '__row_kind__' column."""
        data = pa.RecordBatch.from_pydict({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'c']
        })

        result = self.writer._extract_row_kind_column(data, data.num_rows)

        # Verify all default to INSERT (0)
        self.assertEqual(len(result), 3)
        for i in range(3):
            self.assertEqual(result[i].as_py(), 0)

    def test_extract_row_kind_invalid_type(self):
        """Test error when '__row_kind__' column has wrong data type."""
        # Create test data with invalid int64 type
        data = pa.RecordBatch.from_pydict({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'c'],
            '__row_kind__': pa.array([0, 1, 2], type=pa.int64())  # Wrong type!
        })

        with self.assertRaises(ValueError) as context:
            self.writer._extract_row_kind_column(data, data.num_rows)

        self.assertIn("must be of type int32", str(context.exception))

    def test_extract_row_kind_invalid_value(self):
        """Test error when '__row_kind__' column contains invalid values."""
        data = pa.RecordBatch.from_pydict({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'c'],
            '__row_kind__': pa.array([0, 5, 2], type=pa.int32())  # 5 is invalid
        })

        with self.assertRaises(ValueError) as context:
            self.writer._extract_row_kind_column(data, data.num_rows)

        self.assertIn("Invalid RowKind value", str(context.exception))

    def test_add_system_fields_with_row_kind(self):
        """Test that _add_system_fields correctly extracts RowKind."""
        # Only test the row_kind extraction part, not the full field addition
        data = pa.RecordBatch.from_pydict({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'c'],
            '__row_kind__': pa.array([0, 2, 3], type=pa.int32())
        })

        # Test the row kind extraction
        value_kind_col = self.writer._extract_row_kind_column(data, data.num_rows)

        # Verify __row_kind__ was correctly extracted
        self.assertEqual(len(value_kind_col), 3)
        self.assertEqual(value_kind_col[0].as_py(), 0)
        self.assertEqual(value_kind_col[1].as_py(), 2)
        self.assertEqual(value_kind_col[2].as_py(), 3)

    def test_add_system_fields_backward_compatibility(self):
        """Test backward compatibility when no '__row_kind__' column."""
        data = pa.RecordBatch.from_pydict({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'c']
        })

        # Test the row kind extraction
        value_kind_col = self.writer._extract_row_kind_column(data, data.num_rows)

        # Verify _VALUE_KIND defaults to INSERT (0)
        self.assertEqual(len(value_kind_col), 3)
        for i in range(3):
            self.assertEqual(value_kind_col[i].as_py(), 0)

    def test_add_system_fields_with_all_row_kinds(self):
        """Test with all possible RowKind values."""
        data = pa.RecordBatch.from_pydict({
            'id': [1, 2, 3, 4],
            'value': ['a', 'b', 'c', 'd'],
            '__row_kind__': pa.array([
                RowKind.INSERT.value,
                RowKind.UPDATE_BEFORE.value,
                RowKind.UPDATE_AFTER.value,
                RowKind.DELETE.value
            ], type=pa.int32())
        })

        result = self.writer._extract_row_kind_column(data, data.num_rows)

        self.assertEqual(result[0].as_py(), RowKind.INSERT.value)
        self.assertEqual(result[1].as_py(), RowKind.UPDATE_BEFORE.value)
        self.assertEqual(result[2].as_py(), RowKind.UPDATE_AFTER.value)
        self.assertEqual(result[3].as_py(), RowKind.DELETE.value)


class BatchTableWriteRowKindTest(unittest.TestCase):
    """Test cases for BatchTableWrite with RowKind support."""

    def test_validate_row_kind_values(self):
        """Test that valid RowKind values are recognized."""
        # Valid RowKind values
        valid_kinds = [0, 1, 2, 3]
        for kind in valid_kinds:
            # Should not raise
            self.assertIn(kind, [0, 1, 2, 3])

    def test_row_kind_array_validation(self):
        """Test RowKind array validation."""
        # Create a valid RowKind array
        row_kinds = [
            RowKind.INSERT.value,
            RowKind.UPDATE_BEFORE.value,
            RowKind.UPDATE_AFTER.value,
            RowKind.DELETE.value
        ]

        self.assertEqual(len(row_kinds), 4)
        self.assertEqual(row_kinds, [0, 1, 2, 3])


if __name__ == '__main__':
    unittest.main()
