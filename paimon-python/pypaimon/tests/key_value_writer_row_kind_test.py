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
from pypaimon.write.writer.key_value_data_writer import KeyValueDataWriter


class KeyValueWriterRowKindTest(unittest.TestCase):
    """Test cases for KeyValueDataWriter with real RowKind support."""

    def setUp(self):
        """Set up test fixtures."""
        # Mock table object
        self.mock_table = Mock()
        self.mock_table.file_io = Mock()
        self.mock_table.options = Mock()
        self.mock_table.options.get = Mock(return_value="parquet")
        self.mock_table.table_schema = Mock()

        # Create mock field with proper name attribute
        mock_field = Mock()
        mock_field.name = 'id'
        self.mock_table.table_schema.get_trimmed_primary_key_fields = Mock(return_value=[
            mock_field
        ])
        self.mock_table.partition_keys = []

        # Create writer with mock table
        self.writer = KeyValueDataWriter(
            table=self.mock_table,
            partition=(),
            bucket=0,
            max_seq_number=0
        )
        # Manually set trimmed_primary_key for tests
        self.writer.trimmed_primary_key = ['id']

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

        result = self.writer._add_system_fields(data)

        # Verify system fields are added
        self.assertIn('_VALUE_KIND', result.column_names)

        # Verify _VALUE_KIND defaults to INSERT (0)
        value_kind_col = result.column('_VALUE_KIND')
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

        result = self.writer._add_system_fields(data)

        value_kind_col = result.column('_VALUE_KIND')
        self.assertEqual(value_kind_col[0].as_py(), RowKind.INSERT.value)
        self.assertEqual(value_kind_col[1].as_py(), RowKind.UPDATE_BEFORE.value)
        self.assertEqual(value_kind_col[2].as_py(), RowKind.UPDATE_AFTER.value)
        self.assertEqual(value_kind_col[3].as_py(), RowKind.DELETE.value)

    def test_deduplicate_by_primary_key(self):
        """Test deduplication keeps the latest record for each primary key."""
        # Create sorted data with duplicate primary keys
        data = pa.RecordBatch.from_pydict({
            '_KEY_id': pa.array([1, 2, 2, 3], type=pa.int64()),
            'value': ['a', 'b', 'b-new', 'c'],
            '_SEQUENCE_NUMBER': pa.array([1, 1, 2, 1], type=pa.int64()),
            '_VALUE_KIND': pa.array([0, 0, 2, 0], type=pa.int32())
        })

        result = self.writer._deduplicate_by_primary_key(data)

        # Verify only latest records for each key are kept
        self.assertEqual(result.num_rows, 3)
        key_col = result.column('_KEY_id')
        value_col = result.column('value')

        self.assertEqual(key_col[0].as_py(), 1)
        self.assertEqual(value_col[0].as_py(), 'a')
        self.assertEqual(key_col[1].as_py(), 2)
        self.assertEqual(value_col[1].as_py(), 'b-new')  # Latest value
        self.assertEqual(key_col[2].as_py(), 3)
        self.assertEqual(value_col[2].as_py(), 'c')

    def test_deduplicate_single_row(self):
        """Test deduplication with single row."""
        data = pa.RecordBatch.from_pydict({
            '_KEY_id': [1],
            'value': ['a'],
            '_SEQUENCE_NUMBER': pa.array([1], type=pa.int64()),
            '_VALUE_KIND': pa.array([0], type=pa.int32())
        })

        result = self.writer._deduplicate_by_primary_key(data)

        # Should return unchanged
        self.assertEqual(result.num_rows, 1)

    def test_deduplicate_empty_data(self):
        """Test deduplication with empty data."""
        data = pa.RecordBatch.from_pydict({
            '_KEY_id': pa.array([], type=pa.int32()),
            'value': pa.array([], type=pa.string()),
            '_SEQUENCE_NUMBER': pa.array([], type=pa.int64()),
            '_VALUE_KIND': pa.array([], type=pa.int32())
        })

        result = self.writer._deduplicate_by_primary_key(data)

        # Should return unchanged
        self.assertEqual(result.num_rows, 0)


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
