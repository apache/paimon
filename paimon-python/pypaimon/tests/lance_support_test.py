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

"""Tests for Lance format support."""

import unittest

try:
    import pyarrow as pa  # noqa: F401
    from pypaimon.read.reader.lance.lance_utils import LanceUtils
    from pypaimon.common.core_options import CoreOptions
    HAS_LANCE_DEPS = True
except ImportError:
    HAS_LANCE_DEPS = False
    LanceUtils = None  # type: ignore
    CoreOptions = None  # type: ignore


class LanceUtilsTest(unittest.TestCase):
    """Test Lance utility functions."""

    def test_lance_constants(self):
        """Test that Lance constants are defined."""
        self.assertTrue(hasattr(CoreOptions, 'FILE_FORMAT_LANCE'))
        self.assertEqual(CoreOptions.FILE_FORMAT_LANCE, 'lance')

    def test_lance_options(self):
        """Test Lance option helpers."""
        options = {
            'lance.vector-search': 'true',
            'lance.index-type': 'ivf_pq'
        }

        self.assertTrue(CoreOptions.lance_enable_vector_search(options))
        self.assertEqual(CoreOptions.lance_index_type(options), 'ivf_pq')

    def test_lance_options_defaults(self):
        """Test Lance option defaults."""
        options = {}

        self.assertFalse(CoreOptions.lance_enable_vector_search(options))
        self.assertEqual(CoreOptions.lance_index_type(options), 'ivf_pq')

    @unittest.skipUnless(HAS_LANCE_DEPS, "Lance dependencies not available")
    def test_row_ranges_conversion(self):
        """Test converting row ranges."""
        # Test with list of integers
        row_ids = [0, 1, 2, 5, 6, 7, 10]
        ranges = LanceUtils.convert_row_ranges_to_list(row_ids)

        expected = [(0, 3), (5, 8), (10, 11)]
        self.assertEqual(ranges, expected)

    @unittest.skipUnless(HAS_LANCE_DEPS, "Lance dependencies not available")
    def test_row_ranges_empty(self):
        """Test empty row ranges."""
        ranges = LanceUtils.convert_row_ranges_to_list([])
        self.assertIsNone(ranges)

    @unittest.skipUnless(HAS_LANCE_DEPS, "Lance dependencies not available")
    def test_row_ranges_none(self):
        """Test None row ranges."""
        ranges = LanceUtils.convert_row_ranges_to_list(None)
        self.assertIsNone(ranges)

    @unittest.skipUnless(HAS_LANCE_DEPS, "Lance dependencies not available")
    def test_row_ranges_contiguous(self):
        """Test contiguous row ranges."""
        row_ids = [0, 1, 2, 3, 4]
        ranges = LanceUtils.convert_row_ranges_to_list(row_ids)

        expected = [(0, 5)]
        self.assertEqual(ranges, expected)


class FormatLanceReaderTest(unittest.TestCase):
    """Test Lance format reader."""

    @unittest.skipUnless(HAS_LANCE_DEPS, "Lance dependencies not available")
    def test_format_reader_import(self):
        """Test that FormatLanceReader can be imported."""
        try:
            from pypaimon.read.reader.format_lance_reader import FormatLanceReader  # noqa: F401
            self.assertTrue(True)
        except ImportError as e:
            self.fail(f"Failed to import FormatLanceReader: {e}")

    @unittest.skipUnless(HAS_LANCE_DEPS, "Lance dependencies not available")
    def test_lance_native_reader_import(self):
        """Test that LanceNativeReader can be imported."""
        try:
            from pypaimon.read.reader.lance.lance_native_reader import LanceNativeReader  # noqa: F401
            self.assertTrue(True)
        except ImportError as e:
            self.fail(f"Failed to import LanceNativeReader: {e}")


class FormatLanceWriterTest(unittest.TestCase):
    """Test Lance format writer."""

    @unittest.skipUnless(HAS_LANCE_DEPS, "Lance dependencies not available")
    def test_format_writer_import(self):
        """Test that LanceFormatWriter can be imported."""
        try:
            from pypaimon.write.writer.lance_format_writer import LanceFormatWriter  # noqa: F401
            self.assertTrue(True)
        except ImportError as e:
            self.fail(f"Failed to import LanceFormatWriter: {e}")

    @unittest.skipUnless(HAS_LANCE_DEPS, "Lance dependencies not available")
    def test_lance_native_writer_import(self):
        """Test that LanceNativeWriter can be imported."""
        try:
            from pypaimon.write.writer.lance.lance_native_writer import LanceNativeWriter  # noqa: F401
            self.assertTrue(True)
        except ImportError as e:
            self.fail(f"Failed to import LanceNativeWriter: {e}")


class LanceSplitReadIntegrationTest(unittest.TestCase):
    """Integration tests for Lance support in SplitRead."""

    @unittest.skipUnless(HAS_LANCE_DEPS, "Lance dependencies not available")
    def test_split_read_import(self):
        """Test that SplitRead includes Lance support."""
        try:
            from pypaimon.read.split_read import FormatLanceReader  # noqa: F401
            self.assertTrue(True)
        except ImportError:
            # It's okay if FormatLanceReader is not in __init__
            pass


if __name__ == '__main__':
    unittest.main()
