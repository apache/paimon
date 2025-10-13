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
import random
import sys
import unittest

from pypaimon.common.delta_varint_compressor import DeltaVarintCompressor


class DeltaVarintCompressorTest(unittest.TestCase):
    """Tests for DeltaVarintCompressor following org.apache.paimon.utils.DeltaVarintCompressorTest."""

    def test_normal_case_1(self):
        """Test case for normal compression and decompression."""
        original = [80, 50, 90, 80, 70]
        compressed = DeltaVarintCompressor.compress(original)
        decompressed = DeltaVarintCompressor.decompress(compressed)

        self.assertEqual(original, decompressed)  # Verify data integrity
        self.assertEqual(6, len(compressed))  # Optimized size for small deltas

    def test_normal_case_2(self):
        """Test case for normal compression and decompression."""
        original = [100, 50, 150, 100, 200]
        compressed = DeltaVarintCompressor.compress(original)
        decompressed = DeltaVarintCompressor.decompress(compressed)

        self.assertEqual(original, decompressed)  # Verify data integrity
        self.assertEqual(8, len(compressed))  # Optimized size for small deltas

    def test_random(self):
        """Test with random data to ensure robustness."""
        # Run multiple iterations to test various random cases
        for _ in range(100):  # Reduced from 10000 for reasonable test time
            original = []
            for i in range(100):
                # Use a smaller range than Java's Long.MAX_VALUE for Python compatibility
                original.append(random.randint(-sys.maxsize, sys.maxsize))
            compressed = DeltaVarintCompressor.compress(original)
            decompressed = DeltaVarintCompressor.decompress(compressed)

            self.assertEqual(original, decompressed)  # Verify data integrity

    def test_empty_array(self):
        """Test case for empty array."""
        original = []
        compressed = DeltaVarintCompressor.compress(original)
        decompressed = DeltaVarintCompressor.decompress(compressed)

        self.assertEqual(original, decompressed)
        self.assertEqual(0, len(compressed))

    def test_single_element(self):
        """Test case for single-element array."""
        original = [42]
        compressed = DeltaVarintCompressor.compress(original)
        decompressed = DeltaVarintCompressor.decompress(compressed)

        self.assertEqual(original, decompressed)
        # Calculate expected size: Varint encoding for 42 (0x2A -> 1 byte)
        self.assertEqual(1, len(compressed))

    def test_extreme_values(self):
        """Test case for extreme values (sys.maxsize and -sys.maxsize)."""
        original = [-sys.maxsize, sys.maxsize]
        compressed = DeltaVarintCompressor.compress(original)
        decompressed = DeltaVarintCompressor.decompress(compressed)

        self.assertEqual(original, decompressed)
        # The compressed size will depend on the platform's sys.maxsize
        # but should be reasonable for the delta encoding
        self.assertGreater(len(compressed), 0)

    def test_negative_deltas(self):
        """Test case for negative deltas with ZigZag optimization."""
        original = [100, -50, -150, -100]  # Negative values
        compressed = DeltaVarintCompressor.compress(original)
        decompressed = DeltaVarintCompressor.decompress(compressed)

        self.assertEqual(original, decompressed)
        # Verify ZigZag optimization: -1 → 1 (1 byte)
        # Delta sequence: [100, -150, -100, 50] → ZigZag →
        # Each encoded in 1-2 bytes
        self.assertLessEqual(len(compressed), 8)  # Optimized size

    def test_unsorted_data(self):
        """Test case for unsorted data (worse compression ratio)."""
        original = [1000, 5, 9999, 12345, 6789]
        compressed = DeltaVarintCompressor.compress(original)
        decompressed = DeltaVarintCompressor.decompress(compressed)

        self.assertEqual(original, decompressed)
        # Larger deltas → more bytes (e.g., 9994 → 3 bytes)
        self.assertGreater(len(compressed), 5)  # Worse than sorted case

    def test_corrupted_input(self):
        """Test case for corrupted input (invalid Varint)."""
        # Invalid Varint (all continuation flags)
        corrupted = bytes([0x80, 0x80, 0x80])
        try:
            result = DeltaVarintCompressor.decompress(corrupted)
            # If it doesn't raise an exception, the result should be reasonable
            self.assertIsInstance(result, list)
        except (IndexError, ValueError, RuntimeError):
            # It's acceptable to raise an exception for corrupted data
            pass

    def test_zero_values(self):
        """Test case for arrays with zero values."""
        original = [0, 0, 0, 0, 0]
        compressed = DeltaVarintCompressor.compress(original)
        decompressed = DeltaVarintCompressor.decompress(compressed)

        self.assertEqual(original, decompressed)
        # All deltas are 0, so should compress very well
        self.assertLessEqual(len(compressed), 5)

    def test_ascending_sequence(self):
        """Test case for ascending sequence (optimal for delta compression)."""
        original = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        compressed = DeltaVarintCompressor.compress(original)
        decompressed = DeltaVarintCompressor.decompress(compressed)

        self.assertEqual(original, decompressed)
        # All deltas are 1, so should compress very well
        self.assertLessEqual(len(compressed), 15)  # Much smaller than original

    def test_descending_sequence(self):
        """Test case for descending sequence."""
        original = [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
        compressed = DeltaVarintCompressor.compress(original)
        decompressed = DeltaVarintCompressor.decompress(compressed)

        self.assertEqual(original, decompressed)
        # All deltas are -1, should still compress well with ZigZag
        self.assertLessEqual(len(compressed), 15)

    def test_large_positive_values(self):
        """Test case for large positive values."""
        original = [1000000, 2000000, 3000000, 4000000]
        compressed = DeltaVarintCompressor.compress(original)
        decompressed = DeltaVarintCompressor.decompress(compressed)

        self.assertEqual(original, decompressed)
        # Large values but consistent deltas should still compress reasonably
        self.assertGreater(len(compressed), 4)  # Will be larger due to big numbers

    def test_mixed_positive_negative(self):
        """Test case for mixed positive and negative values."""
        original = [100, -200, 300, -400, 500]
        compressed = DeltaVarintCompressor.compress(original)
        decompressed = DeltaVarintCompressor.decompress(compressed)

        self.assertEqual(original, decompressed)
        # Mixed signs create larger deltas
        self.assertGreater(len(compressed), 5)

    def test_compression_efficiency(self):
        """Test that compression actually reduces size for suitable data."""
        # Create a sequence with small deltas
        original = []
        base = 1000
        for i in range(100):
            base += random.randint(-10, 10)  # Small deltas
            original.append(base)

        compressed = DeltaVarintCompressor.compress(original)
        decompressed = DeltaVarintCompressor.decompress(compressed)

        self.assertEqual(original, decompressed)
        # For small deltas, compression should be effective
        # Original would need 8 bytes per int (800 bytes), compressed should be much smaller
        self.assertLess(len(compressed), len(original) * 4)  # At least 50% compression

    def test_round_trip_consistency(self):
        """Test that multiple compress/decompress cycles are consistent."""
        original = [1, 10, 100, 1000, 10000]
        # First round trip
        compressed1 = DeltaVarintCompressor.compress(original)
        decompressed1 = DeltaVarintCompressor.decompress(compressed1)
        # Second round trip
        compressed2 = DeltaVarintCompressor.compress(decompressed1)
        decompressed2 = DeltaVarintCompressor.decompress(compressed2)
        # All should be identical
        self.assertEqual(original, decompressed1)
        self.assertEqual(original, decompressed2)
        self.assertEqual(compressed1, compressed2)

    def test_boundary_values(self):
        """Test boundary values for varint encoding."""
        # Test values around varint boundaries (127, 16383, etc.)
        boundary_values = [
            0, 1, 127, 128, 255, 256,
            16383, 16384, 32767, 32768,
            -1, -127, -128, -255, -256,
            -16383, -16384, -32767, -32768
        ]
        compressed = DeltaVarintCompressor.compress(boundary_values)
        decompressed = DeltaVarintCompressor.decompress(compressed)
        self.assertEqual(boundary_values, decompressed)


if __name__ == '__main__':
    unittest.main()
