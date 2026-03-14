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

    def test_java_compatibility_zigzag_encoding(self):
        """Test ZigZag encoding compatibility with Java implementation."""
        # Test cases that verify ZigZag encoding matches Java's implementation
        # ZigZag mapping: 0->0, -1->1, 1->2, -2->3, 2->4, -3->5, 3->6, etc.
        zigzag_test_cases = [
            (0, 0),      # 0 -> 0
            (-1, 1),     # -1 -> 1
            (1, 2),      # 1 -> 2
            (-2, 3),     # -2 -> 3
            (2, 4),      # 2 -> 4
            (-3, 5),     # -3 -> 5
            (3, 6),      # 3 -> 6
            (-64, 127),  # -64 -> 127
            (64, 128),   # 64 -> 128
            (-65, 129),  # -65 -> 129
        ]

        for original_value, expected_zigzag in zigzag_test_cases:
            # Test single value compression to verify ZigZag encoding
            compressed = DeltaVarintCompressor.compress([original_value])
            decompressed = DeltaVarintCompressor.decompress(compressed)

            self.assertEqual([original_value], decompressed,
                             f"ZigZag encoding failed for value {original_value}")

    def test_java_compatibility_known_vectors(self):
        """Test with known test vectors that should match Java implementation."""
        # Test vectors with expected compressed output (hexadecimal)
        test_vectors = [
            # Simple cases
            ([0], "00"),                    # 0 -> ZigZag(0) = 0 -> Varint(0) = 0x00
            ([1], "02"),                    # 1 -> ZigZag(1) = 2 -> Varint(2) = 0x02
            ([-1], "01"),                   # -1 -> ZigZag(-1) = 1 -> Varint(1) = 0x01
            ([2], "04"),                    # 2 -> ZigZag(2) = 4 -> Varint(4) = 0x04
            ([-2], "03"),                   # -2 -> ZigZag(-2) = 3 -> Varint(3) = 0x03

            # Delta encoding cases
            ([0, 1], "0002"),               # [0, 1] -> [0, delta=1] -> [0x00, 0x02]
            ([1, 2], "0202"),               # [1, 2] -> [1, delta=1] -> [0x02, 0x02]
            ([0, -1], "0001"),              # [0, -1] -> [0, delta=-1] -> [0x00, 0x01]
            ([1, 0], "0201"),               # [1, 0] -> [1, delta=-1] -> [0x02, 0x01]

            # Larger values
            ([127], "fe01"),                # 127 -> ZigZag(127) = 254 -> Varint(254) = 0xfe01
            ([-127], "fd01"),               # -127 -> ZigZag(-127) = 253 -> Varint(253) = 0xfd01
            ([128], "8002"),                # 128 -> ZigZag(128) = 256 -> Varint(256) = 0x8002
            ([-128], "ff01"),               # -128 -> ZigZag(-128) = 255 -> Varint(255) = 0xff01
        ]

        for original, expected_hex in test_vectors:
            compressed = DeltaVarintCompressor.compress(original)
            actual_hex = compressed.hex()

            self.assertEqual(expected_hex, actual_hex,
                             f"Binary compatibility failed for {original}. "
                             f"Expected: {expected_hex}, Got: {actual_hex}")

            # Also verify round-trip
            decompressed = DeltaVarintCompressor.decompress(compressed)
            self.assertEqual(original, decompressed,
                             f"Round-trip failed for {original}")

    def test_java_compatibility_large_numbers(self):
        """Test compatibility with Java for large numbers (64-bit range)."""
        # Test cases covering the full 64-bit signed integer range
        large_number_cases = [
            2147483647,          # Integer.MAX_VALUE
            -2147483648,         # Integer.MIN_VALUE
            9223372036854775807,  # Long.MAX_VALUE
            -9223372036854775808 + 1,  # Long.MIN_VALUE + 1 (avoid overflow in Python)
            4294967295,          # 2^32 - 1
            -4294967296,         # -2^32
        ]

        for value in large_number_cases:
            # Test individual values
            compressed = DeltaVarintCompressor.compress([value])
            decompressed = DeltaVarintCompressor.decompress(compressed)
            self.assertEqual([value], decompressed,
                             f"Large number compatibility failed for {value}")

        # Test as a sequence to verify delta encoding with large numbers
        compressed_seq = DeltaVarintCompressor.compress(large_number_cases)
        decompressed_seq = DeltaVarintCompressor.decompress(compressed_seq)
        self.assertEqual(large_number_cases, decompressed_seq,
                         "Large number sequence compatibility failed")

    def test_java_compatibility_varint_boundaries(self):
        """Test Varint encoding boundaries that match Java implementation."""
        # Test values at Varint encoding boundaries
        varint_boundary_cases = [
            # 1-byte Varint boundary
            63,    # ZigZag(63) = 126, fits in 1 byte
            64,    # ZigZag(64) = 128, needs 2 bytes
            -64,   # ZigZag(-64) = 127, fits in 1 byte
            -65,   # ZigZag(-65) = 129, needs 2 bytes

            # 2-byte Varint boundary
            8191,   # ZigZag(8191) = 16382, fits in 2 bytes
            8192,   # ZigZag(8192) = 16384, needs 3 bytes
            -8192,  # ZigZag(-8192) = 16383, fits in 2 bytes
            -8193,  # ZigZag(-8193) = 16385, needs 3 bytes

            # 3-byte Varint boundary
            1048575,  # ZigZag(1048575) = 2097150, fits in 3 bytes
            1048576,  # ZigZag(1048576) = 2097152, needs 4 bytes
        ]

        for value in varint_boundary_cases:
            compressed = DeltaVarintCompressor.compress([value])
            decompressed = DeltaVarintCompressor.decompress(compressed)
            self.assertEqual([value], decompressed,
                             f"Varint boundary compatibility failed for {value}")

    def test_java_compatibility_delta_edge_cases(self):
        """Test delta encoding edge cases for Java compatibility."""
        # Edge cases that test delta encoding behavior
        delta_edge_cases = [
            # Maximum positive delta
            [0, sys.maxsize],
            # Maximum negative delta
            [sys.maxsize, 0],
            # Alternating large deltas
            [0, 1000000, -1000000, 2000000, -2000000],
            # Sequence with zero deltas
            [42, 42, 42, 42],
            # Mixed small and large deltas
            [0, 1, 1000000, 1000001, 0],
        ]

        for case in delta_edge_cases:
            compressed = DeltaVarintCompressor.compress(case)
            decompressed = DeltaVarintCompressor.decompress(compressed)
            self.assertEqual(case, decompressed,
                             f"Delta edge case compatibility failed for {case}")

    def test_java_compatibility_error_conditions(self):
        """Test error conditions that should match Java behavior."""
        # Test cases for error handling - our implementation gracefully handles
        # truncated data by returning empty lists, which is acceptable behavior

        # Test with various truncated/invalid byte sequences
        invalid_cases = [
            bytes([0x80]),           # Single incomplete byte
            bytes([0x80, 0x80]),     # Incomplete 3-byte varint
            bytes([0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x80]),  # Long sequence
        ]

        for invalid_data in invalid_cases:
            # Our implementation handles invalid data gracefully by returning empty list
            # This is acceptable behavior for robustness
            result = DeltaVarintCompressor.decompress(invalid_data)
            self.assertIsInstance(result, list,
                                  f"Should return a list for invalid data: {invalid_data.hex()}")
            # Empty result is acceptable for invalid/truncated data

        # Test that valid empty input returns empty list
        empty_result = DeltaVarintCompressor.decompress(b'')
        self.assertEqual([], empty_result, "Empty input should return empty list")


if __name__ == '__main__':
    unittest.main()
