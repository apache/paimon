# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import struct
import unittest

from pypaimon.data._variant_float_format import (
    format_float32,
    format_float64,
)


def _float_from_bits(bits):
    return struct.unpack('>f', struct.pack('>I', bits))[0]


def _double_from_bits(bits):
    return struct.unpack('>d', struct.pack('>Q', bits))[0]


class TestVariantFloatFormat(unittest.TestCase):

    def test_float_to_string_matches_jdk8(self):
        # Generated with OpenJDK 8u492 Float.toString.
        samples = (
            (0x00000000, '0.0'),
            (0x80000000, '-0.0'),
            (0x00000001, '1.4E-45'),
            (0x007FFFFF, '1.1754942E-38'),
            (0x00800000, '1.17549435E-38'),
            (0xD44F9F82, '-3.56693731E12'),
            (0xEA75E34D, '-7.4315055E25'),
            (0xE8FDF7D7, '-9.5946444E24'),
            (0x7F7FFFFF, '3.4028235E38'),
            (0x7F800000, 'Infinity'),
            (0xFF800000, '-Infinity'),
            (0x7FC00000, 'NaN'),
        )
        for bits, expected in samples:
            with self.subTest(bits=hex(bits)):
                self.assertEqual(
                    format_float32(_float_from_bits(bits)), expected)

    def test_double_to_string_matches_jdk8(self):
        # Generated with OpenJDK 8u492 Double.toString.
        samples = (
            (0x0000000000000000, '0.0'),
            (0x8000000000000000, '-0.0'),
            (0x0000000000000001, '4.9E-324'),
            (0x000FFFFFFFFFFFFF, '2.225073858507201E-308'),
            (0x0010000000000000, '2.2250738585072014E-308'),
            (0x439F4B86CD6A5E0C, '5.6376106381000781E17'),
            (0x439DDD7467AF36D9, '5.3800104640575648E17'),
            (0x7FEFFFFFFFFFFFFF, '1.7976931348623157E308'),
            (0x7FF0000000000000, 'Infinity'),
            (0xFFF0000000000000, '-Infinity'),
            (0x7FF8000000000000, 'NaN'),
        )
        for bits, expected in samples:
            with self.subTest(bits=hex(bits)):
                self.assertEqual(
                    format_float64(_double_from_bits(bits)), expected)
