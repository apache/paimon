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

"""Golden numeric digits from Java 8, 11 and 17 Double.toString."""

from decimal import Decimal
import struct

import pytest

from pypaimon.casting.numeric_utils import java_double_decimal_text


@pytest.mark.parametrize('bits,expected', [
    (0xc381e88f38c0c8fd, '-1.61304845894623136E17'),
    (0x44b52d02c7e14af6, '9.999999999999999E22'),
    (0x0000000000000001, '4.9E-324'),
    (0x0010000000000000, '2.2250738585072014E-308'),
    (0x7fefffffffffffff, '1.7976931348623157E308'),
    (0x0000000000000002, '1.0E-323'),
    (0x000fffffffffffff, '2.225073858507201E-308'),
    (0x0010000000000001, '2.225073858507202E-308'),
    (0x016fffffffffffff, '9.332636185032188E-302'),
    (0x0170000000000000, '9.332636185032189E-302'),
    (0x0170000000000001, '9.33263618503219E-302'),
    (0x39afffffffffffff, '7.888609052210117E-31'),
    (0x39b0000000000000, '7.888609052210118E-31'),
    (0x39b0000000000001, '7.88860905221012E-31'),
    (0x3befffffffffffff, '5.4210108624275216E-20'),
    (0x3bf0000000000000, '5.421010862427522E-20'),
    (0x3bf0000000000001, '5.421010862427523E-20'),
    (0x3c9fffffffffffff, '1.1102230246251564E-16'),
    (0x3ca0000000000000, '1.1102230246251565E-16'),
    (0x3ca0000000000001, '1.1102230246251568E-16'),
    (0x3e9fffffffffffff, '4.7683715820312495E-7'),
    (0x3ea0000000000000, '4.76837158203125E-7'),
    (0x3ea0000000000001, '4.768371582031251E-7'),
    (0x3fbfffffffffffff, '0.12499999999999999'),
    (0x3fc0000000000000, '0.125'),
    (0x3fc0000000000001, '0.12500000000000003'),
    (0x3fefffffffffffff, '0.9999999999999999'),
    (0x3ff0000000000000, '1.0'),
    (0x3ff0000000000001, '1.0000000000000002'),
    (0x3fffffffffffffff, '1.9999999999999998'),
    (0x4000000000000000, '2.0'),
    (0x4000000000000001, '2.0000000000000004'),
    (0x41efffffffffffff, '4.2949672959999995E9'),
    (0x41f0000000000000, '4.294967296E9'),
    (0x41f0000000000001, '4.294967296000001E9'),
    (0x432fffffffffffff, '4.5035996273704955E15'),
    (0x4330000000000000, '4.503599627370496E15'),
    (0x4330000000000001, '4.503599627370497E15'),
    (0x433fffffffffffff, '9.007199254740991E15'),
    (0x4340000000000000, '9.007199254740992E15'),
    (0x4340000000000001, '9.007199254740994E15'),
    (0x434fffffffffffff, '1.8014398509481982E16'),
    (0x4350000000000000, '1.8014398509481984E16'),
])
def test_double_decimal_digits_match_supported_jvms(bits, expected):
    value = struct.unpack('>d', struct.pack('>Q', bits))[0]
    assert Decimal(java_double_decimal_text(value)) == Decimal(expected)
