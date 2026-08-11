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

"""Format IEEE floats like ``Float/Double.toString`` on the JDK 8 baseline."""

import struct


def java_floating_text(value, single_precision=False):
    """Return the legacy Java decimal representation of an IEEE value."""
    if single_precision:
        bits = struct.unpack('>I', struct.pack('>f', value))[0]
        width, fraction_width, exponent_width, bias = 32, 23, 8, 127
    else:
        bits = struct.unpack('>Q', struct.pack('>d', value))[0]
        width, fraction_width, exponent_width, bias = 64, 52, 11, 1023

    negative = bool(bits >> (width - 1))
    exponent = ((bits >> fraction_width)
                & ((1 << exponent_width) - 1))
    fraction = bits & ((1 << fraction_width) - 1)
    if exponent == (1 << exponent_width) - 1:
        if fraction:
            return 'NaN'
        return '-Infinity' if negative else 'Infinity'
    if exponent == 0:
        if fraction == 0:
            return '-0.0' if negative else '0.0'
        highest_bit = fraction.bit_length() - 1
        shift = fraction_width - highest_bit
        significand = fraction << shift
        binary_exponent = 1 - shift - bias
        significant_bits = highest_bit + 1
    else:
        significand = (1 << fraction_width) | fraction
        binary_exponent = exponent - bias
        significant_bits = fraction_width + 1

    trailing_zeros = (significand & -significand).bit_length() - 1
    fraction_bits = fraction_width + 1 - trailing_zeros
    tiny_bits = max(0, fraction_bits - binary_exponent - 1)
    if tiny_bits == 0 and -21 <= binary_exponent <= 62:
        return _format_small_integer(
            negative,
            significand,
            fraction_width,
            binary_exponent,
            significant_bits,
        )

    decimal_exponent = _estimate_decimal_exponent(
        significand, binary_exponent, fraction_width)
    base5 = max(0, -decimal_exponent)
    base2 = base5 + tiny_bits + binary_exponent
    scale5 = max(0, decimal_exponent)
    scale2 = scale5 + tiny_bits
    margin2 = base2 - significant_bits
    reduced = significand >> trailing_zeros
    base2 -= fraction_bits - 1
    common2 = min(base2, scale2)
    base2 -= common2
    scale2 -= common2
    margin2 -= common2
    if fraction_bits == 1:
        margin2 -= 1
    if margin2 < 0:
        base2 -= margin2
        scale2 -= margin2
        margin2 = 0

    base = reduced * 5 ** base5 << base2
    scale = 5 ** scale5 << scale2
    margin = 5 ** base5 << margin2
    base_bits = fraction_bits + base2 + _five_bits(base5)
    ten_scale_bits = scale2 + 1 + _five_bits(scale5 + 1)
    arithmetic_width = (
        32 if base_bits < 32 and ten_scale_bits < 32
        else 64 if base_bits < 64 and ten_scale_bits < 64
        else None
    )
    ten_scale = scale * 10

    digits = []
    iteration = 0
    while True:
        digit, base = divmod(base, scale)
        base *= 10
        margin *= 10
        if arithmetic_width is not None:
            margin = _signed(margin, arithmetic_width)
        if arithmetic_width is None:
            low = base < margin
            high = base + margin >= ten_scale
        elif iteration == 0 or margin > 0:
            low = base < margin
            high = _signed(base + margin, arithmetic_width) > ten_scale
        else:
            low = high = True

        if iteration == 0 and digit == 0 and not high:
            decimal_exponent -= 1
        else:
            digits.append(digit)
        if iteration == 0 and (
                decimal_exponent < -3 or decimal_exponent >= 8):
            low = high = False
        iteration += 1
        if low or high:
            break

    if high:
        round_up = not low
        if low:
            if arithmetic_width is None:
                difference = 2 * base - ten_scale
            else:
                difference = _signed(
                    _signed(base << 1, arithmetic_width) - ten_scale,
                    arithmetic_width,
                )
            round_up = difference > 0 or (
                difference == 0 and digits[-1] & 1)
        if round_up:
            decimal_exponent = _round_up(digits, decimal_exponent)
    return _format(negative, digits, decimal_exponent + 1)


def _format_small_integer(
        negative,
        significand,
        fraction_width,
        binary_exponent,
        significant_bits,
):
    insignificant = 0
    if binary_exponent > significant_bits:
        power = binary_exponent - significant_bits - 1
        insignificant = len(str(1 << power)) - 1
    if binary_exponent >= fraction_width:
        integer = significand << (binary_exponent - fraction_width)
    else:
        integer = significand >> (fraction_width - binary_exponent)
    if insignificant:
        power10 = 10 ** insignificant
        integer, residue = divmod(integer, power10)
        if residue >= power10 // 2:
            integer += 1
    raw_digits = str(integer)
    digits = [int(digit) for digit in raw_digits.rstrip('0')]
    return _format(negative, digits, len(raw_digits) + insignificant)


def _estimate_decimal_exponent(
        significand, binary_exponent, fraction_width):
    normalized = significand << (52 - fraction_width)
    bits = (1023 << 52) | (normalized & ((1 << 52) - 1))
    scaled = struct.unpack('>d', struct.pack('>Q', bits))[0]
    estimate = ((scaled - 1.5) * 0.289529654 + 0.176091259
                + binary_exponent * 0.301029995663981)
    estimate_bits = struct.unpack('>Q', struct.pack('>d', estimate))[0]
    exponent = ((estimate_bits >> 52) & 0x7FF) - 1023
    negative = bool(estimate_bits >> 63)
    fraction = estimate_bits & ((1 << 52) - 1)
    if 0 <= exponent < 52:
        mask = ((1 << 52) - 1) >> exponent
        integer = ((fraction | (1 << 52)) >> (52 - exponent))
        if negative:
            return -integer if fraction & mask == 0 else -integer - 1
        return integer
    if exponent < 0:
        magnitude = estimate_bits & ((1 << 63) - 1)
        return 0 if magnitude == 0 else (-1 if negative else 0)
    return int(estimate)


def _five_bits(power):
    if power == 0:
        return 0
    return (5 ** power).bit_length() if power <= 26 else power * 3


def _signed(value, width):
    value &= (1 << width) - 1
    sign = 1 << (width - 1)
    return value - (1 << width) if value & sign else value


def _round_up(digits, decimal_exponent):
    index = len(digits) - 1
    while index >= 0 and digits[index] == 9:
        digits[index] = 0
        index -= 1
    if index < 0:
        digits[0] = 1
        return decimal_exponent + 1
    digits[index] += 1
    return decimal_exponent


def _format(negative, digits, decimal_point):
    text = ''.join(str(digit) for digit in digits)
    if 0 < decimal_point < 8:
        if len(text) < decimal_point:
            result = text + '0' * (decimal_point - len(text)) + '.0'
        else:
            result = text[:decimal_point] + '.' + (
                text[decimal_point:] or '0')
    elif -3 < decimal_point <= 0:
        result = '0.' + '0' * -decimal_point + text
    else:
        result = (text[0] + '.' + (text[1:] or '0')
                  + 'E' + str(decimal_point - 1))
    return ('-' if negative else '') + result
