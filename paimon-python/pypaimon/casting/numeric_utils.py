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

"""Java numeric text shared by partition paths and value conversions."""

import math
import struct
from decimal import Decimal


# Java Character.digit scans UTF-16 chars, so supplementary digits do not
# participate.
_DECIMAL_ZEROES = (
    0x0030, 0x0660, 0x06f0, 0x07c0, 0x0966, 0x09e6, 0x0a66, 0x0ae6, 0x0b66, 0x0be6, 0x0c66,
    0x0ce6, 0x0d66, 0x0de6, 0x0e50, 0x0ed0, 0x0f20, 0x1040, 0x1090, 0x17e0, 0x1810, 0x1946,
    0x19d0, 0x1a80, 0x1a90, 0x1b50, 0x1bb0, 0x1c40, 0x1c50, 0xa620, 0xa8d0, 0xa900, 0xa9d0,
    0xa9f0, 0xaa50, 0xabf0, 0xff10,
)


def java_decimal_digit(character):
    code = ord(character)
    for zero in _DECIMAL_ZEROES:
        if zero <= code < zero + 10:
            return code - zero
    return None


def java_floating_string(value, single_precision=False) -> str:
    # Java's JDK 19+ contract selects a shortest round-tripping form with at
    # least two significant digits before applying its notation thresholds.
    encoding = '>f' if single_precision else '>d'
    bits = struct.pack(encoding, value)
    value = struct.unpack(encoding, bits)[0]
    if math.isnan(value):
        return 'NaN'
    if math.isinf(value):
        return '-Infinity' if value < 0 else 'Infinity'
    for precision in range(2, 10 if single_precision else 18):
        text = format(value, '.{}g'.format(precision))
        try:
            rounded = struct.pack(encoding, float(text))
        except OverflowError:
            continue
        if rounded != bits:
            continue
        decimal = Decimal(text)
        if decimal.is_zero() or Decimal('0.001') <= abs(decimal) < Decimal('1e7'):
            text = format(decimal, 'f')
            if '.' in text:
                text = text.rstrip('0').rstrip('.')
            if '.' not in text:
                text += '.0'
        else:
            mantissa, exponent = format(decimal, 'e').split('e')
            mantissa = mantissa.rstrip('0').rstrip('.') if '.' in mantissa else mantissa
            if '.' not in mantissa:
                mantissa += '.0'
            text = '{}E{}'.format(mantissa, int(exponent))
        return text
    return str(value)


def java_double_decimal_text(value):
    """Numeric digits of Java 8/11/17 Double.toString for BigDecimal.valueOf.

    The pre-JDK-19 converter has an integral fast path and distinct interval
    endpoints for its fixed-width and arbitrary-precision digit generators.
    Use integer arithmetic over IEEE-754 components to preserve those choices.
    The emitted coefficient/exponent is numeric text, not a display format.
    """
    return _floating_decimal_text(value, False)


def java_number_string(value, single_precision=False):
    """Java 8/11/17 Number.toString; FLOAT uses its own rounding interval."""
    if not math.isfinite(value) or value == 0:
        return java_floating_string(value)
    text = _floating_decimal_text(value, single_precision)
    coefficient, exponent = text.split('e')
    sign = '-' if coefficient.startswith('-') else ''
    digits = coefficient.lstrip('-')
    adjusted = int(exponent) + len(digits) - 1
    digits = digits.rstrip('0')
    if adjusted < -3 or adjusted >= 7:
        return sign + digits[0] + '.' + (digits[1:] or '0') + 'E' + str(adjusted)
    point = adjusted + 1
    if point <= 0:
        return sign + '0.' + '0' * (-point) + digits
    if point >= len(digits):
        return sign + digits + '0' * (point - len(digits)) + '.0'
    return sign + digits[:point] + '.' + digits[point:]


def _floating_decimal_text(value, single_precision):
    if not math.isfinite(value):
        return java_floating_string(value)
    if value == 0:
        return '0'
    raw = struct.unpack('>Q', struct.pack('>d', value))[0]
    sign = '-' if raw >> 63 else ''
    exponent = ((raw >> 52) & 2047) - 1023
    mantissa = raw & ((1 << 52) - 1)
    if exponent == -1023:
        significant = mantissa.bit_length()
        normalize = 53 - significant
        mantissa <<= normalize
        exponent = -1022 - normalize
    else:
        significant = 53
        mantissa |= 1 << 52
    if single_precision:
        bits = struct.unpack('>I', struct.pack('>f', value))[0]
        significant = (bits & 0x007fffff).bit_length() if bits & 0x7f800000 == 0 else 24
    trailing = (mantissa & -mantissa).bit_length() - 1
    width = 53 - trailing
    tiny = max(0, width - exponent - 1)
    if -21 <= exponent <= 62 and tiny == 0:
        integer = mantissa << (exponent - 52) if exponent >= 52 else mantissa >> (52 - exponent)
        discarded = (len(str(1 << max(0, exponent - significant - 1))) - 1
                     if exponent > significant else 0)
        factor = 10 ** discarded
        return sign + str((integer + factor // 2) // factor) + 'e' + str(discarded)

    # Approximate log10 from the normalized binary significand. A leading zero
    # digit corrects an overestimate without changing the generated interval.
    normalized = mantissa / (1 << 52)
    decimal_exponent = math.floor(
        (normalized - 1.5) * 0.289529654 + 0.176091259 + exponent * 0.301029995663981)
    negative_power = max(0, -decimal_exponent)
    positive_power = max(0, decimal_exponent)
    numerator_power = negative_power + tiny + exponent - width + 1
    denominator_power = positive_power + tiny
    margin_power = negative_power + tiny + exponent - significant
    common = min(numerator_power, denominator_power)
    numerator_power -= common
    denominator_power -= common
    margin_power -= common
    if width == 1:
        margin_power -= 1
    if margin_power < 0:
        numerator_power -= margin_power
        denominator_power -= margin_power
        margin_power = 0
    numerator = (mantissa >> trailing) * 5 ** negative_power * (1 << numerator_power)
    denominator = 5 ** positive_power * (1 << denominator_power)
    margin = 5 ** negative_power * (1 << margin_power)

    def five_bits(power):
        return (5 ** power).bit_length() if 0 < power < 27 else power * 3

    bits = max(width + numerator_power + five_bits(negative_power),
               denominator_power + 1 + five_bits(positive_power + 1))
    word = 32 if bits < 32 else 64 if bits < 64 else 0

    def wrap(number):
        return (number + (1 << (word - 1))) % (1 << word) - (1 << (word - 1)) if word else number

    ten_denominator = denominator * 10
    coefficient, count = 0, 0
    while True:
        digit, numerator = divmod(numerator, denominator)
        numerator *= 10
        margin = wrap(margin * 10)
        low = numerator < margin
        high = (wrap(numerator + margin) > ten_denominator if word else
                numerator + margin >= ten_denominator)
        if count and word and margin <= 0:
            low = high = True
        if digit == 0 and count == 0 and not high:
            decimal_exponent -= 1
        else:
            coefficient = coefficient * 10 + digit
            count += 1
        if count <= 1 and (decimal_exponent < -3 or decimal_exponent >= 8):
            low = high = False
        if low or high:
            difference = wrap(2 * numerator - ten_denominator)
            if high and (not low or difference > 0 or difference == 0 and coefficient % 2):
                coefficient += 1
            return sign + str(coefficient) + 'e' + str(decimal_exponent - count + 1)
