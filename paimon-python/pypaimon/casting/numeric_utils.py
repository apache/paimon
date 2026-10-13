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
