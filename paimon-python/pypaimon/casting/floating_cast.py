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

"""Java float text, rounding directly to the target IEEE format."""

import math
import re
import struct


_TRIM = ''.join(chr(i) for i in range(33))
_DECIMAL = re.compile(r'[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?[fFdD]?\Z')
_HEX = re.compile(r'[+-]?0[xX](?:[0-9a-fA-F]+(?:\.[0-9a-fA-F]*)?|\.[0-9a-fA-F]+)'
                  r'[pP][+-]?[0-9]+[fFdD]?\Z')


def _exponent(text):
    negative = text.startswith('-')
    digits = text.lstrip('+-').lstrip('0')
    if len(digits) > 19:
        return -(1 << 63) if negative else (1 << 63) - 1
    value = int(digits or '0')
    return -value if negative else value


def _ieee_ratio(numerator, denominator, negative, single):
    fraction, bias, packing = (23, 127, '>I') if single else (52, 1023, '>Q')
    unpacking = '>f' if single else '>d'
    sign = int(negative) << (31 if single else 63)

    def build(bits):
        return struct.unpack(unpacking, struct.pack(packing, bits))[0]

    if not numerator:
        return build(sign)
    leading = numerator.bit_length() - denominator.bit_length()
    if (numerator < denominator << leading if leading >= 0 else numerator << (-leading) < denominator):
        leading -= 1
    if leading > bias:
        return -math.inf if negative else math.inf
    minimum = 1 - bias
    if leading < minimum - fraction - 1:
        return build(sign)
    unit = max(leading, minimum) - fraction
    if unit >= 0:
        denominator <<= unit
    else:
        numerator <<= -unit
    significand, remainder = divmod(numerator, denominator)
    if 2 * remainder > denominator or 2 * remainder == denominator and significand % 2:
        significand += 1
    if significand == 1 << (fraction + 1):
        significand >>= 1
        unit += 1
    normal = unit + fraction
    if normal > bias:
        return -math.inf if negative else math.inf
    bits = significand if significand < 1 << fraction else (
        (normal + bias) << fraction | (significand - (1 << fraction)))
    return build(sign | bits)


def parse_java_float(text, single=False):
    value = text.strip(_TRIM)
    unsigned = value[1:] if value.startswith(('+', '-')) else value
    if unsigned == 'NaN':
        return math.nan
    if unsigned == 'Infinity':
        return -math.inf if value.startswith('-') else math.inf
    hexadecimal = _HEX.fullmatch(value) is not None
    if not hexadecimal and _DECIMAL.fullmatch(value) is None:
        raise ValueError("Invalid Java floating value: '%s'" % text)
    if value[-1:] in ('f', 'F', 'd', 'D'):
        value = value[:-1]
    negative = value.startswith('-')
    value = value.lstrip('+-')
    if hexadecimal:
        mantissa, exponent = re.split('[pP]', value[2:])
        whole, _, fraction = mantissa.partition('.')
        numerator = int(whole + fraction, 16)
        exponent = _exponent(exponent) - 4 * len(fraction)
        leading = numerator.bit_length() - 1 + exponent if numerator else 0
        bias, subnormal = (127, -150) if single else (1023, -1075)
        if numerator and leading > bias:
            return -math.inf if negative else math.inf
        if not numerator or leading < subnormal:
            return _ieee_ratio(0, 1, negative, single)
        return _ieee_ratio(numerator << max(0, exponent), 1 << max(0, -exponent), negative, single)
    parts = re.split('[eE]', value)
    whole, _, fraction = parts[0].partition('.')
    digits = (whole + fraction).lstrip('0')
    exponent = (_exponent(parts[1]) if len(parts) == 2 else 0) - len(fraction)
    adjusted = len(digits) - 1 + exponent
    high, low = (38, -46) if single else (308, -324)
    if not digits or adjusted < low:
        return _ieee_ratio(0, 1, negative, single)
    if adjusted > high:
        return -math.inf if negative else math.inf
    # Every IEEE midpoint has fewer than 800 significant decimal digits.
    # Retain a sticky digit beyond that boundary instead of building unbounded
    # decimal powers for a long input that is close to a rounding midpoint.
    if len(digits) > 800:
        remainder = digits[800:]
        exponent += len(remainder)
        digits = digits[:800]
        if any(ch != '0' for ch in remainder):
            digits += '1'
            exponent -= 1
    numerator = int(digits) * 10 ** max(0, exponent)
    denominator = 10 ** max(0, -exponent)
    return _ieee_ratio(numerator, denominator, negative, single)
