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

"""Java decimal schema evolution: decimal text, HALF_UP, then precision."""

from decimal import Context, Decimal, InvalidOperation, localcontext, ROUND_HALF_UP
import re

import pyarrow as pa

from pypaimon.casting.numeric_utils import java_floating_string


_DECIMAL_TEXT = re.compile(r'[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?\Z')


def cast_decimal(array, target):
    """Return None for conversions outside the existing decimal read support.

    FLOAT values from Arrow are already widened to Python double. Java does
    the same widening before BigDecimal.valueOf, retaining the float's binary
    rounding. A valid decimal overflowing the target is NULL; invalid text
    fails the conversion instead of silently becoming NULL.
    """
    floating = pa.types.is_floating(array.type)
    text = pa.types.is_string(array.type)
    if not pa.types.is_decimal(target) or not (floating or text or pa.types.is_decimal(array.type)):
        return None
    values = []
    # The caller's rounding traps and exponent limits must not affect reads.
    # One extra digit allows rounding to carry, before the precision check.
    context = Context(prec=target.precision + 1, rounding=ROUND_HALF_UP,
                      Emin=-999999, Emax=999999, clamp=0, traps=[InvalidOperation])
    with localcontext(context):
        quantum = Decimal(1).scaleb(-target.scale)
        for value in array.to_pylist():
            if value is None:
                values.append(None)
                continue
            if floating:
                value = java_floating_string(value)
            if floating or text:
                if _DECIMAL_TEXT.fullmatch(value) is None:
                    raise pa.ArrowInvalid('Invalid decimal schema-evolution value: %s' % value)
                try:
                    value = Decimal(value)
                except InvalidOperation as error:
                    raise pa.ArrowInvalid('Invalid decimal schema-evolution value: %s' % value) from error
            # BigDecimal validates the adjusted input scale, then setScale and
            # BigInteger.TEN.pow check their supported range before precision.
            # Zero bypasses the power check, but still needs a valid input scale.
            input_scale = -value.as_tuple().exponent
            if not -(2 ** 31) <= input_scale < 2 ** 31:
                raise pa.ArrowInvalid('Invalid decimal schema-evolution input scale')
            power = abs(target.scale - input_scale)
            if not value.is_zero() and (power > 2 ** 31 - 1 or 4 * power // 32 > (2 ** 31 - 1) // 32 + 1):
                raise pa.ArrowInvalid("Decimal scale conversion exceeds Java's supported range")
            # Avoid quantizing values whose integer part already overflows.
            if not value.is_zero() and value.adjusted() + target.scale + 1 > target.precision:
                values.append(None)
                continue
            try:
                rounded = value.quantize(quantum, rounding=ROUND_HALF_UP)
            except InvalidOperation as error:
                raise pa.ArrowInvalid('Invalid decimal schema-evolution value: %s' % value) from error
            values.append(rounded if len(rounded.as_tuple().digits) <= target.precision else None)
    return pa.array(values, type=target)
