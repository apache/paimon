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
