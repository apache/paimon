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

import math

import numpy as np
import pyarrow as pa


def floating_sort_key(value):
    """Java Float/Double.compare order for a non-null scalar."""
    if math.isnan(value):
        return True, 0.0, 0.0
    return False, value, math.copysign(1.0, value)


def floating_array_sort_key(column):
    """Unsigned keys in Java Float/Double.compare order, preserving nulls."""
    # The NumPy protocol supports Array and older ChunkedArray APIs alike.
    values = np.asarray(column)
    bits = values.view(np.dtype('uint{}'.format(column.type.bit_width)))
    sign_bit = np.array(1 << (column.type.bit_width - 1), dtype=bits.dtype)
    # Invert negative IEEE bits, flip the sign bit for nonnegative values.
    # This orders -0.0 before +0.0, unlike Arrow's floating-point sort.
    keys = np.where(bits & sign_bit, ~bits, bits ^ sign_bit)
    # All NaN signs/payloads compare equal, above positive infinity.
    keys[np.isnan(values)] = np.iinfo(bits.dtype).max
    return pa.array(keys, mask=np.asarray(column.is_null()))
