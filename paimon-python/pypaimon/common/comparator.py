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
from typing import Any

import numpy as np


FLOATING_TYPES = (float, np.floating)


def compare_values(left: Any, right: Any) -> int:
    """Nulls first; floats follow Java ordering: NaNs tie last, -0.0 < +0.0."""
    if left is None:
        return 0 if right is None else -1
    if right is None:
        return 1
    if isinstance(left, FLOATING_TYPES) and isinstance(right, FLOATING_TYPES):
        if math.isnan(left):
            return 0 if math.isnan(right) else 1
        if math.isnan(right):
            return -1
        if left == right == 0.0:
            left, right = math.copysign(1.0, left), math.copysign(1.0, right)
    return -1 if left < right else (1 if left > right else 0)
