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

from decimal import Decimal

import pyarrow as pa
import pytest

from pypaimon.casting.schema_evolution import cast_array_for_schema_evolution
from pypaimon.schema.data_types import AtomicType


@pytest.mark.parametrize('source_type,values,target_type,expected', [
    (pa.float32(), [128.9, -129.9, 1e30, -1e30, None], pa.int8(), [-128, 127, -1, 0, None]),
    (pa.float64(), [32768.9, -32769.9, 1e30, -1e30, None], pa.int16(), [-32768, 32767, -1, 0, None]),
    (pa.float64(), [42.9, -42.9, 1e30, -1e30, None], pa.int32(),
     [42, -42, 2 ** 31 - 1, -(2 ** 31), None]),
    (pa.float64(), [42.9, -42.9, 1e30, -1e30, None], pa.int64(),
     [42, -42, 2 ** 63 - 1, -(2 ** 63), None]),
    (pa.decimal128(38, 2),
     [Decimal('999999999999999999999999999999999999.99'), Decimal('-42.99'), None],
     pa.int64(), [-5527149226598858753, -42, None]),
])
@pytest.mark.parametrize('chunked', [False, True])
def test_java_numeric_narrowing_of_sliced_and_chunked_arrays(source_type, values, target_type, expected, chunked):
    source = pa.array([None] + values + [None], type=source_type).slice(1, len(values))
    if chunked:
        source = pa.chunked_array([source.slice(0, 2), source.slice(2)])
    output = cast_array_for_schema_evolution(source, target_type)
    assert output.type == target_type
    assert output.to_pylist() == expected


@pytest.mark.parametrize('zone', [None, 'UTC'])
@pytest.mark.parametrize('precision', [7, 8])
def test_timestamp_floor_overflow_returns_null(zone, precision):
    source = pa.array([-(2 ** 63), -(2 ** 63) + 1000, -1, None], type=pa.timestamp('ns', zone))
    name = 'TIMESTAMP_LTZ' if zone else 'TIMESTAMP'
    output = cast_array_for_schema_evolution(
        source, source.type, AtomicType(name + '(9)'), AtomicType('%s(%d)' % (name, precision)))
    quantum = 10 ** (9 - precision)
    assert output.cast(pa.int64()).to_pylist() == [None, (-(2 ** 63) + 1000) // quantum * quantum, -quantum, None]
