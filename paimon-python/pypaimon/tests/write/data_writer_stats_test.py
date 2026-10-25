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
from typing import List, Optional, Union

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from pypaimon.execution.python.write.writer.data_writer import DataWriter


def inputs(array: pa.Array) -> List[Union[pa.RecordBatch, pa.Table]]:
    # Exercise the batch interface and extrema in different table chunks.
    chunks = [array.slice(0, 2), pa.array([], type=array.type), array.slice(2)]
    return [
        pa.record_batch([array], names=['value']),
        pa.table({'value': pa.chunked_array(chunks)}),
    ]


@pytest.mark.parametrize(
    'dtype',
    [
        pa.int8(),
        pa.int16(),
        pa.int32(),
        pa.int64(),
        pa.uint32(),
        pa.uint64(),
        pa.float32(),
        pa.float64(),
        pa.decimal128(12, 2),
        pa.decimal256(40, 3),
        pa.date32(),
        pa.date64(),
        pa.time32('s'),
        pa.time64('us'),
        pa.timestamp('us'),
        pa.timestamp('us', tz='UTC'),
    ],
)
def test_numeric_and_temporal_column_stats(dtype: pa.DataType) -> None:
    array = pa.array([None, 3, 1, 2, None], type=dtype)
    for data in inputs(array):
        assert DataWriter._get_column_stats(data, 'value') == {
            'min_values': array[2].as_py(),
            'max_values': array[1].as_py(),
            'null_counts': 2,
        }


@pytest.mark.parametrize(
    'dtype', [pa.string(), pa.large_string(), pa.binary(), pa.binary(1)]
)
def test_string_and_binary_column_stats(dtype: pa.DataType) -> None:
    binary = pa.types.is_binary(dtype) or pa.types.is_fixed_size_binary(dtype)
    array = pa.array(
        [b'z', None, b'a', b'm'] if binary else ['z', None, 'a', 'm'], type=dtype
    )
    for data in inputs(array):
        assert DataWriter._get_column_stats(data, 'value') == {
            'min_values': array[2].as_py(),
            'max_values': array[0].as_py(),
            'null_counts': 1,
        }


def test_boolean_column_stats() -> None:
    for data in inputs(pa.array([True, None, False])):
        assert DataWriter._get_column_stats(data, 'value') == {
            'min_values': False,
            'max_values': True,
            'null_counts': 1,
        }


@pytest.mark.parametrize(
    'dtype',
    [pa.int64(), pa.string(), pa.bool_(), pa.timestamp('us'), pa.list_(pa.int32())],
)
@pytest.mark.parametrize('size', [0, 3])
def test_empty_and_all_null_column_stats(dtype: pa.DataType, size: int) -> None:
    for data in inputs(pa.nulls(size, type=dtype)):
        assert DataWriter._get_column_stats(data, 'value') == {
            'min_values': None,
            'max_values': None,
            'null_counts': size,
        }


@pytest.mark.parametrize(
    'array',
    [
        pa.array([[1], None, [2]], type=pa.list_(pa.int32())),
        pa.array([{'x': 1}, None], type=pa.struct([('x', pa.int32())])),
        pa.array([[('x', 1)], None], type=pa.map_(pa.string(), pa.int32())),
        pa.array([b'z', None, b'a'], type=pa.large_binary()),
    ],
)
def test_columns_without_extrema_keep_null_counts(array: pa.Array) -> None:
    for data in inputs(array):
        assert DataWriter._get_column_stats(data, 'value') == {
            'min_values': None,
            'max_values': None,
            'null_counts': 1,
        }


@pytest.mark.parametrize('dtype', [pa.float32(), pa.float64()])
@pytest.mark.parametrize(
    'values',
    [
        [float('nan'), None, 3.0, -1.0],
        [float('nan'), None, float('nan')],
        [float('inf'), None, -float('inf')],
        [-0.0, 0.0, None],
        [0.0, -0.0, None],
        [-0.0, None, -0.0],
    ],
)
def test_floating_column_stats_preserve_arrow_semantics(
    dtype: pa.DataType, values: List[Optional[float]]
) -> None:
    for data in inputs(pa.array(values, type=dtype)):
        array = data.column('value')
        actual = DataWriter._get_column_stats(data, 'value')
        assert actual['null_counts'] == array.null_count
        for name, expected in [
            ('min_values', pc.call_function('min', [array]).as_py()),
            ('max_values', pc.call_function('max', [array]).as_py()),
        ]:
            value = actual[name]
            if math.isnan(expected):
                assert math.isnan(value)
            else:
                assert value == expected
                if value == 0.0:
                    assert math.copysign(1.0, value) == math.copysign(1.0, expected)
