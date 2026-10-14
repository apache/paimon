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

"""Authorization outputs preserve Java object values in canonical Arrow storage."""

from decimal import Decimal

import pyarrow as pa
import pytest

from pypaimon.casting.mask_output import cast_mask_output
from pypaimon.read.reader.auth_masking_reader import apply_auth_masking


@pytest.mark.parametrize('source', [pa.int32(), pa.date32(), pa.time32('ms')])
@pytest.mark.parametrize('target', [pa.int32(), pa.date32(), pa.time32('ms')])
def test_integer_class_masks_preserve_raw_values(source, target):
    values = pa.array([-1, 123, None], type=source)
    actual = cast_mask_output(values, target)
    assert actual.cast(pa.int32()).to_pylist() == [-1, 123, None]


@pytest.mark.parametrize('type_name,target,values', [
    ('large_string', pa.string(), ['a', None]), ('string_view', pa.string(), ['a', None]),
    ('large_binary', pa.binary(), [b'a', None]), ('binary_view', pa.binary(), [b'a', None]),
])
def test_equivalent_arrow_string_and_byte_encodings_are_normalized(type_name, target, values):
    factory = getattr(pa, type_name, None)
    if factory is None:
        pytest.skip('Physical view encoding is unavailable in this PyArrow version')
    actual = cast_mask_output(pa.array(values, type=factory()), target)
    assert actual.type == target and actual.to_pylist() == values


def test_decimal_mask_output_never_rounds_or_overflows():
    target = pa.decimal128(4, 2)
    actual = cast_mask_output(pa.array([Decimal('2.670'), None], type=pa.decimal128(7, 3)), target)
    assert actual.to_pylist() == [Decimal('2.67'), None]
    for value in ('2.675', '-2.675', '1000.000'):
        with pytest.raises(pa.ArrowInvalid):
            cast_mask_output(pa.array([Decimal(value)], type=pa.decimal128(7, 3)), target)


def test_timestamp_mask_output_never_truncates_or_overflows():
    target = pa.timestamp('ms')
    actual = cast_mask_output(pa.array([1000, None], type=pa.timestamp('us')), target)
    assert actual.cast(pa.int64()).to_pylist() == [1, None]
    for value in (1001, -1001):
        with pytest.raises(pa.ArrowInvalid):
            cast_mask_output(pa.array([value], type=pa.timestamp('us')), target)
    with pytest.raises(pa.ArrowInvalid):
        cast_mask_output(pa.array([(1 << 63) - 1], type=target), pa.timestamp('ns'))


def test_masking_preserves_field_and_schema_metadata():
    schema = pa.schema([pa.field('v', pa.string(), nullable=False, metadata={b'field': b'id'})],
                       metadata={b'table': b'contract'})
    batch = pa.RecordBatch.from_arrays([pa.array(['secret'])], schema=schema)
    output = apply_auth_masking(batch, {'v': {'name': 'NULL'}})
    assert output.column(0).to_pylist() == [None]
    assert output.schema.field(0).nullable
    assert output.schema.field(0).metadata == schema.field(0).metadata
    assert output.schema.metadata == schema.metadata
    assert not batch.schema.field(0).nullable


@pytest.mark.parametrize('values', [[], [None, None]])
def test_null_and_empty_masks_skip_runtime_class_check(values):
    output = cast_mask_output(pa.array(values, type=pa.string()), pa.int32())
    assert output.type == pa.int32() and output.to_pylist() == values
    with pytest.raises(ValueError):
        cast_mask_output(pa.array(['bad', None]), pa.int32())
