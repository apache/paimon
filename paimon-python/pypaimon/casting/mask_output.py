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

"""Adapt Java mask row objects to their Arrow output representation without loss."""

import pyarrow as pa


def _integer_object(type_):
    return type_ == pa.int32() or type_ == pa.date32() or type_ == pa.time32('ms')


def _text(type_):
    return (pa.types.is_string(type_) or pa.types.is_large_string(type_)
            or getattr(pa.types, 'is_string_view', lambda _: False)(type_))


def _binary(type_):
    return (pa.types.is_binary(type_) or pa.types.is_large_binary(type_)
            or getattr(pa.types, 'is_binary_view', lambda _: False)(type_))


def cast_mask_output(array, target):
    """Decimal and Timestamp getters preserve objects, not declared precision.

    Arrow needs one fixed storage type. Loss of a fractional digit or overflow
    is an error, including when the executor itself validly returned NULL.
    """
    if array.null_count == len(array):
        return pa.nulls(len(array), type=target)
    source = array.type
    if source == target:
        return array
    if (pa.types.is_decimal(source) and pa.types.is_decimal(target)
            or pa.types.is_timestamp(source) and pa.types.is_timestamp(target)):
        # Python Arrow safe casts reject decimal rounding, timestamp truncation
        # and overflow; Rust checks the coefficients/ticks before using Arrow.
        return array.cast(target, safe=True)
    if _integer_object(source) and _integer_object(target):
        return array.cast(pa.int32()).cast(target)
    if _text(source) and _text(target) or _binary(source) and target == pa.binary():
        return array.cast(target)
    raise ValueError('Mask result %s does not have the Java row class of output %s' % (source, target))
