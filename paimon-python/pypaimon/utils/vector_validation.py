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

import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.schema.data_types import ArrayType, MapType, MultisetType, RowType, VectorType


def contains_vector(data_type):
    if isinstance(data_type, VectorType):
        return True
    if isinstance(data_type, (ArrayType, MultisetType)):
        return contains_vector(data_type.element)
    if isinstance(data_type, MapType):
        return contains_vector(data_type.key) or contains_vector(data_type.value)
    if isinstance(data_type, RowType):
        return any(contains_vector(field.type) for field in data_type.fields)
    return False


def validate_vector_batch(batch, fields, write_cols=None):
    """Reject null elements in VECTORs, including nested ones; allow null vectors."""
    if batch is None:
        return
    for field in fields:
        if (field.name in batch.schema.names
                and (write_cols is None or field.name in write_cols)
                and contains_vector(field.type)):
            validate_vector_array(batch.column(field.name), field.type, field.name)


def validate_vector_array(column, data_type, path):
    if not contains_vector(data_type):
        return
    if isinstance(column, pa.ChunkedArray):
        for chunk in column.chunks:
            validate_vector_array(chunk, data_type, path)
        return
    if column.null_count == len(column):
        return
    if isinstance(data_type, VectorType):
        # flatten ignores child slots underneath null vectors and respects slices.
        # Looking at column.values alone would reject valid whole-vector nulls.
        if pc.list_flatten(column).null_count:
            raise ValueError("VECTOR column '{}' must not contain null elements".format(path))
    elif isinstance(data_type, ArrayType):
        validate_vector_array(pc.list_flatten(column), data_type.element, path + '[]')
    else:
        # Ignore children of null containers before descending into them.
        if column.null_count:
            column = pc.drop_null(column)
        if isinstance(data_type, RowType):
            for field in data_type.fields:
                validate_vector_array(column.field(field.name), field.type, path + '.' + field.name)
        elif isinstance(data_type, MapType):
            start, end = column.offsets[0].as_py(), column.offsets[-1].as_py()
            validate_vector_array(column.keys.slice(start, end - start), data_type.key, path + '.key')
            validate_vector_array(column.items.slice(start, end - start), data_type.value, path + '.value')
        elif isinstance(data_type, MultisetType):
            start, end = column.offsets[0].as_py(), column.offsets[-1].as_py()
            validate_vector_array(column.keys.slice(start, end - start), data_type.element, path + '.element')


def validate_vector_value(value, data_type, path):
    """Validate row writes without materializing unrelated blob values."""
    if value is None or not contains_vector(data_type):
        return
    if isinstance(data_type, VectorType):
        if any(element is None for element in value):
            raise ValueError("VECTOR column '{}' must not contain null elements".format(path))
    elif isinstance(data_type, ArrayType):
        for element in value:
            validate_vector_value(element, data_type.element, path + '[]')
    elif isinstance(data_type, RowType):
        for index, field in enumerate(data_type.fields):
            child = value.get(field.name) if isinstance(value, dict) else value[index]
            validate_vector_value(child, field.type, path + '.' + field.name)
    elif isinstance(data_type, (MapType, MultisetType)):
        entries = value.items() if isinstance(value, dict) else value
        for key, item in entries:
            if isinstance(data_type, MapType):
                validate_vector_value(key, data_type.key, path + '.key')
                validate_vector_value(item, data_type.value, path + '.value')
            else:
                validate_vector_value(key, data_type.element, path + '.element')
