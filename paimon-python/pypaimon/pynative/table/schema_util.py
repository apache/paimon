################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import pyarrow as pa
import re

from pypaimon.api.schema import Schema
from pypaimon.pynative.common.data_field import DataType, DataField


def convert_pa_schema_to_data_fields(pa_schema: pa.Schema) -> list[DataField]:
    fields = []
    for i, field in enumerate(pa_schema):
        field: pa.Field
        type_name = str(field.type)
        if type_name.startswith('int'):
            type_name = 'INT'
        elif type_name.startswith('float'):
            type_name = 'FLOAT'
        elif type_name.startswith('double'):
            type_name = 'DOUBLE'
        elif type_name.startswith('bool'):
            type_name = 'BOOLEAN'
        elif type_name.startswith('string'):
            type_name = 'STRING'
        elif type_name.startswith('binary'):
            type_name = 'BINARY'
        elif type_name.startswith('date'):
            type_name = 'DATE'
        elif type_name.startswith('timestamp'):
            type_name = 'TIMESTAMP'
        elif type_name.startswith('decimal'):
            match = re.match(r'decimal\((\d+),\s*(\d+)\)', type_name)
            if match:
                precision, scale = map(int, match.groups())
                type_name = f'DECIMAL({precision},{scale})'
            else:
                type_name = 'DECIMAL(38,18)'
        elif type_name.startswith('list'):
            type_name = 'ARRAY'
        elif type_name.startswith('struct'):
            type_name = 'STRUCT'
        elif type_name.startswith('map'):
            type_name = 'MAP'
        data_type = DataType(type_name, field.nullable)

        data_field = DataField(
            id=i,
            name=field.name,
            type=data_type,
            description=field.metadata.get(b'description', b'').decode
            ('utf-8') if field.metadata and b'description' in field.metadata else None
        )
        fields.append(data_field)

    return fields


def convert_data_fields_to_pa_schema(fields: list[DataField]) -> pa.Schema:
    """Convert a list of DataField to PyArrow Schema."""
    pa_fields = []
    for field in fields:
        type_name = field.type.type_name.upper()
        if type_name == 'INT':
            type_name = pa.int32()
        elif type_name == 'BIGINT':
            type_name = pa.int64()
        elif type_name == 'FLOAT':
            type_name = pa.float32()
        elif type_name == 'DOUBLE':
            type_name = pa.float64()
        elif type_name == 'BOOLEAN':
            type_name = pa.bool_()
        elif type_name == 'STRING':
            type_name = pa.string()
        elif type_name == 'BINARY':
            type_name = pa.binary()
        elif type_name == 'DATE':
            type_name = pa.date32()
        elif type_name == 'TIMESTAMP':
            type_name = pa.timestamp('ms')
        elif type_name.startswith('DECIMAL'):
            match = re.match(r'DECIMAL\((\d+),\s*(\d+)\)', type_name)
            if match:
                precision, scale = map(int, match.groups())
                type_name = pa.decimal128(precision, scale)
            else:
                type_name = pa.decimal128(38, 18)
        elif type_name == 'ARRAY':
            # TODO: support arra / struct / map element type
            type_name = pa.list_(pa.string())
        elif type_name == 'STRUCT':
            type_name = pa.struct([])
        elif type_name == 'MAP':
            type_name = pa.map_(pa.string(), pa.string())
        else:
            raise ValueError(f"Unsupported data type: {type_name}")
        metadata = {}
        if field.description:
            metadata[b'description'] = field.description.encode('utf-8')
        pa_fields.append(pa.field(field.name, type_name, nullable=field.type.nullable, metadata=metadata))
    return pa.schema(pa_fields)


def get_highest_field_id(fields: list) -> int:
    return max(field.id for field in fields)


def check_schema_for_external_table(exists_schema: Schema, new_schema: Schema):
    """Check if the new schema is compatible with the existing schema for external table."""
    if ((not new_schema.pa_schema or new_schema.pa_schema.equals(exists_schema.pa_schema))
            and (not new_schema.partition_keys or new_schema.partition_keys == exists_schema.partition_keys)
            and (not new_schema.primary_keys or new_schema.primary_keys == exists_schema.primary_keys)):
        exists_options = exists_schema.options
        new_options = new_schema.options
        for key, value in new_options.items():
            if (key != 'owner' and key != 'path'
                    and (key not in exists_options or exists_options[key] != value)):
                raise ValueError(
                    f"New schema's options are not equal to the exists schema's, "
                    f"new schema: {new_options}, exists schema: {exists_options}")
    else:
        raise ValueError(
            f"New schema is not equal to the exists schema, "
            f"new schema: {new_schema}, exists schema: {exists_schema}")
