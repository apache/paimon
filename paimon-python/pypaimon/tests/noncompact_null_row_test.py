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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""NULL noncompact fields retain Java's updateable BinaryRow layout."""

from decimal import Decimal
import struct

import pyarrow as pa
import pytest

from pypaimon.schema.data_types import AtomicType, DataField, PyarrowFieldParser
from pypaimon.schema.table_schema import TableSchema
from pypaimon.table.row.generic_row import GenericRow, GenericRowDeserializer, GenericRowSerializer
from pypaimon.execution.python.write.input_routing import FixedBucketInputRouter, _hash_bytes_by_words


# Current Java InternalRowSerializer for [NULL, BinaryString("abcdefghijk")].
# Java BinaryRow.hashCode() hashes these raw bytes without Python's arity prefix.
_COMPACT = '000100000000000000000000000000000b000000180000006162636465666768696a6b0000000000'
_DECIMAL = ('000100000000000000000000180000000b00000028000000'
            '000000000000000000000000000000006162636465666768696a6b0000000000')
_TIMESTAMP = ('000100000000000000000000180000000b00000020000000'
              '00000000000000006162636465666768696a6b0000000000')


@pytest.mark.parametrize('type_name,body,java_hash', [
    ('DECIMAL(18, 2)', _COMPACT, 1425286886),
    ('DECIMAL(19, 2)', _DECIMAL, 1558048601),
    ('DECIMAL(20, 2)', _DECIMAL, 1558048601),
    ('TIMESTAMP(3)', _COMPACT, 1425286886),
    ('TIMESTAMP(4)', _TIMESTAMP, -684602128),
    ('TIMESTAMP(9)', _TIMESTAMP, -684602128),
    ('TIMESTAMP(3) WITH LOCAL TIME ZONE', _COMPACT, 1425286886),
    ('TIMESTAMP(4) WITH LOCAL TIME ZONE', _TIMESTAMP, -684602128),
    ('TIMESTAMP(9) WITH LOCAL TIME ZONE', _TIMESTAMP, -684602128),
    ('TIMESTAMP_LTZ(6)', _TIMESTAMP, -684602128),
])
def test_null_variable_field_bytes_hash_and_following_string_match_java(type_name, body, java_hash):
    fields = [DataField(0, 'key', AtomicType(type_name)), DataField(1, 'label', AtomicType('STRING'))]
    values = [None, 'abcdefghijk']
    encoded = GenericRowSerializer.to_bytes(GenericRow(values, fields))
    assert encoded == struct.pack('>i', 2) + bytes.fromhex(body)
    assert _hash_bytes_by_words(encoded[4:]) == java_hash & 0xFFFFFFFF
    assert GenericRowDeserializer.from_bytes(encoded, fields).values == values

    # Real Python input routing must retain both the NULL slot and the next
    # variable field's offset when hashing a nullable composite bucket key.
    partition = DataField(2, 'p', AtomicType('STRING'))
    schema = TableSchema(fields=fields + [partition], primary_keys=['key', 'label', 'p'], partition_keys=['p'],
                         options={'bucket': '100', 'primary-key.nullable': 'true'})
    arrow_schema = PyarrowFieldParser.from_paimon_schema(schema.fields)
    batch = pa.RecordBatch.from_arrays([
        pa.array([None], type=arrow_schema.field('key').type),
        pa.array(['abcdefghijk']), pa.array(['west']),
    ], schema=arrow_schema)
    partitions, buckets = FixedBucketInputRouter(schema).extract_partition_bucket_batch(batch)
    assert partitions == [('west',)]
    assert buckets == [abs(java_hash) % 100]


@pytest.mark.parametrize('type_name', ['DECIMAL(19, 2)', 'NUMERIC(19, 2)'])
def test_decimal_overflow_uses_the_same_noncompact_null_layout(type_name):
    fields = [DataField(0, 'key', AtomicType(type_name)), DataField(1, 'label', AtomicType('STRING'))]
    encoded = GenericRowSerializer.to_bytes(GenericRow([Decimal('99999999999999999.999'), 'abcdefghijk'], fields))
    assert encoded == struct.pack('>i', 2) + bytes.fromhex(_DECIMAL)
    assert _hash_bytes_by_words(encoded[4:]) == 1558048601
    assert GenericRowDeserializer.from_bytes(encoded, fields).values == [None, 'abcdefghijk']
