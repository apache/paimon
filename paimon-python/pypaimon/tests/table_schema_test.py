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
#  limitations under the License.
################################################################################

import unittest

from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.schema.table_schema import TableSchema


def _bigint_field(idx: int, name: str) -> DataField:
    return DataField(idx, name, AtomicType('BIGINT', nullable=False))


def _string_field(idx: int, name: str) -> DataField:
    return DataField(idx, name, AtomicType('STRING'))


class TableSchemaBucketKeysTest(unittest.TestCase):
    """Cover the ``bucket-key`` resolution lifted onto TableSchema.

    Mirrors Java ``TableSchema.bucketKeys()`` / ``logicalBucketKeyType()``.
    """

    def _schema(self, primary_keys=None, partition_keys=None, options=None):
        fields = [
            _bigint_field(0, 'id'),
            _string_field(1, 'region'),
            _bigint_field(2, 'val'),
        ]
        return TableSchema(
            id=0,
            fields=fields,
            partition_keys=partition_keys or [],
            primary_keys=primary_keys or [],
            options=options or {},
        )

    def test_explicit_bucket_key_option_returns_those_columns(self):
        schema = self._schema(
            primary_keys=['id'],
            options={'bucket-key': 'region,val'},
        )
        self.assertEqual(schema.bucket_keys, ['region', 'val'])

        fields = schema.logical_bucket_key_fields
        self.assertEqual([f.name for f in fields], ['region', 'val'])

    def test_no_bucket_key_falls_back_to_trimmed_primary_keys(self):
        # PK includes a partition column; trimmed bucket keys drop it.
        schema = self._schema(
            primary_keys=['region', 'id'],
            partition_keys=['region'],
        )
        self.assertEqual(schema.bucket_keys, ['id'])
        self.assertEqual(
            [f.name for f in schema.logical_bucket_key_fields], ['id'])

    def test_no_bucket_key_no_primary_keys_returns_empty(self):
        schema = self._schema()
        self.assertEqual(schema.bucket_keys, [])
        self.assertEqual(schema.logical_bucket_key_fields, [])

    def test_unknown_bucket_key_column_raises(self):
        schema = self._schema(options={'bucket-key': 'nope'})
        with self.assertRaises(ValueError):
            _ = schema.bucket_keys

    def test_whitespace_only_option_falls_back(self):
        # Whitespace-only ``bucket-key`` mirrors an unset option.
        schema = self._schema(
            primary_keys=['id'],
            options={'bucket-key': '   '},
        )
        self.assertEqual(schema.bucket_keys, ['id'])


if __name__ == '__main__':
    unittest.main()
