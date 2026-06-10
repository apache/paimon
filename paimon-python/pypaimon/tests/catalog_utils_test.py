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

import unittest

import pyarrow as pa

from pypaimon import Schema
from pypaimon.catalog.catalog_utils import validate_create_table


class CatalogUtilsTest(unittest.TestCase):

    def test_blob_format_table_requires_single_blob_data_field(self):
        options = {'type': 'format-table', 'file.format': 'blob'}

        valid = Schema.from_pyarrow_schema(
            pa.schema([
                ('payload', pa.large_binary()),
                ('dt', pa.int32()),
            ]),
            partition_keys=['dt'],
            options=options,
        )
        validate_create_table(valid)

        too_many_data_fields = Schema.from_pyarrow_schema(
            pa.schema([
                ('payload', pa.large_binary()),
                ('id', pa.int32()),
                ('dt', pa.int32()),
            ]),
            partition_keys=['dt'],
            options=options,
        )
        with self.assertRaisesRegex(
            ValueError, 'only supports one non-partition field'
        ):
            validate_create_table(too_many_data_fields)

        non_blob_data_field = Schema.from_pyarrow_schema(
            pa.schema([
                ('payload', pa.binary()),
                ('dt', pa.int32()),
            ]),
            partition_keys=['dt'],
            options=options,
        )
        with self.assertRaisesRegex(
            ValueError, 'only supports BLOB type as non-partition field'
        ):
            validate_create_table(non_blob_data_field)


if __name__ == '__main__':
    unittest.main()
