"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import pyarrow as pa

from pypaimon import Schema
from pypaimon.table.iceberg import IcebergTable
from pypaimon.tests.rest.rest_base_test import RESTBaseTest


class RESTIcebergTableTest(RESTBaseTest):

    def test_get_iceberg_table(self):
        schema = Schema.from_pyarrow_schema(
            pa.schema([("id", pa.int32()), ("dt", pa.string())]),
            partition_keys=["dt"],
            options={"type": "iceberg-table"},
        )
        table_name = "default.iceberg_table_basic"
        self.rest_catalog.drop_table(table_name, True)
        self.rest_catalog.create_table(table_name, schema, False)

        table = self.rest_catalog.get_table(table_name)
        self.assertIsInstance(table, IcebergTable)
        self.assertEqual(table.name(), "iceberg_table_basic")
        self.assertEqual(table.full_name(), table_name)
        self.assertEqual(table.partition_keys, ["dt"])
        self.assertEqual(table.primary_keys, [])
        self.assertEqual(table.options().get("type"), "iceberg-table")
        self.assertTrue(table.location().startswith("file://"))

    def test_iceberg_table_unsupported_read_write(self):
        schema = Schema.from_pyarrow_schema(
            pa.schema([("id", pa.int32())]),
            options={"type": "iceberg-table"},
        )
        table_name = "default.iceberg_table_unsupported_ops"
        self.rest_catalog.drop_table(table_name, True)
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)

        with self.assertRaises(NotImplementedError):
            table.new_read_builder()
        with self.assertRaises(NotImplementedError):
            table.new_batch_write_builder()
        with self.assertRaises(NotImplementedError):
            table.new_stream_write_builder()

    def test_iceberg_table_unsupported_drop_partitions(self):
        schema = Schema.from_pyarrow_schema(
            pa.schema([("id", pa.int32()), ("dt", pa.string())]),
            partition_keys=["dt"],
            options={"type": "iceberg-table"},
        )
        table_name = "default.iceberg_table_unsupported_drop_partitions"
        self.rest_catalog.drop_table(table_name, True)
        self.rest_catalog.create_table(table_name, schema, False)

        with self.assertRaisesRegex(
                ValueError,
                "drop_partitions is not supported for table type 'IcebergTable'",
        ):
            self.rest_catalog.drop_partitions(
                table_name,
                [{"dt": "20250101"}],
            )
