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

from pypaimon import Schema
from pypaimon.catalog.catalog_exception import IllegalStateError, TableNotExistException
from pypaimon.common.identifier import Identifier
from pypaimon.table.format.format_table import FormatTable
from pypaimon.tests.rest.rest_base_test import RESTBaseTest


class RESTCreatePartitionsTest(RESTBaseTest):

    def _create_table(self, table_name, partition_keys=None):
        schema = Schema.from_pyarrow_schema(
            pa.schema([("id", pa.int32()), ("dt", pa.string()), ("hh", pa.string())]),
            partition_keys=partition_keys if partition_keys is not None else ["dt", "hh"],
            options={"type": "format-table", "file.format": "parquet",
                     "metastore.partitioned-table": "true"},
        )
        self.rest_catalog.drop_table(table_name, True)
        self.rest_catalog.create_table(table_name, schema, False)
        return table_name

    def _listed_specs(self, table_name):
        return [p.spec for p in self.rest_catalog.list_partitions_paged(table_name).elements]

    def test_created_partitions_come_back_from_list(self):
        table_name = self._create_table("default.create_partitions_round_trip")
        specs = [
            {"dt": "20260807", "hh": "01"},
            {"dt": "20260807", "hh": "02"},
        ]
        self.rest_catalog.create_partitions(table_name, specs)

        listed = self._listed_specs(table_name)
        for spec in specs:
            self.assertIn(spec, listed)
        fresh = self.rest_catalog.list_partitions_paged(table_name).elements[0]
        self.assertEqual(fresh.record_count, 0)
        self.assertEqual(fresh.last_file_creation_time, 0)

    def test_creating_twice_is_a_no_op_by_default(self):
        table_name = self._create_table("default.create_partitions_idempotent")
        specs = [{"dt": "20260807", "hh": "01"}]

        self.rest_catalog.create_partitions(table_name, specs)
        self.rest_catalog.create_partitions(table_name, specs)

        listed = self._listed_specs(table_name)
        self.assertEqual(listed.count({"dt": "20260807", "hh": "01"}), 1)

    def test_creating_twice_raises_when_not_ignoring(self):
        table_name = self._create_table("default.create_partitions_strict")
        specs = [{"dt": "20260807", "hh": "01"}]
        self.rest_catalog.create_partitions(table_name, specs)

        with self.assertRaises(IllegalStateError):
            self.rest_catalog.create_partitions(table_name, specs, ignore_if_exists=False)

    def test_rejected_request_creates_nothing(self):
        table_name = self._create_table("default.create_partitions_atomic")
        self.rest_catalog.create_partitions(table_name, [{"dt": "20260807", "hh": "01"}])
        before = self._listed_specs(table_name)

        with self.assertRaises(IllegalStateError):
            self.rest_catalog.create_partitions(
                table_name,
                [{"dt": "20260808", "hh": "00"}, {"dt": "20260807", "hh": "01"}],
                ignore_if_exists=False,
            )

        self.assertEqual(self._listed_specs(table_name), before)

    def test_empty_list_leaves_the_table_untouched(self):
        table_name = self._create_table("default.create_partitions_empty")
        before = self._listed_specs(table_name)
        self.rest_catalog.create_partitions(table_name, [])
        self.assertEqual(self._listed_specs(table_name), before)

    def test_unknown_table_raises_table_not_exist(self):
        with self.assertRaises(TableNotExistException):
            self.rest_catalog.create_partitions(
                "default.create_partitions_no_such_table", [{"dt": "20260807"}])

    def test_unknown_table_raises_on_an_empty_list_too(self):
        with self.assertRaises(TableNotExistException):
            self.rest_catalog.create_partitions(
                "default.create_partitions_no_such_table", [])

    def test_registers_partitions_of_a_format_table(self):
        table_name = self._create_table("default.create_partitions_format_table")
        self.assertIsInstance(self.rest_catalog.get_table(table_name), FormatTable)

        self.rest_catalog.create_partitions(table_name, [{"dt": "20260807", "hh": "01"}])

        self.assertIn({"dt": "20260807", "hh": "01"}, self._listed_specs(table_name))

    def test_response_separates_created_from_existed(self):
        table_name = self._create_table("default.create_partitions_response")
        old = {"dt": "20260807", "hh": "01"}
        new = {"dt": "20260807", "hh": "02"}
        self.rest_catalog.create_partitions(table_name, [old])

        response = self.rest_catalog.rest_api.create_partitions(
            Identifier.from_string(table_name), [old, new], True)

        self.assertIsNotNone(response)
        self.assertEqual(response.created, [new])
        self.assertEqual(response.existed, [old])

    def test_works_for_a_single_key_partitioned_table(self):
        table_name = self._create_table(
            "default.create_partitions_single_key", partition_keys=["dt"])
        self.rest_catalog.create_partitions(table_name, [{"dt": "20260807"}])
        self.assertIn({"dt": "20260807"}, self._listed_specs(table_name))
