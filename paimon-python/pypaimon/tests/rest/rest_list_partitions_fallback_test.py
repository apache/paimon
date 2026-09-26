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

from pypaimon import Schema
from pypaimon.api.rest_exception import NotImplementedException
from pypaimon.common.identifier import Identifier
from pypaimon.tests.rest.rest_base_test import RESTBaseTest


class RESTListPartitionsFallbackTest(RESTBaseTest):
    """When the REST server does not implement partition listing, the catalog
    must fall back to computing partitions from the table's own metadata,
    mirroring the Java RESTCatalog."""

    def _create_partitioned_table(self, name):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.rest_catalog.create_table(name, schema, False)
        table = self.rest_catalog.get_table(name)
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(self.expected)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()
        return Identifier.from_string(name)

    def _make_endpoint_not_implemented(self):
        def _raise(*args, **kwargs):
            raise NotImplementedException("partition listing not implemented")
        self.rest_catalog.rest_api.list_partitions_paged = _raise

    def _create_format_table(self, name):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            partition_keys=['dt'],
            options={"type": "format-table", "file.format": "parquet"})
        self.rest_catalog.create_table(name, schema, False)
        table = self.rest_catalog.get_table(name)
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(self.expected)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()
        return Identifier.from_string(name)

    def test_falls_back_to_metadata(self):
        identifier = self._create_partitioned_table('default.parts_fallback')
        self._make_endpoint_not_implemented()

        result = self.rest_catalog.list_partitions_paged(identifier)

        specs = sorted(tuple(sorted(p.spec.items())) for p in result.elements)
        self.assertEqual(specs, [(('dt', 'p1'),), (('dt', 'p2'),)])
        # Stats come from the manifests, not zero placeholders.
        by_dt = {p.spec['dt']: p for p in result.elements}
        self.assertEqual(by_dt['p1'].record_count, 4)
        self.assertEqual(by_dt['p2'].record_count, 4)
        for p in result.elements:
            self.assertGreater(p.file_count, 0)

    def test_format_table_still_raises(self):
        # The manifest-based fallback only supports data tables. Unlike Java
        # (which scans an unmanaged format table's directory), PyPaimon does
        # not list format-table partitions here, so the server's
        # NotImplemented error must surface rather than be silently swallowed.
        identifier = self._create_format_table('default.parts_fallback_format')
        self._make_endpoint_not_implemented()

        with self.assertRaises(NotImplementedException):
            self.rest_catalog.list_partitions_paged(identifier)

    def test_fallback_honors_pattern_and_pagination(self):
        identifier = self._create_partitioned_table('default.parts_fallback_paged')
        self._make_endpoint_not_implemented()

        only_p1 = self.rest_catalog.list_partitions_paged(
            identifier, partition_name_pattern="dt=p1")
        self.assertEqual([p.spec for p in only_p1.elements], [{"dt": "p1"}])

        first = self.rest_catalog.list_partitions_paged(identifier, max_results=1)
        self.assertEqual(len(first.elements), 1)
        self.assertIsNotNone(first.next_page_token)
        second = self.rest_catalog.list_partitions_paged(
            identifier, max_results=1, page_token=first.next_page_token)
        self.assertEqual(len(second.elements), 1)
        self.assertIsNone(second.next_page_token)
        self.assertNotEqual(first.elements[0].spec, second.elements[0].spec)
