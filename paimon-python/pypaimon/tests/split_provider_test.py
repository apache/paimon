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

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.read.datasource.split_provider import (
    CatalogSplitProvider,
    PreResolvedSplitProvider,
)


class SplitProviderTest(unittest.TestCase):
    """Unit tests for the two SplitProvider implementations."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog_options = {'warehouse': cls.warehouse}

        catalog = CatalogFactory.create(cls.catalog_options)
        catalog.create_database('default', True)

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
        ])
        cls.identifier = 'default.split_provider_test'
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(cls.identifier, schema, False)
        table = catalog.get_table(cls.identifier)

        data = pa.Table.from_pydict(
            {'id': [1, 2, 3], 'name': ['a', 'b', 'c']}, schema=pa_schema
        )
        wb = table.new_batch_write_builder()
        writer = wb.new_write()
        writer.write_arrow(data)
        wb.new_commit().commit(writer.prepare_commit())
        writer.close()

    @classmethod
    def tearDownClass(cls):
        try:
            shutil.rmtree(cls.tempdir)
        except OSError:
            pass

    def test_catalog_provider_resolves_table_and_splits(self):
        """CatalogSplitProvider does the catalog→table→ReadBuilder→Scan dance lazily."""
        provider = CatalogSplitProvider(
            table_identifier=self.identifier,
            catalog_options=self.catalog_options,
        )

        self.assertIsNone(provider._table_cached)
        self.assertIsNone(provider._splits_cached)
        self.assertIsNone(provider._read_type_cached)

        table = provider.table()
        self.assertIsNotNone(table)
        self.assertIs(provider.table(), table)  # cached

        splits = provider.splits()
        self.assertGreater(len(splits), 0)
        self.assertIs(provider.splits(), splits)  # cached
        self.assertIsNotNone(provider.read_type())
        self.assertIsNone(provider.predicate())

    def test_catalog_provider_propagates_projection(self):
        """``projection`` reaches ``ReadBuilder.with_projection`` (visible via read_type)."""
        provider = CatalogSplitProvider(
            table_identifier=self.identifier,
            catalog_options=self.catalog_options,
            projection=['id'],
        )

        read_type = provider.read_type()
        field_names = [f.name for f in read_type]
        self.assertEqual(field_names, ['id'])

    def test_catalog_provider_propagates_predicate(self):
        """``predicate`` is held on the provider and surfaced via predicate()."""
        catalog = CatalogFactory.create(self.catalog_options)
        table = catalog.get_table(self.identifier)
        pb = table.new_read_builder().new_predicate_builder()
        pred = pb.equal('id', 2)

        provider = CatalogSplitProvider(
            table_identifier=self.identifier,
            catalog_options=self.catalog_options,
            predicate=pred,
        )

        self.assertIs(provider.predicate(), pred)

    def test_catalog_provider_propagates_limit(self):
        """``limit`` reaches ``ReadBuilder.with_limit``: splits are pruned once
        the per-split row budget is met. Uses a fresh partitioned table so
        each commit produces its own split."""
        pa_schema = pa.schema([('id', pa.int32()), ('name', pa.string())])
        identifier = 'default.split_provider_limit'
        schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=['id'])
        catalog = CatalogFactory.create(self.catalog_options)
        catalog.create_table(identifier, schema, False)
        table = catalog.get_table(identifier)
        for i in range(3):
            data = pa.Table.from_pydict({'id': [i], 'name': [f'r{i}']}, schema=pa_schema)
            wb = table.new_batch_write_builder()
            writer = wb.new_write()
            writer.write_arrow(data)
            wb.new_commit().commit(writer.prepare_commit())
            writer.close()

        unlimited = CatalogSplitProvider(
            table_identifier=identifier, catalog_options=self.catalog_options,
        )
        limited = CatalogSplitProvider(
            table_identifier=identifier, catalog_options=self.catalog_options,
            limit=1,
        )

        # Three single-row commits → three splits; limit=1 prunes after the
        # first split meets the budget.
        self.assertEqual(len(unlimited.splits()), 3)
        self.assertLess(len(limited.splits()), len(unlimited.splits()))

    def test_catalog_provider_requires_identifier_and_options(self):
        with self.assertRaises(ValueError):
            CatalogSplitProvider(
                table_identifier='', catalog_options=self.catalog_options
            )
        with self.assertRaises(ValueError):
            CatalogSplitProvider(
                table_identifier=self.identifier, catalog_options=None
            )

    def test_pre_resolved_provider_returns_inputs(self):
        """PreResolvedSplitProvider just hands back what it was given."""
        catalog = CatalogFactory.create(self.catalog_options)
        table = catalog.get_table(self.identifier)
        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        read_type = rb.read_type()

        provider = PreResolvedSplitProvider(
            table=table, splits=splits, read_type=read_type, predicate=None
        )

        self.assertIs(provider.table(), table)
        self.assertIs(provider.splits(), splits)
        self.assertIs(provider.read_type(), read_type)
        self.assertIsNone(provider.predicate())


if __name__ == '__main__':
    unittest.main()
