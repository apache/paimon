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
import ray

from pypaimon import CatalogFactory, Schema


class RayIntegrationTest(unittest.TestCase):
    """Tests for the top-level read_paimon() / write_paimon() API."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog_options = {'warehouse': cls.warehouse}

        catalog = CatalogFactory.create(cls.catalog_options)
        catalog.create_database('default', True)

        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, num_cpus=2)

    @classmethod
    def tearDownClass(cls):
        try:
            if ray.is_initialized():
                ray.shutdown()
        except Exception:
            pass
        try:
            shutil.rmtree(cls.tempdir)
        except OSError:
            pass

    def _create_and_populate_table(self, table_name, pa_schema, data_dict,
                                   primary_keys=None, partition_keys=None, options=None):
        """Helper to create a table and write a single batch of data."""
        identifier = 'default.{}'.format(table_name)
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=primary_keys,
            partition_keys=partition_keys,
            options=options,
        )
        catalog = CatalogFactory.create(self.catalog_options)
        catalog.create_table(identifier, schema, False)
        table = catalog.get_table(identifier)

        test_data = pa.Table.from_pydict(data_dict, schema=pa_schema)
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        return identifier

    def test_read_paimon_basic(self):
        """read_paimon() reads back the data we wrote."""
        from pypaimon.ray import read_paimon

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('value', pa.int64()),
        ])
        identifier = self._create_and_populate_table(
            'test_read_basic', pa_schema,
            {'id': [1, 2, 3], 'name': ['a', 'b', 'c'], 'value': [10, 20, 30]},
        )

        ds = read_paimon(identifier, self.catalog_options, override_num_blocks=1)
        self.assertEqual(ds.count(), 3)

        df = ds.to_pandas().sort_values('id').reset_index(drop=True)
        self.assertEqual(list(df['id']), [1, 2, 3])
        self.assertEqual(list(df['name']), ['a', 'b', 'c'])

    def test_read_paimon_with_projection(self):
        """read_paimon() respects column projection."""
        from pypaimon.ray import read_paimon

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('value', pa.int64()),
        ])
        identifier = self._create_and_populate_table(
            'test_read_proj', pa_schema,
            {'id': [1, 2], 'name': ['a', 'b'], 'value': [10, 20]},
        )

        ds = read_paimon(identifier, self.catalog_options, projection=['id', 'name'])
        df = ds.to_pandas()
        self.assertEqual(set(df.columns), {'id', 'name'})
        self.assertEqual(len(df), 2)

    def test_read_paimon_with_filter(self):
        """read_paimon() pushes down a predicate filter."""
        from pypaimon.ray import read_paimon

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('category', pa.string()),
        ])
        identifier = self._create_and_populate_table(
            'test_read_filter', pa_schema,
            {'id': [1, 2, 3], 'category': ['A', 'B', 'A']},
        )

        catalog = CatalogFactory.create(self.catalog_options)
        table = catalog.get_table(identifier)
        pb = table.new_read_builder().new_predicate_builder()
        predicate = pb.equal('category', 'A')

        ds = read_paimon(identifier, self.catalog_options, filter=predicate)
        self.assertEqual(ds.count(), 2)
        df = ds.to_pandas()
        self.assertEqual(set(df['category'].tolist()), {'A'})

    def test_read_paimon_with_limit(self):
        """``read_paimon(limit=N)`` propagates the limit into the scan plan.

        Writes 10 rows across two partitions (5 + 5) so the scan produces two
        raw-convertible splits. ``limit=3`` causes ``FileScanner`` to drop the
        second split once the first already covers the limit, so the Ray
        Dataset contains strictly fewer than the full 10 rows.

        We assert ``< 10`` (not ``== N``) because Paimon's scan-time limit is
        a per-split cap — whole-split granularity at this layer — not a
        row-exact hard limit. Row-exact short-circuiting in the reader is a
        separate follow-up.
        """
        from pypaimon.ray import read_paimon

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('part', pa.string()),
            ('value', pa.string()),
        ])
        identifier = self._create_and_populate_table(
            'test_read_limit', pa_schema,
            {
                'id': list(range(10)),
                'part': ['a'] * 5 + ['b'] * 5,
                'value': [str(i) for i in range(10)],
            },
            partition_keys=['part'],
        )

        # Sanity baseline: the full unbounded scan returns all 10 rows.
        ds_full = read_paimon(identifier, self.catalog_options)
        self.assertEqual(ds_full.count(), 10)

        # With limit=3, the scan plan drops the second partition's split
        # once the first split's row count already covers the limit.
        ds = read_paimon(identifier, self.catalog_options, limit=3)
        limited_count = ds.count()
        self.assertGreater(limited_count, 0)
        self.assertLess(limited_count, 10)

    def test_read_paimon_empty_table(self):
        """read_paimon() on a table with no data returns an empty dataset."""
        from pypaimon.ray import read_paimon

        pa_schema = pa.schema([('id', pa.int32())])
        identifier = 'default.test_read_empty'
        catalog = CatalogFactory.create(self.catalog_options)
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(identifier, schema, False)

        ds = read_paimon(identifier, self.catalog_options)
        self.assertEqual(ds.count(), 0)

    def test_write_paimon_basic(self):
        """write_paimon() writes data that read_paimon() can round-trip."""
        from pypaimon.ray import read_paimon, write_paimon

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
        ])
        identifier = 'default.test_write_basic'
        catalog = CatalogFactory.create(self.catalog_options)
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(identifier, schema, False)

        source = pa.Table.from_pydict(
            {'id': [1, 2, 3], 'name': ['x', 'y', 'z']}, schema=pa_schema,
        )
        ds = ray.data.from_arrow(source)
        write_paimon(ds, identifier, self.catalog_options)

        result = read_paimon(identifier, self.catalog_options)
        self.assertEqual(result.count(), 3)
        df = result.to_pandas().sort_values('id').reset_index(drop=True)
        self.assertEqual(list(df['name']), ['x', 'y', 'z'])

    def test_write_paimon_overwrite(self):
        """write_paimon(overwrite=True) replaces existing data."""
        from pypaimon.ray import read_paimon, write_paimon

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('val', pa.int64()),
        ])
        identifier = 'default.test_write_overwrite'
        catalog = CatalogFactory.create(self.catalog_options)
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(identifier, schema, False)

        ds1 = ray.data.from_arrow(
            pa.Table.from_pydict({'id': [1, 2], 'val': [10, 20]}, schema=pa_schema)
        )
        write_paimon(ds1, identifier, self.catalog_options)

        ds2 = ray.data.from_arrow(
            pa.Table.from_pydict({'id': [3], 'val': [30]}, schema=pa_schema)
        )
        write_paimon(ds2, identifier, self.catalog_options, overwrite=True)

        result = read_paimon(identifier, self.catalog_options)
        self.assertEqual(result.count(), 1)
        df = result.to_pandas()
        self.assertEqual(list(df['id']), [3])

    def test_read_paimon_primary_key(self):
        """read_paimon() merges PK rows correctly after an upsert."""
        from pypaimon.ray import read_paimon

        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            ('name', pa.string()),
        ])
        identifier = self._create_and_populate_table(
            'test_read_pk', pa_schema,
            {'id': [1, 2, 3], 'name': ['a', 'b', 'c']},
            primary_keys=['id'],
            options={'bucket': '2'},
        )

        catalog = CatalogFactory.create(self.catalog_options)
        table = catalog.get_table(identifier)
        update = pa.Table.from_pydict({'id': [1, 4], 'name': ['a2', 'd']}, schema=pa_schema)
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        w.write_arrow(update)
        msgs = w.prepare_commit()
        wb.new_commit().commit(msgs)
        w.close()

        ds = read_paimon(identifier, self.catalog_options)
        self.assertEqual(ds.count(), 4)
        df = ds.to_pandas().sort_values('id').reset_index(drop=True)
        self.assertEqual(list(df['name']), ['a2', 'b', 'c', 'd'])

    def test_read_paimon_invalid_override_num_blocks(self):
        """override_num_blocks below 1 is rejected with a clear error."""
        from pypaimon.ray import read_paimon

        with self.assertRaises(ValueError):
            read_paimon('default.does_not_matter', self.catalog_options,
                        override_num_blocks=0)


if __name__ == '__main__':
    unittest.main()
