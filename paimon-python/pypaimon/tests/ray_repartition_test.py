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

"""End-to-end tests for HASH_FIXED Ray writes.

For append-only HASH_FIXED tables, ``write_paimon`` writes rows to the
correct bucket by default without pre-clustering. HASH_FIXED
primary-key tables fail fast unless the legacy ``map_groups`` mode is
explicitly selected. These tests cover:

  * default roundtrip correctness on an append-only HASH_FIXED table.
  * default fail-fast behaviour on a HASH_FIXED PK table.
  * roundtrip correctness on a partitioned HASH_FIXED PK table.
  * explicit ``map_groups`` mode strips the transient bucket column
    from the sink-visible schema.
  * explicit ``map_groups`` mode can produce one file per
    (partition, bucket) on the small test dataset.
  * regression: a table whose schema already contains a column named
    ``__paimon_bucket__`` still works (collision-safe column name).
  * non-HASH_FIXED append-only tables pass through unchanged.
  * dynamic-bucket primary-key tables fail fast, while postpone-bucket
    primary-key tables pass through.
"""

import glob
import os
import shutil
import tempfile
import unittest

import pyarrow as pa
import ray

from pypaimon import CatalogFactory, Schema


class RayShuffleTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog_options = {'warehouse': cls.warehouse}

        catalog = CatalogFactory.create(cls.catalog_options)
        catalog.create_database('default', True)

        if not ray.is_initialized():
            # 4 CPUs gives us enough room to actually fan a multi-block
            # write across multiple workers so the "small-file" claim
            # is observable.
            ray.init(ignore_reinit_error=True, num_cpus=4)

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

    def _make_table(self, table_name, pa_schema, *, primary_keys=None,
                    partition_keys=None, options=None):
        identifier = 'default.{}'.format(table_name)
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=primary_keys,
            partition_keys=partition_keys,
            options=options,
        )
        catalog = CatalogFactory.create(self.catalog_options)
        catalog.create_table(identifier, schema, False)
        return identifier

    def _read_table(self, identifier):
        """Read table data via the direct API (not ``read_paimon``).

        This avoids going through ``RayDatasource._get_read_task`` which
        has a pre-existing strict nullability check (``from_batches``
        with Paimon schema) that rejects batches where the reader drops
        ``not null`` (a raw-convertible PK split issue). Shuffle tests
        care about *write* correctness, not the Ray read path.
        """
        catalog = CatalogFactory.create(self.catalog_options)
        table = catalog.get_table(identifier)
        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        arrow = rb.new_read().to_arrow(splits)
        return arrow.to_pandas() if arrow is not None else pa.table({}).to_pandas()

    def _count_data_files(self, table_name):
        """All data files under the table directory, regardless of partition."""
        root = os.path.join(self.warehouse, 'default.db', table_name)
        patterns = ['*.parquet', '*.orc', '*.avro']
        files = []
        for pattern in patterns:
            files.extend(glob.glob(
                os.path.join(root, '**', 'bucket-*', pattern), recursive=True,
            ))
        return files

    # ----- HASH_FIXED writes -----

    def test_append_only_fixed_bucket_roundtrip(self):
        from pypaimon.ray import write_paimon

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
        ])
        table_name = 'test_append_only_fixed_bucket_roundtrip'
        identifier = self._make_table(
            table_name, pa_schema, options={'bucket': '4'},
        )

        rows = pa.Table.from_pydict(
            {'id': list(range(40)), 'name': [f'v{i}' for i in range(40)]},
            schema=pa_schema,
        )
        ds = ray.data.from_arrow(rows).repartition(4)
        write_paimon(ds, identifier, self.catalog_options)

        result = self._read_table(identifier)
        self.assertEqual(len(result), 40)
        self.assertEqual(set(result['id']), set(range(40)))
        self.assertNotIn('__paimon_bucket__', result.columns)

    def test_primary_key_fixed_bucket_default_fails_fast(self):
        from pypaimon.ray import write_paimon

        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            ('name', pa.string()),
        ])
        table_name = 'test_pk_fixed_bucket_default_fails_fast'
        identifier = self._make_table(
            table_name, pa_schema,
            primary_keys=['id'], options={'bucket': '4'},
        )

        rows = pa.Table.from_pydict(
            {'id': list(range(40)), 'name': [f'v{i}' for i in range(40)]},
            schema=pa_schema,
        )
        ds = ray.data.from_arrow(rows).repartition(4)

        with self.assertRaisesRegex(ValueError, "HASH_FIXED primary-key"):
            write_paimon(ds, identifier, self.catalog_options)

    def test_table_write_ray_primary_key_fixed_bucket_default_fails_fast(self):
        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            ('name', pa.string()),
        ])
        table_name = 'test_table_write_ray_pk_default_fails_fast'
        identifier = self._make_table(
            table_name, pa_schema,
            primary_keys=['id'], options={'bucket': '4'},
        )

        rows = pa.Table.from_pydict(
            {'id': list(range(40)), 'name': [f'v{i}' for i in range(40)]},
            schema=pa_schema,
        )
        ds = ray.data.from_arrow(rows).repartition(4)

        catalog = CatalogFactory.create(self.catalog_options)
        table = catalog.get_table(identifier)
        writer = table.new_batch_write_builder().new_write()
        try:
            with self.assertRaisesRegex(ValueError, "HASH_FIXED primary-key"):
                writer.write_ray(ds)
        finally:
            writer.close()

    def test_primary_key_dynamic_bucket_default_fails_fast(self):
        from pypaimon.ray import write_paimon

        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            ('name', pa.string()),
        ])
        table_name = 'test_pk_dynamic_bucket_default_fails_fast'
        identifier = self._make_table(
            table_name, pa_schema, primary_keys=['id'],
        )

        rows = pa.Table.from_pydict(
            {'id': list(range(40)), 'name': [f'v{i}' for i in range(40)]},
            schema=pa_schema,
        )
        ds = ray.data.from_arrow(rows).repartition(4)

        with self.assertRaisesRegex(ValueError, "HASH_DYNAMIC primary-key"):
            write_paimon(ds, identifier, self.catalog_options)

    def test_table_write_ray_primary_key_dynamic_bucket_default_fails_fast(self):
        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            ('name', pa.string()),
        ])
        table_name = 'test_table_write_ray_pk_dynamic_default_fails_fast'
        identifier = self._make_table(
            table_name, pa_schema, primary_keys=['id'],
        )

        rows = pa.Table.from_pydict(
            {'id': list(range(40)), 'name': [f'v{i}' for i in range(40)]},
            schema=pa_schema,
        )
        ds = ray.data.from_arrow(rows).repartition(4)

        catalog = CatalogFactory.create(self.catalog_options)
        table = catalog.get_table(identifier)
        writer = table.new_batch_write_builder().new_write()
        try:
            with self.assertRaisesRegex(ValueError, "HASH_DYNAMIC primary-key"):
                writer.write_ray(ds)
        finally:
            writer.close()

    def test_primary_key_postpone_bucket_roundtrip_to_postpone_files(self):
        from pypaimon.ray import write_paimon

        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            ('dt', pa.string()),
            ('value', pa.int64()),
        ])
        table_name = 'test_pk_postpone_bucket_ray_write'
        identifier = self._make_table(
            table_name, pa_schema,
            primary_keys=['id', 'dt'], partition_keys=['dt'],
            options={'bucket': '-2'},
        )

        rows = pa.Table.from_pydict({
            'id': list(range(10)),
            'dt': ['2026-01-01'] * 5 + ['2026-01-02'] * 5,
            'value': list(range(10)),
        }, schema=pa_schema)
        write_paimon(
            ray.data.from_arrow(rows).repartition(2),
            identifier,
            self.catalog_options,
        )

        files = self._count_data_files(table_name)
        self.assertGreater(len(files), 0)
        self.assertTrue(all('/bucket-postpone/' in path for path in files))
        self.assertEqual(len(self._read_table(identifier)), 0)

    def test_partitioned_fixed_bucket_roundtrip(self):
        """Partitioned table — confirms the post-groupby schema does not
        end up with duplicated partition-key or bucket columns."""
        from pypaimon.ray import write_paimon

        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            ('dt', pa.string()),
            ('value', pa.int64()),
        ])
        table_name = 'test_partitioned_fixed_bucket_roundtrip'
        identifier = self._make_table(
            table_name, pa_schema,
            primary_keys=['id', 'dt'], partition_keys=['dt'],
            options={'bucket': '4'},
        )

        rows = pa.Table.from_pydict({
            'id': list(range(20)),
            'dt': ['2026-01-01'] * 10 + ['2026-01-02'] * 10,
            'value': list(range(20)),
        }, schema=pa_schema)
        ds = ray.data.from_arrow(rows).repartition(4)
        write_paimon(
            ds,
            identifier,
            self.catalog_options,
            hash_fixed_precluster="map_groups",
        )

        result = self._read_table(identifier)
        self.assertEqual(set(result.columns), {'id', 'dt', 'value'})
        self.assertEqual(len(result), 20)
        self.assertEqual(set(result['dt']), {'2026-01-01', '2026-01-02'})

    def test_table_write_ray_primary_key_fixed_bucket_map_groups_roundtrip(self):
        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            ('name', pa.string()),
        ])
        table_name = 'test_table_write_ray_pk_map_groups'
        identifier = self._make_table(
            table_name, pa_schema,
            primary_keys=['id'], options={'bucket': '4'},
        )

        rows = pa.Table.from_pydict(
            {'id': list(range(40)), 'name': [f'v{i}' for i in range(40)]},
            schema=pa_schema,
        )
        ds = ray.data.from_arrow(rows).repartition(4)

        catalog = CatalogFactory.create(self.catalog_options)
        table = catalog.get_table(identifier)
        writer = table.new_batch_write_builder().new_write()
        try:
            writer.write_ray(ds, hash_fixed_precluster="map_groups")
        finally:
            writer.close()

        result = self._read_table(identifier)
        self.assertEqual(len(result), 40)
        self.assertEqual(set(result['id']), set(range(40)))

    def test_fixed_bucket_writes_one_file_per_bucket(self):
        """With multiple input blocks, explicit map_groups clustering
        collapses per-task files into per-bucket files."""
        from pypaimon.ray import write_paimon

        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            ('value', pa.int64()),
        ])
        rows = pa.Table.from_pydict(
            {'id': list(range(200)), 'value': list(range(200))},
            schema=pa_schema,
        )

        identifier = self._make_table(
            'test_one_file_per_bucket', pa_schema,
            primary_keys=['id'], options={'bucket': '4'},
        )

        # Materialise 4 input blocks. Without the explicit map_groups
        # mode, each task would emit one file per bucket it touched.
        write_paimon(
            ray.data.from_arrow(rows).repartition(4),
            identifier, self.catalog_options,
            hash_fixed_precluster="map_groups",
        )

        files = self._count_data_files('test_one_file_per_bucket')
        # 4 buckets × 1 file each.
        self.assertEqual(len(files), 4)

    def test_fixed_bucket_with_colliding_column_name(self):
        """A table that has a column named ``__paimon_bucket__`` must
        still work — the helper picks a collision-free transient
        column name."""
        from pypaimon.ray import write_paimon

        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            ('__paimon_bucket__', pa.string()),
        ])
        table_name = 'test_fixed_bucket_collide_col'
        identifier = self._make_table(
            table_name, pa_schema,
            primary_keys=['id'], options={'bucket': '2'},
        )

        rows = pa.Table.from_pydict(
            {'id': list(range(10)),
             '__paimon_bucket__': [f'v{i}' for i in range(10)]},
            schema=pa_schema,
        )
        ds = ray.data.from_arrow(rows).repartition(2)
        write_paimon(
            ds,
            identifier,
            self.catalog_options,
            hash_fixed_precluster="map_groups",
        )

        result = self._read_table(identifier)
        self.assertEqual(len(result), 10)
        self.assertEqual(set(result.columns), {'id', '__paimon_bucket__'})

    # ----- non-HASH_FIXED passthrough -----

    def test_non_fixed_bucket_roundtrip(self):
        """BUCKET_UNAWARE tables are written without pre-clustering;
        roundtrip data must still be correct."""
        from pypaimon.ray import read_paimon, write_paimon

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('value', pa.int64()),
        ])
        # bucket=-1 + no primary keys → BUCKET_UNAWARE
        table_name = 'test_non_fixed_bucket_roundtrip'
        identifier = self._make_table(
            table_name, pa_schema, options={'bucket': '-1'},
        )

        rows = pa.Table.from_pydict(
            {'id': list(range(10)), 'value': list(range(10))},
            schema=pa_schema,
        )
        write_paimon(
            ray.data.from_arrow(rows), identifier, self.catalog_options,
        )

        result = read_paimon(identifier, self.catalog_options).to_pandas()
        self.assertEqual(len(result), 10)
        self.assertEqual(set(result['id']), set(range(10)))


if __name__ == '__main__':
    unittest.main()
