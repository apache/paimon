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

"""End-to-end tests for shuffle / override_num_blocks on ``write_paimon``.

Covers:

  * back-compat: ``shuffle=False`` (default) write + read roundtrip.
  * HASH_FIXED + ``shuffle=True``: roundtrip correctness, output-file
    count reduction, transient bucket column stripped from the sink-
    visible schema.
  * Soft fallback: ``shuffle=True`` on a non-HASH_FIXED table logs a
    warning and writes successfully.
  * ``override_num_blocks`` alone (no shuffle) is a plain Ray block
    rebalance.
"""

import glob
import logging
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

    # ----- back-compat -----

    def test_shuffle_off_is_default_and_roundtrip_works(self):
        from pypaimon.ray import read_paimon, write_paimon

        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            ('name', pa.string()),
        ])
        table_name = 'test_shuffle_off'
        identifier = self._make_table(
            table_name, pa_schema,
            primary_keys=['id'], options={'bucket': '2'},
        )

        rows = pa.Table.from_pydict(
            {'id': list(range(20)), 'name': [f'v{i}' for i in range(20)]},
            schema=pa_schema,
        )
        write_paimon(ray.data.from_arrow(rows), identifier, self.catalog_options)

        result = read_paimon(identifier, self.catalog_options).to_pandas()
        self.assertEqual(len(result), 20)
        self.assertEqual(set(result['id']), set(range(20)))

    # ----- HASH_FIXED + shuffle=True -----

    def test_shuffle_on_fixed_bucket_roundtrip(self):
        from pypaimon.ray import read_paimon, write_paimon

        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            ('name', pa.string()),
        ])
        table_name = 'test_shuffle_on_roundtrip'
        identifier = self._make_table(
            table_name, pa_schema,
            primary_keys=['id'], options={'bucket': '4'},
        )

        rows = pa.Table.from_pydict(
            {'id': list(range(40)), 'name': [f'v{i}' for i in range(40)]},
            schema=pa_schema,
        )
        ds = ray.data.from_arrow(rows).repartition(4)
        write_paimon(ds, identifier, self.catalog_options, shuffle=True)

        result = read_paimon(identifier, self.catalog_options).to_pandas()
        self.assertEqual(len(result), 40)
        self.assertEqual(set(result['id']), set(range(40)))
        # Sink schema must not carry the transient shuffle column.
        self.assertNotIn('__paimon_bucket__', result.columns)

    def test_shuffle_on_partitioned_fixed_bucket_roundtrip(self):
        """Partitioned table — confirms the post-groupby schema does not
        end up with duplicated partition-key or bucket columns."""
        from pypaimon.ray import read_paimon, write_paimon

        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            ('dt', pa.string()),
            ('value', pa.int64()),
        ])
        table_name = 'test_shuffle_on_partitioned'
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
        write_paimon(ds, identifier, self.catalog_options, shuffle=True)

        result = read_paimon(identifier, self.catalog_options).to_pandas()
        self.assertEqual(set(result.columns), {'id', 'dt', 'value'})
        self.assertEqual(len(result), 20)
        self.assertEqual(set(result['dt']), {'2026-01-01', '2026-01-02'})

    def test_shuffle_reduces_data_file_count(self):
        """With multiple input blocks, shuffle=True collapses per-task
        files into per-bucket files."""
        from pypaimon.ray import write_paimon

        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            ('value', pa.int64()),
        ])
        rows = pa.Table.from_pydict(
            {'id': list(range(200)), 'value': list(range(200))},
            schema=pa_schema,
        )

        without_shuffle = self._make_table(
            'test_files_without_shuffle', pa_schema,
            primary_keys=['id'], options={'bucket': '4'},
        )
        with_shuffle = self._make_table(
            'test_files_with_shuffle', pa_schema,
            primary_keys=['id'], options={'bucket': '4'},
        )

        # Materialise 4 input blocks so shuffle=False has 4 writers per
        # bucket worth of opportunities to scatter rows.
        write_paimon(
            ray.data.from_arrow(rows).repartition(4),
            without_shuffle, self.catalog_options,
        )
        write_paimon(
            ray.data.from_arrow(rows).repartition(4),
            with_shuffle, self.catalog_options, shuffle=True,
        )

        files_off = self._count_data_files('test_files_without_shuffle')
        files_on = self._count_data_files('test_files_with_shuffle')

        # With 4 buckets and shuffle=True, every (partition, bucket) is
        # a single Ray task → 4 files (one per bucket).
        self.assertEqual(len(files_on), 4)
        # Without shuffle, multiple Ray tasks each produce one file per
        # bucket they touch → strictly more files.
        self.assertGreater(len(files_off), len(files_on))

    # ----- soft fallback -----

    def test_shuffle_on_non_fixed_bucket_falls_through(self):
        from pypaimon.ray import read_paimon, write_paimon

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('value', pa.int64()),
        ])
        # bucket=-1 + no primary keys → BUCKET_UNAWARE
        table_name = 'test_shuffle_on_unaware'
        identifier = self._make_table(
            table_name, pa_schema, options={'bucket': '-1'},
        )

        rows = pa.Table.from_pydict(
            {'id': list(range(10)), 'value': list(range(10))},
            schema=pa_schema,
        )
        with self.assertLogs('pypaimon.ray.shuffle', level=logging.WARNING) as cm:
            write_paimon(
                ray.data.from_arrow(rows), identifier,
                self.catalog_options, shuffle=True,
            )
        self.assertTrue(any('HASH_FIXED' in m for m in cm.output))

        result = read_paimon(identifier, self.catalog_options).to_pandas()
        self.assertEqual(len(result), 10)

    # ----- override_num_blocks-only path -----

    def test_invalid_override_num_blocks_raises(self):
        from pypaimon.ray import write_paimon

        pa_schema = pa.schema([('id', pa.int32())])
        identifier = self._make_table(
            'test_invalid_override_num_blocks', pa_schema,
        )
        ds = ray.data.from_arrow(pa.Table.from_pydict(
            {'id': [1]}, schema=pa_schema,
        ))

        with self.assertRaises(ValueError):
            write_paimon(
                ds, identifier, self.catalog_options, override_num_blocks=0,
            )

    def test_override_num_blocks_only_does_plain_rebalance(self):
        from pypaimon.ray import read_paimon, write_paimon

        pa_schema = pa.schema([('id', pa.int32())])
        table_name = 'test_override_num_blocks_only'
        identifier = self._make_table(table_name, pa_schema)

        rows = pa.Table.from_pydict({'id': list(range(30))}, schema=pa_schema)
        # Start with many blocks; override_num_blocks=2 + shuffle=False
        # should collapse them via plain Ray rebalance. Row content
        # unchanged.
        ds = ray.data.from_arrow(rows).repartition(8)
        write_paimon(
            ds, identifier, self.catalog_options, override_num_blocks=2,
        )

        result = read_paimon(identifier, self.catalog_options).to_pandas()
        self.assertEqual(set(result['id']), set(range(30)))


if __name__ == '__main__':
    unittest.main()
