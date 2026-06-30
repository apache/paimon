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

import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.common.predicate import Predicate
from pypaimon.manifest.schema.data_file_meta import DataFileMeta


class TestManifestReadRowRangePerformance(unittest.TestCase):
    """Reproduce: scan planning constructs ALL manifest entries even when
    row_ranges only match a few files."""

    NUM_COMMITS = 20
    ROWS_PER_COMMIT = 10

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('value', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'manifest.merge-min-count': '1',
        })
        cls.catalog.create_table('default.test_row_range_perf', schema, False)
        cls.table = cls.catalog.get_table('default.test_row_range_perf')
        cls.pa_schema = pa_schema

        write_builder = cls.table.new_batch_write_builder()
        for i in range(cls.NUM_COMMITS):
            tw = write_builder.new_write()
            tc = write_builder.new_commit()
            start_id = i * cls.ROWS_PER_COMMIT
            data = pa.Table.from_pydict({
                'id': list(range(start_id, start_id + cls.ROWS_PER_COMMIT)),
                'value': [f'v{j}' for j in range(cls.ROWS_PER_COMMIT)],
            }, schema=pa_schema)
            tw.write_arrow(data)
            cmts = tw.prepare_commit()
            for msg in cmts:
                for nf in msg.new_files:
                    nf.first_row_id = start_id
            tc.commit(cmts)
            tw.close()
            tc.close()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_scan_constructs_all_entries_without_early_row_range_filter(self):
        """With manifest.merge-min-count=1, all entries are in one manifest.
        Querying with _ROW_ID BETWEEN 5 AND 14 should return 2 files, but
        the current code constructs DataFileMeta for ALL 20 entries because
        the row-range filter runs after full object construction."""
        construction_count = [0]
        original_init = DataFileMeta.__init__

        def counting_init(self_meta, *args, **kwargs):
            construction_count[0] += 1
            original_init(self_meta, *args, **kwargs)

        predicate = Predicate(method='between', index=None,
                              field='_ROW_ID', literals=[5, 14])

        with patch.object(DataFileMeta, '__init__', counting_init):
            rb = self.table.new_read_builder().with_filter(predicate)
            splits = rb.new_scan().plan().splits()

        total_files = sum(len(s.files) for s in splits)
        self.assertEqual(total_files, 2)

        actual = self.table.new_read_builder().with_filter(predicate) \
            .new_read().to_arrow(splits)
        self.assertEqual(sorted(actual.column('id').to_pylist()),
                         list(range(5, 15)))

        # 2 matching files × 2 (ADD + DELETE from manifest merge) = 4
        self.assertLessEqual(
            construction_count[0], 2 * total_files,
            f"Expected at most {2 * total_files} DataFileMeta constructions, "
            f"got {construction_count[0]}")

    def test_add_delete_pair_not_broken_by_early_filter(self):
        """Guard ADD/DELETE merge semantics: when a file is overwritten,
        its ADD and DELETE entries share the same _FIRST_ROW_ID. The early
        filter must either keep both or drop both — never keep ADD but
        drop DELETE (which would resurrect a deleted file)."""
        pa_schema = self.pa_schema
        self.catalog.create_table('default.test_add_delete_pair',
                                  Schema.from_pyarrow_schema(pa_schema, options={
                                      'row-tracking.enabled': 'true',
                                      'data-evolution.enabled': 'true',
                                      'manifest.merge-min-count': '1',
                                  }), False)
        table = self.catalog.get_table('default.test_add_delete_pair')
        wb = table.new_batch_write_builder()

        # Commit 1: write file at row_id 0-9
        tw, tc = wb.new_write(), wb.new_commit()
        tw.write_arrow(pa.Table.from_pydict(
            {'id': list(range(10)), 'value': ['old'] * 10}, schema=pa_schema))
        cmts = tw.prepare_commit()
        for m in cmts:
            for nf in m.new_files:
                nf.first_row_id = 0
        tc.commit(cmts)
        tw.close()
        tc.close()

        # Commit 2: overwrite same row_id 0-9 with new data
        # This creates DELETE(old_file, row_id=0) + ADD(new_file, row_id=0)
        tw = wb.new_write().with_write_type(['id', 'value'])
        tc = wb.new_commit()
        tw.write_arrow(pa.Table.from_pydict(
            {'id': list(range(10)), 'value': ['new'] * 10}, schema=pa_schema))
        cmts = tw.prepare_commit()
        for m in cmts:
            for nf in m.new_files:
                nf.first_row_id = 0
        tc.commit(cmts)
        tw.close()
        tc.close()

        # Commit 3: write file at row_id 100-109 (unrelated, outside query range)
        tw, tc = wb.new_write(), wb.new_commit()
        tw.write_arrow(pa.Table.from_pydict(
            {'id': list(range(100, 110)), 'value': ['other'] * 10}, schema=pa_schema))
        cmts = tw.prepare_commit()
        for m in cmts:
            for nf in m.new_files:
                nf.first_row_id = 100
        tc.commit(cmts)
        tw.close()
        tc.close()

        # Query row_id [0, 9]: should return the NEW data, not the old
        predicate = Predicate(method='between', index=None,
                              field='_ROW_ID', literals=[0, 9])
        rb = table.new_read_builder().with_filter(predicate)
        splits = rb.new_scan().plan().splits()
        actual = rb.new_read().to_arrow(splits)

        values = actual.column('value').to_pylist()
        self.assertTrue(all(v == 'new' for v in values),
                        f"Expected all 'new' but got {values}. "
                        "Early filter may have dropped DELETE without its ADD.")

        # Also verify row_id [100, 109] is NOT in the result
        ids = actual.column('id').to_pylist()
        self.assertTrue(all(i < 100 for i in ids),
                        f"row_id [100,109] should be filtered out but got ids={ids}")


if __name__ == '__main__':
    unittest.main()
